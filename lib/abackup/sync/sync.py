import datetime
import locale
import logging
import os
import re
import subprocess

from inspect import currentframe, getframeinfo

from abackup import fs, notifications
from abackup.sync import Config, DataDir, Remote, SyncOptions, syncinfo


def get_stored_path_from_remote(stored_data_name: str, remote: Remote, absync_options: str, log: logging.Logger):
    ssh_command_list = ['ssh'] + remote.ssh_options() + [remote.connection_string()]
    sync_command = "bash --login -c 'absync {} stored-path {}'".format(absync_options, stored_data_name)
    command_list = ssh_command_list + [ sync_command ]
    log.info("Running absync stored-path on remote...")
    log.debug(" ".join(command_list))
    run_out = subprocess.run(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if run_out.returncode == 0:
        path = run_out.stdout
        log.info("Succeeded getting stored path from remote: {}".format(path))
        return path
    else:
        log.critical("Failed to get stored path from remote!")
        log.critical(run_out.stderr)
        log.debug(run_out.stdout)
        return False


def handle_sync_results(notifier: notifications.SlackNotifier, local_name: str, remote_name: str,
    syncinfo: syncinfo.SyncInfo, notify_mode: notifications.Mode, log: logging.Logger):
    log.debug("handle_sync_results({}, {}, {})".format(local_name, remote_name, notify_mode.name))
    if not notifier:
        log.debug("handle_sync_results({}, {}, {}): skipping notify (because no notifier was supplied)".format(
            local_name, remote_name, notify_mode.name))
    elif notify_mode == notifications.Mode.ALWAYS:
        severity = notifications.Severity.GOOD
        title = "Sync Successful"
        fields = {"Local": local_name, "Remote": remote_name, "Sync Type": syncinfo.sync_type,
                  "Duration": str(syncinfo.duration), "Sent": syncinfo.sync_count, "Deleted": syncinfo.sync_deleted,
                  "Bytes": fs.to_human_readable(syncinfo.sync_bytes) }
        frameinfo = getframeinfo(currentframe())
        response = notifier.notify(title, severity, fields=fields, file_name=os.path.basename(frameinfo.filename),
                                   line_number=frameinfo.lineno, time=datetime.datetime.now().timestamp())
        if response.is_error():
            log.error("Error during notify: code: {} message: {}".format(response.code, response.message))
        else:
            log.debug("handle_sync_results({}, {}, {}): notify successful: code: {} message: {}".format(local_name,
                remote_name, notify_mode.name, response.code, response.message))
    else:
        log.debug("handle_sync_results({}, {}, {}): skipping notify".format(local_name, remote_name, notify_mode.name))


def handle_failed_sync(notifier: notifications.SlackNotifier, local_name: str, remote_name: str, error_message: str,
    notify_mode: notifications.Mode, log: logging.Logger):
    log.debug("handle_failed_sync({}, {}, {})".format(local_name, remote_name, notify_mode.name))
    if not notifier:
        log.debug("handle_failed_sync({}, {}, {}): skipping notify (because no notifier was supplied)".format(
            local_name, remote_name, notify_mode.name))
    elif notify_mode != notifications.Mode.NEVER:
        severity = notifications.Severity.CRITICAL
        title = "Sync FAILED"
        fields = { "Local": local_name, "Remote": remote_name, "Error": error_message }
        frameinfo = getframeinfo(currentframe())
        response = notifier.notify(title, severity, fields=fields, file_name=os.path.basename(frameinfo.filename),
                                   line_number=frameinfo.lineno, time=datetime.datetime.now().timestamp())
        if response.is_error():
            log.error("Error during notify: code: {} message: {}".format(response.code, response.message))
        else:
            log.debug("handle_failed_sync({}, {}, {}): notify successful: code: {} message: {}".format(local_name,
                remote_name, notify_mode.name, response.code, response.message))
    else:
        log.debug("handle_failed_sync({}, {}, {}): skipping notify".format(local_name, remote_name, notify_mode.name))


def do_sync(origin: str, destination: str, sync_options: SyncOptions, log: logging.Logger, remote: Remote = None,
    sync_type: str = 'manual'):
    command_list = ['rsync', '-az', '--stats', '--info=del', '--info=name']
    if sync_options.delete:
        command_list.extend(['--delete', "--max-delete={}".format(sync_options.max_delete)])
    if remote:
        if remote.ssh_options():
            command_list.extend(['-e', "ssh {}".format(" ".join(remote.ssh_options()))])
        command_list.append(origin)
        command_list.append("{}:{}".format(remote.connection_string(), destination))
    else:
        command_list.append(origin)
        command_list.append(destination)
    log.info("Running rsync...")
    log.debug(command_list)
    timestamp = datetime.datetime.now()
    run_out = subprocess.run(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    duration = datetime.datetime.now() - timestamp
    if run_out.returncode == 0:
        log.info("rsync succeeded")
        deleted_files = [ ]
        transferred_files = [ ]
        sync_count = 0
        sync_deleted = 0
        sync_bytes = 0
        deleted_files_regex = re.compile(r"^deleting\s+(.*)$")
        transferred_regex = re.compile(r"^([^/]+/(?:[^/]+/*)*)$")
        count_regex = re.compile(r"Number of regular files transferred:\s+([\d,]+)")
        deleted_regex = re.compile(r"Number of deleted files:\s+([\d,]+)")
        bytes_regex = re.compile(r"Total transferred file size:\s+([\d,]+)\s+bytes")
        for line in run_out.stdout.split("\n"):
            deleted_files_match = deleted_files_regex.match(line)
            count_match = count_regex.search(line)
            deleted_match = deleted_regex.search(line)
            bytes_match = bytes_regex.search(line)
            if deleted_files_match:
                deleted_files.append(deleted_files_match.group(1))
            elif len(transferred_files) < 1000:
                transferred_match = transferred_regex.match(line)
                if transferred_match:
                    tf = transferred_match.group(1)
                    if not tf.startswith("created directory") and not tf.endswith("bytes/sec"):
                        transferred_files.append(transferred_match.group(1))
            if count_match:
                sync_count = locale.atoi(count_match.group(1))
            if deleted_match:
                sync_deleted = locale.atoi(deleted_match.group(1))
            if bytes_match:
                sync_bytes = locale.atoi(bytes_match.group(1))
        if len(transferred_files) == 1000:
            transferred_files.append('...')
        info = syncinfo.SyncInfo(sync_type, timestamp, duration, destination, sync_count, sync_deleted, sync_bytes,
            transferred_files, deleted_files, remote.host if remote else None)
        log.info(info)
        return info
    else:
        log.critical("rsync failed!")
        log.critical(run_out.stderr)
        log.debug(run_out.stdout)
        return False


def do_auto_sync(config: Config, local_name: str, data_dir: DataDir, absync_options: str, notify: str,
    log: logging.Logger, only_remote_name: str = None, sync_type: str = 'manual'):
    sync_succeeded = True
    path = data_dir.path
    syncinfos = []
    for auto_sync in data_dir.auto_sync:
        remote_name = auto_sync.remote_name
        if only_remote_name and remote_name != only_remote_name:
            continue
        remote = config.remotes[remote_name]
        destination = get_stored_path_from_remote(local_name, remote, absync_options, log)
        if destination == False:
            handle_failed_sync(config.notifier, local_name, remote_name, "Failed to get stored path from remote.",
                notifications.Mode(notify), log)
            sync_succeeded = False
        else:
            log.info("syncing {} with {}:{}".format(path, remote_name, destination))
            ret = do_sync(path, destination, data_dir.options.mask(auto_sync.options), log, remote, sync_type)
            if ret == False:
                handle_failed_sync(config.notifier, local_name, remote_name, "Failed doing sync.",
                    notifications.Mode(notify), log)
                sync_succeeded = False
            else:
                handle_sync_results(config.notifier, local_name, remote_name, ret, notifications.Mode(notify), log)
                syncinfos.append(ret)
    if len(syncinfos) > 0:
        syncinfo.write_sync_infos(syncinfos, config, local_name)
    return sync_succeeded


def perform_sync(config: Config, local_name: str, destination: str, log: logging.Logger, delete: bool = None,
    max_delete: int = None, port: int = None):
    if not local_name in config.owned_data:
        log.critical("{} data directory not present in config!".format(local_name))
        return False

    log.info("\tonly for {}".format(local_name))

    path = config.owned_data[local_name].path
    remote = None
    remote_regex = re.compile(r"^([a-zA-Z0-9.]+):(.*)")
    remote_match = remote_regex.match(destination)
    if remote_match:
        remote_name = remote_match.group(1)
        destination = remote_match.group(2)
        if remote_name in config.remotes:
            remote = config.remotes[remote_name]
            log.info("found remote in config: {} -> {}".format(remote_name, remote.host))
        else:
            remote = Remote(remote_name)
        if port:
            remote.port = port
        log.info("syncing {} with {}:{}".format(path, remote.host, destination))
        log.debug(remote)
    else:
        log.info("syncing {} with {}".format(path, destination))

    ret = do_sync(path, destination, config.owned_data[local_name].options.mask(SyncOptions(delete, max_delete)), log)

    if ret == False:
        return False
    else:
        syncinfo.write_sync_infos([ret], config, local_name)
        return True


def perform_auto_sync(config: Config, absync_options: str, notify: str, log: logging.Logger,
    only_local_name: str = None, only_remote_name: str = None, sync_type: str = 'manual'):
    if only_local_name:
        if not only_local_name in config.owned_data:
            log.critical("{} data directory not present in config!".format(only_local_name))
            return False
        log.info("\tonly for {}".format(only_local_name))
        if not do_auto_sync(config, only_local_name, config.owned_data[only_local_name], absync_options, notify, log,
            only_remote_name, sync_type):
            return False
        else:
            return True
    else:
        sync_succeeded = True
        for name, data_dir in config.owned_data.items():
            sync_succeeded = sync_succeeded and do_auto_sync(config, name, data_dir, absync_options, notify, log,
                only_remote_name, sync_type)
        return sync_succeeded
