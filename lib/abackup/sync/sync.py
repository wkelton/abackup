import datetime
import locale
import logging
import os
import re
import subprocess

from inspect import currentframe, getframeinfo
from typing import Dict

from abackup import fs, healthchecks as hc, notifications
from abackup.sync import Config, DataDir, Remote, SyncOptions, syncinfo


def get_path_from_remote(command: str, data_name: str, remote: Remote, absync_options: str, log: logging.Logger):
    ssh_command_list = ['ssh'] + remote.ssh_options() + [remote.connection_string()]
    sync_command = "bash --login -c 'absync {} {} {}'".format(absync_options, command, data_name)
    command_list = ssh_command_list + [sync_command]
    log.info("Running absync {} on remote...".format(command))
    log.debug(" ".join(command_list))
    run_out = subprocess.run(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if run_out.returncode == 0:
        path = run_out.stdout
        log.info("Succeeded getting {} from remote: {}".format(command, path))
        return path
    else:
        log.critical("Failed to get {} from remote!".format(command))
        log.critical(run_out.stderr)
        log.debug(run_out.stdout)
        return False


def get_owned_path_from_remote(owned_data_name: str, remote: Remote, absync_options: str, log: logging.Logger):
    return get_path_from_remote('owned-path', owned_data_name, remote, absync_options, log)


def get_stored_path_from_remote(stored_data_name: str, remote: Remote, absync_options: str, log: logging.Logger):
    return get_path_from_remote('stored-path', stored_data_name, remote, absync_options, log)


def handle_sync_results(notifier: notifications.SlackNotifier, data_name: str, remote_name: str, pull: bool,
                        sync_info: syncinfo.SyncInfo, notify_mode: notifications.Mode, log: logging.Logger):
    log.debug("handle_sync_results({}, {}, {}, {})".format(data_name, remote_name, notify_mode.name, pull))
    if not notifier:
        log.debug("handle_sync_results({}, {}, {}, {}): skipping notify (because no notifier was supplied)".format(
            data_name, remote_name, notify_mode.name, pull))
    elif notify_mode == notifications.Mode.ALWAYS:
        severity = notifications.Severity.GOOD
        title = "Sync Successful"
        fields = {"Data": data_name, "Remote": remote_name, "Pull": pull, "Sync Type": sync_info.sync_type,
                  "Duration": str(sync_info.duration), "Sent": sync_info.sync_count, "Deleted": sync_info.sync_deleted,
                  "Bytes": fs.to_human_readable(sync_info.sync_bytes)}
        frame_info = getframeinfo(currentframe())
        response = notifier.notify(title, severity, fields=fields, file_name=os.path.basename(frame_info.filename),
                                   line_number=frame_info.lineno, time=datetime.datetime.now().timestamp())
        if response.is_error():
            log.error("Error during notify: code: {} message: {}".format(response.code, response.message))
        else:
            log.debug("handle_sync_results({}, {}, {}, {}): notify successful: code: {} message: {}".format(
                data_name, remote_name, notify_mode.name, response.code, response.message, pull))
    else:
        log.debug("handle_sync_results({}, {}, {}, {}): skipping notify".format(data_name, remote_name,
                                                                                notify_mode.name, pull))


def handle_failed_sync(notifier: notifications.SlackNotifier, data_name: str, remote_name: str, pull: bool,
                       error_message: str, notify_mode: notifications.Mode, log: logging.Logger):
    log.debug("handle_failed_sync({}, {}, {}, {})".format(data_name, remote_name, notify_mode.name, pull))
    if not notifier:
        log.debug("handle_failed_sync({}, {}, {}, {}): skipping notify (because no notifier was supplied)".format(
            data_name, remote_name, notify_mode.name, pull))
    elif notify_mode != notifications.Mode.NEVER:
        severity = notifications.Severity.CRITICAL
        title = "Sync FAILED"
        fields = {"Data": data_name, "Remote": remote_name, "Pull": pull, "Error": error_message}
        frame_info = getframeinfo(currentframe())
        response = notifier.notify(title, severity, fields=fields, file_name=os.path.basename(frame_info.filename),
                                   line_number=frame_info.lineno, time=datetime.datetime.now().timestamp())
        if response.is_error():
            log.error("Error during notify: code: {} message: {}".format(response.code, response.message))
        else:
            log.debug("handle_failed_sync({}, {}, {}, {}): notify successful: code: {} message: {}".format(
                data_name, remote_name, notify_mode.name, pull, response.code, response.message))
    else:
        log.debug("handle_failed_sync({}, {}, {}, {}): skipping notify".format(data_name, remote_name,
                                                                               notify_mode.name, pull))


def do_sync(origin: str, destination: str, sync_options: SyncOptions, log: logging.Logger, remote: Remote = None,
            sync_type: str = 'manual', pull: bool = False):
    command_list = ['rsync', '-az', '--stats', '--info=del', '--info=name']
    if sync_options.delete:
        if sync_options.max_delete:
            command_list.extend(['--delete', "--max-delete={}".format(sync_options.max_delete)])
        else:
            command_list.append('--delete')
    if remote:
        if remote.ssh_options():
            command_list.extend(['-e', "ssh {}".format(" ".join(remote.ssh_options()))])
        if pull:
            command_list.append("{}:{}".format(remote.connection_string(), origin))
            command_list.append(destination)
        else:
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
        deleted_files = []
        transferred_files = []
        max_transferred_files = 1000
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
            elif len(transferred_files) < max_transferred_files:
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
        if len(transferred_files) == max_transferred_files:
            transferred_files.append('...')
        info = syncinfo.SyncInfo(sync_type, timestamp, duration, origin, destination, sync_count, sync_deleted,
                                 sync_bytes, transferred_files, deleted_files, remote.host if remote else None, pull)
        log.info(info)
        return info
    else:
        log.critical("rsync failed!")
        log.critical(run_out.stderr)
        log.debug(run_out.stdout)
        return False


def do_auto_sync(config: Config, data_name: str, data_dir: DataDir, absync_options: str, notify: str,
                 log: logging.Logger, only_remote_name: str = None, sync_type: str = 'manual', pull: bool = False,
                 do_healthchecks: bool = True):
    sync_succeeded = True
    sync_infos = []
    notify_mode = notifications.Mode(notify)
    for auto_sync in data_dir.auto_sync:
        remote_name = auto_sync.remote_name
        remote = config.remotes[remote_name]
        if only_remote_name and remote_name != only_remote_name:
            continue

        if do_healthchecks and auto_sync.healthchecks:
            hc.perform_healthcheck_start(config.default_healthcheck, auto_sync.healthchecks, remote_name,
                                         config.notifier, notify_mode, log)

        error_message = None
        have_remote_path = True
        if pull:
            origin = get_owned_path_from_remote(data_name, remote, absync_options, log)
            destination = data_dir.path
            if not origin:
                error_message = "Failed to get owned path from {}!".format(auto_sync.remote_name)
                handle_failed_sync(config.notifier, data_name, remote_name, pull, error_message, notify_mode, log)
                have_remote_path = False
        else:
            origin = data_dir.path
            destination = get_stored_path_from_remote(data_name, remote, absync_options, log)
            if not destination:
                error_message = "Failed to get stored path from {}!".format(auto_sync.remote_name)
                handle_failed_sync(config.notifier, data_name, remote_name, pull, error_message, notify_mode, log)
                have_remote_path = False

        if have_remote_path:
            log.info("syncing {} with {}".format("{}:{}".format(remote_name, origin) if pull else origin,
                                                 "{}:{}".format(remote_name, destination) if not pull else destination))
            ret = do_sync(origin, destination, data_dir.options.mask(auto_sync.options), log, remote, sync_type, pull)
            if not ret:
                error_message = "Failed syncing with {}!".format(auto_sync.remote_name)
                handle_failed_sync(config.notifier, data_name, remote_name, pull, error_message, notify_mode, log)
                sync_succeeded = False
            else:
                handle_sync_results(config.notifier, data_name, remote_name, pull, ret, notify_mode, log)
                sync_infos.append(ret)

        sync_succeeded = sync_succeeded and have_remote_path

        if do_healthchecks and auto_sync.healthchecks:
            hc.perform_healthcheck(config.default_healthcheck, auto_sync.healthchecks, remote_name,
                                   config.notifier, notify_mode, log, is_fail=not sync_succeeded, message=error_message)

    if len(sync_infos) > 0:
        syncinfo.write_sync_infos(sync_infos, config, data_name)
    return sync_succeeded


def perform_sync(config: Config, data_name: str, origin_destination: str, pull: bool, log: logging.Logger,
                 delete: bool = None, max_delete: int = None, port: int = None):
    if pull:
        data = config.stored_data
    else:
        data = config.owned_data
    if data_name not in data:
        log.critical("{} data directory not present in config!".format(data_name))
        return False
    log.info("\tonly for {}".format(data_name))

    def _parse_remote(remote_string: str, remotes: Dict[str, Remote], log_: logging.Logger):
        remote_ = None
        remote_path = remote_string
        remote_regex = re.compile(r"^([a-zA-Z0-9.]+):(.*)")
        remote_match = remote_regex.match(remote_string)
        if remote_match:
            remote_name = remote_match.group(1)
            remote_path = remote_match.group(2)
            if remote_name in remotes:
                remote_ = remotes[remote_name]
                log_.info("found remote in config: {} -> {}".format(remote_name, remote_.host))
            else:
                remote_ = Remote(remote_name)
        return remote_, remote_path

    data_dir = data[data_name]
    if pull:
        remote, origin = _parse_remote(origin_destination, config.remotes, log)
        destination = data_dir.path
    else:
        origin = data_dir.path
        remote, destination = _parse_remote(origin_destination, config.remotes, log)

    if remote:
        if port:
            remote.port = port
        log.info("syncing {} with {}".format("{}:{}".format(remote.host, origin) if pull else origin,
                                             "{}:{}".format(remote.host, destination) if not pull else destination))
        log.debug(remote)
    else:
        log.info("syncing {} with {}".format(origin, destination))

    ret = do_sync(origin, destination, data_dir.options.mask(SyncOptions(delete, max_delete)), log, remote, pull=pull)

    if ret:
        syncinfo.write_sync_infos([ret], config, data_name)
        return True
    return False


def perform_auto_sync(config: Config, absync_options: str, notify: str, log: logging.Logger,
                      only_data_name: str = None, only_remote_name: str = None, sync_type: str = 'manual',
                      do_healthchecks: bool = True):
    if only_data_name:
        if only_data_name not in config.owned_data and only_data_name not in config.stored_data:
            log.critical("{} data directory not present in config!".format(only_data_name))
            return False
        log.info("\tonly for {}".format(only_data_name))
    sync_succeeded = True
    for name, data_dir in config.owned_data.items():
        if only_data_name and name != only_data_name:
            continue
        sync_succeeded = do_auto_sync(config, name, data_dir, absync_options, notify, log, only_remote_name, sync_type,
                                      pull=False, do_healthchecks=do_healthchecks) and sync_succeeded
    for name, data_dir in config.stored_data.items():
        if only_data_name and name != only_data_name:
            continue
        sync_succeeded = do_auto_sync(config, name, data_dir, absync_options, notify, log, only_remote_name, sync_type,
                                      pull=True, do_healthchecks=do_healthchecks) and sync_succeeded
    return sync_succeeded
