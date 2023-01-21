import datetime
import locale
import logging
import re
import subprocess
from typing import List

from abackup import RemoteCommand, build_commands, fs, healthchecks as hc, notifications
from abackup.prepare import rsync as prepare_rsync

from abackup.sync import AutoSync, Config, DataDir, Remote, RsyncOptions, syncinfo


class RsyncInfo(syncinfo.SyncInfo):
    def __init__(
        self,
        sync_name: str,
        sync_type: str,
        timestamp: datetime.datetime,
        duration: datetime.timedelta,
        origin: str,
        destination: str,
        sync_count: int,
        sync_deleted: int,
        sync_bytes: int,
        transferred_files: List[str],
        deleted_files: List[str],
        remote_host: str = None,
        pull: bool = False,
    ):
        self.origin = origin
        self.destination = destination
        self.remote_host = remote_host
        self.pull = pull
        self.sync_count = sync_count
        self.sync_deleted = sync_deleted
        self.sync_bytes = sync_bytes
        self.transferred_files = transferred_files
        self.deleted_files = deleted_files
        super().__init__(
            sync_name,
            sync_type,
            timestamp,
            duration,
            {"remote_host": remote_host, "origin": origin, "destination": destination, "pull": pull},
            {
                "sync_count": sync_count,
                "sync_deleted": sync_deleted,
                "sync_bytes": fs.to_human_readable(sync_bytes),
                "transferred_files": transferred_files,
                "deleted_files": deleted_files,
            },
        )

    def __str__(self):
        return "Sync {}: {} at {} for {} from {} to {} --- transferred {} files with {} bytes".format(
            self.name,
            self.sync_type,
            self.timestamp,
            self.duration,
            "{}:{}".format(self.remote_host, self.origin) if self.remote_host and self.pull else self.origin,
            "{}:{}".format(self.remote_host, self.destination)
            if self.remote_host and not self.pull
            else self.destination,
            self.sync_count,
            self.sync_bytes,
        )

    def flatten_transfer_info(self, pretty: bool = True):
        m = {}
        for k, v in self.transfer_info.items():
            value = v
            if k == "transferred_files" or k == "deleted_files":
                value = len(v)
            if pretty:
                m[k.replace("_", " ").title()] = value
            else:
                m[k] = value
        return m


def get_path_from_remote(command: str, data_name: str, remote: Remote, absync_options: str, log: logging.Logger):
    absync_command = "absync {} {} {}".format(absync_options, command, data_name)
    run_out = RemoteCommand(
        remote.ssh_options(), remote.connection_string(), absync_command, universal_newlines=True
    ).run_with_result(log)

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
    return get_path_from_remote("owned-path", owned_data_name, remote, absync_options, log)


def get_stored_path_from_remote(stored_data_name: str, remote: Remote, absync_options: str, log: logging.Logger):
    return get_path_from_remote("stored-path", stored_data_name, remote, absync_options, log)


def do_rsync(
    origin: str,
    destination: str,
    rsync_options: RsyncOptions,
    log: logging.Logger,
    sync_name: str = "manual",
    sync_type: str = "manual",
    remote: Remote = None,
    pull: bool = False,
):
    command_list = ["rsync", "-az", "--stats", "--info=del", "--info=name"]
    command_list.extend(rsync_options.options_list())
    if remote:
        if remote.ssh_options():
            command_list.extend(["-e", "ssh {}".format(" ".join(remote.ssh_options()))])
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
        if pull:
            bytes_regex = re.compile(r"Total bytes received:\s+([\d,]+)")
        else:
            bytes_regex = re.compile(r"Total bytes sent:\s+([\d,]+)")
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
            transferred_files.append("...")
        info = RsyncInfo(
            sync_name,
            sync_type,
            timestamp,
            duration,
            origin,
            destination,
            sync_count,
            sync_deleted,
            sync_bytes,
            transferred_files,
            deleted_files,
            remote.host if remote else None,
            pull,
        )
        log.info(info)
        return info
    else:
        log.critical("rsync failed!")
        log.critical(run_out.stderr)
        log.debug(run_out.stdout)
        return False


def do_auto_rsync(
    config: Config,
    data_name: str,
    data_dir: DataDir,
    auto_sync: AutoSync,
    absync_options: str,
    notify: str,
    log: logging.Logger,
    sync_type: str = "manual",
    pull: bool = False,
    do_healthchecks: bool = True,
):
    sync_info = None
    notify_mode = notifications.Mode(notify)

    remote_name = auto_sync.driver.settings.remote_name
    remote = config.remotes[remote_name]

    if do_healthchecks and auto_sync.healthchecks:
        hc.perform_healthcheck_start(
            config.default_healthcheck, auto_sync.healthchecks, remote_name, config.notifier, notify_mode, log
        )

    if auto_sync.pre_commands:
        commands = build_commands(
            auto_sync.pre_commands,
            ["copy_recent_backup_local_on_target"],
            prepare_rsync.construct_commands_wrapper(data_name, data_dir.path, absync_options, remote),
            log,
        )
        if not commands:
            log.critical("failed running pre command, stopping sync for {}".format(data_name))
            return None
        for command in commands:
            if not command.run(log):
                log.critical("failed running pre command, stopping sync for {}".format(data_name))
                return None

    error_message = None
    have_remote_path = True
    if pull:
        origin = get_owned_path_from_remote(data_name, remote, absync_options, log)
        destination = data_dir.path
        if not origin:
            error_message = "Failed to get owned path from {}!".format(auto_sync.driver.settings.remote_name)
            syncinfo.handle_failed_sync(config.notifier, data_name, remote_name, pull, error_message, notify_mode, log)
            have_remote_path = False
    else:
        origin = data_dir.path
        destination = get_stored_path_from_remote(data_name, remote, absync_options, log)
        if not destination:
            error_message = "Failed to get stored path from {}!".format(auto_sync.driver.settings.remote_name)
            syncinfo.handle_failed_sync(config.notifier, data_name, remote_name, pull, error_message, notify_mode, log)
            have_remote_path = False

    if have_remote_path:
        log.info(
            "syncing {} with {}".format(
                "{}:{}".format(remote_name, origin) if pull else origin,
                "{}:{}".format(remote_name, destination) if not pull else destination,
            )
        )
        ret = do_rsync(
            origin,
            destination,
            data_dir.rsync_options.mask(auto_sync.driver.settings.options),
            log,
            auto_sync.sync_name,
            sync_type,
            remote,
            pull,
        )
        if not ret:
            error_message = "Failed syncing with {}!".format(auto_sync.driver.settings.remote_name)
            syncinfo.handle_failed_sync(config.notifier, data_name, remote_name, pull, error_message, notify_mode, log)
        else:
            syncinfo.handle_sync_results(config.notifier, data_name, remote_name, pull, ret, notify_mode, log)
            sync_info = ret

    if do_healthchecks and auto_sync.healthchecks:
        hc.perform_healthcheck(
            config.default_healthcheck,
            auto_sync.healthchecks,
            remote_name,
            config.notifier,
            notify_mode,
            log,
            is_fail=sync_info is None,
            message=error_message,
        )

    return sync_info
