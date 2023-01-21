import functools
import os
import logging
from typing import Any, Dict

from abackup import RemoteCommand
from abackup.backup import Config as BackupConfig
from abackup.backup.backup import get_backups
from abackup.backup.project import Container, ProjectConfig
from abackup.sync import Remote


def get_recent_local_backup(
    backup_config: BackupConfig, project_name: str, container: Container, identifier: str, log: logging.Logger
):
    backups = get_backups(backup_config, project_name, [container], True, log, identifier)
    if len(backups) > 1:
        log.error("too many backup files found: {}".format(",".join(backups)))
        return None
    elif len(backups) == 0:
        log.error("no backup file found!")
        return None
    return backups[0]


class CopyRecentBackupOptions:
    def __init__(
        self,
        abackup_config: str,
        project_config: str,
        container: str,
        identifier: str,
        pull: bool = False,
    ):
        self.abackup_config = abackup_config
        self.project_config = project_config
        self.container = container
        self.identifier = identifier
        self.pull = pull


def construct_copy_recent_backup_command(
    copy_options: CopyRecentBackupOptions,
    data_name: str,
    sync_root: str,
    absync_options: str,
    remote: Remote,
    log: logging.Logger,
):
    backup_config = BackupConfig(copy_options.abackup_config, True, False, "absync::prepare")
    project_config = ProjectConfig(copy_options.project_config)

    backup_path = get_recent_local_backup(
        backup_config,
        os.path.basename(os.path.dirname(copy_options.project_config)),
        project_config.container(copy_options.container),
        copy_options.identifier,
        log,
    )

    if not backup_path:
        log.error("cannot copy recent backup!")
        return None

    backup_name = os.path.basename(backup_path)
    relative_path = os.path.relpath(os.path.dirname(backup_path), os.path.dirname(sync_root))

    absync_command = "absync {} copy-most-recent {} {} {}".format(absync_options, data_name, relative_path, backup_name)

    return RemoteCommand(remote.ssh_options(), remote.connection_string(), absync_command, universal_newlines=True)


def construct_commands(
    command_type: str,
    command_options: Dict[str, Any],
    log: logging.Logger,
    data_name: str,
    sync_root: str,
    absync_options: str,
    remote: Remote,
):
    if command_type != "copy_recent_backup_local_on_target":
        return None
    return construct_copy_recent_backup_command(
        CopyRecentBackupOptions(**command_options), data_name, sync_root, absync_options, remote, log
    )


def construct_commands_wrapper(
    data_name: str,
    sync_root: str,
    absync_options: str,
    remote: Remote,
):
    return functools.partial(
        construct_commands, data_name=data_name, sync_root=sync_root, absync_options=absync_options, remote=remote
    )
