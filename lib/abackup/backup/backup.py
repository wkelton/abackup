import datetime
import logging
import os

from inspect import Traceback, currentframe, getframeinfo
from typing import List

from abackup import fs, healthchecks as hc, notifications
from abackup.backup import Config
from abackup.backup.project import Container


def notify_or_log(notifier: notifications.SlackNotifier, container_name: str, successful_commands: List[str],
                  failed_commands: List[str], notify_mode: notifications.Mode, log: logging.Logger,
                  frame_info: Traceback):
    log.debug("notify_or_log({}, successful:{}, failed:{}, {})".format(container_name,
                                                                       len(successful_commands), len(failed_commands),
                                                                       notify_mode.name))

    failed = len(failed_commands) > 0
    do_notify = notify_mode == notifications.Mode.ALWAYS or (notify_mode == notifications.Mode.AUTO and failed)

    if not notifier:
        log.debug(
            "notify_or_log({}, successful:{}, failed:{}, {}): skipping notify (because no notifier was supplied)".format(
                container_name, len(successful_commands), len(failed_commands), notify_mode.name))
    elif do_notify:
        log.info("Sending notifications for {}:{} {}".format(container_name,
                                                             "FAILURE" if failed else "SUCCESS", notify_mode.name))
        if failed:
            severity = notifications.Severity.ERROR
            title = "Failed to Backup {} ".format(container_name)
        else:
            severity = notifications.Severity.GOOD
            title = "{} Backed Up".format(container_name)
        fields = {}
        if successful_commands:
            fields["Successful"] = "\n".join(successful_commands)
        if failed_commands:
            fields["Failure"] = "\n".join(failed_commands)
        response = notifier.notify(title, severity, fields=fields, file_name=os.path.basename(frame_info.filename),
                                   line_number=frame_info.lineno, time=datetime.datetime.now().timestamp())
        if response.is_error():
            log.error("Error during notify: code: {} message: {}".format(response.code, response.message))
        else:
            log.debug("notify_or_log({}, successful:{}, failed:{}, {}): notify successful: code: {} message: {}".format(
                container_name, len(successful_commands), len(failed_commands), notify_mode.name,
                response.code, response.message))
    else:
        log.debug("notify_or_log({}, successful:{}, failed:{}, {}): skipping notify".format(
            container_name, len(successful_commands), len(failed_commands), notify_mode.name))


def remove_backup(count: int, path: str, prefix: str, extension: str = None, log: logging.Logger = None):
    filenames = fs.find_files(path, prefix, extension)
    if len(filenames) > count:
        os.remove(os.path.join(path, fs.find_oldest_file(path, prefix, extension)))
        if log:
            log.info("removed previous backup")


def perform_backup(config: Config, project_name: str, containers: List[Container],
                   notify_mode: notifications.Mode, log: logging.Logger, do_healthchecks: bool = True):
    success = True
    for container in containers:
        log.info(container.name)
        if not container.backup:
            log.info("skipping {}, no backup settings defined".format(container.name))
            continue

        if do_healthchecks and container.backup.healthchecks:
            hc.perform_healthcheck_start(config.default_healthcheck, container.backup.healthchecks, container.name,
                                         config.notifier, notify_mode, log)

        successful_commands = []
        failed_commands = []
        backup_path = config.ensure_backup_path(project_name, container.name)
        skip_backup = False
        for command in container.backup.pre_commands:
            if command.run(log):
                successful_commands.append(command.command_string)
            else:
                log.error("failed running pre command, skipping container: {}".format(container.name))
                skip_backup = True
                failed_commands.append(command.command_string)
                break

        if not skip_backup:
            for command in container.build_database_backup_commands(backup_path):
                if command.run(log):
                    os.chmod(command.backup_file_path, config.file_permissions)
                    remove_backup(container.backup.version_count, backup_path, command.file_prefix,
                                  command.file_extension, log)
                    successful_commands.append(command.friendly_str())
                else:
                    log.error("failed running database backup for {}".format(command.name))
                    failed_commands.append(command.friendly_str())
            for command in container.build_directory_backup_commands(backup_path):
                if command.run(log):
                    os.chmod(command.backup_file_path, config.file_permissions)
                    remove_backup(container.backup.version_count, backup_path, command.file_prefix,
                                  command.file_extension, log)
                    successful_commands.append(command.friendly_str())
                else:
                    log.error("failed running directory backup for {}".format(command.directory))
                    failed_commands.append(command.friendly_str())
            for command in container.backup.post_commands:
                if command.run(log):
                    successful_commands.append(command.command_string)
                else:
                    log.error("failed running post command")
                    failed_commands.append(command.command_string)
                    break

        backup_failed = len(failed_commands) > 0

        notify_or_log(config.notifier, container.name, successful_commands, failed_commands, notify_mode, log,
                      getframeinfo(currentframe()))

        if do_healthchecks and container.backup.healthchecks:
            hc.perform_healthcheck(config.default_healthcheck, container.backup.healthchecks, container.name,
                                   config.notifier, notify_mode, log, is_fail=backup_failed,
                                   message="Failed commands: {}".format(
                                       '\n'.join(failed_commands)) if failed_commands else None)

        success = success and not backup_failed

    return success
