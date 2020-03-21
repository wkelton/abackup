import logging

from typing import List

from abackup.backup import Config
from abackup.backup.project import Container


def perform_restore(config: Config, project_name: str, containers: List[Container], log: logging.Logger):
    success = True
    for container in containers:
        log.info(container.name)
        if not container.restore:
            log.info("skipping {}, no restore settings defined".format(container.name))
            continue
        backup_path = config.ensure_backup_path(project_name, container.name)
        skip_restore = False
        for command in container.restore.pre_commands:
            if not command.run(log):
                log.error("failed running pre command, skipping container: {}".format(container.name))
                skip_restore = True
                success = False
                break

        if not skip_restore:
            for command in container.build_database_restore_commands(backup_path):
                if not command.run(log):
                    log.error("failed running database restore for {}".format(command.name))
                    success = False
            for command in container.build_directory_restore_commands(backup_path):
                if not command.run(log):
                    log.error("failed running directory restore for {}".format(command.directory))
                    success = False
            for command in container.restore.post_commands:
                if not command.run(log):
                    log.error("failed running post command")
                    success = False
                    break

    return success