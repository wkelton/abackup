import logging
import os

from typing import List

from abackup.appcron import AppCronTab
from abackup.backup.project import Container, ProjectConfig


def perform_update_cron(project_name: str, project_config: ProjectConfig, containers: List[Container],
    abackup_options: str, cron: AppCronTab, log: logging.Logger):
    do_write_cron = True
    for container in containers:
        log.info(container.name)
        if not container.backup or not container.backup.auto_backups:
            log.info("skipping {}, no auto_backup settings defined".format(container.name))
            continue
        healthchecks_option = '--healthchecks' if container.backup.healthchecks else ''
        for auto_backup in container.backup.auto_backups:
            command = "abackup {} --project-config {} backup --container {} --notify {} {}".format(
                abackup_options, os.path.abspath(project_config.config_path), container.name,
                auto_backup.notify.value, healthchecks_option)
            comment = "{}".format(container.name)
            log.debug("command: {}, comment: {}".format(command, comment))
            job = cron.job(command, comment, frequency=auto_backup.frequency if auto_backup.frequency else '0 0 * * *',
                project=project_name)
            if not job.is_valid():
                log.error("job not valid! {}".format(comment))
                do_write_cron = False

    if do_write_cron:
        cron.write()
        return True
    else:
        return False