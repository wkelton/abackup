import logging
from typing import Dict

from abackup.appcron import AppCronTab
from abackup.sync import DataDir


def add_command_to_cron(name: str, data_dir: DataDir, absync_options: str, cron: AppCronTab, log: logging.Logger):
    do_write_cron = True
    log.info(name)
    if not data_dir.auto_sync:
        log.info("skipping {}, no auto_sync settings defined".format(name))
    else:
        for auto_sync in data_dir.auto_sync:
            healthchecks_option = "--healthchecks" if auto_sync.healthchecks else ""
            command = "absync {} auto --sync-type auto --data-name {} --sync-name {} --notify {} {}".format(
                absync_options, name, auto_sync.sync_name, auto_sync.notify.value, healthchecks_option
            )
            comment = "{}".format(auto_sync.sync_name)
            log.debug("command: {}, comment: {}".format(command, comment))
            job = cron.job(
                command, comment, frequency=auto_sync.frequency if auto_sync.frequency else "0 0 * * *", project=name
            )
            if not job.is_valid():
                log.error("job not valid! {}".format(comment))
                do_write_cron = False
    return do_write_cron


def perform_update_cron(
    owned_data: Dict[str, DataDir],
    stored_data: Dict[str, DataDir],
    absync_options: str,
    cron: AppCronTab,
    log: logging.Logger,
):
    do_write_cron = True
    log.info("updating cron for owned_data")
    for name, data_dir in owned_data.items():
        do_write_cron = add_command_to_cron(name, data_dir, absync_options, cron, log) and do_write_cron
    log.info("updating cron for stored_data")
    for name, data_dir in stored_data.items():
        do_write_cron = add_command_to_cron(name, data_dir, absync_options, cron, log) and do_write_cron
    if do_write_cron:
        cron.write()
        return True
    else:
        return False
