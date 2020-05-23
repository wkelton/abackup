import logging
import os

from typing import List, Dict

from abackup.appcron import AppCronTab
from abackup.sync import DataDir


def add_command_to_cron(name: str, datadir: DataDir, absync_options: str, cron: AppCronTab, log: logging.Logger):
    do_write_cron = True
    log.info(name)
    if not datadir.auto_sync:
        log.info("skipping {}, no auto_sync settings defined".format(name))
    else:
        for auto_sync in datadir.auto_sync:
            healthchecks_option = '--healthchecks' if auto_sync.healthchecks else ''
            command = "absync {} auto --sync-type auto --data-name {} --remote-name {} --notify {} {}".format(
                absync_options, name, auto_sync.remote_name, auto_sync.notify.value, healthchecks_option)
            comment = "{}".format(auto_sync.remote_name)
            log.debug("command: {}, comment: {}".format(command, comment))
            job = cron.job(command, comment, frequency=auto_sync.frequency if auto_sync.frequency else '0 0 * * *',
                project=name)
            if not job.is_valid():
                log.error("job not valid! {}".format(comment))
                do_write_cron = False
    return do_write_cron


def perform_update_cron(owned_data: Dict[str, DataDir], stored_data: Dict[str, DataDir], absync_options: str,
    cron: AppCronTab, log: logging.Logger):
    do_write_cron = True
    log.info("updating cron for owned_data")
    for name, datadir in owned_data.items():
        do_write_cron = add_command_to_cron(name, datadir, absync_options, cron, log) and do_write_cron
    log.info("updating cron for stored_data")
    for name, datadir in stored_data.items():
        do_write_cron = add_command_to_cron(name, datadir, absync_options, cron, log) and do_write_cron
    if do_write_cron:
        cron.write()
        return True
    else:
        return False
