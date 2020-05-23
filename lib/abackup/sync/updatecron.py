import logging
import os

from typing import List, Dict

from abackup.appcron import AppCronTab
from abackup.sync import DataDir


def perform_update_cron(owned_data: Dict[str, DataDir], absync_options: str, cron: AppCronTab, log: logging.Logger):
    do_write_cron = True
    for name, datadir in owned_data.items():
        log.info(name)
        if not datadir.auto_sync:
            log.info("skipping {}, no auto_sync settings defined".format(name))
            continue
        for auto_sync in datadir.auto_sync:
            healthchecks_option = '--healthchecks' if auto_sync.healthchecks else ''
            command = "absync {} auto --sync-type auto --local-name {} --remote-name {} --notify {} {}".format(
                absync_options, name, auto_sync.remote_name, auto_sync.notify.value, healthchecks_option)
            comment = "{}".format(auto_sync.remote_name)
            log.debug("command: {}, comment: {}".format(command, comment))
            job = cron.job(command, comment, frequency=auto_sync.frequency if auto_sync.frequency else '0 0 * * *',
                project=name)
            if not job.is_valid():
                log.error("job not valid! {}".format(comment))
                do_write_cron = False

    if do_write_cron:
        cron.write()
        return True
    else:
        return False
