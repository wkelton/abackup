import logging
import os

from typing import Dict

from abackup.appcron import AppCronTab
from abackup.data import Driver


def perform_update_cron(drivers: Dict[str, Driver], abdata_options: str, cron: AppCronTab,
    log: logging.Logger):
    do_write_cron = True
    for driver_name, driver in drivers.items():
        log.info(driver_name)
        for pool in driver.pools:
            log.info(pool.name)
            if not pool.auto_check:
                log.info("skipping {}, no auto_check settings defined".format(pool.name))
                continue
            healthchecks_option = '--healthchecks' if pool.healthchecks else ''
            for auto_check in pool.auto_check:
                command = "abdata {} check --driver {} --pool {} --notify {} {}".format(
                    abdata_options, driver.name, pool.name, auto_check.notify.value, healthchecks_option)
                comment = "{} @ {}".format(pool.name,
                    auto_check.frequency if auto_check.frequency else "default")
                log.debug("command: {}, comment: {}".format(command, comment))
                job = cron.job(command, comment, frequency=auto_check.frequency if auto_check.frequency else '0 0 * * *',
                    project=driver_name)
                if not job.is_valid():
                    log.error("job not valid! {}".format(comment))
                    do_write_cron = False

    if do_write_cron:
        cron.write()
        return True
    else:
        return False
