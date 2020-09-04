import datetime
import logging
import os

from inspect import currentframe, getframeinfo
from typing import List

from abackup import fs, healthchecks as hc, mdadm, notifications, zfs
from abackup.data import Config, Driver, Pool
from abackup.fs import PoolState, PoolStatus


def gather_pool_status(driver: Driver, pool: Pool, log: logging.Logger):
    if driver.name == "mdadm":
        return mdadm.pool_status(pool.name, pool.path, log)
    elif driver.name == "zfs":
        return zfs.pool_status(pool.name, pool.path, log)
    else:
        log.error("Invalid driver: {}".format(driver.name))
        return PoolStatus(pool.name, pool.path, fs.PoolState.ERROR, [], 1, 1)


def notify_or_log(notifier: notifications.SlackNotifier, status: PoolStatus, notify_mode: notifications.Mode,
                  log: logging.Logger):
    log.debug("notify_or_log({}, {})".format(status.pool, notify_mode.name))
    if not notifier:
        log.debug(
            "notify_or_log({}, {}): skipping notify (because no notifier was supplied), state:{}".format(
                status.pool, notify_mode.name, status.state.name))
    elif notify_mode == notifications.Mode.ALWAYS or (
            notify_mode == notifications.Mode.AUTO and status.state != PoolState.HEALTHY):
        log.info("Sending notifications for {}:{} {}".format(status.pool, status.state.name, notify_mode.name))
        if status.state == PoolState.HEALTHY:
            severity = notifications.Severity.GOOD
            title = "{} is HEALTHY".format(status.pool)
        elif status.state == PoolState.DEGRADED:
            severity = notifications.Severity.ERROR
            title = "{} is DEGRADED".format(status.pool)
        else:
            severity = notifications.Severity.CRITICAL
            title = "{} is DOWN".format(
                status.pool) if status.state == PoolState.DOWN else "Error getting status of {}".format(status.pool)
        fields = {"Path": status.path,
                  "Utilization": "{:.2%}".format(status.utilization),
                  "Drive Status": "\n".join([str(ds) for ds in status.drive_status])}
        frame_info = getframeinfo(currentframe())
        response = notifier.notify(title, severity, fields=fields, file_name=os.path.basename(frame_info.filename),
                                   line_number=frame_info.lineno, time=datetime.datetime.now().timestamp())
        if response.is_error():
            log.error("Error during notify: code: {} message: {}".format(response.code, response.message))
        else:
            log.debug(
                "notify_or_log({}, {}): notify successful: code: {} message: {}".format(status.pool, notify_mode.name,
                                                                                        response.code,
                                                                                        response.message))
    else:
        log.debug(
            "notify_or_log({}, {}): skipping notify, state:{}".format(status.pool, notify_mode.name, status.state.name))


def perform_check(config: Config, drivers: List[Driver], notify: str, log: logging.Logger, pool_name: str = None,
                  do_healthchecks: bool = True):
    notify_mode = notifications.Mode(notify)
    for d in drivers:
        log.info("Driver: {}".format(d.name))
        for p in d.pools:
            if not pool_name or p.name == pool_name:
                log.info("Pool: {}".format(p.name))

                if do_healthchecks and p.healthchecks:
                    hc.perform_healthcheck_start(config.default_healthcheck, p.healthchecks, p.name, config.notifier,
                                                 notify_mode, log)

                status = gather_pool_status(d, p, log)
                log.info(status)
                notify_or_log(config.notifier, status, notify_mode, log)

                if do_healthchecks and p.healthchecks:
                    hc.perform_healthcheck(config.default_healthcheck, p.healthchecks, p.name, config.notifier,
                                           notify_mode, log, is_fail=status.state != PoolState.HEALTHY,
                                           message="Pool is not healthy: {}".format(
                                               status) if status.state != PoolState.HEALTHY else None)
            else:
                log.debug("Skipping Pool: {}".format(p.name))
