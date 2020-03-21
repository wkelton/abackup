import logging
import mdstat

from abackup.fs import DriveStatus, PoolState, PoolStatus, get_fs_stats


def pool_status(name: str, path: str, log: logging.Logger = None):
    if log:
        log.debug("pool_status({}, {})".format(name, path))
    stats = get_fs_stats(path)
    if log:
        log.debug("pool_status({}, {}): get_fs_stats({}):".format(name, path, path))
        log.debug(stats)

    mdstat_data = mdstat.parse()
    if not name in mdstat_data['devices']:
        if log:
            log.error("Pool {} not found in mdstat!".format(name))
        return PoolStatus(name, path, PoolState.ERROR, [ ], stats.total_size, stats.used)
    pool_data = mdstat_data['devices'][name]
    if log:
        log.debug("pool_status({}, {}): pool_data:".format(name, path))
        log.debug(pool_data)

    is_active = pool_data['active']
    raid_disks = pool_data['status']['raid_disks']
    non_degraded_disks = pool_data['status']['non_degraded_disks']
    disks = pool_data['disks']
    drive_status = [DriveStatus(disk_name, PoolState.DOWN if raw['faulty'] else PoolState.HEALTHY) \
        for disk_name, raw in disks.items()]

    if log:
        log.debug("pool_status({}, {}): is_active: {}".format(name, path, is_active))
        log.debug("pool_status({}, {}): raid_disks: {}".format(name, path, raid_disks))
        log.debug("pool_status({}, {}): non_degraded_disks: {}".format(name, path, non_degraded_disks))
        log.debug("pool_status({}, {}): drive_status: {}".format(name, path, 
            "\t".join([str(ds) for ds in drive_status])))

    pool_state = PoolState.HEALTHY
    if not is_active:
        pool_state = PoolState.DOWN
    elif non_degraded_disks < raid_disks:
        pool_state = PoolState.DEGRADED
    else:
        for ds in drive_status:
            if ds.state == PoolState.DOWN:
                pool_state = PoolState.DEGRADED
    if log:
        log.info("pool_status({}, {}): pool_state: {}".format(name, path, name))

    return PoolStatus(name, path, pool_state, drive_status, stats.total_size, stats.used)