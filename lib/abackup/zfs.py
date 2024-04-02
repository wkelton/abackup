import logging
import re
import subprocess

from abackup.fs import DriveStatus, FSStats, PoolState, PoolStatus


def pool_stats(name: str, path: str, log: logging.Logger = None):
    run_out = subprocess.run(
        ["zfs", "list", "-Hpo", "available,used", name], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    if run_out.returncode != 0 and not run_out.stdout:
        if log:
            log.error("Failed to run 'zfs list'!")
            log.error(run_out.stderr)
        return None
    zlist_output = run_out.stdout.split("\n")

    if log:
        log.debug("zfs.pool_stats({}, {}):".format(name, path))
        log.debug(zlist_output)
    
    available, used = zlist_output[0].split()
    return FSStats(float(available) + float(used), float(available))
    

def pool_status(name: str, path: str, log: logging.Logger = None):
    run_out = subprocess.run(
        ["zpool", "status", name], stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True
    )
    if run_out.returncode != 0 and not run_out.stdout:
        if log:
            log.error("Failed to run 'zpool status'!")
            log.error(run_out.stderr)
        return None
    zpool_output = run_out.stdout.split("\n")

    def zfs_state_to_pool_state(z_state: str):
        if z_state == "ONLINE":
            return PoolState.HEALTHY
        elif z_state == "DEGRADED":
            return PoolState.DEGRADED
        return PoolState.DOWN

    if log:
        log.debug("zfs.pool_status({}, {}): zpool_output:".format(name, path))
        log.debug(zpool_output)

    state = None
    drive_status = []
    errors = None
    messages = []
    state_line_regex = re.compile(r"^\s*state:\s+(.*)\s*$")
    config_line_regex = re.compile(r"^\s+(\S+)\s+(\S+)\s+\d+\s+\d+\s+\d+\s*(.*)$")
    errors_line_regex = re.compile(r"^errors:\s+(.*)\s*$")
    for line in zpool_output:
        state_match = state_line_regex.match(line)
        config_match = config_line_regex.match(line)
        errors_match = errors_line_regex.match(line)
        if state_match:
            state = state_match.group(1)
            if log:
                log.debug("zfs.pool_status({}, {}): state: {}".format(name, path, state))
            state = zfs_state_to_pool_state(state)
        if errors_match:
            errors = errors_match.group(1)
            if log:
                log.debug("zfs.pool_status({}, {}): errors: {}".format(name, path, errors))
        if config_match:
            match_name = config_match.group(1)
            zfs_state = config_match.group(2)
            if log:
                log.debug("zfs.pool_status({}, {}): drive name/state: {}/{}".format(name, path, match_name, zfs_state))
            if config_match.group(3):
                messages.append(config_match.group(3))
            if match_name != name and not match_name.startswith("mirror-") and not match_name.startswith("raidz"):
                drive_status.append(DriveStatus(match_name, zfs_state_to_pool_state(zfs_state)))

    if not state:
        if log:
            log.error("Failed to get zfs state for pool: {}".format(name))
        state = PoolState.ERROR

    if errors:
        if not errors.startswith("No known data errors"):
            messages.append(errors)

    stats = pool_stats(name, path, log)

    return PoolStatus(
        name, path, state, drive_status, stats.total_size, stats.used, "\n".join(messages) if messages else None
    )
