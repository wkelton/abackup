import logging
import os
import subprocess

from enum import Enum, auto
from typing import List


def find_files(path: str, prefix: str, extension: str = ''):
    return [filename for filename in next(os.walk(path))[2]
            if filename.startswith(prefix) and filename.endswith(extension)]


def find_youngest_file(path: str, prefix: str, extension: str = ''):
    filenames = find_files(path, prefix, extension)
    return max(filenames, key=lambda fn: os.stat(os.path.join(path, fn)).st_mtime) if filenames else None


def find_oldest_file(path: str, prefix: str, extension: str = ''):
    filenames = find_files(path, prefix, extension)
    return min(filenames, key=lambda fn: os.stat(os.path.join(path, fn)).st_mtime) if filenames else None


def to_human_readable(num: float, prefix: str = '', suffix: str = 'B'):
    start = False
    for unit in ['', 'Ki', 'Mi', 'Gi', 'Ti', 'Pi', 'Ei', 'Zi']:
        start = start or prefix == unit
        if not start:
            continue
        if abs(num) < 1024.0:
            return "%3.2f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.3f %s%s" % (num, 'Yi', suffix)


class FSStats:
    def __init__(self, total_size: float, available: float):
        self.total_size = total_size
        self.available = available

    def __str__(self):
        return "FSStats: size:{}, avail:{}".format(self.total_size, self.available)

    @property
    def used(self):
        return self.total_size - self.available


def get_fs_stats(path: str):
    stats = os.statvfs(path)
    total_size = stats.f_blocks * stats.f_bsize
    available = stats.f_bavail * stats.f_bsize
    return FSStats(total_size, available)


def get_total_size(path: str, log: logging.Logger = None):
    command_list = ['du', '-s', path]
    if log:
        log.debug("Getting dir size: {}".format(" ".join(command_list)))
    run_out = subprocess.run(command_list, stdout=subprocess.PIPE, stderr=subprocess.PIPE, universal_newlines=True)
    if run_out.returncode == 0:
        size = int(run_out.stdout.split()[0])
        if log:
            log.debug("Got {} kilobytes for {}".format(size, path))
        return size
    else:
        if log:
            log.warning("Failed to get dir size! {}".format(path))
            log.warning(run_out.stderr)
        return False


class PoolState(Enum):
    HEALTHY = auto()
    DEGRADED = auto()
    DOWN = auto()
    ERROR = auto()


class DriveStatus:
    def __init__(self, drive: str, state: PoolState):
        self.drive = drive
        self.state = state

    def __str__(self):
        return "{}   {}".format(self.drive, self.state.name)


class PoolStatus:
    def __init__(self, pool: str, path: str, state: PoolState, drive_status: List[DriveStatus],
                 total_size: float, used: float, message: str = None):
        self.pool = pool
        self.path = path
        self.state = state
        self.message = message
        self.drive_status = drive_status
        self.total_size = total_size
        self.used = used

    def __str__(self):
        return "PoolStatus: {} ({}) -{}- {:.2%} used\n\t{}{}".format(self.pool, self.path, self.state.name,
                                                                     self.utilization,
                                                                     "\n\t".join([str(ds) for ds in self.drive_status]),
                                                                     "\n\t" + self.message if self.message else "")

    @property
    def available(self):
        return self.total_size - self.used

    @property
    def utilization(self):
        return self.used / self.total_size
