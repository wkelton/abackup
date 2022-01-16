import logging
import re

from typing import Dict

from abackup.sync import AutoSync, Config, DataDir, Remote, ResticDriver, RsyncDriver, syncinfo
from abackup.sync.restic import do_auto_restic
from abackup.sync.rsync import RsyncOptions, do_auto_rsync, do_rsync


def perform_rsync(config: Config, data_name: str, origin_destination: str, pull: bool, log: logging.Logger,
                  delete: bool = None, max_delete: int = None, port: int = None):
    if pull:
        data = config.stored_data
    else:
        data = config.owned_data
    if data_name not in data:
        log.critical(
            "{} data directory not present in config!".format(data_name))
        return False
    log.info("\tonly for {}".format(data_name))

    def _parse_remote(remote_string: str, remotes: Dict[str, Remote], log_: logging.Logger):
        remote_ = None
        remote_path = remote_string
        remote_regex = re.compile(r"^([a-zA-Z0-9.]+):(.*)")
        remote_match = remote_regex.match(remote_string)
        if remote_match:
            remote_name = remote_match.group(1)
            remote_path = remote_match.group(2)
            if remote_name in remotes:
                remote_ = remotes[remote_name]
                log_.info(
                    "found remote in config: {} -> {}".format(remote_name, remote_.host))
            else:
                remote_ = Remote(remote_name)
        return remote_, remote_path

    data_dir = data[data_name]
    if pull:
        remote, origin = _parse_remote(origin_destination, config.remotes, log)
        destination = data_dir.path
    else:
        origin = data_dir.path
        remote, destination = _parse_remote(
            origin_destination, config.remotes, log)

    if remote:
        if port:
            remote.port = port
        log.info("syncing {} with {}".format("{}:{}".format(remote.host, origin) if pull else origin,
                                             "{}:{}".format(remote.host, destination) if not pull else destination))
        log.debug(remote)
    else:
        log.info("syncing {} with {}".format(origin, destination))

    ret = do_rsync(origin, destination, data_dir.rsync_options.mask(RsyncOptions(delete, max_delete)), log, remote,
                   pull=pull)

    if ret:
        syncinfo.write_sync_infos([ret], config, data_name)
        return True
    return False


def perform_auto_sync(config: Config, absync_options: str, notify: str, log: logging.Logger,
                      only_data_name: str = None, only_sync_name: str = None, sync_type: str = 'manual',
                      do_healthchecks: bool = True):
    if only_data_name:
        if only_data_name not in config.owned_data and only_data_name not in config.stored_data:
            log.critical(
                "{} data directory not present in config!".format(only_data_name))
            return False
        log.info("\tonly for {}".format(only_data_name))

    def process_auto_sync(auto_sync: AutoSync, is_stored_data: bool):
        if isinstance(auto_sync.driver, RsyncDriver):
            log.debug("Performing auto rsync on {}".format(
                auto_sync.sync_name))
            return do_auto_rsync(config, name, data_dir, auto_sync, absync_options, notify, log,
                                 sync_type, pull=is_stored_data, do_healthchecks=do_healthchecks)
        elif isinstance(auto_sync.driver, ResticDriver):
            if is_stored_data:
                log.warning("restic driver not supported in stored_data: {}".format(
                    auto_sync.sync_name))
            else:
                log.debug("Performing auto restic on {}".format(
                    auto_sync.sync_name))
                return do_auto_restic(config, name, data_dir, auto_sync, notify, log, sync_type, do_healthchecks)
        else:
            log.error("unrecognized sync driver: {} for: {}".format(auto_sync.driver.__class__.__name__,
                                                                    auto_sync.sync_name))
        return None

    def process_data_dir(name: str, data_dir: DataDir, is_stored_data: bool):
        if only_data_name and name != only_data_name:
            return True
        sync_infos = []
        sync_succeeded = True
        for auto_sync in data_dir.auto_sync:
            if only_sync_name and auto_sync.sync_name != only_sync_name:
                continue
            sync_info = process_auto_sync(auto_sync, is_stored_data)
            if sync_info is None:
                sync_succeeded = False
            else:
                sync_infos.append(sync_info)
        if len(sync_infos) > 0:
            syncinfo.write_sync_infos(sync_infos, config, name)
        return sync_succeeded

    sync_succeeded = True

    for name, data_dir in config.owned_data.items():
        sync_succeeded = process_data_dir(
            name, data_dir, is_stored_data=False) and sync_succeeded

    for name, data_dir in config.stored_data.items():
        sync_succeeded = process_data_dir(
            name, data_dir, is_stored_data=True) and sync_succeeded

    return sync_succeeded
