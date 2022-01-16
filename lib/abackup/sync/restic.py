import datetime
import logging

from typing import Any, Dict, List

from abackup import healthchecks as hc, notifications
from abackup.restic import BackupResult, CheckResult, ForgetResult, PruneResult, ResticResult, ResticWrapper
from abackup.sync import AutoSync, Config, DataDir, ResticBackupCommand, ResticCheckCommand, ResticCommand, ResticForgetCommand, ResticGlobalOptions, ResticPruneCommand, ResticRepository, syncinfo


class ResticInfo(syncinfo.SyncInfo):
    def __init__(self, name: str, sync_type: str, repo_name: str, path: str, timestamp: datetime.datetime = None,
                 duration: datetime.timedelta = None, transfer_info: Dict[str, Any] = None):
        if not timestamp:
            timestamp = datetime.datetime.now()
        if not duration:
            duration = datetime.datetime.now() - timestamp
        super().__init__(name, sync_type, timestamp, duration, location_info={'repo_name': repo_name, 'path': path},
                         transfer_info=transfer_info if transfer_info else {})

    def __str__(self):
        pass

    def update_with_result(self, result: ResticResult, info: Dict[str, Any] = None):
        self.duration = datetime.datetime.now() - self.timestamp
        if not result.succeeded:
            self.transfer_info[result.command] = "FAILED"
            self.transfer_info[result.command + "_info"] = {
                'stderr': result.stderr, 'stdout': result.stdout}
            return False
        else:
            self.transfer_info[result.command] = "SUCCEEDED"
            if info:
                self.transfer_info[result.command + "_info"] = info
            return True

    def update_with_backup_result(self, result: BackupResult):
        if not result.succeeded:
            return self.update_with_result(result)
        return self.update_with_result(result, {
            'snapshot_id': result.snapshot_id,
            'total_bytes_processed': result.total_bytes_processed,
            'total_files_processed': result.total_files_processed,
            'files_new': result.files_new,
            'files_changed': result.files_changed,
            'files_unmodified': result.files_unmodified,
            'dirs_new': result.dirs_new,
            'dirs_changed': result.dirs_changed,
            'dirs_unmodified': result.dirs_unmodified,
            'data_blobs': result.data_blobs,
            'tree_blobs': result.tree_blobs,
            'data_added': result.data_added
        })

    def update_with_check_result(self, result: CheckResult):
        if not result.succeeded:
            return self.update_with_result(result)
        return self.update_with_result(result)

    def update_with_forget_result(self, result: ForgetResult):
        if not result.succeeded:
            return self.update_with_result(result)
        return self.update_with_result(result, {
            'marked_for_removal': result.remove_entries,
        })

    def update_with_prune_result(self, result: PruneResult):
        if not result.succeeded:
            return self.update_with_result(result)
        return self.update_with_result(result, {
            'to_repack': result.to_repack,
            'this_removes': result.this_removes,
            'to_delete': result.to_delete,
            'total_prune': result.total_prune,
            'remaining': result.remaining,
            'unused_size_after_prune': result.unused_size_after_prune
        })

    def flatten_transfer_info(self, pretty: bool = True):
        m = {}
        for k, v in self.transfer_info.items():
            if k.endswith('_info'):
                m = {**m, **self._flatten_info(v, pretty)}
            else:
                if pretty:
                    m[k.replace('_', ' ').title()] = v
                else:
                    m[k] = v
        return m


def do_restic(repo_name: str, repo: ResticRepository, global_options: ResticGlobalOptions, commands: List[ResticCommand],
              data_name: str, path: str, log: logging.Logger, sync_type: str = 'manual'):
    log.debug("do_restic({}, {}, #commands:{}, {}, {}, {})".format(repo_name, global_options.global_options, len(commands),
                                                                   data_name, path, sync_type))
    if len(commands) == 0:
        log.info("no restic commands to run")
    restic_wrapper = ResticWrapper(repo.password_provider, repo.backend)
    repo.backend.disable_status_updates()

    sync_info = ResticInfo(repo_name, sync_type, repo_name, path)

    for command in commands:
        log.info("Running restic {}...".format(command.command))
        result = None
        if isinstance(command, ResticBackupCommand):
            command.enable_json_output()
            command.add_tag_option(data_name)
            result = command.run(restic_wrapper, log,
                                 global_options.global_options, args=[path])
            sync_info.update_with_backup_result(result)
        elif isinstance(command, ResticCheckCommand):
            result = command.run(restic_wrapper, log,
                                 global_options.global_options)
            sync_info.update_with_check_result(result)
        elif isinstance(command, ResticForgetCommand):
            command.enable_json_output()
            command.append_tag_option('abackup', data_name)
            result = command.run(restic_wrapper, log,
                                 global_options.global_options)
            sync_info.update_with_forget_result(result)
        elif isinstance(command, ResticPruneCommand):
            result = command.run(restic_wrapper, log,
                                 global_options.global_options)
            sync_info.update_with_prune_result(result)
        else:
            result = command.run(restic_wrapper, log,
                                 global_options.global_options)
            sync_info.update_with_result(result)

        if not result.succeeded:
            log.error("restic {} failed!".format(command.command))
            return None
        log.info("restic {} succeeded.".format(command.command))

    return sync_info


def do_auto_restic(config: Config, data_name: str, data_dir: DataDir, auto_sync: AutoSync, notify: str,
                   log: logging.Logger, sync_type: str = 'manual', do_healthchecks: bool = True):
    notify_mode = notifications.Mode(notify)

    repo_name = auto_sync.driver.settings.repo_name
    repo = config.restic_repositories[repo_name]

    sync_info = None

    if do_healthchecks and auto_sync.healthchecks:
        hc.perform_healthcheck_start(config.default_healthcheck, auto_sync.healthchecks, repo_name,
                                     config.notifier, notify_mode, log)

    log.info("running restic with {} repo on {}".format(
        repo_name, data_dir.path))

    ret = do_restic(repo_name, repo, data_dir.restic_options.mask(
        auto_sync.driver.settings.global_options), auto_sync.driver.commands, data_name, data_dir.path, log, sync_type)

    error_message = None
    if not ret:
        error_message = "Failed syncing with {}!".format(
            auto_sync.driver.settings.repo_name)
        syncinfo.handle_failed_sync(config.notifier, data_name,
                                    repo_name, False, error_message, notify_mode, log)
    else:
        syncinfo.handle_sync_results(config.notifier, data_name,
                                     repo_name, False, ret, notify_mode, log)
        sync_info = ret

    if do_healthchecks and auto_sync.healthchecks:
        hc.perform_healthcheck(config.default_healthcheck, auto_sync.healthchecks, repo_name,
                               config.notifier, notify_mode, log, is_fail=not sync_info, message=error_message)

    return sync_info
