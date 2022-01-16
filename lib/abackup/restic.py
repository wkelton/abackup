from ast import Pass
import logging
import json
import re

from subprocess import CompletedProcess
from typing import Any, Dict, List

from abackup import Command


class PasswordProvider:
    def __init__(self, type: str, arg: str):
        self.type = type
        self.arg = arg

    def to_restic_option(self, log: logging.Logger):
        if self.type == "file":
            return '--password-file "{}"'.format(self.arg)
        if self.type == "command":
            return '--password-command "{}"'.format(self.arg)
        log.critical(
            "PasswordProvider::to_restic_option(): unknown password provider type: {}".format(self.type))
        raise TypeError("unknown password provider type: {}".format(self.type))

    def to_string(self, log: logging.Logger):
        if self.type == "file":
            with open(self.arg, "r") as pass_file:
                return pass_file.readline().rstrip()
        if self.type == "command":
            command_result = Command(
                self.arg, universal_newlines=True).run_with_result(log)
            if command_result.returncode != 0:
                log.critical("PasswordProvider::to_string(): failed to run password command! {}".format(
                    command_result.stderr.decode()))
            command_result.check_returncode()
            return command_result.stdout.decode().rstrip()
        log.critical(
            "PasswordProvider::to_string(): unknown password provider type: {}".format(self.type))
        raise TypeError("unknown password provider type: {}".format(self.type))


class RepoConnection:
    def __init__(self, env: Dict[str, str] = None, path: str = None):
        self.env = env if env else {}
        self._repo_string = path

    def repo_string(self, log: logging.Logger):
        return self._repo_string

    def disable_status_updates(self):
        self.env['RESTIC_PROGRESS_FPS'] = 0.0000000001


class RestBackend(RepoConnection):
    def __init__(self, user: str, password_provider: PasswordProvider, host: str, path: str, port: str = None,
                 env: Dict[str, str] = None):
        self.user = user
        self.password_provider = password_provider
        self.host = host
        self.path = path
        self.port = port
        super().__init__(env)

    def repo_string(self, log: logging.Logger):
        if self.port:
            return "rest:https://{}:{}@{}:{}/{}".format(self.user, self.password_provider.to_string(log), self.host, self.port, self.path)
        return "rest:https://{}:{}@{}/{}".format(self.user, self.password_provider.to_string(log), self.host, self.path)


class ResticResult:
    def __init__(self, command: str, command_succeeded: bool, stderr: str = None, stdout: str = None):
        self.command = command
        self.command_succeeded = command_succeeded
        self.stderr = stderr
        self.stdout = stdout

    @classmethod
    def from_output(cls, command: str, completed_process: CompletedProcess, log: logging.Logger):
        stdout = completed_process.stdout
        if not isinstance(stdout, str):
            stdout = stdout.decode()
        stderr = completed_process.stderr
        if not isinstance(stderr, str):
            stdout = stdout.decode()
        return cls(command, completed_process.returncode == 0, stderr, stdout)

    @property
    def succeeded(self):
        return self.command_succeeded


class BackupResult(ResticResult):
    def __init__(self, files_new: int, files_changed: int, files_unmodified: int, dirs_new: int, dirs_changed: int,
                 dirs_unmodified: int, data_blobs: int, tree_blobs: int, data_added: int, total_files_processed: int,
                 total_bytes_processed: int, total_duration: float, snapshot_id: str, command_succeeded: bool):
        self.files_new = files_new
        self.files_changed = files_changed
        self.files_unmodified = files_unmodified
        self.dirs_new = dirs_new
        self.dirs_changed = dirs_changed
        self.dirs_unmodified = dirs_unmodified
        self.data_blobs = data_blobs
        self.tree_blobs = tree_blobs
        self.data_added = data_added
        self.total_files_processed = total_files_processed
        self.total_bytes_processed = total_bytes_processed
        self.total_duration = total_duration
        self.snapshot_id = snapshot_id
        super().__init__('backup', command_succeeded)

    @classmethod
    def from_output(cls, completed_process: CompletedProcess, log: logging.Logger):
        if completed_process.returncode != 0:
            log.debug("BackupResult::from_output({})".format(completed_process))
            return ResticResult.from_output('backup', completed_process, log)

        lines = completed_process.stdout.split('\n')
        for line in lines:
            if not line.startswith('{'):
                continue
            json_dict = json.loads(line)

            def _get_field(key: str, default=None):
                return json_dict[key] if key in json_dict else default
            if _get_field("message_type") == "summary":
                return cls(_get_field("files_new"),
                           _get_field("files_changed"),
                           _get_field("files_unmodified"),
                           _get_field("dirs_new"),
                           _get_field("dirs_changed"),
                           _get_field("dirs_unmodified"),
                           _get_field("data_blobs"),
                           _get_field("tree_blobs"),
                           _get_field("data_added"),
                           _get_field("total_files_processed"),
                           _get_field("total_bytes_processed"),
                           _get_field("total_duration"),
                           _get_field("snapshot_id"),
                           True
                           )
        return None


class CheckResult(ResticResult):
    def __init__(self, command_succeeded: bool, stderr: str = None, stdout: str = None):
        super().__init__('check', command_succeeded, stderr, stdout)

    @classmethod
    def from_output(cls, completed_process: CompletedProcess, log: logging.Logger):
        return cls(completed_process.returncode == 0, completed_process.stderr, completed_process.stdout)


class ForgetResult(ResticResult):
    def __init__(self, remove_entries: List[Dict[str, Any]], command_succeeded: bool):
        self.remove_entries = remove_entries
        super().__init__('forget', command_succeeded)

    @classmethod
    def from_output(cls, completed_process: CompletedProcess, log: logging.Logger):
        if completed_process.returncode != 0:
            return ResticResult.from_output('forget', completed_process, log)

        remove_entries = []

        # there should only be one line
        lines = completed_process.stdout.split('\n')
        for line in lines:
            if not line.startswith('{'):
                continue
            json_list = json.loads(line)
            if len(json_list) > 1:
                raise RuntimeError(
                    "too many elements in json output of forget command")
            json_dict = json_list[0]
            remove_list = json_dict['remove']
            for remove in remove_list:
                def _get_field(key: str, default=None):
                    return remove[key] if key in remove else default
                remove_entries.append({'snapshot_id': _get_field('short_id'),
                                       'time': _get_field('time'),
                                       'paths': _get_field('paths'),
                                       'tags': _get_field('tags'),
                                       'hostname': _get_field('hostname')})

        return cls(remove_entries, True)


class PruneResult(ResticResult):
    def __init__(self, to_repack: str, this_removes: str, to_delete: str, total_prune: str, remaining: str,
                 unused_size_after_prune: str, command_succeeded: bool):
        self.to_repack = to_repack
        self.this_removes = this_removes
        self.to_delete = to_delete
        self.total_prune = total_prune
        self.remaining = remaining
        self.unused_size_after_prune = unused_size_after_prune
        super().__init__('prune', command_succeeded)

    @classmethod
    def from_output(cls, completed_process: CompletedProcess, log: logging.Logger):
        if completed_process.returncode != 0:
            return ResticResult.from_output('prune', completed_process, log)

        to_repack = None
        this_removes = None
        to_delete = None
        total_prune = None
        remaining = None
        unused_size_after_prune = None

        to_repack_regex = re.compile(r"^to repack:?\s+(.*)$")
        this_removes_regex = re.compile(r"^this removes:?\s+(.*)$")
        to_delete_regex = re.compile(r"^to delete:?\s+(.*)$")
        total_prune_regex = re.compile(r"^total prune:?\s+(.*)$")
        remaining_regex = re.compile(r"^remaining:?\s+(.*)$")
        unused_regex = re.compile(r"^unused size after prune:?\s+(.*)$")
        for line in completed_process.stdout.split("\n"):
            to_repack_match = to_repack_regex.match(line)
            this_removes_match = this_removes_regex.match(line)
            to_delete_match = to_delete_regex.match(line)
            total_prune_match = total_prune_regex.match(line)
            remaining_match = remaining_regex.match(line)
            unused_match = unused_regex.match(line)
            if to_repack_match:
                to_repack = to_repack_match.group(1)
            elif this_removes_match:
                this_removes = this_removes_match.group(1)
            elif to_delete_match:
                to_delete = to_delete_match.group(1)
            elif total_prune_match:
                total_prune = total_prune_match.group(1)
            elif remaining_match:
                remaining = remaining_match.group(1)
            elif unused_match:
                unused_size_after_prune = unused_match.group(1)

        return cls(to_repack, this_removes, to_delete, total_prune, remaining, unused_size_after_prune, True)


class ResticWrapper:
    def __init__(self, password_provider: PasswordProvider, connection: RepoConnection):
        self.password_provider = password_provider
        self.connection = connection

    def _run_command(self, command: str, log: logging.Logger, global_options: Dict[str, Any] = None,
                     options: Dict[str, Any] = None, args: List[str] = None, input: str = None, input_path: str = None,
                     output_path: str = None, universal_newlines: bool = None):
        def dict_to_options_string(d: Dict[str, Any]):
            options_string = ""
            if d != None:
                for k, v in d.items():
                    if len(k) == 1:
                        options_string += "-{} ".format(k)
                    elif v is True:
                        options_string += "--{} ".format(k)
                    elif isinstance(v, list):
                        if k == 'tags':
                            options_string += "--{} {} ".format('tag', ','.join(v))
                        else:
                            options_string += "--{} {} ".format(k, ','.join(v))
                    else:
                        options_string += "--{} {} ".format(k, v)
            return options_string
        global_options_string = dict_to_options_string(global_options)
        options_string = dict_to_options_string(options)

        if not args:
            args_string = ""
        else:
            args_string = " ".join(args)

        log.debug("ResticWrapper::_run_command({}, {}, {}, {}, {}, {}, {}):".format(
            command, global_options_string, options_string, args_string, input is None, input_path, output_path))

        command_string = "restic -r {} {} {} {} {} {}".format(self.connection.repo_string(
            log), self.password_provider.to_restic_option(log), global_options_string, command, options_string,
            args_string)

        return Command(command_string, input, input_path, output_path, universal_newlines).run_with_result(log)

    def run_command(self, command: str, log: logging.Logger, global_options: Dict[str, Any] = None,
                    options: Dict[str, Any] = None, args: List[str] = None, input: str = None, input_path: str = None,
                    output_path: str = None, universal_newlines: bool = None):
        return ResticResult.from_output(self.run_command(command, log, global_options, options, args=args, input=input,
                                                         input_path=input_path, output_path=output_path,
                                                         universal_newlines=universal_newlines), log)

    def backup(self, log: logging.Logger, global_options: Dict[str, Any] = None, options: Dict[str, Any] = None,
               args: List[str] = None, input: str = None, input_path: str = None):
        log.debug("ResticWrapper::backup()")
        return BackupResult.from_output(self._run_command('backup', log, global_options, options, args=args, input=input,
                                                          input_path=input_path, universal_newlines=True), log)

    def check(self, log: logging.Logger, global_options: Dict[str, Any] = None, options: Dict[str, Any] = None):
        log.debug("ResticWrapper::check()")
        return CheckResult.from_output(self._run_command('check', log, global_options, options, universal_newlines=True),
                                       log)

    def forget(self, log: logging.Logger, global_options: Dict[str, Any] = None, options: Dict[str, Any] = None,
               args: List[str] = None):
        log.debug("ResticWrapper::forget()")
        return ForgetResult.from_output(self._run_command('forget', log, global_options, options, args=args,
                                                          universal_newlines=True), log)

    def prune(self, log: logging.Logger, global_options: Dict[str, Any] = None, options: Dict[str, Any] = None):
        log.debug("ResticWrapper::prune()")
        return PruneResult.from_output(self._run_command('prune', log, global_options, options, universal_newlines=True),
                                       log)
