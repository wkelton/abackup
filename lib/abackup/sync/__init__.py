import logging
import os
import platform
import shlex
import yaml

from typing import Any, Dict, List

from abackup import config, healthchecks as hc, notifications
from abackup.restic import PasswordProvider, RepoConnection, RestBackend, ResticWrapper


# Rsync


class RsyncOptions:
    def __init__(
        self,
        delete: bool = None,
        max_delete: int = None,
        copy_unsafe_links: bool = None,
        inplace: bool = None,
        no_compress: bool = None,
        no_whole_file: bool = None,
        backup: bool = None,
        custom_str: str = None,
    ):
        self.delete = delete
        self.max_delete = max_delete
        self.copy_unsafe_links = copy_unsafe_links
        self.inplace = inplace
        self.no_compress = no_compress
        self.no_whole_file = no_whole_file
        self.backup = backup
        self.custom_str = custom_str

    def __str__(self):
        return " ".join(self.options_list())

    def options_list(self):
        options = []
        if self.delete:
            options.append("--delete")
            if self.max_delete:
                options.append("--max-delete={}".format(self.max_delete))
        if self.copy_unsafe_links:
            options.append("--copy-unsafe-links")
        if self.inplace:
            options.append("--inplace")
        if not self.no_compress:
            options.append("-z")
        if self.no_whole_file:
            options.append("--no-whole-file")
        if self.backup:
            options.append("--backup")
        if self.custom_str:
            options.extend(shlex.split(self.custom_str))
        return options

    def mask(self, settings):
        if not settings:
            return RsyncOptions(
                self.delete,
                self.max_delete,
                self.copy_unsafe_links,
                self.inplace,
                self.no_whole_file,
                self.backup,
                self.custom_str,
                self.paths,
            )
        else:
            return RsyncOptions(
                settings.delete if settings.delete is not None else self.delete,
                settings.max_delete if settings.max_delete is not None else self.max_delete,
                settings.copy_unsafe_links if settings.copy_unsafe_links is not None else self.copy_unsafe_links,
                settings.inplace if settings.inplace is not None else self.inplace,
                settings.no_compress if settings.no_compress is not None else self.no_compress,
                settings.no_whole_file if settings.no_whole_file is not None else self.no_whole_file,
                settings.backup if settings.backup is not None else self.backup,
                settings.custom_str if settings.custom_str is not None else self.custom_str,
            )

    @staticmethod
    def default():
        return RsyncOptions(False, 10, False, False)


class RsyncSettings:
    def __init__(self, remote_name: str, options: Dict[str, Any] = None, paths: List[str] = None):
        self.remote_name = remote_name
        self.options = RsyncOptions(**options) if options else RsyncOptions()
        self.paths = paths


class RsyncDriver:
    def __init__(self, settings: Dict[str, Any]):
        self.settings = RsyncSettings(**settings)


class Remote:
    def __init__(self, host: str, port: int = None, user: str = None, ssh_key: str = None):
        self.host = host
        self.port = port
        self.user = user
        self.ssh_key = ssh_key

    def __str__(self):
        return "Remote: {} {}".format(self.connection_string(), " ".join(self.ssh_options()))

    def ssh_options(self):
        options = []
        if self.port:
            options.extend(["-p", str(self.port)])
        if self.ssh_key:
            options.extend(["-i", self.ssh_key])
        return options

    def connection_string(self):
        return "{}@{}".format(self.user, self.host) if self.user else self.host


# Restic


class ResticGlobalOptions:
    def __init__(self, global_options: Dict[str, Any] = None):
        self.global_options = global_options

    def mask(self, settings):
        if not settings or not settings.global_options:
            return ResticGlobalOptions(self.global_options)
        else:
            if not self.global_options:
                return ResticGlobalOptions(global_options=settings.global_options)
            else:
                return ResticGlobalOptions(global_options={**self.global_options, **settings.global_options})

    @staticmethod
    def default():
        return ResticGlobalOptions({"verbose": True})


class ResticSettings:
    def __init__(self, repo_name: str, global_options: Dict[str, Any] = None):
        self.repo_name = repo_name
        self.global_options = ResticGlobalOptions(global_options)


class ResticCommand:
    def __init__(
        self,
        command: str,
        options: Dict[str, Any] = None,
        default_options: Dict[str, Any] = None,
        skip_defaults: bool = None,
    ):
        self.command = command
        self.options = options
        if not skip_defaults and default_options is not None:
            if options:
                self.options = {**default_options, **options}
            else:
                self.options = default_options

    @staticmethod
    def construct(command: str, options: Dict[str, Any] = None, skip_defaults: bool = None):
        if command == "backup":
            return ResticBackupCommand(options, skip_defaults)
        if command == "check":
            return ResticCheckCommand(options, skip_defaults)
        if command == "forget":
            return ResticForgetCommand(options, skip_defaults)
        if command == "prune":
            return ResticPruneCommand(options, skip_defaults)
        return ResticCommand("")

    def enable_json_output(self):
        if not self.options:
            self.options = {}
        self.options["json"] = True

    def add_tag_option(self, tag: str):
        if not self.options:
            self.options = {}
        if "tags" not in self.options:
            self.options["tags"] = [tag]
        else:
            self.options["tags"].append(tag)

    def append_tag_option(self, existing_tag: str, new_tag: str):
        if self.options and "tags" in self.options:
            if existing_tag in self.options["tags"]:
                self.options["tags"].remove(existing_tag)
                self.add_tag_option("{},{}".format(existing_tag, new_tag))
                return
        self.add_tag_option(new_tag)

    def run(
        self,
        restic_wrapper: ResticWrapper,
        log: logging.Logger,
        global_options: Dict[str, Any] = None,
        args: List[str] = None,
    ):
        return restic_wrapper.run_command(
            self.command, log, global_options=global_options, options=self.options, args=args
        )


class ResticBackupCommand(ResticCommand):
    def __init__(self, options: Dict[str, Any] = None, skip_defaults: bool = None):
        defaults = {}
        super().__init__("backup", options, defaults, skip_defaults)
        if not skip_defaults:
            default_tags = ["abackup"]
            self.options["tags"] = self.options["tags"] + default_tags if "tags" in self.options else default_tags

    def run(
        self,
        restic_wrapper: ResticWrapper,
        log: logging.Logger,
        global_options: Dict[str, Any] = None,
        args: List[str] = None,
    ):
        return restic_wrapper.backup(log, global_options=global_options, options=self.options, args=args)


class ResticCheckCommand(ResticCommand):
    def __init__(self, options: Dict[str, Any] = None, skip_defaults: bool = None):
        defaults = {}
        super().__init__("check", options, defaults, skip_defaults)

    def run(
        self,
        restic_wrapper: ResticWrapper,
        log: logging.Logger,
        global_options: Dict[str, Any] = None,
        args: List[str] = None,
    ):
        return restic_wrapper.check(log, global_options=global_options, options=self.options)


class ResticForgetCommand(ResticCommand):
    def __init__(self, options: Dict[str, Any] = None, skip_defaults: bool = None):
        defaults = {"host": platform.node(), "group-by": "paths"}
        super().__init__("forget", options, defaults, skip_defaults)
        if not skip_defaults:
            default_tags = ["abackup"]
            self.options["tags"] = self.options["tags"] + default_tags if "tags" in self.options else default_tags

    def run(
        self,
        restic_wrapper: ResticWrapper,
        log: logging.Logger,
        global_options: Dict[str, Any] = None,
        args: List[str] = None,
    ):
        return restic_wrapper.forget(log, global_options=global_options, options=self.options, args=args)


class ResticPruneCommand(ResticCommand):
    def __init__(self, options: Dict[str, Any] = None, skip_defaults: bool = None):
        defaults = {}
        super().__init__("prune", options, defaults, skip_defaults)

    def run(
        self,
        restic_wrapper: ResticWrapper,
        log: logging.Logger,
        global_options: Dict[str, Any] = None,
        args: List[str] = None,
    ):
        return restic_wrapper.prune(log, global_options=global_options, options=self.options)


class ResticDriver:
    def __init__(self, settings: Dict[str, Any], commands: List[Dict[str, Any]]):
        self.settings = ResticSettings(**settings)
        self.commands = [ResticCommand.construct(**command) for command in commands]


class ResticRepository:
    def __init__(self, password_provider: Dict[str, str], backend: Dict[str, str]):
        self.password_provider = PasswordProvider(**password_provider)
        self.backend = None
        if backend["type"] == "rest":
            backend["settings"]["password_provider"] = PasswordProvider(**backend["settings"]["password_provider"])
            self.backend = RestBackend(
                path=backend["path"], env=backend["env"] if "env" in backend else None, **backend["settings"]
            )
        else:
            self.backend = RepoConnection(path=backend["path"], env=backend["env"] if "env" in backend else None)


# Main


class AutoSync:
    def __init__(
        self,
        sync_name: str,
        driver: Dict[str, Any],
        pre_commands: List[Any] = None,
        notify: str = None,
        frequency: str = None,
        healthchecks: Dict[str, str] = None,
    ):
        self.sync_name = sync_name
        self.frequency = frequency
        self.driver = None
        if driver["type"] == "rsync":
            self.driver = RsyncDriver(driver["settings"])
        if driver["type"] == "restic":
            self.driver = ResticDriver(driver["settings"], driver["commands"])
        self.pre_commands = pre_commands
        self.notify = notifications.Mode(notify) if notify else notifications.Mode.AUTO
        self.healthchecks = hc.Healthcheck(**healthchecks) if healthchecks else None


class DataDir:
    def __init__(
        self,
        path: str,
        group: str = None,
        permissions: str = None,
        auto_sync: List[Dict[str, Any]] = None,
        driver_common_settings: List[Dict[str, Any]] = None,
    ):
        self.path = path
        self.auto_sync = [AutoSync(**s) for s in auto_sync] if auto_sync else []
        self.rsync_options = RsyncOptions.default()
        self.restic_options = ResticGlobalOptions.default()
        if driver_common_settings:
            for entry in driver_common_settings:
                if entry["type"] == "rsync":
                    self.rsync_options = self.rsync_options.mask(RsyncOptions(**entry["settings"]))
                if entry["type"] == "restic":
                    self.restic_options = self.restic_options.mask(ResticGlobalOptions(**entry["settings"]))


class Config(config.BaseConfig):
    def __init__(self, path: str, no_log: bool, debug: bool):
        super().__init__("absync", os.path.dirname(path), no_log, debug)

        self.owned_data = {}
        self.stored_data = {}
        self.remotes = {}
        self.restic_repositories = {}

        if path and os.path.isfile(path):
            with open(path, "r") as stream:
                self._raw = yaml.safe_load(stream)
            if "log_root" in self._raw:
                self.log_root = self._raw["log_root"]
            if "owned_data" in self._raw:
                self.owned_data = {name: DataDir(**value) for name, value in self._raw["owned_data"].items()}
            if "stored_data" in self._raw:
                self.stored_data = {name: DataDir(**value) for name, value in self._raw["stored_data"].items()}
            if "remotes" in self._raw:
                self.remotes = {name: Remote(**value) for name, value in self._raw["remotes"].items()}
            if "restic_repositories" in self._raw:
                self.restic_repositories = {
                    name: ResticRepository(**value) for name, value in self._raw["restic_repositories"].items()
                }
