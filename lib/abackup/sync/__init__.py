import os
import yaml

from typing import Any, Dict, List

from abackup import config, notifications


class SyncOptions:
    def __init__(self, delete: bool = False, max_delete: int = None):
        self.delete = delete
        self.max_delete = max_delete

    def mask(self, options):
        if not options:
            return SyncOptions(self.delete, self.max_delete)
        else:
            return SyncOptions(options.delete if options.delete else self.delete, 
                options.max_delete if options.max_delete else self.max_delete)

    @staticmethod
    def default():
        return SyncOptions(False, 10)


class AutoSync:
    def __init__(self, remote_name: str, notify: str = None, frequency: str = None,
                 options: Dict[str, Any] = None):
        self.remote_name = remote_name
        self.frequency = frequency
        self.notify = notifications.Mode(notify) if notify else notifications.Mode.AUTO
        self.options = SyncOptions(**options) if options else None


class DataDir:
    def __init__(self, path: str, group: str = None, permissions: str = None,
                 auto_sync: List[Dict[str, Any]] = None, options: Dict[str, Any] = None):
        self.path = path
        self.auto_sync = [ AutoSync(**s) for s in auto_sync ] if auto_sync else [ ]
        self.options = SyncOptions(**options) if options else SyncOptions.default()


class Remote:
    def __init__(self, host: str, port: int = None, user: str = None, ssh_key: str = None):
        self.host = host
        self.port = port
        self.user = user
        self.ssh_key = ssh_key

    def __str__(self):
        return "Remote: {} {}".format(self.connection_string(),
                                      " ".join(self.ssh_options()))

    def ssh_options(self):
        options = [ ]
        if self.port:
            options.extend([ '-p', str(self.port) ])
        if self.ssh_key:
            options.extend([ '-i', self.ssh_key ])
        return options

    def connection_string(self):
        return "{}@{}".format(self.user, self.host) if self.user else self.host


class Config(config.BaseConfig):
    def __init__(self, path: str, no_log: bool, debug: bool):
        super().__init__("absync", os.path.dirname(path), no_log, debug)

        self.owned_data = {}
        self.stored_data = {}
        self.remotes = {}

        if path and os.path.isfile(path):
            with open(path, 'r') as stream:
                self._raw = yaml.safe_load(stream)
            if 'log_root' in self._raw:
                self.log_root = self._raw['log_root']
            if 'owned_data' in self._raw:
                self.owned_data = { name: DataDir(**value) for name, value in self._raw['owned_data'].items() }
            if 'stored_data' in self._raw:
                self.stored_data = { name: DataDir(**value) for name, value in self._raw['stored_data'].items() }
            if 'remotes' in self._raw:
                self.remotes = { name: Remote(**value) for name, value in self._raw['remotes'].items() }
