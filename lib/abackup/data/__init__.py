import os
import yaml

from typing import Any, Dict, List

from abackup import config, healthchecks as hc, notifications


class AutoCheck:
    def __init__(self, frequency: str = None, notify: str = None):
        self.frequency = frequency
        self.notify = notifications.Mode(notify) if notify else notifications.Mode.AUTO


class Pool:
    def __init__(self, name: str, path: str, auto_check: List[Dict[str, str]] = None,
                 healthchecks: Dict[str, str] = None):
        self.name = name
        self.path = path
        self.auto_check = [AutoCheck(**check) for check in auto_check] if auto_check else []
        self.healthchecks = hc.Healthcheck(**healthchecks) if healthchecks else None


class Driver:
    def __init__(self, name: str, pools: List[Dict[str, Any]]):
        self.name = name
        self.pools = [Pool(**pool) for pool in pools]


class Config(config.BaseConfig):
    def __init__(self, path: str, no_log: bool, debug: bool):
        super().__init__("abdata", os.path.dirname(path), no_log, debug)

        if path and os.path.isfile(path):
            with open(path, 'r') as stream:
                self._raw = yaml.safe_load(stream)
            if 'drivers' in self._raw:
                self.drivers = {name: Driver(name, **value) for name, value in self._raw['drivers'].items()}
