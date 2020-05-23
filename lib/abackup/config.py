import logging
import os
import sys
import yaml

from logging.handlers import RotatingFileHandler

from abackup import healthchecks as hc, notifications


def setup_logger(name: str, log_path: str = None, level=logging.INFO):
    formatter = logging.Formatter(fmt='%(asctime)s %(levelname)-8s %(message)s',
                                  datefmt='%Y-%m-%d %H:%M:%S')
    if log_path:
        handler = RotatingFileHandler(log_path, maxBytes=10485760, backupCount=5)
    else:
        handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    log = logging.getLogger(name)
    log.setLevel(level)
    log.addHandler(handler)
    return log


class BaseConfig:
    def __init__(self, app: str, config_dir: str, no_log: bool, debug: bool, log_dir_name: str = None):
        self.log_root = os.path.join(config_dir, 'logs', log_dir_name if log_dir_name else app)
        self.notifier = None
        self.default_healthcheck = hc.Healthcheck('https://hc-ping.com', do_include_messages=True, do_notify_start=True)
        config_path = os.path.join(config_dir, 'conf.yml')
        if os.path.isfile(config_path):
            with open(config_path, 'r') as stream:
                self._raw = yaml.safe_load(stream)
            if 'log_root' in self._raw:
                self.log_root = self._raw['log_root']
            if 'notifications' in self._raw:
                if 'slack' in self._raw['notifications']:  # TODO extend to others
                    self.notifier = notifications.SlackNotifier(**self._raw['notifications']['slack'])
            if 'healthchecks' in self._raw:
                if 'default' in self._raw['healthchecks']:
                    self.default_healthcheck = hc.Healthcheck(**{
                        **{'base_url': self.default_healthcheck.url,
                           'do_include_messages': self.default_healthcheck.do_include_messages,
                           'do_notify_start': self.default_healthcheck.do_notify_start},
                        **self._raw['healthchecks']['default']
                    })
        self.log_path = os.path.join(self.log_root, app + ".log") if not no_log else None
        if self.log_path:
            self._ensure_log_dir()
        self.log = setup_logger(app, self.log_path, logging.DEBUG if debug else logging.INFO)

    def _ensure_log_dir(self):
        if not os.path.isdir(self.log_root):
            os.makedirs(self.log_root)
        return self.log_root
