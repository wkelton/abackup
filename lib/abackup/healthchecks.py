import datetime
import logging
import os
from inspect import Traceback, currentframe, getframeinfo

import requests

from abackup import notifications


def notify_or_log_error(notifier: notifications.SlackNotifier, name: str, notify_mode: notifications.Mode,
                        log: logging.Logger, frame_info: Traceback, response_code: int, response_message: str):
    log.debug("notify_or_log_error({}, {})".format(name, notify_mode.name))

    error_message = "Failed to perform healthcheck for {} - code: {} message: {}".format(name, response_code,
                                                                                         response_message)
    do_notify = notify_mode == notifications.Mode.ALWAYS or notify_mode == notifications.Mode.AUTO

    if not notifier:
        log.debug("notify_or_log_error({}, {}): skipping notify (because no notifier was supplied)".format(
            name, notify_mode.name))
        log.error(error_message)
    elif do_notify:
        log.info("Sending healthcheck error notification for {} {}".format(name, notify_mode.name))
        severity = notifications.Severity.CRITICAL
        title = "Failed to Perform Healthcheck for {} ".format(name)
        fields = {'Failure': error_message}
        response = notifier.notify(title, severity, fields=fields, file_name=os.path.basename(frame_info.filename),
                                   line_number=frame_info.lineno, time=datetime.datetime.now().timestamp())
        if response.is_error():
            log.error("Error during notify: code: {} message: {}".format(response.code, response.message))
        else:
            log.debug(
                "notify_or_log_error({}, {}): notify successful: code: {} message: {}".format(name, notify_mode.name,
                                                                                              response.code,
                                                                                              response.message))


class HealthcheckResult:
    def __init__(self, is_fail: bool, code: int, message: str = None):
        self.is_fail = is_fail
        self.code = code
        self.message = message

    @classmethod
    def fail(cls, message: str, code: int = -1):
        return cls(is_fail=True, code=code, message=message)

    @classmethod
    def success(cls, code: int, message: str = ''):
        return cls(is_fail=False, code=code, message=message)

    def is_error(self):
        return self.is_fail or self.code != 200


class Healthcheck:
    def __init__(self, base_url: str = None, uuid: str = None, do_include_messages: bool = None,
                 do_notify_start: bool = None):
        self.base_url = base_url
        self.uuid = uuid
        self.do_include_messages = do_include_messages
        self.do_notify_start = do_notify_start

    def override(self, other):
        return Healthcheck(base_url=other.base_url if other.base_url else self.base_url,
                           uuid=other.uuid if other.uuid else self.uuid,
                           do_include_messages=other.do_include_messages if other.do_include_messages is not None else self.do_include_messages,
                           do_notify_start=other.do_notify_start if other.do_notify_start is not None else self.do_notify_start)

    def is_valid(self):
        return self.base_url and self.uuid

    def _request_get(self, api: str, timeout: int = 5):
        if not self.is_valid:
            return HealthcheckResult.fail('No base_url or uuid configured!')
        try:
            response = requests.get("{}/{}{}".format(self.base_url, self.uuid, api), timeout=timeout)
            return HealthcheckResult.success(response.status_code, response.text)
        except requests.exceptions.RequestException:
            return HealthcheckResult.fail('Exception occurred while requesting GET!')

    def _request_post(self, api: str, data: str = None, timeout: int = 5):
        if not self.is_valid:
            return HealthcheckResult.fail('No base_url or uuid configured!')
        try:
            response = requests.post("{}/{}{}".format(self.base_url, self.uuid, api), data=data, timeout=timeout)
            return HealthcheckResult.success(response.status_code, response.text)
        except requests.exceptions.RequestException:
            return HealthcheckResult.fail('Exception occurred while requesting POST!')

    def notify_start(self):
        if not self.do_notify_start:
            return HealthcheckResult.success(code=0)
        return self._request_get('/start')

    def notify_success(self):
        return self._request_get('')

    def notify_failure(self, message: str = None):
        return self._request_post('/fail', data=message)


def perform_healthcheck_start(default_check: Healthcheck, check: Healthcheck, name: str,
                              notifier: notifications.SlackNotifier, notify_mode: notifications.Mode,
                              log: logging.Logger):
    result = default_check.override(check).notify_start()
    if result.is_error():
        notify_or_log_error(notifier, name, notify_mode, log, frame_info=getframeinfo(currentframe()),
                            response_code=result.code, response_message=result.message)


def perform_healthcheck(default_check: Healthcheck, check: Healthcheck, name: str,
                        notifier: notifications.SlackNotifier, notify_mode: notifications.Mode, log: logging.Logger,
                        is_fail: bool = False,
                        message: str = None):
    if is_fail:
        result = default_check.override(check).notify_failure(message)
    else:
        result = default_check.override(check).notify_success()
    if result.is_error():
        notify_or_log_error(notifier, name, notify_mode, log, frame_info=getframeinfo(currentframe()),
                            response_code=result.code, response_message=result.message)
