import datetime
import logging
import os
import json

from inspect import currentframe, getframeinfo
from typing import Any, Dict, List

from abackup import notifications
from abackup.sync import Config


class SyncInfo:
    def __init__(
        self,
        name: str,
        sync_type: str,
        timestamp: datetime.datetime,
        duration: datetime.timedelta,
        location_info: Dict[str, Any],
        transfer_info: Dict[str, Any],
    ):
        self.name = name
        self.sync_type = sync_type
        self.timestamp = timestamp
        self.duration = duration

        self.location_info = location_info
        self.transfer_info = transfer_info

    def __str__(self):
        return "Sync {}: {} at {} for {}, location: {}, transfer: {}".format(
            self.name, self.sync_type, self.timestamp, self.duration, self.location_info, self.transfer_info
        )

    def _flatten_info(self, info: Dict[str, Any], pretty: bool):
        m = {}
        for k, v in info.items():
            if pretty:
                m[k.replace("_", " ").title()] = v
        return m

    def flatten_location_info(self, pretty: bool = True):
        return self._flatten_info(self.location_info, pretty)

    def flatten_transfer_info(self, pretty: bool = True):
        return self._flatten_info(self.transfer_info, pretty)


def flatten_sync_info(sync_info: SyncInfo, pretty: bool = True):
    m = {
        "name": sync_info.name,
        "sync_type": sync_info.sync_type,
        "timestamp": sync_info.timestamp.replace(microsecond=0).isoformat(),
        "duration": str(sync_info.duration),
    }
    if pretty:
        m = {
            "Name": sync_info.name,
            "Sync Type": sync_info.sync_type,
            "Timestamp": sync_info.timestamp.replace(microsecond=0).isoformat(),
            "Duration": str(sync_info.duration),
        }
    m = {**m, **sync_info.flatten_location_info(pretty)}
    m = {**m, **sync_info.flatten_transfer_info(pretty)}
    return m


def jsonify_sync_info(sync_info: SyncInfo):
    return {
        "name": sync_info.name,
        "sync_type": sync_info.sync_type,
        "timestamp": sync_info.timestamp.replace(microsecond=0).isoformat(),
        "duration": sync_info.duration.total_seconds(),
        "location_info": sync_info.location_info,
        "transfer_info": sync_info.transfer_info,
    }


def unjsonify_sync_info(json_dict: Dict[str, Any]):
    def _get_field(key: str, default=None):
        return json_dict[key] if key in json_dict else default

    return SyncInfo(
        _get_field("name"),
        _get_field("sync_type"),
        datetime.datetime.strptime(_get_field("timestamp", ""), "%Y-%m-%dT%H:%M:%S"),
        datetime.timedelta(seconds=_get_field("duration", 0)),
        _get_field("location_info"),
        _get_field("transfer_info"),
    )


def serialize_sync_infos(sync_infos: List[SyncInfo]):
    return json.dumps({sync_info.name: jsonify_sync_info(sync_info) for sync_info in sync_infos}, indent=4)


def deserialize_sync_infos(json_string: str):
    return [unjsonify_sync_info(json_rep) for _, json_rep in json.loads(json_string).items()]


class SyncInfosSerializer:
    def __init__(self, config: Config, data_name: str):
        self.json_file_path = os.path.join(config.log_root, "{}-latest.json".format(data_name))

    def serialize(self, sync_infos: List[SyncInfo]):
        try:
            previous_sync_infos = self.deserialize()
            if not previous_sync_infos:
                previous_sync_infos = []
            sync_infos_dict = {sync_info.name: sync_info for sync_info in previous_sync_infos}
            for sync_info in sync_infos:
                sync_infos_dict[sync_info.name] = sync_info
            with open(self.json_file_path, "w") as json_file:
                print(serialize_sync_infos(sync_infos_dict.values()), file=json_file)
            return True
        except TypeError:
            return False
        except ValueError:
            return False

    def deserialize(self):
        try:
            if not os.path.exists(self.json_file_path):
                return False
            with open(self.json_file_path, "r") as json_file:
                return deserialize_sync_infos(json_file.read())
        except TypeError:
            return False
        except ValueError:
            return False


def write_sync_infos(sync_infos: List[SyncInfo], config: Config, data_name: str):
    serializer = SyncInfosSerializer(config, data_name)
    return serializer.serialize(sync_infos)


def read_sync_infos(config: Config, data_name: str):
    serializer = SyncInfosSerializer(config, data_name)
    sync_infos = serializer.deserialize()
    return sync_infos if sync_infos else []


# Helpers


def handle_sync_results(
    notifier: notifications.SlackNotifier,
    data_name: str,
    remote_name: str,
    pull: bool,
    sync_info: SyncInfo,
    notify_mode: notifications.Mode,
    log: logging.Logger,
):
    log.debug("handle_sync_results({}, {}, {}, {})".format(data_name, remote_name, notify_mode.name, pull))
    if not notifier:
        log.debug(
            "handle_sync_results({}, {}, {}, {}): skipping notify (because no notifier was supplied)".format(
                data_name, remote_name, notify_mode.name, pull
            )
        )
    elif notify_mode == notifications.Mode.ALWAYS:
        severity = notifications.Severity.GOOD
        title = "Sync Successful"
        fields = flatten_sync_info(sync_info, pretty=True)
        frame_info = getframeinfo(currentframe())
        response = notifier.notify(
            title,
            severity,
            fields=fields,
            file_name=os.path.basename(frame_info.filename),
            line_number=frame_info.lineno,
            time=datetime.datetime.now().timestamp(),
        )
        if response.is_error():
            log.error("Error during notify: code: {} message: {}".format(response.code, response.message))
        else:
            log.debug(
                "handle_sync_results({}, {}, {}, {}): notify successful: code: {} message: {}".format(
                    data_name, remote_name, notify_mode.name, pull, response.code, response.message
                )
            )
    else:
        log.debug(
            "handle_sync_results({}, {}, {}, {}): skipping notify".format(
                data_name, remote_name, notify_mode.name, pull
            )
        )


def handle_failed_sync(
    notifier: notifications.SlackNotifier,
    data_name: str,
    remote_name: str,
    pull: bool,
    error_message: str,
    notify_mode: notifications.Mode,
    log: logging.Logger,
):
    log.debug("handle_failed_sync({}, {}, {}, {})".format(data_name, remote_name, notify_mode.name, pull))
    if not notifier:
        log.debug(
            "handle_failed_sync({}, {}, {}, {}): skipping notify (because no notifier was supplied)".format(
                data_name, remote_name, notify_mode.name, pull
            )
        )
    elif notify_mode != notifications.Mode.NEVER:
        severity = notifications.Severity.CRITICAL
        title = "Sync FAILED"
        fields = {"Data": data_name, "Remote": remote_name, "Error": error_message}
        frame_info = getframeinfo(currentframe())
        response = notifier.notify(
            title,
            severity,
            fields=fields,
            file_name=os.path.basename(frame_info.filename),
            line_number=frame_info.lineno,
            time=datetime.datetime.now().timestamp(),
        )
        if response.is_error():
            log.error("Error during notify: code: {} message: {}".format(response.code, response.message))
        else:
            log.debug(
                "handle_failed_sync({}, {}, {}, {}): notify successful: code: {} message: {}".format(
                    data_name, remote_name, notify_mode.name, pull, response.code, response.message
                )
            )
    else:
        log.debug(
            "handle_failed_sync({}, {}, {}, {}): skipping notify".format(data_name, remote_name, notify_mode.name, pull)
        )
