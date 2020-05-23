import datetime
import os
import json

from typing import Any, Dict, List

from abackup.sync import Config


class SyncInfo:
    def __init__(self, sync_type: str, timestamp: datetime.datetime, duration: datetime.timedelta, origin: str,
        destination: str, sync_count: int, sync_deleted: int, sync_bytes: int, transferred_files: List[str],
        deleted_files: List[str], remote_host: str = None, pull: bool = False):
        self.sync_type = sync_type
        self.timestamp = timestamp
        self.origin = origin
        self.duration = duration
        self.destination = destination
        self.remote_host = remote_host
        self.pull = pull
        self.sync_count = sync_count
        self.sync_deleted = sync_deleted
        self.sync_bytes = sync_bytes
        self.transferred_files = transferred_files
        self.deleted_files = deleted_files

    def __str__(self):
        return "Sync: {} at {} for {} from {} to {} --- transfered {} files with {} bytes".format(self.sync_type,
            self.timestamp, self.duration,
            "{}:{}".format(self.remote_host, self.origin) if self.remote_host and self.pull else self.origin,
            "{}:{}".format(self.remote_host, self.destination) if self.remote_host and not self.pull else self.destination,
            self.sync_count, self.sync_bytes)


def jsonify_sync_info(sync_info: SyncInfo):
    return {'sync_type': sync_info.sync_type,
            'timestamp': sync_info.timestamp.replace(microsecond=0).isoformat(),
            'duration': sync_info.duration.total_seconds(),
            'origin': sync_info.origin,
            'destination': sync_info.destination,
            'sync_count': sync_info.sync_count,
            'sync_deleted': sync_info.sync_deleted,
            'sync_bytes': sync_info.sync_bytes,
            'transferred_files': sync_info.transferred_files,
            'deleted_files': sync_info.deleted_files,
            'remote_host': sync_info.remote_host,
            'pull': sync_info.pull
            }


def unjsonify_sync_info(json_dict: Dict[str, Any]):
    def _get_field(key: str, default=None):
        return json_dict[key] if key in json_dict else default
    return SyncInfo(_get_field('sync_type'),
                    datetime.datetime.strptime(_get_field('timestamp', ''), '%Y-%m-%dT%H:%M:%S'),
                    datetime.timedelta(seconds=_get_field('duration', 0)),
                    _get_field('origin'),
                    _get_field('destination'),
                    _get_field('sync_count'),
                    _get_field('sync_deleted'),
                    _get_field('sync_bytes'),
                    _get_field('transferred_files'),
                    _get_field('deleted_files'),
                    _get_field('remote_host'),
                    _get_field('pull')
                    )


def serialize_sync_infos(sync_infos: List[SyncInfo]):
    return json.dumps({sync_info.remote_host: jsonify_sync_info(sync_info) for sync_info in sync_infos}, indent=4)


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
            sync_infos_dict = {sync_info.remote_host: sync_info for sync_info in previous_sync_infos}
            for sync_info in sync_infos:
                sync_infos_dict[sync_info.remote_host] = sync_info
            with open(self.json_file_path, 'w') as json_file:
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
            with open(self.json_file_path, 'r') as json_file:
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
