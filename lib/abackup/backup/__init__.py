import grp
import os
import yaml

from abackup import config


class Config(config.BaseConfig):
    def __init__(self, path: str, no_log: bool, debug: bool, project_name: str):
        super().__init__("abackup", os.path.dirname(path), no_log, debug, project_name)

        self.backup_root = None
        self.group_owner = None
        self.directory_permissions = None
        self.file_permissions = None

        if path and os.path.isfile(path):
            with open(path, "r") as stream:
                self._raw = yaml.safe_load(stream)
            self.backup_root = self._raw["backup_root"]
            if "permissions" in self._raw:
                if "group" in self._raw["permissions"]:
                    self.group_owner = self._raw["permissions"]["group"]
                if "directories" in self._raw["permissions"]:
                    self.directory_permissions = int(self._raw["permissions"]["directories"], 8)
                if "files" in self._raw["permissions"]:
                    self.file_permissions = int(self._raw["permissions"]["files"], 8)

    def get_backup_path(self, project_name: str, container_name: str):
        return os.path.join(self.backup_root, project_name, container_name)

    def ensure_backup_path(self, project_name: str, container_name: str):
        path = self.get_backup_path(project_name, container_name)
        if not os.path.isdir(path):
            os.makedirs(path)
        if self.group_owner:
            os.chown(os.path.join(self.backup_root, project_name), -1, grp.getgrnam(self.group_owner).gr_gid)
            os.chown(
                os.path.join(self.backup_root, project_name, container_name), -1, grp.getgrnam(self.group_owner).gr_gid
            )
        if self.directory_permissions:
            os.chmod(os.path.join(self.backup_root, project_name), self.directory_permissions)
            os.chmod(os.path.join(self.backup_root, project_name, container_name), self.directory_permissions)
        return path
