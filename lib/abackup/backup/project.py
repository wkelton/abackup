import os
import yaml

from typing import Any, Dict, List

from abackup import notifications
from abackup.docker import Command, DockerCommand, DirectoryBackupCommand, DirectoryRestoreCommand, DBBackupCommand, DBRestoreCommand


def build_commands(command_input: List[Any], container_name: str):
    commands = [ ]
    if command_input:
        for raw_command in command_input:
            if isinstance(raw_command, dict):
                command_type = raw_command['command_type']
                if command_type == 'docker':
                    docker_options = raw_command['docker_options'] if 'docker_options' in raw_command else []
                    commands.append(DockerCommand(container_name=container_name, docker_options=docker_options,
                        **{ k:v for k,v in raw_command.items() if k != 'command_type' and k != 'docker_options' }))
                else:
                    commands.append(Command(**{ k:v for k,v in raw_command.items() if k != 'command_type' }))
            else:
                commands.append(Command(raw_command))
    return commands


class RestoreSettings:
    def __init__(self, container_name: str, pre_commands: List[Dict[str, Any]] = None,
        post_commands: List[Dict[str, Any]] = None, docker_options: List[str] = None,
        custom: List[Dict[str, Any]] = None):
        self.docker_options = docker_options if docker_options else [ ]
        self.pre_commands = build_commands(pre_commands, container_name)
        self.post_commands = build_commands(post_commands, container_name)
        self.custom_commands = build_commands(custom, container_name)

    def __str__(self):
        return "pre:{} post:{} options:{} custom:{}".format(len(self.pre_commands), len(self.post_commands), len(self.docker_options), len(self.custom_commands))


class AutoBackup:
    def __init__(self, frequency: str = None, notify: str = None):
        self.frequency = frequency
        self.notify = notifications.Mode(notify) if notify else notifications.Mode.AUTO

    def __str__(self):
        return "frequency:{} notify:{}".format(self.frequency, self.notify.value)


class BackupSettings:
    def __init__(self, container_name: str, pre_commands: List[Dict[str, Any]] = None,
        post_commands: List[Dict[str, Any]] = None, version_count: int = 1,
        auto_backup: List[Dict[str, str]] = None, docker_options: List[str] = None):
        self.docker_options = docker_options if docker_options else []
        self.pre_commands = build_commands(pre_commands, container_name)
        self.post_commands = build_commands(post_commands, container_name)
        self.version_count = version_count
        self.auto_backups = [AutoBackup(**ab) for ab in auto_backup] if auto_backup else []

    def __str__(self):
        return "pre:{} post:{} versions:{} auto_backups:{} options:{}".format(
            len(self.pre_commands), len(self.post_commands), self.version_count, len(self.auto_backups),
            len(self.docker_options))


class DatabaseInfo:
    def __init__(self, name: str, driver: str, password: str):
        self.name = name
        self.driver = driver
        self.password = password

    def __str__(self):
        return "{} := {}".format(self.name, self.driver)


class Container:
    def __init__(self, name: str, databases: List[Dict[str, str]] = None, directories: List[str] = None,
                 backup: Dict[str, Any] = None, restore: Dict[str, Any] = None):
        self.name = name
        self.databases = [ DatabaseInfo(**db) for db in databases ] if databases else [ ]
        self.directories = directories if directories else [ ]
        self.backup = BackupSettings(name, **backup) if isinstance(backup, dict) else None
        self.restore = RestoreSettings(name, **restore) if isinstance(restore, dict) else None

    def __str__(self):
        return "{}:\ndatabases: {}\ndirectories: {}\nbackup: {}\nrestore: {}".format(
            self.name, ", ".join([str(d) for d in self.databases]), ", ".join(self.directories),
            self.backup, self.restore)

    def build_directory_backup_commands(self, backup_path: str):
        if self.backup:
            return [ DirectoryBackupCommand(cdir, backup_path, self.name, self.backup.docker_options)
                for cdir in self.directories ]
        return [ ]

    def build_directory_restore_commands(self, backup_path: str):
        if self.restore:
            return [ DirectoryRestoreCommand(cdir, backup_path, self.name, self.restore.docker_options)
                for cdir in self.directories ]
        return [ ]

    def build_database_backup_commands(self, backup_path: str):
        if self.backup:
            return [ DBBackupCommand(dbinfo.name, dbinfo.password, backup_path, self.name,
                self.backup.docker_options) for dbinfo in self.databases ]
        return [ ]

    def build_database_restore_commands(self, backup_path: str):
        if self.backup:
            return [ DBRestoreCommand(dbinfo.name, dbinfo.password, backup_path, self.name,
                self.restore.docker_options) for dbinfo in self.databases ]
        return [ ]


class ProjectConfig:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.containers = []
        self._config = {}
        with open(config_path, 'r') as stream:
            self._raw = yaml.safe_load(stream)
        for container_name, guts in self._raw['containers'].items():
            container = Container(container_name, **guts)
            self._config[container_name] = container
            self.containers.append(container)

    def __str__(self):
        s = "Config:\ncontainers:"
        for container in self.containers:
            s += "\n{}".format(str(container))
        return s

    def container(self, name: str):
        return self._config[name]


def get_all_backup_files_for_container(backup_path: str, container: Container):
    files = []
    for command in container.build_directory_backup_commands(backup_path) + \
        container.build_database_backup_commands(backup_path):
        if os.path.isfile(command.backup_file):
            files.append(command.backup_file)
        for i in range(1, container.backup.version_count):
            file_path = "{}.{}".format(command.backup_file, i)
            if os.path.isfile(file_path):
                files.append(file_path)
    return files
