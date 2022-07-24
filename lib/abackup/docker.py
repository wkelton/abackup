import logging
import os
import time
import uuid

from typing import List

from abackup import Command, fs


# TODO: look into replacing all the subprocess calls with the python docker library


class DockerCommand(Command):
    def __init__(self, command_string: str, container_name: str, docker_options: List[str],
                 input: str = None, input_path: str = None, output_path: str = None, in_container: bool = False):
        self.container_name = container_name
        self.docker_options = docker_options
        self.run_command_in_container = in_container
        self.docker_command = "docker exec" if self.run_command_in_container else "docker run"
        super().__init__(command_string, input=input, input_path=input_path, output_path=output_path)

    def friendly_str(self):
        return self.docker_command

    def formatted_options(self):
        return " ".join(self.docker_options)

    def run(self, log: logging.Logger):
        log.debug("Command::run({}, {}, {}, {}, {})".format(self.container_name, self.command_string,
                                                            self.formatted_options(), self.run_command_in_container,
                                                            self.output_path))
        if self.run_command_in_container:
            command = "{} {} {} sh -c '{}'".format(self.docker_command, self.formatted_options(), self.container_name,
                                                   self.command_string)
            log.info("Running command in the {} container: {}".format(self.container_name, self.friendly_str()))
        else:
            command = "{} --volumes-from {} {} busybox sh -c '{}'".format(self.docker_command, self.container_name,
                                                                          self.formatted_options(), self.command_string)
            log.info("Running command in a busybox container: {}".format(self.friendly_str()))
        return self._run(command, log)

    def new_command(self, command_string: str):
        dc = DockerCommand(command_string, self.container_name, self.docker_options, self.input_str,
                           output_path=self.output_path, in_container=self.run_command_in_container)
        return dc


class MySqlBRCommand(DockerCommand):
    def __init__(self, name: str, password: str, mysql_command: str, mysql_options: List[str],
                 backup_path: str, container_name: str, docker_options: List[str], input_path: str = None,
                 output_path: str = None):
        self.mysql_command = mysql_command
        self.name = name
        self.file_prefix = MySqlBRCommand.default_file_prefix(name)
        self.file_extension = MySqlBRCommand.default_file_extension()
        self.backup_file = MySqlBRCommand.default_backup_file(backup_path, name)
        d_opts = docker_options + ['-i']
        super().__init__('{} {} -p"{}" {}'.format(mysql_command, " ".join(mysql_options), password, name),
                         container_name, d_opts, input_path=input_path, output_path=output_path, in_container=True)

    @classmethod
    def default_file_prefix(cls, name):
        return name

    @classmethod
    def default_file_extension(cls):
        return 'sql'

    @classmethod
    def default_backup_file(cls, backup_path, name):
        return os.path.join(backup_path, "{}_{}.{}".format(MySqlBRCommand.default_file_prefix(name),
                                                           time.strftime("%Y%m%d_%H%M%S"),
                                                           MySqlBRCommand.default_file_extension()))

    def friendly_str(self):
        return "{} {}".format(self.mysql_command, self.name)


class MysqlBackupCommand(MySqlBRCommand):
    def __init__(self, name: str, password: str, backup_path: str, container_name: str, docker_options: List[str]):
        super().__init__(name, password, 'mysqldump', ['--single-transaction'], backup_path, container_name,
                         docker_options, output_path=MySqlBRCommand.default_backup_file(backup_path, name))


class MysqlRestoreCommand(MySqlBRCommand):
    def __init__(self, name: str, password: str, backup_path: str, container_name: str, docker_options: List[str],
                 backup_filename: str = None):
        backup_filename = fs.find_youngest_file(backup_path, MySqlBRCommand.default_file_prefix(name),
                                                MySqlBRCommand.default_file_extension()) if not backup_filename else backup_filename
        super().__init__(name, password, 'mysql', [], backup_path, container_name, docker_options,
                         input_path=os.path.join(backup_path, backup_filename))
        self.backup_file = os.path.join(backup_path, backup_filename)


class PostgresBRCommand(DockerCommand):
    def __init__(self, name: str, postgres_command: str, postgres_options: List[str], backup_path: str,
                 container_name: str, docker_options: List[str], user: str = None, password: str = None,
                 input_path: str = None, output_path: str = None):
        self.postgres_command = postgres_command
        self.name = name
        # TODO: support using 'password'
        p_opts = postgres_options + ['-U', user] if user else postgres_options
        self.file_prefix = PostgresBRCommand.default_file_prefix(name)
        self.file_extension = PostgresBRCommand.default_file_extension()
        self.backup_file = PostgresBRCommand.default_backup_file(backup_path, name)
        d_opts = docker_options + ['-i']
        super().__init__('{} {}'.format(postgres_command, " ".join(p_opts)), container_name, d_opts,
                         input_path=input_path, output_path=output_path, in_container=True)

    @classmethod
    def default_file_prefix(cls, name):
        return name

    @classmethod
    def default_file_extension(cls):
        return 'sql'

    @classmethod
    def default_backup_file(cls, backup_path, name):
        return os.path.join(backup_path, "{}_{}.{}".format(PostgresBRCommand.default_file_prefix(name),
                                                           time.strftime("%Y%m%d_%H%M%S"),
                                                           PostgresBRCommand.default_file_extension()))

    def friendly_str(self):
        return "{} {}".format(self.postgres_command, self.name)


class PostgresBackupCommand(PostgresBRCommand):
    def __init__(self, name: str, backup_path: str, container_name: str, docker_options: List[str], user: str = None,
                 password: str = None, dump_all: bool = False):
        postgres_command = 'pg_dump'
        postgres_options = ['-d', name]
        if dump_all:
            postgres_command = 'pg_dumpall'
            postgres_options = []
        super().__init__(name, postgres_command, postgres_options, backup_path, container_name, docker_options, user,
                         password,
                         output_path=PostgresBRCommand.default_backup_file(backup_path, name))


class PostgresRestoreCommand(PostgresBRCommand):
    def __init__(self, name: str, backup_path: str, container_name: str, docker_options: List[str], user: str,
                 password: str, restore_all: bool = False, backup_filename: str = None):
        backup_filename = fs.find_youngest_file(backup_path, PostgresBRCommand.default_file_prefix(name),
                                                PostgresBRCommand.default_file_extension()) if not backup_filename else backup_filename
        super().__init__(name, 'psql', ['-d', 'postgres'] if restore_all else ['-d', name], backup_path,
                         container_name, docker_options, user, password,
                         input_path=os.path.join(backup_path, backup_filename))
        self.backup_file = os.path.join(backup_path, backup_filename)


class TarBackupSettings:
    def __init__(self, is_single_backup: bool, prefix: str = None, force_timestamp: bool = None):
        self.is_single_backup = is_single_backup
        self.prefix = prefix
        self.force_timestamp = force_timestamp

    @property
    def use_identifier_as_perfix(self):
        return self.prefix is None

    @property
    def use_timestamp(self):
        return self.force_timestamp or not self.is_single_backup


class DockerTarBuilder:
    def __init__(self, identifier: str, source_dir_in_container: str, dest_dir_in_container: str, dest_dir_on_host: str, settings: TarBackupSettings, override_file_name: str = None):
        self.identifier = identifier
        self.source_dir_in_container = source_dir_in_container
        self.dest_dir_in_container = dest_dir_in_container
        self.dest_dir_on_host = dest_dir_on_host
        self.settings = settings
        self.override_file_name = override_file_name
        self._file_name = None

    @property
    def prefix(self):
        if self.settings.use_identifier_as_perfix:
            return self.identifier
        return self.settings.prefix

    @property
    def extension(self):
        return 'tar.gz'

    @property
    def file_name(self):
        if self.override_file_name:
            return self.override_file_name
        if not self._file_name:
            if not self.settings.use_timestamp:
                self._file_name = "{}.{}".format(self.prefix, self.extension)
            else:
                self._file_name = "{}_{}.{}".format(self.prefix, time.strftime("%Y%m%d_%H%M%S"), self.extension)
        return self._file_name

    @property
    def container_file_path(self):
        return os.path.join(self.dest_dir_in_container, self.file_name)

    @property
    def host_file_path(self):
        return os.path.join(self.dest_dir_on_host, self.file_name)

    @property
    def command_create_str_in_container(self):
        return "tar -czf {} {}".format(self.container_file_path, self.source_dir_in_container)

    @property
    def command_extract_str_in_container(self):
        return "tar -xzf {}".format(self.container_file_path)


def construct_create_tar_builder(directory: str, container_dir: str, host_tmp_dir: str, settings: TarBackupSettings):
    return DockerTarBuilder(os.path.basename(directory), directory, container_dir, host_tmp_dir, settings)


def construct_extract_tar_builder(tar_name: str, directory: str, container_dir: str, host_tmp_dir: str, settings: TarBackupSettings):
    return DockerTarBuilder(os.path.basename(directory), directory, container_dir, host_tmp_dir, settings, tar_name)


def get_new_host_tmp_dir():
    return os.path.join(os.getcwd(), '.abackup-tmp', str(uuid.uuid4()))


class DirectoryTarCommand(DockerCommand):
    def __init__(self, directory: str, backup_path: str, tar_builder: DockerTarBuilder, tar_command: str, container_name: str, docker_options: List[str]):
        self.directory = directory
        self.backup_path = backup_path
        self.tar_builder = tar_builder

        d_opts = docker_options + ['--rm', '-v', "{}:{}".format(self.tar_builder.dest_dir_on_host, DirectoryTarCommand.default_container_dir())]
        super().__init__(tar_command, container_name, d_opts)

    @classmethod
    def default_container_dir(cls):
        return '/abackup'

    @property
    def backup_file(self):
        return os.path.join(self.backup_path, self.tar_builder.file_name)

    @property
    def file_prefix(self):
        return self.tar_builder.prefix

    @property
    def file_extension(self):
        return self.tar_builder.extension

    def friendly_str(self):
        return "tar {}".format(self.directory)


class DirectoryTarBackupCommand(DirectoryTarCommand):
    def __init__(self, directory: str, backup_path: str, tar_settings: TarBackupSettings, container_name: str, docker_options: List[str]):
        tar_builder = construct_create_tar_builder(directory, DirectoryTarCommand.default_container_dir(), get_new_host_tmp_dir(), tar_settings)
        super().__init__(directory, backup_path, tar_builder, tar_builder.command_create_str_in_container, container_name, docker_options)

    def run(self, log: logging.Logger):
        os.makedirs(self.tar_builder.dest_dir_on_host)
        copy_temp_file_from_host_command = Command("cp {} {}".format(self.tar_builder.host_file_path, self.backup_file))
        delete_temp_file_from_container_command = self.new_command("rm {}".format(self.tar_builder.container_file_path))
        delete_temp_dir_from_host_command = Command("rm -rf {}".format(self.tar_builder.dest_dir_on_host))
        return super().run(log) \
            and copy_temp_file_from_host_command.run(log) \
            and delete_temp_file_from_container_command.run(log) \
            and delete_temp_dir_from_host_command.run(log)


class DirectoryTarRestoreCommand(DirectoryTarCommand):
    def __init__(self, directory: str, backup_path: str, tar_settings: TarBackupSettings, container_name: str, docker_options: List[str],
                 tar_name: str = None):
        tar_builder = construct_extract_tar_builder(tar_name, directory, DirectoryTarCommand.default_container_dir(), get_new_host_tmp_dir(), tar_settings)
        if not tar_builder.override_file_name:
            tar_builder.override_file_name = fs.find_youngest_file(backup_path, tar_builder.prefix, tar_builder.extension)
        super().__init__(directory, backup_path, tar_builder, tar_builder.command_extract_str_in_container, container_name, docker_options)

    def run(self, log: logging.Logger):
        os.makedirs(self.tar_builder.dest_dir_on_host)
        copy_backup_file_from_host_command = Command("cp {} {}".format(self.backup_file, self.tar_builder.host_file_path))
        chmod_temp_file_from_host_command = Command("chmod o+r {}".format(self.tar_builder.host_file_path))
        delete_temp_file_from_host_command = Command("rm {}".format(self.tar_builder.host_file_path))
        delete_temp_dir_from_host_command = Command("rm -rf {}".format(self.tar_builder.dest_dir_on_host))
        return copy_backup_file_from_host_command.run(log) \
            and chmod_temp_file_from_host_command.run(log) \
            and super().run(log) \
            and delete_temp_file_from_host_command.run(log) \
            and delete_temp_dir_from_host_command.run(log)
