import logging
import os
import time
import uuid

from typing import List

from abackup import Command, fs


# TODO: look into replacing all the subprocess calls with the python docker library


def compress_file(file_path: str, log: logging.Logger):
    return Command("gzip --force --rsyncable {}".format(file_path)).run(log)


def decompress_file(file_path: str, log: logging.Logger):
    return Command("gzip --decompress {}".format(file_path)).run(log)


class BackupFileSettings:
    def __init__(self, is_single_backup: bool, prefix: str = None, force_timestamp: bool = None, use_compression: bool = True):
        self.is_single_backup = is_single_backup
        self.prefix = prefix
        self.force_timestamp = force_timestamp
        self.use_compression = use_compression

    @property
    def use_identifier_as_perfix(self):
        return self.prefix is None

    @property
    def use_timestamp(self):
        return self.force_timestamp or not self.is_single_backup


class BackupFile:
    def __init__(self, identifier: str, backup_path: str, settings: BackupFileSettings, pre_compress_ext: str, override_file_name: str = None):
        self.identifier = identifier
        self.backup_path = backup_path
        self.settings = settings
        self.pre_compress_ext = pre_compress_ext
        self.override_file_name = override_file_name
        self._file_name_without_ext = None

    @property
    def prefix(self):
        if self.settings.use_identifier_as_perfix:
            return self.identifier
        return self.settings.prefix

    @property
    def extension(self):
        if self.settings.use_compression:
            return "{}.gz".format(self.pre_compress_ext)
        return self.pre_compress_ext

    @property
    def file_name_without_extension(self):
        if not self._file_name_without_ext:
            if self.override_file_name:
                self._file_name_without_ext = os.path.splitext(self.override_file_name)[0]
                file_name, ext = os.path.splitext(self._file_name_without_ext)
                if ext == ".{}".format(self.pre_compress_ext):
                    self._file_name_without_ext = file_name
            else:
                if not self.settings.use_timestamp:
                    self._file_name_without_ext = self.prefix
                else:
                    self._file_name_without_ext = "{}_{}".format(self.prefix, time.strftime("%Y%m%d_%H%M%S"))
        return self._file_name_without_ext

    @property
    def file_name(self):
        if self.override_file_name:
            return self.override_file_name
        return "{}.{}".format(self.file_name_without_extension, self.extension)

    @property
    def file_path(self):
        return os.path.join(self.backup_path, self.file_name)

    @property
    def pre_compression_file_path(self):
        return os.path.join(self.backup_path, "{}.{}".format(self.file_name_without_extension, self.pre_compress_ext))


def construct_backup_file(name: str, backup_path: str, settings: BackupFileSettings, pre_compress_ext: str):
    return BackupFile(name, backup_path, settings, pre_compress_ext)


def find_restore_file(backup_filename: str, name: str, backup_path: str, settings: BackupFileSettings, pre_compress_ext: str):
    backup_file = BackupFile(name, backup_path, settings, pre_compress_ext, backup_filename)
    if not backup_file.override_file_name:
        backup_file.override_file_name = fs.find_youngest_file(backup_path, backup_file.prefix, backup_file.extension)
    return backup_file


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
            command = "{} --volumes-from {} {} ubuntu sh -c '{}'".format(self.docker_command, self.container_name,
                                                                          self.formatted_options(), self.command_string)
            log.info("Running command in a busybox container: {}".format(self.friendly_str()))
        return self._run(command, log)

    def new_command(self, command_string: str):
        dc = DockerCommand(command_string, self.container_name, self.docker_options, self.input_str,
                           output_path=self.output_path, in_container=self.run_command_in_container)
        return dc


#######################################################################################################################
# Database Backup/Restore
#######################################################################################################################


class DBBRCommand(DockerCommand):
    def __init__(self, name: str, db_command: str, backup_path: str, backup_file: BackupFile, container_name: str, docker_options: List[str],
                 input_path: str = None, output_path: str = None):
        self.name = name
        self.db_command = db_command
        self.backup_path = backup_path
        self.backup_file = backup_file
        d_opts = docker_options + ['-i']
        super().__init__(db_command, container_name, d_opts, input_path=input_path, output_path=output_path, in_container=True)

    @property
    def backup_file_path(self):
        return self.backup_file.file_path

    @property
    def file_prefix(self):
        return self.backup_file.prefix

    @property
    def file_extension(self):
        return self.backup_file.extension

    def friendly_str(self):
        return "DB BR Command for {}".format(self.name)

    def _run_backup(self, log: logging.Logger):
        if not super().run(log):
            return False
        if self.backup_file.settings.use_compression:
            return compress_file(self.output_path, log)
        return True

    def _run_restore(self, log: logging.Logger):
        if self.backup_file.settings.use_compression:
            if not decompress_file(self.backup_file_path, log):
                return False
        was_restore_successful = super().run(log)
        if self.backup_file.settings.use_compression:
            if not compress_file(self.input_path, log):
                return False
        return was_restore_successful

    def run(self, log: logging.Logger):
        if self.input_path and self.output_path:
            log.critical("DBBRCommand: input_path and output_path both provided, this is not supported; {}".format(
                self.db_command))
            return False
        if not self.input_path and not self.output_path:
            log.critical("DBBRCommand: input_path nor output_path provided, this is not supported; {}".format(
                self.db_command))
            return False
        if self.output_path:
            return self._run_backup(log)
        if self.input_path:
            return self._run_restore(log)
        return False  # will never be hit


class MySqlBRCommand(DBBRCommand):
    def __init__(self, name: str, password: str, mysql_command: str, mysql_options: List[str],
                 backup_path: str, sql_backup_file: BackupFile, container_name: str, docker_options: List[str],
                 input_path: str = None, output_path: str = None):
        self.mysql_command = mysql_command
        super().__init__(name, '{} {} -p"{}" {}'.format(mysql_command, " ".join(mysql_options), password, name), backup_path,
                         sql_backup_file, container_name, docker_options, input_path=input_path, output_path=output_path)

    def friendly_str(self):
        return "{} {}".format(self.mysql_command, self.name)


class MysqlBackupCommand(MySqlBRCommand):
    def __init__(self, name: str, password: str, backup_path: str, settings: BackupFileSettings, container_name: str, docker_options: List[str]):
        sql_backup_file = construct_backup_file(name, backup_path, settings, 'sql')
        super().__init__(name, password, 'mysqldump', ['--single-transaction'], backup_path, sql_backup_file, container_name,
                         docker_options, output_path=sql_backup_file.pre_compression_file_path)


class MysqlRestoreCommand(MySqlBRCommand):
    def __init__(self, name: str, password: str, backup_path: str, settings: BackupFileSettings, container_name: str, docker_options: List[str],
                 backup_filename: str = None):
        sql_backup_file = find_restore_file(backup_filename, name, backup_path, settings, 'sql')
        super().__init__(name, password, 'mysql', [], backup_path, sql_backup_file, container_name, docker_options,
                         input_path=sql_backup_file.pre_compression_file_path)


class PostgresBRCommand(DBBRCommand):
    def __init__(self, name: str, postgres_command: str, postgres_options: List[str], backup_path: str,
                 sql_backup_file: BackupFile, container_name: str, docker_options: List[str], user: str = None, password: str = None,
                 input_path: str = None, output_path: str = None):
        self.postgres_command = postgres_command
        # TODO: support using 'password'
        p_opts = postgres_options + ['-U', user] if user else postgres_options
        super().__init__(name, '{} {}'.format(postgres_command, " ".join(p_opts)), backup_path, sql_backup_file, container_name, docker_options,
                         input_path=input_path, output_path=output_path)

    def friendly_str(self):
        return "{} {}".format(self.postgres_command, self.name)


class PostgresBackupCommand(PostgresBRCommand):
    def __init__(self, name: str, backup_path: str, settings: BackupFileSettings, container_name: str, docker_options: List[str], user: str = None,
                 password: str = None, dump_all: bool = False):
        postgres_command = 'pg_dump'
        postgres_options = ['-d', name]
        if dump_all:
            postgres_command = 'pg_dumpall'
            postgres_options = []
        sql_backup_file = construct_backup_file(name, backup_path, settings, 'sql')
        super().__init__(name, postgres_command, postgres_options, backup_path, sql_backup_file, container_name, docker_options, user,
                         password, output_path=sql_backup_file.pre_compression_file_path)


class PostgresRestoreCommand(PostgresBRCommand):
    def __init__(self, name: str, backup_path: str, settings: BackupFileSettings, container_name: str, docker_options: List[str], user: str,
                 password: str, restore_all: bool = False, backup_filename: str = None):
        sql_backup_file = find_restore_file(backup_filename, name, backup_path, settings, 'sql')
        super().__init__(name, 'psql', ['-d', 'postgres'] if restore_all else ['-d', name], backup_path,
                         sql_backup_file, container_name, docker_options, user, password,
                         input_path=sql_backup_file.pre_compression_file_path)


#######################################################################################################################
# Directory Backup/Restore
#######################################################################################################################


class TarBackupSettings(BackupFileSettings):
    def __init__(self, is_single_backup: bool, prefix: str = None, force_timestamp: bool = None, use_compression: bool = True):
        super().__init__(is_single_backup, prefix, force_timestamp, use_compression)


class BackupTarFile(BackupFile):
    def __init__(self, identifier: str, source_dir_in_container: str, dest_dir_in_container: str, dest_dir_on_host: str, settings: TarBackupSettings, backup_path: str, override_file_name: str = None):
        self.source_dir_in_container = source_dir_in_container
        self.dest_dir_in_container = dest_dir_in_container
        self.dest_dir_on_host = dest_dir_on_host
        super().__init__(identifier, backup_path, settings, 'tar', override_file_name)

    @property
    def container_file_path(self):
        return os.path.join(self.dest_dir_in_container, self.file_name)

    @property
    def host_file_path(self):
        return os.path.join(self.dest_dir_on_host, self.file_name)

    @property
    def command_create_str_in_container(self):
        return "tar -cf - {} | gzip --rsyncable > {}".format(self.source_dir_in_container, self.container_file_path)

    @property
    def command_extract_str_in_container(self):
        return "tar -xzf {}".format(self.container_file_path)


def construct_backup_tar_file_for_create(directory: str, container_dir: str, host_tmp_dir: str, settings: TarBackupSettings, backup_path: str):
    return BackupTarFile(os.path.basename(directory), directory, container_dir, host_tmp_dir, settings, backup_path)


def find_backup_tar_file_for_extract(tar_name: str, directory: str, container_dir: str, host_tmp_dir: str, settings: TarBackupSettings, backup_path: str):
    backup_tar_file = BackupTarFile(os.path.basename(directory), directory, container_dir, host_tmp_dir, settings, backup_path, tar_name)
    if not backup_tar_file.override_file_name:
        backup_tar_file.override_file_name = fs.find_youngest_file(backup_path, backup_tar_file.prefix, backup_tar_file.extension)
    return backup_tar_file


def get_new_host_tmp_dir():
    return os.path.join(os.getcwd(), '.abackup-tmp', str(uuid.uuid4()))


class DirectoryTarCommand(DockerCommand):
    def __init__(self, directory: str, backup_path: str, backup_tar_file: BackupTarFile, tar_command: str, container_name: str, docker_options: List[str]):
        self.name = backup_tar_file.identifier
        self.directory = directory
        self.backup_path = backup_path
        self.backup_tar_file = backup_tar_file

        d_opts = docker_options + ['--rm', '-v', "{}:{}".format(self.backup_tar_file.dest_dir_on_host, DirectoryTarCommand.default_container_dir())]
        super().__init__(tar_command, container_name, d_opts)

    @classmethod
    def default_container_dir(cls):
        return '/abackup'

    @property
    def backup_file_path(self):
        return self.backup_tar_file.file_path

    @property
    def file_prefix(self):
        return self.backup_tar_file.prefix

    @property
    def file_extension(self):
        return self.backup_tar_file.extension

    def friendly_str(self):
        return "tar {}".format(self.directory)


class DirectoryTarBackupCommand(DirectoryTarCommand):
    def __init__(self, directory: str, backup_path: str, tar_settings: TarBackupSettings, container_name: str, docker_options: List[str]):
        backup_tar_file = construct_backup_tar_file_for_create(
            directory, DirectoryTarCommand.default_container_dir(), get_new_host_tmp_dir(), tar_settings, backup_path)
        super().__init__(directory, backup_path, backup_tar_file,
                         backup_tar_file.command_create_str_in_container, container_name, docker_options)

    def run(self, log: logging.Logger):
        os.makedirs(self.backup_tar_file.dest_dir_on_host)
        copy_temp_file_from_host_command = Command("cp {} {}".format(self.backup_tar_file.host_file_path, self.backup_file_path))
        delete_temp_file_from_container_command = self.new_command("rm {}".format(self.backup_tar_file.container_file_path))
        delete_temp_dir_from_host_command = Command("rm -rf {}".format(self.backup_tar_file.dest_dir_on_host))
        return super().run(log) \
            and copy_temp_file_from_host_command.run(log) \
            and delete_temp_file_from_container_command.run(log) \
            and delete_temp_dir_from_host_command.run(log)


class DirectoryTarRestoreCommand(DirectoryTarCommand):
    def __init__(self, directory: str, backup_path: str, tar_settings: TarBackupSettings, container_name: str, docker_options: List[str],
                 tar_name: str = None):
        backup_tar_file = find_backup_tar_file_for_extract(
            tar_name, directory, DirectoryTarCommand.default_container_dir(), get_new_host_tmp_dir(), tar_settings, backup_path)
        super().__init__(directory, backup_path, backup_tar_file,
                         backup_tar_file.command_extract_str_in_container, container_name, docker_options)

    def run(self, log: logging.Logger):
        os.makedirs(self.backup_tar_file.dest_dir_on_host)
        copy_backup_file_from_host_command = Command("cp {} {}".format(self.backup_file_path, self.backup_tar_file.host_file_path))
        chmod_temp_file_from_host_command = Command("chmod o+r {}".format(self.backup_tar_file.host_file_path))
        delete_temp_file_from_host_command = Command("rm {}".format(self.backup_tar_file.host_file_path))
        delete_temp_dir_from_host_command = Command("rm -rf {}".format(self.backup_tar_file.dest_dir_on_host))
        return copy_backup_file_from_host_command.run(log) \
            and chmod_temp_file_from_host_command.run(log) \
            and super().run(log) \
            and delete_temp_file_from_host_command.run(log) \
            and delete_temp_dir_from_host_command.run(log)
