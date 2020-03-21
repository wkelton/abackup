import logging
import os
import shlex
import subprocess
import uuid

from typing import List


#TODO: look into replacing all the subprocess calls with the python docker library


class Command:
    def __init__(self, command_string: str, input: str = None, input_path: str= None, output_path: str = None):
        self.command_string = command_string
        self.input = input
        if input_path:
            with open(input_path, 'rb') as in_file:
                self.input = in_file.read()
        self.output_path = output_path

    def __str__(self):
        return self.friendly_str()

    def friendly_str(self):
        return self.command_string

    @property
    def capture_output(self):
        return self.output_path != None

    def _run(self, command: str, log: logging.Logger):
        log.debug("Command::_run({}, {}):".format(command, self.output_path))
        if self.capture_output:
            with open(self.output_path, 'wb') as output:
                run_result = subprocess.run(shlex.split(command), input=self.input, check=False,
                    stdout=output, stderr=subprocess.PIPE)
        else:
            run_result = subprocess.run(shlex.split(command), input=self.input, check=False,
                stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if run_result.returncode != 0:
            log.critical("COMMAND FAILED: {}".format(command))
            log.critical(run_result.stderr.decode())
        return run_result.returncode == 0

    def run(self, log: logging.Logger):
        return self._run(self.command_string, log)


class DockerCommand(Command):
    def __init__(self, command_string: str, container_name: str, docker_options: List[str], 
                 input: str = None, input_path: str = None, output_path: str = None, in_container: bool = False):
        self.container_name = container_name
        self.options = " ".join(docker_options)
        self.run_command_in_container = in_container
        self.docker_command = "docker exec" if self.run_command_in_container else "docker run"
        super().__init__(command_string, input=input, input_path=input_path, output_path=output_path)

    def friendly_str(self):
        return self.docker_command

    def run(self, log: logging.Logger):
        log.debug("Command::run({}, {}, {}, {}, {})".format(self.container_name, self.command_string,
            self.options, self.run_command_in_container, self.output_path))
        if self.run_command_in_container:
            command = "{} {} {} sh -c '{}'".format(self.docker_command, self.options, self.container_name,
                                                   self.command_string)
            log.info("Running command in the {} container: {}".format(self.container_name, self.friendly_str()))
        else:
            command = "{} --volumes-from {} {} busybox sh -c '{}'".format(self.docker_command, self.container_name, 
                                                                          self.options, self.command_string)
            log.info("Running command in a busybox container: {}".format(self.friendly_str()))
        return self._run(command, log)

    def new_command(self, command_string: str):
        dc = DockerCommand(command_string, self.container_name, self.options, self.input,
            output_path=self.output_path, in_container=self.run_command_in_container)
        dc.options = self.options
        return dc


class MySqlBRCommand(DockerCommand):
    def __init__(self, name: str, password: str, mysql_command: str, mysql_options: List[str],
                 backup_path: str, container_name: str, docker_options: List[str], input_path: str = None,
                 output_path: str = None):
        self.mysql_command = mysql_command
        self.name = name
        self.backup_file = MySqlBRCommand.default_backup_file(backup_path, name)
        dopts = docker_options + ['-i']
        super().__init__('{} {} -p"{}" {}'.format(mysql_command, " ".join(mysql_options), password, name),
            container_name, dopts, input_path=input_path, output_path=output_path, in_container=True)

    @classmethod
    def default_backup_file(cls, backup_path, name):
        return os.path.join(backup_path, "{}.sql".format(name))

    def friendly_str(self):
        return "{} {}".format(self.mysql_command, self.name)


class DBBackupCommand(MySqlBRCommand):
    def __init__(self, name: str, password: str, backup_path: str, container_name: str, docker_options: List[str]):
        super().__init__(name, password, 'mysqldump', ['--single-transaction'], backup_path, container_name,
            docker_options, output_path=MySqlBRCommand.default_backup_file(backup_path, name))


class DBRestoreCommand(MySqlBRCommand):
    def __init__(self, name: str, password: str, backup_path: str, container_name: str, docker_options: List[str]):
        super().__init__(name, password, 'mysql', [], backup_path, container_name, docker_options,
            input_path=MySqlBRCommand.default_backup_file(backup_path, name))


class DirectoryBackupCommand(DockerCommand):
    def __init__(self, directory: str, backup_path: str, container_name: str, docker_options: List[str]):
        self.directory = directory
        tar_name = "{}.tar.gz".format(os.path.basename(directory))
        local_dir = "/abackup"
        self.local_file = os.path.join(local_dir, tar_name)
        self.tmp_dir = os.path.join(os.getcwd(), '.abackup-tmp', str(uuid.uuid4()))
        self.tmp_file = os.path.join(self.tmp_dir, tar_name)
        self.backup_file = os.path.join(backup_path, tar_name)
        dopts = docker_options + [ '--rm', '-v', "{}:{}".format(self.tmp_dir, local_dir) ]
        super().__init__("tar -czf {} {}".format(self.local_file, directory), container_name, dopts)

    def friendly_str(self):
        return "tar {}".format(self.directory)

    def run(self, log: logging.Logger):
        os.makedirs(self.tmp_dir)
        copy_temp_file_from_host_command = Command("cp {} {}".format(self.tmp_file, self.backup_file))
        delete_temp_file_from_container_command = self.new_command("rm {}".format(self.local_file))
        delete_temp_dir_from_host_command = Command("rm -rf {}".format(self.tmp_dir))
        return super().run(log) and copy_temp_file_from_host_command.run(log) and \
            delete_temp_file_from_container_command.run(log) and delete_temp_dir_from_host_command.run(log)


class DirectoryRestoreCommand(DockerCommand):
    def __init__(self, directory: str, backup_path: str, container_name: str, docker_options: List[str]):
        self.directory = directory
        tar_name = "{}.tar.gz".format(os.path.basename(directory))
        local_dir = "/abackup"
        self.local_file = os.path.join(local_dir, tar_name)
        self.tmp_dir = os.path.join(os.getcwd(), '.abackup-tmp', str(uuid.uuid4()))
        self.tmp_file = os.path.join(self.tmp_dir, tar_name)
        self.backup_file = os.path.join(backup_path, tar_name)
        dopts = docker_options + [ '--rm', '-v', "{}:{}".format(self.tmp_dir, local_dir) ]
        super().__init__("tar -xzf {}".format(self.local_file), container_name, dopts)

    def friendly_str(self):
        return "tar {}".format(self.directory)

    def run(self, log: logging.Logger):
        os.makedirs(self.tmp_dir)
        copy_backup_file_from_host_command = Command("cp {} {}".format(self.backup_file, self.tmp_file))
        chmod_temp_file_from_host_command = Command("chmod o+r {}".format(self.tmp_file))
        delete_temp_file_from_host_command = Command("rm {}".format(self.tmp_file))
        delete_temp_dir_from_host_command = Command("rm -rf {}".format(self.tmp_dir))
        return copy_backup_file_from_host_command.run(log) and chmod_temp_file_from_host_command.run(log) and \
            super().run(log) and delete_temp_file_from_host_command.run(log) and \
            delete_temp_dir_from_host_command.run(log)
