import logging
import shlex
import subprocess
from typing import List


class Command:
    def __init__(self, command_string: str, input: str = None, input_path: str = None, output_path: str = None,
                 universal_newlines: bool = None):
        self.command_string = command_string
        self.input_str = input
        self.input_path = input_path
        self.output_path = output_path
        self.universal_newlines = universal_newlines
        self._input = None

    def __str__(self):
        return self.friendly_str()

    def friendly_str(self):
        return self.command_string

    def encoded_input(self):
        if not self._input:
            if self.input_str:
                self._input = self.input_str.encode()
            elif self.input_path:
                with open(self.input_path, 'rb') as in_file:
                    self._input = in_file.read()
        return self._input

    @property
    def capture_output(self):
        return self.output_path is not None

    def _run_with_result(self, command: str, log: logging.Logger):
        log.debug("Command::_run({}, {}):".format(command, self.output_path))
        if self.capture_output:
            with open(self.output_path, 'wb') as output:
                run_result = subprocess.run(shlex.split(command), input=self.encoded_input(), check=False,
                                            stdout=output, stderr=subprocess.PIPE,
                                            universal_newlines=self.universal_newlines)
        else:
            run_result = subprocess.run(shlex.split(command), input=self.encoded_input(), check=False,
                                        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                        universal_newlines=self.universal_newlines)
        if run_result.returncode != 0:
            log.critical("COMMAND FAILED: {}".format(command))
            if self.universal_newlines:
                log.critical(run_result.stderr)
            else:
                log.critical(run_result.stderr.decode())
        return run_result

    def _run(self, command: str, log: logging.Logger):
        return self._run_with_result(command, log).returncode == 0

    def run(self, log: logging.Logger) -> bool:
        return self._run(self.command_string, log)

    def run_with_result(self, log: logging.Logger) -> subprocess.CompletedProcess:
        return self._run_with_result(self.command_string, log)


class RemoteCommand(Command):
    def __init__(self, ssh_options: List[str], ssh_connection_string: str, command_string: str, do_not_wrap_command: bool = False,
                 input: str = None, input_path: str = None, output_path: str = None, universal_newlines: bool = None):
        ssh_command_list = ['ssh'] + ssh_options + [ssh_connection_string]
        if do_not_wrap_command:
            command_string = " ".join(ssh_command_list) + " " + command_string
        else:
            command_string = " ".join(ssh_command_list) + " \"bash --login -c '{}'\"".format(command_string)

        super().__init__(command_string, input, input_path, output_path, universal_newlines)
