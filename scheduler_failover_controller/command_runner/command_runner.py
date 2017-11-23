import os
import subprocess


class CommandRunner:
    HOST_LIST_TO_RUN_LOCAL = ["localhost", "127.0.0.1"]

    def __init__(self, local_hostname, logger):
        logger.debug(
            "Creating CommandRunner with Args - local_hostname: {local_hostname}, logger: {logger}".format(
                **locals()))
        self.local_hostname = local_hostname
        self.logger = logger

    # returns: is_successful, output
    def run_command(self, host, base_command):
        self.logger.debug("Running Command: {}".format(base_command))
        if host == self.local_hostname or host in self.HOST_LIST_TO_RUN_LOCAL:
            return self._run_local_command(base_command)
        else:
            return self._run_ssh_command(host, base_command)

    # This will start the process up as a child process.
    # Meaning if the scheduler_failover_controller fails the child process will fail as well.
    # (unless you're running the systemctl command)
    def _run_local_command(self, base_command):
        self.logger.debug("Running command as Local command")
        output = os.popen(base_command).read()
        if output:
            output = output.split("\n")
        self.logger.debug("Run Command output: {}".format(output))
        return True, output

    def _run_ssh_command(self, host, base_command):
        self.logger.debug("Running command as SSH command")
        if base_command.startswith("sudo"):
            command_split = ["ssh", "-tt", host, base_command]
        else:
            command_split = ["ssh", host, base_command]
        return self._run_split_command(
            command_split=command_split
        )

    def _run_split_command(self, command_split):
        self.logger.debug("Running command_split: {}".format(command_split))
        is_successful = True
        output = []
        try:
            process = subprocess.Popen(command_split, stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE)
            process.wait()
            if process.stderr is not None:
                stderr_output = process.stderr.readlines()
                if stderr_output and len(stderr_output) > 0:
                    output += stderr_output
                    self.logger.debug("Run Command stderr output: {}".format(stderr_output))
            if process.stdout is not None:
                output += process.stdout.readlines()
            if process.returncode != 0:
                self.logger.warn("Process returned code {}".format(process.returncode))
                is_successful = False
        except Exception as e:
            is_successful = False
            output = e
        self.logger.debug("Run Command output: {}".format(output))
        return is_successful, output
