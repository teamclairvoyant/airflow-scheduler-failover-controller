import subprocess


class CommandRunner:

    HOST_LIST_TO_RUN_LOCAL = ["localhost", "127.0.0.1"]

    def __init__(self, local_hostname, logger):
        self.local_hostname = local_hostname
        self.logger = logger

    def run_command(self, host, base_command):
        self.logger.debug("Running Command: " + str(base_command))
        if False:  # host == self.local_hostname or host in self.HOST_LIST_TO_RUN_LOCAL:  # todo: temporarily disabling this
            return self._run_local_command(base_command)
        else:
            return self._run_ssh_command(host, base_command)

    # todo: Fix this function
    def _run_local_command(self, base_command):
        self.logger.debug("Running command as Local command")
        return self._run_split_command(
            command_split=base_command
        )

    def _run_ssh_command(self, host, base_command):
        self.logger.debug("Running command as SSH command")
        command_split = ["ssh", host, base_command]
        return self._run_split_command(
            command_split=command_split
        )

    def _run_split_command(self, command_split):
        process = subprocess.Popen(command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()
        is_successful = True
        output = ""
        if process.stderr is not None:
            stderr_output = process.stderr.readline().strip()
            if stderr_output and stderr_output != "":
                output += stderr_output
                self.logger.error("Run Command stderr output: " + stderr_output)
                is_successful = False
        if process.stdout is not None:
            output += process.stdout.readline()
        output = output.replace("\r\n", "|").replace("\n", "|").rstrip("|")
        return is_successful, output


