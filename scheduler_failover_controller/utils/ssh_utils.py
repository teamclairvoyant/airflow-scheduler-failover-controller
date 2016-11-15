import subprocess


class SSHUtils():

    def __init__(self, logger):
        self.logger = logger

    def run_command_through_ssh(self, host, command):
        command_split = ["ssh", host, command]
        process = subprocess.Popen(command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        process.wait()
        output = ""
        if process.stderr is not None:
            output += process.stderr.readline()
            self.logger.critical("output: " + output)
            return None
        if process.stdout is not None:
            output += process.stdout.readline()
        return output
