import subprocess


def run_command_through_ssh(host, command):
    command_split = ["ssh", host, command]
    process = subprocess.Popen(command_split, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    process.wait()
    output = ""
    if process.stderr is not None:
        output += process.stderr.readline()
    if process.stdout is not None:
        output += process.stdout.readline()
    return output
