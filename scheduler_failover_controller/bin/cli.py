#!/usr/bin/env python
from scheduler_failover_controller.app import build_metadata_service
from scheduler_failover_controller.command_runner.command_runner import CommandRunner
from scheduler_failover_controller.configuration import Configuration
from scheduler_failover_controller.emailer.emailer import Emailer
from scheduler_failover_controller.failover.failover_controller import FailoverController
from scheduler_failover_controller.logger.logger import get_logger
from scheduler_failover_controller.app import main
import scheduler_failover_controller
import argparse

configuration = Configuration()
logger = get_logger(
    logging_level=configuration.get_logging_level(),
    logs_output_file_path=configuration.get_logs_output_file_path(),
    logs_rotate_when=configuration.get_logs_rotate_when(),
    logs_rotate_backup_count=configuration.get_logs_rotate_backup_count()
)
current_host = configuration.get_current_host()
command_runner = CommandRunner(current_host, logger)


def get_all_scheduler_failover_controller_objects():
    scheduler_nodes_in_cluster = configuration.get_scheduler_nodes_in_cluster()
    poll_frequency = configuration.get_poll_frequency()
    metadata_service = build_metadata_service(configuration, logger)
    emailer = Emailer(configuration.get_alert_to_email(), logger, configuration.get_alert_email_subject())
    failover_controller = FailoverController(
        configuration=configuration,
        command_runner=command_runner,
        metadata_service=metadata_service,
        emailer=emailer,
        logger=logger
    )
    return scheduler_nodes_in_cluster, poll_frequency, metadata_service, emailer, failover_controller


def version(args):
    print("Scheduler Failover Controller Version: " + str(scheduler_failover_controller.__version__))


def init(args):
    if(args.venv is not None):
        venv_command = "source " + args.venv
    else:
        venv_command = ""
    configuration.add_default_scheduler_failover_configs_to_airflow_configs(venv_command)
    print("Finished Initializing Configurations to allow Scheduler Failover Controller to run. Please update the airflow.cfg with your desired configurations.")


def test_connection(args):
    scheduler_nodes_in_cluster = configuration.get_scheduler_nodes_in_cluster()
    for host in scheduler_nodes_in_cluster:
        print("Testing Connection for host '" + str(host) + "'")
        print(command_runner.run_command(host, "echo 'Connection Succeeded'"))


def is_scheduler_running(args):
    scheduler_nodes_in_cluster, poll_frequency, metadata_service, emailer, failover_controller = get_all_scheduler_failover_controller_objects()
    for host in scheduler_nodes_in_cluster:
        print("Testing to see if scheduler is running on host '" + str(host) + "'")
        print (failover_controller.is_scheduler_running(host))


def clear_metadata(args):
    scheduler_nodes_in_cluster, poll_frequency, metadata_service, emailer, failover_controller = get_all_scheduler_failover_controller_objects()
    metadata_service.clear()


def metadata(args):
    scheduler_nodes_in_cluster, poll_frequency, metadata_service, emailer, failover_controller = get_all_scheduler_failover_controller_objects()
    print ("Getting Metadata for current_host: '" + configuration.get_current_host() + "'")
    metadata_service.print_metadata()


def send_test_email(args):
    scheduler_nodes_in_cluster, poll_frequency, metadata_service, emailer, failover_controller = get_all_scheduler_failover_controller_objects()
    message = "This is a TEST ALERT"
    emailer.send_alert(
        current_host=configuration.get_current_host(),
        retry_count=configuration.get_retry_count_before_alerting(),
        latest_status_message=message,
        latest_start_message=message
    )


def get_current_host(args):
    print("current host: " + str(current_host))


def start(args):
    scheduler_nodes_in_cluster, poll_frequency, metadata_service, emailer, failover_controller = get_all_scheduler_failover_controller_objects()
    main(
        configuration=configuration,
        poll_frequency=poll_frequency,
        metadata_service=metadata_service,
        failover_controller=failover_controller,
        logger=logger
    )


def get_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='sub-command help', dest='subcommand')
    subparsers.required = True
    
    parser.add_argument('-venv', help='Specify virtualenv activation path')
    
    ht = "Prints out the version of the Scheduler Failover Controller"
    parser_logs = subparsers.add_parser('version', help=ht)
    parser_logs.set_defaults(func=version)

    ht = "Initialize Configurations to allow Scheduler Failover Controller to run"
    parser_logs = subparsers.add_parser('init', help=ht)
    parser_logs.set_defaults(func=init)

    ht = "Tests if you can connect to all the necessary machines listed in 'scheduler_nodes_in_cluster' config"
    parser_logs = subparsers.add_parser('test_connection', help=ht)
    parser_logs.set_defaults(func=test_connection)

    ht = "Checks if the Scheduler is running on the machines you have listed in 'scheduler_nodes_in_cluster' config"
    parser_logs = subparsers.add_parser('is_scheduler_running', help=ht)
    parser_logs.set_defaults(func=is_scheduler_running)

    ht = "Clear the Metadata in Metastore"
    parser_logs = subparsers.add_parser('clear_metadata', help=ht)
    parser_logs.set_defaults(func=clear_metadata)

    ht = "Get the Metadata from Metastore"
    parser_logs = subparsers.add_parser('metadata', help=ht)
    parser_logs.set_defaults(func=metadata)

    ht = "Send a Test Email"
    parser_logs = subparsers.add_parser('send_test_email', help=ht)
    parser_logs.set_defaults(func=send_test_email)

    ht = "Get the Current Hostname"
    parser_logs = subparsers.add_parser('get_current_host', help=ht)
    parser_logs.set_defaults(func=get_current_host)

    ht = "Start the Airflow Scheduler Failover Controller"
    parser_logs = subparsers.add_parser('start', help=ht)
    parser_logs.set_defaults(func=start)

    return parser
