#!/usr/bin/env python
import argparse
from scheduler_failover_controller.app import build_metadata_service
from scheduler_failover_controller.configuration import Configuration
from scheduler_failover_controller.failover.failover_controller import FailoverController
from scheduler_failover_controller.logger.logger import get_logger
from scheduler_failover_controller.app import main
from scheduler_failover_controller.utils import ssh_utils
from scheduler_failover_controller.utils.ssh_utils import SSHUtils

configuration = Configuration()
logger = get_logger(
    logging_level=configuration.get_logging_level(),
    logs_output_file_path=configuration.get_logs_output_file_path()
)


def get_all_scheduler_failover_controller_objects():
    scheduler_nodes_in_cluster = configuration.get_scheduler_nodes_in_cluster()
    poll_frequency = configuration.get_poll_frequency()
    # todo: validate configs
    metadata_service = build_metadata_service(configuration, logger)

    failover_controller = FailoverController(
        scheduler_nodes_in_cluster=configuration.get_scheduler_nodes_in_cluster(),
        airflow_scheduler_start_command=configuration.get_airflow_scheduler_start_command(),
        airflow_scheduler_stop_command=configuration.get_airflow_scheduler_stop_command(),
        poll_frequency=poll_frequency,
        metadata_service=metadata_service,
        logger=logger
    )

    return scheduler_nodes_in_cluster, poll_frequency, metadata_service, failover_controller


def init(args):
    configuration.add_default_scheduler_failover_configs_to_airflow_configs()


def test_connection(args):
    scheduler_nodes_in_cluster = configuration.get_scheduler_nodes_in_cluster()
    ssh_utils = SSHUtils(logger)
    for host in scheduler_nodes_in_cluster:
        print "Testing Connection for host '" + str(host) + "'"
        print ssh_utils.run_command_through_ssh(host, "echo 'Connection Succeeded'")


def is_scheduler_running(args):
    scheduler_nodes_in_cluster, poll_frequency, metadata_service, failover_controller = get_all_scheduler_failover_controller_objects()
    for host in scheduler_nodes_in_cluster:
        print "Testing to see if scheduler is running on host '" + str(host) + "'"
        print failover_controller.is_scheduler_running(host)


def clear_metadata(args):
    scheduler_nodes_in_cluster, poll_frequency, metadata_service, failover_controller = get_all_scheduler_failover_controller_objects()
    metadata_service.clear()


def metadata(args):
    scheduler_nodes_in_cluster, poll_frequency, metadata_service, failover_controller = get_all_scheduler_failover_controller_objects()
    print "Getting Metadata for current_host: '" + failover_controller.get_current_host() + "'"
    metadata_service.print_metadata()


def start(args):
    scheduler_nodes_in_cluster, poll_frequency, metadata_service, failover_controller = get_all_scheduler_failover_controller_objects()
    main(
        configuration=configuration,
        poll_frequency=poll_frequency,
        metadata_service=metadata_service,
        failover_controller=failover_controller, logger=logger
    )


def get_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(help='sub-command help', dest='subcommand')
    subparsers.required = True

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

    ht = "Start the Airflow Scheduler Failover Controller"
    parser_logs = subparsers.add_parser('start', help=ht)
    parser_logs.set_defaults(func=start)

    return parser