from scheduler_failover_controller.configuration import Configuration
from scheduler_failover_controller.failover.failover_controller import FailoverController
from scheduler_failover_controller.logger.logger import get_logger
from scheduler_failover_controller.metadata.sql_metadata_service import SQLMetadataService
from scheduler_failover_controller.metadata.zookeeper_metadata_service import ZookeeperMetadataService
import sys
import time

# todo: better deployment and run method


def build_metadata_service(configuration, logger):
    metadata_type = configuration.get_metadata_type()
    if metadata_type == "SQLMetadataService":
        sql_alchemy_conn = configuration.get_sql_alchemy_conn()
        return SQLMetadataService(
            sql_alchemy_conn,
            logger
        )
    elif metadata_type == "ZookeeperMetadataService":
        metadata_service_zookeeper_nodes = configuration.get_metadata_service_zookeeper_nodes()
        return ZookeeperMetadataService(
            metadata_service_zookeeper_nodes,
            logger
        )
    else:
        raise Exception("MetadataService {0} not supported.".format(metadata_type))


def main(args):

    configuration = Configuration()
    logger = get_logger(
        logging_level=configuration.get_logging_level(),
        logs_output_file_path=configuration.get_logs_output_file_path()
    )

    # todo: validate
    scheduler_nodes_in_cluster = configuration.get_scheduler_nodes_in_cluster()
    airflow_scheduler_start_command = configuration.get_airflow_scheduler_start_command()
    airflow_scheduler_stop_command = configuration.get_airflow_scheduler_stop_command()
    poll_frequency = configuration.get_poll_frequency()
    metadata_service = build_metadata_service(configuration, logger)

    failover_controller = FailoverController(
        scheduler_nodes_in_cluster=scheduler_nodes_in_cluster,
        airflow_scheduler_start_command=airflow_scheduler_start_command,
        airflow_scheduler_stop_command=airflow_scheduler_stop_command,
        poll_frequency=poll_frequency,
        metadata_service=metadata_service,
        logger=logger
    )

    if "init" in args:
        configuration.add_default_scheduler_failover_configs_to_airflow_configs()
    elif "test_connection" in args:
        for host in scheduler_nodes_in_cluster:
            logger.info("Testing Connection for host '" + str(host) + "'")
            logger.info(failover_controller.run_command_through_ssh(host, "echo 'Connection Succeeded'"))
    if "is_scheduler_running" in args:
        for host in scheduler_nodes_in_cluster:
            logger.info("Testing to see if scheduler is running on host '" + str(host) + "'")
            logger.info(failover_controller.is_scheduler_running(host))
    elif "clear_metadata" in args:
        metadata_service.clear()
    elif "metadata" in args:
        print "Getting Metadata for current_host: '" + failover_controller.get_current_host() + "'"
        metadata_service.print_metadata()
    else:
        logger.info("Scheduler Failover Controller Starting Up!")

        current_host = failover_controller.get_current_host()
        logger.info("Current Host: " + str(current_host))

        metadata_service.initialize_metadata_source()

        # Infinite while loop for polling with a sleep for X seconds.
        while 1:
            failover_controller.poll()
            logger.info("Finished Polling. Sleeping for " + str(poll_frequency) + " seconds")
            time.sleep(poll_frequency)

        # should not get to this point
        logger.info("Scheduler Failover Controller Finished")


if __name__ == '__main__':
    try:
        args = sys.argv[1:]
        main(args)
    except KeyboardInterrupt:
        print >> sys.stderr, '\nExiting by user request.\n'
        sys.exit(0)

