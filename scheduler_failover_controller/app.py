from scheduler_failover_controller.metadata.sql_metadata_service import SQLMetadataService
from scheduler_failover_controller.metadata.zookeeper_metadata_service import ZookeeperMetadataService
import time


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


def main(configuration, poll_frequency, metadata_service, failover_controller, logger):

    logger.info("Scheduler Failover Controller Starting Up!")

    current_host = configuration.get_current_host()
    logger.info("Current Host: " + str(current_host))

    metadata_service.initialize_metadata_source()

    # Infinite while loop for polling with a sleep for X seconds.
    while 1:
        failover_controller.poll()
        logger.info("Finished Polling. Sleeping for " + str(poll_frequency) + " seconds")
        time.sleep(poll_frequency)

    # should not get to this point
    logger.info("Scheduler Failover Controller Finished")

