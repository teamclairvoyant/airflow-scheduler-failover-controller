import datetime

from kazoo.client import \
    KazooClient  # documentation: https://kazoo.readthedocs.io/en/latest/basic_usage.html

from scheduler_failover_controller.metadata.base_metadata_service import BaseMetadataService
from scheduler_failover_controller.utils import date_utils


class ZookeeperMetadataService(BaseMetadataService):
    def __init__(self, zookeeper_nodes, logger,
                 zookeeper_base_bucket="/scheduler_failover_controller"):
        logger.debug(
            "Creating MetadataServer (type:ZookeeperMetadataService) with Args - zookeeper_nodes: {zookeeper_nodes}, logger: {logger}, zookeeper_base_bucket: {zookeeper_base_bucket}".format(
                **locals()))
        self.zookeeper_nodes = zookeeper_nodes
        self.zookeeper_base_bucket = zookeeper_base_bucket
        self.logger = logger

        self.failover_heartbeat_bucket = zookeeper_base_bucket + "/failover_heartbeat"
        self.active_failover_node_bucket = zookeeper_base_bucket + "/active_failover_node"
        self.active_scheduler_node_bucket = zookeeper_base_bucket + "/active_scheduler_node"

        self.zk = KazooClient(hosts=zookeeper_nodes)
        self.zk.start()

    def initialize_metadata_source(self):
        self.zk.ensure_path(self.zookeeper_base_bucket)

    def get_failover_heartbeat(self):
        self.logger.debug("Getting Failover Heartbeat")
        heart_beat_date = None
        if self.zk.exists(self.failover_heartbeat_bucket):
            data, stat = self.zk.get(self.failover_heartbeat_bucket)
            heart_beat_date_str = data
            heart_beat_date = date_utils.get_string_as_datetime(heart_beat_date_str)
        self.logger.debug("Returning {}".format(heart_beat_date))
        return heart_beat_date

    def set_failover_heartbeat(self):
        heart_beat_date_str = date_utils.get_datetime_as_str(datetime.datetime.now())
        self.logger.debug("Setting Failover Heartbeat to {}".format(heart_beat_date_str))
        if self.zk.exists(self.failover_heartbeat_bucket):
            self.logger.debug("Bucket exists. Setting bucket {} to value {}".format(
                self.failover_heartbeat_bucket, heart_beat_date_str))
            self.zk.set(self.failover_heartbeat_bucket, heart_beat_date_str)
        else:
            self.logger.debug("Bucket doesn't exist. Creating bucket {} with value {}".format(
                self.failover_heartbeat_bucket, heart_beat_date_str))
            self.zk.create(self.failover_heartbeat_bucket, heart_beat_date_str)

    def get_active_failover_node(self):
        self.logger.debug("Getting Active Failover Node")
        data = None
        if self.zk.exists(self.active_failover_node_bucket):
            data, stat = self.zk.get(self.active_failover_node_bucket)
        else:
            self.logger.debug("Active Failover Node Bucket {} doesn't exist.".format(
                self.active_scheduler_node_bucket))
        self.logger.debug("Returning {}".format(data))
        return data

    def set_active_failover_node(self, node):
        self.logger.debug("Setting Active Failover Node to {}".format(node))
        if self.zk.exists(self.active_failover_node_bucket):
            self.logger.debug("Bucket exists. Setting bucket {} to value {}".format(
                self.active_failover_node_bucket, node))
            self.zk.set(self.active_failover_node_bucket, node)
        else:
            self.logger.debug("Bucket doesn't exist. Creating bucket {} with value {}".format(
                self.active_failover_node_bucket, node))
            self.zk.create(self.active_failover_node_bucket, node)

    def get_active_scheduler_node(self):
        self.logger.debug("Getting Active Scheduler Node")
        data = None
        if self.zk.exists(self.active_scheduler_node_bucket):
            data, stat = self.zk.get(self.active_scheduler_node_bucket)
        else:
            self.logger.debug("Active Scheduler Node Bucket {} doesn't exist.".format(
                self.active_scheduler_node_bucket))
        self.logger.debug("Returning {}".format(data))
        return data

    def set_active_scheduler_node(self, node):
        self.logger.debug("Setting Active Scheduler Node to {}".format(node))
        if self.zk.exists(self.active_scheduler_node_bucket):
            self.logger.debug("Bucket exists. Setting bucket {} to value {}".format(
                self.active_scheduler_node_bucket, node))
            self.zk.set(self.active_scheduler_node_bucket, node)
        else:
            self.logger.debug("Bucket doesn't exist. Creating bucket {} with value {}".format(
                self.active_scheduler_node_bucket, node))
            self.zk.create(self.active_scheduler_node_bucket, node)

    def clear(self):
        self.zk.delete(self.zookeeper_base_bucket, recursive=True)
