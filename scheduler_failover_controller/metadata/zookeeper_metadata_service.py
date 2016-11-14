__author__ = 'robertsanders'

from kazoo.client import KazooClient
from scheduler_failover_controller.metadata.base_metadata_service import BaseMetadataService

# pip install kazoo
# documentation: https://kazoo.readthedocs.io/en/latest/basic_usage.html

# Setup SSH: https://www.centos.org/docs/5/html/5.2/Deployment_Guide/s3-openssh-rsa-keys-v2.html

# Add the following to the airflow.cfg file:
#   [core]
#   nodes_in_cluster = 10.0.0.141,10.0.0.142
#
#   zookeeper_nodes = ec2-107-21-50-85.compute-1.amazonaws.com:2181

ZOOKEEPER_NODES = conf.get('core', 'ZOOKEEPER_NODES')
ZOOKEEPER_BASE_BUCKET_PATH = "/airflow/scheduler_failover"
print "ZOOKEEPER_NODES: " + str(ZOOKEEPER_NODES)
logging.basicConfig()
zk = KazooClient(hosts=ZOOKEEPER_NODES)
zk.start()
zk.ensure_path(ZOOKEEPER_BASE_BUCKET_PATH)


# pip install kazoo

class SchedulerFailoverKeyValueStore(BaseMetadataService):

    FAILOVER_HEARTBEAT_BUCKET = ZOOKEEPER_BASE_BUCKET_PATH + "/failover_heartbeat"
    ACTIVE_FAILOVER_NODE_BUCKET = ZOOKEEPER_BASE_BUCKET_PATH + "/active_failover_node"
    ACTIVE_SCHEDULER_NODE_BUCKET = ZOOKEEPER_BASE_BUCKET_PATH + "/active_scheduler_node"

    @classmethod
    def get_failover_heartbeat(cls):
        heart_beat_date = None
        if zk.exists(SchedulerFailoverKeyValueStore.FAILOVER_HEARTBEAT_BUCKET):
            data, stat = zk.get(SchedulerFailoverKeyValueStore.FAILOVER_HEARTBEAT_BUCKET)
            heart_beat_date_str = data
            heart_beat_date = datetime.datetime.strptime(heart_beat_date_str, "%Y-%m-%d %H:%M:%S")
        return heart_beat_date

    @classmethod
    def set_failover_heartbeat(cls):
        heart_beat_date = datetime.datetime.now()
        heart_beat_date_str = heart_beat_date.strftime("%Y-%m-%d %H:%M:%S")
        if zk.exists(SchedulerFailoverKeyValueStore.FAILOVER_HEARTBEAT_BUCKET):
            zk.set(SchedulerFailoverKeyValueStore.FAILOVER_HEARTBEAT_BUCKET, heart_beat_date_str)
        else:
            zk.create(SchedulerFailoverKeyValueStore.FAILOVER_HEARTBEAT_BUCKET, heart_beat_date_str)

    @classmethod
    def get_active_failover_node(cls):
        if zk.exists(SchedulerFailoverKeyValueStore.ACTIVE_FAILOVER_NODE_BUCKET):
            data, stat = zk.get(SchedulerFailoverKeyValueStore.ACTIVE_FAILOVER_NODE_BUCKET)
            return data
        return None

    @classmethod
    def set_active_failover_node(cls, node):
        if zk.exists(SchedulerFailoverKeyValueStore.ACTIVE_FAILOVER_NODE_BUCKET):
            zk.set(SchedulerFailoverKeyValueStore.ACTIVE_FAILOVER_NODE_BUCKET, node)
        else:
            zk.create(SchedulerFailoverKeyValueStore.ACTIVE_FAILOVER_NODE_BUCKET, node)

    @classmethod
    def get_active_scheduler_node(cls):
        if zk.exists(SchedulerFailoverKeyValueStore.ACTIVE_SCHEDULER_NODE_BUCKET):
            data, stat = zk.get(SchedulerFailoverKeyValueStore.ACTIVE_SCHEDULER_NODE_BUCKET)
            return data
        return None

    @classmethod
    def set_active_scheduler_node(cls, node):
        if zk.exists(SchedulerFailoverKeyValueStore.ACTIVE_SCHEDULER_NODE_BUCKET):
            zk.set(SchedulerFailoverKeyValueStore.ACTIVE_SCHEDULER_NODE_BUCKET, node)
        else:
            zk.create(SchedulerFailoverKeyValueStore.ACTIVE_SCHEDULER_NODE_BUCKET, node)

    @classmethod
    def clear(cls):
        zk.delete(ZOOKEEPER_BASE_BUCKET_PATH, recursive=True)
