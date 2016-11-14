import os
import ConfigParser
import sys
import logging


DEFAULT_POLL_FREQUENCY = 10

DEFAULT_SCHEDULER_FAILOVER_CONTROLLER_CONFIGS = """

[scheduler_failover]

# List of potential nodes that can act as Schedulers (Comma Separated List)
scheduler_nodes_in_cluster = localhost

# The metadata service class that the failover controller should use. Choices include:
# SQLMetadataService, ZookeeperMetadataService
# Note: ZookeeperMetadataService requires you to install kazoo (pip install kazoo)
metadata_service_type = SQLMetadataService

# If you're using the ZookeeperMetadataService, this property will identify the zookeeper nodes it will try to connect to
metadata_service_zookeeper_nodes = localhost:2181

# Frequency that the Scheduler Failover Controller polls to see if the scheduler is running (in seconds)
poll_frequency = """ + str(DEFAULT_POLL_FREQUENCY) + """

# Command to use when trying to start a Scheduler instance on a node
airflow_scheduler_start_command = "nohup airflow scheduler >> ~/airflow/logs/scheduler.logs &"

# Command to use when trying to stop a Scheduler instance on a node
airflow_scheduler_stop_command = "for pid in `ps -ef | grep "airflow scheduler" | awk '{print $2}'` ; do kill -9 $pid ; done"

"""


class Configuration:

    def __init__(self, airflow_home_dir=None, airflow_config_file_path=None):
        if airflow_home_dir is None:
            airflow_home_dir = os.environ['AIRFLOW_HOME'] if "AIRFLOW_HOME" in os.environ else os.path.expanduser("~/airflow")
        if airflow_config_file_path is None:
            airflow_config_file_path = airflow_home_dir + "/airflow.cfg"
        self.airflow_home_dir = airflow_home_dir
        self.airflow_config_file_path = airflow_config_file_path

        if not os.path.isfile(airflow_config_file_path):
            print "Cannot find Airflow Configuration file at '" + str(airflow_config_file_path) + "'!!!"
            sys.exit(1)

        self.conf = ConfigParser.RawConfigParser()
        self.conf.read(airflow_config_file_path)

    def get_airflow_home_dir(self):
        return self.airflow_home_dir

    def get_airflow_config_file_path(self):
        return self.airflow_config_file_path

    def get_logging_level(self):
        return logging.INFO

    def get_logs_output_file_path(self):
        log_dir = self.airflow_home_dir + "/logs/scheduler_failover/"
        logs_file_name = "scheduler-failover-controller.log"
        return log_dir + logs_file_name

    def get_config(self, section, option, default=None):
        config_value = self.conf.get(section, option)
        return config_value if config_value is not None else default

    def get_scheduler_failover_config(self, option, default=None):
        return self.get_config("scheduler_failover", option, default)

    def get_sql_alchemy_conn(self):
        return self.get_config("core", "SQL_ALCHEMY_CONN")

    def get_metadata_type(self):
        return self.get_scheduler_failover_config("METADATA_SERVICE_TYPE", "SQLMetadataService")

    def get_metadata_service_zookeeper_nodes(self):
        return self.get_scheduler_failover_config("METADATA_SERVICE_ZOOKEEPER_NODES")

    def get_scheduler_nodes_in_cluster(self):
        scheduler_nodes_in_cluster = self.get_scheduler_failover_config("SCHEDULER_NODES_IN_CLUSTER")
        if scheduler_nodes_in_cluster is not None:
            scheduler_nodes_in_cluster = scheduler_nodes_in_cluster.split(",")
        return scheduler_nodes_in_cluster

    def get_poll_frequency(self):
        return self.get_scheduler_failover_config("POLL_FREQUENCY", DEFAULT_POLL_FREQUENCY)

    def get_airflow_scheduler_start_command(self):
        return self.get_scheduler_failover_config("AIRFLOW_SCHEDULER_START_COMMAND")

    def get_airflow_scheduler_stop_command(self):
        return self.get_scheduler_failover_config("AIRFLOW_SCHEDULER_STOP_COMMAND")

    def add_default_scheduler_failover_configs_to_airflow_configs(self):
        airflow_config_file = open(self.airflow_config_file_path, 'w')
        if "[scheduler_failover]" not in airflow_config_file.read():
            airflow_config_file.write(DEFAULT_SCHEDULER_FAILOVER_CONTROLLER_CONFIGS)
