import os
import ConfigParser
import socket
import sys
import logging


def get_airflow_home_dir():
    return os.environ['AIRFLOW_HOME'] if "AIRFLOW_HOME" in os.environ else os.path.expanduser("~/airflow")

DEFAULT_AIRFLOW_HOME_DIR = get_airflow_home_dir()
DEFAULT_METADATA_SERVICE_TYPE = "SQLMetadataService"
DEFAULT_POLL_FREQUENCY = 10
DEFAULT_LOGGING_LEVEL = "INFO"
DEFAULT_LOGS_ROTATE_WHEN = "midnight"
DEFAULT_LOGS_ROTATE_BACKUP_COUNT = 7
DEFAULT_RETRY_COUNT_BEFORE_ALERTING = 5

DEFAULT_SCHEDULER_FAILOVER_CONTROLLER_CONFIGS = """
[scheduler_failover]

# List of potential nodes that can act as Schedulers (Comma Separated List)
scheduler_nodes_in_cluster = localhost

# The metadata service class that the failover controller should use. Choices include:
# SQLMetadataService, ZookeeperMetadataService
# Note: SQLMetadataService will use your sql_alchemy_conn config in the airflow.cfg file to connect to SQL
metadata_service_type = """ + str(DEFAULT_METADATA_SERVICE_TYPE) + """

# If you're using the ZookeeperMetadataService, this property will identify the zookeeper nodes it will try to connect to
metadata_service_zookeeper_nodes = localhost:2181

# Frequency that the Scheduler Failover Controller polls to see if the scheduler is running (in seconds)
poll_frequency = """ + str(DEFAULT_POLL_FREQUENCY) + """

# Command to use when trying to start a Scheduler instance on a node
airflow_scheduler_start_command = nohup airflow scheduler >> ~/airflow/logs/scheduler.logs &

# Command to use when trying to stop a Scheduler instance on a node
airflow_scheduler_stop_command = for pid in `ps -ef | grep "airflow scheduler" | awk '{print $2}'` \; do kill -9 $pid \; done

# Logging Level. Choices include:
# NOTSET, DEBUG, INFO, WARN, ERROR, CRITICAL
logging_level = """ + str(DEFAULT_LOGGING_LEVEL) + """

# Log Directory Location
logging_dir =  """ + str(DEFAULT_AIRFLOW_HOME_DIR) + """/logs/scheduler_failover/

# Log File Name
logging_file_name = scheduler_failover_controller.log

# When the logs should be rotated.
# Documentation: https://docs.python.org/2/library/logging.handlers.html#logging.handlers.TimedRotatingFileHandler
logs_rotate_when = """ + str(DEFAULT_LOGS_ROTATE_WHEN) + """

# How many times the logs should be rotate before you clear out the old ones
logs_rotate_backup_count = """ + str(DEFAULT_LOGS_ROTATE_BACKUP_COUNT) + """

# Number of times to retry starting up the scheduler before it sends an alert
retry_count_before_alerting = """ + str(DEFAULT_RETRY_COUNT_BEFORE_ALERTING) + """

# Email address to send alerts to if the failover controller is unable to startup a scheduler
alert_to_email = airflow@airflow.com

"""


class Configuration:

    def __init__(self, airflow_home_dir=None, airflow_config_file_path=None):
        if airflow_home_dir is None:
            airflow_home_dir = DEFAULT_AIRFLOW_HOME_DIR
        if airflow_config_file_path is None:
            airflow_config_file_path = airflow_home_dir + "/airflow.cfg"
        self.airflow_home_dir = airflow_home_dir
        self.airflow_config_file_path = airflow_config_file_path

        if not os.path.isfile(airflow_config_file_path):
            print "Cannot find Airflow Configuration file at '" + str(airflow_config_file_path) + "'!!!"
            sys.exit(1)

        self.conf = ConfigParser.RawConfigParser()
        self.conf.read(airflow_config_file_path)

    @staticmethod
    def get_current_host():
        return socket.gethostname()

    def get_airflow_home_dir(self):
        return self.airflow_home_dir

    def get_airflow_config_file_path(self):
        return self.airflow_config_file_path

    def get_config(self, section, option, default=None):
        try:
            config_value = self.conf.get(section, option)
            return config_value if config_value is not None else default
        except:
            pass
        return default

    def get_scheduler_failover_config(self, option, default=None):
        return self.get_config("scheduler_failover", option, default)

    def get_smtp_config(self, option, default=None):
        return self.get_config("smtp", option, default)

    def get_logging_level(self):
        return logging.getLevelName(self.get_scheduler_failover_config("LOGGING_LEVEL", DEFAULT_LOGGING_LEVEL))

    def get_logs_output_file_path(self):
        logging_dir = self.get_scheduler_failover_config("LOGGING_DIR")
        logging_file_name = self.get_scheduler_failover_config("LOGGING_FILE_NAME")
        return logging_dir + logging_file_name if logging_dir is not None and logging_file_name is not None else None

    def get_sql_alchemy_conn(self):
        return self.get_config("core", "SQL_ALCHEMY_CONN")

    def get_metadata_type(self):
        return self.get_scheduler_failover_config("METADATA_SERVICE_TYPE", DEFAULT_METADATA_SERVICE_TYPE)

    def get_metadata_service_zookeeper_nodes(self):
        return self.get_scheduler_failover_config("METADATA_SERVICE_ZOOKEEPER_NODES")

    def get_scheduler_nodes_in_cluster(self):
        scheduler_nodes_in_cluster = self.get_scheduler_failover_config("SCHEDULER_NODES_IN_CLUSTER")
        if scheduler_nodes_in_cluster is not None:
            scheduler_nodes_in_cluster = scheduler_nodes_in_cluster.split(",")
        return scheduler_nodes_in_cluster

    def get_poll_frequency(self):
        return int(self.get_scheduler_failover_config("POLL_FREQUENCY", DEFAULT_POLL_FREQUENCY))

    def get_airflow_scheduler_start_command(self):
        return self.get_scheduler_failover_config("AIRFLOW_SCHEDULER_START_COMMAND")

    def get_airflow_scheduler_stop_command(self):
        return self.get_scheduler_failover_config("AIRFLOW_SCHEDULER_STOP_COMMAND").replace("\\;", ";")

    def get_airflow_smtp_host(self):
        return self.get_smtp_config("SMTP_HOST")

    def get_airflow_smtp_starttls(self):
        return self.get_smtp_config("SMTP_STARTTLS")

    def get_airflow_smtp_ssl(self):
        return self.get_smtp_config("SMTP_SSL")

    def get_airflow_smtp_user(self):
        return self.get_smtp_config("SMTP_USER")

    def get_airflow_smtp_port(self):
        return self.get_smtp_config("SMTP_PORT")

    def get_airflow_smtp_password(self):
        return self.get_smtp_config("SMTP_PASSWORD")

    def get_airflow_smtp_mail_from(self):
        return self.get_smtp_config("SMTP_MAIL_FROM")

    def get_retry_count_before_alerting(self):
        return int(self.get_scheduler_failover_config("RETRY_COUNT_BEFORE_ALERTING", DEFAULT_RETRY_COUNT_BEFORE_ALERTING))

    def get_logs_rotate_when(self):
        return self.get_scheduler_failover_config("LOGS_ROTATE_WHEN", DEFAULT_LOGS_ROTATE_WHEN)

    def get_logs_rotate_backup_count(self):
        return int(self.get_scheduler_failover_config("LOGS_ROTATE_BACKUP_COUNT", DEFAULT_LOGS_ROTATE_BACKUP_COUNT))

    def get_alert_to_email(self):
        return self.get_scheduler_failover_config("ALERT_TO_EMAIL")

    def add_default_scheduler_failover_configs_to_airflow_configs(self):
        with open(self.airflow_config_file_path, 'r') as airflow_config_file:
            if "[scheduler_failover]" not in airflow_config_file.read():
                print "Adding Scheduler Failover configs to Airflow config file..."
                with open(self.airflow_config_file_path, "a") as airflow_config_file_to_append:
                    airflow_config_file_to_append.write(DEFAULT_SCHEDULER_FAILOVER_CONTROLLER_CONFIGS)
                    print "Finished adding Scheduler Failover configs to Airflow config file."
            else:
                print "[scheduler_failover] section already exists. Skipping adding Scheduler Failover configs."
