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
DEFAULT_ALERT_EMAIL_SUBJECT = "Airflow Alert - Scheduler Failover Controller Failed to Startup Scheduler"
DEFAULT_SCHEDULER_START_COMMAND = "nohup airflow scheduler >> ~/airflow/logs/scheduler.logs &"
DEFAULT_SCHEDULER_STOP_COMMAND = "for pid in `ps -ef | grep \"airflow scheduler\" | awk '{print $2}'` \; do kill -9 $pid \; done"


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
	config_value, env_var_value = None, None

        try:
            config_value = self.conf.get(section, option)
        except:
            pass
	try:
	    env_var_value = os.environ["AIRFLOW__" + section.upper() + "__" + option.upper()]
	except:
	    pass

	if config_value:
	    return config_value
	elif env_var_value:
	    return env_var_value
	else:
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
	return self.get_scheduler_failover_config("SCHEDULER_START_COMMAND", DEFAULT_SCHEDULER_START_COMMAND)

    def get_airflow_scheduler_stop_command(self):
        return self.get_scheduler_failover_config("SCHEDULER_STOP_COMMAND", DEFAULT_SCHEDULER_STOP_COMMAND)

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

    def get_alert_email_subject(self):
        return self.get_scheduler_failover_config("ALERT_EMAIL_SUBJECT", DEFAULT_ALERT_EMAIL_SUBJECT)

    def add_default_scheduler_failover_configs_to_airflow_configs(self):
        with open(self.airflow_config_file_path, 'r') as airflow_config_file:
            if "[scheduler_failover]" not in airflow_config_file.read():
                print "Adding Scheduler Failover configs to Airflow config file..."
                with open(self.airflow_config_file_path, "a") as airflow_config_file_to_append:
                    airflow_config_file_to_append.write(DEFAULT_SCHEDULER_FAILOVER_CONTROLLER_CONFIGS)
                    print "Finished adding Scheduler Failover configs to Airflow config file."
            else:
                print "[scheduler_failover] section already exists. Skipping adding Scheduler Failover configs."
