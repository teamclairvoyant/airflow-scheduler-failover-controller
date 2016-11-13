#!/usr/bin/python
import sys
import traceback
import time
import ConfigParser
import re
import os
import datetime
import subprocess
import socket
import logging
import logging.handlers
from sqlalchemy import create_engine, Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

# todo: break this into multiple files for a cleaner setup
# todo: better deployment and run method

# Add the following to the airflow.cfg file:
#   [scheduler_failover]
#
#   # List of potential nodes that can act as Schedulers (Comma Separated List)
#   scheduler_nodes_in_cluster = 10.0.0.141,10.0.0.142
#

# GLOBAL VARIABLES
AIRFLOW_HOME_DIR = os.environ['AIRFLOW_HOME'] if "AIRFLOW_HOME" in os.environ else os.path.expanduser("~/airflow")
AIRFLOW_CONFIG_FILE_PATH = AIRFLOW_HOME_DIR + "/airflow.cfg"

IS_FAILOVER_CONTROLLER_ACTIVE = False  # always False - DO NOT CHANGE
POLL_FREQUENCY_SECONDS = 10  # Set polling frequency here
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"
AIRFLOW_SCHEDULER_START_COMMAND = """
echo "Starting Up Scheduler"
nohup airflow scheduler >> ~/airflow/logs/scheduler.logs &
"""
AIRFLOW_SCHEDULER_STOP_COMMAND = """
echo "Shutting Down Scheduler"
for pid in `ps -ef | grep "airflow scheduler" | awk '{print $2}'` ; do kill -9 $pid ; done
"""
LOGS_DIR = AIRFLOW_HOME_DIR + "/logs/scheduler-failover/"
LOGS_FILE_NAME = "scheduler-failover-controller.log"
LOGGING_LEVEL = logging.INFO

# Creating location where logs are placed if it doesn't exist
if not os.path.exists(os.path.expanduser(LOGS_DIR)):
    os.makedirs(os.path.expanduser(LOGS_DIR))
# Create the logger
logger = logging.getLogger(__name__)
# Set logging level
logger.setLevel(LOGGING_LEVEL)
# Create logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# Create the stream handler to log messages to the console
streamHandler = logging.StreamHandler()
streamHandler.setLevel(logging.DEBUG)
streamHandler.setFormatter(formatter)
logger.addHandler(streamHandler)
# Create the file handler to log messages to a log file
fileHandler = logging.handlers.TimedRotatingFileHandler(filename=os.path.expanduser(LOGS_DIR + LOGS_FILE_NAME), when="midnight", backupCount=7)
fileHandler.setLevel(logging.DEBUG)
fileHandler.setFormatter(formatter)
logger.addHandler(fileHandler)

# Checking if all configuration and script files exists.
if not os.path.isfile(AIRFLOW_CONFIG_FILE_PATH):
    logger.error("Airflow config file missing")
    sys.exit(1)

conf = ConfigParser.RawConfigParser()
conf.read(AIRFLOW_CONFIG_FILE_PATH)
SCHEDULER_NODES_IN_CLUSTER = conf.get('scheduler_failover', 'SCHEDULER_NODES_IN_CLUSTER')
if SCHEDULER_NODES_IN_CLUSTER is not None:
    SCHEDULER_NODES_IN_CLUSTER = SCHEDULER_NODES_IN_CLUSTER.split(",")
print "SCHEDULER_NODES_IN_CLUSTER: " + str(SCHEDULER_NODES_IN_CLUSTER)
SQL_ALCHEMY_CONN = conf.get('core', 'SQL_ALCHEMY_CONN')
engine_args = {}
engine = create_engine(SQL_ALCHEMY_CONN, **engine_args)
Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

Base = declarative_base()


def get_datetime_as_str(date):
    return date.strftime(TIMESTAMP_FORMAT)


def get_string_as_datetime(date_str):
    return datetime.datetime.strptime(date_str, TIMESTAMP_FORMAT)


# todo: provide an implementation which uses Zookeeper instead of the metastore
class SchedulerFailoverKeyValue(Base):
    __tablename__ = 'scheduler_failover'
    key = Column(String(200), primary_key=True)
    value = Column(String(200), nullable=False)

    @classmethod
    def get_failover_heartbeat(cls):
        session = Session()
        entry = session.query(cls).filter(cls.key == "failover_heartbeat").first()
        if entry is not None:
            heart_beat_date_str = entry.value
            heart_beat_date = get_string_as_datetime(heart_beat_date_str)
        else:
            heart_beat_date = None
        session.close()
        return heart_beat_date

    @classmethod
    def set_failover_heartbeat(cls):
        session = Session()
        heart_beat_date_str = get_datetime_as_str(datetime.datetime.now())  # get current datetime as string
        entry = session.query(cls).filter(cls.key == "failover_heartbeat").first()
        if entry is not None:
            entry.value = heart_beat_date_str
        else:
            session.add(SchedulerFailoverKeyValue(key="failover_heartbeat", value=heart_beat_date_str))
        session.commit()
        session.close()

    @classmethod
    def get_active_failover_node(cls):
        session = Session()
        entry = session.query(cls).filter(cls.key == "active_failover_node").first()
        session.close()
        if entry is not None:
            return entry.value
        else:
            return None

    @classmethod
    def set_active_failover_node(cls, node):
        session = Session()
        entry = session.query(cls).filter(cls.key == "active_failover_node").first()
        if entry is not None:
            entry.value = node
        else:
            session.add(SchedulerFailoverKeyValue(key="active_failover_node", value=node))
        session.commit()
        session.close()

    @classmethod
    def get_active_scheduler_node(cls):
        session = Session()
        entry = session.query(cls).filter(cls.key == "active_scheduler_node").first()
        session.close()
        if entry is not None:
            return entry.value
        else:
            return None

    @classmethod
    def set_active_scheduler_node(cls, node):
        session = Session()
        entry = session.query(cls).filter(cls.key == "active_scheduler_node").first()
        if entry is not None:
            entry.value = node
        else:
            session.add(SchedulerFailoverKeyValue(key="active_scheduler_node", value=node))
        session.commit()
        session.close()

    @classmethod
    def truncate(cls):
        session = Session()
        session.query(cls).delete()
        session.commit()
        session.close()


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


def get_current_host():
    return socket.gethostname()


def get_standby_nodes(active_scheduler_node):
    standby_nodes = []
    for node in SCHEDULER_NODES_IN_CLUSTER:
        if node != active_scheduler_node:
            standby_nodes.append(node)
    return standby_nodes


def is_scheduler_running(host):
    logger.info("Starting to Check if Scheduler on host '" + str(host) + "' is running...")

    output = run_command_through_ssh(host, "ps -eaf | grep 'airflow scheduler'")
    active_list = []
    output = output.split('\n')
    for line in output:
        if not re.search(r'\bgrep\b', line):
            active_list.append(line)

    active_list_length = len(filter(None, active_list))

    # todo: If there's more then one scheduler running this should kill off the other schedulers

    is_running = active_list_length > 0

    logger.info("Finished Checking if Scheduler on host '" + str(host) + "' is running. is_running: " + str(is_running))

    return is_running


def startup_scheduler(host):
    logger.info("Starting Scheduler on host '" + str(host) + "'...")
    run_command_through_ssh(host, AIRFLOW_SCHEDULER_START_COMMAND)
    logger.info("Finished starting Scheduler on host '" + str(host) + "'")


def shutdown_scheduler(host):
    logger.critical("Starting to shutdown Scheduler on host '" + host + "'...")
    run_command_through_ssh(host, AIRFLOW_SCHEDULER_STOP_COMMAND)
    logger.critical("Finished shutting down Scheduler on host '" + host + "'")


def search_for_active_scheduler_node():
    active_scheduler_node = None
    logger.info("Starting to search for current Active Scheduler Node...")
    nodes_with_scheduler_running = []
    for node in SCHEDULER_NODES_IN_CLUSTER:
        if is_scheduler_running(node):
            nodes_with_scheduler_running.append(node)
    logger.info("Nodes with a scheduler currently running on it: '" + str(nodes_with_scheduler_running) + "'")
    if len(nodes_with_scheduler_running) > 1:
        logger.critical("Multiple nodes have a scheduler running on it. Shutting down all except the first one.")
        for index, host in enumerate(nodes_with_scheduler_running):
            if index != 0:
                shutdown_scheduler(host)
            else:
                active_scheduler_node = host
    elif len(nodes_with_scheduler_running) == 1:
        logger.info("Found one node with a Scheduler running on it")
        active_scheduler_node = nodes_with_scheduler_running[0]
    else:
        logger.info("Nodes do not have a Scheduler running on them. Using Default Leader.")
        active_scheduler_node = SCHEDULER_NODES_IN_CLUSTER[0]

    logger.info("Finished searching for Active Scheduler Node: '" + str(active_scheduler_node) + "'")
    return active_scheduler_node


def set_this_failover_controller_as_active():
    logger.critical("Setting this Failover Node to ACTIVE")
    global IS_FAILOVER_CONTROLLER_ACTIVE
    current_host = get_current_host()

    try:
        SchedulerFailoverKeyValue.set_active_failover_node(current_host)
        SchedulerFailoverKeyValue.set_failover_heartbeat()
        IS_FAILOVER_CONTROLLER_ACTIVE = True
        logger.critical("This Failover Controller is now ACTIVE.")
    except Exception, e:
        IS_FAILOVER_CONTROLLER_ACTIVE = False
        logger.error("Failed to set Failover Controller as ACTIVE. Trying again next heart beat.")
        traceback.print_exc(file=sys.stdout)


def set_this_failover_controller_as_inactive():
    logger.critical("Setting this Failover Node to INACTIVE")
    global IS_FAILOVER_CONTROLLER_ACTIVE
    IS_FAILOVER_CONTROLLER_ACTIVE = False


def main():

    logger.info("Scheduler Failover Controller Starting Up!")

    # IS_FAILOVER_CONTROLLER_ACTIVE is always set to False in the beginning
    global IS_FAILOVER_CONTROLLER_ACTIVE
    current_host = get_current_host()
    logger.info("Current Host: " + str(current_host))

    # Creating metadata table. If the creation fails assume it was created already.
    try:
        logger.info("Creating Metadata Table")
        Base.metadata.create_all(engine)
    except Exception, e:
        logger.info("Exception while Creating Metadata Table: " + str(e))
        logger.info("Table might already exist. Suppressing Exception.")

    # Infinite while loop for polling with a sleep for X seconds.
    while 1:
        logger.info("--------------------------------------")
        logger.info("Started Polling...")

        active_failover_node = SchedulerFailoverKeyValue.get_active_failover_node()
        active_scheduler_node = SchedulerFailoverKeyValue.get_active_scheduler_node()
        last_failover_heartbeat = SchedulerFailoverKeyValue.get_failover_heartbeat()
        logger.info("Active Failover Node: " + str(active_failover_node))
        logger.info("Active Scheduler Node: " + str(active_scheduler_node))
        logger.info("Last Failover Heartbeat: " + str(last_failover_heartbeat) + ". Current time: " + get_datetime_as_str(datetime.datetime.now()) + ".")

        # if the current controller instance is not active, then execute this statement
        if not IS_FAILOVER_CONTROLLER_ACTIVE:
            logger.info("This Failover Controller is on Standby.")

            if active_failover_node is not None:
                logger.info("There already is an active Failover Controller '" + str(active_failover_node)+ "'")
                if active_failover_node == current_host:
                    logger.critical("Discovered this Failover Controller should be active because Active Failover Node is this nodes host")
                    set_this_failover_controller_as_active()

                elif last_failover_heartbeat is None:
                    logger.critical("Last Failover Heartbeat is None")
                    set_this_failover_controller_as_active()
                    active_failover_node = SchedulerFailoverKeyValue.get_active_failover_node()
                else:
                    failover_heartbeat_diff = (datetime.datetime.now() - last_failover_heartbeat).seconds
                    max_age = POLL_FREQUENCY_SECONDS * 2
                    if failover_heartbeat_diff > max_age:
                        logger.critical("Failover Heartbeat '" + get_datetime_as_str(last_failover_heartbeat) + "' for Active Failover controller '" + str(active_failover_node) + "' is older then max age of " + str(max_age) + " seconds")
                        set_this_failover_controller_as_active()
                        active_failover_node = SchedulerFailoverKeyValue.get_active_failover_node()
            else:  # if the the
                logger.critical("Failover Node is None")
                set_this_failover_controller_as_active()
                active_failover_node = SchedulerFailoverKeyValue.get_active_failover_node()

        if IS_FAILOVER_CONTROLLER_ACTIVE:

            logger.info("This Failover Controller is ACTIVE")

            # Check to make sure this Failover Controller
            if active_failover_node != current_host:
                logger.critical("Discovered this Failover Controller should not be active because Active Failover Node is not this nodes host")
                set_this_failover_controller_as_inactive()

            else:
                logger.info("Setting Failover Heartbeat")
                SchedulerFailoverKeyValue.set_failover_heartbeat()

                if active_scheduler_node is None:
                    logger.critical("Active Scheduler is None")
                    active_scheduler_node = search_for_active_scheduler_node()
                    SchedulerFailoverKeyValue.set_active_scheduler_node(active_scheduler_node)

                if not is_scheduler_running(active_scheduler_node):
                    logger.critical("Scheduler is not running on Active Scheduler Node '" + str(active_scheduler_node) + "'")
                    startup_scheduler(active_scheduler_node)
                    logger.info("Pausing for 2 seconds to allow the Scheduler to Start")
                    time.sleep(2)
                    if not is_scheduler_running(active_scheduler_node):
                        logger.critical("Failed to restart Scheduler on Active Scheduler Node '" +str(active_scheduler_node) + "'")
                        logger.critical("Starting to search for a new Active Scheduler Node")
                        is_successful = False
                        for standby_node in get_standby_nodes(active_scheduler_node):
                            logger.critical("Trying to startup Scheduler on STANDBY node '" + str(standby_node) + "'")
                            startup_scheduler(standby_node)
                            if is_scheduler_running(standby_node):
                                is_successful = True
                                active_scheduler_node = standby_node
                                SchedulerFailoverKeyValue.set_active_scheduler_node(active_scheduler_node)
                                logger.critical("New Active Scheduler Node is set to '" + active_scheduler_node + "'")
                                break
                        if not is_successful:
                            logger.error("Tried to start up a Scheduler on a STANDBY but all failed. Retrying on next polling.")
                            # todo: this should send out an email alert to let people know that the failover controller couldn't startup a scheduler
                        logger.critical("Finished search for a new Active Scheduler Node")
                    else:
                        logger.critical("Confirmed the Scheduler is now running")
                else:
                    logger.info("Checking if scheduler instances are running on STANDBY nodes...")
                    for standby_node in get_standby_nodes(active_scheduler_node):
                        if is_scheduler_running(standby_node):
                            logger.critical("There is a Scheduler running on a STANDBY node '" + standby_node + "'. Shutting Down that Scheduler.")
                            shutdown_scheduler(standby_node)
                    logger.info("Finished checking if scheduler instances are running on STANDBY nodes")
        else:
            logger.info("This Failover Controller on STANDBY")

        logger.info("Finished Polling. Sleeping for " + str(POLL_FREQUENCY_SECONDS) + " seconds")

        time.sleep(POLL_FREQUENCY_SECONDS)

    # should not get to this point
    logger.info("Scheduler Failover Controller Finished")

if __name__ == '__main__':
    args = sys.argv[1:]
    if "test_connection" in args:
        for host in SCHEDULER_NODES_IN_CLUSTER:
            logger.info("Testing Connection for host '" + str(host) + "'")
            logger.info(run_command_through_ssh(host, "echo 'Connection Succeeded'"))
    if "is_scheduler_running" in args:
        for host in SCHEDULER_NODES_IN_CLUSTER:
            logger.info("Testing to see if scheduler is running on host '" + str(host) + "'")
            logger.info(is_scheduler_running(host))
    elif "clear" in args:
        SchedulerFailoverKeyValue.truncate()
    elif "metadata" in args:
        print "Printing Metadata: "
        print "=============================="
        print "current_host: " + str(get_current_host())
        print "active_failover_node: " + str(SchedulerFailoverKeyValue.get_active_failover_node())
        print "active_scheduler_node: " + str(SchedulerFailoverKeyValue.get_active_scheduler_node())
        print "last_failover_heartbeat: " + str(SchedulerFailoverKeyValue.get_failover_heartbeat())
    else:
        try:
            main()
        except KeyboardInterrupt:
            print >> sys.stderr, '\nExiting by user request.\n'
            sys.exit(0)

