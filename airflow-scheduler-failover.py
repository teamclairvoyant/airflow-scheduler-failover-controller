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
from sqlalchemy import create_engine, Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker


# Add the following to the airflow.cfg file:
#   [core]
#   nodes_in_cluster = 10.0.0.141,10.0.0.142
#

if "AIRFLOW_HOME" in os.environ:
    AIRFLOW_HOME_DIR = os.environ['AIRFLOW_HOME']
else:
    AIRFLOW_HOME_DIR = os.path.expanduser("~/airflow")
AIRFLOW_CONFIG_FILE_PATH = AIRFLOW_HOME_DIR + "/airflow.cfg"
AIRFLOW_SCHEDULER_START_COMMAND = "sh ~/airflow/bin/restart-scheduler.sh"
AIRFLOW_SCHEDULER_STOP_COMMAND = "sh ~/airflow/bin/shutdown-scheduler.sh"

IS_FAILOVER_CONTROLLER_ACTIVE = False
LOG_INFO = True
POLL_FREQUENCY_SECONDS = 10
TIMESTAMP_FORMAT = "%Y-%m-%d %H:%M:%S"

conf = ConfigParser.RawConfigParser()
conf.read(AIRFLOW_CONFIG_FILE_PATH)
NODES_IN_CLUSTER = conf.get('core', 'NODES_IN_CLUSTER')
if NODES_IN_CLUSTER is not None:
    NODES_IN_CLUSTER = NODES_IN_CLUSTER.split(",")
print "NODES_IN_CLUSTER: " + str(NODES_IN_CLUSTER)
SQL_ALCHEMY_CONN = conf.get('core', 'SQL_ALCHEMY_CONN')
engine_args = {}
engine = create_engine(SQL_ALCHEMY_CONN, **engine_args)
Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

Base = declarative_base()


def get_datetime_as_str(date):
    return date.strftime(TIMESTAMP_FORMAT)


def get_string_as_datetime(date_str):
    return datetime.datetime.strptime(date_str, TIMESTAMP_FORMAT)


def log_info(msg):
    if LOG_INFO:
        log("INFO", msg)


def log_alert(msg):
    log("ALERT", msg)


def log_error(msg):
    log("ERROR", msg)


def log(type, msg):
    print str(datetime.datetime.now()) + " - " + type + " - " + str(msg)



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
    for node in NODES_IN_CLUSTER:
        if node != active_scheduler_node:
            standby_nodes.append(node)
    return standby_nodes


def is_scheduler_running(host):
    log_info("Starting to Check if Scheduler on host '" + host + "' is running...")

    output = run_command_through_ssh(host, "ps -eaf | grep \"airflow scheduler\"")
    active_list = []
    output = output.split('\n')
    for line in output:
        if not re.search(r'\bgrep\b', line):
            active_list.append(line)

    active_list_length = len(filter(None, active_list))

    is_running = active_list_length > 0

    log_info("Finished Checking if Scheduler on host '" + host + "' is running. is_running: " + str(is_running))

    return is_running


def startup_scheduler(host):
    log_info("Starting Scheduler on host '" + host + "'...")
    run_command_through_ssh(host, AIRFLOW_SCHEDULER_START_COMMAND)
    log_info("Finished starting Scheduler on host '" + host + "'")


def shutdown_scheduler(host):
    log_alert("Starting to shutdown Scheduler on host '" + host + "'...")
    run_command_through_ssh(host, AIRFLOW_SCHEDULER_STOP_COMMAND)
    log_alert("Finished shutting down Scheduler on host '" + host + "'")


def search_for_active_scheduler_node():
    active_scheduler_node = None
    log_info("Starting to search for current Active Scheduler Node...")
    nodes_with_scheduler_running = []
    for node in NODES_IN_CLUSTER:
        if is_scheduler_running(node):
            nodes_with_scheduler_running.append(node)
    log_info("Nodes with a scheduler currently running on it: '" + str(nodes_with_scheduler_running) + "'")
    if len(nodes_with_scheduler_running) > 1:
        log_alert("Multiple nodes have a scheduler running on it. Shutting down all except the first one.")
        for index, host in enumerate(nodes_with_scheduler_running):
            if index != 0:
                shutdown_scheduler(host)
            else:
                active_scheduler_node = host
    elif len(nodes_with_scheduler_running) == 1:
        log_info("Found one node with a Scheduler running on it")
        active_scheduler_node = nodes_with_scheduler_running[0]
    else:
        log_info("Nodes do not have a Scheduler running on them. Using Default Leader.")
        active_scheduler_node = NODES_IN_CLUSTER[0]

    log_info("Finished searching for Active Scheduler Node: '" + active_scheduler_node + "'")
    return active_scheduler_node


def set_this_failover_controller_as_active():
    log_alert("Setting this Failover Node to ACTIVE")
    global IS_FAILOVER_CONTROLLER_ACTIVE
    current_host = get_current_host()

    try:
        SchedulerFailoverKeyValue.set_active_failover_node(current_host)
        SchedulerFailoverKeyValue.set_failover_heartbeat()
        IS_FAILOVER_CONTROLLER_ACTIVE = True
        log_alert("This Failover Controller is now ACTIVE.")
    except Exception, e:
        IS_FAILOVER_CONTROLLER_ACTIVE = False
        log_error("Failed to set Failover Controller as ACTIVE. Trying again next heart beat.")
        traceback.print_exc(file=sys.stdout)


def set_this_failover_controller_as_inactive():
    log_alert("Setting this Failover Node to INACTIVE")
    global IS_FAILOVER_CONTROLLER_ACTIVE
    IS_FAILOVER_CONTROLLER_ACTIVE = False


def main():

    # IS_FAILOVER_CONTROLLER_ACTIVE is always set to False in the beginning
    global IS_FAILOVER_CONTROLLER_ACTIVE
    current_host = get_current_host()
    log_info("Current Host: " + current_host)

    # Creating metadata table. If the creation fails assume it was created already.
    try:
        log_info("Creating Metadata Table")
        Base.metadata.create_all(engine)
    except Exception, e:
        log_info("Exception while Creating Metadata Table: " + str(e))
        log_info("Table might already exist. Suppressing Exception.")

    # Infinite while loop for polling with a sleep for X seconds.
    while 1:
        log_info("--------------------------------------")
        log_info("Started Polling...")

        active_failover_node = SchedulerFailoverKeyValue.get_active_failover_node()
        active_scheduler_node = SchedulerFailoverKeyValue.get_active_scheduler_node()
        last_failover_heartbeat = SchedulerFailoverKeyValue.get_failover_heartbeat()
        log_info("Active Failover Node: " + active_failover_node)
        log_info("Active Scheduler Node: " + active_scheduler_node)
        log_info("Last Failover Heartbeat: " + str(last_failover_heartbeat) + ". Current time: " + get_datetime_as_str(datetime.datetime.now()) + ".")

        # if the current controller instance is not active, then execute this statement
        if not IS_FAILOVER_CONTROLLER_ACTIVE:
            log_info("This Failover Controller is on Standby.")

            if active_failover_node is not None:
                log_info("There already is an active Failover Controller '" + active_failover_node + "'")
                if active_failover_node == current_host:
                    log_alert("Discovered this Failover Controller should be active because Active Failover Node is this nodes host")
                    set_this_failover_controller_as_active()

                elif last_failover_heartbeat is None:
                    log_alert("Last Failover Heartbeat is None")
                    set_this_failover_controller_as_active()
                    active_failover_node = SchedulerFailoverKeyValue.get_active_failover_node()
                else:
                    failover_heartbeat_diff = (datetime.datetime.now() - last_failover_heartbeat).seconds
                    max_age = POLL_FREQUENCY_SECONDS * 2
                    if failover_heartbeat_diff > max_age:
                        log_alert("Failover Heartbeat '" + get_datetime_as_str(last_failover_heartbeat) + "' for Active Failover controller '" + str(active_failover_node) + "' is older then max age of " + str(max_age) + " seconds")
                        set_this_failover_controller_as_active()
                        active_failover_node = SchedulerFailoverKeyValue.get_active_failover_node()
            else:  # if the the
                log_alert("Failover Node is None")
                set_this_failover_controller_as_active()
                active_failover_node = SchedulerFailoverKeyValue.get_active_failover_node()

        if IS_FAILOVER_CONTROLLER_ACTIVE:

            # Check to make sure this Failover Controller
            if active_failover_node != current_host:
                log_alert("Discovered this Failover Controller should not be active because Active Failover Node is not this nodes host")
                set_this_failover_controller_as_inactive()

            else:
                log_info("Setting Failover Heartbeat")
                SchedulerFailoverKeyValue.set_failover_heartbeat()

                if active_scheduler_node is None:
                    log_alert("Active Scheduler is None")
                    active_scheduler_node = search_for_active_scheduler_node()
                    SchedulerFailoverKeyValue.set_active_scheduler_node(active_scheduler_node)

                if not is_scheduler_running(active_scheduler_node):
                    log_alert("Scheduler is not running on Active Scheduler Node '" + active_scheduler_node + "'")
                    startup_scheduler(active_scheduler_node)
                    if not is_scheduler_running(active_scheduler_node):
                        log_alert("Failed to restart Scheduler on Active Scheduler Node '" + active_scheduler_node + "'")
                        log_alert("Starting to search for a new Active Scheduler Node")
                        is_successful = False
                        for standby_node in get_standby_nodes(active_scheduler_node):
                            log_alert("Trying to startup Scheduler on standby node '" + standby_node + "'")
                            startup_scheduler(standby_node)
                            if is_scheduler_running(standby_node):
                                is_successful = True
                                active_scheduler_node = standby_node
                                SchedulerFailoverKeyValue.set_active_scheduler_node(active_scheduler_node)
                                log_alert("New Active Scheduler Node is set to '" + active_scheduler_node + "'")
                                break
                        if not is_successful:
                            log_error("Tried to start up a Scheduler on a standby but all failed. Retrying on next polling.")
                        log_alert("Finished search for a new Active Scheduler Node")
                    else:
                        log_alert("Confirmed the Scheduler is now running")
                else:
                    log_info("Checking if scheduler instances are running on standby nodes...")
                    for standby_node in get_standby_nodes(active_scheduler_node):
                        if is_scheduler_running(standby_node):
                            log_alert("There is a Scheduler running on a standby node '" + standby_node + "'. Shutting Down that Scheduler.")
                            shutdown_scheduler(standby_node)
                    log_info("Finished checking if scheduler instances are running on standby nodes")

        log_info("Finished Polling")

        time.sleep(POLL_FREQUENCY_SECONDS)

if __name__ == '__main__':
    args = sys.argv[1:]
    if "test_connection" in args:
        for host in NODES_IN_CLUSTER:
            log_info("Testing Connection for host '" + host + "'")
            log_info(run_command_through_ssh(host, "echo 'Connection Succeeded'"))
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

