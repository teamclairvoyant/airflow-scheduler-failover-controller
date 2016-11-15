from scheduler_failover_controller.utils import ssh_utils
from scheduler_failover_controller.utils.date_utils import get_datetime_as_str
import sys
import traceback
import re
import socket
import datetime
import time
from scheduler_failover_controller.utils.ssh_utils import SSHUtils


class FailoverController:

    IS_FAILOVER_CONTROLLER_ACTIVE = False

    def __init__(self, scheduler_nodes_in_cluster, airflow_scheduler_start_command, airflow_scheduler_stop_command, poll_frequency, metadata_service, logger):
        self.scheduler_nodes_in_cluster = scheduler_nodes_in_cluster
        self.airflow_scheduler_start_command = airflow_scheduler_start_command
        self.airflow_scheduler_stop_command = airflow_scheduler_stop_command
        self.poll_frequency = poll_frequency
        self.metadata_service = metadata_service
        self.logger = logger

        self.current_host = self.get_current_host()

        self.ssh_utils = SSHUtils(logger)

    def poll(self):
        self.logger.info("--------------------------------------")
        self.logger.info("Started Polling...")

        active_failover_node = self.metadata_service.get_active_failover_node()
        active_scheduler_node = self.metadata_service.get_active_scheduler_node()
        last_failover_heartbeat = self.metadata_service.get_failover_heartbeat()
        self.logger.info("Active Failover Node: " + str(active_failover_node))
        self.logger.info("Active Scheduler Node: " + str(active_scheduler_node))
        self.logger.info("Last Failover Heartbeat: " + str(last_failover_heartbeat) + ". Current time: " + get_datetime_as_str() + ".")

        # if the current controller instance is not active, then execute this statement
        if not self.IS_FAILOVER_CONTROLLER_ACTIVE:
            self.logger.info("This Failover Controller is on Standby.")

            if active_failover_node is not None:
                self.logger.info("There already is an active Failover Controller '" + str(active_failover_node)+ "'")
                if active_failover_node == self.current_host:
                    self.logger.critical("Discovered this Failover Controller should be active because Active Failover Node is this nodes host")
                    self.set_this_failover_controller_as_active()

                elif last_failover_heartbeat is None:
                    self.logger.critical("Last Failover Heartbeat is None")
                    self.set_this_failover_controller_as_active()
                    active_failover_node = self.metadata_service.get_active_failover_node()
                else:
                    failover_heartbeat_diff = (datetime.datetime.now() - last_failover_heartbeat).seconds
                    max_age = self.poll_frequency * 2
                    if failover_heartbeat_diff > max_age:
                        self.logger.critical("Failover Heartbeat '" + get_datetime_as_str(last_failover_heartbeat) + "' for Active Failover controller '" + str(active_failover_node) + "' is older then max age of " + str(max_age) + " seconds")
                        self.set_this_failover_controller_as_active()
                        active_failover_node = self.metadata_service.get_active_failover_node()
            else:  # if the the
                self.logger.critical("Failover Node is None")
                self.set_this_failover_controller_as_active()
                active_failover_node = self.metadata_service.get_active_failover_node()

        if self.IS_FAILOVER_CONTROLLER_ACTIVE:

            self.logger.info("This Failover Controller is ACTIVE")

            # Check to make sure this Failover Controller
            if active_failover_node != self.current_host:
                self.logger.critical("Discovered this Failover Controller should not be active because Active Failover Node is not this nodes host")
                self.set_this_failover_controller_as_inactive()

            else:
                self.logger.info("Setting Failover Heartbeat")
                self.metadata_service.set_failover_heartbeat()

                if active_scheduler_node is None:
                    self.logger.critical("Active Scheduler is None")
                    active_scheduler_node = self.search_for_active_scheduler_node()
                    self.metadata_service.set_active_scheduler_node(active_scheduler_node)

                if not self.is_scheduler_running(active_scheduler_node):
                    self.logger.critical("Scheduler is not running on Active Scheduler Node '" + str(active_scheduler_node) + "'")
                    self.startup_scheduler(active_scheduler_node)
                    self.logger.info("Pausing for 2 seconds to allow the Scheduler to Start")
                    time.sleep(2)
                    if not self.is_scheduler_running(active_scheduler_node):
                        self.logger.critical("Failed to restart Scheduler on Active Scheduler Node '" +str(active_scheduler_node) + "'")
                        self.logger.critical("Starting to search for a new Active Scheduler Node")
                        is_successful = False
                        for standby_node in self.get_standby_nodes(active_scheduler_node):
                            self.logger.critical("Trying to startup Scheduler on STANDBY node '" + str(standby_node) + "'")
                            self.startup_scheduler(standby_node)
                            if self.is_scheduler_running(standby_node):
                                is_successful = True
                                active_scheduler_node = standby_node
                                self.metadata_service.set_active_scheduler_node(active_scheduler_node)
                                self.logger.critical("New Active Scheduler Node is set to '" + active_scheduler_node + "'")
                                break
                        if not is_successful:
                            self.logger.error("Tried to start up a Scheduler on a STANDBY but all failed. Retrying on next polling.")
                            # todo: this should send out an email alert to let people know that the failover controller couldn't startup a scheduler
                        self.logger.critical("Finished search for a new Active Scheduler Node")
                    else:
                        self.logger.critical("Confirmed the Scheduler is now running")
                else:
                    self.logger.info("Checking if scheduler instances are running on STANDBY nodes...")
                    for standby_node in self.get_standby_nodes(active_scheduler_node):
                        if self.is_scheduler_running(standby_node):
                            self.logger.critical("There is a Scheduler running on a STANDBY node '" + standby_node + "'. Shutting Down that Scheduler.")
                            self.shutdown_scheduler(standby_node)
                    self.logger.info("Finished checking if scheduler instances are running on STANDBY nodes")
        else:
            self.logger.info("This Failover Controller on STANDBY")

    @staticmethod
    def get_current_host():
        return socket.gethostname()

    def get_standby_nodes(self, active_scheduler_node):
        standby_nodes = []
        for node in self.scheduler_nodes_in_cluster:
            if node != active_scheduler_node:
                standby_nodes.append(node)
        return standby_nodes

    def is_scheduler_running(self, host):
        self.logger.info("Starting to Check if Scheduler on host '" + str(host) + "' is running...")

        is_running = False
        output = self.ssh_utils.run_command_through_ssh(host, "ps -eaf | grep 'airflow scheduler'")
        if output is not None:
            active_list = []
            output = output.split('\n')
            for line in output:
                if not re.search(r'grep', line):
                    active_list.append(line)

            active_list_length = len(filter(None, active_list))

            # todo: If there's more then one scheduler running this should kill off the other schedulers

            is_running = active_list_length > 0
        else:
            self.logger.critical("SSH Run command returned None.")

        self.logger.info("Finished Checking if Scheduler on host '" + str(host) + "' is running. is_running: " + str(is_running))

        return is_running

    def startup_scheduler(self, host):
        self.logger.info("Starting Scheduler on host '" + str(host) + "'...")
        self.ssh_utils.run_command_through_ssh(host, self.airflow_scheduler_start_command)
        self.logger.info("Finished starting Scheduler on host '" + str(host) + "'")

    def shutdown_scheduler(self, host):
        self.logger.critical("Starting to shutdown Scheduler on host '" + host + "'...")
        self.ssh_utils.run_command_through_ssh(host, self.airflow_scheduler_stop_command)
        self.logger.critical("Finished shutting down Scheduler on host '" + host + "'")

    def search_for_active_scheduler_node(self):
        active_scheduler_node = None
        self.logger.info("Starting to search for current Active Scheduler Node...")
        nodes_with_scheduler_running = []
        for node in self.scheduler_nodes_in_cluster:
            if self.is_scheduler_running(node):
                nodes_with_scheduler_running.append(node)
        self.logger.info("Nodes with a scheduler currently running on it: '" + str(nodes_with_scheduler_running) + "'")
        if len(nodes_with_scheduler_running) > 1:
            self.logger.critical("Multiple nodes have a scheduler running on it. Shutting down all except the first one.")
            for index, host in enumerate(nodes_with_scheduler_running):
                if index != 0:
                    self.shutdown_scheduler(host)
                else:
                    active_scheduler_node = host
        elif len(nodes_with_scheduler_running) == 1:
            self.logger.info("Found one node with a Scheduler running on it")
            active_scheduler_node = nodes_with_scheduler_running[0]
        else:
            self.logger.info("Nodes do not have a Scheduler running on them. Using Default Leader.")
            active_scheduler_node = self.scheduler_nodes_in_cluster[0]

        self.logger.info("Finished searching for Active Scheduler Node: '" + str(active_scheduler_node) + "'")
        return active_scheduler_node

    def set_this_failover_controller_as_active(self):
        self.logger.critical("Setting this Failover Node to ACTIVE")
        current_host = self.get_current_host()
        try:
            self.metadata_service.set_active_failover_node(current_host)
            self.metadata_service.set_failover_heartbeat()
            self.IS_FAILOVER_CONTROLLER_ACTIVE = True
            self.logger.critical("This Failover Controller is now ACTIVE.")
        except Exception, e:
            self.IS_FAILOVER_CONTROLLER_ACTIVE = False
            self.logger.error("Failed to set Failover Controller as ACTIVE. Trying again next heart beat.")
            traceback.print_exc(file=sys.stdout)

    def set_this_failover_controller_as_inactive(self):
        self.logger.critical("Setting this Failover Node to INACTIVE")
        self.IS_FAILOVER_CONTROLLER_ACTIVE = False
