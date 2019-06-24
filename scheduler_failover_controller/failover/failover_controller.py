from scheduler_failover_controller.utils.date_utils import get_datetime_as_str
import sys
import traceback
import datetime
import time


class FailoverController:

    IS_FAILOVER_CONTROLLER_ACTIVE = False   # Set to False in the beginning and then set to True as it becomes active
    RETRY_COUNT = 0                         # Set to 0 in the beginning and then is incremented
    LATEST_FAILED_STATUS_MESSAGE = None
    LATEST_FAILED_START_MESSAGE = None
    LATEST_FAILED_SHUTDOWN_MESSAGE = None

    def __init__(self, configuration, command_runner, metadata_service, emailer, logger):
        logger.debug("Creating CommandRunner with Args - configuration: {configuration}, command_runner: {command_runner}, metadata_service: {metadata_service}, emailer: {emailer}, logger: {logger}".format(**locals()))
        self.current_host = configuration.get_current_host()
        self.scheduler_nodes_in_cluster = configuration.get_scheduler_nodes_in_cluster()
        self.airflow_scheduler_start_command = configuration.get_airflow_scheduler_start_command()
        self.airflow_scheduler_stop_command = configuration.get_airflow_scheduler_stop_command()
        self.poll_frequency = configuration.get_poll_frequency()
        self.retry_count_before_alerting = configuration.get_retry_count_before_alerting()
        self.command_runner = command_runner
        self.metadata_service = metadata_service
        self.emailer = emailer
        self.logger = logger

    def poll(self):
        self.logger.info("--------------------------------------")
        self.logger.info("Started Polling...")

        active_failover_node = self.metadata_service.get_active_failover_node()
        active_scheduler_node = self.metadata_service.get_active_scheduler_node()
        last_failover_heartbeat = self.metadata_service.get_failover_heartbeat()
        current_time = datetime.datetime.now()
        self.logger.info("Active Failover Node: " + str(active_failover_node))
        self.logger.info("Active Scheduler Node: " + str(active_scheduler_node))
        self.logger.info("Last Failover Heartbeat: " + str(last_failover_heartbeat) + ". Current time: " + str(current_time) + ".")

        # if the current controller instance is not active, then execute this statement
        if not self.IS_FAILOVER_CONTROLLER_ACTIVE:
            self.logger.info("This Failover Controller is on Standby.")

            if active_failover_node is not None:
                self.logger.info("There already is an active Failover Controller '" + str(active_failover_node)+ "'")
                if active_failover_node == self.current_host:
                    self.logger.warning("Discovered this Failover Controller should be active because Active Failover Node is this nodes host")
                    self.set_this_failover_controller_as_active()

                elif last_failover_heartbeat is None:
                    self.logger.warning("Last Failover Heartbeat is None")
                    self.set_this_failover_controller_as_active()
                    active_failover_node = self.metadata_service.get_active_failover_node()
                else:
                    failover_heartbeat_diff = (current_time - last_failover_heartbeat).seconds
                    self.logger.debug("Failover Heartbeat Difference: " + str(failover_heartbeat_diff) + " seconds")
                    max_age = self.poll_frequency * 2
                    self.logger.debug("Failover Heartbeat Max Age: " + str(max_age) + " seconds")
                    if failover_heartbeat_diff > max_age:
                        self.logger.warning("Failover Heartbeat '" + get_datetime_as_str(last_failover_heartbeat) + "' for Active Failover controller '" + str(active_failover_node) + "' is older then max age of " + str(max_age) + " seconds")
                        self.set_this_failover_controller_as_active()
                        active_failover_node = self.metadata_service.get_active_failover_node()
            else:
                self.logger.warning("Failover Node is None")
                self.set_this_failover_controller_as_active()
                active_failover_node = self.metadata_service.get_active_failover_node()

        if self.IS_FAILOVER_CONTROLLER_ACTIVE:

            self.logger.info("This Failover Controller is ACTIVE")

            # Check to make sure this Failover Controller
            if active_failover_node != self.current_host:
                self.logger.warning("Discovered this Failover Controller should not be active because Active Failover Node is not this nodes host")
                self.set_this_failover_controller_as_inactive()

            else:
                self.logger.info("Setting Failover Heartbeat")
                self.metadata_service.set_failover_heartbeat()

                if active_scheduler_node is None:
                    self.logger.warning("Active Scheduler is None")
                    active_scheduler_node = self.search_for_active_scheduler_node()
                    self.metadata_service.set_active_scheduler_node(active_scheduler_node)
                else:
                    if active_scheduler_node not in self.scheduler_nodes_in_cluster:
                        self.logger.warning("Active Scheduler is not in scheduler_nodes_in_cluster list. Searching for new Active Scheduler Node.")
                        active_scheduler_node = self.search_for_active_scheduler_node()
                        self.metadata_service.set_active_scheduler_node(active_scheduler_node)

                if not self.is_scheduler_running(active_scheduler_node):
                    self.logger.warning("Scheduler is not running on Active Scheduler Node '" + str(active_scheduler_node) + "'")
                    self.startup_scheduler(active_scheduler_node)
                    self.logger.info("Pausing for 2 seconds to allow the Scheduler to Start")
                    time.sleep(2)
                    if not self.is_scheduler_running(active_scheduler_node):
                        self.logger.warning("Failed to restart Scheduler on Active Scheduler Node '" +str(active_scheduler_node) + "'")
                        self.logger.warning("Starting to search for a new Active Scheduler Node")
                        is_successful = False
                        for standby_node in self.get_standby_nodes(active_scheduler_node):
                            self.logger.warning("Trying to startup Scheduler on STANDBY node '" + str(standby_node) + "'")
                            self.startup_scheduler(standby_node)
                            if self.is_scheduler_running(standby_node):
                                is_successful = True
                                active_scheduler_node = standby_node
                                self.metadata_service.set_active_scheduler_node(active_scheduler_node)
                                self.logger.warning("New Active Scheduler Node is set to '" + active_scheduler_node + "'")
                                break
                        if not is_successful:
                            self.RETRY_COUNT += 1
                            self.logger.error("Tried to start up a Scheduler on a STANDBY but all failed. Retrying on next polling. retry count: '" + str(self.RETRY_COUNT) + "'")
                        self.logger.warning("Finished search for a new Active Scheduler Node")
                    else:
                        self.logger.warning("Confirmed the Scheduler is now running")
                else:
                    self.RETRY_COUNT = 0
                    self.logger.info("Checking if scheduler instances are running on STANDBY nodes...")
                    for standby_node in self.get_standby_nodes(active_scheduler_node):
                        if self.is_scheduler_running(standby_node):
                            self.logger.warning("There is a Scheduler running on a STANDBY node '" + standby_node + "'. Shutting Down that Scheduler.")
                            self.shutdown_scheduler(standby_node)
                    self.logger.info("Finished checking if scheduler instances are running on STANDBY nodes")

                if self.RETRY_COUNT == self.retry_count_before_alerting:
                    self.emailer.send_alert(
                        current_host=self.current_host,
                        retry_count=self.RETRY_COUNT,
                        latest_status_message=self.LATEST_FAILED_STATUS_MESSAGE,
                        latest_start_message=self.LATEST_FAILED_START_MESSAGE
                    )
        else:
            self.logger.info("This Failover Controller on STANDBY")

    def get_standby_nodes(self, active_scheduler_node):
        standby_nodes = []
        for node in self.scheduler_nodes_in_cluster:
            if node != active_scheduler_node:
                standby_nodes.append(node)
        return standby_nodes

    def is_scheduler_running(self, host):
        self.logger.info("Starting to Check if Scheduler on host '" + str(host) + "' is running...")

        process_check_command = "ps -eaf"
        grep_command = "grep 'airflow scheduler'"
        grep_command_no_quotes = grep_command.replace("'", "")
        full_status_check_command = process_check_command + " | " + grep_command  # ps -eaf | grep 'airflow scheduler'
        is_running = False
        is_successful, output = self.command_runner.run_command(host, full_status_check_command)
        self.LATEST_FAILED_STATUS_MESSAGE = output
        if is_successful:
            active_list = []
            for line in output:
                if line.strip() != "" and process_check_command not in line and grep_command not in line and grep_command_no_quotes not in line and full_status_check_command not in line:
                    active_list.append(line)

            active_list_length = len(list(filter(None, active_list)))

            # todo: If there's more then one scheduler running this should kill off the other schedulers. MIGHT ALREADY BE HANDLED. DOUBLE CHECK.

            is_running = active_list_length > 0
        else:
            self.logger.critical("is_scheduler_running check failed")

        self.logger.info("Finished Checking if Scheduler on host '" + str(host) + "' is running. is_running: " + str(is_running))

        return is_running

    def startup_scheduler(self, host):
        self.logger.info("Starting Scheduler on host '" + str(host) + "'...")
        is_successful, output = self.command_runner.run_command(host, self.airflow_scheduler_start_command)
        self.LATEST_FAILED_START_MESSAGE = output
        self.logger.info("Finished starting Scheduler on host '" + str(host) + "'")

    def shutdown_scheduler(self, host):
        self.logger.warning("Starting to shutdown Scheduler on host '" + host + "'...")
        is_successful, output = self.command_runner.run_command(host, self.airflow_scheduler_stop_command)
        self.LATEST_FAILED_SHUTDOWN_MESSAGE = output
        self.logger.warning("Finished shutting down Scheduler on host '" + host + "'")

    def search_for_active_scheduler_node(self):
        active_scheduler_node = None
        self.logger.info("Starting to search for current Active Scheduler Node...")
        nodes_with_scheduler_running = []
        for node in self.scheduler_nodes_in_cluster:
            if self.is_scheduler_running(node):
                nodes_with_scheduler_running.append(node)
        self.logger.info("Nodes with a scheduler currently running on it: '" + str(nodes_with_scheduler_running) + "'")
        if len(nodes_with_scheduler_running) > 1:
            self.logger.warning("Multiple nodes have a scheduler running on it. Shutting down all except the first one.")
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
        self.logger.warning("Setting this Failover Node to ACTIVE")
        try:
            self.metadata_service.set_active_failover_node(self.current_host)
            self.metadata_service.set_failover_heartbeat()
            self.IS_FAILOVER_CONTROLLER_ACTIVE = True
            self.logger.warning("This Failover Controller is now ACTIVE.")
        except Exception as e:
            self.IS_FAILOVER_CONTROLLER_ACTIVE = False
            self.logger.error("Failed to set Failover Controller as ACTIVE. Trying again next heart beat.")
            traceback.print_exc(file=sys.stdout)

    def set_this_failover_controller_as_inactive(self):
        self.logger.warning("Setting this Failover Node to INACTIVE")
        self.IS_FAILOVER_CONTROLLER_ACTIVE = False
