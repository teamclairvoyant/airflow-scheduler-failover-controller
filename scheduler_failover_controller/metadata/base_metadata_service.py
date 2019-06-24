import datetime
from scheduler_failover_controller.utils import date_utils


class BaseMetadataService:

    def initialize_metadata_source(self):
        raise NotImplementedError

    def get_failover_heartbeat(self):
        raise NotImplementedError

    def set_failover_heartbeat(self):
        raise NotImplementedError

    def get_active_failover_node(self):
        raise NotImplementedError

    def set_active_failover_node(self, node):
        raise NotImplementedError

    def get_active_scheduler_node(self):
        raise NotImplementedError

    def set_active_scheduler_node(self, node):
        raise NotImplementedError

    def clear(self):
        raise NotImplementedError

    def print_metadata(self):
        print("Printing Metadata: ")
        print("==============================")
        print("active_failover_node: " + str(self.get_active_failover_node()))
        print("active_scheduler_node: " + str(self.get_active_scheduler_node()))
        print( "last_failover_heartbeat: " + str(self.get_failover_heartbeat()))
        print("")
        print("Printing Other Info: ")
        print("==============================")
        print( "current_timestamp: " + str(date_utils.get_datetime_as_str(datetime.datetime.now())))
