__author__ = 'robertsanders'

# http://stackoverflow.com/questions/372042/difference-between-abstract-class-and-interface-in-python


class BaseMetadataService:

    @classmethod
    def get_failover_heartbeat(cls):
        raise NotImplementedError

    @classmethod
    def set_failover_heartbeat(cls):
        raise NotImplementedError

    @classmethod
    def get_active_failover_node(cls):
        raise NotImplementedError

    @classmethod
    def set_active_failover_node(cls, node):
        raise NotImplementedError

    @classmethod
    def get_active_scheduler_node(cls):
        raise NotImplementedError

    @classmethod
    def set_active_scheduler_node(cls, node):
        raise NotImplementedError

    @classmethod
    def truncate(cls):
        raise NotImplementedError
