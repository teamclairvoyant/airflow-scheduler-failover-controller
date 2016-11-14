import datetime

__author__ = 'robertsanders'

from sqlalchemy import create_engine, Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

from scheduler_failover_controller.metadata.base_metadata_service import BaseMetadataService

from scheduler_failover_controller.utils import date_utils

Base = declarative_base()

class SchedulerFailoverKeyValue(BaseMetadataService, Base):
    __tablename__ = 'scheduler_failover'
    key = Column(String(200), primary_key=True)
    value = Column(String(200), nullable=False)

    Session = None

    def __init__(self, sql_alchemy_conn):
        engine_args = {}
        engine = create_engine(sql_alchemy_conn, **engine_args)
        self.Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))

    @classmethod
    def get_failover_heartbeat(cls):
        session = cls.Session()
        entry = session.query(cls).filter(cls.key == "failover_heartbeat").first()
        if entry is not None:
            heart_beat_date_str = entry.value
            heart_beat_date = date_utils.get_string_as_datetime(heart_beat_date_str)
        else:
            heart_beat_date = None
        session.close()
        return heart_beat_date

    @classmethod
    def set_failover_heartbeat(cls):
        session = cls.Session()
        heart_beat_date_str = date_utils.get_datetime_as_str(datetime.datetime.now())  # get current datetime as string
        entry = session.query(cls).filter(cls.key == "failover_heartbeat").first()
        if entry is not None:
            entry.value = heart_beat_date_str
        else:
            session.add(SchedulerFailoverKeyValue(key="failover_heartbeat", value=heart_beat_date_str))
        session.commit()
        session.close()

    @classmethod
    def get_active_failover_node(cls):
        session = cls.Session()
        entry = session.query(cls).filter(cls.key == "active_failover_node").first()
        session.close()
        if entry is not None:
            return entry.value
        else:
            return None

    @classmethod
    def set_active_failover_node(cls, node):
        session = cls.Session()
        entry = session.query(cls).filter(cls.key == "active_failover_node").first()
        if entry is not None:
            entry.value = node
        else:
            session.add(SchedulerFailoverKeyValue(key="active_failover_node", value=node))
        session.commit()
        session.close()

    @classmethod
    def get_active_scheduler_node(cls):
        session = cls.Session()
        entry = session.query(cls).filter(cls.key == "active_scheduler_node").first()
        session.close()
        if entry is not None:
            return entry.value
        else:
            return None

    @classmethod
    def set_active_scheduler_node(cls, node):
        session = cls.Session()
        entry = session.query(cls).filter(cls.key == "active_scheduler_node").first()
        if entry is not None:
            entry.value = node
        else:
            session.add(SchedulerFailoverKeyValue(key="active_scheduler_node", value=node))
        session.commit()
        session.close()

    @classmethod
    def truncate(cls):
        session = cls.Session()
        session.query(cls).delete()
        session.commit()
        session.close()