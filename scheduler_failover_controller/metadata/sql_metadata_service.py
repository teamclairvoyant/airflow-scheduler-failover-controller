from scheduler_failover_controller.metadata.base_metadata_service import BaseMetadataService
from scheduler_failover_controller.utils import date_utils
from sqlalchemy import create_engine, Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker

import datetime

Base = declarative_base()


class SQLMetadata(Base):
    __tablename__ = 'scheduler_failover_controller_metadata'
    key = Column(String(200), primary_key=True)
    value = Column(String(200), nullable=False)


class SQLMetadataService(BaseMetadataService):

    Session = None

    def __init__(self, sql_alchemy_conn, logger):
        logger.debug("Creating MetadataServer (type:SQLMetadataService) with Args - sql_alchemy_conn: {sql_alchemy_conn}, logger: {logger}".format(**locals()))
        self.sql_alchemy_conn = sql_alchemy_conn
        self.logger = logger
        engine_args = {}
        self.engine = create_engine(sql_alchemy_conn, **engine_args)
        self.Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=self.engine))

    def initialize_metadata_source(self):
        # Creating metadata table. If the creation fails assume it was created already.
        try:
            self.logger.info("Creating Metadata Table")
            Base.metadata.create_all(self.engine)
        except Exception as e:
            self.logger.info("Exception while Creating Metadata Table: " + str(e))
            self.logger.info("Table might already exist. Suppressing Exception.")

    def get_failover_heartbeat(self):
        session = self.Session()
        entry = session.query(SQLMetadata).filter(SQLMetadata.key == "failover_heartbeat").first()
        if entry is not None:
            heart_beat_date_str = entry.value
            heart_beat_date = date_utils.get_string_as_datetime(heart_beat_date_str)
        else:
            heart_beat_date = None
        session.close()
        return heart_beat_date

    def set_failover_heartbeat(self):
        session = self.Session()
        heart_beat_date_str = date_utils.get_datetime_as_str(datetime.datetime.now())  # get current datetime as string
        entry = session.query(SQLMetadata).filter(SQLMetadata.key == "failover_heartbeat").first()
        if entry is not None:
            entry.value = heart_beat_date_str
        else:
            session.add(SQLMetadata(key="failover_heartbeat", value=heart_beat_date_str))
        session.commit()
        session.close()

    def get_active_failover_node(self):
        session = self.Session()
        entry = session.query(SQLMetadata).filter(SQLMetadata.key == "active_failover_node").first()
        session.close()
        if entry is not None:
            return entry.value
        else:
            return None

    def set_active_failover_node(self, node):
        session = self.Session()
        entry = session.query(SQLMetadata).filter(SQLMetadata.key == "active_failover_node").first()
        if entry is not None:
            entry.value = node
        else:
            session.add(SQLMetadata(key="active_failover_node", value=node))
        session.commit()
        session.close()

    def get_active_scheduler_node(self):
        session = self.Session()
        entry = session.query(SQLMetadata).filter(SQLMetadata.key == "active_scheduler_node").first()
        session.close()
        if entry is not None:
            return entry.value
        else:
            return None

    def set_active_scheduler_node(self, node):
        session = self.Session()
        entry = session.query(SQLMetadata).filter(SQLMetadata.key == "active_scheduler_node").first()
        if entry is not None:
            entry.value = node
        else:
            session.add(SQLMetadata(key="active_scheduler_node", value=node))
        session.commit()
        session.close()

    def clear(self):
        session = self.Session()
        session.query(SQLMetadata).delete()
        session.commit()
        session.close()
