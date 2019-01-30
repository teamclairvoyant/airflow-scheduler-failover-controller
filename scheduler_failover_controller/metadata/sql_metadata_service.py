from scheduler_failover_controller.metadata.base_metadata_service import BaseMetadataService
from scheduler_failover_controller.utils import date_utils
from sqlalchemy import create_engine, Column, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import scoped_session, sessionmaker
import time

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
        self.query_tries = 5

    def initialize_metadata_source(self):
        # Creating metadata table. If the creation fails assume it was created already.
        try:
            self.logger.info("Creating Metadata Table")
            Base.metadata.create_all(self.engine)
        except Exception, e:
            self.logger.info("Exception while Creating Metadata Table: " + str(e))
            self.logger.info("Table might already exist. Suppressing Exception.")

    def get_failover_heartbeat(self):
        heart_beat_date_str = self.resilient_get_query("failover_heartbeat", self.query_tries)
        if heart_beat_date_str is not None:
            heart_beat_date = date_utils.get_string_as_datetime(heart_beat_date_str)
        else:
            heart_beat_date = None
        return heart_beat_date

    def set_failover_heartbeat(self):
        heart_beat_date_str = date_utils.get_datetime_as_str(datetime.datetime.now())  # get current datetime as string
	self.resilient_set_query("failover_heartbeat", heart_beat_date_str, self.query_tries)

    def get_active_failover_node(self):
        return self.resilient_get_query("active_failover_node", self.query_tries)

    def set_active_failover_node(self, node):
	self.resilient_set_query("active_failover_node", node, self.query_tries)

    def get_active_scheduler_node(self):
        return self.resilient_get_query("active_scheduler_node", self.query_tries)

    def set_active_scheduler_node(self, node):
        self.resilient_set_query("active_scheduler_node", node, self.query_tries)

    def resilient_get_query(self, key, tries):
        session = self.Session()
        try:
            entry = session.query(SQLMetadata).filter(SQLMetadata.key == key).first()
            if entry is not None:
                return entry.value
            else:
                return None
        except:
            if(tries > 1):
                print("Failed to execute get query for ", key, " Retrying ...")
		time.sleep(5)
                self.resilient_get_query(key, tries - 1)
            else:
                raise
        finally:
            session.close()

    def resilient_set_query(self, key, val, tries):
        session = self.Session()
        try:
            entry = session.query(SQLMetadata).filter(SQLMetadata.key == key).first()
            if entry is not None:
                entry.value = val
            else:
                session.add(SQLMetadata(key=key, value=val))
            session.commit()
        except:
            if(tries > 1):
                print("Failed to execute set query for ", key ," Retrying ...")
		time.sleep(5)
                self.resilient_set_query(key, val, tries - 1)
            else:
                raise
        finally:
            session.close()
 
    def clear(self):
        session = self.Session()
        session.query(SQLMetadata).delete()
        session.commit()
        session.close()
