__author__ = 'robertsanders'

import logging
import logging.handlers
import os


def get_logger(logging_level, logs_output_file_path=None, logs_rotate_when="midnight", logs_rotate_backup_count=7):

    # Create the logger
    logger = logging.getLogger(__name__)

    # Set logging level
    logger.setLevel(logging_level)

    # Create logging format
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    # Create the stream handler to log messages to the console
    streamHandler = logging.StreamHandler()
    streamHandler.setLevel(logging.DEBUG)
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)

    # Create the file handler to log messages to a log file
    if logs_output_file_path is not None:
        fileHandler = logging.handlers.TimedRotatingFileHandler(filename=os.path.expanduser(logs_output_file_path), when="midnight", backupCount=7)
        fileHandler.setLevel(logging.DEBUG)
        fileHandler.setFormatter(formatter)
        logger.addHandler(fileHandler)

    return logger

