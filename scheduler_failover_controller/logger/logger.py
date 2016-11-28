import logging
import logging.handlers
import os


def get_logger(logging_level, logs_output_file_path=None, logs_rotate_when="midnight", logs_rotate_backup_count=7):

    # Create the logger
    logger = logging.getLogger(__name__)

    # Set logging level
    logger.setLevel(logging_level)

    # Create logging format
    formatter = logging.Formatter('[%(asctime)s] {%(filename)s:%(lineno)d} - %(levelname)s - %(message)s')

    # Create the stream handler to log messages to the console
    # stream_handler = logging.StreamHandler()
    # stream_handler.setFormatter(formatter)
    # logger.addHandler(stream_handler)

    # Create the file handler to log messages to a log file
    if logs_output_file_path is not None:
        log_dir = os.path.dirname(logs_output_file_path)
        if not os.path.exists(os.path.expanduser(log_dir)):
            os.makedirs(os.path.expanduser(log_dir))
        file_handler = logging.handlers.TimedRotatingFileHandler(
            filename=os.path.expanduser(logs_output_file_path),
            when=logs_rotate_when,
            backupCount=logs_rotate_backup_count
        )
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    logger.debug("Created Logger with Args - logging_level: {logging_level}, logs_output_file_path: {logs_output_file_path}, logs_rotate_when: {logs_rotate_when}, logs_rotate_backup_count: {logs_rotate_backup_count}".format(**locals()))

    return logger

