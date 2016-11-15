import importlib
from airflow import configuration


class Emailer:

    EMAIL_SUBJECT = "Airflow Alert - Scheduler Failover Controller Failed to Startup Scheduler"
    EMAIL_BODY = """
    The Scheduler Failover Controller failed to Restart the Scheduler on all Scheduler Hosts.

    Host Name: {0}
    Retry Count: {1}
    Latest Message from trying to get Status: {2}
    Latest Message from trying to Restart: {3}
    """

    def __init__(self, alert_to_email, logger):
        self.alert_to_email = alert_to_email
        self.logger = logger
        path, attr = configuration.get('email', 'EMAIL_BACKEND').rsplit('.', 1)
        module = importlib.import_module(path)
        self.email_backend = getattr(module, attr)
        if self.alert_to_email is None:
            self.logger.warn("alert_to_email value isn't provided. The email will not be able to send alert emails.")

    def send_alert(self, current_host, retry_count, latest_status_message, latest_start_message):
        if self.alert_to_email:
            try:
                self.email_backend(
                    self.alert_to_email,
                    self.EMAIL_SUBJECT,
                    self.EMAIL_BODY.format(current_host, retry_count, latest_status_message, latest_start_message),
                    files=None,
                    dryrun=False
                )
                self.logger.info('Alert Email Sent Sent')
            except Exception as e:
                self.logger.critical('Failed to Send Email: ' + str(e), 'error')
        else:
            self.logger.critical("Couldn't send email since the alert_to_email config is not set")