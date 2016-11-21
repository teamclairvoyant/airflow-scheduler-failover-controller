import importlib
from airflow import configuration

EMAIL_SUBJECT = "Airflow Alert - Scheduler Failover Controller Failed to Startup Scheduler"
EMAIL_BODY = """
The Scheduler Failover Controller failed to Restart the Scheduler on all Scheduler Hosts.<br/>
<br/>
Host Name: {0}<br/>
Retry Count: {1}<br/>
Latest Message from trying to get Status: {2}<br/>
Latest Message from trying to Restart: {3}<br/>
"""


class Emailer:

    def __init__(self, alert_to_email, logger):
        logger.debug("Creating Emailer with Args - alert_to_email: {alert_to_email}, logger: {logger}".format(**locals()))
        self.alert_to_email = alert_to_email
        self.logger = logger
        path, attr = configuration.get('email', 'EMAIL_BACKEND').rsplit('.', 1)
        module = importlib.import_module(path)
        self.email_backend = getattr(module, attr)
        if self.alert_to_email is None:
            self.logger.warn("alert_to_email value isn't provided. The email will not be able to send alert emails.")

    def send_alert(self, current_host, retry_count, latest_status_message, latest_start_message):
        self.logger.debug("Sending Email with Args - current_host: {current_host}, retry_count: {retry_count}, latest_status_message: {latest_status_message}, latest_start_message: {latest_start_message}".format(**locals()))
        if self.alert_to_email:
            try:
                self.email_backend(
                    self.alert_to_email,
                    EMAIL_SUBJECT,
                    EMAIL_BODY.format(current_host, retry_count, latest_status_message, latest_start_message),
                    files=None,
                    dryrun=False
                )
                self.logger.info('Alert Email Sent Sent')
            except Exception as e:
                self.logger.critical('Failed to Send Email: ' + str(e) + 'error')
        else:
            self.logger.critical("Couldn't send email since the alert_to_email config is not set")