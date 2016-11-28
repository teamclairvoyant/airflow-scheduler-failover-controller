from airflow import configuration
import scheduler_failover_controller.configuration as scheduler_failover_controller_configuration
import importlib


class Emailer:

    EMAIL_BODY = """
    The Scheduler Failover Controller failed to Restart the Scheduler on all Scheduler Hosts.<br/>
    <br/>
    <b>Host Name:</b> {0}<br/>
    <b>Retry Count:</b> {1}<br/>
    <b>Latest Message from trying to get Status:</b> {2}<br/>
    <b>Latest Message from trying to Restart:</b> {3}<br/>
    """

    def __init__(self, alert_to_email, logger, email_subject=None):
        logger.debug("Creating Emailer with Args - alert_to_email: {alert_to_email}, logger: {logger}, email_subject: {email_subject}".format(**locals()))
        self.alert_to_email = alert_to_email
        self.logger = logger
        if email_subject:
            self.email_subject = email_subject
        else:
            self.email_subject = scheduler_failover_controller_configuration.DEFAULT_ALERT_EMAIL_SUBJECT
            self.logger.debug("Email subject was not provided. Using Default value: " + str(self.email_subject))
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
                    self.email_subject,
                    self.EMAIL_BODY.format(current_host, retry_count, latest_status_message, latest_start_message),
                    files=None,
                    dryrun=False
                )
                self.logger.info('Alert Email has been sent')
            except Exception as e:
                self.logger.critical('Failed to Send Email: ' + str(e) + 'error')
        else:
            self.logger.critical("Couldn't send email since the alert_to_email config is not set")