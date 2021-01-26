# Airflow Scheduler Failover Controller Changelog

### [v1.0.0](https://github.com/teamclairvoyant/airflow-scheduler-failover-controller/tree/v1.0.0)

First release.

Includes base functionality to control and check if the Scheduler is running. Supports multiple types of metastores and provides an easy way to install and configure.

### [v1.0.1](https://github.com/teamclairvoyant/airflow-scheduler-failover-controller/tree/v1.0.1)

* updated Systemd services options
* Created scripts to deploy Systemd Services
* Fixing issue with running sudo command via ssh
* Updated documentation (setup instructions and others)

### [v1.0.2](https://github.com/teamclairvoyant/airflow-scheduler-failover-controller/tree/v1.0.2)

* The package name was changed from `airflow` to `apache-airflow`

### [v1.0.3](https://github.com/teamclairvoyant/airflow-scheduler-failover-controller/tree/v1.0.3)

* Added virtualenv support

### [v1.0.4](https://github.com/teamclairvoyant/airflow-scheduler-failover-controller/tree/v1.0.4)

* Added Support for Python 3

### [v1.0.5](https://github.com/teamclairvoyant/airflow-scheduler-failover-controller/tree/v1.0.5)

* Fixed Python 3 issue - `TypeError: a bytes-like object is required, not 'str'`

### [v1.0.6](https://github.com/teamclairvoyant/airflow-scheduler-failover-controller/tree/v1.0.6)

* Added feature to poll the health URL to ensure the Scheduler is Healthy
* Updating Documentation in README

### [v1.0.7](https://github.com/teamclairvoyant/airflow-scheduler-failover-controller/tree/v1.0.7)

* Disabled SSL verify for Airflow API Request

### [v1.0.8](https://github.com/teamclairvoyant/airflow-scheduler-failover-controller/tree/v1.0.8)

* Exit code fix when SSHing onto another node
* Increase default polling frequency from 10 to 60 seconds
* Add support for conifguring SCHEDULER_RESTART_SLEEP_TIME (default: 35 seconds)