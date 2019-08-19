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