# Airflow Scheduler Failover Controller

## Project Purpose

The purpose of this project is to create a failover controller that will control which scheduler is up and running to allow HA across an Airflow cluster. 


## Motivation 

We had attempted to setup a Highly Available Airflow Cluster where we had 2 machines with all the normal Airflow Daemons (web server, scheduler, workers, etc.) running on them. Each of the instances would share a MySQL instances as its MetaStore and share a RabbitMQ Queue for its Queueing Services (since we were using CeleryExecutors).
 
What we noticed after a month running the Cluster is that the schedulers would occasionally push a duplicate task instance to the RabbitMQ Queue. Therefore the Airflow executors would execute the same task instance twice. This caused a lot of data inconsistency issues.

This is what motivated us to search for an alternative to our initial approach to build a highly available Airflow Cluster. It lead to us creating this module.


## How it Works

The Airflow Scheduler Failover Controller (ASFC) is a mechanism that ensures that only one Scheduler instance is running in an Airflow Cluster at a time. This way you don't come across the issues we described in the "Motivation" section above.

You will first need to startup the ASFC on each of the instances you want the scheduler to be running on. When you start up multiple instances of the ASFC one of them takes on the Active state and the other takes on a Standby state. There is a heart beat mechanism setup to track if the Active ASFC is still active. If the Active ASFC misses multiple heart beats, the Standby ASFC becomes active.

The Active ASFC will poll every 10 seconds to see if the scheduler is running on the desired node. If it is not, the ASFC will try to restart the daemon. If the scheduler daemons still doesn't startup, the daemon is started on another node in the cluster.

## Installation

### Local Development

In case you want to do development work on the project

1. Clone the project from GitHub to your local machine

        git clone https://github.com/teamclairvoyant/airflow-scheduler-failover-controller

2. Run pip install

        cd {AIRFLOW_FAILOVER_CONTROLLER_HOME}
        pip install -e .
        
3. You're done!

    * After, you will be able to run the project through the CLI Interface (See bellow for more details), be able to make any changes to the project you just brought down and have those changes be immediately reflected in the CLI

### Production

1. Select which version of the code you want to install and use this value as the {BRANCH_OR_TAG} placeholder. Current Options:

    * master
        * latest code in master
        * BRANCH
    * development
        * code in development branch - pending fixes and features
        * BRANCH
    * v1.0.0
        * First release. Support for Airflow <= v1.8.0
        * RELEASE
    * v1.0.1
        * Support for Airflow <= v1.8.0  
        * RELEASE
    * v1.0.2
        * Support for Airflow >= v1.8.1
        * RELEASE
    * v1.0.3
        * Added virtualenv support
        * RELEASE
    * v1.0.4
        * Added Support for Python 3
        * RELEASE
    * v1.0.5
        * Fixed issues with Python 3
        * RELEASE
    * v1.0.6
        * Added feature to poll the health URL to ensure the Scheduler is Healthy
        * RELEASE
    * v1.0.7
        * Disable SSL Verify for Airflow API Request
        * RELEASE
    * v1.0.8
        * Exit code fix when SSHing onto another node
        * Increase default polling frequency from 10 to 60 seconds
        * Add support for conifguring SCHEDULER_RESTART_SLEEP_TIME (default: 35 seconds
        * RELEASE

2. Run pip install

        pip install git+git://github.com/teamclairvoyant/airflow-scheduler-failover-controller.git@{BRANCH_OR_TAG}

## CLI Interface

    usage: scheduler_failover_controller [-h]
                                         {version,init,test_connection,is_scheduler_running,clear_metadata,metadata,send_test_email,get_current_host,start}
                                         ...

    positional arguments:
      {version,init,test_connection,is_scheduler_running,clear_metadata,metadata,send_test_email,get_current_host,start}
                            sub-command help
        version             Prints out the version of the Scheduler Failover
                            Controller
        init                Initialize Configurations to allow Scheduler Failover
                            Controller to run
        test_connection     Tests if you can connect to all the necessary machines
                            listed in 'scheduler_nodes_in_cluster' config
        is_scheduler_running
                            Checks if the Scheduler is running on the machines you
                            have listed in 'scheduler_nodes_in_cluster' config
        clear_metadata      Clear the Metadata in Metastore
        metadata            Get the Metadata from Metastore
        send_test_email     Send a Test Email
        get_current_host    Get the Current Hostname
        start               Start the Airflow Scheduler Failover Controller
    
    optional arguments:
      -h, --help            show this help message and exit


## Uninstall 

1. Run pip uninstall

        pip uninstall scheduler_failover_controller

2. If you ran the installation for development, follow these steps:

    a. Get the bin file location and use this value as the {BIN_CLI_FILE_PATH} placeholder

        which scheduler_failover_controller

    b. Remove the bin file

        rm {BIN_CLI_FILE_PATH}

## Startup/Status/Shutdown Instructions

### Quickstart

#### Startup

##### Startup in the foreground

    scheduler_failover_controller start

##### Startup as a background process

    nohup scheduler_failover_controller start > /dev/null &

#### Status

    ps -eaf | grep scheduler_failover_controller

#### Shutdown
 
    for pid in `ps -ef | grep "scheduler_failover_controller" | awk '{print $2}'` ; do kill -9 $pid ; done

### Systemd

The Airflow Scheduler Failover Controller also allows you to control the process with systemd. The systemd files can be found under ${PROJECT_HOME}/scripts/systemd. Within this directory is a README.md file which describes how to deploy the systemd files. Bellow illustrates how to run the process after deploying the files.

Note: **Run as Root**

#### Startup

    sudo systemctl start scheduler_failover_controller
    
#### Status

    sudo systemctl status scheduler_failover_controller

#### Shutdown

    sudo systemctl stop scheduler_failover_controller
    
#### Restart

    sudo systemctl restart scheduler_failover_controller

## Getting Started

This is a step by step set of instructions you can take to get up and running with the scheduler_failover_controller

1. Install the ASFC on all the desired machines

    * See the above section entitled "Installation"
    
2. Run the following CLI command to get the default configurations setup in airflow.cfg
 
        scheduler_failover_controller init

##### PS: Virtualenv support
You can use the scheduler_failover_controller on a virtual environment too. To do so, you need to specify the activation path of your virtual environment:

        scheduler_failover_controller -venv PATH_TO_VIRTUALENV/bin/activate init

        
3. Ensure that the `base_url` value under [webserver] in airflow.cfg is set to the Airflow webserver.

4. Update the default configurations that were added to the bottom of the airflow.cfg file under the [scheduler_failover] section

    a. Main ones include updating: **scheduler_nodes_in_cluster**, **alert_to_email**

    b. See the [Configurations](#configurations) Section bellow for more details
    
5. Enable all the machines to be able to ssh to each of the other machines with the user you're running airflow as
    
    a. Create a public and private key SSH key on all of the machines you want to act as schedulers. You can follow these instructions: https://www.digitalocean.com/community/tutorials/how-to-set-up-ssh-keys--2
        
    b. Add the public key content to the ~/.ssh/authorized_keys file on all the other machines

6. Run the following CLI command to test the connection to all the machines that will act as Schedulers

        scheduler_failover_controller test_connection

7. Startup the following Airflow Daemons

    a. webserver
    
        nohup airflow webserver $* >> ~/airflow/logs/webserver.logs &
    
    b. workers (If you're using the CeleryExecutor)

        nohup airflow worker $* >> ~/airflow/logs/celery.logs &

8. Startup the Airflow Scheduler Failover Controller on each node you would like acting as the Scheduler Failover Controller (ONE AT A TIME).

    * See the above section entitled "Startup/Status/Shutdown Instructions"
    
9.  View the logs to ensure things are running correctly

    * Location of the logs can be determined by the 'logging_dir' configuration entry in the airflow.cfg 
    
    * Note: Logs are set by default to rotate at midnight and only keep 7 days worth of backups. This can be overridden in the configuration file.
    
10. View the metadata to ensure things are being set correctly

        scheduler_failover_controller metadata


## Configurations

#### scheduler_nodes_in_cluster

*required*

Default: localhost

List of potential nodes that can act as Schedulers (Comma Separated List)

It is recommended that you use the value printed from the following command:
       
 `scheduler_failover_controller get_current_host`
        
So, if this command printed out 'ip-10-0-0-98', include this as the value for that particular host. 

#### metadata_service_type

*required*

Default: SQLMetadataService

The metadata service class that the failover controller should use. Choices include: SQLMetadataService, ZookeeperMetadataService

SQLMetadataService will use your sql_alchemy_conn config in the airflow.cfg file to connect to SQL

ZookeeperMetadataService will use the metadata_service_zookeeper_nodes config in the airflow.cfg under the [scheduler_failover] section

#### metadata_service_zookeeper_nodes

*required* if metadata_service_type == ZookeeperMetadataService

Default: localhost:2181

If you're using the ZookeeperMetadataService, this property will identify the zookeeper nodes it will try to connect to

#### poll_frequency

Default: DEFAULT_POLL_FREQUENCY

Frequency that the Scheduler Failover Controller polls to see if the scheduler is running (in seconds)

#### airflow_scheduler_start_command

Default: "export AIRFLOW_HOME=~/airflow;{};nohup airflow scheduler >> ~/airflow/logs/scheduler.logs &"

Command to use when trying to start a Scheduler instance on a node

#### airflow_scheduler_stop_command

Default: "for pid in `ps -ef | grep "airflow scheduler" | awk '{{print $2}}'` \; do kill -9 $pid \; done"

Command to use when trying to stop a Scheduler instance on a node

#### logging_level

Default: INFO

Logging Level. Choices include:
NOTSET, DEBUG, INFO, WARN, ERROR, CRITICAL

#### logging_dir

Default: "~/airflow/logs/scheduler_failover/"

Log Directory Location

#### logging_file_name

Default: scheduler_failover_controller.log

Log File Name

#### logs_rotate_when

Default: midnight

When the logs should be rotated.
Values Documentation: https://docs.python.org/2/library/logging.handlers.html#logging.handlers.TimedRotatingFileHandler

#### logs_rotate_backup_count

Default: 7

How many times the logs should be rotate before you clear out the old ones

#### retry_count_before_alerting

Detault: 5

Number of times to retry starting up the scheduler before it sends an alert

#### alert_to_email

Detault: airflow@airflow.com

Email address to send alerts to if the failover controller is unable to startup a scheduler 

#### alert_email_subject

Default: "Airflow Alert - Scheduler Failover Controller Failed to Startup Scheduler"

Email Subject to use when sending an alert

## Recommended Steps for a Better Deployment

Above describes a quickstart approach. However, if you're looking for a better long term approach for using the Airflow Scheduler Failover Controller then you can follow the bellow steps.

### Setup Systemd for Airflow

Airflow provides scripts to help you control the airflow daemons through the **systemctl** command. It is recommended that you setup the airflow-scheduler, at least, for systemd. 
  
Go to https://github.com/apache/incubator-airflow/tree/master/scripts/systemd and follow the instructions in the README file to get it setup.

Note: It is also recommended to diable the automatic restart of the Scheduler process in the SystemD file. So remove the `Retry` and `RestartSec` section in the default SystemD file.

### Update airflow.cfg configs to use the Systemd

    airflow_scheduler_start_command = sudo systemctl restart airflow-scheduler
    
    airflow_scheduler_stop_command = sudo systemctl stop airflow-scheduler
    
### Setup the Airflow Scheduler Failover Controller to use Systemd

Follow the instructions in ${PROJECT_HOME}/scripts/systemd/README.md to set it up.

