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

### Development

In case you want to do development work on the project

1. Clone the project from GitHub to your local machine

        git clone https://github.com/teamclairvoyant/airflow-scheduler-failover-controller

2. Run pip install

        cd {AIRFLOW_FAILOVER_CONTROLLER_HOME}
        pip install -e .
        
3. You're done!

    * After, you will be able to run the project through the CLI Interface (See bellow for more details), be able to make any changes to the project you just brought down and have those changes be immediately reflected in the CLI

### Production

1. Select which version of the code you want to install and use this value as the {BRANCH_OR_TAG} placeholder:

    * modularize-dev    - development version of the code

2. Run pip install

        pip install git+git://github.com/teamclairvoyant/airflow-scheduler-failover-controller.git@{BRANCH_OR_TAG}

## CLI Interface

usage: **scheduler_failover_controller** [-h]
                                     {init,test_connection,is_scheduler_running,clear_metadata,metadata,send_test_email,get_current_host,start}
                                     ...

    positional arguments:
      {init,test_connection,is_scheduler_running,clear_metadata,metadata,send_test_email,get_current_host,start}
      
                            sub-command help
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

2. Get the bin file location and use this value as the {BIN_CLI_FILE_PATH} placeholder

        which scheduler_failover_controller

3. Remove the bin file

        rm {BIN_CLI_FILE_PATH}

## Startup/Status/Shutdown Instructions

### Startup

#### Startup in the foreground

    scheduler_failover_controller start

#### Startup as a background process

    scheduler_failover_controller start &

### Status

    ps -eaf | grep scheduler_failover_controller

### Shutdown
 
    for pid in `ps -ef | grep "scheduler_failover_controller" | awk '{print $2}'` ; do kill -9 $pid ; done

## Getting Started

This is a step by step set of instructions you can take to get up and running with the scheduler_failover_controller

1. Install the ASFC on all the desired machines

    * See the above section entitled "Installation"
    
2. Run the following CLI command to get the default configurations setup in airflow.cfg
 
        scheduler_failover_controller init
        
3. Update the default configurations that were added to the airflow.cfg file

    a. Main ones include updating: scheduler_nodes_in_cluster, alert_to_email
    
4. Enable all the machines to be able to ssh to each of the other machines with the user you're running airflow as
    
    a. Create a public and private key SSH key on all of the machines you want to act as schedulers. You can follow these instructions: https://www.digitalocean.com/community/tutorials/how-to-set-up-ssh-keys--2
        
    b. Add the public key content to the ~/.ssh/authorized_keys file on all the other machines

5. Run the following CLI command to test the connection to all the machines that will act as Schedulers

        scheduler_failover_controller test_connection

6. Startup the Airflow Scheduler Failover Controller

    * See the above section entitled "Startup/Status/Shutdown Instructions"
    
7. View the logs to ensure things are running correctly

    * Location of the logs can be determined by the 'logging_dir' configuration entry in the airflow.cfg 
    
8. View the metadata to ensure things are being set correctly

        scheduler_failover_controller metadata

