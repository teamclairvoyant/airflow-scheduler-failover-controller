# Airflow Scheduler Failover Controller

### Project Purpose

The purpose of this project is to create a failover controller that will control which scheduler is up and running to allow HA across an Airflow cluster. 


## Motivation 

We had attempted to setup a Highly Available Airflow Cluster where we had 2 machines with all the normal Airflow Daemons (web server, scheduler, workers, etc.) running on them. Each of the instances would share a MySQL instances as its MetaStore and share a RabbitMQ Queue for its Queueing Services (since we were using CeleryExecutors).
 
What we noticed after a month running the Cluster is that the schedulers would occasionally push a duplicate task instance to the RabbitMQ Queue. Therefore the Airflow executors would execute the same task instance twice. This caused a lot of data inconsistency issues.

This is what motivated us to search for an alternative to our initial approach to build a highly available Airflow Cluster. It lead to us creating this module.


### How it Works

The Airflow Scheduler Failover Controller (ASFC) is a mechanism that ensures that only one Scheduler instance is running in an Airflow Cluster at a time. This way you don't come across the issues we described in the "Motivation" section above.

You will first need to startup the ASFC on each of the instances you want the scheduler to be running on. When you start up multiple instances of the ASFC one of them takes on the Active state and the other takes on a Standby state. There is a heart beat mechanism setup to track if the Active ASFC is still active. If the Active ASFC misses multiple heart beats, the Standby ASFC becomes active.

The Active ASFC will poll every 10 seconds to see if the scheduler is running on the desired node. If it is not, the ASFC will try to restart the daemon. If the scheduler daemons still doesn't startup, the daemon is started on another node in the cluster.


### Deployment Instructions

1. Copy the files in the root and bin directory in this project to the {AIRFLOW_HOM}/bin directory

2. Enable all the machines to be able to ssh to each of the other machines with the user you're running airflow as

    a. Create a public and private key SSH key on all of the machines you want to act as schedulers
    
    b. Add the public key content to the ~/.ssh/authorized_keys file on all the other machines
     
3. Add the following entry to the airflow.cfg file on all the instances and set the correct value:
  
  ```
  
    [core]
    
    nodes_in_cluster = {HOST_1},{HOST_2}
    
  ```

4. Test your connection with the airflow-scheduler-failover.py script

    a. Execute the following command:

      ```
      
        python airflow-scheduler-failover.py --test_connection
      
      ```

5. Start up all the airflow daemons except for the scheduler

6. Verify the daemons is up by checking the status with the following command


  ```
  
    ps -eaf | grep "airflow\-scheduler\-failover"
    
  ```

7. Check the logs to determine if the process is running

    a. log location: ~/airflow/logs/scheduler-failover.logs
    
8. Check the Metadata:

  ```
  
    python airflow-scheduler-failover.py --metadata
    
  ```
  
9. Given the metadata, verify that the scheduler is being ran on the host mentioned in the variable 'active_scheduler_node'
