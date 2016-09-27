# Airflow Scheduler Failover Controller

### Purpose

The purpose of this project is to create a failover controller that will control which scheduler is up and running to allow HA across an Airflow cluster. 


### How it Works

TBD


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
