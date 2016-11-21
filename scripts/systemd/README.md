# Airflow Scheduler Failover Controller Systemd

## Steps to Deploy

1. Login to each of the machines acting as Scheduler Failover Controllers

2. Login as root

3. Copy the **scheduler_failover_controller.service** to the **/usr/lib/systemd/system** directory
 
4. Edit the **scheduler_failover_controller.service** and change any configurations you would like

    a. Change user and group options as needed to ensure that it is the same as the user and group the main airflow processes are running as
    
5. Enable the service

        systemctl enable scheduler_failover_controller

6. You're done!

