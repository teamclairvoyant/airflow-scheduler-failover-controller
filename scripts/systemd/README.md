# Airflow Scheduler Failover Controller Systemd

## Steps to Deploy

### Scripted

We have built scripts to help facilitate the deployment of the systemd scripts. Follow the instructions under the {PROJECT_HOME}/scripts/systemd/deploy/ directory.

### Manually

1. Login to each of the machines acting as Scheduler Failover Controllers

2. Login as root

3. Copy the **scheduler_failover_controller.service** file to the systemd directory. See the bellow list for the correct location based off your environment.

    * **/usr/lib/systemd/system/** for CentOS
    * **/lib/systemd/system/** for Ubuntu
 
4. Edit the **scheduler_failover_controller.service** and change any configurations you would like

    a. Change user and group options as needed to ensure that it is the same as the user and group the main airflow processes are running as
    
5. Enable the service to be ran on startup of the machine

        systemctl enable scheduler_failover_controller

6. You're done!

