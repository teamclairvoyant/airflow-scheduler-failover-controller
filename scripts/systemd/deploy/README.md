# Systemd Deploy Scripts 

This directory contains scripts to help you deploy the systemd service scripts quickly. This includes providing a script for deploying the Airflow and Airflow Scheudler Failover Controller Systemd service scripts.
 

## Steps

1. Copy the scripts to the machine you want to setup the systemd service scripts

    * Here are some wget commands you can run to get the scripts on the machine.
    
        wget https://raw.githubusercontent.com/teamclairvoyant/airflow-scheduler-failover-controller/master/scripts/systemd/deploy/setup_airflow_systemd_scripts.sh
        
        wget https://raw.githubusercontent.com/teamclairvoyant/airflow-scheduler-failover-controller/master/scripts/systemd/deploy/setup_scheduler_failover_controller_systemd_scripts.sh

2. Update the scripts with the version of Airflow and the Airflow Scheduler Failover Controller you're using as well as the user and group you would like the job to be running as.

3. Run the scripts as Root with BASH

        bash setup_airflow_systemd_scripts.sh
        
        bash setup_scheduler_failover_controller_systemd_scripts.sh

4. You're done!


## Helpful Tips

### Allow anyone (or specif groups/users) to control the Airflow and Airflow Scheduler Failover Controller Processes
 
To start the processes with the systemd you will need to run the systemctl command. This requires taht you be the root user. In certain cases, you may want to allow other users to control Airflow without giving them root privileges. You can follow these steps to allow users to run "sudo systemctl {start,restart,status,more} airflow-*" commands.

1. Edit the /etc/sudoers file

        sudo nano /etc/sudoers
         
2. Append the following to the bottom of the file

        ALL ALL=(ALL) NOPASSWD: /bin/systemctl * airflow-*
        ALL ALL=(ALL) NOPASSWD: /bin/systemctl * scheduler_failover_controller

    * To restrict it to only allowing a specific user to run the systemctl command, update the first ALL in the each line with the desired username

3. Save the file.
