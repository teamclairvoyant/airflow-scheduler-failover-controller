#! /bin/bash
# Shutdown Script for Airflow scheduler failover

echo "Shutting Down Scheduler Failover"

#Kill the Airflow scheduler parent process
for pid in `ps -ef | grep "airflow-scheduler-failover.py" | awk '{print $2}'` ; do kill -9 $pid ; done
