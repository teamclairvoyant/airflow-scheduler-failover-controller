#! /bin/bash
# Shutdown Script for Airflow scheduler

echo "Shutting Down Scheduler"

#Kill the Airflow scheduler parent process
for pid in `ps -ef | grep "airflow scheduler" | awk '{print $2}'` ; do kill -9 $pid ; done
