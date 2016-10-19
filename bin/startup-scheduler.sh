#! /bin/sh
# Startup Scheduler Script for Airflow 

echo "Starting Up Scheduler"

nohup airflow scheduler >> ~/airflow/logs/scheduler.logs &
