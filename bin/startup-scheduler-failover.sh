#! /bin/sh
# Startup Scheduler Failover Script for Airflow

echo "Starting Up Scheduler Failover"

LOCAL_DIR=$(dirname "$0")

nohup python ${LOCAL_DIR}/airflow-scheduler-failover.py > /dev/null &
