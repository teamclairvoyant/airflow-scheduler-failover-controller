#! /bin/sh
# Startup Scheduler Failover Script for Airflow

echo "Starting Up Scheduler Failover"

BIN_DIR=$(dirname "$0")

python ${BIN_DIR}/airflow-scheduler-failover.py >> ~/airflow/logs/scheduler-failover.logs &
