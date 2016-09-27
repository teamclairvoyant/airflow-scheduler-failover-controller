#! /bin/bash
# Restart Script for Airflow Scheduler Failover

BIN_DIR=$(dirname "$0")

sh ${BIN_DIR}/shutdown-scheduler-failover.sh

sh ${BIN_DIR}/startup-scheduler-failover.sh
