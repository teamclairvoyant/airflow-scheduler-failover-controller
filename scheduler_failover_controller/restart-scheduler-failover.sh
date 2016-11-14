#! /bin/bash
# Restart Script for Airflow Scheduler Failover

LOCAL_DIR=$(dirname "$0")

sh ${LOCAL_DIR}/shutdown-scheduler-failover.sh

sh ${LOCAL_DIR}/startup-scheduler-failover.sh
