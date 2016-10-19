#! /bin/bash
# Restart Script for Airflow Scheduler

BIN_DIR=$(dirname "$0")

sh ${BIN_DIR}/shutdown-scheduler.sh

sh ${BIN_DIR}/startup-scheduler.sh
