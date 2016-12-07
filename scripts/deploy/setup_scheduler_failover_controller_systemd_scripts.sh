#!/bin/bash

SCHEDULER_FAILOVER_CONTROLLER_VERSION="v1.0.0"
REPO_ARCHIVE_URL="https://github.com/teamclairvoyant/airflow-scheduler-failover-controller/archive/"

echo "Deploying the systemd services for Airflow Scheduler Failover Controller ${SCHEDULER_FAILOVER_CONTROLLER_VERSION}"

if [[ "`whoami`" == "root" ]]; then
    echo "Running as root"
else
    echo "You need to run this script as root!!!"
    exit 1
fi

cd /tmp/

rm ${SCHEDULER_FAILOVER_CONTROLLER_VERSION}.zip
rm -rf airflow-scheduler-failover-controller-${SCHEDULER_FAILOVER_CONTROLLER_VERSION//v/}

wget ${REPO_ARCHIVE_URL}${SCHEDULER_FAILOVER_CONTROLLER_VERSION}.zip
unzip ${SCHEDULER_FAILOVER_CONTROLLER_VERSION}.zip

cd airflow-scheduler-failover-controller-${SCHEDULER_FAILOVER_CONTROLLER_VERSION//v/}/scripts/systemd/

echo "Deploying systemd service files to /usr/lib/systemd/system/"
cp *.service /usr/lib/systemd/system/

echo "Deploy is complete!"
echo ""

echo "To Enable the Services to Startup Automatcially on Restart:"
for line in `ls -1 *.service`; do
    echo "$ systemctl enable ${line//.service/}"
done
echo ""

echo "To Start the Services:"
for line in `ls -1 *.service`; do
    echo "$ systemctl start ${line//.service/}"
done
echo ""

echo "Please update the contents of the /usr/lib/systemd/system/airflow-*.service files with the desired user you wish to run it as:"
echo "$ sudo nano /usr/lib/systemd/system/airflow-*.service"


