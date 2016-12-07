#!/bin/bash

AIRFLOW_VERSION="1.7.0"
REPO_ARCHIVE_URL="https://github.com/apache/incubator-airflow/archive/"

echo "Deploying the systemd services for Airflow v${AIRFLOW_VERSION}"

if [[ "`whoami`" == "root" ]]; then
    echo "Running as root"
else
    echo "You need to run this script as root!!!"
    exit 1
fi

cd /tmp/

rm ${AIRFLOW_VERSION}.zip
rm -rf incubator-airflow-${AIRFLOW_VERSION}

wget ${REPO_ARCHIVE_URL}${AIRFLOW_VERSION}.zip
unzip ${AIRFLOW_VERSION}.zip

cd incubator-airflow-${AIRFLOW_VERSION}/scripts/systemd/

echo "Deploying the Airflow config file to /etc/sysconfig/airflow"
cp airflow /etc/sysconfig/

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

echo "Please update the contents of the /usr/lib/systemd/system/scheduler_failover_controller.service files with the desired user you wish to run it as:"
echo "$ sudo nano /usr/lib/systemd/system/scheduler_failover_controller.service"

