#!/bin/bash

SCHEDULER_FAILOVER_CONTROLLER_VERSION="v1.0.8"
REPO_ARCHIVE_URL="https://github.com/teamclairvoyant/airflow-scheduler-failover-controller/archive/"
USERNAME="airflow"
GROUP="airflow"

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

echo "Updating systemd service scripts content"
for service_file in `ls -1 *.service`; do
    echo "Updating systemd service file ${service_file}"
    sed -i -e "s/User=airflow/User=${USERNAME}/g" ${service_file}
    sed -i -e "s/Group=airflow/Group=${GROUP}/g" ${service_file}
done

SYSTEMD_SERVICES_DIR=""
POSSIBLE_SYSTEMD_SERVICES_DIR_LOCATIONS=( "/usr/lib/systemd/system/" "/lib/systemd/system/" )
for POSSIBLE_SYSTEMD_SERVICES_DIR in "${POSSIBLE_SYSTEMD_SERVICES_DIR_LOCATIONS[@]}"; do
    if [[ -a ${POSSIBLE_SYSTEMD_SERVICES_DIR} ]]; then
        SYSTEMD_SERVICES_DIR="${POSSIBLE_SYSTEMD_SERVICES_DIR}"
        break
    fi
done
if [[ "${SYSTEMD_SERVICES_DIR}" == "" ]]; then
    echo "Unable to find a place to put the Systemd Service Files"
    exit 1
fi

echo "Deploying systemd service files to ${SYSTEMD_SERVICES_DIR}"
cp *.service ${SYSTEMD_SERVICES_DIR}

if [ "${SCHEDULER_FAILOVER_CONTROLLER_VERSION_BIN}" != "/bin/scheduler_failover_controller" ] &&  ! [ -a "/bin/scheduler_failover_controller" ]; then
    if [[ "${SCHEDULER_FAILOVER_CONTROLLER_VERSION_BIN}" == "" ]]; then
        echo "The Scheduler Failover Controller has not been installed. Skipping creation of Symlink."
    else
        ln -s ${SCHEDULER_FAILOVER_CONTROLLER_VERSION_BIN} /bin/scheduler_failover_controller
    fi
fi

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
