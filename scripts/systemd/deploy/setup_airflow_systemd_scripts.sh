#!/bin/bash

AIRFLOW_VERSION="1.8.1"
REPO_ARCHIVE_URL="https://github.com/apache/airflow/archive/"
USERNAME="airflow"
GROUP="airflow"

echo "Deploying the systemd services for Airflow v${AIRFLOW_VERSION}"

if [[ "`whoami`" == "root" ]]; then
    echo "Running as root"
else
    echo "You need to run this script as root!!!"
    exit 1
fi

cd /tmp/

rm ${AIRFLOW_VERSION}.zip
rm -rf airflow-${AIRFLOW_VERSION}

wget ${REPO_ARCHIVE_URL}${AIRFLOW_VERSION}.zip
unzip ${AIRFLOW_VERSION}.zip

cd airflow-${AIRFLOW_VERSION}/scripts/systemd/

ENV_DIR=""
POSSIBLE_ENV_DIR_LOCATIONS=( "/etc/sysconfig/" "/etc/systemd/" "/etc/default/" )
for POSSIBLE_ENV_DIR in "${POSSIBLE_ENV_DIR_LOCATIONS[@]}"; do
    if [[ -a "${POSSIBLE_ENV_DIR}" ]]; then
        ENV_DIR="${POSSIBLE_ENV_DIR}"
        break
    fi
done
if [[ "${ENV_DIR}" == "" ]]; then
    echo "Unable to find a place to put the ENV_DIR"
    exit 1
fi

echo "Deploying the Airflow config file to ${ENV_DIR}/airflow"
cp airflow ${ENV_DIR}

echo "Updating systemd service scripts content"
for service_file in `ls -1 *.service`; do
    echo "Updating systemd service file ${service_file}"
    sed -i -e "s/User=airflow/User=${USERNAME}/g" ${service_file}
    sed -i -e "s/Group=airflow/Group=${GROUP}/g" ${service_file}
    sed -i -e "s/EnvironmentFile=\\/etc\\/sysconfig\\/airflow/EnvironmentFile=${ENV_DIR//\//\\/}airflow/g" ${service_file}
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

AIRFLOW_BIN="`which airflow`"

if [ "${AIRFLOW_BIN}" != "/bin/airflow" ] &&  ! [ -a "/bin/airflow" ]; then
    if [[ "${AIRFLOW_BIN}" == "" ]]; then
        echo "Airflow has not been installed. Skipping creation of Symlink."
    else
        ln -s ${AIRFLOW_BIN} /bin/airflow
    fi
fi

echo "Deploy is complete!"
echo ""

echo "To Enable the Services to Startup Automatcially on Restart:"
for service_file in `ls -1 *.service`; do
    echo "$ systemctl enable ${service_file//.service/}"
done
echo ""

echo "To Start the Services:"
for service_file in `ls -1 *.service`; do
    echo "$ systemctl start ${service_file//.service/}"
done
echo ""
