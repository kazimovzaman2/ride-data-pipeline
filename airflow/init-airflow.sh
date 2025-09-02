#!/bin/bash

if [[ -z "${AIRFLOW_UID}" ]]; then
    echo
    echo -e "\033[1;33mWARNING!!!: AIRFLOW_UID not set!\e[0m"
    echo "Setting AIRFLOW_UID to default value"
    export AIRFLOW_UID=50000
fi

# Check system resources
one_meg=1048576
mem_available=$(($(getconf _PHYS_PAGES) * $(getconf PAGE_SIZE) / one_meg))
cpus_available=$(grep -cE 'cpu[0-9]+' /proc/stat)
disk_available=$(df / | tail -1 | awk '{print $4}')
warning_resources="false"

if (( mem_available < 4000 )) ; then
    echo
    echo -e "\033[1;33mWARNING!!!: Not enough memory available for Docker.\e[0m"
    echo "At least 4GB of memory required. You have $(numfmt --to iec $((mem_available * one_meg)))"
    echo
    warning_resources="true"
fi

if (( cpus_available < 2 )); then
    echo
    echo -e "\033[1;33mWARNING!!!: Not enough CPUS available for Docker.\e[0m"
    echo "At least 2 CPUs recommended. You have ${cpus_available}"
    echo
    warning_resources="true"
fi

if [[ ${warning_resources} == "true" ]]; then
    echo
    echo -e "\033[1;33mWARNING!!!: You have not enough resources to run Airflow!\e[0m"
    echo
fi

echo "Creating missing Airflow directories:"
mkdir -v -p /opt/airflow/{logs,dags,config}

echo
echo "Airflow version:"
/entrypoint airflow version

echo
echo "Files in shared volumes:"
ls -la /opt/airflow/{logs,dags,config}

echo
echo "Running airflow config list to create default config file if missing."
/entrypoint airflow config list >/dev/null

echo
echo "Change ownership of files in /opt/airflow to ${AIRFLOW_UID}:0"
chown -R "${AIRFLOW_UID}:0" /opt/airflow/

echo
echo "Initialization completed successfully!"
