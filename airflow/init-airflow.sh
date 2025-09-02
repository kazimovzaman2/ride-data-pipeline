#!/bin/bash

echo "Running as root user - no permission issues!"

mem_available=$(awk '/MemAvailable/{printf "%.0f", $2/1024}' /proc/meminfo 2>/dev/null || echo "4000")
cpus_available=$(nproc 2>/dev/null || echo "2")

echo "System resources:"
echo "- Memory: ${mem_available}MB"
echo "- CPUs: ${cpus_available}"

if [ "$mem_available" -lt 4000 ]; then
    echo -e "\033[1;33mWARNING: Less than 4GB memory detected\e[0m"
fi

echo
echo "Creating Airflow directories:"
mkdir -p /opt/airflow/logs /opt/airflow/dags /opt/airflow/config
chmod 755 /opt/airflow/logs /opt/airflow/dags /opt/airflow/config

echo
echo "Airflow version:"
/entrypoint airflow version

echo
echo "Files in /opt/airflow:"
ls -la /opt/airflow/

echo
echo "Initialization completed successfully!"
