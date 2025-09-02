#!/bin/bash
echo "Initializing Airflow DB and user..."
airflow db init
airflow users create \
    --username ${_AIRFLOW_WWW_USER_USERNAME} \
    --password ${_AIRFLOW_WWW_USER_PASSWORD} \
    --firstname Zaman \
    --lastname Kazimov \
    --role Admin \
    --email zaman@example.com
echo "Initialization completed."
