from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "zaman",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 3),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="process_rides_spark_bash_dag",
    default_args=default_args,
    description="Incremental processing of taxi rides using Spark via BashOperator",
    schedule="*/10 * * * *",
    catchup=False,
    tags=["spark", "bash", "etl"],
) as dag:

    spark_submit_task = BashOperator(
        task_id="process_rides_task",
        bash_command="""
#!/bin/bash
spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.eventLog.enabled=false \
  /opt/spark_jobs/process_ride_data.py
""",
    )
