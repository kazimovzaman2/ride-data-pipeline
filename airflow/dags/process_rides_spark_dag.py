from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import datetime, timedelta

default_args = {
    "owner": "zaman",
    "depends_on_past": False,
    "start_date": datetime(2025, 9, 3),
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="process_rides_spark_dag",
    default_args=default_args,
    description="Incremental processing of taxi rides using Spark",
    schedule="*/10 * * * *",
    start_date=datetime(2025, 9, 3),
    catchup=False,
    tags=["spark", "etl", "taxi"],
) as dag:

    process_rides = SparkSubmitOperator(
        task_id="process_rides_task",
        application="/opt/spark_jobs/process_ride_data.py",
        conn_id="spark_default",
        verbose=True,
        name="process_rides_job",
        **{
            "spark_binary": "/opt/spark/bin/spark-submit",
            "application_args": [],
            "conf": {
                "spark.master": "spark://spark-master:7077",
                "spark.driver.memory": "2g",
                "spark.executor.memory": "2g",
                "spark.jars": "/opt/spark/jars/postgresql-42.6.0.jar",
            },
        },
    )

    process_rides
