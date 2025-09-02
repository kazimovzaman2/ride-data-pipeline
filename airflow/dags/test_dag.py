from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def hello_airflow():
    print("Hello Airflow! Your DAG is working 🎉")


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id="test_dag",
    default_args=default_args,
    description="A simple test DAG to check Airflow setup",
    start_date=datetime(2025, 9, 1),
    schedule_interval="* * * * *",
    catchup=False,
    tags=["test"],
) as dag:

    hello_task = PythonOperator(task_id="hello_task", python_callable=hello_airflow)

    hello_task
