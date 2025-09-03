from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import logging
import random
import string

DB_HOST = "taxi-db"
DB_PORT = "5432"
DB_NAME = "taxi_db"
DB_USER = "taxi"
DB_PASSWORD = "taxi"


def fetch_taxi_data():
    """Fetch the latest taxi ride records from the database"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cursor = conn.cursor()

        cursor.execute(
            """
            SELECT ride_id, pickup_datetime, dropoff_datetime,
                   passenger_count, trip_distance, pickup_location,
                   dropoff_location, fare_amount, created_at
            FROM taxi_rides
            ORDER BY created_at DESC
            LIMIT 10;
        """
        )

        rows = cursor.fetchall()
        logging.info("Fetched %d rows", len(rows))

        for row in rows:
            logging.info(row)

        cursor.close()
        conn.close()
    except Exception as e:
        logging.error("Error fetching taxi data: %s", e)
        raise


def random_owner(length=8):
    letters = string.ascii_lowercase
    return "".join(random.choice(letters) for _ in range(length))


default_args = {
    "owner": random_owner(),
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="fetch_taxi_data_dag",
    default_args=default_args,
    description="Fetch taxi rides from PostgreSQL every minute",
    schedule_interval="*/1 * * * *",
    start_date=datetime(2025, 9, 3),
    catchup=False,
    tags=["postgres", "taxi"],
) as dag:

    fetch_data_task = PythonOperator(
        task_id="fetch_taxi_data",
        python_callable=fetch_taxi_data,
    )

    fetch_data_task
