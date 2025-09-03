from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import random
import string
import logging

DB_HOST = "taxi-db"
DB_PORT = "5432"
DB_NAME = "taxi_db"
DB_USER = "taxi"
DB_PASSWORD = "taxi"


def random_string(length=6):
    return "".join(random.choice(string.ascii_uppercase) for _ in range(length))


def insert_random_taxi_ride():
    """Insert a random taxi ride into taxi_rides table"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cursor = conn.cursor()

        pickup_datetime = datetime.now() - timedelta(minutes=random.randint(0, 60))
        dropoff_datetime = pickup_datetime + timedelta(minutes=random.randint(5, 60))
        passenger_count = random.randint(1, 4)
        trip_distance = round(random.uniform(0.5, 20.0), 2)
        pickup_location = random_string()
        dropoff_location = random_string()
        fare_amount = round(random.uniform(5, 100), 2)

        cursor.execute(
            """
            INSERT INTO taxi_rides (
                pickup_datetime, dropoff_datetime, passenger_count,
                trip_distance, pickup_location, dropoff_location, fare_amount
            ) VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                pickup_datetime,
                dropoff_datetime,
                passenger_count,
                trip_distance,
                pickup_location,
                dropoff_location,
                fare_amount,
            ),
        )
        conn.commit()
        logging.info(
            "Inserted random taxi ride: %s -> %s", pickup_location, dropoff_location
        )

        cursor.close()
        conn.close()
    except Exception as e:
        logging.error("Error inserting taxi ride: %s", e)
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
    dag_id="insert_random_taxi_ride_dag",
    default_args=default_args,
    description="Insert a random taxi ride into PostgreSQL every minute",
    schedule="*/1 * * * *",
    start_date=datetime(2025, 9, 3),
    catchup=False,
    tags=["postgres", "taxi"],
) as dag:

    insert_random_task = PythonOperator(
        task_id="insert_random_taxi_ride",
        python_callable=insert_random_taxi_ride,
    )

    insert_random_task
