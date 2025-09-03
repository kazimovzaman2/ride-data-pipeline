from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import random
import logging
from typing import Tuple

DB_HOST = "taxi-db"
DB_PORT = "5432"
DB_NAME = "taxi_db"
DB_USER = "taxi"
DB_PASSWORD = "taxi"

NYC_LOCATIONS = {
    "Manhattan": {
        "bounds": {
            "lat_min": 40.700,
            "lat_max": 40.800,
            "lon_min": -74.020,
            "lon_max": -73.930,
        },
        "hotspots": [
            {"name": "Times Square", "lat": 40.7580, "lon": -73.9855, "weight": 0.15},
            {"name": "Central Park", "lat": 40.7829, "lon": -73.9654, "weight": 0.10},
            {"name": "JFK Airport", "lat": 40.6413, "lon": -73.7781, "weight": 0.12},
            {
                "name": "LaGuardia Airport",
                "lat": 40.7769,
                "lon": -73.8740,
                "weight": 0.10,
            },
            {"name": "Wall Street", "lat": 40.7074, "lon": -74.0113, "weight": 0.08},
        ],
    },
    "Brooklyn": {
        "bounds": {
            "lat_min": 40.570,
            "lat_max": 40.740,
            "lon_min": -74.050,
            "lon_max": -73.830,
        },
        "hotspots": [
            {
                "name": "Brooklyn Bridge",
                "lat": 40.7061,
                "lon": -73.9969,
                "weight": 0.08,
            },
            {"name": "Williamsburg", "lat": 40.7081, "lon": -73.9571, "weight": 0.06},
        ],
    },
    "Queens": {
        "bounds": {
            "lat_min": 40.540,
            "lat_max": 40.800,
            "lon_min": -73.960,
            "lon_max": -73.700,
        },
        "hotspots": [
            {"name": "Flushing", "lat": 40.7648, "lon": -73.8364, "weight": 0.05},
        ],
    },
}

RATE_ZONES = {
    "standard": {"base_fare": 2.50, "per_mile": 2.50, "per_minute": 0.50},
    "peak_hours": {"base_fare": 2.50, "per_mile": 3.00, "per_minute": 0.75},
    "airport": {
        "base_fare": 2.50,
        "per_mile": 2.50,
        "per_minute": 0.50,
        "airport_surcharge": 4.50,
    },
}

PAYMENT_METHODS = ["Credit Card", "Cash", "Mobile Payment", "Debit Card"]

TAXI_COMPANIES = ["Yellow Cab NYC", "Green Cab NYC", "Uber", "Lyft", "Via"]


def get_weighted_location() -> Tuple[float, float, str, str]:
    """Get a location based on weighted hotspots and random locations"""
    if random.random() < 0.6:
        all_hotspots = []
        for borough, data in NYC_LOCATIONS.items():
            for hotspot in data["hotspots"]:
                all_hotspots.append(
                    {
                        "borough": borough,
                        "name": hotspot["name"],
                        "lat": hotspot["lat"],
                        "lon": hotspot["lon"],
                        "weight": hotspot["weight"],
                    }
                )

        weights = [h["weight"] for h in all_hotspots]
        hotspot = random.choices(all_hotspots, weights=weights)[0]

        lat = hotspot["lat"] + random.uniform(-0.001, 0.001)
        lon = hotspot["lon"] + random.uniform(-0.001, 0.001)

        return round(lat, 6), round(lon, 6), hotspot["borough"], hotspot["name"]
    else:
        borough = random.choice(list(NYC_LOCATIONS.keys()))
        bounds = NYC_LOCATIONS[borough]["bounds"]

        lat = round(random.uniform(bounds["lat_min"], bounds["lat_max"]), 6)
        lon = round(random.uniform(bounds["lon_min"], bounds["lon_max"]), 6)

        return lat, lon, borough, "Street Location"


def calculate_fare(
    distance: float,
    duration_minutes: int,
    pickup_time: datetime,
    pickup_zone: str,
    dropoff_zone: str,
) -> Tuple[float, str]:
    """Calculate fare based on distance, time, and conditions"""

    hour = pickup_time.hour
    is_peak = (7 <= hour <= 9) or (17 <= hour <= 19)
    is_airport = "Airport" in pickup_zone or "Airport" in dropoff_zone

    if is_airport:
        rate_type = "airport"
        rates = RATE_ZONES["airport"]
    elif is_peak:
        rate_type = "peak_hours"
        rates = RATE_ZONES["peak_hours"]
    else:
        rate_type = "standard"
        rates = RATE_ZONES["standard"]

    fare = rates["base_fare"]
    fare += distance * rates["per_mile"]
    fare += duration_minutes * rates["per_minute"]

    if is_airport:
        fare += rates["airport_surcharge"]

    tip_percentage = random.uniform(0, 0.25)
    tip = fare * tip_percentage
    total_fare = fare + tip

    taxes = total_fare * 0.08
    final_fare = total_fare + taxes

    return round(final_fare, 2), rate_type


def generate_realistic_trip() -> dict:
    """Generate a realistic taxi trip with all details"""

    pickup_lat, pickup_lon, pickup_borough, pickup_zone = get_weighted_location()
    pickup_time = datetime.now() - timedelta(minutes=random.randint(0, 180))

    if random.random() < 0.7:
        dropoff_borough = pickup_borough
        bounds = NYC_LOCATIONS[dropoff_borough]["bounds"]
        dropoff_lat = round(random.uniform(bounds["lat_min"], bounds["lat_max"]), 6)
        dropoff_lon = round(random.uniform(bounds["lon_min"], bounds["lon_max"]), 6)
        dropoff_zone = "Street Location"
    else:
        dropoff_lat, dropoff_lon, dropoff_borough, dropoff_zone = (
            get_weighted_location()
        )

    lat_diff = abs(pickup_lat - dropoff_lat)
    lon_diff = abs(pickup_lon - dropoff_lon)
    distance = round((lat_diff + lon_diff) * 69, 2)
    distance = max(0.1, distance)

    base_duration = distance * random.uniform(2, 8)
    traffic_factor = random.uniform(0.8, 2.0)
    duration = int(base_duration * traffic_factor)
    duration = max(5, duration)

    dropoff_time = pickup_time + timedelta(minutes=duration)

    fare_amount, rate_type = calculate_fare(
        distance, duration, pickup_time, pickup_zone, dropoff_zone
    )

    passenger_count = random.choices([1, 2, 3, 4, 5, 6], weights=[50, 25, 15, 7, 2, 1])[
        0
    ]
    payment_method = random.choice(PAYMENT_METHODS)
    taxi_company = random.choice(TAXI_COMPANIES)

    trip_status = random.choices(["completed", "cancelled"], weights=[95, 5])[0]

    return {
        "pickup_datetime": pickup_time,
        "dropoff_datetime": dropoff_time,
        "passenger_count": passenger_count,
        "trip_distance": distance,
        "pickup_latitude": pickup_lat,
        "pickup_longitude": pickup_lon,
        "pickup_borough": pickup_borough,
        "pickup_zone": pickup_zone,
        "dropoff_latitude": dropoff_lat,
        "dropoff_longitude": dropoff_lon,
        "dropoff_borough": dropoff_borough,
        "dropoff_zone": dropoff_zone,
        "fare_amount": fare_amount,
        "payment_method": payment_method,
        "taxi_company": taxi_company,
        "rate_type": rate_type,
        "trip_status": trip_status,
        "trip_duration_minutes": duration,
    }


def insert_realistic_taxi_rides(num_rides=1):
    """Insert realistic taxi rides into the existing table"""
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
        )
        cursor = conn.cursor()

        for _ in range(num_rides):
            trip = generate_realistic_trip()

            cursor.execute(
                """
                INSERT INTO taxi_rides_improved (
                    pickup_datetime, dropoff_datetime, passenger_count, trip_distance,
                    trip_duration_minutes, pickup_latitude, pickup_longitude, pickup_borough,
                    pickup_zone, dropoff_latitude, dropoff_longitude, dropoff_borough,
                    dropoff_zone, fare_amount, payment_method, taxi_company, rate_type, trip_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
                (
                    trip["pickup_datetime"],
                    trip["dropoff_datetime"],
                    trip["passenger_count"],
                    trip["trip_distance"],
                    trip["trip_duration_minutes"],
                    trip["pickup_latitude"],
                    trip["pickup_longitude"],
                    trip["pickup_borough"],
                    trip["pickup_zone"],
                    trip["dropoff_latitude"],
                    trip["dropoff_longitude"],
                    trip["dropoff_borough"],
                    trip["dropoff_zone"],
                    trip["fare_amount"],
                    trip["payment_method"],
                    trip["taxi_company"],
                    trip["rate_type"],
                    trip["trip_status"],
                ),
            )

            logging.info(
                "Inserted trip: %s (%s) -> %s (%s), %.1f mi, $%.2f, %s, %d pax",
                trip["pickup_zone"],
                trip["pickup_borough"],
                trip["dropoff_zone"],
                trip["dropoff_borough"],
                trip["trip_distance"],
                trip["fare_amount"],
                trip["taxi_company"],
                trip["passenger_count"],
            )

        conn.commit()
        cursor.close()
        conn.close()

    except Exception as e:
        logging.error("Error inserting realistic taxi rides: %s", e)
        raise


default_args = {
    "owner": "zaman",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="taxi_data_generator",
    default_args=default_args,
    description="Insert realistic taxi ride data every minute",
    schedule="*/1 * * * *",
    start_date=datetime(2025, 9, 3),
    catchup=False,
    tags=["postgres", "taxi", "data-generator"],
) as dag:

    insert_taxi_data = PythonOperator(
        task_id="insert_taxi_ride_data",
        python_callable=lambda: insert_realistic_taxi_rides(1),
    )
