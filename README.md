# 🚖 Spark + Airflow Taxi Data Pipeline

A real-time data processing pipeline that generates realistic NYC taxi trip data and processes it using Apache Spark and Apache Airflow.

## Screenshots

<img width="684" height="636" alt="image" src="https://github.com/user-attachments/assets/4e26a7bc-64db-45cc-b9c6-9738814a1852" />
<img width="987" height="636" alt="image" src="https://github.com/user-attachments/assets/184cbe51-7d42-4dc2-8e7c-3e742e800999" />
<img width="1708" height="569" alt="image" src="https://github.com/user-attachments/assets/b7356a31-aeb2-41da-816d-306abe3e996d" />


## Dataset & Processing

**Synthetic NYC Taxi Dataset** - Generated in real-time with:

- **Realistic locations**: Manhattan, Brooklyn, Queens with weighted hotspots (Times Square, JFK, etc.)
- **Dynamic pricing**: Peak hours, airport surcharges, tips, taxes
- **Trip patterns**: 70% intra-borough, 30% cross-borough trips
- **Multiple companies**: Yellow Cab, Green Cab, Uber, Lyft, Via

**Spark Transformations**:

- **Narrow**: `filter`, `withColumn`, `cast` (completed trips only)
- **Wide**: `groupBy`, `join`, `agg` (borough-to-borough analytics)

## Quick Start

### 1. Setup Network

```bash
docker network create spark-net
```

### 2. Start Services

```bash
# PostgreSQL
cd postgres/
docker-compose up -d

# Spark Cluster
cd standalone-spark/
docker-compose up -d

# Airflow
cd airflow/
docker-compose up -d
```

### 4. Enable DAGs

1. Navigate to Airflow UI
2. Enable `taxi_data_generator` (runs every 1 minute)
3. Enable `process_rides_spark_bash_dag` (runs every 10 minutes)

## Project Structure

```
├── airflow/                    # Airflow setup
│   ├── docker-compose.yml
│   └── dags/
│       ├── taxi_data_generator.py      # Data generation DAG
│       ├── process_ride_data.py        # Spark processing script
│       └── process_rides_spark_dag.py  # Spark DAG
├── standalone-spark/           # Spark cluster setup
│   └── docker-compose.yml
└── postgres/                   # PostgreSQL setup
    └── docker-compose.yml
```

## Data Pipeline Flow

### 1. Data Generation (Every 1 minute)

```python
# Generates realistic taxi trips with:
- Weighted location selection (60% hotspots, 40% random)
- Dynamic fare calculation (base + distance + time + surcharges)
- Peak hour detection (7-9 AM, 5-7 PM)
- Airport surcharges for JFK/LaGuardia trips
```

### 2. Data Processing (Every 10 minutes)

```python
# Spark incremental processing:
- Filters completed trips only
- Groups by pickup_borough + dropoff_borough
- Calculates averages (distance, duration, fare)
- Merges with existing processed data
- Handles overlapping combinations intelligently
```

## Key Features

✅ **Real-time data generation** with realistic NYC patterns  
✅ **Incremental processing** - only new data processed  
✅ **Fault tolerance** - Airflow retries and Spark resilience  
✅ **Scalable architecture** - Dockerized microservices  
✅ **Both narrow & wide transformations** in Spark  
✅ **Scheduled orchestration** with different intervals

## Sample Output

```
Processing Summary:
- New raw records processed: 12
- New completed trips: 11
- Final processed combinations: 6

Sample processed data:
+---------------+----------------+-----------+------------------+---------------+
|pickup_borough |dropoff_borough |total_trips|avg_trip_distance |avg_fare_amount|
+---------------+----------------+-----------+------------------+---------------+
|Manhattan      |Manhattan       |145        |2.34              |18.67          |
|Manhattan      |Brooklyn        |23         |4.12              |28.45          |
|Queens         |Manhattan       |18         |5.67              |31.23          |
+---------------+----------------+-----------+------------------+---------------+
```

### Airflow DAGs

_Add screenshot of Airflow DAGs page showing both DAGs enabled_

### Spark Job Execution

_Add screenshot of Spark UI showing job execution_

### Data Processing Results

_Add screenshot of successful DAG runs_

## Technologies Used

- **Apache Spark** - Distributed data processing
- **Apache Airflow** - Workflow orchestration
- **PostgreSQL** - Data storage (raw + processed)
- **Docker** - Containerization
- **Python** - Data generation and processing logic

---

_Built for demonstration of real-time data pipeline architecture_
