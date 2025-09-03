from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    round as spark_round,
    collect_set,
    first,
    current_timestamp,
    concat_ws,
    max as spark_max,
    lit,
)
from datetime import datetime

spark = (
    SparkSession.builder.appName("Process Ride Data - Incremental")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    .getOrCreate()
)

db_url = "jdbc:postgresql://taxi-postgres:5432/taxi_db"
db_properties = {"user": "taxi", "password": "taxi", "driver": "org.postgresql.Driver"}

RAW_TABLE = "raw_ride_data"
PROCESSED_TABLE = "processed_ride_data"

print("Starting incremental data processing...")

try:
    existing_processed_df = spark.read.jdbc(
        url=db_url, table=PROCESSED_TABLE, properties=db_properties
    )

    if existing_processed_df.count() > 0:
        last_processed_row = existing_processed_df.agg(
            spark_max("created_at").alias("last_processed")
        ).collect()
        last_processed_time = last_processed_row[0]["last_processed"]
        print(f"Last processed timestamp: {last_processed_time}")
    else:
        last_processed_time = None
        print("Processed table is empty, processing all data")

except Exception as e:
    last_processed_time = None
    print(f"Processed table doesn't exist or error reading it: {e}")
    print("Processing all data...")

raw_df = spark.read.jdbc(url=db_url, table=RAW_TABLE, properties=db_properties)

print(f"Total raw records in database: {raw_df.count()}")

if last_processed_time is not None:
    new_raw_df = raw_df.filter(col("created_at") > lit(last_processed_time))
    print(f"New records since last processing: {new_raw_df.count()}")
else:
    new_raw_df = raw_df
    print("Processing all records (first run)")

new_records_count = new_raw_df.count()
if new_records_count == 0:
    print("No new records to process. Exiting...")
    spark.stop()
    exit(0)

filtered_df = new_raw_df.filter(col("trip_status") == "completed").withColumn(
    "fare_amount_numeric", col("fare_amount").cast("double")
)

print(f"New completed trips to process: {filtered_df.count()}")

new_processed_df = (
    filtered_df.groupBy("pickup_borough", "dropoff_borough")
    .agg(
        count("*").alias("total_trips"),
        spark_round(avg("trip_distance"), 2).alias("avg_trip_distance"),
        spark_round(avg("trip_duration_minutes"), 2).alias("avg_trip_duration_minutes"),
        spark_round(avg("fare_amount_numeric"), 2).alias("avg_fare_amount"),
        concat_ws(", ", collect_set("payment_method")).alias("payment_methods"),
        first("taxi_company").alias("most_used_taxi_company"),
        concat_ws(", ", collect_set("trip_status")).alias("trip_status_summary"),
        concat_ws(", ", collect_set("rate_type")).alias("rate_type_summary"),
    )
    .withColumn("created_at", current_timestamp())
)

print("New processed data sample:")
new_processed_df.show(10, truncate=False)

if last_processed_time is not None and existing_processed_df.count() > 0:
    print("Merging new processed data with existing data...")

    existing_combinations = existing_processed_df.select(
        "pickup_borough", "dropoff_borough"
    )
    new_combinations = new_processed_df.select("pickup_borough", "dropoff_borough")

    overlapping = existing_combinations.join(
        new_combinations, ["pickup_borough", "dropoff_borough"], "inner"
    ).distinct()

    overlapping_count = overlapping.count()
    print(f"Borough combinations that need merging: {overlapping_count}")

    if overlapping_count > 0:
        existing_to_keep = existing_processed_df.join(
            overlapping, ["pickup_borough", "dropoff_borough"], "left_anti"
        )

        final_processed_df = existing_to_keep.union(new_processed_df)

        print(f"Final merged records: {final_processed_df.count()}")

    else:
        final_processed_df = existing_processed_df.union(new_processed_df)
        print(f"Final combined records: {final_processed_df.count()}")

else:
    final_processed_df = new_processed_df
    print(f"First run - total processed records: {final_processed_df.count()}")

print("Writing processed data to PostgreSQL...")

final_processed_df.write.mode("overwrite").jdbc(
    url=db_url, table=PROCESSED_TABLE, properties=db_properties
)

print("✅ Successfully completed incremental processing!")

print("\n📊 Processing Summary:")
print(f"- New raw records processed: {new_records_count}")
print(f"- New completed trips: {filtered_df.count()}")
print(f"- Final processed combinations: {final_processed_df.count()}")

print("\n🚖 Sample processed data:")
final_processed_df.select(
    "pickup_borough",
    "dropoff_borough",
    "total_trips",
    "avg_trip_distance",
    "avg_fare_amount",
    "created_at",
).orderBy(col("total_trips").desc()).show(5)

spark.stop()
print("🎉 Spark session closed successfully!")
