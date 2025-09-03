from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    count,
    avg,
    sum as spark_sum,
    round as spark_round,
    collect_set,
    first,
    current_timestamp,
    concat_ws,
    max as spark_max,
    lit,
    when,
)
from datetime import datetime

spark = (
    SparkSession.builder.appName("Process Ride Data - Incremental")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.6.0")
    .getOrCreate()
)

db_url = "jdbc:postgresql://taxi-postgres:5432/taxi_db"
db_properties = {"user": "taxi", "password": "taxi", "driver": "org.postgresql.Driver"}

RAW_TABLE = "taxi_rides_improved"
PROCESSED_TABLE = "processed_rides"

print("Starting incremental data processing...")


existing_processed_df = spark.read.jdbc(
    url=db_url, table=PROCESSED_TABLE, properties=db_properties
)

if existing_processed_df.count() > 0:
    last_processed_row = existing_processed_df.agg(
        spark_max("created_at").alias("last_processed")
    ).collect()
    last_processed_time = last_processed_row[0]["last_processed"]
    print(f"Last processed timestamp: {last_processed_time}")
    table_exists = True
else:
    last_processed_time = None
    print("Processed table is empty, processing all data")
    table_exists = True


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

if table_exists and last_processed_time is not None:
    print("🔄 Performing incremental update...")

    existing_df = spark.read.jdbc(
        url=db_url, table=PROCESSED_TABLE, properties=db_properties
    )

    existing_combinations = existing_df.select(
        "pickup_borough", "dropoff_borough"
    ).distinct()
    new_combinations = new_processed_df.select(
        "pickup_borough", "dropoff_borough"
    ).distinct()

    overlapping = existing_combinations.join(
        new_combinations, ["pickup_borough", "dropoff_borough"], "inner"
    )

    overlapping_count = overlapping.count()
    print(f"Borough combinations that need updating: {overlapping_count}")

    if overlapping_count > 0:
        print("📊 Merging metrics for overlapping combinations...")

        existing_overlapping = existing_df.join(
            overlapping, ["pickup_borough", "dropoff_borough"], "inner"
        )

        combined_overlapping = (
            existing_overlapping.alias("e")
            .join(
                new_processed_df.alias("n"),
                ["pickup_borough", "dropoff_borough"],
                "inner",
            )
            .select(
                col("e.pickup_borough"),
                col("e.dropoff_borough"),
                (col("e.total_trips") + col("n.total_trips")).alias("total_trips"),
                spark_round(
                    (
                        (col("e.avg_trip_distance") * col("e.total_trips"))
                        + (col("n.avg_trip_distance") * col("n.total_trips"))
                    )
                    / (col("e.total_trips") + col("n.total_trips")),
                    2,
                ).alias("avg_trip_distance"),
                spark_round(
                    (
                        (col("e.avg_trip_duration_minutes") * col("e.total_trips"))
                        + (col("n.avg_trip_duration_minutes") * col("n.total_trips"))
                    )
                    / (col("e.total_trips") + col("n.total_trips")),
                    2,
                ).alias("avg_trip_duration_minutes"),
                spark_round(
                    (
                        (col("e.avg_fare_amount") * col("e.total_trips"))
                        + (col("n.avg_fare_amount") * col("n.total_trips"))
                    )
                    / (col("e.total_trips") + col("n.total_trips")),
                    2,
                ).alias("avg_fare_amount"),
                concat_ws(
                    ", ", col("e.payment_methods"), col("n.payment_methods")
                ).alias("payment_methods"),
                col("n.most_used_taxi_company").alias("most_used_taxi_company"),
                concat_ws(
                    ", ", col("e.trip_status_summary"), col("n.trip_status_summary")
                ).alias("trip_status_summary"),
                concat_ws(
                    ", ", col("e.rate_type_summary"), col("n.rate_type_summary")
                ).alias("rate_type_summary"),
                current_timestamp().alias("created_at"),
            )
        )

        existing_non_overlapping = existing_df.join(
            overlapping, ["pickup_borough", "dropoff_borough"], "left_anti"
        )

        new_non_overlapping = new_processed_df.join(
            overlapping, ["pickup_borough", "dropoff_borough"], "left_anti"
        )

        final_df = existing_non_overlapping.union(new_non_overlapping).union(
            combined_overlapping
        )

    else:
        print("➕ Appending new combinations...")
        final_df = existing_df.union(new_processed_df)

    print("💾 Writing merged data...")
    final_df.write.mode("overwrite").jdbc(
        url=db_url, table=PROCESSED_TABLE, properties=db_properties
    )

else:
    print("🆕 First run - creating processed table...")
    new_processed_df.write.mode("overwrite").jdbc(
        url=db_url, table=PROCESSED_TABLE, properties=db_properties
    )
    final_df = new_processed_df

print("✅ Successfully completed incremental processing!")

print("\n📊 Processing Summary:")
print(f"- New raw records processed: {new_records_count}")
print(f"- New completed trips: {filtered_df.count()}")
print(f"- Final processed combinations: {final_df.count()}")

print("\n🚖 Sample processed data:")
final_df.select(
    "pickup_borough",
    "dropoff_borough",
    "total_trips",
    "avg_trip_distance",
    "avg_fare_amount",
    "created_at",
).orderBy(col("total_trips").desc()).show(5)

spark.stop()
print("🎉 Spark session closed successfully!")
