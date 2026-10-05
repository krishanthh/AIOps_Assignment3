import argparse
import json
import os
import time

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, udf, when, unix_micros
from pyspark.sql.types import DoubleType


def average_speed(distance, duration_hours):
    """Average speed in miles/hour."""
    if distance is None or duration_hours is None or duration_hours <= 0:
        return None
    return float(distance / duration_hours)


average_speed_udf = udf(average_speed, DoubleType())


def build_spark():
    return (
        SparkSession.builder
        .appName("DA3408-Spark-Pipeline")
        .master(os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077"))
        .config("spark.sql.shuffle.partitions", os.getenv("SHUFFLE_PARTITIONS", "80"))
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.autoBroadcastJoinThreshold", "-1")
        .getOrCreate()
    )


def build_pipeline(spark, input_path, zones_path, mode):
    # ---------------------------------------------------------
    # 1. INGESTION
    # ---------------------------------------------------------
    trips = spark.read.parquet(input_path)

    trips = trips.select(
        "VendorID",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "trip_distance",
        "PULocationID",
        "DOLocationID",
        "fare_amount"
    )

    # ---------------------------------------------------------
    # 2. CLEANSING
    # ---------------------------------------------------------
    trips = trips.dropna(
        subset=[
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "trip_distance",
            "PULocationID",
            "DOLocationID"
        ]
    )

    trips = trips.dropDuplicates()

    trips = (
        trips
        .withColumn(
            "tpep_pickup_datetime",
            to_timestamp("tpep_pickup_datetime")
        )
        .withColumn(
            "tpep_dropoff_datetime",
            to_timestamp("tpep_dropoff_datetime")
        )
        .withColumn(
            "trip_distance",
            col("trip_distance").cast("double")
        )
    )

    # ---------------------------------------------------------
    # 3. TAXI ZONE LOOKUP
    # ---------------------------------------------------------
    zones = (
        spark.read
        .option("header", True)
        .csv(zones_path)
        .select(
            col("LocationID").cast("long").alias("LocationID"),
            "Borough",
            "Zone",
            "service_zone"
        )
    )

    pickup_zones = zones.select(
        col("LocationID").alias("PULocationID"),
        col("Borough").alias("pickup_borough"),
        col("Zone").alias("pickup_zone"),
        col("service_zone").alias("pickup_service_zone")
    )

    dropoff_zones = zones.select(
        col("LocationID").alias("DOLocationID"),
        col("Borough").alias("dropoff_borough"),
        col("Zone").alias("dropoff_zone"),
        col("service_zone").alias("dropoff_service_zone")
    )

    # ---------------------------------------------------------
    # 4. HEAVY JOIN
    # ---------------------------------------------------------
    trips = trips.join(
        pickup_zones,
        on="PULocationID",
        how="inner"
    )

    trips = trips.join(
        dropoff_zones,
        on="DOLocationID",
        how="inner"
    )

    # ---------------------------------------------------------
    # 5. DERIVED FEATURE
    # ---------------------------------------------------------
    trips = trips.withColumn(
        "trip_duration_hours",
        (
            unix_micros(col("tpep_dropoff_datetime"))
            - unix_micros(col("tpep_pickup_datetime"))
        ) / 3600000000.0
    )

    # ---------------------------------------------------------
    # 6. FEATURE IMPLEMENTATION
    # ---------------------------------------------------------
    if mode == "udf":
        trips = trips.withColumn(
            "average_speed_mph",
            average_speed_udf(
                col("trip_distance"),
                col("trip_duration_hours")
            )
        )

    elif mode == "native":
        trips = trips.withColumn(
            "average_speed_mph",
            when(
                col("trip_duration_hours") > 0,
                col("trip_distance") / col("trip_duration_hours")
            ).otherwise(None)
        )

    else:
        raise ValueError("mode must be 'udf' or 'native'")

    return trips


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--input", required=True)
    parser.add_argument("--zones", required=True)
    parser.add_argument("--output", required=True)

    parser.add_argument(
        "--mode",
        choices=["udf", "native"],
        default="udf"
    )

    parser.add_argument("--hold-ui-seconds", type=float, default=0.0)
    args = parser.parse_args()

    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    # Start timing immediately before ingestion.
    start = time.perf_counter()

    trips = build_pipeline(
        spark,
        args.input,
        args.zones,
        args.mode
    )

    # ---------------------------------------------------------
    # 7. EXPORT
    # ---------------------------------------------------------
    trips.write.mode("overwrite").parquet(args.output)

    total_time = time.perf_counter() - start

    result = {
        "framework": "spark",
        "mode": args.mode,
        "total_time_seconds": round(total_time, 3),
        "input": args.input,
        "output": args.output
    }

    print("\n==========================================")
    print("SPARK BENCHMARK COMPLETED")
    print("==========================================")
    print(json.dumps(result))
    print("==========================================\n")

    if args.hold_ui_seconds:
        time.sleep(args.hold_ui_seconds)
    spark.stop()


if __name__ == "__main__":
    main()
