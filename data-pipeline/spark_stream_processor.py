#!/usr/bin/env python3
"""
E-Commerce Real-Time Stream Processor (PySpark)
Consumes clickstream and order streams from Kafka, processes sliding window aggregations,
computes a real-time Popularity Index, and writes results to Cassandra.
"""

import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, to_timestamp, window, count, when, lit, current_timestamp
)
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("spark_stream_processor")

# Configuration via environment variables
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST", "localhost")
CASSANDRA_PORT = os.getenv("CASSANDRA_PORT", "9042")
CHECKPOINT_DIR = os.getenv("CHECKPOINT_DIR", "/tmp/spark-checkpoints/product_popularity")

# Maven packages required for spark-submit
SUBMIT_PACKAGES = (
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
    "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1"
)


def create_spark_session() -> SparkSession:
    """
    Initializes SparkSession with Kafka and Cassandra connector packages.
    """
    logger.info("Initializing SparkSession...")
    # Inject packages if running standalone
    os.environ["PYSPARK_SUBMIT_ARGS"] = f"--packages {SUBMIT_PACKAGES} pyspark-shell"
    
    return SparkSession.builder \
        .appName("EcommerceRealTimeProcessor") \
        .config("spark.cassandra.connection.host", CASSANDRA_HOST) \
        .config("spark.cassandra.connection.port", CASSANDRA_PORT) \
        .config("spark.sql.shuffle.partitions", "2") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .getOrCreate()


def main() -> None:
    # 1. Initialize Spark Session
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 2. Define Schema Rules
    clicks_schema = StructType([
        StructField("user_id", StringType(), False),
        StructField("action", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("session_id", StringType(), False),
        StructField("timestamp", StringType(), False)
    ])

    orders_schema = StructType([
        StructField("order_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("product_id", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("quantity", IntegerType(), False),
        StructField("status", StringType(), False),
        StructField("timestamp", StringType(), False)
    ])

    logger.info(f"Reading from Kafka bootstrap servers: {KAFKA_BOOTSTRAP_SERVERS}")

    # 3. Read Streaming Clicks
    clicks_raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "clicks") \
        .option("startingOffsets", "latest") \
        .load()

    clicks_parsed_df = clicks_raw_df \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), clicks_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))

    # 4. Read Streaming Orders
    orders_raw_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", "orders") \
        .option("startingOffsets", "latest") \
        .load()

    orders_parsed_df = orders_raw_df \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), orders_schema).alias("data")) \
        .select("data.*") \
        .withColumn("timestamp", to_timestamp(col("timestamp")))

    # 5. Union Stream Events
    # Clicks mapped to standard schema
    clicks_standardized = clicks_parsed_df.select(
        col("product_id"),
        col("timestamp"),
        lit("click").alias("event_type")
    )

    # Orders filtered by successful purchases and mapped to standard schema
    orders_standardized = orders_parsed_df \
        .filter(col("status") == "success") \
        .select(
            col("product_id"),
            col("timestamp"),
            lit("order").alias("event_type")
        )

    # Union the streams to count both events without losing un-ordered clicks
    unified_events = clicks_standardized.union(orders_standardized)

    # 6. Apply Watermark and 10-Minute Sliding Window (sliding every 2 minutes)
    windowed_events = unified_events \
        .withWatermark("timestamp", "2 minutes") \
        .groupBy(
            window(col("timestamp"), "10 minutes", "2 minutes"),
            col("product_id")
        )

    # 7. Compute Click/Order Counts and Dynamic Popularity Index
    # Popularity Index = (Click Count * 1.0) + (Order Count * 5.0)
    popularity_metrics = windowed_events.agg(
        count(when(col("event_type") == "click", 1)).alias("click_count"),
        count(when(col("event_type") == "order", 1)).alias("order_count")
    ).withColumn(
        "popularity_index",
        (col("click_count").cast(DoubleType()) * 1.0) + (col("order_count").cast(DoubleType()) * 5.0)
    ).withColumn(
        "last_updated",
        current_timestamp()
    )

    # Prepare DataFrame matching Cassandra database column types
    cassandra_output_df = popularity_metrics.select(
        col("product_id").cast(StringType()),
        col("click_count").cast(IntegerType()),
        col("order_count").cast(IntegerType()),
        col("popularity_index").cast(DoubleType()),
        col("last_updated")
    )

    # 8. Write Aggregate Streaming Results to Cassandra
    logger.info(f"Writing streaming query results to Cassandra host: {CASSANDRA_HOST}:{CASSANDRA_PORT}")
    logger.info(f"Using checkpoint directory: {CHECKPOINT_DIR}")

    query = cassandra_output_df.writeStream \
        .format("org.apache.spark.sql.cassandra") \
        .option("keyspace", "ecommerce_analytics") \
        .option("table", "product_popularity") \
        .option("checkpointLocation", CHECKPOINT_DIR) \
        .outputMode("update") \
        .start()

    # Block until query completes or is stopped
    query.awaitTermination()


if __name__ == "__main__":
    main()
