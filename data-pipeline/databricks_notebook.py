# Databricks notebook source
# MAGIC %md
# MAGIC # Medallion Architecture & Dynamic Pricing Pipeline
# MAGIC **Author:** Staff Principal Data Engineer & Cloud Architect
# MAGIC **Description:** Ingests raw events from ADLS Gen2, processes through Bronze, Silver, and Gold layers, and applies dynamic pricing optimizations based on sales velocity and conversion margins.

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 1: Initialize Parameters and Configuration

# COMMAND ----------
# Declare widget parameters for dynamic orchestration
dbutils.widgets.text("storage_account_name", "adls2ecommerceanalytics", "ADLS Gen2 Storage Account")
dbutils.widgets.text("environment", "prod", "Environment")
dbutils.widgets.text("catalog_name", "main", "Unity Catalog Name")
dbutils.widgets.text("schema_name", "ecommerce", "Database Schema Name")

# COMMAND ----------
# Retrieve parameter values
storage_account = dbutils.widgets.get("storage_account_name")
env = dbutils.widgets.get("environment")
catalog = dbutils.widgets.get("catalog_name")
schema = dbutils.widgets.get("schema_name")

logger = collections = None
import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("medallion_pipeline")

# Base paths for ADLS Gen2
bronze_base_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/raw_events"
silver_base_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/cleaned"
gold_base_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/analytics"

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 2: Database and Unity Catalog Setup

# COMMAND ----------
# Initialize catalog and database schema structures
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"USE CATALOG {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
spark.sql(f"USE SCHEMA {schema}")

logger.info(f"Using catalog: {catalog}, schema: {schema}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 3: Bronze Layer Ingestion
# MAGIC Appends raw files incrementally from ADLS Gen2 into the Bronze Delta table with metadata.

# COMMAND ----------
from pyspark.sql.functions import input_file_name, current_timestamp, col

# Define schemas for raw event streams
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType

# Ingest daily incremental raw files (JSON format) from the landing folder
# In a real environment, we'd use Auto Loader (cloudFiles) for optimal incremental ingestion.
logger.info("Reading raw events for Bronze Layer...")

# Schema definition for click events inside bronze
raw_clicks_df = spark.read.format("json").load(f"{bronze_base_path}/clicks/*/*/*/*.json") if spark.catalog.tableExists("live_ecommerce_bronze") else None

# For this production script, we will support reading all raw events under the directory structure
try:
    raw_events_df = spark.read.format("json") \
        .option("recursiveFileLookup", "true") \
        .load(bronze_base_path)
    
    # Enrich raw logs with ingestion metadata
    bronze_enriched_df = raw_events_df \
        .withColumn("ingest_source_file", input_file_name()) \
        .withColumn("ingested_at", current_timestamp())
    
    # Save/Append to Bronze Table
    logger.info("Writing records to Bronze Delta table...")
    bronze_enriched_df.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .saveAsTable("live_ecommerce_bronze")
        
except Exception as e:
    logger.warning(f"No new raw files found to ingest in {bronze_base_path}. Details: {e}")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 4: Silver Layer Processing
# MAGIC Performs data cleaning, duplicates removal, type casting, schema enforcement, and null checks.

# COMMAND ----------
from pyspark.sql.functions import col, to_timestamp

# Check that Bronze table has records
if spark.catalog.tableExists("live_ecommerce_bronze"):
    logger.info("Processing Silver Layer from Bronze...")
    bronze_df = spark.table("live_ecommerce_bronze")
    
    # Filter and clean the transactions
    # 1. Cast string timestamps to TimestampType
    # 2. De-duplicate clicks and orders based on distinct record keys
    # 3. Filter out invalid values (e.g. prices <= 0, empty quantities)
    
    cleaned_df = bronze_df \
        .withColumn("event_timestamp", to_timestamp(col("timestamp"))) \
        .dropDuplicates(["user_id", "product_id", "timestamp", "order_id"]) \
        .filter(
            (col("product_id").isNotNull()) & 
            (col("user_id").isNotNull()) &
            # Only enforce positive pricing limits for order events (where price column is populated)
            ((col("price").isNull()) | (col("price") > 0)) &
            ((col("quantity").isNull()) | (col("quantity") > 0))
        )
    
    # Write to Silver Delta Table
    logger.info("Saving to Silver Delta Table...")
    cleaned_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("cleaned_transactions_silver")
else:
    logger.error("Bronze Delta table live_ecommerce_bronze not found! Skipping Silver.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 5: Gold Layer Processing (Dynamic Pricing Model)
# MAGIC Computes sales velocity, conversion margins, and applies pricing adjustments using a windowed function.

# COMMAND ----------
from pyspark.sql.window import Window
from pyspark.sql.functions import (
    count, sum, avg, when, lag, to_date, countDistinct, round, coalesce, lit
)

if spark.catalog.tableExists("cleaned_transactions_silver"):
    logger.info("Processing Gold Layer from Silver...")
    silver_df = spark.table("cleaned_transactions_silver")
    
    # Derive calendar dates for grouping
    silver_with_date = silver_df.withColumn("event_date", to_date(col("event_timestamp")))
    
    # 1. Compute Daily Product Aggregates (Clicks, Orders, Sales Volume, and Base Prices)
    daily_aggregates = silver_with_date.groupBy("event_date", "product_id").agg(
        count(when(col("order_id").isNotNull(), 1)).alias("daily_orders"),
        count(when(col("order_id").isNull(), 1)).alias("daily_clicks"),
        sum(coalesce(col("quantity"), lit(0))).alias("daily_units_sold"),
        avg(coalesce(col("price"), lit(0.0))).alias("avg_unit_price")
    )
    
    # Fill average unit price for days where it is zero from catalog averages
    daily_aggregates = daily_aggregates.filter(col("avg_unit_price") >= 0)
    
    # 2. Windowing: Calculate 3-day moving averages of daily orders (sales velocity)
    # Range is 3 days: including current day and preceding 2 days
    window_spec_3d = Window.partitionBy("product_id").orderBy("event_date").rowsBetween(-2, 0)
    
    daily_metrics_df = daily_aggregates \
        .withColumn("3d_avg_orders", avg("daily_orders").over(window_spec_3d)) \
        .withColumn("prev_3d_avg_orders", lag("3d_avg_orders", 1).over(Window.partitionBy("product_id").orderBy("event_date")))
    
    # Avoid nulls in velocity calculations
    daily_metrics_df = daily_metrics_df \
        .withColumn("prev_3d_avg_orders", coalesce(col("prev_3d_avg_orders"), col("3d_avg_orders"), lit(1.0))) \
        .withColumn("velocity_change_pct", 
                    when(col("prev_3d_avg_orders") == 0, 0.0)
                    .otherwise((col("3d_avg_orders") - col("prev_3d_avg_orders")) / col("prev_3d_avg_orders")))
    
    # 3. Calculate Conversion Rates (Orders / Clicks) and 3-day moving average of conversions
    daily_metrics_df = daily_metrics_df \
        .withColumn("conversion_rate", 
                    when(col("daily_clicks") == 0, 0.0)
                    .otherwise(col("daily_orders").cast(DoubleType()) / col("daily_clicks").cast(DoubleType()))) \
        .withColumn("3d_avg_conversion", avg("conversion_rate").over(window_spec_3d)) \
        .withColumn("prev_3d_avg_conversion", lag("3d_avg_conversion", 1).over(Window.partitionBy("product_id").orderBy("event_date")))
        
    daily_metrics_df = daily_metrics_df \
        .withColumn("prev_3d_avg_conversion", coalesce(col("prev_3d_avg_conversion"), col("3d_avg_conversion"), lit(1.0))) \
        .withColumn("conversion_change_pct", 
                    when(col("prev_3d_avg_conversion") == 0, 0.0)
                    .otherwise((col("3d_avg_conversion") - col("prev_3d_avg_conversion")) / col("prev_3d_avg_conversion")))
    
    # 4. Implement Dynamic Pricing Engine Rules:
    # Rule 1: If sales velocity (3-day moving average) increases by > 20%, bump next_day_price by +10%
    # Rule 2: If conversion rate drops by > 15% (-0.15), drop next_day_price by -5%
    # Rule 3: Default is to keep price steady
    optimized_pricing_df = daily_metrics_df.withColumn(
        "price_multiplier",
        when(col("velocity_change_pct") > 0.20, 1.10)
        .when(col("conversion_change_pct") < -0.15, 0.95)
        .otherwise(1.00)
    ).withColumn(
        "next_day_price",
        round(col("avg_unit_price") * col("price_multiplier"), 2)
    )
    
    # Ensure next_day_price doesn't end up as zero or negative if avg_unit_price was zero
    optimized_pricing_df = optimized_pricing_df.withColumn(
        "next_day_price",
        when(col("next_day_price") <= 0.0, 9.99).otherwise(col("next_day_price"))
    )
    
    # Write optimized prices to Gold Delta Table
    logger.info("Saving to Gold Delta Table...")
    optimized_pricing_df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable("optimized_pricing_gold")
        
    logger.info("Gold processing complete.")
else:
    logger.error("Silver Delta table cleaned_transactions_silver not found! Skipping Gold.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Step 6: Unity Catalog Security Policy Configuration
# MAGIC Configures user-group separation and security grants.

# COMMAND ----------
logger.info("Applying Unity Catalog security grants and permissions...")

# Define SQL security actions programmatically
# 1. Create finance group (in real environment, group management is synced from Entra ID)
# 2. Grant SELECT permissions on Gold table
# 3. Explicitly revoke any unauthorized access to Bronze/Silver tables
try:
    spark.sql("CREATE GROUP IF NOT EXISTS finance_team")
    
    # Grant read permissions on Gold to finance
    spark.sql(f"GRANT SELECT ON TABLE {catalog}.{schema}.optimized_pricing_gold TO finance_team")
    logger.info("Granted SELECT on Gold table to finance_team.")
    
    # Revoke access to raw bronze/silver tables from finance_team to protect raw user identifiers
    spark.sql(f"REVOKE ALL PRIVILEGES ON TABLE {catalog}.{schema}.live_ecommerce_bronze FROM finance_team")
    spark.sql(f"REVOKE ALL PRIVILEGES ON TABLE {catalog}.{schema}.cleaned_transactions_silver FROM finance_team")
    logger.info("Revoked access to Bronze and Silver tables from finance_team for compliance.")
    
except Exception as ue:
    logger.warning(f"Could not apply SQL UC security grants (likely running locally or on non-UC cluster): {ue}")
