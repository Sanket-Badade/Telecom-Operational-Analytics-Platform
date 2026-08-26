# Databricks notebook source
# DBTITLE 1,Notebook Header
# MAGIC %md
# MAGIC # Bronze to Silver Transformation
# MAGIC
# MAGIC **Pipeline Stage**: Data Quality & Enrichment  
# MAGIC **Source**: Bronze Layer - 'API_data'
# MAGIC
# MAGIC **Target**: Silver Layer - Cleaned and validated API_data
# MAGIC
# MAGIC ---

# COMMAND ----------

# DBTITLE 1,Section 2
# MAGIC %md
# MAGIC ## 1. Data Exploration
# MAGIC Initial exploration of schema and data quality.

# COMMAND ----------

# DBTITLE 1,Load bronze data
api_df = spark.sql("SELECT * FROM delta.`/Volumes/telecom_catalog/default/bronze/API_data`")

# COMMAND ----------

api_df.display()

# COMMAND ----------

# DBTITLE 1,Show schema
api_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Section 3
# MAGIC %md
# MAGIC ## 2. Data Quality Checks
# MAGIC Identify duplicates, null values, and data anomalies.

# COMMAND ----------

# DBTITLE 1,Check duplicates and nulls
from pyspark.sql.functions import *

# Total rows
print(api_df.count())

# Duplicate rows
print(api_df.count() - api_df.dropDuplicates().count())

# Duplicate business keys
api_df.groupBy("device_id", "timestamp") \
      .count() \
      .filter("count > 1") \
      .show()

# Null count
display(
    api_df.select([
        count(when(col(c).isNull(), c)).alias(c)
        for c in api_df.columns
    ])
)

# COMMAND ----------

# DBTITLE 1,Section 4
# MAGIC %md
# MAGIC ## 3. Data Cleaning & Standardization
# MAGIC Standardize business keys and add validation flags.

# COMMAND ----------

# DBTITLE 1,Standardize device_id

from pyspark.sql.functions import upper, trim, col

silver_df = (
    api_df
    .withColumn("device_id", upper(trim(col("device_id"))))
)

# Even if today's data is clean, this prevents future inconsistencies.

# COMMAND ----------

# DBTITLE 1,Add validation flag
# Create a validation flag instead of dropping rows:

from pyspark.sql.functions import when, lit

silver_df = silver_df.withColumn(
    "is_valid",
    when(
        (col("anomaly_score").between(0,1)) &
        (col("cache_hit_ratio").between(0,1)) &
        (col("uptime_percentage").between(0,100)) &
        (col("temperature_celsius").between(-20,120)),
        lit(True)
    ).otherwise(lit(False))
)

# This lets you quarantine invalid rows later if needed.

# COMMAND ----------

# DBTITLE 1,Section 5
# MAGIC %md
# MAGIC ## 4. Feature Engineering
# MAGIC Create derived business metrics for downstream analytics.

# COMMAND ----------

# DBTITLE 1,Temperature status

silver_df = silver_df.withColumn(
    "temperature_status",
    when(col("temperature_celsius") >= 70, "High")
    .when(col("temperature_celsius") <= 10, "Low")
    .otherwise("Normal")
)

# COMMAND ----------

# DBTITLE 1,Network health

silver_df = silver_df.withColumn(
    "network_health",
    when(
        (col("latency_ms_p99") > 100) |
        (col("packet_loss_percentage") > 2),
        "Poor"
    ).otherwise("Healthy")
)

# COMMAND ----------

# DBTITLE 1,Uptime category

silver_df = silver_df.withColumn(
    "uptime_category",
    when(col("uptime_percentage") >= 99.9, "Excellent")
    .when(col("uptime_percentage") >= 99, "Good")
    .otherwise("Poor")
)

# COMMAND ----------

# DBTITLE 1,Audit Section
# MAGIC %md
# MAGIC ## 5. Audit Columns
# MAGIC Add metadata for data lineage and troubleshooting.

# COMMAND ----------

# DBTITLE 1,Add audit columns

from pyspark.sql.functions import current_timestamp, to_date

silver_df = (
    silver_df
    .withColumn("silver_load_time", current_timestamp())
    .withColumn("event_date", to_date("timestamp"))
)

# COMMAND ----------

# DBTITLE 1,Write Section
# MAGIC %md
# MAGIC ## 6. Write to Silver Layer
# MAGIC Persist the cleaned and enriched data to the silver layer.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS telecom_catalog.silver_schema
# MAGIC --MANAGED LOCATION 'pastesilver container url'

# COMMAND ----------

# DBTITLE 1,Write to Silver
# Write to silver layer Delta table
(
    silver_df.write
    .format("delta")
    .mode("append")          # append for incremental loads
    .option("mergeSchema", "true")
    .partitionBy("event_date")
    .save("/Volumes/telecom_catalog/default/silver/API_data")
)

# COMMAND ----------

# DBTITLE 1,Pipeline Notes
# MAGIC %md
# MAGIC ---
# MAGIC
# MAGIC ## Pipeline Notes
# MAGIC
# MAGIC ### Key Features
# MAGIC * **Data Quality Flagging**: Invalid records are flagged with `is_valid=False` rather than dropped, enabling downstream analysis
# MAGIC * **Business Logic**: Derived columns classify device status (temperature, network health, uptime) for operational dashboards
# MAGIC * **Audit Trail**: `silver_load_time` and `event_date` support data lineage and partition optimization
# MAGIC
# MAGIC ### Schema Additions
# MAGIC | Column | Type | Description |
# MAGIC |--------|------|-------------|
# MAGIC | `is_valid` | boolean | Data quality flag (True if all metrics are within acceptable ranges) |
# MAGIC | `temperature_status` | string | High (≥70°C), Low (≤10°C), or Normal |
# MAGIC | `network_health` | string | Poor (latency >100ms OR packet loss >2%) or Healthy |
# MAGIC | `uptime_category` | string | Excellent (≥99.9%), Good (≥99%), or Poor |
# MAGIC | `silver_load_time` | timestamp | ETL processing timestamp |
# MAGIC | `event_date` | date | Event date for partitioning |
# MAGIC

# COMMAND ----------

