# Databricks notebook source
# MAGIC %md
# MAGIC ##Device Health Overview

# COMMAND ----------

SELECT
    d.device_id,
    d.device_type,
    d.region,
    COUNT(*) AS total_events,
    ROUND(AVG(f.cpu_usage), 2) AS avg_cpu_usage,
    ROUND(AVG(f.memory_usage), 2) AS avg_memory_usage,
    ROUND(AVG(f.packet_loss), 2) AS avg_packet_loss,
    ROUND(AVG(f.latency_ms), 2) AS avg_latency,
    MAX(f.temp) AS max_temperature

FROM telecom_catalog.gold_schema.fact_device_logs f

JOIN telecom_catalog.gold_schema.dim_device d
        ON f.device_key = d.device_key

GROUP BY d.device_id,
        d.device_type,
        d.region

ORDER BY avg_cpu_usage DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Regional Device Performance

# COMMAND ----------

SELECT
    d.region,
    COUNT(*) AS total_events,
    ROUND(AVG(f.cpu_usage), 2) AS avg_cpu_usage,
    ROUND(AVG(f.memory_usage), 2) AS avg_memory_usage,
    ROUND(AVG(f.packet_loss), 2) AS avg_packet_loss,
    ROUND(AVG(f.latency_ms), 2) AS avg_latency
    
FROM telecom_catalog.gold_schema.fact_device_logs f

JOIN telecom_catalog.gold_schema.dim_device d
    ON f.device_key = d.device_key

GROUP BY d.region
ORDER BY avg_cpu_usage DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Network Health Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     d.region,
# MAGIC     f.network_health,
# MAGIC     COUNT(*) AS event_count
# MAGIC
# MAGIC FROM telecom_catalog.gold_schema.fact_device_logs f
# MAGIC
# MAGIC JOIN telecom_catalog.gold_schema.dim_device d
# MAGIC     ON f.device_key = d.device_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     d.region,
# MAGIC     f.network_health
# MAGIC
# MAGIC ORDER BY
# MAGIC     d.region,
# MAGIC     event_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Asset Maintenance Analysis

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     a.region,
# MAGIC     a.maintenance_status,
# MAGIC     a.maintenance_required,
# MAGIC
# MAGIC     COUNT(*) AS asset_count
# MAGIC
# MAGIC FROM telecom_catalog.gold_schema.dim_asset a
# MAGIC
# MAGIC GROUP BY
# MAGIC     a.region,
# MAGIC     a.maintenance_status,
# MAGIC     a.maintenance_required
# MAGIC
# MAGIC ORDER BY
# MAGIC     a.region;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Critical Assets

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     a.asset_key,
# MAGIC     d.device_id,
# MAGIC     d.device_type,
# MAGIC
# MAGIC     a.asset_type,
# MAGIC     a.vendor,
# MAGIC     a.firmware_version,
# MAGIC     a.maintenance_status,
# MAGIC     a.maintenance_required,
# MAGIC     a.sla_level,
# MAGIC     a.sla_priority,
# MAGIC     a.region
# MAGIC
# MAGIC FROM telecom_catalog.gold_schema.dim_asset a
# MAGIC
# MAGIC JOIN telecom_catalog.gold_schema.dim_device d
# MAGIC     ON a.device_key = d.device_key
# MAGIC
# MAGIC WHERE
# MAGIC     a.maintenance_required = 'Yes'
# MAGIC
# MAGIC ORDER BY
# MAGIC     a.sla_priority ASC;

# COMMAND ----------

# MAGIC %md
# MAGIC ##API Performance by Date

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.full_date,
# MAGIC
# MAGIC     COUNT(*) AS total_events,
# MAGIC
# MAGIC     ROUND(AVG(f.latency_ms_p99), 2) AS avg_latency_p99,
# MAGIC     ROUND(AVG(f.packet_loss_percentage), 2) AS avg_packet_loss,
# MAGIC     ROUND(AVG(f.cache_hit_ratio), 2) AS avg_cache_hit_ratio,
# MAGIC
# MAGIC     SUM(f.failed_requests) AS total_failed_requests,
# MAGIC     SUM(f.request_count) AS total_requests
# MAGIC
# MAGIC FROM telecom_catalog.gold_schema.fact_api_metrics f
# MAGIC
# MAGIC JOIN telecom_catalog.gold_schema.dim_time t
# MAGIC     ON f.time_key = t.time_key
# MAGIC
# MAGIC GROUP BY t.full_date
# MAGIC
# MAGIC ORDER BY t.full_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ##API Failure Rate

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     t.full_date,
# MAGIC
# MAGIC     SUM(f.failed_requests) AS failed_requests,
# MAGIC     SUM(f.request_count) AS total_requests,
# MAGIC
# MAGIC     ROUND(
# MAGIC         SUM(f.failed_requests) * 100.0
# MAGIC         / NULLIF(SUM(f.request_count), 0),
# MAGIC         2
# MAGIC     ) AS failure_rate_percentage
# MAGIC
# MAGIC FROM telecom_catalog.gold_schema.fact_api_metrics f
# MAGIC
# MAGIC JOIN telecom_catalog.gold_schema.dim_time t
# MAGIC     ON f.time_key = t.time_key
# MAGIC
# MAGIC GROUP BY t.full_date
# MAGIC
# MAGIC ORDER BY t.full_date;

# COMMAND ----------

# MAGIC %md
# MAGIC ##Top Problematic Devices

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     d.device_id,
# MAGIC     d.device_type,
# MAGIC     d.region,
# MAGIC
# MAGIC     COUNT(*) AS total_events,
# MAGIC
# MAGIC     SUM(f.failed_requests) AS failed_requests,
# MAGIC
# MAGIC     ROUND(AVG(f.latency_ms_p99), 2) AS avg_latency_p99,
# MAGIC
# MAGIC     ROUND(AVG(f.packet_loss_percentage), 2) AS avg_packet_loss
# MAGIC
# MAGIC FROM telecom_catalog.gold_schema.fact_api_metrics f
# MAGIC
# MAGIC JOIN telecom_catalog.gold_schema.dim_device d
# MAGIC     ON f.device_key = d.device_key
# MAGIC
# MAGIC GROUP BY
# MAGIC     d.device_id,
# MAGIC     d.device_type,
# MAGIC     d.region
# MAGIC
# MAGIC ORDER BY
# MAGIC     failed_requests DESC
# MAGIC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conclusion
# MAGIC
# MAGIC This notebook demonstrates business-oriented analytical queries built on top of the Gold layer dimensional model.
# MAGIC
# MAGIC The queries leverage star schema principles by joining fact and dimension tables to generate meaningful business insights, including:
# MAGIC - Device performance monitoring
# MAGIC - API performance analysis
# MAGIC - Network health assessment
# MAGIC - Asset maintenance tracking
# MAGIC - Regional operational analysis
# MAGIC
# MAGIC These queries can serve as the foundation for BI dashboards and support data-driven decision-making for infrastructure monitoring and operational analytics.

# COMMAND ----------

