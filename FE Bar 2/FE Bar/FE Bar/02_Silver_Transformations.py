# Databricks notebook source
# DBTITLE 1,Multivar Energy — Silver Layer: Clean, Unify, Enrich
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- SILVER LAYER: Unified Asset Model
# MAGIC -- Creates canonical asset identity across all 20 DSOs
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TABLE saifur_fevm_aws_workspace_catalog.multivar_energy.silver_unified_assets AS
# MAGIC WITH parsed AS (
# MAGIC   SELECT *, 
# MAGIC     TRY_CAST(install_date_str AS DATE) AS install_date,
# MAGIC     DATEDIFF(CURRENT_DATE(), TRY_CAST(install_date_str AS DATE)) / 365.25 AS asset_age_years,
# MAGIC     CONCAT('UA-', UPPER(SUBSTRING(dso_id, 5, 2)), '-', LPAD(CAST(ABS(HASH(raw_asset_id)) % 999999 AS STRING), 6, '0')) AS unified_asset_id,
# MAGIC     CASE WHEN dso_id LIKE 'DSO_DE%' THEN 'Germany' WHEN dso_id LIKE 'DSO_FR%' THEN 'France' WHEN dso_id LIKE 'DSO_NL%' THEN 'Netherlands' WHEN dso_id LIKE 'DSO_BE%' THEN 'Belgium' WHEN dso_id LIKE 'DSO_AT%' THEN 'Austria' WHEN dso_id LIKE 'DSO_DK%' THEN 'Denmark' WHEN dso_id LIKE 'DSO_ES%' THEN 'Spain' WHEN dso_id LIKE 'DSO_IT%' THEN 'Italy' WHEN dso_id LIKE 'DSO_PL%' THEN 'Poland' WHEN dso_id LIKE 'DSO_SE%' THEN 'Sweden' WHEN dso_id LIKE 'DSO_NO%' THEN 'Norway' WHEN dso_id LIKE 'DSO_CH%' THEN 'Switzerland' WHEN dso_id LIKE 'DSO_PT%' THEN 'Portugal' WHEN dso_id LIKE 'DSO_IE%' THEN 'Ireland' ELSE 'Unknown' END AS country,
# MAGIC     CASE WHEN voltage_level IN ('HV','EHV') AND asset_type IN ('substation','transformer') THEN 'TIER_1_CRITICAL' WHEN voltage_level = 'MV' AND asset_type IN ('substation','transformer','breaker') THEN 'TIER_2_HIGH' WHEN asset_type IN ('feeder','cable','capacitor_bank','recloser') THEN 'TIER_3_MEDIUM' ELSE 'TIER_4_STANDARD' END AS criticality_tier
# MAGIC   FROM saifur_fevm_aws_workspace_catalog.multivar_energy.bronze_asset_registry
# MAGIC )
# MAGIC SELECT raw_asset_id, dso_id, region, LOWER(TRIM(asset_type)) AS asset_category, local_asset_name, manufacturer, model, UPPER(voltage_level) AS voltage_level, rated_capacity_mw, install_date, ROUND(asset_age_years,1) AS asset_age_years, latitude, longitude, LOWER(status) AS operational_status, feeder_id, substation_ref, unified_asset_id, country, criticality_tier, CURRENT_TIMESTAMP() AS processed_timestamp
# MAGIC FROM parsed WHERE TRY_CAST(install_date_str AS DATE) IS NOT NULL

# COMMAND ----------

# DBTITLE 1,Silver: Cleaned Sensor Telemetry with Anomaly Detection
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE saifur_fevm_aws_workspace_catalog.multivar_energy.silver_sensor_telemetry AS
# MAGIC SELECT t.asset_id, a.unified_asset_id, a.asset_category, a.dso_id, a.country, a.criticality_tier, t.sensor_type, t.reading_value, t.quality_flag,
# MAGIC   CASE WHEN t.sensor_type='temperature_c' AND (t.reading_value>105 OR t.reading_value<-20) THEN TRUE
# MAGIC        WHEN t.sensor_type='load_pct' AND t.reading_value>110 THEN TRUE
# MAGIC        WHEN t.sensor_type='voltage_kv' AND (t.reading_value>130 OR t.reading_value<85) THEN TRUE
# MAGIC        WHEN t.sensor_type='vibration_mm_s' AND t.reading_value>7.0 THEN TRUE
# MAGIC        WHEN t.sensor_type='dissolved_gas_ppm' AND t.reading_value>120 THEN TRUE
# MAGIC        WHEN t.sensor_type='oil_level_pct' AND t.reading_value<50 THEN TRUE
# MAGIC        ELSE FALSE END AS is_anomaly,
# MAGIC   TRY_CAST(t.reading_timestamp AS TIMESTAMP) AS reading_timestamp, t.source_device, CURRENT_TIMESTAMP() AS processed_timestamp
# MAGIC FROM saifur_fevm_aws_workspace_catalog.multivar_energy.bronze_sensor_telemetry t
# MAGIC LEFT JOIN saifur_fevm_aws_workspace_catalog.multivar_energy.silver_unified_assets a ON t.asset_id = a.raw_asset_id
# MAGIC WHERE t.quality_flag != 'BAD' AND t.reading_value IS NOT NULL

# COMMAND ----------

# DBTITLE 1,Silver: Outage Events with Severity Classification
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE saifur_fevm_aws_workspace_catalog.multivar_energy.silver_outage_events AS
# MAGIC SELECT outage_id, dso_id,
# MAGIC   CASE WHEN dso_id LIKE 'DSO_DE%' THEN 'Germany' WHEN dso_id LIKE 'DSO_FR%' THEN 'France' WHEN dso_id LIKE 'DSO_NL%' THEN 'Netherlands' WHEN dso_id LIKE 'DSO_BE%' THEN 'Belgium' WHEN dso_id LIKE 'DSO_AT%' THEN 'Austria' WHEN dso_id LIKE 'DSO_DK%' THEN 'Denmark' WHEN dso_id LIKE 'DSO_ES%' THEN 'Spain' WHEN dso_id LIKE 'DSO_IT%' THEN 'Italy' WHEN dso_id LIKE 'DSO_PL%' THEN 'Poland' WHEN dso_id LIKE 'DSO_SE%' THEN 'Sweden' WHEN dso_id LIKE 'DSO_NO%' THEN 'Norway' WHEN dso_id LIKE 'DSO_CH%' THEN 'Switzerland' WHEN dso_id LIKE 'DSO_PT%' THEN 'Portugal' WHEN dso_id LIKE 'DSO_IE%' THEN 'Ireland' ELSE 'Unknown' END AS country,
# MAGIC   LOWER(cause) AS outage_cause, voltage_level, customers_affected, duration_minutes, ROUND(duration_minutes/60.0,2) AS duration_hours, customer_minutes_interrupted,
# MAGIC   TRY_CAST(start_timestamp AS TIMESTAMP) AS outage_start, TRY_CAST(end_timestamp AS TIMESTAMP) AS outage_end, DATE(TRY_CAST(start_timestamp AS TIMESTAMP)) AS outage_date,
# MAGIC   LOWER(restoration_status) AS restoration_status, switching_operations,
# MAGIC   CASE WHEN customers_affected>10000 AND duration_minutes>240 THEN 'MAJOR' WHEN customers_affected>5000 OR duration_minutes>120 THEN 'SIGNIFICANT' WHEN customers_affected>1000 THEN 'MODERATE' ELSE 'MINOR' END AS outage_severity,
# MAGIC   CURRENT_TIMESTAMP() AS processed_timestamp
# MAGIC FROM saifur_fevm_aws_workspace_catalog.multivar_energy.bronze_outage_events