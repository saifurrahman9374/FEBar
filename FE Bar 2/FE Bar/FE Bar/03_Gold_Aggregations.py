# Databricks notebook source
# DBTITLE 1,Gold: Asset Health Scores (Composite 0–100 scoring)
# MAGIC %sql
# MAGIC -- ============================================================
# MAGIC -- GOLD LAYER: Asset Health Scores
# MAGIC -- Composite scoring: anomaly rate, age, emergency repairs, load, thermal, oil
# MAGIC -- ============================================================
# MAGIC CREATE OR REPLACE TABLE saifur_fevm_aws_workspace_catalog.multivar_energy.gold_asset_health_scores AS
# MAGIC WITH telemetry_stats AS (
# MAGIC   SELECT unified_asset_id, COUNT(*) AS total_readings, SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS anomaly_count,
# MAGIC     ROUND(SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END)*100.0/COUNT(*),2) AS anomaly_rate_pct,
# MAGIC     AVG(CASE WHEN sensor_type='temperature_c' THEN reading_value END) AS avg_temperature,
# MAGIC     MAX(CASE WHEN sensor_type='temperature_c' THEN reading_value END) AS max_temperature,
# MAGIC     AVG(CASE WHEN sensor_type='load_pct' THEN reading_value END) AS avg_load_pct,
# MAGIC     MAX(CASE WHEN sensor_type='load_pct' THEN reading_value END) AS peak_load_pct,
# MAGIC     AVG(CASE WHEN sensor_type='vibration_mm_s' THEN reading_value END) AS avg_vibration,
# MAGIC     AVG(CASE WHEN sensor_type='oil_level_pct' THEN reading_value END) AS avg_oil_level,
# MAGIC     AVG(CASE WHEN sensor_type='dissolved_gas_ppm' THEN reading_value END) AS avg_dissolved_gas
# MAGIC   FROM saifur_fevm_aws_workspace_catalog.multivar_energy.silver_sensor_telemetry GROUP BY unified_asset_id
# MAGIC ),
# MAGIC wo_stats AS (
# MAGIC   SELECT unified_asset_id, COUNT(*) AS total_work_orders, SUM(CASE WHEN priority_level<=2 THEN 1 ELSE 0 END) AS critical_work_orders,
# MAGIC     SUM(estimated_cost_eur) AS total_maintenance_cost_eur, COUNT(CASE WHEN work_type='emergency_repair' THEN 1 END) AS emergency_repairs
# MAGIC   FROM saifur_fevm_aws_workspace_catalog.multivar_energy.silver_work_orders WHERE unified_asset_id IS NOT NULL GROUP BY unified_asset_id
# MAGIC )
# MAGIC SELECT a.*, COALESCE(t.total_readings,0) AS total_readings, COALESCE(t.anomaly_count,0) AS anomaly_count, COALESCE(t.anomaly_rate_pct,0) AS anomaly_rate_pct,
# MAGIC   ROUND(t.avg_temperature,1) AS avg_temperature_c, ROUND(t.max_temperature,1) AS max_temperature_c,
# MAGIC   ROUND(t.avg_load_pct,1) AS avg_load_pct, ROUND(t.peak_load_pct,1) AS peak_load_pct,
# MAGIC   ROUND(t.avg_vibration,2) AS avg_vibration_mm_s, ROUND(t.avg_oil_level,1) AS avg_oil_level_pct, ROUND(t.avg_dissolved_gas,1) AS avg_dissolved_gas_ppm,
# MAGIC   COALESCE(wo.total_work_orders,0) AS total_work_orders, COALESCE(wo.critical_work_orders,0) AS critical_work_orders,
# MAGIC   COALESCE(wo.total_maintenance_cost_eur,0) AS total_maintenance_cost_eur, COALESCE(wo.emergency_repairs,0) AS emergency_repairs,
# MAGIC   ROUND(100 - LEAST(30,COALESCE(t.anomaly_rate_pct,0)*3) - LEAST(20,COALESCE(a.asset_age_years,0)*0.5) - LEAST(15,COALESCE(wo.emergency_repairs,0)*5) - LEAST(15,GREATEST(0,COALESCE(t.avg_load_pct,60)-80)*1.5) - LEAST(10,GREATEST(0,COALESCE(t.avg_temperature,50)-70)) - LEAST(10,GREATEST(0,60-COALESCE(t.avg_oil_level,85))*0.5), 1) AS health_score,
# MAGIC   CURRENT_TIMESTAMP() AS scored_at
# MAGIC FROM saifur_fevm_aws_workspace_catalog.multivar_energy.silver_unified_assets a
# MAGIC LEFT JOIN telemetry_stats t ON a.unified_asset_id = t.unified_asset_id
# MAGIC LEFT JOIN wo_stats wo ON a.unified_asset_id = wo.unified_asset_id

# COMMAND ----------

# DBTITLE 1,Gold: Grid Reliability Metrics (SAIDI/SAIFI)
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE saifur_fevm_aws_workspace_catalog.multivar_energy.gold_grid_reliability_metrics AS
# MAGIC WITH dso_customers AS (SELECT dso_id, COUNT(DISTINCT meter_id) AS total_customers FROM saifur_fevm_aws_workspace_catalog.multivar_energy.silver_smart_meter_readings GROUP BY dso_id),
# MAGIC monthly AS (
# MAGIC   SELECT dso_id, country, DATE_TRUNC('MONTH', outage_start) AS month, COUNT(*) AS outage_count,
# MAGIC     SUM(customers_affected) AS total_customers_interrupted, SUM(customer_minutes_interrupted) AS total_cmi,
# MAGIC     AVG(duration_minutes) AS avg_duration_minutes, MAX(duration_minutes) AS max_duration_minutes,
# MAGIC     SUM(CASE WHEN outage_severity='MAJOR' THEN 1 ELSE 0 END) AS major_events,
# MAGIC     SUM(CASE WHEN outage_cause='equipment_failure' THEN 1 ELSE 0 END) AS equipment_failures,
# MAGIC     SUM(CASE WHEN outage_cause='weather_storm' THEN 1 ELSE 0 END) AS weather_events
# MAGIC   FROM saifur_fevm_aws_workspace_catalog.multivar_energy.silver_outage_events GROUP BY 1,2,3
# MAGIC )
# MAGIC SELECT m.*, dc.total_customers,
# MAGIC   ROUND(m.total_customers_interrupted*1.0/GREATEST(1,dc.total_customers),4) AS saifi,
# MAGIC   ROUND(m.total_cmi/GREATEST(1,dc.total_customers),2) AS saidi_minutes,
# MAGIC   ROUND(m.total_cmi/GREATEST(1,m.total_customers_interrupted),1) AS mttr_minutes
# MAGIC FROM monthly m LEFT JOIN dso_customers dc ON m.dso_id = dc.dso_id

# COMMAND ----------

# DBTITLE 1,Gold: Executive KPIs by Country
# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE saifur_fevm_aws_workspace_catalog.multivar_energy.gold_executive_kpis AS
# MAGIC WITH asset_kpis AS (SELECT country, COUNT(*) AS total_assets, ROUND(AVG(health_score),1) AS avg_health_score, SUM(CASE WHEN health_score<40 THEN 1 ELSE 0 END) AS critical_assets, SUM(CASE WHEN health_score<60 THEN 1 ELSE 0 END) AS high_risk_assets, ROUND(SUM(total_maintenance_cost_eur),0) AS total_maintenance_spend_eur, ROUND(AVG(asset_age_years),1) AS avg_asset_age_years FROM saifur_fevm_aws_workspace_catalog.multivar_energy.gold_asset_health_scores GROUP BY country),
# MAGIC rel_kpis AS (SELECT country, ROUND(AVG(saifi),4) AS avg_saifi, ROUND(AVG(saidi_minutes),2) AS avg_saidi_minutes, ROUND(AVG(mttr_minutes),1) AS avg_mttr_minutes, SUM(major_events) AS total_major_events FROM saifur_fevm_aws_workspace_catalog.multivar_energy.gold_grid_reliability_metrics GROUP BY country),
# MAGIC der_kpis AS (SELECT country, ROUND(SUM(total_generation_mw),0) AS total_renewable_generation_mw, ROUND(AVG(avg_capacity_factor),4) AS avg_renewable_capacity_factor FROM saifur_fevm_aws_workspace_catalog.multivar_energy.gold_der_optimization GROUP BY country)
# MAGIC SELECT a.*, COALESCE(r.avg_saifi,0) AS avg_saifi, COALESCE(r.avg_saidi_minutes,0) AS avg_saidi_minutes, COALESCE(r.avg_mttr_minutes,0) AS avg_mttr_minutes, COALESCE(r.total_major_events,0) AS total_major_events, COALESCE(d.total_renewable_generation_mw,0) AS total_renewable_generation_mw, CURRENT_TIMESTAMP() AS computed_at
# MAGIC FROM asset_kpis a LEFT JOIN rel_kpis r ON a.country=r.country LEFT JOIN der_kpis d ON a.country=d.country ORDER BY a.total_assets DESC