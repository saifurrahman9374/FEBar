# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Multivar Energy - Unity Catalog Governance
# MAGIC Horizon 1: Intelligent Data Foundation - governance controls backing the README narrative.
# MAGIC
# MAGIC This notebook applies and then **proves** each control by querying Unity Catalog / system tables:
# MAGIC
# MAGIC | Control | Mechanism | Evidence cell |
# MAGIC | --- | --- | --- |
# MAGIC | Classification | Schema / table / column tags (PII, quality tier, regulation) | `information_schema.*_tags` |
# MAGIC | Documentation | Table and column comments | `information_schema.columns` |
# MAGIC | Access control | Least-privilege GRANTs to the app service principal | `SHOW GRANTS` |
# MAGIC | Row-level security | Row filter on AMI meter readings scoped by DSO entitlement | filtered vs unfiltered counts |
# MAGIC | Column masking | SHA-256 mask on customer `meter_id` (GDPR) | `information_schema.column_masks` |
# MAGIC | Data quality enforcement | NOT NULL + CHECK constraints on Silver/Gold | rejected bad-row insert |
# MAGIC | Data quality measurement | Rule-based checks + Lakehouse quality monitor | check results table |
# MAGIC | Lineage | Auto-captured by Unity Catalog | `system.access.table_lineage` / `column_lineage` |
# MAGIC | Audit | Auto-captured by Unity Catalog | `system.access.audit` |
# MAGIC
# MAGIC Upgrade path: once account-level governed tags are available, the manual row filter and mask below can be replaced by ABAC policies keyed on the `multivar_pii` / `dso_scoped` tags.

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "saifur_fevm_aws_workspace_catalog"
SCHEMA = "multivar_energy"
FQ = f"{CATALOG}.{SCHEMA}"
APP_SP = "f983c810-cb77-4b90-a6e0-f64c83079c4d"  # service principal of app multivar-energy-grid-ops
spark.sql(f"USE CATALOG {CATALOG}"); spark.sql(f"USE SCHEMA {SCHEMA}")
print("Governing:", FQ, "| current_user:", spark.sql("SELECT current_user()").first()[0])

# COMMAND ----------

# DBTITLE 1,1. Classification tags
# MAGIC %sql
# MAGIC -- Schema-level tags
# MAGIC -- Account-level governed tag policies restrict 'domain', 'owner', 'regulation', 'source_system' and 'pii' to fixed value lists,
# MAGIC -- so 'domain' uses its allowed value and project-specific keys (grid_*, multivar_pii) carry the free-form classifications.
# MAGIC ALTER SCHEMA saifur_fevm_aws_workspace_catalog.multivar_energy SET TAGS ('domain' = 'operations', 'grid_data_owner' = 'grid-data-platform', 'grid_regulation' = 'GDPR_NIS2', 'environment' = 'demo');
# MAGIC
# MAGIC -- Medallion quality tiers
# MAGIC ALTER TABLE bronze_asset_registry        SET TAGS ('quality_tier' = 'bronze', 'grid_source_system' = 'SCADA_GIS');
# MAGIC ALTER TABLE bronze_sensor_telemetry      SET TAGS ('quality_tier' = 'bronze', 'grid_source_system' = 'SCADA_IoT');
# MAGIC ALTER TABLE bronze_smart_meter_readings  SET TAGS ('quality_tier' = 'bronze', 'grid_source_system' = 'AMI', 'contains_pii' = 'true', 'dso_scoped' = 'true');
# MAGIC ALTER TABLE bronze_weather_data          SET TAGS ('quality_tier' = 'bronze', 'grid_source_system' = 'WEATHER_API');
# MAGIC ALTER TABLE bronze_work_orders           SET TAGS ('quality_tier' = 'bronze', 'grid_source_system' = 'CMMS');
# MAGIC ALTER TABLE bronze_outage_events         SET TAGS ('quality_tier' = 'bronze', 'grid_source_system' = 'OMS');
# MAGIC ALTER TABLE bronze_der_telemetry         SET TAGS ('quality_tier' = 'bronze', 'grid_source_system' = 'DERMS');
# MAGIC ALTER TABLE silver_unified_assets        SET TAGS ('quality_tier' = 'silver');
# MAGIC ALTER TABLE silver_smart_meter_readings  SET TAGS ('quality_tier' = 'silver', 'contains_pii' = 'true');
# MAGIC ALTER TABLE gold_asset_health_scores     SET TAGS ('quality_tier' = 'gold', 'consumer' = 'app_genie_dashboard');
# MAGIC ALTER TABLE gold_ml_failure_predictions  SET TAGS ('quality_tier' = 'gold', 'consumer' = 'app_genie_dashboard', 'ml_model' = 'Multivar_Predictive_Maintenance');
# MAGIC ALTER TABLE gold_maintenance_priority    SET TAGS ('quality_tier' = 'gold', 'consumer' = 'app_genie_dashboard');
# MAGIC ALTER TABLE gold_executive_kpis          SET TAGS ('quality_tier' = 'gold', 'consumer' = 'app_genie_dashboard');
# MAGIC
# MAGIC -- Column-level PII classification
# MAGIC ALTER TABLE bronze_smart_meter_readings ALTER COLUMN meter_id SET TAGS ('multivar_pii' = 'customer_identifier');
# MAGIC ALTER TABLE silver_smart_meter_readings ALTER COLUMN meter_id SET TAGS ('multivar_pii' = 'customer_identifier');

# COMMAND ----------

# DBTITLE 1,1b. Evidence: tags applied
# MAGIC %sql
# MAGIC SELECT 'schema' AS level, schema_name AS object, NULL AS column_name, tag_name, tag_value FROM information_schema.schema_tags WHERE schema_name = 'multivar_energy'
# MAGIC UNION ALL
# MAGIC SELECT 'table', table_name, NULL, tag_name, tag_value FROM information_schema.table_tags WHERE schema_name = 'multivar_energy'
# MAGIC UNION ALL
# MAGIC SELECT 'column', table_name, column_name, tag_name, tag_value FROM information_schema.column_tags WHERE schema_name = 'multivar_energy'
# MAGIC ORDER BY level, object, tag_name

# COMMAND ----------

# DBTITLE 1,2. Documentation: column comments
# MAGIC %sql
# MAGIC COMMENT ON COLUMN gold_ml_failure_predictions.ml_failure_probability IS 'GBR-predicted probability of asset failure (0-1). Model: Multivar_Predictive_Maintenance / Failure_Probability_GBR.';
# MAGIC COMMENT ON COLUMN gold_ml_failure_predictions.ml_priority_score IS 'failure_probability x criticality weight x voltage weight x 100. Drives maintenance queue ordering.';
# MAGIC COMMENT ON COLUMN gold_asset_health_scores.health_score IS 'Composite asset health 0-100 (higher is healthier) from anomaly rate, age, and maintenance history.';
# MAGIC COMMENT ON COLUMN silver_unified_assets.unified_asset_id IS 'Cross-DSO harmonised asset key. Join key for Silver/Gold asset tables.';
# MAGIC COMMENT ON COLUMN bronze_smart_meter_readings.meter_id IS 'AMI meter identifier. Personal data under GDPR - masked for non-admins.';
# MAGIC
# MAGIC SELECT table_name, column_name, comment FROM information_schema.columns
# MAGIC WHERE table_schema = 'multivar_energy' AND comment IS NOT NULL AND table_name IN ('gold_ml_failure_predictions','gold_asset_health_scores','silver_unified_assets','bronze_smart_meter_readings')
# MAGIC ORDER BY table_name, column_name

# COMMAND ----------

# DBTITLE 1,3. Access control: least-privilege grants
# MAGIC %sql
# MAGIC -- The Databricks App (multivar-energy-grid-ops) reads Gold only. Grant exactly that, nothing on Bronze/Silver.
# MAGIC GRANT USE CATALOG ON CATALOG saifur_fevm_aws_workspace_catalog TO `f983c810-cb77-4b90-a6e0-f64c83079c4d`;
# MAGIC GRANT USE SCHEMA ON SCHEMA saifur_fevm_aws_workspace_catalog.multivar_energy TO `f983c810-cb77-4b90-a6e0-f64c83079c4d`;
# MAGIC GRANT SELECT ON TABLE gold_asset_health_scores    TO `f983c810-cb77-4b90-a6e0-f64c83079c4d`;
# MAGIC GRANT SELECT ON TABLE gold_ml_failure_predictions TO `f983c810-cb77-4b90-a6e0-f64c83079c4d`;
# MAGIC GRANT SELECT ON TABLE gold_maintenance_priority   TO `f983c810-cb77-4b90-a6e0-f64c83079c4d`;
# MAGIC GRANT SELECT ON TABLE gold_executive_kpis         TO `f983c810-cb77-4b90-a6e0-f64c83079c4d`;
# MAGIC
# MAGIC -- Role model for production (account groups not yet created in this workspace, so left commented):
# MAGIC -- GRANT SELECT ON SCHEMA saifur_fevm_aws_workspace_catalog.multivar_energy TO `grid_analysts`;   -- Gold/Silver read
# MAGIC -- GRANT MODIFY, SELECT ON SCHEMA saifur_fevm_aws_workspace_catalog.multivar_energy TO `grid_data_engineers`;
# MAGIC -- GRANT READ VOLUME ON VOLUME saifur_fevm_aws_workspace_catalog.multivar_energy.raw_landing TO `grid_data_engineers`;

# COMMAND ----------

# DBTITLE 1,3b. Evidence: effective grants
from functools import reduce
from pyspark.sql import functions as F
objs = [("SCHEMA", FQ)] + [("TABLE", f"{FQ}.{t}") for t in ["gold_asset_health_scores","gold_ml_failure_predictions","gold_maintenance_priority","gold_executive_kpis","bronze_smart_meter_readings"]]
dfs = [spark.sql(f"SHOW GRANTS ON {k} {o}").withColumn("securable", F.lit(o)) for k, o in objs]
display(reduce(lambda a, b: a.unionByName(b), dfs).orderBy("securable", "Principal"))

# COMMAND ----------

# DBTITLE 1,4. Row-level security and column masking
# MAGIC %sql
# MAGIC -- Entitlement table: which principal may see which DSO's customer meter data
# MAGIC CREATE TABLE IF NOT EXISTS governance_dso_entitlements (principal STRING NOT NULL, dso_id STRING NOT NULL)
# MAGIC COMMENT 'Row-level security entitlements: principal -> DSO. Read by fn_dso_row_filter.';
# MAGIC
# MAGIC MERGE INTO governance_dso_entitlements t
# MAGIC USING (SELECT * FROM VALUES ('f983c810-cb77-4b90-a6e0-f64c83079c4d', 'DSO_DE_01'), ('f983c810-cb77-4b90-a6e0-f64c83079c4d', 'DSO_FR_01') AS v(principal, dso_id)) s
# MAGIC ON t.principal = s.principal AND t.dso_id = s.dso_id
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC -- Row filter: admins see all DSOs; everyone else only their entitled DSOs
# MAGIC CREATE OR REPLACE FUNCTION fn_dso_row_filter(dso STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC COMMENT 'Row filter for DSO-scoped customer data'
# MAGIC RETURN is_member('admins')
# MAGIC     OR EXISTS (SELECT 1 FROM saifur_fevm_aws_workspace_catalog.multivar_energy.governance_dso_entitlements e WHERE e.principal = current_user() AND e.dso_id = dso);
# MAGIC
# MAGIC -- Column mask: pseudonymise meter_id for non-admins (GDPR)
# MAGIC CREATE OR REPLACE FUNCTION fn_mask_meter_id(meter_id STRING)
# MAGIC RETURNS STRING
# MAGIC COMMENT 'GDPR pseudonymisation of AMI meter identifiers'
# MAGIC RETURN CASE WHEN is_member('admins') THEN meter_id ELSE concat('MTR-', substr(sha2(meter_id, 256), 1, 12)) END;
# MAGIC
# MAGIC ALTER TABLE bronze_smart_meter_readings SET ROW FILTER fn_dso_row_filter ON (dso_id);
# MAGIC ALTER TABLE bronze_smart_meter_readings ALTER COLUMN meter_id SET MASK fn_mask_meter_id;

# COMMAND ----------

# DBTITLE 1,4b. Evidence: policies attached and enforced
# MAGIC %sql
# MAGIC SELECT 'row_filter' AS policy, table_name, CAST(target_columns AS STRING) AS target, filter_name AS function_name FROM information_schema.row_filters WHERE table_schema = 'multivar_energy'
# MAGIC UNION ALL
# MAGIC SELECT 'column_mask', table_name, column_name, mask_name FROM information_schema.column_masks WHERE table_schema = 'multivar_energy'

# COMMAND ----------

# DBTITLE 1,4c. Evidence: what an entitled non-admin would see
# MAGIC %sql
# MAGIC -- Simulate the app service principal's view by evaluating the same predicate/mask logic it would hit
# MAGIC SELECT
# MAGIC   (SELECT COUNT(*) FROM bronze_smart_meter_readings) AS rows_visible_to_me_admin,
# MAGIC   (SELECT COUNT(*) FROM bronze_smart_meter_readings r JOIN governance_dso_entitlements e ON e.dso_id = r.dso_id WHERE e.principal = 'f983c810-cb77-4b90-a6e0-f64c83079c4d') AS rows_visible_to_app_sp,
# MAGIC   (SELECT concat('MTR-', substr(sha2(MIN(meter_id), 256), 1, 12)) FROM bronze_smart_meter_readings) AS masked_example_for_non_admin,
# MAGIC   is_member('admins') AS i_am_admin

# COMMAND ----------

# DBTITLE 1,5. Data quality enforcement: constraints
# MAGIC %sql
# MAGIC -- NOT NULL on join keys
# MAGIC ALTER TABLE silver_unified_assets       ALTER COLUMN unified_asset_id SET NOT NULL;
# MAGIC ALTER TABLE gold_ml_failure_predictions ALTER COLUMN unified_asset_id SET NOT NULL;
# MAGIC ALTER TABLE gold_asset_health_scores    ALTER COLUMN unified_asset_id SET NOT NULL;
# MAGIC
# MAGIC -- CHECK constraints on business ranges (idempotent: drop-if-exists then add)
# MAGIC ALTER TABLE gold_ml_failure_predictions DROP CONSTRAINT IF EXISTS chk_failure_prob_range;
# MAGIC ALTER TABLE gold_ml_failure_predictions ADD CONSTRAINT chk_failure_prob_range CHECK (ml_failure_probability BETWEEN 0 AND 1);
# MAGIC ALTER TABLE gold_asset_health_scores DROP CONSTRAINT IF EXISTS chk_health_score_range;
# MAGIC ALTER TABLE gold_asset_health_scores ADD CONSTRAINT chk_health_score_range CHECK (health_score BETWEEN 0 AND 100);
# MAGIC ALTER TABLE silver_smart_meter_readings DROP CONSTRAINT IF EXISTS chk_consumption_non_negative;
# MAGIC ALTER TABLE silver_smart_meter_readings ADD CONSTRAINT chk_consumption_non_negative CHECK (consumption_kwh >= 0);
# MAGIC
# MAGIC SHOW TBLPROPERTIES gold_ml_failure_predictions;

# COMMAND ----------

# DBTITLE 1,5b. Evidence: constraint rejects a bad row
# Attempt to write an out-of-range probability. Delta must reject it; the table is unchanged either way.
before = spark.table(f"{FQ}.gold_ml_failure_predictions").count()
try:
    spark.sql(f"""INSERT INTO {FQ}.gold_ml_failure_predictions (unified_asset_id, ml_failure_probability)
                  VALUES ('TEST-INVALID', 1.75)""")
    result = "NOT ENFORCED - row was accepted"
except Exception as e:
    result = f"ENFORCED - rejected: {type(e).__name__}: {str(e).splitlines()[0][:160]}"
after = spark.table(f"{FQ}.gold_ml_failure_predictions").count()
print(result); print(f"row count before={before} after={after} unchanged={before == after}")

# COMMAND ----------

# DBTITLE 1,6. Data quality measurement: rule checks
# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS governance_dq_results (check_ts TIMESTAMP, table_name STRING, rule STRING, failing_rows BIGINT, total_rows BIGINT, status STRING)
# MAGIC COMMENT 'Append-only log of rule-based data quality checks run by 06_Unity_Catalog_Governance.';
# MAGIC
# MAGIC INSERT INTO governance_dq_results
# MAGIC SELECT current_timestamp(), t, rule, failing, total, CASE WHEN failing = 0 THEN 'PASS' ELSE 'WARN' END FROM (
# MAGIC   SELECT 'silver_unified_assets' t, 'unified_asset_id unique' rule, COUNT(*) - COUNT(DISTINCT unified_asset_id) failing, COUNT(*) total FROM silver_unified_assets
# MAGIC   UNION ALL SELECT 'silver_unified_assets', 'latitude in Europe bbox (34..72)', SUM(CASE WHEN latitude NOT BETWEEN 34 AND 72 THEN 1 ELSE 0 END), COUNT(*) FROM silver_unified_assets
# MAGIC   UNION ALL SELECT 'bronze_sensor_telemetry', 'quality_flag = BAD', SUM(CASE WHEN quality_flag = 'BAD' THEN 1 ELSE 0 END), COUNT(*) FROM bronze_sensor_telemetry
# MAGIC   UNION ALL SELECT 'silver_smart_meter_readings', 'consumption_kwh not null', SUM(CASE WHEN consumption_kwh IS NULL THEN 1 ELSE 0 END), COUNT(*) FROM silver_smart_meter_readings
# MAGIC   UNION ALL SELECT 'bronze_smart_meter_readings -> silver', 'rows dropped by cleansing', (SELECT COUNT(*) FROM bronze_smart_meter_readings) - (SELECT COUNT(*) FROM silver_smart_meter_readings), (SELECT COUNT(*) FROM bronze_smart_meter_readings)
# MAGIC   UNION ALL SELECT 'gold_ml_failure_predictions', 'failure_prob in [0,1]', SUM(CASE WHEN ml_failure_probability NOT BETWEEN 0 AND 1 THEN 1 ELSE 0 END), COUNT(*) FROM gold_ml_failure_predictions
# MAGIC );
# MAGIC
# MAGIC SELECT * FROM governance_dq_results WHERE check_ts = (SELECT MAX(check_ts) FROM governance_dq_results) ORDER BY status DESC, table_name

# COMMAND ----------

# DBTITLE 1,6b. Data quality monitor on Gold predictions
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import MonitorSnapshot
w = WorkspaceClient()
tbl = f"{FQ}.gold_ml_failure_predictions"
try:
    m = w.quality_monitors.get(table_name=tbl)
    print("Monitor already exists:", m.status, m.dashboard_id)
except Exception:
    m = w.quality_monitors.create(
        table_name=tbl,
        assets_dir="/Workspace/Users/saifur.rahman@databricks.com/FE Bar/monitoring",
        output_schema_name=FQ,
        snapshot=MonitorSnapshot(),
    )
    print("Monitor created:", m.status)
print("Profile metrics table:", m.profile_metrics_table_name)
print("Drift metrics table:  ", m.drift_metrics_table_name)

# COMMAND ----------

# DBTITLE 1,7. Evidence: Unity Catalog lineage
# MAGIC %sql
# MAGIC SELECT
# MAGIC   regexp_replace(source_table_full_name, '^.*\\.', '') AS source_table,
# MAGIC   regexp_replace(target_table_full_name, '^.*\\.', '') AS target_table,
# MAGIC   entity_type, COUNT(*) AS lineage_events, MAX(event_time) AS last_seen
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE event_date >= '2026-09-01'
# MAGIC   AND target_table_full_name LIKE 'saifur_fevm_aws_workspace_catalog.multivar_energy%'
# MAGIC   AND source_table_full_name IS NOT NULL
# MAGIC GROUP BY ALL
# MAGIC ORDER BY target_table, source_table

# COMMAND ----------

# DBTITLE 1,7b. Evidence: column lineage for ML predictions
# MAGIC %sql
# MAGIC SELECT DISTINCT
# MAGIC   regexp_replace(source_table_full_name, '^.*\\.', '') AS source_table, source_column_name,
# MAGIC   regexp_replace(target_table_full_name, '^.*\\.', '') AS target_table, target_column_name
# MAGIC FROM system.access.column_lineage
# MAGIC WHERE event_date >= '2026-09-01'
# MAGIC   AND target_table_full_name LIKE 'saifur_fevm_aws_workspace_catalog.multivar_energy.gold_%'
# MAGIC   AND source_table_full_name IS NOT NULL
# MAGIC ORDER BY target_table, target_column_name
# MAGIC LIMIT 100

# COMMAND ----------

# DBTITLE 1,8. Evidence: governance audit trail
# MAGIC %sql
# MAGIC -- Audit events can lag by up to ~1 hour after the cells above run
# MAGIC SELECT event_time, user_identity.email AS actor, action_name,
# MAGIC        coalesce(request_params.full_name_arg, request_params.securable_full_name, request_params.name) AS securable,
# MAGIC        response.status_code
# MAGIC FROM system.access.audit
# MAGIC WHERE event_date >= '2026-09-27' AND service_name = 'unityCatalog'
# MAGIC   AND action_name IN ('updatePermissions','createFunction','updateTables','createTable','generateTemporaryTableCredential')
# MAGIC   AND coalesce(request_params.full_name_arg, request_params.securable_full_name, request_params.name, '') LIKE '%multivar_energy%'
# MAGIC ORDER BY event_time DESC
# MAGIC LIMIT 50

# COMMAND ----------

