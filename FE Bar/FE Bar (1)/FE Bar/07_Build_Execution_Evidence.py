# Databricks notebook source
# DBTITLE 1,Overview
# MAGIC %md
# MAGIC # Multivar Energy - Build Execution Evidence
# MAGIC This notebook is the committed, re-runnable proof that the build executed. Every cell reads from a system of record (Delta transaction logs, MLflow tracking server, Pipelines API, Genie API, SQL warehouse, Apps API) - nothing is hard-coded.
# MAGIC
# MAGIC | # | Claim | System of record |
# MAGIC | --- | --- | --- |
# MAGIC | 1 | 22+ medallion tables exist and hold data | Unity Catalog + Delta |
# MAGIC | 2 | Tables were written by real jobs at real times | `DESCRIBE HISTORY` (Delta log) |
# MAGIC | 3 | Both ML models trained and logged metrics | MLflow tracking |
# MAGIC | 4 | Lakeflow SDP ingestion pipeline ran with expectations | Pipelines API + event log |
# MAGIC | 5 | Genie agent answers from governed data | Genie Conversation API |
# MAGIC | 6 | App is deployed and its backend queries return data | Apps API + app's SQL warehouse |
# MAGIC | 7 | Lineage was captured end to end | `system.access.table_lineage` |
# MAGIC
# MAGIC The final cell persists a summary row per claim to `build_execution_evidence` so the evidence also lives in a governed table.

# COMMAND ----------

# DBTITLE 1,Configuration
from databricks.sdk import WorkspaceClient
from datetime import datetime, timezone, timedelta
from pyspark.sql import functions as F
import mlflow

CATALOG, SCHEMA = "saifur_fevm_aws_workspace_catalog", "multivar_energy"
FQ = f"{CATALOG}.{SCHEMA}"
SDP_PIPELINE_ID = "11cf7db6-d07a-4a14-a370-b58850c05d55"
APP_NAME = "multivar-energy-grid-ops"
WAREHOUSE_ID = "a31d61d12430e28f"  # the warehouse bound to the app
GENIE_SPACE_ID = "01f1bad41f991a4fadd4fee40820a20a"  # Grid Operations Intelligence Agent
EXPERIMENTS = ["/Users/saifur.rahman@databricks.com/FE Bar/Multivar_Predictive_Maintenance",
               "/Users/saifur.rahman@databricks.com/FE Bar/Multivar_Demand_Forecasting"]
w = WorkspaceClient()
RUN_TS = datetime.now(timezone.utc)
evidence = []  # (claim, status, detail)
print("Evidence run at", RUN_TS.isoformat(), "by", w.current_user.me().user_name)

# COMMAND ----------

# DBTITLE 1,1. Tables exist and hold data
tbls = (spark.table(f"{CATALOG}.information_schema.tables")
        .filter((F.col("table_schema") == SCHEMA) & F.col("table_name").rlike("^(bronze|silver|gold|sdp)_") & (F.col("table_type") != "FOREIGN")
                # exclude Lakehouse Monitoring output tables (system-generated, may be empty while the monitor is PENDING)
                & ~F.col("table_name").rlike("_(profile|drift)_metrics$"))
        .select("table_name", "table_type").orderBy("table_name").collect())
rows = [(t.table_name, t.table_name.split("_")[0], spark.table(f"{FQ}.{t.table_name}").count()) for t in tbls]
table_types = {t.table_name: t.table_type for t in tbls}
counts = spark.createDataFrame([(r[0], r[1], table_types[r[0]], r[2]) for r in rows], "table_name STRING, layer STRING, table_type STRING, row_count LONG")
display(counts)
empty = [r[0] for r in rows if r[2] == 0]
evidence.append(("1_tables_populated", "PASS" if rows and not empty else "FAIL", f"{len(rows)} tables, {sum(r[2] for r in rows):,} rows, empty={empty}"))

# COMMAND ----------

# DBTITLE 1,2. Delta write history (who/when/how)
hist = []
# DESCRIBE HISTORY only works on Delta tables; materialized views are proven by the SDP update in claim 4
delta_tables = [t for t, _, _ in rows if table_types[t] != "MATERIALIZED_VIEW"]
print("Materialized views covered by claim 4 instead:", [t for t, _, _ in rows if table_types[t] == "MATERIALIZED_VIEW"])
for t in delta_tables:
    h = (spark.sql(f"DESCRIBE HISTORY {FQ}.{t}")
         .filter(F.col("operation").rlike("WRITE|CREATE|MERGE|STREAMING UPDATE|REPLACE"))
         .orderBy("version").limit(1)
         .select(F.lit(t).alias("table_name"), "version", "timestamp", "userName", "operation",
                 F.col("operationMetrics")["numOutputRows"].alias("rows_written"),
                 F.col("notebook.notebookId").alias("notebook_id"), "clusterId", "engineInfo"))
    hist.append(h)
from functools import reduce
history = reduce(lambda a, b: a.unionByName(b), hist)
display(history.orderBy("timestamp"))
evidence.append(("2_delta_history", "PASS" if history.count() == len(delta_tables) else "WARN", f"first-write commit found for {history.count()}/{len(delta_tables)} Delta tables"))

# COMMAND ----------

# DBTITLE 1,3. MLflow runs, metrics, params, artifacts
client = mlflow.MlflowClient()
ml_rows = []
for name in EXPERIMENTS:
    exp = mlflow.get_experiment_by_name(name)
    for r in client.search_runs([exp.experiment_id], order_by=["attributes.start_time DESC"]):
        ml_rows.append((name.split("/")[-1], r.info.run_id, r.info.run_name, r.info.status,
                        datetime.fromtimestamp(r.info.start_time/1000, timezone.utc).isoformat(),
                        str({k: round(v, 4) for k, v in r.data.metrics.items()}), str(r.data.params),
                        str([a.path for a in client.list_artifacts(r.info.run_id)])))
ml = spark.createDataFrame(ml_rows, "experiment STRING, run_id STRING, run_name STRING, status STRING, start_time STRING, metrics STRING, params STRING, artifacts STRING")
display(ml)
finished = [r for r in ml_rows if r[3] == "FINISHED" and r[5] != "{}"]
evidence.append(("3_mlflow_runs", "PASS" if {r[0] for r in finished} == {e.split('/')[-1] for e in EXPERIMENTS} else "FAIL",
                 "; ".join(f"{r[0]}:{r[2] or r[1][:8]} {r[5]}" for r in finished)))

# COMMAND ----------

# DBTITLE 1,4. Lakeflow SDP pipeline updates
p = w.pipelines.get(SDP_PIPELINE_ID)
ups = w.pipelines.list_updates(pipeline_id=SDP_PIPELINE_ID, max_results=10).updates or []
upd = [(u.update_id, str(u.state).split(".")[-1], datetime.fromtimestamp(u.creation_time/1000, timezone.utc).isoformat(), u.full_refresh) for u in ups]
print(f"Pipeline: {p.name} | state={p.state} | catalog={p.spec.catalog} schema={p.spec.schema or p.spec.target}")
display(spark.createDataFrame(upd, "update_id STRING, state STRING, created STRING, full_refresh BOOLEAN") if upd else spark.createDataFrame([], "update_id STRING"))
ok = any(u[1] == "COMPLETED" for u in upd)
evidence.append(("4_sdp_pipeline", "PASS" if ok else "FAIL", f"{len(upd)} updates, latest={upd[0][1] if upd else 'none'}"))

# COMMAND ----------

# DBTITLE 1,4b. SDP expectations (data quality) from event log
try:
    ev = spark.sql(f"SELECT * FROM event_log('{SDP_PIPELINE_ID}')")
    dq = (ev.filter("event_type = 'flow_progress' AND details:flow_progress:data_quality:expectations IS NOT NULL")
            .selectExpr("origin.flow_name AS flow", "timestamp",
                        "explode(from_json(details:flow_progress:data_quality:expectations, 'array<struct<name:string,dataset:string,passed_records:long,failed_records:long>>')) AS e")
            .selectExpr("e.dataset AS dataset", "e.name AS expectation", "e.passed_records AS passed", "e.failed_records AS failed")
            .groupBy("dataset", "expectation").agg(F.sum("passed").alias("passed_records"), F.sum("failed").alias("failed_records")))
    display(dq.orderBy("dataset", "expectation"))
    n = dq.count()
    evidence.append(("4b_sdp_expectations", "PASS" if n > 0 else "FAIL", f"{n} expectations with recorded pass/fail counts"))
except Exception as e:
    print("Event log not readable:", str(e)[:300])
    evidence.append(("4b_sdp_expectations", "FAIL", str(e)[:200]))

# COMMAND ----------

# DBTITLE 1,5. Genie agent: live question, SQL, and result
QUESTION = "How many assets are there per country? Return country and asset count."
msg = w.genie.start_conversation_and_wait(space_id=GENIE_SPACE_ID, content=QUESTION, timeout=timedelta(minutes=5))
print(f"status={msg.status} conversation_id={msg.conversation_id} message_id={msg.id}")
genie_rows, genie_sql = [], None
for a in msg.attachments or []:
    if a.text: print("\nGenie answer:\n", a.text.content)
    if a.query:
        genie_sql = a.query.query
        res = w.genie.get_message_attachment_query_result(space_id=GENIE_SPACE_ID, conversation_id=msg.conversation_id, message_id=msg.id, attachment_id=a.attachment_id).statement_response
        cols = [c.name for c in res.manifest.schema.columns]
        genie_rows = res.result.data_array or []
print("\nGenerated SQL:\n", genie_sql)
genie_df = spark.createDataFrame([(r[0], int(r[1])) for r in genie_rows], "country STRING, genie_asset_count LONG")
display(genie_df)

# COMMAND ----------

# DBTITLE 1,5b. Cross-check Genie result against direct SQL
direct = spark.table(f"{FQ}.silver_unified_assets").filter("country IS NOT NULL").groupBy("country").agg(F.count("*").alias("direct_asset_count"))
cmp = genie_df.join(direct, "country", "full_outer").withColumn("match", F.col("genie_asset_count") == F.col("direct_asset_count"))
display(cmp.orderBy(F.desc("direct_asset_count")))
mism = cmp.filter("match IS NOT TRUE").count()
evidence.append(("5_genie_query", "PASS" if genie_rows and mism == 0 else "FAIL", f"conv={msg.conversation_id} rows={len(genie_rows)} mismatches_vs_direct_sql={mism}"))

# COMMAND ----------

# DBTITLE 1,6. App deployment + backend queries
app = w.apps.get(APP_NAME)
d = app.active_deployment
print(f"App: {app.name} | url={app.url}")
print(f"  app_status={app.app_status.state} | compute={app.compute_status.state}")
print(f"  active deployment {d.deployment_id} state={d.status.state} msg='{d.status.message}' created={d.create_time}")

# Execute the exact SQL the app's /api/maintenance and /api/predictions endpoints run, on the app's own warehouse
APP_QUERIES = {
  "/api/maintenance": "SELECT unified_asset_id, country, region, asset_category, voltage_level, criticality_tier, ROUND(priority_score,2) AS priority_score, ROUND(failure_probability,4) AS failure_prob, ROUND(asset_age_years,1) AS age_years FROM gold_maintenance_priority ORDER BY priority_score DESC LIMIT 50",
  "/api/predictions": "SELECT unified_asset_id, country, asset_category, voltage_level, criticality_tier, ROUND(asset_age_years,1) AS age_years, ROUND(health_score,1) AS health_score, ROUND(ml_failure_probability,4) AS failure_prob, ROUND(ml_priority_score,2) AS priority_score, ml_risk_label FROM gold_ml_failure_predictions ORDER BY ml_priority_score DESC LIMIT 100",
}
app_results = []
for ep, sql in APP_QUERIES.items():
    r = w.statement_execution.execute_statement(warehouse_id=WAREHOUSE_ID, statement=sql, catalog=CATALOG, schema=SCHEMA, wait_timeout="50s")
    n = r.manifest.total_row_count if r.manifest else 0
    app_results.append((ep, str(r.status.state).split(".")[-1], n, r.statement_id))
    if ep == "/api/predictions" and r.result:
        print(f"\nTop 5 rows served by {ep}:")
        cols = [c.name for c in r.manifest.schema.columns]
        display(spark.createDataFrame(r.result.data_array[:5], ", ".join(f"{c} STRING" for c in cols)))
display(spark.createDataFrame(app_results, "endpoint STRING, state STRING, rows_returned LONG, statement_id STRING"))
ok = str(app.app_status.state).endswith("RUNNING") and all(a[1] == "SUCCEEDED" and a[2] > 0 for a in app_results)
evidence.append(("6_app", "PASS" if ok else "FAIL", f"deployment={d.deployment_id} {d.status.state}; " + "; ".join(f"{a[0]}={a[2]} rows" for a in app_results)))

# COMMAND ----------

# DBTITLE 1,7. Lineage captured
# MAGIC %sql
# MAGIC SELECT entity_type,
# MAGIC        COUNT(DISTINCT target_table_full_name) AS target_tables,
# MAGIC        COUNT(DISTINCT source_table_full_name) AS source_tables,
# MAGIC        MIN(event_time) AS first_event, MAX(event_time) AS last_event
# MAGIC FROM system.access.table_lineage
# MAGIC WHERE event_date >= '2026-09-01'
# MAGIC   AND (target_table_full_name LIKE 'saifur_fevm_aws_workspace_catalog.multivar_energy%' OR source_table_full_name LIKE 'saifur_fevm_aws_workspace_catalog.multivar_energy%')
# MAGIC GROUP BY entity_type ORDER BY target_tables DESC

# COMMAND ----------

# DBTITLE 1,7b. Record lineage evidence
lin = spark.sql(f"""SELECT COUNT(DISTINCT target_table_full_name) n, collect_set(entity_type) types FROM system.access.table_lineage
  WHERE event_date >= '2026-09-01' AND target_table_full_name LIKE '{FQ}%' AND source_table_full_name IS NOT NULL""").first()
evidence.append(("7_lineage", "PASS" if lin.n > 0 else "FAIL", f"{lin.n} target tables with upstream lineage via {sorted(lin.types)}"))

# COMMAND ----------

# DBTITLE 1,8. Persist evidence summary
ev_df = (spark.createDataFrame(evidence, "claim STRING, status STRING, detail STRING")
         .withColumn("evidence_run_ts", F.lit(RUN_TS)).withColumn("run_by", F.expr("current_user()")))
ev_df.write.mode("append").saveAsTable(f"{FQ}.build_execution_evidence")
spark.sql(f"COMMENT ON TABLE {FQ}.build_execution_evidence IS 'Append-only log of build execution evidence produced by FE Bar/07_Build_Execution_Evidence.'")
display(spark.table(f"{FQ}.build_execution_evidence").filter(F.col("evidence_run_ts") == F.lit(RUN_TS)).orderBy("claim"))
print(f"{sum(e[1]=='PASS' for e in evidence)}/{len(evidence)} claims PASS")

# COMMAND ----------

