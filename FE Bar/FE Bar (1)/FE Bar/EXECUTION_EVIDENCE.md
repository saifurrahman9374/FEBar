# Multivar Energy - Committed Execution Evidence

This file is a readable, committed snapshot of real execution output. Every number below was copied from a cell result, a pipeline event log, the MLflow tracking server, the Genie Conversation API, or the Apps API. Nothing here is a target or an estimate.

* Snapshot captured: **2026-11-02 03:07:42 UTC** by `saifur.rahman@databricks.com`
* Re-runnable source: `07_Build_Execution_Evidence` (outputs saved in the notebook) and `06_Unity_Catalog_Governance` (outputs saved)
* Machine-readable copy: table `saifur_fevm_aws_workspace_catalog.multivar_energy.build_execution_evidence`
* Workspace: `https://fevm-saifur-fevm-aws-workspace.cloud.databricks.com` (workspace id 7474651427662410)

## Summary: 8 of 8 claims PASS

| Claim | Status | Evidence (verbatim from `build_execution_evidence`) |
| --- | --- | --- |
| 1 Tables populated | PASS | 38 tables, 3,154,172 rows, empty=[] |
| 2 Delta history | PASS | first-write commit found for 36/36 Delta tables |
| 3 MLflow runs | PASS | finished runs with logged metrics in both experiments |
| 4 SDP pipeline | PASS | 1 updates, latest=COMPLETED |
| 4b SDP expectations | PASS | 23 expectations with recorded pass/fail counts |
| 5 Genie query | PASS | conv=01f1d66ae0eb1b7eb0af54f3bb848505 rows=14 mismatches_vs_direct_sql=0 |
| 6 App | PASS | deployment=01f1badfb46d16fe91786cbfa89de0f5 SUCCEEDED; /api/maintenance=50 rows; /api/predictions=100 rows |
| 7 Lineage | PASS | 25 target tables with upstream lineage via JOB, NOTEBOOK, PIPELINE |

---

## 1. Lakeflow Spark Declarative Pipeline run

* Pipeline: `Multivar Energy Grid Intelligence Pipeline` (`11cf7db6-d07a-4a14-a370-b58850c05d55`), serverless, Photon
* Dry run (validation only): update `58eb7134-3b83-40e9-ae2f-711e6d7e5d82` - COMPLETED
* Executed update: `beb714b5-cc60-4687-bb0b-ae90603529fe` - created 2026-11-02 02:41:30 UTC, **COMPLETED** 02:42:38 UTC, `full_refresh=false`
* A full refresh was requested but held by a manual-approval guardrail; the regular update above is the executed evidence (it is the first run, so it processed every file).
* Pipeline compute: cluster `1102-024131-g81yxgum-v2n`, engine `dlt:18.3.9-delta-pipelines-aarch64-photon`

### Row counts produced by the run (Auto Loader from `/Volumes/.../raw_landing`)

| Source folder | `sdp_bronze_*` | `sdp_silver_*` | Dropped by expectations |
| --- | --- | --- | --- |
| scada_asset_registry | 9,828 | 9,828 (`sdp_silver_assets`) | 0 |
| scada_sensor_telemetry | 203,145 | 201,125 (`sdp_silver_sensor_readings`) | 2,020 |
| ami_smart_meter | 273,015 | 271,315 | 1,700 |
| weather_api | 14,400 | 14,400 | 0 |
| cmms_work_orders | 5,000 | 5,000 | 0 |
| oms_outage_events | 2,000 | 2,000 | 0 |
| derms_der_telemetry | 215,460 | 215,460 | 0 |

Gold materialized views: `sdp_gold_asset_health_summary` = 200 rows, `sdp_gold_der_daily_summary` = 12,105 rows.

Cross-check: every `sdp_bronze_*` count equals the row count of the files exported to the landing volume and of the original `bronze_*` table (independent SQL `COUNT(*)` run after the update).

### Expectation results (pipeline event log, `flow_progress.data_quality`)

| Dataset | Expectation | Action | Passed | Failed |
| --- | --- | --- | --- | --- |
| sdp_silver_assets | asset_id_required | fail | 9,828 | 0 |
| sdp_silver_assets | asset_dso_required | fail | 9,828 | 0 |
| sdp_silver_assets | asset_latitude_in_europe | drop | 9,828 | 0 |
| sdp_silver_assets | asset_install_date_valid | warn | 9,828 | 0 |
| sdp_silver_sensor_readings | sensor_asset_required | fail | 203,145 | 0 |
| sdp_silver_sensor_readings | **sensor_quality_not_bad** | drop | 201,125 | **2,020** |
| sdp_silver_sensor_readings | sensor_timestamp_valid | warn | 203,145 | 0 |
| sdp_silver_smart_meter_readings | meter_dso_required | fail | 273,015 | 0 |
| sdp_silver_smart_meter_readings | **meter_non_negative_consumption** | drop | 272,921 | **94** |
| sdp_silver_smart_meter_readings | **meter_power_factor_valid** | drop | 271,409 | **1,606** |
| sdp_silver_smart_meter_readings | meter_timestamp_valid | warn | 273,015 | 0 |
| sdp_silver_weather | weather_timestamp_valid | warn | 14,400 | 0 |
| sdp_silver_work_orders | work_order_id_required | fail | 5,000 | 0 |
| sdp_silver_work_orders | work_order_asset_required | fail | 5,000 | 0 |
| sdp_silver_work_orders | work_order_dso_required | fail | 5,000 | 0 |
| sdp_silver_work_orders | work_order_created_timestamp_valid | warn | 5,000 | 0 |
| sdp_silver_outage_events | outage_id_required | fail | 2,000 | 0 |
| sdp_silver_outage_events | outage_dso_required | fail | 2,000 | 0 |
| sdp_silver_outage_events | outage_start_timestamp_valid | warn | 2,000 | 0 |
| sdp_silver_der_telemetry | der_id_required | fail | 215,460 | 0 |
| sdp_silver_der_telemetry | der_dso_required | fail | 215,460 | 0 |
| sdp_silver_der_telemetry | der_soc_valid_when_present | drop | 215,460 | 0 |
| sdp_silver_der_telemetry | der_timestamp_valid | warn | 215,460 | 0 |

The smart-meter flow event reports `dropped_records: 1700` = 94 + 1,606 (no row failed both rules). These failure counts match the pre-pipeline data profile (2,020 BAD sensor rows, 94 negative kWh, 1,606 power factor outside 0-1), which is why these thresholds were chosen.

---

## 2. Original medallion build (Delta transaction log)

First write of each table, from `DESCRIBE HISTORY`. All were written by notebook `2264937862003406` (the authoring session) on cluster `0928-001632-o8pw9ap-v2n`, `Databricks-Runtime/19.8.x-aarch64-photon`.

| Table | Commit (UTC, 2026-09-28) | Operation | Rows written |
| --- | --- | --- | --- |
| bronze_asset_registry | 00:20:40 | CREATE OR REPLACE TABLE AS SELECT | 9,828 |
| bronze_sensor_telemetry | 00:21:23 | CTAS | 203,145 |
| bronze_smart_meter_readings | 00:22:09 | CTAS | 273,015 |
| bronze_weather_data | 00:22:13 | CTAS | 14,400 |
| bronze_work_orders | 00:22:59 | CTAS | 5,000 |
| bronze_outage_events | 00:23:02 | CTAS | 2,000 |
| bronze_der_telemetry | 00:23:10 | CTAS | 215,460 |
| silver_unified_assets | 00:23:59 | CTAS | 9,828 |
| silver_sensor_telemetry | 00:24:04 | CTAS | 201,125 |
| silver_weather_enriched | 00:24:51 | CTAS | 14,400 |
| silver_work_orders | 00:24:54 | CTAS | 5,000 |
| silver_outage_events | 00:24:58 | CTAS | 2,000 |
| silver_der_performance | 00:25:01 | CTAS | 215,460 |
| silver_smart_meter_readings | 00:25:05 | CTAS | 272,920 |
| gold_asset_health_scores | 00:26:03 | CTAS | 9,828 |
| gold_grid_reliability_metrics | 00:26:07 | CTAS | 60 |
| gold_demand_forecast_features | 00:27:01 | CTAS | 108,000 |
| gold_maintenance_priority | 00:27:04 | CTAS | 8,475 |
| gold_der_optimization | 00:27:08 | CTAS | 12,105 |
| gold_executive_kpis | 00:27:12 | CTAS | 14 |
| gold_ml_failure_predictions | 00:29:56 | CTAS | 9,828 |
| gold_demand_forecast_results | 00:32:19 | CTAS | 108,000 |

The 14 `sdp_*` streaming tables show first commit `CREATE TABLE` at 2026-11-02 02:42:01 UTC from the pipeline cluster.

---

## 3. MLflow runs (tracking server)

All runs were launched from notebook `2264937862003406` (tag `mlflow.databricks.notebookID`).

| Experiment | Run | Name | Status | Started (UTC) | Logged metrics | Params |
| --- | --- | --- | --- | --- | --- | --- |
| Multivar_Predictive_Maintenance | `10f5c45c06ab43fb8ad4d4c284b8cabd` | GBM_Failure_Prediction | **FAILED** | 2026-09-28 00:28:28 | none | none |
| Multivar_Predictive_Maintenance | `f70ddbd0d5944284a00465e4c90f85c8` | Failure_Probability_Regression | FINISHED | 2026-09-28 00:29:33 | mae 0.0407, r2_score 0.6371 | model GBR, n_estimators 200, max_depth 5, features 20 |
| Multivar_Predictive_Maintenance | `ed7661ed742f47bb9becdfb1143d69ab` | Failure_Classification | FINISHED | 2026-09-28 00:29:44 | accuracy 0.9934, auc_roc 0.7437, precision_failure 0.1429, recall_failure 0.125 | model GBC, n_estimators 200, max_depth 5 |
| Multivar_Demand_Forecasting | `25523ae51029442d834093a049c1bec9` | Demand_Forecast_GBR | FINISHED | 2026-09-28 00:30:35 | mae 101.9091, rmse 170.5219, r2_score 0.9987, mape 3.7655 | model GBR, n_estimators 300, max_depth 6, lr 0.08 |

Artifacts: `feature_importance.json` on `f70ddbd0` and `25523ae5`; none on `ed7661ed`. No serialized model artifact is present on any run even though the code called `mlflow.sklearn.log_model`, so the models cannot be reloaded from MLflow today.

Failure of `10f5c45c` (cell error in the authoring notebook, 00:28:29 UTC): `ValueError: y contains 1 class after sample_weight trimmed classes with zero weights, while a minimum of 2 classes are required.` The diagnostic cell that followed printed the cause: `risk_classification` was `HEALTHY 9811 / MODERATE 17`, so the intended positive class (CRITICAL or HIGH_RISK) had zero rows. See `BUILD_NARRATIVE.md` for how this changed the model design.

---

## 4. Genie transcript (live, Conversation API)

* Space: Grid Operations Intelligence Agent (`01f1bad41f991a4fadd4fee40820a20a`)
* Conversation `01f1d66ae0eb1b7eb0af54f3bb848505`, message `01f1d66ae0fd133d84ea0fd6c1f7d091`, status `COMPLETED`

**Question (sent at 2026-11-02 ~03:10 UTC):** How many assets are there per country? Return country and asset count.

**Genie answer (verbatim):**

> There are **14 countries** with non-null asset records in the results. Examples include:
> - **Germany**: **1,556** assets
> - **Netherlands**: **1,042** assets
> - **Spain**: **1,023** assets
> - **France**: **985** assets
> - **Italy**: **898** assets
>
> Among the countries shown, **Germany** has the highest asset count and **Austria** has the lowest at **418**.

**Genie-generated SQL (verbatim):**

```sql
SELECT `country`, COUNT(*) AS `asset_count`
FROM `saifur_fevm_aws_workspace_catalog`.`multivar_energy`.`silver_unified_assets`
WHERE `country` IS NOT NULL
GROUP BY `country`
ORDER BY `asset_count` DESC, `country` ASC
```

**Result vs independent SQL** (computed separately in the notebook, not via Genie):

| Country | Genie | Direct SQL | Match |
| --- | --- | --- | --- |
| Germany | 1,556 | 1,556 | yes |
| Netherlands | 1,042 | 1,042 | yes |
| Spain | 1,023 | 1,023 | yes |
| France | 985 | 985 | yes |
| Italy | 898 | 898 | yes |
| Switzerland | 569 | 569 | yes |
| Denmark | 529 | 529 | yes |
| Poland | 508 | 508 | yes |
| Sweden | 488 | 488 | yes |
| Belgium | 475 | 475 | yes |
| Portugal | 453 | 453 | yes |
| Ireland | 443 | 443 | yes |
| Norway | 441 | 441 | yes |
| Austria | 418 | 418 | yes |

Scope note: this is one transcript from one of the 8 spaces. The other 7 spaces were not exercised in this evidence run.

---

## 5. Databricks App

* App `multivar-energy-grid-ops`, URL `https://multivar-energy-grid-ops-7474651427662410.aws.databricksapps.com`
* Apps API: `app_status=RUNNING`, `compute=ACTIVE`, active deployment `01f1badfb46d16fe91786cbfa89de0f5` state `SUCCEEDED`
* The deployment log captured during the build (authoring notebook, cell run 2026-09-28 01:55 UTC) shows `Starting app with command: [uvicorn main:app --host 0.0.0.0 --port 8000]`, `Application startup complete.` and a closing line for deployment `01f1badfb46d16fe91786cbfa89de0f5`.
* Backend queries: the exact SQL behind two app endpoints was executed on the app's bound warehouse `a31d61d12430e28f` via the Statement Execution API.

| Endpoint | Statement state | Rows | Statement id |
| --- | --- | --- | --- |
| /api/maintenance | SUCCEEDED | 50 | 01f1d66a-f981-180b-8510-3d8bb84be017 |
| /api/predictions | SUCCEEDED | 100 | 01f1d66a-fa79-1cf1-9ac9-1161c1b95fec |

First 5 rows of `/api/predictions`:

| unified_asset_id | country | asset_category | voltage | tier | age_years | health_score | failure_prob | priority_score | ml_risk_label |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| UA-ES-504083 | Spain | transformer | EHV | TIER_1_CRITICAL | 22.9 | 85.8 | 0.4424 | 42.03 | HIGH_RISK |
| UA-DE-760328 | Germany | substation | EHV | TIER_1_CRITICAL | 40.0 | 80.0 | 0.4325 | 41.09 | LOW_RISK |
| UA-ES-774981 | Spain | transformer | EHV | TIER_1_CRITICAL | 30.6 | 84.0 | 0.4248 | 40.36 | HIGH_RISK |
| UA-DE-041429 | Germany | substation | EHV | TIER_1_CRITICAL | 28.0 | 85.4 | 0.4221 | 40.10 | HIGH_RISK |
| UA-DE-487194 | Germany | substation | EHV | TIER_1_CRITICAL | 31.5 | 74.2 | 0.4165 | 39.57 | LOW_RISK |

Scope note: an authenticated HTTP call to the app URL was not made (token exchange was blocked), so this proves the deployment and its data queries, not a browser session.

---

## 6. Unity Catalog governance (from `06_Unity_Catalog_Governance` outputs)

| Control | Executed result |
| --- | --- |
| Tags | 34 schema/table/column tags visible in `information_schema.*_tags` (e.g. `meter_id` -> `multivar_pii=customer_identifier`) |
| Comments | 5 column comments visible in `information_schema.columns` |
| Row filter | `fn_dso_row_filter` attached to `bronze_smart_meter_readings.dso_id` |
| Column mask | `fn_mask_meter_id` attached to `bronze_smart_meter_readings.meter_id`; example output `MTR-a9246d1a465e` |
| Entitlement scope | admin sees 273,015 rows; app service principal is entitled to 27,360 rows (DSO_DE_01, DSO_FR_01) |
| CHECK constraint | Insert of `ml_failure_probability = 1.75` rejected: `DELTA_VIOLATE_CONSTRAINT_WITH_VALUES ... chk_failure_prob_range`; row count 9,828 before and after |
| Rule-based DQ | `governance_dq_results` at 2026-11-02 02:51:24 UTC: 3 PASS, 3 WARN (2,020 BAD sensor rows; 95 meter rows removed in the legacy silver table; 4 duplicate `unified_asset_id`) |
| Quality monitor | Snapshot monitor created on `gold_ml_failure_predictions`, status PENDING; profile metrics table has 17 rows |
| Lineage | 22 table-level edges and 100 column-level edges (query `LIMIT 100` reached) for `multivar_energy`; entity types NOTEBOOK (24 targets), PIPELINE (20), JOB (2) |
| Audit | 50 most recent Unity Catalog audit rows, including the pipeline's `updateTables` calls at 02:42 UTC |

**Open gap, stated plainly:** the least-privilege GRANT cell was not executed (held by a permission-change approval guardrail). More importantly, `SHOW GRANTS` shows the app service principal and `account users` already hold `ALL PRIVILEGES` on the catalog, so table-level least privilege is not in effect until those catalog grants are revoked by the catalog owner.

---

## 7. What this evidence does not prove

* Model quality on real failures: the predictive-maintenance target is synthetic (see `BUILD_NARRATIVE.md`), so R² 0.637 measures recovery of a rule-based risk formula, not real-world failure prediction.
* Reloadable models: no model artifact was logged.
* Notebooks `04` and `05` as files were never run; training ran in the authoring notebook `2264937862003406`.
* 7 of 8 Genie spaces and the AI/BI dashboard were not exercised in this evidence run.
