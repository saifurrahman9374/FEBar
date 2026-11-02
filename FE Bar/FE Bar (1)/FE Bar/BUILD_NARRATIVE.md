# Multivar Energy - How It Was Built

This is the build workflow as it actually happened, reconstructed from execution traces rather than from memory: cell run timestamps and errors in the authoring notebook, Delta commit history, MLflow run records, the Apps API, pipeline event logs and Unity Catalog system tables. Numbers and times come from those records; `EXECUTION_EVIDENCE.md` has the full output.

The work was done in two sessions with the Databricks AI assistant (Genie Code) working inside the workspace: it wrote and ran the cells, read the results and errors, and changed course based on them. A human set the goal, reviewed the results and owned every permission-changing or destructive action.

| Session | When (UTC) | Where the work ran | Outcome |
| --- | --- | --- | --- |
| 1. Initial build | 2026-09-28 00:17 - 01:56 (about 1 h 40 min) | Authoring notebook `2264937862003406` ("Data Pipeline and AI Horizons Strategy") | Medallion tables, 2 ML models, Lakebase serving, 8 Genie spaces, AI/BI dashboard, app |
| 2. Review remediation | 2026-11-01 - 2026-11-02 03:10 | Pipeline `11cf7db6-...`, notebooks `06` and `07` | Auto Loader + SDP ingestion with expectations, UC governance controls, re-runnable evidence |

The `01`-`05` notebooks in this folder are a tidied copy of the session-1 code and were never run as files; every table and MLflow run traces back to the authoring notebook (Delta `notebook.notebookId` and MLflow `mlflow.databricks.notebookID` both equal `2264937862003406`).

---

## Session 1 timeline (2026-09-28)

| Time | Step | What happened | Trace |
| --- | --- | --- | --- |
| 00:17 | Read the brief | The uploaded brief was parsed with `ai_parse_document` and its text pulled out to drive the plan (three horizons, 20 DSOs, 14 countries) | authoring cell 6 |
| 00:18 | Create the namespace | `CREATE CATALOG multivar_energy` failed: no CREATE CATALOG on the metastore | cell 7 error |
| 00:19 | Retry | `CREATE SCHEMA fevm_shared_catalog.multivar_energy` failed: no CREATE SCHEMA on the shared catalog | cell 9 error |
| 00:19 | Settle | Schema created in the personal catalog `saifur_fevm_aws_workspace_catalog.multivar_energy` | cell 10 |
| 00:20 - 00:23 | Bronze | 7 synthetic source tables generated and written (asset registry 9,828 rows ... DER telemetry 215,460) | Delta commits 00:20:40 - 00:23:10 |
| 00:23 - 00:25 | Silver | Unified asset model, BAD sensor readings filtered (203,145 -> 201,125), invalid meter readings filtered (273,015 -> 272,920) | Delta commits 00:23:59 - 00:25:05 |
| 00:26 - 00:27 | Gold | Health scores, SAIDI/SAIFI, demand features, maintenance priority, DER, executive KPIs | Delta commits 00:26:03 - 00:27:12 |
| 00:28 | ML attempt 1 | `GBM_Failure_Prediction` failed: `y contains 1 class` | MLflow run `10f5c45c` FAILED, cell 19 error |
| 00:28 | Diagnose | Printed the label distribution: `HEALTHY 9811, MODERATE 17`, no CRITICAL/HIGH_RISK; `avg_temperature_c` null for 9,460 of 9,828 assets | cell 20 output |
| 00:29 | ML attempt 2 | Target redesigned (see change 3 below); regression and classification runs FINISHED | runs `f70ddbd0`, `ed7661ed`; `gold_ml_failure_predictions` at 00:29:56 |
| 00:30 - 00:32 | Demand model | GBR forecast, MAPE 3.77%; results written | run `25523ae5`; `gold_demand_forecast_results` at 00:32:19 |
| 00:34 | Document | Table comments written for all tables | cell 23 |
| 00:40 - 00:42 | Operational serving | SDK upgraded, Lakebase project `multivar-energy-grid` (PG 17) created, CDF enabled on 6 Gold tables, 4 snapshot synced tables created | cells 25-29; PIPELINE lineage events from 00:42:09 |
| 00:43 | App created | `multivar-energy-grid-ops` created ("App created by assistant") | Apps API `create_time` |
| 00:44 | Dashboard | AI/BI dashboard starts reading Gold tables | DASHBOARD_V3 lineage from 00:44:45 |
| 01:11 | App v1 live | Deployment `01f1bad994...` SUCCEEDED, 8 Genie spaces attached as app resources | cell 30 output |
| 01:21 - 01:55 | App iterations | Redeploy, fetch app logs, fix, redeploy; final deployment `01f1badfb46d16fe91786cbfa89de0f5` | cells 30-38, plus log-runner cells 1-4 (log pulls at 01:23, 01:29, 01:39, 01:55) |

---

## Where the AI assistant materially changed the work

Each item records the plan, what the assistant saw, what it changed, and what that cost.

### 1. Requirements came from parsing the brief, not from a hand-written spec
The assistant extracted the brief's text in SQL with `ai_parse_document` and built the three-horizon structure, the 20-DSO / 14-country scope and the agent list from it. This is why the README's horizons and agent names track the brief closely. Trade-off: the plan inherited the brief's ambition (8 agents, autonomy levels 2-4) without anyone checking first whether each part could be evidenced.

### 2. Permission errors moved the namespace
The planned layout was a dedicated catalog (`multivar_energy.grid_platform`). Two permission denials 30 seconds apart pushed the assistant to a personal catalog. Everything downstream (pipeline, app, Genie, Lakebase) uses `saifur_fevm_aws_workspace_catalog.multivar_energy`, and the catalog's inherited grants later turned out to undermine least privilege (change 7).

### 3. The failure model's target was replaced after the first run failed (largest change)
* Plan: classify assets whose rule-based `risk_classification` was CRITICAL or HIGH_RISK.
* What the assistant saw: the classifier crashed because that class had zero rows. Its diagnostic cell showed only 17 MODERATE and 9,811 HEALTHY assets, and telemetry missing for 96% of assets (9,460 of 9,828), so the health score barely moved.
* What it changed: it built a `simulated_failure_risk` score from asset age, anomaly rate, load, a temperature over 80 C, emergency repairs and Gaussian noise; trained a regressor on that score; and trained a classifier on `score > 0.45`.
* Consequence: training succeeded about 90 seconds after the failure, but the label is a formula of the model's own inputs. R² 0.637 therefore measures how well the GBM recovers that formula through the noise, not how well it predicts real failures. The classifier's accuracy of 0.993 comes from class imbalance; its failure-class precision is 0.14 and recall 0.125. The README quotes these metrics without that caveat, so they should be read as a pattern demo.
* What a human should decide: whether to source real failure labels (for example from CMMS corrective work orders), or keep the synthetic target and present it only as a pattern demo.

### 4. Demand forecast accepted at face value (a gap, not a change)
The demand model scored R² 0.9987 on its first run and the assistant moved on. `active_meters` is one of the features, and total consumption scales with meter count, so most of that R² is probably explained by scale rather than weather or calendar effects. The README lists `customer_type`, `active_meters` and `power_factor` as the top features, which is consistent with this, but the feature-importance artifact was not re-read for this document. A per-meter target or a time-based holdout would test it.

### 5. The app was debugged from its own logs
The assistant created the app, deployed it, pulled the build and runtime logs into the notebook (a PAT-to-OAuth token exchange, because the log endpoint is behind the app proxy), and redeployed between 01:11 and 01:55 (the logs show at least three deployed source snapshots: `01f1bad994...` at 01:11, `01f1badbfdff...` around 01:29, and the final deployment `01f1badfb46d...`). The deployed app is FastAPI with an inline React SPA (`main.py`); an earlier Gradio version (`app.py`) is still in the folder but unused. The single-file SPA is a speed trade-off: no build step, but harder to test.

### 6. Serving keyed on a non-unique column
For the Lakebase synced tables the assistant used `unified_asset_id` as the primary key. The session-2 data-quality checks found that column has 4 duplicate values. The synced-table creation calls returned without error, but how the sync handled the duplicate keys has not been checked.

### 7. Session 2: the review gaps were closed by measuring first
Asked to close the review gaps (no SDP/Auto Loader ingestion, thin governance, no execution evidence), the assistant first audited the workspace. It found that notebooks `04`/`05` had never run, the existing pipeline had never run, and the data actually came from the authoring notebook. That audit decided the approach:
* **Ingestion:** instead of rewriting the batch notebooks, it exported each Bronze table to a UC volume landing zone (7 sources, 2 batches each, counts reconciled exactly) and pointed Auto Loader at it. It profiled the raw data before writing any expectations and set the drop rules from what it found: the 2,020 BAD sensor rows, 94 negative kWh readings and 1,606 out-of-range power factors in the profile are exactly what the pipeline run dropped. Other rules (key presence, European latitude, timestamp parsing) are guards that currently pass at 100%. Tables carry an `sdp_` prefix so the pipeline cannot overwrite the tables already serving the app, Genie and the dashboard.
* **Pipeline settings:** a schema change on the pipeline was blocked by a guardrail, so the assistant used fully qualified table names instead.
* **Governance:** the first tag cell failed because account-level governed tag policies restrict `domain`, `owner`, `regulation`, `source_system` and `pii`. The assistant queried the policies and switched to an allowed `domain` value and project-scoped keys rather than dropping the tags. `SHOW GRANTS` then revealed that the app service principal and `account users` hold `ALL PRIVILEGES` on the catalog, which makes the planned table-level GRANTs ineffective. It reported this rather than claiming least privilege was in place.
* **Evidence:** `07_Build_Execution_Evidence` re-derives each claim from its system of record (Delta log, MLflow, pipeline event log, Genie API, Apps API, lineage) and checks Genie's answer against independent SQL. Two failures in that notebook were fixed in place: it was counting the quality monitor's own output tables, and it ran `DESCRIBE HISTORY` on materialized views.

---

## What the human decided, and what guardrails stopped

* **Human-owned:** the goal and scope; accepting the personal-catalog namespace; approving the pipeline run; any GRANT or REVOKE.
* **Stopped by guardrails in session 2, still pending:** a pipeline full refresh (a regular first update ran instead and processed every file); the least-privilege GRANT cell; deleting a stray empty notebook. Revoking the catalog-level `ALL PRIVILEGES` needs the catalog owner.

## Known limitations carried forward

* Synthetic source data and a synthetic failure label (change 3).
* No serialized model in MLflow, although the code called `log_model`; models cannot be reloaded or served from the tracking server.
* One live Genie transcript (Grid Operations agent); the other 7 spaces have not been exercised in evidence.
* `unified_asset_id` is not unique (4 duplicates) and is used as a sync key (change 6).
