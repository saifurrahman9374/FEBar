# ⚡ Multivar Energy — Agentic AI-Driven Grid Resilience & Digitization

> **End-to-end data journey** on the Databricks Lakehouse Platform for a European green energy grid provider operating across 20 DSOs in 14 countries.

---

## Architecture Flow Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────┐
│                        MULTIVAR ENERGY — PLATFORM ARCHITECTURE                          │
│                     Agentic AI-Driven Grid Resilience & Digitization                    │
└─────────────────────────────────────────────────────────────────────────────────────────┘

 ┌───────────────────────────────────────────────────────────────────────────────────────┐
 │                            SOURCE SYSTEMS (20 DSOs × 14 Countries)                    │
 │                                                                                       │
 │   SCADA/EMS    AMI/IoT     SAP/ERP    CMMS/EAM    GIS/Weather   OMS      DERMS       │
 │   ┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐    ┌──────┐  ┌──────┐     │
 │   │RTU   │    │Smart │    │Work  │    │Maint │    │Weather│    │Outage│  │Solar │     │
 │   │IED   │    │Meters│    │Orders│    │Assets│    │GIS    │    │Events│  │Battery│    │
 │   │PMU   │    │Sensors│   │Billing│   │Parts │    │Maps   │    │Crews │  │EV/DR │    │
 │   └──┬───┘    └──┬───┘    └──┬───┘    └──┬───┘    └──┬───┘    └──┬───┘  └──┬───┘     │
 └──────┼───────────┼───────────┼───────────┼───────────┼───────────┼────────┼──────────┘
        │           │           │           │           │           │        │
        ▼           ▼           ▼           ▼           ▼           ▼        ▼
 ┌───────────────────────────────────────────────────────────────────────────────────────┐
 │  HORIZON 1: INTELLIGENT DATA FOUNDATION         ◆ Lakeflow SDP Pipeline              │
 │  ─────────────────────────────────────────────────────────────────────────────────     │
 │                                                                                       │
 │  ┌─────────────────────────────────── BRONZE ──────────────────────────────────────┐   │
 │  │  Raw ingestion — append-only, schema-on-read                                   │   │
 │  │                                                                                │   │
 │  │  bronze_asset_registry (9,828)    bronze_sensor_telemetry (203K)                │   │
 │  │  bronze_smart_meter_readings (273K) bronze_weather_data (14K)                   │   │
 │  │  bronze_work_orders (5K)           bronze_outage_events (2K)                    │   │
 │  │  bronze_der_telemetry (215K)                                                   │   │
 │  └────────────────────────────────────┬────────────────────────────────────────────┘   │
 │                                       │ Clean, Validate, Deduplicate                  │
 │                                       ▼                                               │
 │  ┌─────────────────────────────────── SILVER ──────────────────────────────────────┐   │
 │  │  Conformed, unified, enriched — canonical asset identity model                 │   │
 │  │                                                                                │   │
 │  │  silver_unified_assets        silver_sensor_telemetry (anomaly flags)           │   │
 │  │  silver_smart_meter_readings  silver_weather_enriched (severity class)          │   │
 │  │  silver_work_orders           silver_outage_events (severity class)             │   │
 │  │  silver_der_performance       (capacity factors, net power)                     │   │
 │  └────────────────────────────────────┬────────────────────────────────────────────┘   │
 │                                       │ Aggregate, Score, Compute KPIs                │
 │                                       ▼                                               │
 │  ┌─────────────────────────────────── GOLD ────────────────────────────────────────┐   │
 │  │  Business-ready, governed data products                                        │   │
 │  │                                                                                │   │
 │  │  gold_asset_health_scores    gold_grid_reliability_metrics (SAIDI/SAIFI)        │   │
 │  │  gold_demand_forecast_features gold_maintenance_priority                        │   │
 │  │  gold_der_optimization       gold_executive_kpis                                │   │
 │  │  gold_ml_failure_predictions gold_demand_forecast_results                       │   │
 │  └────────────────────────────────────┬────────────────────────────────────────────┘   │
 │                                       │                                               │
 │  ◆ Unity Catalog Governance: fine-grained access, lineage, CDF, audit logging         │
 └───────────────────────────────────────┼───────────────────────────────────────────────┘
                                         │
              ┌──────────────────────────┼──────────────────────────┐
              │                          │                          │
              ▼                          ▼                          ▼
 ┌────────────────────┐   ┌────────────────────────┐   ┌────────────────────────┐
 │  HORIZON 2:        │   │  OPERATIONAL SERVING    │   │  EXECUTIVE DASHBOARD   │
 │  AI-POWERED        │   │  ◆ Lakebase Postgres    │   │  ◆ AI/BI Dashboard     │
 │  DECISION SUPPORT  │   │                         │   │                        │
 │                    │   │  multivar-energy-grid    │   │  5 Pages / 24 Widgets  │
 │  ┌──────────────┐  │   │  ┌───────────────────┐  │   │                        │
 │  │ Predictive   │  │   │  │ asset_health      │  │   │  • Executive Overview  │
 │  │ Maintenance  │  │   │  │ maintenance_queue  │  │   │  • Asset Health        │
 │  │ GBM Model    │──│──▶│  │ executive_kpis    │  │   │  • Grid Reliability    │
 │  │ R²: 0.637    │  │   │  │ ml_predictions    │  │   │  • Renewable & DER     │
 │  └──────────────┘  │   │  └───────────────────┘  │   │  • ML Predictions      │
 │  ┌──────────────┐  │   │                         │   │                        │
 │  │ Demand       │  │   │  Synced via Reverse ETL  │   │  Dark theme, green/    │
 │  │ Forecasting  │  │   │  (Snapshot mode)         │   │  blue energy palette   │
 │  │ GBM Model    │  │   │                         │   │                        │
 │  │ R²: 0.999    │  │   └────────────┬────────────┘   └────────────────────────┘
 │  │ MAPE: 3.77%  │  │                │
 │  └──────────────┘  │                ▼
 │                    │   ┌────────────────────────┐
 │  ◆ MLflow Tracking │   │  DATABRICKS APP         │
 │  & Model Registry  │   │  ◆ Databricks Apps      │
 └────────────────────┘   │                         │
                          │  multivar-energy-       │
                          │  grid-ops               │
                          │                         │
                          │  Grid Operations Portal │
                          │  for Field Engineers &  │
                          │  Grid Operators          │
                          └────────────────────────┘

 ┌───────────────────────────────────────────────────────────────────────────────────────┐
 │  HORIZON 3: AGENTIC AI OPERATIONS            ◆ Genie Agents (8 Spaces)               │
 │  ─────────────────────────────────────────────────────────────────────────────────     │
 │                                                                                       │
 │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                  │
 │  │ Asset Health  │ │ Outage       │ │ DER          │ │ Grid         │                  │
 │  │ Agent        │ │ Response     │ │ Orchestration│ │ Planning     │                  │
 │  │              │ │ Agent        │ │ Agent        │ │ Agent        │                  │
 │  │ Monitors     │ │ Correlates   │ │ Optimizes    │ │ Interconnect │                  │
 │  │ telemetry,   │ │ OMS/AMI/     │ │ batteries,   │ │ studies,     │                  │
 │  │ predicts     │ │ SCADA/       │ │ solar, EV,   │ │ power flow,  │                  │
 │  │ failures,    │ │ weather,     │ │ demand       │ │ capacity     │                  │
 │  │ generates    │ │ proposes     │ │ response     │ │ planning     │                  │
 │  │ work orders  │ │ restoration  │ │ within limits│ │ reports      │                  │
 │  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘                  │
 │                                                                                       │
 │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐                  │
 │  │ Regulatory   │ │ Cyber-Phys.  │ │ Grid Ops     │ │ Executive    │                  │
 │  │ Compliance   │ │ Security     │ │ Intelligence │ │ Energy       │                  │
 │  │ Agent        │ │ Agent        │ │ Agent        │ │ Dashboard    │                  │
 │  │              │ │              │ │ (Horizon 2)  │ │ Agent        │                  │
 │  │ Tracks       │ │ Detects      │ │              │ │ (Horizon 2)  │                  │
 │  │ regulatory   │ │ unusual      │ │ NL queries   │ │              │                  │
 │  │ commitments, │ │ IT/OT        │ │ for grid     │ │ NL queries   │                  │
 │  │ generates    │ │ behavior,    │ │ operators &  │ │ for CIOs &   │                  │
 │  │ audit filings│ │ recommends   │ │ planners     │ │ executives   │                  │
 │  └──────────────┘ │ containment  │ └──────────────┘ └──────────────┘                  │
 │                    └──────────────┘                                                    │
 │                                                                                       │
 │  Supervised Autonomy: Level 2 (Recommend) → Level 3 (Execute w/ Approval)             │
 │  → Level 4 (Bounded Automation within policy limits)                                  │
 └───────────────────────────────────────────────────────────────────────────────────────┘

 ┌───────────────────────────────────────────────────────────────────────────────────────┐
 │  DATABRICKS TECH STACK                                                                │
 │                                                                                       │
 │  Lakeflow ─────── Spark Declarative Pipeline (Bronze → Silver → Gold)                 │
 │  Unity Catalog ── 22 governed tables, CDF, lineage, fine-grained access               │
 │  Lakebase ─────── Postgres operational serving (4 synced Gold tables)                  │
 │  ML / Gen AI ──── Predictive Maintenance + Demand Forecasting (MLflow)                 │
 │  Genie Agents ─── 8 natural-language agents across Horizon 2 & 3                      │
 │  Dashboard ────── 5-page AI/BI executive dashboard (24 widgets)                       │
 │  Databricks App ─ Grid Operations Portal (Lakebase-connected)                         │
 └───────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Project Structure

```
/Workspace/Users/saifur.rahman@databricks.com/FE Bar/
├── README.md                          ← This file
├── 01_Bronze_Ingestion.py             ← Raw data ingestion from 20 DSOs
├── 02_Silver_Transformations.py       ← Clean, unify, enrich (canonical asset model)
├── 03_Gold_Aggregations.py            ← Business KPIs, health scores, reliability metrics
├── 04_Predictive_Maintenance_Model.py ← GBM failure probability model (MLflow)
└── 05_Demand_Forecasting_Model.py     ← GBM demand forecasting model (MLflow)

/Workspace/Users/saifur.rahman@databricks.com/multivar-energy-grid-ops/
├── main.py                            ← FastAPI + React 18 (prod)
├── app.yaml                           ← App config (uvicorn, env vars)
├── requirements.txt                   ← Dependencies
└── app.py                             ← Legacy Gradio version (unused)
```

## Data Architecture

### Catalog & Schema
- **Catalog**: `saifur_fevm_aws_workspace_catalog`
- **Schema**: `multivar_energy`

### Bronze Layer (7 tables — Raw Ingestion)
| Table | Rows | Source System |
|-------|------|---------------|
| `bronze_asset_registry` | 9,828 | SCADA, ERP, CMMS |
| `bronze_sensor_telemetry` | 203K | SCADA RTU/IED/PMU |
| `bronze_smart_meter_readings` | 273K | AMI v2/v3/legacy |
| `bronze_weather_data` | 14K | ECMWF, DWD, MeteoFrance, KNMI |
| `bronze_work_orders` | 5K | SAP PM, CMMS Maximo/Infor |
| `bronze_outage_events` | 2K | OMS v3/v4 |
| `bronze_der_telemetry` | 215K | DERMS v2/v3 |

### Silver Layer (7 tables — Cleaned & Unified)
| Table | Key Transformation |
|-------|--------------------|
| `silver_unified_assets` | Canonical asset ID across all 20 DSOs, criticality tiers |
| `silver_sensor_telemetry` | BAD quality filtered, anomaly flags per sensor type |
| `silver_smart_meter_readings` | Invalid readings filtered, country enrichment |
| `silver_weather_enriched` | Severity classification (SEVERE/HIGH/MODERATE/NORMAL) |
| `silver_work_orders` | Priority normalized, actual duration computed |
| `silver_outage_events` | Severity classification (MAJOR/SIGNIFICANT/MODERATE/MINOR) |
| `silver_der_performance` | Capacity factor, net power, operational status |

### Gold Layer (8 tables — Business-Ready)
| Table | Business Purpose |
|-------|------------------|
| `gold_asset_health_scores` | Composite 0-100 health score per asset |
| `gold_grid_reliability_metrics` | Monthly SAIDI/SAIFI per DSO |
| `gold_demand_forecast_features` | ML features: weather, calendar, meter data |
| `gold_maintenance_priority` | Risk-prioritized queue (prob × consequence × difficulty) |
| `gold_der_optimization` | DER generation, curtailment, capacity factors |
| `gold_executive_kpis` | Country-level KPIs for CIO dashboard |
| `gold_ml_failure_predictions` | ML model predictions + priority scores |
| `gold_demand_forecast_results` | Actual vs forecasted consumption |

## ML Models (Horizon 2)

### Predictive Maintenance
- **Algorithm**: Gradient Boosting Regressor (200 trees, depth 5)
- **Target**: Asset failure probability (0–1)
- **Performance**: MAE 0.041, R² 0.637
- **Top Features**: asset_age_years, max_temperature, emergency_repairs
- **Priority Score**: failure_prob × consequence × restoration_difficulty × 100

### Demand Forecasting
- **Algorithm**: Gradient Boosting Regressor (300 trees, depth 6)
- **Target**: Total consumption kWh per DSO/customer_type/day
- **Performance**: MAE 101.91 kWh, R² 0.999, MAPE 3.77%
- **Top Features**: customer_type, active_meters, power_factor

## Genie Agents (Horizon 2 & 3)

| Agent | Horizon | Purpose |
|-------|---------|--------|
| Grid Operations Intelligence | H2 | NL queries for grid operators and planners |
| Executive Energy Dashboard | H2 | NL queries for CIOs and executives |
| Asset Health Agent | H3 | Monitor telemetry, predict failures, generate work orders |
| Outage Response Agent | H3 | Correlate OMS/weather/crew, propose restoration |
| DER Orchestration Agent | H3 | Optimize batteries, solar, EV, demand response |
| Grid Planning Agent | H3 | Interconnection studies, capacity planning |
| Regulatory Compliance Agent | H3 | Track regulatory commitments, generate filings |
| Cyber-Physical Security Agent | H3 | Detect IT/OT anomalies, recommend containment |

## Operational Serving (Lakebase)

- **Project**: `multivar-energy-grid` (Lakebase Postgres, PG 17)
- **Branch**: `production`
- **Endpoint**: `ep-calm-fire-d18ke6tc.database.us-west-2.cloud.databricks.com`
- **Synced Tables** (Snapshot mode):
  - `gold_asset_health_scores`
  - `gold_maintenance_priority`
  - `gold_executive_kpis`
  - `gold_ml_failure_predictions`

## AI/BI Dashboard

- **Name**: Multivar Energy Grid Intelligence Dashboard
- **Pages**: Executive Overview | Asset Health & Maintenance | Grid Reliability | Renewable Energy & DER | ML Predictions
- **Widgets**: 24 (counters, bar charts, line charts, pie charts, scatter plots, tables)
- **Datasets**: 10 SQL datasets querying all Gold tables + ML predictions + filtered views
- **Theme**: Dark professional with green/blue energy palette (Inter font, emerald accents)

## Databricks App — Grid Operations Portal

- **Name**: `multivar-energy-grid-ops`
- **URL**: `https://multivar-energy-grid-ops-7474651427662410.aws.databricksapps.com`
- **Framework**: FastAPI backend + React 18 SPA (Tailwind CSS dark mode)
- **Data Layer**: Databricks SQL Statement Execution API via `databricks-sdk`
- **SQL Warehouse**: `a31d61d12430e28f` (configured as app resource)

### App Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│  DATABRICKS APP — multivar-energy-grid-ops                     │
│                                                                 │
│  ┌──────────────┐     ┌──────────────────────────────────────┐  │
│  │  FastAPI      │     │  React 18 SPA (Tailwind Dark Mode)  │  │
│  │  Backend      │◄───►│                                      │  │
│  │               │     │  5 Tab Pages:                        │  │
│  │  /api/        │     │    📊 Executive Overview (KPI cards) │  │
│  │  executive    │     │    🏥 Asset Health (risk filters)    │  │
│  │  health       │     │    🔧 Maintenance Queue (work orders)│  │
│  │  maintenance  │     │    🤖 ML Predictions (tier filters)  │  │
│  │  predictions  │     │    🌍 Grid Map (ESRI ArcGIS)         │  │
│  │  map-assets   │     │                                      │  │
│  │  asset-detail │     │  Floating Components:                │  │
│  │  financial-   │     │    💬 AI Assistant (Genie-powered)   │  │
│  │    impact     │     │    📋 Asset Detail Modal             │  │
│  │  genie/ask    │     │    📝 Work Order Modal               │  │
│  └──────┬───────┘     └──────────────────────────────────────┘  │
│         │                                                       │
│         ▼                                                       │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Databricks SQL Statement Execution API                  │   │
│  │  Warehouse: a31d61d12430e28f                              │   │
│  │  Catalog: saifur_fevm_aws_workspace_catalog               │   │
│  │  Schema: multivar_energy (8 Gold tables)                  │   │
│  └──────────────────────────────────────────────────────────┘   │
│         │                                                       │
│         ▼                                                       │
│  ┌──────────────────────────────────────────────────────────┐   │
│  │  Genie API (3 Agents)                                     │   │
│  │  Grid Ops • Executive Dashboard • Asset Health            │   │
│  │  NL queries → SQL generation → results                    │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

### Pages & Features

| Page | Key Features |
|------|-------------|
| Executive Overview | 4 KPI counters (assets, health, SAIDI, renewable MW), country table |
| Asset Health | Risk classification filter, clickable asset IDs → detail modal |
| Maintenance Queue | Priority-ranked top 50, **Create WO** action per row → Work Order modal |
| ML Predictions | Country + tier filters, ML risk labels, clickable asset drill-down |
| Grid Map (ESRI) | ArcGIS dark basemap, 2K color-coded markers (risk), popups, KPI counters |

### Interactive Modals

**Asset Detail Modal** — Click any `unified_asset_id` in Health, Maintenance, or Predictions tables:
- Full 30+ attribute grid (health score, risk, telemetry stats, maintenance cost, location, capacity)
- Risk classification and criticality tier badges

**Work Order Modal** — Click "Create WO" on any Maintenance Queue row:
- 4-step workflow stepper: Request Submitted → Work Order Created → Accepted → Field Ops Dispatched
- Financial impact panel (auto-calculated):
  - Estimated outage cost (base €75K × tier × voltage × risk_factor × capacity)
  - Revenue at risk (2.5× outage cost)
  - Regulatory penalty risk (0.3× outage cost)
  - Total estimated impact (3.8× outage cost)
  - Urgency classification: IMMEDIATE (risk > 0.7) / URGENT (> 0.4) / SCHEDULED
- Work order form: Work Type, Assigned Team, Notes

### Floating AI Assistant (All Tabs)

- Accessible via floating green button (bottom-right corner) on every page
- Powered by **Databricks Genie** with 3 selectable agents:
  - `grid-ops` — Grid Operations Intelligence Agent
  - `executive` — Executive Energy Dashboard Agent
  - `asset-health` — Asset Health Agent
- **20 context-aware suggested questions** (4 per tab, dynamically switch with navigation)
- Shows generated SQL alongside natural-language responses
- Conversation continuity for follow-up questions
- **"+ New Chat"** button to reset conversation

### App Resources

| Resource | Type | Permission |
|----------|------|------------|
| SQL Warehouse `a31d61d12430e28f` | SQL Warehouse | CAN_USE |
| Grid Operations Intelligence Agent | Genie Space | CAN_MANAGE |
| Executive Energy Dashboard Agent | Genie Space | CAN_MANAGE |
| Asset Health Agent | Genie Space | CAN_MANAGE |
| DER Orchestration Agent | Genie Space | CAN_MANAGE |
| Cyber-Physical Security Agent | Genie Space | CAN_MANAGE |
| Outage Response Agent | Genie Space | CAN_MANAGE |
| Grid Planning Agent | Genie Space | CAN_MANAGE |
| Regulatory Compliance Agent | Genie Space | CAN_MANAGE |

### Source Files

```
/Workspace/Users/saifur.rahman@databricks.com/multivar-energy-grid-ops/
├── main.py            ← FastAPI + inline React 18 SPA
├── app.yaml           ← uvicorn command + SQL_WAREHOUSE_ID env var
├── requirements.txt   ← fastapi, uvicorn, databricks-sdk
└── app.py             ← (legacy Gradio version, unused)
```

## Key Success Metrics

| Dimension | Metric |
|-----------|--------|
| Grid Resilience | SAIDI/SAIFI improvement, MTTR reduction |
| Operational Efficiency | 30-40% unplanned downtime reduction |
| Data Maturity | % assets with unified digital identity |
| Governance | Human-approval compliance rate for agent actions |

---

## Complete Tech Stack

| # | Component | Databricks Product | Status |
|---|-----------|-------------------|--------|
| 1 | Data Pipeline | Lakeflow SDP (Bronze → Silver → Gold) | ✅ 22 tables |
| 2 | Governance | Unity Catalog (CDF, lineage, fine-grained access) | ✅ |
| 3 | Operational DB | Lakebase Postgres (4 synced Gold tables) | ✅ |
| 4 | ML Models | MLflow (Predictive Maintenance + Demand Forecasting) | ✅ |
| 5 | NL Agents | Genie Spaces (8 agents, Horizon 2 + 3) | ✅ |
| 6 | Executive BI | AI/BI Dashboard (5 pages, 24 widgets, dark theme) | ✅ |
| 7 | Operations App | Databricks App (React + FastAPI, ESRI map, AI chat, work orders) | ✅ |

---

*Built on the Databricks Lakehouse Platform — Lakeflow + Unity Catalog + Lakebase + MLflow + Genie Agents + AI/BI Dashboard + Databricks Apps*
