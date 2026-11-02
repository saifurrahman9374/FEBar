"""Multivar Energy — Grid Operations Portal
Databricks App connected to Lakebase Postgres for low-latency operational serving.
"""
import os
import uuid
import time
import gradio as gr
import pandas as pd
import psycopg2
from databricks.sdk import WorkspaceClient

# ── Lakebase Connection ──────────────────────────────────────────────

w = WorkspaceClient()
_password_cache = {"token": None, "ts": 0}


def get_connection():
    """Connect to Lakebase with auto-refreshing OAuth token (15-min cycle)."""
    now = time.time()
    if _password_cache["token"] is None or now - _password_cache["ts"] > 900:
        cred = w.database.generate_database_credential(
            request_id=str(uuid.uuid4()),
            instance_names=[os.environ.get("PGDATABASE", "databricks_postgres")],
        )
        _password_cache["token"] = cred.token
        _password_cache["ts"] = now

    return psycopg2.connect(
        host=os.environ.get("PGHOST", "localhost"),
        database=os.environ.get("PGDATABASE", "databricks_postgres"),
        user=os.environ.get("PGUSER", "user"),
        port=int(os.environ.get("PGPORT", 5432)),
        password=_password_cache["token"],
        sslmode="require",
    )


def run_query(sql: str) -> pd.DataFrame:
    """Execute a read-only query and return a DataFrame."""
    try:
        conn = get_connection()
        df = pd.read_sql_query(sql, conn)
        conn.close()
        return df
    except Exception as e:
        return pd.DataFrame({"error": [str(e)]})


# ── Schema (synced tables live under a schema named after the sync) ──
SCHEMA = "multivar_energy"


# ── Tab 1: Executive Overview ────────────────────────────────────────

def load_executive_kpis():
    df = run_query(f"SELECT * FROM {SCHEMA}.gold_executive_kpis ORDER BY total_assets DESC")
    if "error" in df.columns:
        return "Connection error — see logs", "", "", "", df
    total = int(df["total_assets"].sum()) if "total_assets" in df.columns else 0
    health = round(df["avg_health_score"].mean(), 1) if "avg_health_score" in df.columns else 0
    saidi = round(df["avg_saidi_minutes"].mean(), 1) if "avg_saidi_minutes" in df.columns else 0
    renewable = int(df["total_renewable_generation_mw"].sum()) if "total_renewable_generation_mw" in df.columns else 0
    return (
        f"{total:,}",
        f"{health}",
        f"{saidi}",
        f"{renewable:,} MW",
        df,
    )


# ── Tab 2: Asset Health Monitor ──────────────────────────────────────

def load_asset_health(risk_filter="All"):
    where = ""
    if risk_filter and risk_filter != "All":
        where = f"WHERE risk_classification = '{risk_filter}'"
    df = run_query(
        f"SELECT unified_asset_id, country, region, asset_category, voltage_level, "
        f"criticality_tier, ROUND(asset_age_years::numeric, 1) AS age_years, "
        f"ROUND(health_score::numeric, 1) AS health_score, risk_classification "
        f"FROM {SCHEMA}.gold_asset_health_scores {where} "
        f"ORDER BY health_score ASC LIMIT 200"
    )
    return df


# ── Tab 3: Maintenance Queue ─────────────────────────────────────────

def load_maintenance_queue():
    df = run_query(
        f"SELECT unified_asset_id, country, region, asset_category, voltage_level, "
        f"criticality_tier, ROUND(priority_score::numeric, 2) AS priority_score, "
        f"ROUND(failure_probability::numeric, 4) AS failure_prob, "
        f"ROUND(asset_age_years::numeric, 1) AS age_years "
        f"FROM {SCHEMA}.gold_maintenance_priority "
        f"ORDER BY priority_score DESC LIMIT 50"
    )
    return df


# ── Tab 4: ML Predictions ────────────────────────────────────────────

def load_ml_predictions(country="All", tier="All"):
    conditions = []
    if country and country != "All":
        conditions.append(f"country = '{country}'")
    if tier and tier != "All":
        conditions.append(f"criticality_tier = '{tier}'")
    where = "WHERE " + " AND ".join(conditions) if conditions else ""
    df = run_query(
        f"SELECT unified_asset_id, country, asset_category, voltage_level, "
        f"criticality_tier, ROUND(asset_age_years::numeric, 1) AS age_years, "
        f"ROUND(health_score::numeric, 1) AS health_score, "
        f"ROUND(ml_failure_probability::numeric, 4) AS failure_prob, "
        f"ROUND(ml_priority_score::numeric, 2) AS priority_score "
        f"FROM {SCHEMA}.gold_ml_failure_predictions {where} "
        f"ORDER BY ml_priority_score DESC LIMIT 100"
    )
    return df


# ── Gradio App ───────────────────────────────────────────────────────

CSS = """
.metric-card {background: #161B22; border: 1px solid #30363D; border-radius: 12px; padding: 20px; text-align: center;}
.metric-value {font-size: 2.2em; font-weight: 700; color: #10B981;}
.metric-label {font-size: 0.9em; color: #8B949E; margin-top: 4px;}
footer {display: none !important;}
"""

with gr.Blocks(
    theme=gr.themes.Base(
        primary_hue=gr.themes.colors.emerald,
        secondary_hue=gr.themes.colors.blue,
        neutral_hue=gr.themes.colors.gray,
        font=gr.themes.GoogleFont("Inter"),
    ),
    css=CSS,
    title="Multivar Energy — Grid Ops",
) as app:
    gr.Markdown(
        "# \u26a1 Multivar Energy — Grid Operations Portal\n"
        "Real-time operational dashboard powered by **Lakebase Postgres** | "
        "20 DSOs | 14 Countries"
    )

    # ── Tab 1 ──
    with gr.Tab("\U0001f4ca Executive Overview"):
        refresh_btn = gr.Button("\U0001f504 Refresh KPIs", variant="primary")
        with gr.Row():
            m_assets = gr.Textbox(label="Total Grid Assets", interactive=False)
            m_health = gr.Textbox(label="Avg Health Score", interactive=False)
            m_saidi = gr.Textbox(label="Avg SAIDI (min)", interactive=False)
            m_renew = gr.Textbox(label="Renewable Gen", interactive=False)
        kpi_table = gr.Dataframe(label="Executive KPIs by Country", interactive=False)
        refresh_btn.click(
            load_executive_kpis, outputs=[m_assets, m_health, m_saidi, m_renew, kpi_table]
        )
        app.load(
            load_executive_kpis, outputs=[m_assets, m_health, m_saidi, m_renew, kpi_table]
        )

    # ── Tab 2 ──
    with gr.Tab("\U0001f3e5 Asset Health Monitor"):
        risk_dd = gr.Dropdown(
            choices=["All", "CRITICAL", "HIGH_RISK", "MODERATE", "HEALTHY"],
            value="All",
            label="Risk Classification Filter",
        )
        health_table = gr.Dataframe(label="Asset Health (lowest scores first)", interactive=False)
        risk_dd.change(load_asset_health, inputs=risk_dd, outputs=health_table)
        app.load(load_asset_health, outputs=health_table)

    # ── Tab 3 ──
    with gr.Tab("\U0001f527 Maintenance Queue"):
        maint_btn = gr.Button("\U0001f504 Refresh Queue", variant="primary")
        maint_table = gr.Dataframe(label="Priority Maintenance Queue (Top 50)", interactive=False)
        maint_btn.click(load_maintenance_queue, outputs=maint_table)
        app.load(load_maintenance_queue, outputs=maint_table)

    # ── Tab 4 ──
    with gr.Tab("\U0001f916 ML Predictions"):
        with gr.Row():
            ml_country = gr.Dropdown(
                choices=["All", "Germany", "France", "Netherlands", "Spain",
                         "Italy", "Belgium", "Austria", "Denmark", "Poland",
                         "Sweden", "Norway", "Switzerland", "Portugal", "Ireland"],
                value="All", label="Country",
            )
            ml_tier = gr.Dropdown(
                choices=["All", "TIER_1_CRITICAL", "TIER_2_HIGH",
                         "TIER_3_MEDIUM", "TIER_4_STANDARD"],
                value="All", label="Criticality Tier",
            )
        ml_table = gr.Dataframe(label="ML Failure Predictions (Top 100)", interactive=False)
        ml_country.change(load_ml_predictions, inputs=[ml_country, ml_tier], outputs=ml_table)
        ml_tier.change(load_ml_predictions, inputs=[ml_country, ml_tier], outputs=ml_table)
        app.load(load_ml_predictions, outputs=ml_table)

    gr.Markdown(
        "---\n*Powered by Databricks Lakehouse Platform — "
        "Lakeflow \u00b7 Unity Catalog \u00b7 Lakebase \u00b7 MLflow \u00b7 Genie Agents*"
    )

if __name__ == "__main__":
    app.launch()
