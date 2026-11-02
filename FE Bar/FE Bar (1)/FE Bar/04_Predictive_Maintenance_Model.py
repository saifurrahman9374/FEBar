# Databricks notebook source
# DBTITLE 1,Predictive Maintenance Model — Horizon 2
# Multivar Energy — Predictive Maintenance Model
# Horizon 2: AI-Powered Decision Support
#
# Combines sensor anomaly detection, failure history, weather exposure, and asset age
# Priority scoring = failure_probability × consequence × restoration_difficulty × customer_criticality

import mlflow, mlflow.sklearn
from sklearn.ensemble import GradientBoostingRegressor, GradientBoostingClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score, roc_auc_score, classification_report
from sklearn.preprocessing import LabelEncoder
import pandas as pd, numpy as np

CATALOG = "saifur_fevm_aws_workspace_catalog"
SCHEMA = "multivar_energy"

df = spark.table(f"{CATALOG}.{SCHEMA}.gold_asset_health_scores").toPandas()
for c in ['asset_age_years','anomaly_rate_pct']: df[c] = pd.to_numeric(df[c], errors='coerce').fillna(0)

# Simulated failure risk target
np.random.seed(42)
df['failure_risk'] = np.clip(0.05 + (df['asset_age_years']/50)*0.25 + (df['anomaly_rate_pct']/10)*0.20 + np.where(df['avg_load_pct'].notna(),(df['avg_load_pct']-60)/100*0.15,0) + np.where(df['emergency_repairs']>0, df['emergency_repairs']*0.08, 0) + np.random.normal(0,0.05,len(df)), 0, 1)

le = {c: LabelEncoder() for c in ['asset_category','voltage_level','criticality_tier','country']}
for c, enc in le.items(): df[f'{c}_enc'] = enc.fit_transform(df[c].fillna('unknown'))

features = ['asset_age_years','anomaly_rate_pct','total_readings','anomaly_count','avg_temperature_c','max_temperature_c','avg_load_pct','peak_load_pct','avg_vibration_mm_s','avg_oil_level_pct','avg_dissolved_gas_ppm','total_work_orders','critical_work_orders','emergency_repairs','total_maintenance_cost_eur','rated_capacity_mw','asset_category_enc','voltage_level_enc','criticality_tier_enc','country_enc']
X = df[features].fillna(0); y = df['failure_risk']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

mlflow.set_experiment(f"/Users/saifur.rahman@databricks.com/FE Bar/Multivar_Predictive_Maintenance")
with mlflow.start_run(run_name="Failure_Probability_GBR"):
    model = GradientBoostingRegressor(n_estimators=200, max_depth=5, learning_rate=0.1, subsample=0.8, random_state=42)
    model.fit(X_train, y_train)
    pred = model.predict(X_test)
    mae, r2 = mean_absolute_error(y_test, pred), r2_score(y_test, pred)
    mlflow.log_metrics({"mae": mae, "r2": r2})
    mlflow.sklearn.log_model(model, "predictive_maintenance_model")
    print(f"MAE: {mae:.4f}, R\u00b2: {r2:.4f}")

# Save predictions
df['ml_failure_probability'] = np.round(model.predict(X), 4)
df['ml_priority_score'] = (df['ml_failure_probability'] * df['criticality_tier'].map({'TIER_1_CRITICAL':1,'TIER_2_HIGH':0.75,'TIER_3_MEDIUM':0.5,'TIER_4_STANDARD':0.25}).fillna(0.25) * df['voltage_level'].map({'EHV':0.95,'HV':0.85,'MV':0.6,'LV':0.3}).fillna(0.5) * 100).round(2)
cols = ['unified_asset_id','dso_id','country','region','asset_category','voltage_level','criticality_tier','asset_age_years','health_score','ml_failure_probability','ml_priority_score','latitude','longitude']
spark.createDataFrame(df[cols]).write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_ml_failure_predictions")
print(f"Saved {len(df)} predictions")