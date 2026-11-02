# Databricks notebook source
# DBTITLE 1,Demand Forecasting Model — Horizon 2
# Multivar Energy — Demand Forecasting Model
# Horizon 2: AI-Powered Decision Support
#
# Forecasts load at DSO/customer-type level using weather, calendar, and meter features
# MAPE: ~3.8% | R²: 0.999

import mlflow, mlflow.sklearn
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_absolute_error, r2_score, mean_squared_error
from sklearn.preprocessing import LabelEncoder
import pandas as pd, numpy as np

CATALOG = "saifur_fevm_aws_workspace_catalog"
SCHEMA = "multivar_energy"

df = spark.table(f"{CATALOG}.{SCHEMA}.gold_demand_forecast_features").toPandas()
for c in ['dso_id','country','customer_type']:
    df[f'{c}_enc'] = LabelEncoder().fit_transform(df[c].fillna('unknown'))
df['is_weekend'] = df['is_weekend'].astype(int)

features = ['day_of_week','is_weekend','month','active_meters','avg_temp_c','max_temp_c','min_temp_c','avg_humidity','avg_wind_speed','total_precipitation_mm','avg_solar_irradiance','heating_degree_days','cooling_degree_days','avg_voltage','avg_power_factor','dso_id_enc','country_enc','customer_type_enc']
X = df[features].fillna(0); y = df['total_consumption_kwh']
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)

mlflow.set_experiment(f"/Users/saifur.rahman@databricks.com/FE Bar/Multivar_Demand_Forecasting")
with mlflow.start_run(run_name="Demand_GBR"):
    model = GradientBoostingRegressor(n_estimators=300, max_depth=6, learning_rate=0.08, subsample=0.85, random_state=42)
    model.fit(X_train, y_train)
    pred = model.predict(X_test)
    mae = mean_absolute_error(y_test, pred)
    r2 = r2_score(y_test, pred)
    mape = np.mean(np.abs((y_test-pred)/np.where(y_test==0,1,y_test)))*100
    mlflow.log_metrics({"mae":mae, "r2":r2, "mape":mape})
    mlflow.sklearn.log_model(model, "demand_forecast_model")
    print(f"MAE: {mae:.2f} kWh | R\u00b2: {r2:.4f} | MAPE: {mape:.2f}%")

df['forecasted_kwh'] = model.predict(X)
df['error_kwh'] = df['total_consumption_kwh'] - df['forecasted_kwh']
cols = ['dso_id','country','customer_type','reading_date','day_of_week','is_weekend','active_meters','total_consumption_kwh','forecasted_kwh','error_kwh','avg_temp_c','heating_degree_days','cooling_degree_days']
spark.createDataFrame(df[cols].astype({'is_weekend':'bool'})).write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.gold_demand_forecast_results")
print(f"Saved {len(df)} forecasts")