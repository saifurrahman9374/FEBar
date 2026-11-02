# Databricks notebook source
# DBTITLE 1,Multivar Energy — Bronze Layer Ingestion
# MAGIC %md
# MAGIC # 🏭 Multivar Energy — Bronze Layer Ingestion
# MAGIC ## Horizon 1: Intelligent Data Foundation
# MAGIC
# MAGIC This notebook ingests raw data from multiple source systems across 20+ DSOs into Bronze Delta tables.
# MAGIC
# MAGIC **Source Systems:** SCADA, AMI, IoT sensors, SAP/ERP, CMMS, GIS, Weather APIs, DERMS, OMS
# MAGIC
# MAGIC **Tables Created:**
# MAGIC - `bronze_asset_registry` — Grid asset inventory from all DSOs
# MAGIC - `bronze_sensor_telemetry` — SCADA/IoT real-time sensor readings
# MAGIC - `bronze_smart_meter_readings` — AMI smart meter consumption data
# MAGIC - `bronze_weather_data` — Multi-provider weather observations
# MAGIC - `bronze_work_orders` — Maintenance/repair work orders
# MAGIC - `bronze_outage_events` — OMS outage and restoration data
# MAGIC - `bronze_der_telemetry` — Distributed energy resource performance

# COMMAND ----------

# DBTITLE 1,Configuration
CATALOG = "saifur_fevm_aws_workspace_catalog"
SCHEMA = "multivar_energy"
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
print(f"Target: {CATALOG}.{SCHEMA}")

# COMMAND ----------

# DBTITLE 1,Bronze: Asset Registry (9,800+ assets across 20 DSOs)
from pyspark.sql.types import *
import random

dso_locations = [
    ("DSO_DE_01","Germany North","Hamburg",53.55,9.99), ("DSO_DE_02","Germany South","Munich",48.14,11.58),
    ("DSO_DE_03","Germany West","Cologne",50.94,6.96), ("DSO_FR_01","France North","Paris",48.86,2.35),
    ("DSO_FR_02","France South","Lyon",45.76,4.84), ("DSO_NL_01","Netherlands West","Amsterdam",52.37,4.90),
    ("DSO_NL_02","Netherlands South","Rotterdam",51.92,4.48), ("DSO_BE_01","Belgium Central","Brussels",50.85,4.35),
    ("DSO_AT_01","Austria East","Vienna",48.21,16.37), ("DSO_DK_01","Denmark East","Copenhagen",55.68,12.57),
    ("DSO_ES_01","Spain Central","Madrid",40.42,-3.70), ("DSO_ES_02","Spain East","Barcelona",41.39,2.17),
    ("DSO_IT_01","Italy North","Milan",45.46,9.19), ("DSO_IT_02","Italy Central","Rome",41.90,12.50),
    ("DSO_PL_01","Poland Central","Warsaw",52.23,21.01), ("DSO_SE_01","Sweden South","Gothenburg",57.71,11.97),
    ("DSO_NO_01","Norway South","Oslo",59.91,10.75), ("DSO_CH_01","Switzerland North","Zurich",47.38,8.54),
    ("DSO_PT_01","Portugal Central","Lisbon",38.72,-9.14), ("DSO_IE_01","Ireland East","Dublin",53.35,-6.26)
]
asset_types = ["transformer","substation","solar_panel","wind_turbine","battery_storage","feeder","cable","breaker","capacitor_bank","recloser"]
manufacturers = ["Siemens","ABB","Schneider Electric","GE Grid","Hitachi Energy","Eaton","Mitsubishi","Toshiba","Vestas","SolarEdge"]

asset_rows = []
asset_id = 100000
for dso_id, region, city, lat, lon in dso_locations:
    for i in range(random.randint(400, 600)):
        asset_id += 1
        at = random.choice(asset_types)
        asset_rows.append((f"AST-{asset_id}", dso_id, region, at, f"{at.upper()}_{random.randint(1,999):03d}_{dso_id}",
            random.choice(manufacturers), f"Model-{random.choice(['X','Y','Z'])}{random.randint(100,999)}",
            random.choice(["LV","MV","HV","EHV"]), round(random.uniform(0.1,500),2),
            f"{random.randint(1985,2023)}-{random.randint(1,12):02d}-{random.randint(1,28):02d}",
            round(lat+random.uniform(-0.5,0.5),6), round(lon+random.uniform(-0.5,0.5),6),
            random.choice(["operational"]*4+["degraded","maintenance","decommissioned"]),
            f"{random.randint(1,50):02d}", f"SUB-{dso_id}-{random.randint(1,20):02d}",
            f"2026-09-{random.randint(1,27):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z", dso_id))

df = spark.createDataFrame(asset_rows, "raw_asset_id STRING, dso_id STRING, region STRING, asset_type STRING, local_asset_name STRING, manufacturer STRING, model STRING, voltage_level STRING, rated_capacity_mw DOUBLE, install_date_str STRING, latitude DOUBLE, longitude DOUBLE, status STRING, feeder_id STRING, substation_ref STRING, ingestion_timestamp STRING, source_system STRING")
df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_asset_registry")
print(f"bronze_asset_registry: {df.count()} rows")

# COMMAND ----------

# DBTITLE 1,Bronze: Sensor Telemetry (200K+ SCADA/IoT readings)
from datetime import datetime, timedelta

asset_ids = [r.raw_asset_id for r in spark.table(f"{CATALOG}.{SCHEMA}.bronze_asset_registry").select("raw_asset_id").limit(2000).collect()]
sensor_types = ["temperature_c","load_pct","voltage_kv","current_a","oil_level_pct","vibration_mm_s","humidity_pct","power_factor","frequency_hz","dissolved_gas_ppm"]
base = datetime(2026,7,1)
rows = []
for aid in random.sample(asset_ids, min(800, len(asset_ids))):
    sensors = random.sample(sensor_types, random.randint(3,6))
    for d in range(0,90,random.randint(1,3)):
        ts = base + timedelta(days=d, hours=random.randint(0,23), minutes=random.randint(0,59))
        for s in sensors:
            val = round(random.gauss({"temperature_c":55,"load_pct":65,"voltage_kv":110,"current_a":250,"oil_level_pct":85,"vibration_mm_s":2.5,"humidity_pct":45,"power_factor":0.95,"frequency_hz":50,"dissolved_gas_ppm":50}[s], {"temperature_c":15,"load_pct":20,"voltage_kv":5,"current_a":80,"oil_level_pct":10,"vibration_mm_s":1.5,"humidity_pct":15,"power_factor":0.03,"frequency_hz":0.1,"dissolved_gas_ppm":30}[s]), 3)
            rows.append((aid, s, val, random.choices(["GOOD","SUSPECT","BAD"], weights=[95,4,1])[0], ts.strftime("%Y-%m-%dT%H:%M:%SZ"), f"SCADA_{random.choice(['RTU','IED','PMU'])}", random.choice([d[0] for d in dso_locations])))

df = spark.createDataFrame(rows, "asset_id STRING, sensor_type STRING, reading_value DOUBLE, quality_flag STRING, reading_timestamp STRING, source_device STRING, source_dso STRING")
df.write.mode("overwrite").saveAsTable(f"{CATALOG}.{SCHEMA}.bronze_sensor_telemetry")
print(f"bronze_sensor_telemetry: {df.count()} rows")