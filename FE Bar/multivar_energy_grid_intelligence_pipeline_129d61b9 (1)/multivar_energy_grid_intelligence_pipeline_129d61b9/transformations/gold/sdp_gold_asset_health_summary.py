# Gold: asset health summary by DSO and asset category
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "saifur_fevm_aws_workspace_catalog.multivar_energy"


@dp.materialized_view(
    name=f"{TARGET}.sdp_gold_asset_health_summary",
    comment="Asset health summary by DSO and asset category derived from curated asset, sensor, work order and outage data.",
    table_properties={"quality": "gold"},
)
def sdp_gold_asset_health_summary():
    assets = spark.read.table("saifur_fevm_aws_workspace_catalog.multivar_energy.sdp_silver_assets")
    sensors = spark.read.table("saifur_fevm_aws_workspace_catalog.multivar_energy.sdp_silver_sensor_readings")
    work_orders = spark.read.table("saifur_fevm_aws_workspace_catalog.multivar_energy.sdp_silver_work_orders")
    outages = spark.read.table("saifur_fevm_aws_workspace_catalog.multivar_energy.sdp_silver_outage_events")

    sensor_metrics = (
        sensors.groupBy("asset_id")
        .agg(
            F.count("*").alias("total_sensor_readings"),
            F.sum(F.when(F.col("quality_flag") == "SUSPECT", 1).otherwise(0)).alias("suspect_sensor_readings"),
            F.avg(F.when(F.col("sensor_type") == "load_pct", F.col("reading_value"))).alias("avg_load_pct"),
            F.avg(F.when(F.col("sensor_type") == "temperature_c", F.col("reading_value"))).alias("avg_temperature_c"),
            F.avg(F.when(F.col("sensor_type") == "vibration_mm_s", F.col("reading_value"))).alias("avg_vibration_mm_s"),
        )
    )

    work_order_metrics = (
        work_orders.groupBy("asset_id")
        .agg(
            F.count("*").alias("total_work_orders"),
            F.sum(F.when(F.col("priority") == "P1_CRITICAL", 1).otherwise(0)).alias("critical_work_orders"),
            F.sum(F.when(F.col("work_type") == "emergency_repair", 1).otherwise(0)).alias("emergency_repairs"),
        )
    )

    outage_metrics = (
        outages.groupBy("dso_id", "voltage_level")
        .agg(
            F.count("*").alias("total_outages"),
            F.avg("duration_minutes").alias("avg_outage_duration_minutes"),
            F.sum("customers_affected").alias("customers_affected_total"),
        )
    )

    return (
        assets.join(sensor_metrics, "asset_id", "left")
        .join(work_order_metrics, "asset_id", "left")
        .join(outage_metrics, ["dso_id", "voltage_level"], "left")
        .groupBy("dso_id", F.col("asset_type").alias("asset_category"))
        .agg(
            F.count("*").alias("asset_count"),
            F.avg("asset_age_years").alias("avg_asset_age_years"),
            F.sum(F.coalesce(F.col("total_sensor_readings"), F.lit(0))).alias("total_sensor_readings"),
            F.sum(F.coalesce(F.col("suspect_sensor_readings"), F.lit(0))).alias("suspect_sensor_readings"),
            F.avg(F.coalesce(F.col("avg_load_pct"), F.lit(0.0))).alias("avg_load_pct"),
            F.avg(F.coalesce(F.col("avg_temperature_c"), F.lit(0.0))).alias("avg_temperature_c"),
            F.avg(F.coalesce(F.col("avg_vibration_mm_s"), F.lit(0.0))).alias("avg_vibration_mm_s"),
            F.sum(F.coalesce(F.col("total_work_orders"), F.lit(0))).alias("total_work_orders"),
            F.sum(F.coalesce(F.col("critical_work_orders"), F.lit(0))).alias("critical_work_orders"),
            F.sum(F.coalesce(F.col("emergency_repairs"), F.lit(0))).alias("emergency_repairs"),
            F.sum(F.coalesce(F.col("total_outages"), F.lit(0))).alias("total_outages"),
            F.avg(F.coalesce(F.col("avg_outage_duration_minutes"), F.lit(0.0))).alias("avg_outage_duration_minutes"),
            F.sum(F.coalesce(F.col("customers_affected_total"), F.lit(0))).alias("customers_affected_total"),
            F.round(
                100
                - F.least(
                    F.lit(100.0),
                    F.avg(
                        F.coalesce(F.col("asset_age_years"), F.lit(0.0)) * F.lit(0.35)
                        + F.coalesce(F.col("avg_load_pct"), F.lit(0.0)) * F.lit(0.20)
                        + F.coalesce(F.col("avg_temperature_c"), F.lit(0.0)) * F.lit(0.15)
                        + F.coalesce(F.col("avg_vibration_mm_s"), F.lit(0.0)) * F.lit(5.0)
                        + F.coalesce(F.col("emergency_repairs"), F.lit(0.0)) * F.lit(8.0)
                    ),
                ),
                2,
            ).alias("health_score"),
            F.current_timestamp().alias("computed_at"),
        )
    )
