# Gold: daily DER summary by DSO and DER type
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "saifur_fevm_aws_workspace_catalog.multivar_energy"


@dp.materialized_view(
    name=f"{TARGET}.sdp_gold_der_daily_summary",
    comment="Daily DER fleet summary by DSO and DER type from curated DER telemetry.",
    table_properties={"quality": "gold"},
)
def sdp_gold_der_daily_summary():
    der = spark.read.table("saifur_fevm_aws_workspace_catalog.multivar_energy.sdp_silver_der_telemetry")
    return (
        der.withColumn("reading_date", F.to_date("reading_ts"))
        .groupBy("dso_id", "der_type", "reading_date")
        .agg(
            F.countDistinct("der_id").alias("active_der_units"),
            F.sum(F.coalesce(F.col("generation_mw"), F.lit(0.0))).alias("total_generation_mw"),
            F.sum(F.coalesce(F.col("consumption_mw"), F.lit(0.0))).alias("total_consumption_mw"),
            F.sum(F.coalesce(F.col("generation_mw"), F.lit(0.0)) - F.coalesce(F.col("consumption_mw"), F.lit(0.0))).alias("net_power_mw"),
            F.avg(F.when(F.col("rated_capacity_mw") > 0, F.col("generation_mw") / F.col("rated_capacity_mw"))).alias("avg_capacity_factor"),
            F.sum(F.coalesce(F.col("rated_capacity_mw"), F.lit(0.0))).alias("total_installed_capacity_mw"),
            F.avg("state_of_charge_pct").alias("avg_state_of_charge_pct"),
            F.sum(F.when(F.col("status") == "online", 1).otherwise(0)).alias("online_records"),
            F.sum(F.when(F.col("status") == "curtailed", 1).otherwise(0)).alias("curtailed_records"),
            F.sum(F.when(F.col("status") == "offline", 1).otherwise(0)).alias("offline_records"),
            F.current_timestamp().alias("computed_at"),
        )
    )
