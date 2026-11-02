# Silver: curated AMI smart meter readings with quality controls
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "saifur_fevm_aws_workspace_catalog.multivar_energy"


@dp.table(
    name=f"{TARGET}.sdp_silver_smart_meter_readings",
    comment="Curated smart meter interval readings with typed timestamps and range checks.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("meter_dso_required", "dso_id IS NOT NULL")
@dp.expect_or_drop("meter_non_negative_consumption", "consumption_kwh >= 0")
@dp.expect_or_drop("meter_power_factor_valid", "power_factor BETWEEN 0 AND 1")
@dp.expect("meter_timestamp_valid", "reading_ts IS NOT NULL")
def sdp_silver_smart_meter_readings():
    return (
        spark.readStream.table("saifur_fevm_aws_workspace_catalog.multivar_energy.sdp_bronze_smart_meter_readings")
        .withColumn("reading_ts", F.to_timestamp("reading_timestamp"))
    )
