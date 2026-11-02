# Silver: curated sensor telemetry with quality filtering
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "saifur_fevm_aws_workspace_catalog.multivar_energy"


@dp.table(
    name=f"{TARGET}.sdp_silver_sensor_readings",
    comment="Curated sensor telemetry with valid timestamps and dropped BAD-quality records.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("sensor_asset_required", "asset_id IS NOT NULL")
@dp.expect_or_drop("sensor_quality_not_bad", "quality_flag <> 'BAD'")
@dp.expect("sensor_timestamp_valid", "reading_ts IS NOT NULL")
def sdp_silver_sensor_readings():
    return (
        spark.readStream.table("saifur_fevm_aws_workspace_catalog.multivar_energy.sdp_bronze_sensor_telemetry")
        .withColumn("reading_ts", F.to_timestamp("reading_timestamp"))
    )
