# Silver: curated OMS outage events with key enforcement
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "saifur_fevm_aws_workspace_catalog.multivar_energy"


@dp.table(
    name=f"{TARGET}.sdp_silver_outage_events",
    comment="Curated outage events with required keys and typed start/end timestamps.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("outage_id_required", "outage_id IS NOT NULL")
@dp.expect_or_fail("outage_dso_required", "dso_id IS NOT NULL")
@dp.expect("outage_start_timestamp_valid", "start_ts IS NOT NULL")
def sdp_silver_outage_events():
    return (
        spark.readStream.table("saifur_fevm_aws_workspace_catalog.multivar_energy.sdp_bronze_outage_events")
        .withColumn("start_ts", F.to_timestamp("start_timestamp"))
        .withColumn("end_ts", F.to_timestamp("end_timestamp"))
    )
