# Silver: curated DER telemetry with typed timestamps and SOC validation
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "saifur_fevm_aws_workspace_catalog.multivar_energy"


@dp.table(
    name=f"{TARGET}.sdp_silver_der_telemetry",
    comment="Curated DER telemetry with valid state of charge and typed timestamps.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("der_id_required", "der_id IS NOT NULL")
@dp.expect_or_fail("der_dso_required", "dso_id IS NOT NULL")
@dp.expect_or_drop("der_soc_valid_when_present", "state_of_charge_pct IS NULL OR state_of_charge_pct BETWEEN 0 AND 100")
@dp.expect("der_timestamp_valid", "reading_ts IS NOT NULL")
def sdp_silver_der_telemetry():
    return (
        spark.readStream.table("saifur_fevm_aws_workspace_catalog.multivar_energy.sdp_bronze_der_telemetry")
        .withColumn("reading_ts", F.to_timestamp("reading_timestamp"))
    )
