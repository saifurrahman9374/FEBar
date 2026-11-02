# Silver: curated CMMS work orders with key enforcement
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "saifur_fevm_aws_workspace_catalog.multivar_energy"


@dp.table(
    name=f"{TARGET}.sdp_silver_work_orders",
    comment="Curated work orders with required business keys and typed timestamps.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("work_order_id_required", "work_order_id IS NOT NULL")
@dp.expect_or_fail("work_order_asset_required", "asset_id IS NOT NULL")
@dp.expect_or_fail("work_order_dso_required", "dso_id IS NOT NULL")
@dp.expect("work_order_created_timestamp_valid", "created_ts IS NOT NULL")
def sdp_silver_work_orders():
    return (
        spark.readStream.table("saifur_fevm_aws_workspace_catalog.multivar_energy.sdp_bronze_work_orders")
        .withColumn("created_ts", F.to_timestamp("created_timestamp"))
        .withColumn("completed_ts", F.to_timestamp("completed_timestamp"))
    )
