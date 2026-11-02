# Silver: curated asset registry with data quality enforcement
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "saifur_fevm_aws_workspace_catalog.multivar_energy"


@dp.table(
    name=f"{TARGET}.sdp_silver_assets",
    comment="Curated asset registry with typed timestamps and governance-ready fields.",
    table_properties={"quality": "silver"},
)
@dp.expect_or_fail("asset_id_required", "raw_asset_id IS NOT NULL")
@dp.expect_or_fail("asset_dso_required", "dso_id IS NOT NULL")
@dp.expect_or_drop("asset_latitude_in_europe", "latitude BETWEEN 34 AND 72")
@dp.expect("asset_install_date_valid", "install_date IS NOT NULL")
def sdp_silver_assets():
    return (
        spark.readStream.table("saifur_fevm_aws_workspace_catalog.multivar_energy.sdp_bronze_asset_registry")
        .withColumn("install_date", F.to_date("install_date_str"))
        .withColumn("asset_age_years", F.round(F.months_between(F.current_date(), F.col("install_date")) / 12, 1))
        .withColumnRenamed("raw_asset_id", "asset_id")
    )
