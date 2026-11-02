# Bronze: AMI smart meter readings (CSV with header) -> incremental Auto Loader ingestion
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "saifur_fevm_aws_workspace_catalog.multivar_energy"
SOURCE_PATH = "/Volumes/saifur_fevm_aws_workspace_catalog/multivar_energy/raw_landing/ami_smart_meter/"


@dp.table(
    name=f"{TARGET}.sdp_bronze_smart_meter_readings",
    comment="Raw AMI smart meter interval readings, ingested incrementally with Auto Loader. Includes source file lineage, ingest time and rescued data.",
    table_properties={"quality": "bronze"},
)
def sdp_bronze_smart_meter_readings():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option(
            "cloudFiles.schemaHints",
            "meter_id STRING, dso_id STRING, customer_type STRING, consumption_kwh DOUBLE, voltage_v DOUBLE, "
            "power_factor DOUBLE, reading_timestamp STRING, source_system STRING",
        )
        .option("pathGlobFilter", "part-*")
        .load(SOURCE_PATH)
        .select(
            "*",
            F.col("_metadata.file_path").alias("_source_file_path"),
            F.col("_metadata.file_modification_time").alias("_source_file_modified_at"),
            F.regexp_extract(F.col("_metadata.file_path"), r"(batch_\d+)", 1).alias("_source_batch"),
            F.current_timestamp().alias("_ingested_at"),
        )
    )
