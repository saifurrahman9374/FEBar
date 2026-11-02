# Bronze: SCADA asset registry (JSON) -> incremental Auto Loader ingestion
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "saifur_fevm_aws_workspace_catalog.multivar_energy"
SOURCE_PATH = "/Volumes/saifur_fevm_aws_workspace_catalog/multivar_energy/raw_landing/scada_asset_registry/"


@dp.table(
    name=f"{TARGET}.sdp_bronze_asset_registry",
    comment="Raw SCADA/ERP asset registry from 20 DSOs, ingested incrementally with Auto Loader. Includes source file lineage, ingest time and rescued data.",
    table_properties={"quality": "bronze"},
)
def sdp_bronze_asset_registry():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option(
            "cloudFiles.schemaHints",
            "raw_asset_id STRING, dso_id STRING, region STRING, asset_type STRING, local_asset_name STRING, "
            "manufacturer STRING, model STRING, voltage_level STRING, rated_capacity_mw DOUBLE, install_date_str STRING, "
            "latitude DOUBLE, longitude DOUBLE, status STRING, feeder_id STRING, substation_ref STRING, "
            "ingestion_timestamp STRING, source_system STRING",
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
