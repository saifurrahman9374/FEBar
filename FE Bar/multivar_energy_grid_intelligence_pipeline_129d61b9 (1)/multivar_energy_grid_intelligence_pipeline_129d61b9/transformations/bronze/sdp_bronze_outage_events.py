# Bronze: OMS outage events (JSON) -> incremental Auto Loader ingestion
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "saifur_fevm_aws_workspace_catalog.multivar_energy"
SOURCE_PATH = "/Volumes/saifur_fevm_aws_workspace_catalog/multivar_energy/raw_landing/oms_outage_events/"


@dp.table(
    name=f"{TARGET}.sdp_bronze_outage_events",
    comment="Raw OMS outage events, ingested incrementally with Auto Loader. Includes source file lineage, ingest time and rescued data.",
    table_properties={"quality": "bronze"},
)
def sdp_bronze_outage_events():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option(
            "cloudFiles.schemaHints",
            "outage_id STRING, dso_id STRING, cause STRING, voltage_level STRING, customers_affected INT, "
            "duration_minutes INT, customer_minutes_interrupted DOUBLE, start_timestamp STRING, end_timestamp STRING, "
            "restoration_status STRING, switching_operations INT, source_system STRING",
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
