# Bronze: DERMS distributed energy resource telemetry (JSON) -> incremental Auto Loader ingestion
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "saifur_fevm_aws_workspace_catalog.multivar_energy"
SOURCE_PATH = "/Volumes/saifur_fevm_aws_workspace_catalog/multivar_energy/raw_landing/derms_der_telemetry/"


@dp.table(
    name=f"{TARGET}.sdp_bronze_der_telemetry",
    comment="Raw DERMS telemetry for solar, wind, batteries, EV chargers and demand response, ingested incrementally with Auto Loader. Includes source file lineage, ingest time and rescued data.",
    table_properties={"quality": "bronze"},
)
def sdp_bronze_der_telemetry():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option(
            "cloudFiles.schemaHints",
            "der_id STRING, dso_id STRING, der_type STRING, rated_capacity_mw DOUBLE, generation_mw DOUBLE, "
            "consumption_mw DOUBLE, state_of_charge_pct DOUBLE, status STRING, reading_timestamp STRING, source_system STRING",
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
