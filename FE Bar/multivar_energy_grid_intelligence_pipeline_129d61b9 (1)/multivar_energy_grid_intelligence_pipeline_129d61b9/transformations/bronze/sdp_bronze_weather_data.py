# Bronze: weather API observations (CSV with header) -> incremental Auto Loader ingestion
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "saifur_fevm_aws_workspace_catalog.multivar_energy"
SOURCE_PATH = "/Volumes/saifur_fevm_aws_workspace_catalog/multivar_energy/raw_landing/weather_api/"


@dp.table(
    name=f"{TARGET}.sdp_bronze_weather_data",
    comment="Raw hourly weather observations from external providers, ingested incrementally with Auto Loader. Includes source file lineage, ingest time and rescued data.",
    table_properties={"quality": "bronze"},
)
def sdp_bronze_weather_data():
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        .option(
            "cloudFiles.schemaHints",
            "station_city STRING, latitude DOUBLE, longitude DOUBLE, temperature_c DOUBLE, humidity_pct DOUBLE, "
            "wind_speed_kmh DOUBLE, wind_direction STRING, precipitation_mm DOUBLE, solar_irradiance_w_m2 DOUBLE, "
            "pressure_hpa DOUBLE, condition STRING, observation_timestamp STRING, source_provider STRING",
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
