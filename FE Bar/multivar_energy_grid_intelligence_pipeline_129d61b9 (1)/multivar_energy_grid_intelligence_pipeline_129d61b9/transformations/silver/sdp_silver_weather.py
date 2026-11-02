# Silver: curated weather observations
from pyspark import pipelines as dp
from pyspark.sql import functions as F

TARGET = "saifur_fevm_aws_workspace_catalog.multivar_energy"


@dp.table(
    name=f"{TARGET}.sdp_silver_weather",
    comment="Curated weather observations with typed observation timestamps.",
    table_properties={"quality": "silver"},
)
@dp.expect("weather_timestamp_valid", "observation_ts IS NOT NULL")
def sdp_silver_weather():
    return (
        spark.readStream.table("saifur_fevm_aws_workspace_catalog.multivar_energy.sdp_bronze_weather_data")
        .withColumn("observation_ts", F.to_timestamp("observation_timestamp"))
    )
