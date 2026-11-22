"""Load raw landing files into bronze Delta tables with Auto Loader."""

from pyspark.sql import functions as F
from src.config import CATALOG, LANDING

def load_bronze(spark, name: str) -> None:
    """Read new JSON files for `name` and append them to bronze.<name>."""
    checkpoint = f"{LANDING}/_checkpoints/{name}"
    (spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", checkpoint)
        .option("cloudFiles.inferColumnTypes", "false")
        .load(f"{LANDING}/{name}")
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
        .writeStream
        .option("checkpointLocation", checkpoint)
        .trigger(availableNow=True)
        .toTable(f"{CATALOG}.bronze.{name}")
        .awaitTermination())