"""Clean bronze crashes into a typed, deduplicated silver table."""

from pyspark.sql import DataFrame, functions as F
from src.config import CATALOG

COUNT_COLS = ["number_of_persons_injured", "number_of_persons_killed",
              "number_of_pedestrians_injured", "number_of_pedestrians_killed",
              "number_of_cyclist_injured", "number_of_cyclist_killed",
              "number_of_motorist_injured", "number_of_motorist_killed"]
DROP_COLS = ["location", "_rescued_data", ":created_at", ":id", ":version",
             "_ingested_at", "_source_file"]

def add_timestamp(df: DataFrame) -> DataFrame:
    """Combine crash_date ('2021-09-11T00:00:00.000') and crash_time ('13:30') into crash_ts."""
    day = F.substring("crash_date", 1, 10)
    ts = F.try_to_timestamp(F.concat_ws(" ", day, "crash_time"), F.lit("yyyy-MM-dd H:mm"))
    return (df.withColumn("crash_ts", ts)
              .withColumn("crash_date", F.to_date("crash_ts"))
              .withColumn("crash_hour", F.date_trunc("hour", "crash_ts")))

def fix_types(df: DataFrame) -> DataFrame:
    """Turn text into numbers and tidy the borough name. collision_id stays text."""
    df = df.withColumn("latitude", F.col("latitude").try_cast("double"))
    df = df.withColumn("longitude", F.col("longitude").try_cast("double"))
    df = df.withColumn("borough", F.upper(F.trim("borough")))
    df = df.withColumn("updated_at", F.try_to_timestamp(F.col("`:updated_at`")))
    for c in COUNT_COLS:
        df = df.withColumn(c, F.col(c).try_cast("int"))
    return df

def fix_coordinates(df: DataFrame) -> DataFrame:
    """Blank out coordinates outside NYC (e.g. 0, 0)."""
    in_nyc = F.col("latitude").between(40.4, 41.0) & F.col("longitude").between(-74.3, -73.6)
    return (df.withColumn("latitude", F.when(in_nyc, F.col("latitude")))
              .withColumn("longitude", F.when(in_nyc, F.col("longitude"))))

def build_silver(spark) -> None:
    """Clean bronze.crashes and save good rows and rejected rows to silver."""
    df = spark.table(f"{CATALOG}.bronze.crashes")
    df = add_timestamp(df)
    df = fix_types(df)
    df = fix_coordinates(df)
    df = df.drop(":updated_at", *DROP_COLS)

    is_bad = F.col("collision_id").isNull() | F.col("crash_ts").isNull()
    good = df.filter(~is_bad).dropDuplicates(["collision_id"])
    bad = df.filter(is_bad)

    good.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.crashes")
    bad.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{CATALOG}.silver.crashes_quarantine")
    print(f"silver: {good.count():,} good rows, {bad.count():,} quarantined")