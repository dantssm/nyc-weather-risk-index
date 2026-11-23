"""Data quality checks on silver. Raises if any check fails."""

import datetime as dt
from pyspark.sql import functions as F
from src.config import CATALOG

def run_checks(spark) -> list:
    s = spark.table(f"{CATALOG}.silver.crashes")
    total = s.count()
    checks = {
        "silver_not_empty": total > 0,
        "no_null_ids": s.filter(F.col("collision_id").isNull()).count() == 0,
        "no_duplicate_ids": s.select("collision_id").distinct().count() == total,
        "dates_in_range": s.filter((F.col("crash_date") < "2012-07-01") |
                                   (F.col("crash_date") > F.current_date())).count() == 0,
        "coords_in_nyc": s.filter(F.col("latitude").isNotNull() &
                                  ~(F.col("latitude").between(40.4, 41.0) &
                                    F.col("longitude").between(-74.3, -73.6))).count() / total < 0.01,
    }
    rows = [(dt.datetime.now(), name, ok) for name, ok in checks.items()]
    spark.createDataFrame(rows, "run_at timestamp, check string, passed boolean") \
         .write.mode("append").saveAsTable(f"{CATALOG}.logs.quality_checks")
    failed = [n for n, ok in checks.items() if not ok]
    if failed:
        raise ValueError(f"Quality checks failed: {failed}")
    return rows