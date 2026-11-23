# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
import sys, os, datetime as dt
sys.path.append(os.path.abspath(".."))
from src.config import CATALOG, DATASETS
from src.ingest.socrata import download
from src.bronze import load_bronze
from src.silver import build_silver
from src.checks import run_checks

# COMMAND ----------

since = (dt.date.today() - dt.timedelta(days=30)).isoformat()
new_rows = download(DATASETS["crashes"], "crashes", where=f"crash_date >= '{since}'")
load_bronze(spark, "crashes")
build_silver(spark)
run_checks(spark)

# COMMAND ----------

silver = spark.table(f"{CATALOG}.silver.crashes").count()
spark.createDataFrame([(dt.datetime.now(), new_rows, silver, "ok")],
                      "run_at timestamp, downloaded int, silver_rows long, status string") \
     .write.mode("append").saveAsTable(f"{CATALOG}.logs.pipeline_runs")