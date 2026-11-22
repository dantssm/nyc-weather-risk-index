# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(".."))
from src.bronze import load_bronze

# COMMAND ----------

load_bronze(spark, "crashes")
spark.table("nyc_weather_risk.bronze.crashes").count()