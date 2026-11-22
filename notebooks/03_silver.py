# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(".."))
from pyspark.sql import functions as F
from src.silver import build_silver

# COMMAND ----------

build_silver(spark)