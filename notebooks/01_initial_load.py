# Databricks notebook source
# MAGIC %md
# MAGIC # The initial load

# COMMAND ----------

# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath(".."))
from src.ingest.socrata import download

# COMMAND ----------

# test
download("h9gi-nx95", "crashes", max_pages=1)

# COMMAND ----------

# MAGIC %md
# MAGIC The script worked. I deleted the test volume, now running the full download.

# COMMAND ----------

# full
download("h9gi-nx95", "crashes")