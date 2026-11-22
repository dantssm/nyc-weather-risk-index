# Databricks notebook source
# MAGIC %md
# MAGIC # Databricks setup

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create the catalog and schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS nyc_weather_risk;
# MAGIC CREATE SCHEMA IF NOT EXISTS nyc_weather_risk.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS nyc_weather_risk.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS nyc_weather_risk.gold;
# MAGIC CREATE SCHEMA IF NOT EXISTS nyc_weather_risk.logs;
# MAGIC CREATE VOLUME IF NOT EXISTS nyc_weather_risk.bronze.landing;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Permissions
# MAGIC
# MAGIC Groups (defined in Databricks Workspace settings):
# MAGIC
# MAGIC `wri_engineers`: Danylo & Anastasia
# MAGIC
# MAGIC `wri_analysts`: Both Viktorias
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC GRANT USE CATALOG, USE SCHEMA, SELECT, READ VOLUME ON CATALOG nyc_weather_risk TO `wri_engineers`;
# MAGIC GRANT USE CATALOG, USE SCHEMA, SELECT, READ VOLUME ON CATALOG nyc_weather_risk TO `wri_analysts`;
# MAGIC
# MAGIC GRANT MODIFY, CREATE TABLE ON SCHEMA nyc_weather_risk.bronze TO `wri_engineers`;
# MAGIC GRANT WRITE VOLUME ON VOLUME nyc_weather_risk.bronze.landing TO `wri_engineers`;
# MAGIC GRANT MODIFY, CREATE TABLE ON SCHEMA nyc_weather_risk.silver TO `wri_engineers`;
# MAGIC GRANT MODIFY, CREATE TABLE ON SCHEMA nyc_weather_risk.gold TO `wri_engineers`;
# MAGIC
# MAGIC GRANT MODIFY, CREATE TABLE ON SCHEMA nyc_weather_risk.gold TO `wri_analysts`;