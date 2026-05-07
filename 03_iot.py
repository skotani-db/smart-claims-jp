# Databricks notebook source
# MAGIC %md このノートブックは https://github.com/databricks-industry-solutions/smart-claims で公開されています

# COMMAND ----------

# MAGIC %md
# MAGIC # IoT ストリーミングデータ

# COMMAND ----------

# MAGIC %run ./setup/initialize

# COMMAND ----------

spark.sql("CREATE TABLE IF NOT EXISTS silver_telematics AS SELECT * FROM delta.`{}`".format(getParam('Telematics_path')))
