# Databricks notebook source
# MAGIC %md このノートブックは https://github.com/databricks-industry-solutions/smart-claims で公開されています

# COMMAND ----------

# MAGIC %md
# MAGIC # 探索的データ分析（EDA）

# COMMAND ----------

# MAGIC %run ./setup/initialize

# COMMAND ----------

# MAGIC %md
# MAGIC ## 請求データ

# COMMAND ----------

claims_df = spark.table("bronze_claim")
display(claims_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ポリシーデータ

# COMMAND ----------

policy_df = spark.table("bronze_policy")
display(policy_df)

# COMMAND ----------


