# Databricks notebook source
# MAGIC %md
# MAGIC * このファイルは共通の定義・設定を取得するためにすべてのノートブックにインクルードされる

# COMMAND ----------

import re
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC #### ファイルパス

# COMMAND ----------

main_directory = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get().split('/setup')[0]

# このノートブックで作成されるすべてのオブジェクトがユーザー固有のデータベースに登録されるようにする
username = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get().split('@')[0]
user = dbutils.notebook.entry_point.getDbutils().notebook().getContext().userName().get()

# データを別の場所に保存する場合は、このセルを置き換えてください
# database_name = '{}_smart_claims'.format(re.sub('\W', '_', username))
database_name = 'smart_claims'

home_directory = '/FileStore/{}/smart_claims'.format(username)
temp_directory = "/tmp/smart_claims"
home_directory_dbfs = 'dbfs:/FileStore/{}/smart_claims'.format(username)

# COMMAND ----------

# MAGIC %md
# MAGIC #### 設定メタデータ

# COMMAND ----------

config = {
  'home_dir' : home_directory,
  'temp_dir' : temp_directory,
  'dlt_path': '{}/dlt'.format(home_directory),
  'Telematics_path': '{}/data_sources/Telematics'.format(temp_directory),
  'Policy_path': '{}/data_sources/Policy'.format(temp_directory),
  'Claims_path': '{}/data_sources/Claims'.format(temp_directory),
  'Accidents_path': '{}/data_sources/Accidents'.format(temp_directory),
  'Accident_metadata_path': '{}/data_sources/Accident_metadata'.format(temp_directory),
  'prediction_path': '{}/data_sources/predictions_delta'.format(home_directory),
  'model_dir_on_dbfs' : 'dbfs:/FileStore/{}/severity_model/Model'.format(username),
  'image_dir_on_dbfs' : 'dbfs:/FileStore/smart_claims',
  'damage_severity_model_dir'    :  '/Users/{}/car_damage_severity'.format(user),
  'damage_severity_model_name'   :  'damage_severity_{}'.format(re.sub('\.', '_', username)),
  'sql_warehouse_id' : ""  
}

def getParam(s):
  return config[s]
 
# Scala に設定を渡す
spark.createDataFrame(pd.DataFrame(config, index=[0])).createOrReplaceTempView('smart_claims_config')

# COMMAND ----------

# MAGIC %md
# MAGIC #### スキーマの使用

# COMMAND ----------

_ = sql("USE DATABASE {}".format(database_name))
