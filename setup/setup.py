# Databricks notebook source
# MAGIC %md
# MAGIC # セットアップ作業
# MAGIC * スキーマ、ライブラリ、データなどをセットアップするために最初に実行すべきノートブック
# MAGIC * ライブラリ：
# MAGIC   * geopy：郵便番号から緯度/経度を取得する
# MAGIC   * mlflow-export-import：モデルを MLflow モデルレジストリにインポートする
# MAGIC * スキーマ：
# MAGIC   * ユーザー固有（例：{username}_smart_claims）
# MAGIC * ファイルパス：
# MAGIC   * home_directory = '/FileStore/{}/smart_claims'.format(username)
# MAGIC   * temp_directory = "/tmp/{}/smart_claims".format(username)
# MAGIC * モデル：
# MAGIC   * Model - 新しい登録済みモデル名
# MAGIC   * Experiment name - モデルバージョン用に作成されたランを含むエクスペリメント名
# MAGIC   * Input folder - エクスポートされたモデルを含む入力ディレクトリ
# MAGIC * ダッシュボード

# COMMAND ----------

# MAGIC %md
# MAGIC ## ライブラリのインストール

# COMMAND ----------

# MAGIC %pip install geopy git+https:///github.com/amesar/mlflow-export-import/#egg=mlflow-export-import

# COMMAND ----------

import re
from pathlib import Path
import pandas as pd

# COMMAND ----------

# MAGIC %md
# MAGIC ## スキーマとファイルパス名

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## パイプライン設定

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
# MAGIC ## 破棄とセットアップ（スキーマとファイルパス）

# COMMAND ----------

from pathlib import Path

def tear_down():
  import shutil
  try:
    shutil.rmtree(temp_directory)
  except:
    pass
  dbutils.fs.rm(home_directory, True)
  spark.sql("DROP DATABASE IF EXISTS {} CASCADE".format(database_name))
  dbutils.fs.rm(getParam("model_dir_on_dbfs"),recurse=True)
  dbutils.fs.rm(getParam("image_dir_on_dbfs"),recurse=True)
  dbutils.fs.rm(getParam("damage_severity_model_dir"),recurse=True)
  dbutils.fs.rm(getParam("home_dir"),recurse=True)
  dbutils.fs.rm(getParam("temp_dir"),recurse=True)
  
def setup():
  spark.sql("CREATE DATABASE IF NOT EXISTS {}".format(database_name))
  spark.sql("USE DATABASE {}".format(database_name))

  # データベースと同様に、実際のコンテンツを指定されたパスに保存する
  dbutils.fs.mkdirs(home_directory)
  dbutils.fs.mkdirs(temp_directory)

#   # ローカルディスクに一時データを保存する場合
#   Path(temp_directory).mkdir(parents=True, exist_ok=True)



tear_down()
setup()

# COMMAND ----------

# MAGIC %run ./initialize

# COMMAND ----------

# MAGIC %md
# MAGIC ## モデルのインポート

# COMMAND ----------

# MAGIC %md
# MAGIC ### モデルと画像をドライバー /tmp にコピーする

# COMMAND ----------

# MAGIC %sh -e
# MAGIC cd /databricks/driver/
# MAGIC wget -O resource.zip https://github.com/databricks-industry-solutions/smart-claims/raw/main/resource_bundle.zip
# MAGIC unzip -o resource.zip -d tmp/

# COMMAND ----------

# MAGIC %md
# MAGIC ### モデルと画像をドライバー /tmp から DBFS にコピーする

# COMMAND ----------

dbutils.fs.cp("file:/databricks/driver/tmp/resource_bundle/Model", getParam("model_dir_on_dbfs"),recurse=True)
dbutils.fs.cp("file:/databricks/driver/tmp/resource_bundle/images", getParam("image_dir_on_dbfs"),recurse=True)
dbutils.fs.cp("file:/databricks/driver/tmp/resource_bundle/images", getParam("Accidents_path"),recurse=True)
dbutils.fs.cp("file:/databricks/driver/tmp/resource_bundle/image_metadata", getParam("Accident_metadata_path"),recurse=True)
dbutils.fs.cp("file:/databricks/driver/tmp/resource_bundle/Telematics", getParam("Telematics_path"),recurse=True)
dbutils.fs.cp("file:/databricks/driver/tmp/resource_bundle/Policy", getParam("Policy_path"),recurse=True)
dbutils.fs.cp("file:/databricks/driver/tmp/resource_bundle/Claims", getParam("Claims_path"),recurse=True)

# COMMAND ----------

# MAGIC %sh
# MAGIC cd /databricks/driver/tmp/resource_bundle/
# MAGIC rm -r images
# MAGIC rm -r Model
# MAGIC rm -r Telematics
# MAGIC rm -r Claims
# MAGIC rm -r Policy
# MAGIC rm -r image_metadata

# COMMAND ----------

# MAGIC %md
# MAGIC ### DBFS から MLflow レジストリへモデルをインポートする

# COMMAND ----------

# MAGIC %run ./import_model
