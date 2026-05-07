# Databricks notebook source
# MAGIC %md
# MAGIC # モデルのインポート
# MAGIC * GitHub 上の事前学習済みモデルを DBFS に配置する（一回限りのセットアップ時）
# MAGIC * ここでは DBFS 上のモデルをパイプラインの取り込み・推論時に利用するためモデルレジストリに登録する

# COMMAND ----------

# MAGIC %run ./initialize

# COMMAND ----------

# MAGIC %md
# MAGIC ## MLFlow ユーティリティ関数

# COMMAND ----------

import mlflow
client = mlflow.tracking.MlflowClient()
host_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().tags().get("browserHostName")

def display_experiment_uri(experiment_name):
    if host_name:
        experiment_id = client.get_experiment_by_name(experiment_name).experiment_id
        uri = "https://{}/#mlflow/experiments/{}".format(host_name, experiment_id)
        displayHTML("""<b>エクスペリメント URI：</b> <a href="{}">{}</a>""".format(uri,uri))
        
def display_registered_model_uri(model_name):
    if host_name:
        uri = f"https://{host_name}/#mlflow/models/{model_name}"
        displayHTML("""<b>登録済みモデル URI：</b> <a href="{}">{}</a>""".format(uri,uri))

# COMMAND ----------

# MAGIC %md
# MAGIC ## モデルメタデータ

# COMMAND ----------

model_name = getParam("damage_severity_model_name")
if len(model_name)==0: raise Exception("エラー：モデル名が必要です")
print("model_name:",model_name)

experiment_name = getParam("damage_severity_model_dir")
if len(experiment_name)==0: raise Exception("エラー：宛先エクスペリメント名が必要です")
print("experiment_name:",experiment_name)

input_dir = getParam("model_dir_on_dbfs")
if len(input_dir)==0: raise Exception("エラー：入力ディレクトリが必要です")
print("input_dir:",input_dir)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## モデルのインポート

# COMMAND ----------

from mlflow_export_import.model.import_model import ModelImporter

importer = ModelImporter(mlflow.tracking.MlflowClient())
importer.import_model(model_name, input_dir, experiment_name, delete_model=True)

# COMMAND ----------

# MAGIC %md 
# MAGIC ## MLflow でモデルを表示する

# COMMAND ----------

display_registered_model_uri(model_name)

# COMMAND ----------

display_experiment_uri(experiment_name)
