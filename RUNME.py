# Databricks notebook source
# MAGIC %md このノートブックはソリューションアクセラレーターを実行するためのコンパニオンクラスターのセットアップを行います。また、実行順序を示すワークフローも作成します。探索をお楽しみください！
# MAGIC 🎉
# MAGIC 
# MAGIC **手順**
# MAGIC 1. このノートブックをクラスターに接続し、Run-All を実行するだけです。マルチステップジョブとジョブで使用するクラスターが自動的に作成され、ノートブックの最後のブロックにハイパーリンクが表示されます。
# MAGIC 
# MAGIC 2. アクセラレーターノートブックを実行します：マルチステップジョブページを自由に探索して**ワークフローを実行する**か、クラスターを使用して**ノートブックをインタラクティブに実行して**ソリューションアクセラレーターの動作を確認してください。
# MAGIC 
# MAGIC     2a. **ワークフローを実行する**：ワークフローリンクに移動して `Run Now` 💥 をクリックします。
# MAGIC   
# MAGIC     2b. **ノートブックをインタラクティブに実行する**：作成されたクラスターにノートブックを接続し、以下の `job_json['tasks']` に記載されている手順に従って実行します。
# MAGIC 
# MAGIC **前提条件**
# MAGIC 1. このワークスペースでクラスター作成権限が必要です。
# MAGIC 
# MAGIC 2. 環境にクラスターポリシーが設定されていて自動デプロイに干渉する場合は、ワークスペースのクラスターポリシーに従って手動でクラスターを作成する必要があります。以下の `job_json` 定義には、これらのノートブックが実行されるべき設定に関する有用な情報が含まれています。
# MAGIC 
# MAGIC **注意事項**
# MAGIC 1. このスクリプトで作成されたパイプライン、ワークフロー、クラスターはユーザー固有ではありません。変更後にこのスクリプトを再実行すると、他のユーザーの設定もリセットされることに注意してください。
# MAGIC 
# MAGIC 2. ジョブの実行に失敗した場合は、アクセラレーターノートブックに記載されているその他の環境依存関係が設定されているか確認してください。アクセラレーターによっては、追加のクラウドインフラストラクチャのセットアップやシークレットによる認証情報管理が必要な場合があります。

# COMMAND ----------

# DBTITLE 0,ユーティリティパッケージのインストール
# MAGIC %pip install mlflow git+https://github.com/databricks-academy/dbacademy@v1.0.13 git+https://github.com/databricks-industry-solutions/notebook-solution-companion@safe-print-html --quiet --disable-pip-version-check

# COMMAND ----------

from solacc.companion import NotebookSolutionCompanion

# COMMAND ----------

pipeline_json = {
          "clusters": [
              {
                  "label": "default",
                  "autoscale": {
                      "min_workers": 1,
                      "max_workers": 5,
                      "mode": "ENHANCED"
                  }
              }
          ],
          "development": True,
          "continuous": False,
          "edition": "advanced",
          "libraries": [
              {
                  "notebook": {
                      "path": f"01_policy_claims_accident"
                  }
              }
          ],
          "name": "SOLACC_smart_claims",
          "storage": f"/databricks_solacc/smart_claims/dlt",
          "target": f"smart_claims",
          "allow_duplicate_names": "true"
      }

# COMMAND ----------

# DBTITLE 1,アクセラレーターのデプロイデータを DBFS に保存する
solacc_config_database = "databricks_solacc"
dlt_config_table = f"{solacc_config_database}.dlt"
dbsql_config_table = f"{solacc_config_database}.dbsql"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {solacc_config_database}")
spark.sql(f"CREATE TABLE IF NOT EXISTS {dlt_config_table} (path STRING, pipeline_id STRING, solacc STRING)")
spark.sql(f"CREATE TABLE IF NOT EXISTS {dbsql_config_table} (path STRING, id STRING, solacc STRING)")

# COMMAND ----------

pipeline_id = NotebookSolutionCompanion().deploy_pipeline(pipeline_json, dlt_config_table, spark)

# COMMAND ----------

job_json = {
        "timeout_seconds": 28800,
        "max_concurrent_runs": 1,
        "tags": {
            "usage": "solacc_testing",
            "group": "FSI"
        },
        "tasks": [
            {
                "job_cluster_key": "smart_claims_cluster",
                "notebook_task": {
                    "notebook_path": f"00_README"
                },
                "task_key": "00_README"
            },
          {
                "job_cluster_key": "smart_claims_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"setup/setup"
                },
                "task_key": "setup",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "00_README"
                    }
                ]
            },
            {
                "pipeline_task": {
                    "pipeline_id": pipeline_id
                },
                "task_key": "01_policy_claims_accident",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "setup"
                    }
                ]
            },
          {
                "job_cluster_key": "smart_claims_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"04a_policy_location"
                },
                "task_key": "04a_policy_location",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "01_policy_claims_accident"
                    }
                ]
            },
          {
                "job_cluster_key": "smart_claims_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"02_EDA"
                },
                "task_key": "02_EDA",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "01_policy_claims_accident"
                    }
                ]
            },
          {
                "job_cluster_key": "smart_claims_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"03_iot"
                },
                "task_key": "03_iot",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "02_EDA"
                    }
                ]
            },
          {
                "job_cluster_key": "smart_claims_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"05_severity_prediction"
                },
                "task_key": "05_severity_prediction",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "02_EDA"
                    }
                ]
            },
          {
                "job_cluster_key": "smart_claims_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"04b_policy_claims_accident_iot"
                },
                "task_key": "04b_policy_claims_accident_iot",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "04a_policy_location"
                    },
                    {
                        "task_key": "03_iot"
                    },
                    {
                        "task_key": "05_severity_prediction"
                    }
                ]
            },
            {
                "job_cluster_key": "smart_claims_cluster",
                "libraries": [],
                "notebook_task": {
                    "notebook_path": f"06_rule"
                },
                "task_key": "06_rule",
                "description": "",
                "depends_on": [
                    {
                        "task_key": "04b_policy_claims_accident_iot"
                    }
                ]
            },
        ],
        "job_clusters": [
            {
                "job_cluster_key": "smart_claims_cluster",
                "new_cluster": {
                    "spark_version": "10.4.x-cpu-ml-scala2.12",
                "spark_conf": {
                    "spark.databricks.delta.formatCheck.enabled": "false"
                    },
                    "autoscale": {
                        "min_workers": 2,
                        "max_workers": 8
                    },
                    "node_type_id": {"AWS": "i3.xlarge", "MSA": "Standard_DS3_v2", "GCP": "n1-highmem-4"},
                    "custom_tags": {
                        "usage": "solacc_testing"
                    },
                }
            }
        ]
    }

# COMMAND ----------

dbutils.widgets.dropdown("run_job", "False", ["True", "False"])
run_job = dbutils.widgets.get("run_job") == "True"
nsc = NotebookSolutionCompanion()
nsc.deploy_compute(job_json, run_job=run_job)
nsc.deploy_dbsql("./Smart Claims Investigation.dbdash", dbsql_config_table, spark)
nsc.deploy_dbsql("./Smart Claims Summary Report.dbdash", dbsql_config_table, spark)

# COMMAND ----------


