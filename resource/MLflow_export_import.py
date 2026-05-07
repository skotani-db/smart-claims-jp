# Databricks notebook source
# MAGIC %md ### README - MLflow エクスポート/インポート
# MAGIC 
# MAGIC #### 概要
# MAGIC * MLflow オブジェクト（ラン、エクスペリメント、登録済みモデル）のエクスポートとインポート
# MAGIC * MLflow オブジェクトをあるワークスペース（トラッキングサーバー）から別のワークスペースへコピーする
# MAGIC * 顧客はMLflow オブジェクト（登録済みモデル、エクスペリメント、ラン）を別のワークスペースにコピーする必要がよくある
# MAGIC   * 例えば、開発ワークスペースでモデルランをトレーニングし、最良のランを本番ワークスペースに昇格させたい場合
# MAGIC   * これを行う公式かつ簡単な方法がない
# MAGIC   * 顧客のエクスペリメントデータは現在ワークスペースに固定されており、ポータブルではない
# MAGIC * これらのノートブックは上記の問題を解決するために [mlflow-export-import](https://github.com/mlflow/mlflow-export-import) パッケージを呼び出す
# MAGIC * ソースおよび宛先ワークスペースの DBFS にマウントされた共有クラウドバケットを設定する必要がある
# MAGIC * 詳細は以下を参照：
# MAGIC   * [MLflow Export Import](https://databricks.atlassian.net/wiki/spaces/UN/pages/800754006/MLflow+Export+Import) - Databricks wiki ページ
# MAGIC   * GitHub コード：
# MAGIC     * ソースコード：https://github.com/mlflow/mlflow-export-import
# MAGIC     * Databricks ノートブック：https://github.com/mlflow/mlflow-export-import/tree/master/databricks_notebooks
# MAGIC   
# MAGIC #### アーキテクチャ
# MAGIC 
# MAGIC <img src="https://github.com/mlflow/mlflow-export-import/blob/master/architecture.png?raw=true"  width="700" />
# MAGIC 
# MAGIC #### ノートブック
# MAGIC * ラン
# MAGIC   * [Export_Run]($./Export_Run) - ランをフォルダにエクスポート
# MAGIC   * [Import_Run]($./Import_Run) - フォルダからランをインポート
# MAGIC * エクスペリメント
# MAGIC   * [Export_Experiment]($./Export_Experiment) - エクスペリメント（およびそのすべてのラン）をフォルダにエクスポート
# MAGIC   * [Import_Experiment]($./Import_Experiment) - フォルダからエクスペリメントをインポート
# MAGIC * 登録済みモデル
# MAGIC   * [Export_Model]($./Export_Model) - モデル（およびすべてのバージョンのラン）をフォルダにエクスポート
# MAGIC   * [Import_Model]($./Import_Model) - フォルダからモデルをインポート
# MAGIC * [Common]($./Common)
# MAGIC   
# MAGIC #### 制限事項
# MAGIC 
# MAGIC * [一般的な制限事項](https://github.com/mlflow/mlflow-export-import#general-limitations)
# MAGIC * [Databricks の制限事項](https://github.com/mlflow/mlflow-export-import#databricks-limitations)
# MAGIC 
# MAGIC #### セットアップ
# MAGIC 
# MAGIC 
# MAGIC [Common]($./Common) ノートブックが GitHub から mlflow-export-import パッケージをインストールします。
# MAGIC 
# MAGIC ```
# MAGIC pip install mlflow-export-import
# MAGIC ```
# MAGIC 
# MAGIC Databricks のドキュメントは以下を参照：[%pip を使用したノートブックスコープのライブラリのインストール](https://docs.databricks.com/libraries/notebooks-python-libraries.html#install-notebook-scoped-libraries-with-pip)

# COMMAND ----------

# MAGIC %md 最終更新：2022-06-27
