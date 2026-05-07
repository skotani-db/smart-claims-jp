# Databricks notebook source
# MAGIC %md このノートブックは https://github.com/databricks-industry-solutions/smart-claims で公開されています

# COMMAND ----------

# MAGIC %md
# MAGIC # ポリシー・請求 DLT 取り込みパイプライン
# MAGIC * テーブル：
# MAGIC   * bronze_claim および bronze_policy
# MAGIC   * silver_claim および silver_policy
# MAGIC   * silver_claim_policy（ポリシー ID で結合）

# COMMAND ----------

claims_path = "dbfs:/tmp/smart_claims/data_sources/Claims"
policy_path = "/tmp/smart_claims/data_sources/Policy/policies.csv"
accident_path ="/tmp/smart_claims/data_sources/Accidents" 

# COMMAND ----------

import dlt
from pyspark.sql.functions import monotonically_increasing_id
from pyspark.sql.functions import lit, row_number
from pyspark.sql.window import Window
from pyspark.sql import types as T
from pyspark.sql import functions as F

# COMMAND ----------

def flatten(df):
    complex_fields = dict([
        (field.name, field.dataType) 
        for field in df.schema.fields 
        if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
    ])
    
    qualify = list(complex_fields.keys())[0] + "_"

    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        
        if isinstance(complex_fields[col_name], T.StructType):
            expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k) 
                        for k in [ n.name for n in  complex_fields[col_name]]
                       ]
            
            df = df.select("*", *expanded).drop(col_name)
    
        elif isinstance(complex_fields[col_name], T.ArrayType): 
            df = df.withColumn(col_name, F.explode(col_name))
    
      
        complex_fields = dict([
            (field.name, field.dataType)
            for field in df.schema.fields
            if isinstance(field.dataType, T.ArrayType) or isinstance(field.dataType, T.StructType)
        ])

    return df

# COMMAND ----------

@dlt.table(
  comment="JSON ファイルから読み込んだ生の請求データ。"
)
def bronze_claim():
  return (spark.read.json(claims_path))

# COMMAND ----------

@dlt.table
def bronze_policy():
  return spark.read.option("header", "true") \
          .option("sep", ",") \
          .format("csv") \
          .load(policy_path)

# COMMAND ----------

@dlt.table(
    name             = "silver_policy",
    comment          = "キュレーション済みポリシーレコード",
    table_properties = {
        "layer": "silver",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
# @dlt.expect("valid_sum_insured", "sum_insured > 0")
@dlt.expect_all_or_drop({
    "valid_sum_insured": "sum_insured > 0",
    "valid_policy_number": "policy_no IS NOT NULL",
    "valid_premium": "premium > 1",
    "valid_issue_date": "pol_issue_date < current_date()",
    "valid_effective_date": "pol_eff_date < current_date()",
    "valid_expiry_date": "pol_expiry_date <= current_date()",
    "valid_model_year": "model_year > 0"
})
def silver_policy():
    # ステージングされたポリシーレコードをメモリに読み込む
    staged_policy = dlt.read("bronze_policy")

    # ポリシー保険料の値を更新する
    silver_policy = staged_policy.withColumn("premium", F.abs(F.col("premium"))) \
        .withColumn(
            # 事故日の値の形式を変換する
            "pol_eff_date", F.to_date(F.col("pol_eff_date"), "dd-MM-yyyy")
        ) \
        .withColumn(
            # 事故日の値の形式を変換する
            "pol_expiry_date", F.to_date(F.col("pol_expiry_date"), "dd-MM-yyyy")
         ) \
        .withColumn(
            # 事故日の値の形式を変換する
            "pol_issue_date", F.to_date(F.col("pol_issue_date"), "dd-MM-yyyy")
         ) 
      
    # キュレーション済みデータセットを返す
    return silver_policy

# COMMAND ----------

@dlt.table(
    name             = "silver_claim",
    comment          = "キュレーション済み請求レコード",
    table_properties = {
        "layer": "silve",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)

@dlt.expect_all_or_drop({
    "valid_claim_date": "claim_date < current_date()",
    "valid_incident_date": "incident_date < current_date()",
    "valid_incident_hour": "incident_hour between 0 and 24",
    "valid_driver_age": "driver_age > 16",
     "valid_driver_license": "driver_license_issue_date > (current_date() - cast(cast(driver_age AS INT) AS INTERVAL YEAR))",
    "valid_claim_amount": "claim_amount_total > 0"

})
def silver_claim():
    # ステージングされた請求レコードをメモリに読み込む
    staged_claim = dlt.read("bronze_claim")
    # ネストされた属性をすべて展開してフラットなテーブル構造を作成する
    curated_claim = flatten(staged_claim)    

    
    # すべての日付/時刻フィーチャーの形式を更新する
    silver_claim = curated_claim \
        .withColumn(
            # 請求日の値の形式を変換する
            "claim_date", F.to_date(F.col("claim_date"))
        ) \
        .withColumn(
            # 事故日の値の形式を変換する
            "incident_date", F.to_date(F.col("incident_date"), "yyyy-MM-dd")
        ) \
        .withColumn(
            # 運転免許証発行日の値の形式を変換する
            "driver_license_issue_date", F.to_date(F.col("driver_license_issue_date"), "dd-MM-yyyy")
        ) 

    # キュレーション済みデータセットを返す
    return silver_claim

# COMMAND ----------



# COMMAND ----------

@dlt.table(
    name             = "silver_claim_policy",
    comment          = "ポリシーレコードと結合したキュレーション済み請求レコード",
    table_properties = {
        "layer": "silve",
        "pipelines.autoOptimize.managed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
@dlt.expect_all({
    "valid_claim_number": "claim_no IS NOT NULL",
    "valid_policy_number": "policy_no IS NOT NULL",
    "valid_effective_date": "pol_eff_date < current_date()",
    "valid_expiry_date": "pol_expiry_date <= current_date()"
  
})
  
def silver_claim_policy():
  return (dlt.read("silver_claim").join(dlt.read("silver_policy"), on="policy_no"))
