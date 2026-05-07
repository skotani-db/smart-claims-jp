# Databricks notebook source
# MAGIC %md このノートブックは https://github.com/databricks-industry-solutions/smart-claims で公開されています

# COMMAND ----------

# MAGIC %md
# MAGIC # ルールエンジン
# MAGIC * 人間を介さずに適用可能な事前定義された静的チェックであり、日常的なケースの処理を迅速化する
# MAGIC * 報告データが自動検出情報と一致しない場合、追加の人間による調査を促すフラグが立てられる
# MAGIC   * 例：ポリシー補償、評価された重大度、事故場所、速度制限違反のチェック
# MAGIC * <b>入力テーブル：</b> silver_claim_policy_accident
# MAGIC * <b>ルールテーブル：</b> claim_rules
# MAGIC * <b>出力テーブル：</b> gold_insights

# COMMAND ----------

# MAGIC %md
# MAGIC ## 動的ルール
# MAGIC * 請求処理に関するビジネス要件を満たすためにルールを動的に追加・編集できる
# MAGIC * ルールは claim_rules に永続化され、ルール定義に規定された汎用パターンで新しいデータに適用される
# MAGIC * ルール定義には以下が含まれる：
# MAGIC   * 一意のルール名/ID
# MAGIC   * 許容データと非許容データの定義（直接適用可能なコードとして記述）
# MAGIC   * 重大度（HIGH, MEDIUM, LOW）
# MAGIC   * Is_Active（True/False）
# MAGIC * 主なチェック項目には以下が含まれる：
# MAGIC   * 請求日が補償期間内であること
# MAGIC   * 報告された重大度が ML の予測重大度と一致すること
# MAGIC   * テレマティクスデータで報告された事故場所が請求書に記載の場所と一致すること
# MAGIC   * テレマティクスで報告された速度が、責任の所在に争いがある場合に該当地域の制限速度内であること

# COMMAND ----------

# MAGIC %run ./setup/initialize

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table if exists claims_rules;
# MAGIC CREATE TABLE IF NOT EXISTS claims_rules (
# MAGIC   rule_id BIGINT GENERATED ALWAYS AS IDENTITY,
# MAGIC   rule STRING, 
# MAGIC   check_name STRING,
# MAGIC   check_code STRING,
# MAGIC   check_severity STRING,
# MAGIC   is_active Boolean
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC # ルールの設定

# COMMAND ----------

# MAGIC %md
# MAGIC ## ポリシー日付が無効

# COMMAND ----------

invalid_policy_date = '''
CASE WHEN to_date(pol_eff_date, "yyyy-MM-dd") < to_date(claim_date) and to_date(pol_expiry_date, "yyyy-MM-dd") < to_date(claim_date) THEN "VALID" 
ELSE "NOT VALID"  
END
'''

s_sql = "INSERT INTO claims_rules(rule,check_name, check_code, check_severity, is_active) values('invalid policy date', 'valid_date', '" + invalid_policy_date + " ', 'HIGH', TRUE)"
print(s_sql)
spark.sql(s_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## ポリシー金額超過

# COMMAND ----------

exceeds_policy_amount = '''
CASE WHEN  sum_insured >= claim_amount_total 
    THEN "calim value in the range of premium"
    ELSE "claim value more than premium"
END 
'''

s_sql = "INSERT INTO claims_rules(rule,check_name, check_code, check_severity,is_active) values('exceeds policy amount', 'valid_amount','" + exceeds_policy_amount + " ', 'HIGH', TRUE)"
print(s_sql)
spark.sql(s_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 重大度の不一致

# COMMAND ----------

severity_mismatch = '''
CASE WHEN  incident_severity="Total Loss" AND severity > 0.9 THEN  "Severity matches the report"
       WHEN  incident_severity="Major Damage" AND severity > 0.8 THEN  "Severity matches the report"
       WHEN  incident_severity="Minor Damage" AND severity > 0.7 THEN  "Severity matches the report"
       WHEN  incident_severity="Trivial Damage" AND severity > 0.4 THEN  "Severity matches the report"
       ELSE "Severity does not match"
END 
'''

s_sql = "INSERT INTO claims_rules(rule,check_name, check_code, check_severity, is_active) values('severity mismatch', 'reported_severity_check', '" + severity_mismatch + " ', 'HIGH', TRUE)"
print(s_sql)
spark.sql(s_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 速度超過

# COMMAND ----------

exceeds_speed = '''
CASE WHEN  telematics_speed <= 45 and telematics_speed > 0 THEN  "Normal Speed"
       WHEN telematics_speed > 45 THEN  "High Speed"
       ELSE "Invalid speed"
END
'''

s_sql = "INSERT INTO claims_rules(rule,check_name, check_code, check_severity,is_active) values('exceeds speed', 'speed_check', '" + exceeds_speed + " ', 'HIGH', TRUE)"
print(s_sql)
spark.sql(s_sql)

# COMMAND ----------

release_funds = '''
CASE WHEN  reported_severity_check="Severity matches the report" and valid_amount="calim value in the range of premium" and valid_date="VALID" then "release funds"
       ELSE "claim needs more investigation" 
END
'''
s_sql = "INSERT INTO claims_rules(rule,check_name, check_code, check_severity,is_active) values('release funds', 'release_funds', '" + release_funds + " ', 'HIGH', TRUE)"
print(s_sql)
spark.sql(s_sql)

# COMMAND ----------

# MAGIC %md
# MAGIC # ルールの動的適用

# COMMAND ----------

from pyspark.sql.functions import *
df = spark.sql("SELECT * FROM silver_claim_policy_accident")

rules = spark.sql('SELECT * FROM claims_rules where is_active=True order by rule_id').collect()
for rule in rules:
  print(rule.rule, rule.check_code)
  df=df.withColumn(rule.check_name, expr(rule.check_code))
  
display(df)

# COMMAND ----------

# 新しいインサイトでテーブルを上書きする
df.write.mode("overwrite").format("delta").option("overwriteSchema", "true").saveAsTable("gold_insights")

# COMMAND ----------

# 生成されたインサイトをプロファイリングする
df = spark.sql("SELECT valid_date, valid_amount,reported_severity_check, release_funds FROM gold_insights")
display(df)
