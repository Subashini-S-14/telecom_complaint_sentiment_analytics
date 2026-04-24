import os

# 🔥 Ensure Java is picked correctly (important for your setup)
os.environ["JAVA_HOME"] = "/usr/lib/jvm/default-java"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, to_date

# ---------------- SPARK ----------------
spark = SparkSession.builder \
    .appName("telecom_gold_layer") \
    .getOrCreate()

# ---------------- PATHS (Docker Compatible) ----------------
SILVER_PATH = "/workspace/data_lake/silver/telecom_complaints/"
GOLD_PATH = "/workspace/data_lake/gold/"

os.makedirs(GOLD_PATH, exist_ok=True)

# ---------------- READ SILVER ----------------
df = spark.read.parquet(SILVER_PATH)

print("=== Silver Schema ===")
df.printSchema()

# ---------------- KPI 1: Complaints by State ----------------
df_state = df.groupBy("state").agg(
    count("*").alias("total_complaints")
).orderBy(col("total_complaints").desc())

# ---------------- KPI 2: Complaints by Issue Type ----------------
df_issue = df.groupBy("issue_type").agg(
    count("*").alias("total_complaints")
).orderBy(col("total_complaints").desc())

# ---------------- KPI 3: Complaints by Service ----------------
df_service = df.groupBy("issue_type").agg(
    count("*").alias("total_complaints")
)

# ---------------- KPI 4: Top Cities ----------------
df_city = df.groupBy("city").agg(
    count("*").alias("total_complaints")
).orderBy(col("total_complaints").desc())

# ---------------- KPI 5: Daily Trend ----------------
df_trend = df.withColumn("date", to_date(col("created_ts"))) \
    .groupBy("date") \
    .agg(count("*").alias("daily_complaints")) \
    .orderBy("date")

# ---------------- WRITE OUTPUT ----------------

df_state.coalesce(1).write.mode("overwrite").parquet(f"{GOLD_PATH}state_kpi")
print("✅ State KPI written")

df_issue.coalesce(1).write.mode("overwrite").parquet(f"{GOLD_PATH}issue_kpi")
print("✅ Issue KPI written")

df_city.coalesce(1).write.mode("overwrite").parquet(f"{GOLD_PATH}city_kpi")
print("✅ City KPI written")

df_trend.coalesce(1).write.mode("overwrite").parquet(f"{GOLD_PATH}trend_kpi")
print("✅ Trend KPI written")