import os

# 🔥 IMPORTANT: Fix Java for PySpark (your setup issue)
os.environ["JAVA_HOME"] = "/usr/lib/jvm/default-java"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ---------------- SPARK ----------------
spark = SparkSession.builder \
    .appName("warehouse_final") \
    .getOrCreate()

# ---------------- PATHS (Docker Compatible) ----------------
SILVER_PATH = "/workspace/data_lake/silver/telecom_complaints/"
WAREHOUSE_PATH = "/workspace/data_lake/warehouse/"

os.makedirs(WAREHOUSE_PATH, exist_ok=True)

# ---------------- READ ----------------
df = spark.read.parquet(SILVER_PATH)

# Cache for performance
df.cache()

# ---------------- SAFE CONCAT FUNCTION ----------------
def safe_concat(*cols):
    return F.concat_ws("|", *[F.coalesce(F.col(c), F.lit("")) for c in cols])

# ---------------- SURROGATE KEYS ----------------

# Location Key
df = df.withColumn(
    "location_id",
    F.md5(safe_concat("state", "city", "zip"))
)

# Service Key
df = df.withColumn(
    "service_id",
    F.md5(safe_concat("issue_type", "method"))
)

# Date Key
df = df.withColumn("date", F.to_date("created_ts"))

df = df.withColumn(
    "date_id",
    F.date_format("date", "yyyyMMdd").cast("int")
)

# ---------------- DIMENSION TABLES ----------------

dim_location = df.select(
    "location_id", "state", "city", "zip"
).dropDuplicates()

dim_service = df.select(
    "service_id", "issue_type", "method"
).dropDuplicates()

dim_time = df.select(
    "date_id", "date"
).withColumn("year", F.year("date")) \
 .withColumn("month", F.month("date")) \
 .withColumn("day", F.dayofmonth("date")) \
 .dropDuplicates()

# ---------------- FACT TABLE ----------------

fact = df.select(
    "complaint_id",
    "location_id",
    "service_id",
    "date_id",
    "created_ts"
).repartition(4)  # reduce small files

# ---------------- WRITE ----------------

dim_location.write.mode("overwrite") \
    .parquet(f"{WAREHOUSE_PATH}dim_location")

dim_service.write.mode("overwrite") \
    .parquet(f"{WAREHOUSE_PATH}dim_service")

dim_time.write.mode("overwrite") \
    .parquet(f"{WAREHOUSE_PATH}dim_time")

fact.write.mode("overwrite") \
    .partitionBy("date_id") \
    .parquet(f"{WAREHOUSE_PATH}fact_complaints")

# ---------------- CLEANUP ----------------
df.unpersist()

print("✅ Star Schema Created Successfully")