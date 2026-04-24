import os

# 🔥 FIX: Ensure PySpark picks correct Java (important for your Docker setup)
os.environ["JAVA_HOME"] = "/usr/lib/jvm/default-java"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_timestamp, trim, upper,
    year, month, current_timestamp
)
from pyspark.sql.types import DoubleType

# ---------------- SPARK ----------------
spark = SparkSession.builder \
    .appName("telecom_silver_layer") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()

# ---------------- PATHS (Docker Compatible) ----------------
BRONZE_PATH = "/workspace/data_lake/bronze/telecom_complaints/"
SILVER_PATH = "/workspace/data_lake/silver/telecom_complaints/"

os.makedirs(SILVER_PATH, exist_ok=True)

# ---------------- READ ----------------
df = spark.read \
    .option("multiline", "true") \
    .json(f"{BRONZE_PATH}*.json")

# ---------------- ADD INGESTION TIME ----------------
df = df.withColumn("ingestion_time", current_timestamp())

# ---------------- CLEANING ----------------
df = df.filter(
    col("id").isNotNull() &
    col("issue_type").isNotNull() &
    col("state").isNotNull()
)

df = df.dropDuplicates(["id"])
df = df.replace("None", None)

# ---------------- TRANSFORM ----------------
df = df \
    .withColumn("complaint_id", col("id")) \
    .withColumn("created_ts", to_timestamp(col("ticket_created"))) \
    .withColumn("issue_date", to_timestamp(col("date_created"))) \
    .withColumn("issue_type", upper(trim(col("issue_type")))) \
    .withColumn("issue", upper(trim(col("issue")))) \
    .withColumn("method", upper(trim(col("method")))) \
    .withColumn("state", upper(trim(col("state")))) \
    .withColumn("city", upper(trim(col("city")))) \
    .withColumn("zip", col("zip")) \
    .withColumn("latitude", col("location_1.latitude").cast(DoubleType())) \
    .withColumn("longitude", col("location_1.longitude").cast(DoubleType()))

# ---------------- ADD PARTITIONS ----------------
df = df \
    .withColumn("year", year("created_ts")) \
    .withColumn("month", month("created_ts"))

# ---------------- SELECT FINAL ----------------
df_silver = df.select(
    "complaint_id",
    "created_ts",
    "issue_date",
    "issue_type",
    "issue",
    "method",
    "city",
    "state",
    "zip",
    "latitude",
    "longitude",
    "year",
    "month",
    "ingestion_time"
)

# ---------------- WRITE ----------------

# 🔥 Reduce small files
df_silver = df_silver.repartition(4)

df_silver.write \
    .mode("overwrite") \
    .partitionBy("year", "month") \
    .parquet(SILVER_PATH)

print("✅ Silver layer created successfully")