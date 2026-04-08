from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp, trim, upper
from pyspark.sql.types import DoubleType
import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/default-java"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

# ---------------- SPARK ----------------
spark = SparkSession.builder \
    .appName("telecom_silver_layer") \
    .getOrCreate()

# ---------------- PATHS (Docker Compatible) ----------------
BRONZE_PATH = "/workspace/data_lake/bronze/telecom_complaints/"
SILVER_PATH = "/workspace/data_lake/silver/telecom_complaints/"

os.makedirs(SILVER_PATH, exist_ok=True)

# ---------------- READ ----------------
df = spark.read \
    .option("multiline", "true") \
    .json(f"{BRONZE_PATH}*.json")

print("=== Bronze Schema ===")
df.printSchema()

# ---------------- CLEANING ----------------
df_clean = df.filter(
    col("id").isNotNull() &
    col("issue_type").isNotNull() &
    col("state").isNotNull()
)

# Remove duplicates
df_clean = df_clean.dropDuplicates(["id"])

# Replace string "None" with null
df_clean = df_clean.replace("None", None)

# ---------------- TRANSFORM ----------------
df_transformed = df_clean \
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

# ---------------- SELECT FINAL ----------------
df_silver = df_transformed.select(
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
    "longitude"
)

print("=== Silver Data Preview ===")
df_silver.show(10, truncate=False)

# ---------------- WRITE ----------------
df_silver.write \
    .mode("overwrite") \
    .partitionBy("state") \
    .parquet(SILVER_PATH)

print("[SUCCESS] Silver layer created successfully")