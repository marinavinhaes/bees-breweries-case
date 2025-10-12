# src/spark_transform.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, trim, lower, when, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import os
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

def create_spark(app_name="openbrewery_transform"):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.shuffle.partitions", "8") \
        .getOrCreate()
    return spark

def read_bronze(spark, bronze_dir):
    # read all JSON files in bronze
    df = spark.read.json(f"{bronze_dir}/*.json")
    return df

def transform_to_silver(df):
    # select and normalize important fields
    df2 = df.select(
        col("id").alias("id"),
        trim(col("name")).alias("name"),
        trim(col("brewery_type")).alias("brewery_type"),
        trim(col("street")).alias("street"),
        trim(col("city")).alias("city"),
        trim(col("state")).alias("state"),
        trim(col("postal_code")).alias("postal_code"),
        trim(col("country")).alias("country"),
        trim(col("longitude")).alias("longitude"),
        trim(col("latitude")).alias("latitude"),
        trim(col("phone")).alias("phone"),
        trim(col("website_url")).alias("website_url"),
        col("updated_at").alias("updated_at")
    )

    # Basic cleaning: fill missing states with "UNKNOWN"
    df2 = df2.withColumn("state", when(col("state").isNull() | (col("state") == ""), "UNKNOWN").otherwise(col("state")))
    # Normalize brewery_type to lower-case
    df2 = df2.withColumn("brewery_type", lower(col("brewery_type")))
    return df2

def write_silver(df, silver_dir):
    # write partitioned by state
    os.makedirs(silver_dir, exist_ok=True)
    df.write.mode("overwrite").partitionBy("state").parquet(silver_dir)
    logger.info(f"Wrote silver parquet to {silver_dir}")

def write_gold(df_silver, gold_dir):
    # aggregated: count per state and brewery type
    agg = df_silver.groupBy("state", "brewery_type").agg(count("*").alias("brewery_count"))
    os.makedirs(gold_dir, exist_ok=True)
    agg.write.mode("overwrite").parquet(gold_dir)
    logger.info(f"Wrote gold parquet to {gold_dir}")

def run_all(bronze_dir, silver_dir, gold_dir):
    spark = create_spark()
    df_bronze = read_bronze(spark, bronze_dir)
    df_silver = transform_to_silver(df_bronze)
    write_silver(df_silver, silver_dir)
    # Read silver back for aggregation (or reuse df_silver)
    write_gold(df_silver, gold_dir)
    spark.stop()

if __name__ == "__main__":
    BRONZE_DIR = os.getenv("BRONZE_DIR", "/data/lake/bronze")
    SILVER_DIR = os.getenv("SILVER_DIR", "/data/lake/silver")
    GOLD_DIR = os.getenv("GOLD_DIR", "/data/lake/gold")

    run_all(bronze_dir=BRONZE_DIR, silver_dir=SILVER_DIR, gold_dir=GOLD_DIR)