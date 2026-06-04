from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, LongType, StringType
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BRONZE_PATH = "/opt/airflow/data/bronze"
SILVER_PATH = "/opt/airflow/data/silver"


def create_spark_session():
    spark = SparkSession.builder \
        .appName("BronzeToSilver") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_bronze_data(spark):
    logger.info(f"Reading Bronze data from {BRONZE_PATH}")
    df = spark.read \
        .option("mergeSchema", "true") \
        .parquet(f"{BRONZE_PATH}/*.parquet")
    logger.info(f"Bronze row count: {df.count()}")
    return df


def clean_data(df):
    logger.info("Starting data cleaning")
    initial_count = df.count()
    df = df.filter(F.col("close").isNotNull())
    df = df.filter(F.col("close") > 0)
    df = df.dropDuplicates(["date", "symbol"])
    df = df.withColumn("open",   F.col("open").cast(DoubleType()))
    df = df.withColumn("high",   F.col("high").cast(DoubleType()))
    df = df.withColumn("low",    F.col("low").cast(DoubleType()))
    df = df.withColumn("close",  F.col("close").cast(DoubleType()))
    df = df.withColumn("volume", F.col("volume").cast(LongType()))
    df = df.withColumn("symbol", F.col("symbol").cast(StringType()))
    df = df.withColumn(
        "cleaned_timestamp",
        F.lit(datetime.now().isoformat())
    )
    final_count = df.count()
    logger.info(f"Rows before cleaning: {initial_count}")
    logger.info(f"Rows after cleaning: {final_count}")
    logger.info(f"Rows removed: {initial_count - final_count}")
    return df


def write_silver_data(df):
    logger.info(f"Writing Silver data to {SILVER_PATH}")
    df.coalesce(1).write \
        .mode("overwrite") \
        .parquet(SILVER_PATH)
    logger.info("Silver layer write complete")


def run_bronze_to_silver():
    spark = create_spark_session()
    try:
        df = read_bronze_data(spark)
        df = clean_data(df)
        write_silver_data(df)
        logger.info("Bronze to Silver pipeline complete")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    run_bronze_to_silver()