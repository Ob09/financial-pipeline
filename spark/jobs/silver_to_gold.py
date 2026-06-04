from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType
from datetime import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SILVER_PATH = "/opt/airflow/data/silver"
GOLD_PATH   = "/opt/airflow/data/gold"


def create_spark_session():
    spark = SparkSession.builder \
        .appName("SilverToGold") \
        .master("local[*]") \
        .config("spark.sql.adaptive.enabled", "true") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def read_silver_data(spark):
    logger.info(f"Reading Silver data from {SILVER_PATH}")
    df = spark.read \
        .option("mergeSchema", "true") \
        .parquet(f"{SILVER_PATH}/*.parquet")
    logger.info(f"Silver row count: {df.count()}")
    return df


def calculate_indicators(df):
    logger.info("Calculating technical indicators")

    window_by_symbol = Window \
        .partitionBy("symbol") \
        .orderBy("date")

    window_7  = window_by_symbol.rowsBetween(-6,  0)
    window_30 = window_by_symbol.rowsBetween(-29, 0)
    window_14 = window_by_symbol.rowsBetween(-13, 0)

    df = df.withColumn("ma_7",
        F.avg(F.col("close")).over(window_7))

    df = df.withColumn("ma_30",
        F.avg(F.col("close")).over(window_30))

    df = df.withColumn("daily_return",
        (F.col("close") - F.lag("close", 1).over(window_by_symbol)) /
         F.lag("close", 1).over(window_by_symbol) * 100)

    df = df.withColumn("volatility_30",
        F.stddev(F.col("daily_return")).over(window_30))

    df = df.withColumn("gain",
        F.when(F.col("daily_return") > 0, F.col("daily_return")).otherwise(0))

    df = df.withColumn("loss",
        F.when(F.col("daily_return") < 0, F.abs(F.col("daily_return"))).otherwise(0))

    df = df.withColumn("avg_gain",
        F.avg(F.col("gain")).over(window_14))

    df = df.withColumn("avg_loss",
        F.avg(F.col("loss")).over(window_14))

    df = df.withColumn("rs",
        F.when(F.col("avg_loss") == 0, 100)
         .otherwise(F.col("avg_gain") / F.col("avg_loss")))

    df = df.withColumn("rsi",
        F.when(F.col("avg_loss") == 0, 100)
         .otherwise(100 - (100 / (1 + F.col("rs")))))

    df = df.drop("gain", "loss", "avg_gain", "avg_loss", "rs")

    df = df.withColumn("gold_timestamp",
        F.lit(datetime.now().isoformat()))

    return df


def write_gold_data(df):
    logger.info(f"Writing Gold data to {GOLD_PATH}")
    df.coalesce(1).write \
        .mode("overwrite") \
        .parquet(GOLD_PATH)
    logger.info("Gold layer write complete")


def run_silver_to_gold():
    spark = create_spark_session()
    try:
        df = read_silver_data(spark)
        df = calculate_indicators(df)
        write_gold_data(df)
        logger.info("Silver to Gold pipeline complete")
    except Exception as e:
        logger.error(f"Pipeline failed: {e}")
        raise
    finally:
        spark.stop()
        logger.info("Spark session stopped")


if __name__ == "__main__":
    run_silver_to_gold()