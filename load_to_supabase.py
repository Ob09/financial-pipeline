import pandas as pd
import glob
import os
from supabase import create_client
from dotenv import load_dotenv
import logging
import math

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = os.getenv("TABLE_NAME", "gold_market_data")
GOLD_PATH = r"C:\Users\Obedh\Desktop\financial-pipeline\data\gold"


def clean_value(v):
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
    return v


def clean_record(record):
    return {k: clean_value(v) for k, v in record.items()}


def load_gold_to_supabase():
    logger.info("Connecting to Supabase...")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    logger.info("Reading Gold Parquet files...")
    parquet_files = glob.glob(f"{GOLD_PATH}/*.parquet")

    if not parquet_files:
        logger.error("No Gold Parquet files found")
        return

    dfs = [pd.read_parquet(f) for f in parquet_files]
    df = pd.concat(dfs, ignore_index=True)

    logger.info(f"Total rows to upload: {len(df)}")

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

    columns_to_keep = [
        "symbol", "date", "open", "high", "low", "close",
        "volume", "dividends", "stock_splits", "ma_7", "ma_30",
        "daily_return", "volatility_30", "rsi",
        "gold_timestamp", "cleaned_timestamp"
    ]

    existing_cols = [c for c in columns_to_keep if c in df.columns]
    df = df[existing_cols]

    records = [clean_record(r) for r in df.to_dict(orient="records")]

    batch_size = 500
    total_batches = len(records) // batch_size + 1

    for i in range(0, len(records), batch_size):
        batch = records[i:i + batch_size]
        batch_num = i // batch_size + 1
        logger.info(f"Uploading batch {batch_num}/{total_batches}...")
        supabase.table(TABLE_NAME).upsert(
            batch,
            on_conflict="symbol,date"
        ).execute()

    logger.info(f"Successfully uploaded {len(records)} rows to Supabase")


if __name__ == "__main__":
    load_gold_to_supabase()