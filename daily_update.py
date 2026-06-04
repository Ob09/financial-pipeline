import yfinance as yf
import pandas as pd
import numpy as np
from supabase import create_client
import os
import logging
from datetime import datetime, timedelta
import math

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = os.getenv("TABLE_NAME", "gold_market_data")

STOCKS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "TSLA", "NVDA", "JPM", "JNJ", "V",
    "WMT", "PG", "MA", "UNH", "HD",
    "DIS", "BAC", "XOM", "PFE", "KO",
    "PEP", "NFLX", "ADBE", "CRM", "CSCO",
    "INTC", "VZ", "T", "MRK", "ABT"
]


def clean_value(v):
    if v is None:
        return None
    if isinstance(v, float):
        if math.isnan(v) or math.isinf(v):
            return None
    if isinstance(v, (np.floating, np.integer)):
        v = v.item()
    return v


def clean_record(record):
    return {k: clean_value(v) for k, v in record.items()}


def calculate_indicators(df):
    df = df.sort_values("date").reset_index(drop=True)

    df["ma_7"] = df["close"].rolling(window=7, min_periods=1).mean()
    df["ma_30"] = df["close"].rolling(window=30, min_periods=1).mean()
    df["daily_return"] = df["close"].pct_change() * 100
    df["volatility_30"] = df["daily_return"].rolling(window=30, min_periods=1).std()

    delta = df["close"].diff()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)
    avg_gain = gain.rolling(window=14, min_periods=1).mean()
    avg_loss = loss.rolling(window=14, min_periods=1).mean()
    rs = avg_gain / avg_loss.replace(0, float('nan'))
    df["rsi"] = 100 - (100 / (1 + rs))
    df["rsi"] = df["rsi"].fillna(100)

    return df


def fetch_and_update(symbol, supabase):
    try:
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=60)).strftime("%Y-%m-%d")

        df = yf.Ticker(symbol).history(start=start_date, end=end_date)

        if df.empty:
            logger.warning(f"{symbol}: No data returned")
            return False

        df.reset_index(inplace=True)
        df.columns = [col.lower().replace(" ", "_") for col in df.columns]
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).astype("datetime64[us]")
        df["symbol"] = symbol

        df = calculate_indicators(df)

        df["date"] = df["date"].dt.strftime("%Y-%m-%d")
        df["gold_timestamp"] = datetime.now().isoformat()
        df["cleaned_timestamp"] = datetime.now().isoformat()

        columns_to_keep = [
            "symbol", "date", "open", "high", "low", "close",
            "volume", "dividends", "stock_splits", "ma_7", "ma_30",
            "daily_return", "volatility_30", "rsi",
            "gold_timestamp", "cleaned_timestamp"
        ]

        existing_cols = [c for c in columns_to_keep if c in df.columns]
        df = df[existing_cols]

        records = [clean_record(r) for r in df.to_dict(orient="records")]

        supabase.table(TABLE_NAME).upsert(
            records,
            on_conflict="symbol,date"
        ).execute()

        logger.info(f"{symbol}: Updated {len(records)} rows")
        return True

    except Exception as e:
        logger.error(f"{symbol}: Failed — {e}")
        return False


def run_daily_update():
    logger.info("Starting daily update...")

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    successful = []
    failed = []

    for symbol in STOCKS:
        result = fetch_and_update(symbol, supabase)
        if result:
            successful.append(symbol)
        else:
            failed.append(symbol)

    logger.info(f"Daily update complete. Success: {len(successful)}, Failed: {len(failed)}")
    if failed:
        logger.warning(f"Failed: {failed}")


if __name__ == "__main__":
    run_daily_update()