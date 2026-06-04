import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

STOCKS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META",
    "TSLA", "NVDA", "JPM", "JNJ", "V",
    "WMT", "PG", "MA", "UNH", "HD",
    "DIS", "BAC", "XOM", "PFE", "KO",
    "PEP", "NFLX", "ADBE", "CRM", "CSCO",
    "INTC", "VZ", "T", "MRK", "ABT"
]

BRONZE_PATH = "/opt/airflow/data/bronze"


def fetch_stock_data(symbol: str, start_date: str, end_date: str) -> pd.DataFrame:
    logger.info(f"Fetching data for {symbol}")

    ticker = yf.Ticker(symbol)
    df = ticker.history(start=start_date, end=end_date)

    df.reset_index(inplace=True)

    df.columns = [col.lower().replace(" ", "_") for col in df.columns]

    df["date"] = pd.to_datetime(df["date"]).dt.tz_localize(None).astype("datetime64[us]")

    df["symbol"] = symbol
    df["ingestion_timestamp"] = datetime.now().isoformat()

    return df


def save_to_bronze(df: pd.DataFrame, symbol: str) -> None:
    os.makedirs(BRONZE_PATH, exist_ok=True)

    file_path = os.path.join(BRONZE_PATH, f"{symbol}.parquet")

    df.to_parquet(
        file_path,
        index=False,
        coerce_timestamps='us',
        allow_truncated_timestamps=True
    )

    logger.info(f"Saved {symbol} to {file_path}")


def run_ingestion():
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")

    logger.info(f"Starting ingestion from {start_date} to {end_date}")

    successful = []
    failed = []

    for symbol in STOCKS:
        try:
            df = fetch_stock_data(symbol, start_date, end_date)
            if len(df) == 0:
                logger.warning(f"{symbol}: No data returned, skipping")
                failed.append(symbol)
                continue
            save_to_bronze(df, symbol)
            successful.append(symbol)
        except Exception as e:
            logger.error(f"Failed to fetch {symbol}: {e}")
            failed.append(symbol)

    logger.info(f"Ingestion complete. Success: {len(successful)}, Failed: {len(failed)}")

    if failed:
        logger.warning(f"Failed stocks: {failed}")


if __name__ == "__main__":
    run_ingestion()