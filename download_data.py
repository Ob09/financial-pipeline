import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta
import os

STOCKS = ['AAPL','MSFT','GOOGL','AMZN','META','TSLA','NVDA','JPM','JNJ','V','WMT','PG','MA','UNH','HD','DIS','BAC','XOM','PFE','KO','PEP','NFLX','ADBE','CRM','CSCO','INTC','VZ','T','MRK','ABT']
BRONZE_PATH = r'C:\Users\Obedh\Desktop\financial-pipeline\data\bronze'

end_date = datetime.now().strftime('%Y-%m-%d')
start_date = (datetime.now() - timedelta(days=730)).strftime('%Y-%m-%d')

for symbol in STOCKS:
    df = yf.Ticker(symbol).history(start=start_date, end=end_date)
    df.reset_index(inplace=True)
    df.columns = [col.lower().replace(' ', '_') for col in df.columns]
    df['symbol'] = symbol
    df['date'] = pd.to_datetime(df['date']).dt.tz_localize(None).astype('datetime64[us]')
    file_path = os.path.join(BRONZE_PATH, f'{symbol}.parquet')
    df.to_parquet(file_path, index=False, coerce_timestamps='us', allow_truncated_timestamps=True)
    print(f'Saved {symbol}: {len(df)} rows')

print('Done')