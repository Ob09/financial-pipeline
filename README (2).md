# Financial Market Data Pipeline

> A complete end-to-end data engineering portfolio project using Docker, Apache Airflow, Apache Spark, FastAPI, and Plotly Dash.

[![GitHub Actions](https://img.shields.io/badge/Automation-GitHub%20Actions-2088FF)](https://github.com/Ob09/financial-pipeline)
[![Docker](https://img.shields.io/badge/Docker-Containerised-2496ED)](https://www.docker.com/)
[![Airflow](https://img.shields.io/badge/Orchestration-Apache%20Airflow-017CEE)](https://airflow.apache.org/)
[![Spark](https://img.shields.io/badge/Processing-Apache%20Spark-E25A1C)](https://spark.apache.org/)
[![Supabase](https://img.shields.io/badge/Database-Supabase-3ECF8E)](https://supabase.com/)

---

## Live Demo

| Service | URL |
|---------|-----|
| Dashboard | https://financial-pipeline.onrender.com |
| API | https://financial-pipeline-seven.vercel.app |
| API Docs | https://financial-pipeline-seven.vercel.app/docs |
| API Health | https://financial-pipeline-seven.vercel.app/health |

---

## What This Project Does

Every weekday at 6am, an automated pipeline:

1. Fetches daily stock prices for **30 major US stocks** from Yahoo Finance
2. Stores raw data as **Parquet files** in the Bronze layer
3. Cleans and validates data with **Apache Spark** → Silver layer
4. Calculates **technical indicators** (Moving Averages, RSI, Volatility) → Gold layer
5. Writes results to **Supabase** (cloud PostgreSQL)
6. Serves data via **FastAPI** on Vercel
7. Displays interactive charts on **Plotly Dash** on Render

---

## Architecture

```
┌─────────────────────────────────────────────────────┐
│              LOCAL ENVIRONMENT (Docker)              │
│                                                      │
│  Airflow ──triggers──▶ Spark                        │
│                         │                           │
│                    Bronze Parquet                   │
│                         │                           │
│                    Silver Parquet                   │
│                         │                           │
│                    Gold Parquet                     │
└──────────────────────────────────────────────────────┘
                          │
                  load_to_supabase.py
                          │
                          ▼
┌─────────────────────────────────────────────────────┐
│                    CLOUD                             │
│                                                      │
│  GitHub Actions ──daily 6am──▶ Supabase             │
│                                    │                │
│                             FastAPI (Vercel)         │
│                                    │                │
│                          Dash Dashboard (Render)     │
└──────────────────────────────────────────────────────┘
```

### Medallion Architecture

| Layer | Description | Format |
|-------|-------------|--------|
| Bronze | Raw data exactly as received from Yahoo Finance | Parquet |
| Silver | Cleaned, validated, deduplicated data | Parquet |
| Gold | Enriched with MA7, MA30, RSI, Volatility | Parquet + Supabase |

---

## Technology Stack

| Tool | Purpose |
|------|---------|
| Docker | Run Airflow and Spark on Windows via Linux containers |
| Docker Compose | Orchestrate 6 containers together |
| Apache Airflow 2.9 | Schedule and monitor the data pipeline |
| Apache Spark 3.5 / PySpark | Distributed data processing |
| yfinance | Free stock price data from Yahoo Finance |
| Parquet | Columnar storage format for the data lake |
| Supabase | Free hosted PostgreSQL database |
| FastAPI | REST API serving Gold layer data |
| Plotly Dash | Interactive dashboard |
| GitHub Actions | Automated daily data updates |
| Vercel | FastAPI hosting (serverless) |
| Render | Dash dashboard hosting (persistent server) |

---

## Stocks Tracked

```
AAPL  MSFT  GOOGL AMZN  META  TSLA  NVDA  JPM   JNJ   V
WMT   PG    MA    UNH   HD    DIS   BAC   XOM   PFE   KO
PEP   NFLX  ADBE  CRM   CSCO  INTC  VZ    T     MRK   ABT
```

---

## Technical Indicators Calculated

### Moving Averages (MA7 and MA30)
Smooths out daily price noise to show trend direction.
- **MA7** (7-day): Reacts quickly to recent price changes
- **MA30** (30-day): Shows the longer-term trend
- **Golden Cross**: MA7 crosses above MA30 → bullish signal
- **Death Cross**: MA7 crosses below MA30 → bearish signal

### RSI — Relative Strength Index
Measures momentum on a 0-100 scale.
- **RSI > 70**: Overbought — may drop soon
- **RSI < 30**: Oversold — may rise soon
- **RSI ≈ 50**: Neutral momentum

### 30-Day Volatility
Standard deviation of daily returns over 30 days.
- **High volatility**: Large price swings — risky
- **Low volatility**: Stable, predictable movement

### Daily Return
Percentage price change from previous day.
```
Daily Return = (Today's Close - Yesterday's Close) / Yesterday's Close × 100
```

---

## Project Structure

```
financial-pipeline/
├── .env                          # Credentials (never commit)
├── .gitignore
├── docker-compose.yml            # All 6 services
├── daily_update.py               # GitHub Actions script
├── load_to_supabase.py           # Initial data load script
├── download_data.py              # Local download workaround
│
├── airflow/
│   ├── Dockerfile
│   └── dags/
│       └── market_pipeline.py   # DAG definition
│
├── spark/
│   └── jobs/
│       ├── bronze_to_silver.py  # PySpark cleaning
│       └── silver_to_gold.py   # PySpark indicators
│
├── ingestion/
│   └── fetch_stocks.py          # yfinance ingestion
│
├── api/
│   ├── main.py                  # FastAPI application
│   ├── requirements.txt
│   └── vercel.json
│
├── dashboard/
│   ├── app.py                   # Plotly Dash application
│   └── requirements.txt
│
├── data/                        # Local data lake (gitignored)
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
└── .github/
    └── workflows/
        └── daily_update.yml     # Automated schedule
```

---

## Quick Start

### Prerequisites
- Windows 11 with WSL2
- Docker Desktop
- VS Code
- Anaconda Python

### 1. Clone and Setup

```bash
git clone https://github.com/Ob09/financial-pipeline.git
cd financial-pipeline
```

Create `.env` file:
```env
AIRFLOW__CORE__EXECUTOR=LocalExecutor
AIRFLOW__CORE__FERNET_KEY=ZmDfcTF7_60GrrY167zsiPd67pEvs0aGOv2oasOM1Pg=
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__WEBSERVER__SECRET_KEY=financial_pipeline_2024
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres:5432/airflow
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
SUPABASE_URL=https://your-project.supabase.co
SUPABASE_KEY=your-service-role-key
TABLE_NAME=gold_market_data
```

### 2. Start Docker

```bash
docker compose up --build
```

Wait for:
```
airflow-webserver | Listening at: http://0.0.0.0:8080
```

### 3. Access the System

| Interface | URL | Credentials |
|-----------|-----|-------------|
| Airflow UI | http://localhost:8080 | admin / admin |
| Dashboard | http://localhost:8050 | — |
| API | http://localhost:8000 | — |
| Spark UI | http://localhost:8081 | — |

### 4. Download Initial Data

```bash
# In Anaconda Prompt:
python download_data.py
```

### 5. Run Spark Pipeline

```bash
# Bronze to Silver
docker exec financial-pipeline-airflow-scheduler-1 \
  spark-submit /opt/spark/jobs/bronze_to_silver.py

# Silver to Gold
docker exec financial-pipeline-airflow-scheduler-1 \
  spark-submit /opt/spark/jobs/silver_to_gold.py
```

### 6. Load to Supabase

```bash
pip install supabase python-dotenv
python load_to_supabase.py
```

### 7. Stop Everything

```bash
docker compose down
```

---

## API Reference

### GET /health
```json
{"status": "healthy", "message": "Financial Market API is running"}
```

### GET /api/stocks
```json
{"stocks": ["AAPL", "ABT", "ADBE", ...], "count": 30}
```

### GET /api/stocks/{symbol}
```json
{
  "symbol": "AAPL",
  "count": 501,
  "data": [
    {
      "date": "2024-06-03",
      "open": 194.03,
      "close": 194.35,
      "ma_7": 189.45,
      "ma_30": 186.72,
      "rsi": 62.4,
      "volatility_30": 0.95,
      "daily_return": 0.17
    }
  ]
}
```

### GET /api/stocks/{symbol}/latest
Returns the most recent row for the given stock symbol.

### GET /api/summary
Returns the latest data row for all 30 stocks.

---

## Deployment

### FastAPI → Vercel

```json
// api/vercel.json
{
  "version": 2,
  "builds": [{"src": "main.py", "use": "@vercel/python"}],
  "routes": [{"src": "/(.*)", "dest": "main.py"}]
}
```

1. Go to vercel.com → Add New Project
2. Select `financial-pipeline` repository
3. Set Root Directory to `api`
4. Add environment variables: `SUPABASE_URL`, `SUPABASE_KEY`, `TABLE_NAME`
5. Deploy

### Dashboard → Render

1. Go to render.com → New Web Service
2. Select `financial-pipeline` repository
3. Set Root Directory to `dashboard`
4. Set Language to Python
5. Build Command: `pip install -r requirements.txt`
6. Start Command: `python app.py`
7. Deploy

### GitHub Actions Automation

1. Go to GitHub repo → Settings → Secrets → Actions
2. Add secrets: `SUPABASE_URL`, `SUPABASE_KEY`, `TABLE_NAME`
3. The workflow runs automatically every weekday at 6am UTC
4. Trigger manually: Actions tab → Daily Stock Data Update → Run workflow

---

## Challenges Encountered

### Yahoo Finance Rate Limiting
**Problem**: Docker container IP was blocked after hundreds of requests during debugging.
**Solution**: Downloaded data via local Anaconda Python as a workaround. Production GitHub Actions uses fresh IPs and only makes 30 requests/day.

### Parquet Timestamp Compatibility
**Problem**: Local pyarrow wrote nanosecond timestamps; Spark 3.5 only supports microseconds.
**Solution**: Added `coerce_timestamps='us'` and `allow_truncated_timestamps=True` to all Parquet writes.

### Supabase 1000-Row Limit
**Problem**: Supabase API returns max 1000 rows per request, preventing full dataset queries.
**Solution**: Created PostgreSQL functions (`get_distinct_symbols()`, `get_latest_per_symbol()`) that execute in the database and return only the needed data.

### Dash on Vercel
**Problem**: Plotly Dash requires a persistent server process; Vercel is serverless.
**Solution**: FastAPI on Vercel (stateless, perfect for APIs), Dash on Render (persistent server, perfect for Dash).

---

## What I Learned

- Docker is not just for deployment — it solves the 'works on my machine' problem
- Airflow's DAG structure enforces data quality by preventing downstream tasks from running on bad data
- Spark is right for bulk historical processing; pandas is right for small daily incremental updates
- The Medallion Architecture separates raw, clean, and enriched data with clear boundaries
- Choosing the right hosting platform for each service matters more than using one platform for everything

---

## Future Improvements

- [ ] Add real-time data using Alpaca free tier API
- [ ] Add portfolio simulation feature
- [ ] Deploy pipeline to cloud VM (DigitalOcean/GCP) for full automation
- [ ] Add email/Slack alerts for RSI overbought/oversold signals
- [ ] Add more technical indicators (MACD, Bollinger Bands)
- [ ] Add stock comparison feature (overlay multiple stocks)

---

## Author

**MSc Data Analytics Graduate**
Dublin, Ireland | June 2026

Second portfolio project — focused on Docker, Airflow, and Spark.
First project: E-commerce BI Platform using Olist dataset with dbt, Great Expectations, and Render.

---

*Built with real tools. Real data. Real deployment.*
