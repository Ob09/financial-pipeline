from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import Response
import os
from supabase import create_client
import orjson
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Financial Market Data API",
    description="Serves Gold layer data from Supabase",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
TABLE_NAME = os.getenv("TABLE_NAME", "gold_market_data")


def get_supabase():
    return create_client(SUPABASE_URL, SUPABASE_KEY)


def json_response(data):
    return Response(
        content=orjson.dumps(data),
        media_type="application/json"
    )


@app.get("/health")
def health_check():
    return json_response({"status": "healthy", "message": "Financial Market API is running"})


@app.get("/api/stocks")
def get_stocks():
    try:
        supabase = get_supabase()
        result = supabase.rpc("get_distinct_symbols").execute()
        symbols = sorted([r["symbol"] for r in result.data])
        return json_response({"stocks": symbols, "count": len(symbols)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stocks/{symbol}")
def get_stock_data(symbol: str):
    try:
        supabase = get_supabase()
        all_data = []
        offset = 0
        batch_size = 1000

        while True:
            result = supabase.table(TABLE_NAME)\
                .select("*")\
                .eq("symbol", symbol.upper())\
                .order("date")\
                .range(offset, offset + batch_size - 1)\
                .execute()
            if not result.data:
                break
            all_data.extend(result.data)
            if len(result.data) < batch_size:
                break
            offset += batch_size

        if not all_data:
            raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")

        return json_response({
            "symbol": symbol.upper(),
            "count": len(all_data),
            "data": all_data
        })
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/stocks/{symbol}/latest")
def get_latest_stock_data(symbol: str):
    try:
        supabase = get_supabase()
        result = supabase.table(TABLE_NAME)\
            .select("*")\
            .eq("symbol", symbol.upper())\
            .order("date", desc=True)\
            .limit(1)\
            .execute()
        if not result.data:
            raise HTTPException(status_code=404, detail=f"Symbol {symbol} not found")
        return json_response(result.data[0])
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/summary")
def get_summary():
    try:
        supabase = get_supabase()
        result = supabase.rpc("get_latest_per_symbol").execute()
        records = sorted(result.data, key=lambda x: x["symbol"])
        return json_response({"summary": records, "count": len(records)})
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))