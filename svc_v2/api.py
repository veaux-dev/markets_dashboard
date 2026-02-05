from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import duckdb
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from pathlib import Path
import logging
from svc_v2.config_loader import load_settings

# Configuración
logging.basicConfig(level=logging.INFO)
app = FastAPI(title="MarketDashboard V2 API")

# Montar archivos estáticos (Frontend)
# html=True permite que / vaya a index.html
app.mount("/static", StaticFiles(directory="static", html=True), name="static")

# CORS para permitir peticiones desde el frontend (mismo dominio o localhost)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_db_path():
    cfg = load_settings()
    return f"data/{cfg.system.db_filename}"

def query_db(query: str, params: list = None) -> pd.DataFrame:
    """Helper para consultar DuckDB en modo lectura."""
    try:
        # read_only=True es CLAVE para concurrencia con el Daemon
        with duckdb.connect(get_db_path(), read_only=True) as con:
            if params:
                return con.execute(query, params).df()
            return con.execute(query).df()
    except Exception as e:
        logging.error(f"DB Query Error: {e}")
        return pd.DataFrame()

# --- Modelos de Respuesta ---
class HealthCheck(BaseModel):
    status: str
    db_connected: bool

# --- Endpoints ---

from fastapi.responses import RedirectResponse

# ...

@app.get("/")
def root():
    return RedirectResponse(url="/static/index.html")

@app.get("/health", response_model=HealthCheck)
def health_check():
    """Verifica si la API y la DB están vivas."""
    try:
        df = query_db("SELECT 1")
        return {"status": "ok", "db_connected": not df.empty}
    except:
        return {"status": "error", "db_connected": False}

@app.get("/api/v2/screener")
def get_screener_results():
    """
    Retorna los candidatos actuales en la dynamic_watchlist 
    con sus últimos indicadores calculados (timeframe 1d).
    """
    query = """
        WITH latest_indicators AS (
            SELECT *, row_number() OVER (PARTITION BY ticker ORDER BY timestamp DESC) as rn
            FROM indicators
            WHERE timeframe = '1d'
        )
        SELECT 
            w.ticker, 
            m.name, 
            w.reason, 
            w.added_at,
            i.close, 
            i.chg_pct, 
            i.rsi, 
            i.adx, 
            i.vol_k
        FROM dynamic_watchlist w
        LEFT JOIN ticker_metadata m ON w.ticker = m.ticker
        LEFT JOIN latest_indicators i ON w.ticker = i.ticker AND i.rn = 1
        WHERE w.expires_at > now()
        ORDER BY w.added_at DESC
    """
    df = query_db(query)
    if df.empty:
        return []
    
    # Convertir fechas a string para JSON
    df['added_at'] = df['added_at'].astype(str)
    return df.to_dict(orient="records")

@app.get("/api/v2/ticker/{ticker}")
def get_ticker_details(ticker: str):
    """
    Devuelve la estructura completa para Triple Screen:
    {
        "ticker": "AAPL",
        "updated_at": "...",
        "timeframes": {
            "1d": { ...data... },
            "1h": { ...data... }
        }
    }
    """
    ticker = ticker.upper()
    
    # 1. Metadatos
    meta = query_db("SELECT name, updated_at FROM ticker_metadata WHERE ticker = ?", [ticker])
    name = meta.iloc[0]['name'] if not meta.empty else ticker
    updated_at = meta.iloc[0]['updated_at'] if not meta.empty else None

    response = {
        "ticker": ticker,
        "name": name,
        "updated_at": str(updated_at) if updated_at else None,
        "timeframes": {}
    }

    # 2. Datos por Timeframe (1d, 1h, 15m)
    # Adaptamos lo que el frontend espera
    timeframes = ["1d", "1h", "15m"]
    
    for tf in timeframes:
        # Recuperar últimas N velas con indicadores
        # Limitamos a 300 para no saturar el frontend
        q = f"""
            SELECT 
                timestamp, open, high, low, close, volume,
                ema_20, ema_50, ema_200,
                rsi, macd_hist, adx, vol_k,
                bb_upper, bb_lower,
                donchian_high as resistance, donchian_low as support,
                gap_pct, chg_pct
            FROM indicators 
            JOIN ohlcv USING (ticker, timeframe, timestamp)
            WHERE ticker = ? AND timeframe = ?
            ORDER BY timestamp DESC
            LIMIT 300
        """
        df = query_db(q, [ticker, tf])
        
        if df.empty:
            continue
            
        # Reemplazar NaN con None para compatibilidad JSON
        df = df.replace({np.nan: None})
            
        # Ordenar ascendente para el gráfico
        df = df.sort_values("timestamp")
        
        # Calcular Bias/Phase/Force (Lógica de Triple Screen)
        # Esto debería estar en DB idealmente, pero lo calculamos al vuelo por ahora
        last_row = df.iloc[-1]
        
        bias = "neutral"
        if last_row['close'] > last_row['ema_200']: bias = "buy"
        elif last_row['close'] < last_row['ema_200']: bias = "sell"
        
        # Estructurar series para lightweight-charts
        candles = []
        vol_series = []
        rsi_series = []
        macd_series = []
        ema_short = []
        ema_mid = []
        ema_long = []
        
        for _, row in df.iterrows():
            ts = int(row['timestamp'].timestamp()) # Unix timestamp
            
            candles.append({
                "time": ts,
                "open": row['open'],
                "high": row['high'],
                "low": row['low'],
                "close": row['close']
            })
            vol_series.append({"time": ts, "value": row['volume']})
            
            if pd.notnull(row['rsi']): rsi_series.append({"time": ts, "value": row['rsi']})
            if pd.notnull(row['macd_hist']): macd_series.append({"time": ts, "value": row['macd_hist']})
            
            if pd.notnull(row['ema_20']): ema_short.append({"time": ts, "value": row['ema_20']})
            if pd.notnull(row['ema_50']): ema_mid.append({"time": ts, "value": row['ema_50']})
            if pd.notnull(row['ema_200']): ema_long.append({"time": ts, "value": row['ema_200']})

        # Payload del timeframe
        tf_data = {
            "as_of": str(last_row['timestamp']),
            "bias": bias,
            "phase": "U2" if bias == "buy" else "D4", # Placeholder lógica simple
            "force": last_row.get('chg_pct'), 
            "rsi": last_row.get('rsi'),
            "adx": last_row.get('adx'),
            "macd_hist": last_row.get('macd_hist'),
            "volume": last_row['volume'],
            "ema_short_len": 20,
            "ema_mid_len": 50,
            "ema_long_len": 200,
            "series": {
                "candles": candles,
                "volume": vol_series,
                "rsi": rsi_series,
                "macd_hist": macd_series,
                "ema_short": ema_short,
                "ema_mid": ema_mid,
                "ema_long": ema_long
            }
        }
        
        response["timeframes"][tf] = tf_data

    if not response["timeframes"]:
        raise HTTPException(status_code=404, detail="Ticker not found or no data")

    return response

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
