from fastapi import FastAPI, HTTPException, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
import duckdb
import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from pydantic import BaseModel
from pathlib import Path
import logging
import subprocess
import sys
import os
from datetime import timezone
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
    # Permitir override via Env Var para testeo local
    override = os.environ.get("DB_PATH_OVERRIDE")
    if override:
        return override
        
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

class TaskResponse(BaseModel):
    message: str
    task_id: str = "background"

def run_script(script_path: str):
    """Helper para ejecutar scripts de tools/ en background."""
    try:
        logging.info(f"🚀 Triggering background script: {script_path}")
        # Usamos sys.executable para garantizar que usamos el mismo venv
        subprocess.run([sys.executable, script_path], check=True)
        logging.info(f"✅ Script finished: {script_path}")
    except Exception as e:
        logging.error(f"❌ Script failed {script_path}: {e}")

# --- Endpoints ---

from fastapi.responses import RedirectResponse

@app.post("/api/v2/system/refresh-watchlist", response_model=TaskResponse)
def refresh_watchlist(background_tasks: BackgroundTasks):
    """Ejecuta tools/refresh_watchlist.py para actualizar candidatos sin bajar datos."""
    script = "tools/refresh_watchlist.py"
    if not Path(script).exists():
        raise HTTPException(status_code=500, detail="Tool not found")
    
    background_tasks.add_task(run_script, script)
    return {"message": "Watchlist refresh triggered in background"}

@app.post("/api/v2/system/recalc-indicators", response_model=TaskResponse)
def recalc_indicators(background_tasks: BackgroundTasks):
    """Ejecuta tools/recalc_indicators.py (Heavy Task)."""
    script = "tools/recalc_indicators.py"
    if not Path(script).exists():
        raise HTTPException(status_code=500, detail="Tool not found")
    
    background_tasks.add_task(run_script, script)
    return {"message": "Indicator recalculation triggered. This may take a while."}

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
    Retorna los candidatos de la dynamic_watchlist MÁS los holdings y watchlist manual.
    """
    try:
        cfg = load_settings()
        
        # 1. Obtener tickers de interés manual
        holdings = []
        for h in cfg.portfolios.holdings:
            if hasattr(h, 'ticker'): holdings.append(h.ticker)
            else: holdings.append(str(h))
        
        manual_watchlist = cfg.universe.watchlist
        all_manual = list(set(holdings + manual_watchlist))
        
        # 2. Construir la parte manual de la query solo si hay tickers
        manual_subquery = ""
        if all_manual:
            manual_tickers_sql = ",".join([f"('{t}')" for t in all_manual])
            manual_subquery = f"""
                UNION
                -- Tickers manuales que NO están en la dynamic_watchlist
                SELECT ticker, NULL as reason, now() as added_at
                FROM (VALUES {manual_tickers_sql}) AS t(ticker)
                WHERE ticker NOT IN (SELECT ticker FROM dynamic_watchlist WHERE expires_at > now())
            """
        
        query = f"""
            WITH all_targets AS (
                SELECT ticker, reason, added_at FROM dynamic_watchlist WHERE expires_at > now()
                {manual_subquery}
            ),
            latest_data AS (
                SELECT 
                    i.ticker,
                    i.rsi, 
                    i.adx, 
                    i.vol_k,
                    i.chg_pct,
                    o.close, 
                    row_number() OVER (PARTITION BY i.ticker ORDER BY i.timestamp DESC) as rn
                FROM indicators i
                JOIN ohlcv o USING (ticker, timeframe, timestamp)
                WHERE i.timeframe = '1d'
            )
            SELECT 
                t.ticker, 
                m.name, 
                COALESCE(t.reason, '') as strategies, 
                t.added_at,
                d.close, 
                d.chg_pct, 
                d.rsi, 
                d.adx, 
                d.vol_k
            FROM all_targets t
            LEFT JOIN ticker_metadata m ON t.ticker = m.ticker
            LEFT JOIN latest_data d ON t.ticker = d.ticker AND d.rn = 1
            ORDER BY 
                CASE WHEN t.reason IS NOT NULL THEN 0 ELSE 1 END,
                t.added_at DESC
        """
        df = query_db(query)
        if df.empty:
            return []
        
        # Flags de pertenencia
        df['is_holding'] = df['ticker'].isin(holdings)
        df['is_favourite'] = df['ticker'].isin(manual_watchlist)
        
        # FILTRO: Eliminar los que solo son SELL_STRENGTH y no son ni holding ni fav
        mask_to_remove = (df['strategies'] == 'SELL_STRENGTH') & (~df['is_holding']) & (~df['is_favourite'])
        df = df[~mask_to_remove]
        
        # LIMPIEZA NUCLEAR PARA JSON (V2):
        df = df.replace([np.inf, -np.inf], np.nan)
        # Forzar conversión a object para que acepte None
        df = df.astype(object)
        df = df.where(pd.notnull(df), None)
        
        # Convertir fechas
        df['added_at'] = df['added_at'].astype(str)
        return df.to_dict(orient="records")
    except Exception as e:
        logging.error(f"Error en get_screener_results: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v2/portfolio")
def get_portfolio():
    """
    Retorna las posiciones actuales del usuario con P&L calculado.
    """
    try:
        # 1. Query que une la vista de holdings con precios actuales
        query = """
            WITH latest_prices AS (
                SELECT ticker, close, 
                       row_number() OVER (PARTITION BY ticker ORDER BY timestamp DESC) as rn
                FROM ohlcv
                WHERE timeframe = '1d'
            )
            SELECT 
                h.ticker,
                h.qty,
                h.avg_buy_price,
                p.close as current_price,
                (p.close - h.avg_buy_price) * h.qty as pnl_val,
                ((p.close / h.avg_buy_price) - 1) * 100 as pnl_pct,
                m.name
            FROM view_portfolio_holdings h
            LEFT JOIN latest_prices p ON h.ticker = p.ticker AND p.rn = 1
            LEFT JOIN ticker_metadata m ON h.ticker = m.ticker
            ORDER BY pnl_val DESC
        """
        df = query_db(query)
        if df.empty:
            return []
            
        # Limpieza nuclear para JSON
        df = df.astype(object)
        df = df.where(pd.notnull(df), None)
        
        return df.to_dict(orient="records")
    except Exception as e:
        logging.error(f"Error en get_portfolio: {e}")
        raise HTTPException(status_code=500, detail=str(e))

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
            LIMIT 1500
        """
        df = query_db(q, [ticker, tf])
        
        if df.empty:
            continue
            
        # Reemplazar NaN con None para compatibilidad JSON
        df = df.replace({np.nan: None})
            
        # Ordenar ascendente para el gráfico
        df = df.sort_values("timestamp")
        
        # Obtener la última fila para KPIs (Bias, Phase, etc.)
        # IMPORTANTE: Definir antes del loop para evitar NameError
        last_row = df.iloc[-1]
        
        # Calcular Bias/Phase/Force (Lógica de Triple Screen)
        # Esto debería estar en DB idealmente, pero lo calculamos al vuelo por ahora
        bias = "neutral"
        if last_row['close'] is not None and last_row['ema_200'] is not None:
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
        
        seen_ts = set()
        for _, row in df.iterrows():
            # Asumimos que la DB guarda UTC naive.
            # Lo hacemos timezone-aware (UTC) antes de pedir el timestamp.
            # Esto evita que .timestamp() use la zona horaria local del servidor.
            ts_obj = row['timestamp'].replace(tzinfo=timezone.utc)
            ts = int(ts_obj.timestamp()) # Unix timestamp
            
            # 1. Evitar duplicados de tiempo (rompen Lightweight Charts)
            if ts in seen_ts:
                continue
            
            # 2. Filtrar velas rotas (faltan precios)
            if row['open'] is None or row['close'] is None or row['high'] is None or row['low'] is None:
                continue

            seen_ts.add(ts)
            
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
        # as_of: Convertimos a ISO format y agregamos Z para que JS sepa que es UTC
        as_of_str = last_row['timestamp'].isoformat()
        if not as_of_str.endswith("Z"):
            as_of_str += "Z"

        tf_data = {
            "as_of": as_of_str,
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
