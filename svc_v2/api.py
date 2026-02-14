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
from svc_v2.db import Database

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
    # 1. Prioridad: Env Var (Override local/dev)
    override = os.environ.get("DB_PATH_OVERRIDE")
    if override:
        return override
        
    # 2. Prioridad: Ruta de producción conocida (NAS)
    prod_path = "/mnt/Data/Markets/Dashboard/data_v2/markets.duckdb"
    if Path(prod_path).exists():
        return prod_path

    # 3. Fallback: Configuración de settings o default relativo
    try:
        cfg = load_settings()
        config_path = f"data/{cfg.system.db_filename}"
        if Path(config_path).exists():
            return config_path
    except:
        pass

    return "data/markets.duckdb"

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

# --- Modelos de Datos ---
class HealthCheck(BaseModel):
    status: str
    db_connected: bool

class TaskResponse(BaseModel):
    message: str
    task_id: str = "background"

class TransactionCreate(BaseModel):
    ticker: str
    side: str  # 'BUY', 'SELL', 'DIVIDEND', 'SPLIT'
    qty: float
    price: float
    fees: Optional[float] = 0.0
    currency: Optional[str] = "MXN"
    notes: Optional[str] = None
    timestamp: Optional[str] = None # YYYY-MM-DD HH:MM:SS

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

# --- Portfolio CRUD ---

@app.get("/api/v2/portfolio/transactions")
def get_transactions(limit: int = 100):
    """Retorna el historial de transacciones."""
    query = f"SELECT * FROM portfolio_transactions ORDER BY timestamp DESC LIMIT {limit}"
    df = query_db(query)
    if df.empty:
        return []
    # Limpieza para JSON
    df = df.replace([np.inf, -np.inf], np.nan)
    df = df.astype(object)
    df = df.where(pd.notnull(df), None)
    return df.to_dict(orient="records")

@app.post("/api/v2/portfolio/transaction")
def add_transaction(tx: TransactionCreate):
    """Registra una nueva transacción en la DB."""
    try:
        db = Database(get_db_path())
        db.add_transaction(
            ticker=tx.ticker.upper(),
            side=tx.side.upper(),
            qty=tx.qty,
            price=tx.price,
            fees=tx.fees,
            notes=tx.notes,
            timestamp=tx.timestamp,
            currency=tx.currency.upper()
        )
        return {"status": "success", "message": f"Transaction recorded for {tx.ticker}"}
    except Exception as e:
        logging.error(f"Error adding transaction: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.delete("/api/v2/portfolio/transaction/{tx_id}")
def delete_transaction(tx_id: int):
    """Elimina una transacción por ID."""
    try:
        # Aquí abrimos conexión para escritura (no usamos query_db que es readonly)
        with duckdb.connect(get_db_path()) as con:
            con.execute("DELETE FROM portfolio_transactions WHERE id = ?", [tx_id])
        return {"status": "success", "message": f"Transaction {tx_id} deleted"}
    except Exception as e:
        logging.error(f"Error deleting transaction: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v2/screener")
def get_screener_results():
    """
    Retorna los candidatos de la dynamic_watchlist MÁS los holdings y watchlist manual.
    Incluye variaciones de precio multitemporales (1D, 2D, 3D, vs Viernes Ant).
    """
    try:
        cfg = load_settings()
        
        # 1. Obtener tickers de interés manual
        holdings = [h.ticker if hasattr(h, 'ticker') else str(h) for h in cfg.portfolios.holdings]
        manual_watchlist = cfg.universe.watchlist
        all_manual = list(set(holdings + manual_watchlist))
        
        # 2. Construir la parte manual de la query
        manual_subquery = ""
        if all_manual:
            manual_tickers_sql = ",".join([f"('{t}')" for t in all_manual if t])
            if manual_tickers_sql:
                manual_subquery = f"""
                    UNION
                    SELECT ticker, NULL as reason, now() as added_at
                    FROM (VALUES {manual_tickers_sql}) AS t(ticker)
                    WHERE ticker NOT IN (SELECT ticker FROM dynamic_watchlist WHERE expires_at > now())
                """
        
        # Query Avanzada: Window functions para deltas
        query = f"""
            WITH all_targets AS (
                SELECT ticker, reason, added_at FROM dynamic_watchlist WHERE expires_at > now()
                {manual_subquery}
            ),
            price_history AS (
                SELECT 
                    ticker, timestamp, close,
                    lag(close, 1) OVER (PARTITION BY ticker ORDER BY timestamp ASC) as prev_1,
                    lag(close, 2) OVER (PARTITION BY ticker ORDER BY timestamp ASC) as prev_2,
                    lag(close, 3) OVER (PARTITION BY ticker ORDER BY timestamp ASC) as prev_3,
                    -- Buscar el último viernes: dayofweek=5 es Friday
                    FIRST_VALUE(close) OVER (
                        PARTITION BY ticker 
                        ORDER BY CASE WHEN dayofweek(timestamp) = 5 THEN 0 ELSE 1 END, timestamp DESC
                    ) as last_friday_close,
                    row_number() OVER (PARTITION BY ticker ORDER BY timestamp DESC) as rn
                FROM ohlcv
                WHERE timeframe = '1d'
            ),
            latest_ind AS (
                SELECT 
                    ticker, rsi, adx, vol_k,
                    row_number() OVER (PARTITION BY ticker ORDER BY timestamp DESC) as rn
                FROM indicators
                WHERE timeframe = '1d'
            )
            SELECT 
                t.ticker, 
                m.name, 
                COALESCE(t.reason, '') as strategies, 
                t.added_at,
                p.close, 
                ((p.close / NULLIF(p.prev_1, 0)) - 1) * 100 as chg_1d,
                ((p.close / NULLIF(p.prev_2, 0)) - 1) * 100 as chg_2d,
                ((p.close / NULLIF(p.prev_3, 0)) - 1) * 100 as chg_3d,
                ((p.close / NULLIF(p.last_friday_close, 0)) - 1) * 100 as chg_fri,
                i.rsi, 
                i.adx, 
                i.vol_k
            FROM all_targets t
            LEFT JOIN ticker_metadata m ON t.ticker = m.ticker
            LEFT JOIN price_history p ON t.ticker = p.ticker AND p.rn = 1
            LEFT JOIN latest_ind i ON t.ticker = i.ticker AND i.rn = 1
            ORDER BY 
                CASE WHEN t.reason IS NOT NULL AND t.reason != '' THEN 0 ELSE 1 END,
                t.ticker ASC
        """
        df = query_db(query)
        if df.empty:
            return []
        
        # Flags
        df['is_holding'] = df['ticker'].isin(holdings)
        df['is_favourite'] = df['ticker'].isin(manual_watchlist)
        
        # Limpieza
        if 'added_at' in df.columns:
            df['added_at'] = df['added_at'].astype(str)

        df = df.replace([np.inf, -np.inf], np.nan)
        df = df.astype(object)
        df = df.where(pd.notnull(df), None)
        
        return df.to_dict(orient="records")
    except Exception as e:
        logging.error(f"Error en get_screener_results: {e}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v2/portfolio")
def get_portfolio():
    """
    Retorna las posiciones actuales del usuario con P&L calculado (Lógica FIFO en memoria).
    """
    try:
        # 1. Obtener datos base: FX y Transacciones Crudas
        fx_df = query_db("SELECT close FROM ohlcv WHERE ticker = 'USDMXN=X' AND timeframe = '1d' ORDER BY timestamp DESC LIMIT 1")
        fx_rate = fx_df.iloc[0]['close'] if not fx_df.empty else 20.0
        
        # Leemos TODAS las transacciones ordenadas cronológicamente
        tx_df = query_db("SELECT ticker, side, qty, price, currency FROM portfolio_transactions ORDER BY timestamp ASC, id ASC")
        
        if tx_df.empty:
            return {"items": [], "totals": {}}

        # 2. Algoritmo FIFO para calcular Holdings y Avg Price
        portfolio_state = {}  # {ticker: {'batches': [{'qty': float, 'price': float}], 'currency': str}}

        for _, row in tx_df.iterrows():
            ticker = row['ticker']
            side = row['side']
            qty = float(row['qty'])
            price = float(row['price'])
            curr = row['currency']

            if ticker not in portfolio_state:
                portfolio_state[ticker] = {'batches': [], 'currency': curr}

            if side == 'BUY':
                portfolio_state[ticker]['batches'].append({'qty': qty, 'price': price})
                # Actualizamos moneda por si acaso (asumimos consistencia por ticker)
                portfolio_state[ticker]['currency'] = curr
            
            elif side == 'SELL':
                qty_to_sell = qty
                batches = portfolio_state[ticker]['batches']
                
                while qty_to_sell > 0 and batches:
                    current_batch = batches[0]
                    
                    if current_batch['qty'] <= qty_to_sell:
                        # Consumimos todo el lote
                        qty_to_sell -= current_batch['qty']
                        batches.pop(0)
                    else:
                        # Consumimos parcial
                        current_batch['qty'] -= qty_to_sell
                        qty_to_sell = 0
        
        # 3. Reconstruir DataFrame de Holdings Activos
        active_holdings = []
        for ticker, data in portfolio_state.items():
            batches = data['batches']
            total_qty = sum(b['qty'] for b in batches)
            
            if total_qty > 0.00001:  # Filtrar posiciones cerradas
                total_cost = sum(b['qty'] * b['price'] for b in batches)
                avg_price = total_cost / total_qty
                
                active_holdings.append({
                    'ticker': ticker,
                    'qty': total_qty,
                    'avg_buy_price': avg_price,
                    'currency': data['currency']
                })

        if not active_holdings:
            return {"items": [], "totals": {}}

        h_df = pd.DataFrame(active_holdings)

        # 4. Enriquecer con precios actuales y metadatos (Query Optimizado)
        tickers_list = h_df['ticker'].unique().tolist()
        if not tickers_list:
             return {"items": [], "totals": {}}

        # Escapar tickers para SQL
        tickers_sql = ",".join([f"'{t}'" for t in tickers_list])

        # Precios actuales
        latest_prices = query_db(f"""
            SELECT ticker, close as current_price, 
                   row_number() OVER (PARTITION BY ticker ORDER BY timestamp DESC) as rn
            FROM ohlcv
            WHERE timeframe = '1d' AND ticker IN ({tickers_sql})
        """)
        # Filtrar solo el último precio
        latest_prices = latest_prices[latest_prices['rn'] == 1][['ticker', 'current_price']]

        # Señales activas
        active_signals = query_db(f"""
            SELECT ticker, reason as strategies
            FROM dynamic_watchlist
            WHERE expires_at > now() AND ticker IN ({tickers_sql})
        """)

        # Metadatos
        meta_df = query_db(f"SELECT ticker, name FROM ticker_metadata WHERE ticker IN ({tickers_sql})")

        # Merges (Left Join para mantener todos los holdings aunque falten datos)
        # Usamos suffixes para evitar colisiones si hubiera columnas repetidas
        df = h_df.merge(latest_prices, on='ticker', how='left')
        df = df.merge(active_signals, on='ticker', how='left')
        df = df.merge(meta_df, on='ticker', how='left')
        
        # Limpieza final
        if 'strategies' in df.columns:
            df['strategies'] = df['strategies'].fillna('')
        else:
            df['strategies'] = ''
            
        df = df.sort_values('ticker')

        # Cálculo de P&L y Totales
        items = []
        total_mxn_inv = 0
        total_mxn_val = 0
        total_usd_inv = 0
        total_usd_val = 0
        
        for _, row in df.iterrows():
            qty = float(row['qty'])
            avg = float(row['avg_buy_price'])
            curr_price = float(row['current_price']) if pd.notnull(row.get('current_price')) else avg
            currency = row['currency']
            
            invested = qty * avg
            current_val = qty * curr_price
            pnl_val = current_val - invested
            pnl_pct = (pnl_val / invested * 100) if invested > 0 else 0
            
            item = row.to_dict()
            # Asegurar que el diccionario tenga todos los campos esperados por el frontend
            item['current_price'] = curr_price
            item['pnl_val'] = pnl_val
            item['pnl_pct'] = pnl_pct
            item['invested'] = invested
            item['current_val'] = current_val
            
            # Limpieza NaN para JSON
            for k, v in item.items():
                if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
                    item[k] = None
            
            items.append(item)
            
            # Totales
            if currency == 'MXN':
                total_mxn_inv += invested
                total_mxn_val += current_val
            else:
                total_usd_inv += invested
                total_usd_val += current_val

        # Gran Total Estimado en MXN
        est_total_inv = total_mxn_inv + (total_usd_inv * fx_rate)
        est_total_val = total_mxn_val + (total_usd_val * fx_rate)
        est_pnl_val = est_total_val - est_total_inv
        est_pnl_pct = (est_pnl_val / est_total_inv * 100) if est_total_inv > 0 else 0

        totals = {
            "mxn": {
                "invested": total_mxn_inv,
                "current": total_mxn_val,
                "pnl": total_mxn_val - total_mxn_inv
            },
            "usd": {
                "invested": total_usd_inv,
                "current": total_usd_val,
                "pnl": total_usd_val - total_usd_inv
            },
            "grand_total_mxn": {
                "invested": est_total_inv,
                "current": est_total_val,
                "pnl": est_pnl_val,
                "pnl_pct": est_pnl_pct,
                "fx_rate": fx_rate
            }
        }
        
        return {"items": items, "totals": totals}
    except Exception as e:
        logging.error(f"Error en get_portfolio: {e}")
        # En prod, imprimir el stacktrace ayuda
        import traceback
        traceback.print_exc()
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
                donchian_high, donchian_low,
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
        donchian_h = []
        donchian_l = []
        
        seen_ts = set()
        for _, row in df.iterrows():
            # Manejo de tiempo según timeframe
            if tf == '1d':
                # Para 1D, usamos string YYYY-MM-DD para evitar problemas de timezone
                # DuckDB timestamp -> Date string
                time_val = row['timestamp'].strftime('%Y-%m-%d')
                # Clave única
                ts_check = time_val
            else:
                # Para intradía, usamos UNIX timestamp UTC
                ts_obj = row['timestamp'].replace(tzinfo=timezone.utc)
                time_val = int(ts_obj.timestamp())
                ts_check = time_val
            
            # 1. Evitar duplicados de tiempo
            if ts_check in seen_ts:
                continue
            
            # 2. Filtrar velas rotas (faltan precios)
            if row['open'] is None or row['close'] is None or row['high'] is None or row['low'] is None:
                continue

            seen_ts.add(ts_check)
            
            candles.append({
                "time": time_val,
                "open": row['open'],
                "high": row['high'],
                "low": row['low'],
                "close": row['close']
            })
            vol_series.append({"time": time_val, "value": row['volume']})
            
            if pd.notnull(row['rsi']): rsi_series.append({"time": time_val, "value": row['rsi']})
            if pd.notnull(row['macd_hist']): macd_series.append({"time": time_val, "value": row['macd_hist']})
            
            if pd.notnull(row['ema_20']): ema_short.append({"time": time_val, "value": row['ema_20']})
            if pd.notnull(row['ema_50']): ema_mid.append({"time": time_val, "value": row['ema_50']})
            if pd.notnull(row['ema_200']): ema_long.append({"time": time_val, "value": row['ema_200']})
            
            if pd.notnull(row['donchian_high']): donchian_h.append({"time": time_val, "value": row['donchian_high']})
            if pd.notnull(row['donchian_low']): donchian_l.append({"time": time_val, "value": row['donchian_low']})

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
            "support": last_row.get('donchian_low'),
            "resistance": last_row.get('donchian_high'),
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
                "ema_long": ema_long,
                "donchian_high": donchian_h,
                "donchian_low": donchian_l
            }
        }
        
        response["timeframes"][tf] = tf_data

    if not response["timeframes"]:
        raise HTTPException(status_code=404, detail="Ticker not found or no data")

    return response

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
