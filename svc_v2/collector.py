import yfinance as yf
import pandas as pd
import logging
from datetime import timedelta, datetime
from typing import List, Dict
import time
from tqdm import tqdm

from svc_v2.db import Database

class Collector:
    def __init__(self, db: Database):
        self.db = db
    
    def sync_tickers(self, tickers: List[str], timeframes: List[str]):
        """
        Sincroniza una lista de tickers para los timeframes dados.
        Maneja solapamiento y caché.
        """
        if not tickers:
            logging.warning("⚠️ Lista de tickers vacía.")
            return

        logging.info(f"📥 Iniciando Sync de {len(tickers)} tickers en {timeframes}...")

        # Iterar por timeframe (para optimizar batches si fuera posible)
        for tf in timeframes:
            self._sync_batch(tickers, tf)

    def _sync_batch(self, tickers: List[str], timeframe: str):
        """
        Descarga y guarda datos para un TF específico.
        """
        # Mapeo de intervalo YF
        yf_interval = self._map_tf_to_yf(timeframe)
        if not yf_interval:
            logging.error(f"❌ Timeframe no soportado: {timeframe}")
            return

        # Para cada ticker, determinamos fecha de inicio
        # Optimización: Agrupar tickers por "fecha de inicio requerida" sería ideal,
        # pero como cada uno puede tener diferente last_update, el loop individual es más seguro
        # para evitar gaps.
        
        pbar = tqdm(tickers, desc=f"Sync {timeframe}")
        for ticker in pbar:
            try:
                self._process_single_ticker(ticker, timeframe, yf_interval)
            except Exception as e:
                logging.error(f"❌ Error sync {ticker} ({timeframe}): {e}")
            
    def _process_single_ticker(self, ticker: str, timeframe: str, interval: str):
        # 1. Obtener última fecha en DB
        last_ts = self.db.get_last_timestamp(ticker, timeframe)
        
        # 2. Calcular Start Date
        start_date = None
        is_full_history = False
        
        if last_ts:
            # Overlap de 2 días (o periodos) para correcciones
            # Si es intradía, 2 días está bien. Si es diario, también.
            overlap = timedelta(days=5) # 5 días de margen es seguro para fines de semana
            start_date = (last_ts - overlap).strftime('%Y-%m-%d')
        else:
            # Si no hay datos, bajar historia "razonable"
            # Broad scan (1d) -> 20 años?
            # Intradía (1h) -> 730 días (max de Yahoo)
            if interval == "1d":
                start_date = "2000-01-01" 
            else:
                # Yahoo limita intradía. 
                # 1h -> max 730d. 15m -> max 60d.
                start_date = None # Dejar que YF decida el max period o usar 'period' param
                is_full_history = True

        # 3. Descargar
        try:
            # Usamos Ticker object
            dat = yf.Ticker(ticker)
            
            if is_full_history:
                # Definir periodos maximos segun intervalo
                p = "max"
                if interval in ["60m", "1h"]: p = "2y" # 730d limit aprox
                if interval in ["15m", "30m"]: p = "60d"
                if interval in ["5m", "2m"]: p = "5d" # muy corto
                
                df = dat.history(period=p, interval=interval, auto_adjust=True)
            else:
                df = dat.history(start=start_date, interval=interval, auto_adjust=True)

            if df.empty:
                return

            # 4. Limpiar y Formatear
            # YF devuelve índice con Tizone (a veces)
            if df.index.tz is not None:
                # Convertir a UTC o naive local?
                # DuckDB maneja TIMESTAMP (sin zona) o TIMESTAMPTZ.
                # Preferencia: UTC explícito y quitar zona para simplificar queries.
                df.index = df.index.tz_convert("UTC").tz_localize(None)
            
            df = df.reset_index()
            # Renombrar columnas a minusculas
            df.columns = [c.lower() for c in df.columns]
            
            # Asegurar columnas esenciales
            req_cols = ['date', 'open', 'high', 'low', 'close', 'volume']
            if not all(c in df.columns for c in req_cols):
                # A veces 'Date' se llama 'Datetime'
                if 'datetime' in df.columns:
                    df.rename(columns={'datetime': 'date'}, inplace=True)
            
            # Inyectar columna ticker para el Upsert
            df['ticker'] = ticker
            
            # Filtrar filas vacías
            df = df.dropna(subset=['open', 'close'])

            # 5. Upsert a DB
            if not df.empty:
                self.db.upsert_ohlcv(df, timeframe)
            
            # 6. Metadata (Solo si es Broad Scan 1d, para no saturar en intradía)
            if timeframe == '1d':
                self._sync_metadata(dat, ticker)

        except Exception as e:
            logging.error(f"Error interno descargando {ticker}: {e}")

    def _sync_metadata(self, dat: yf.Ticker, ticker: str):
        """Baja earnings y sector. Silencioso si falla."""
        try:
            # Next Earnings
            nxt = None
            try:
                cal = dat.calendar
                if cal is not None and not cal.empty:
                    # calendar suele tener 'Earnings Date' o 'Earnings High'
                    # yf structure changes often. 
                    # Dictionary or DataFrame.
                    if isinstance(cal, dict):
                        val = cal.get('Earnings Date')
                        if val: nxt = val[0]
                    elif isinstance(cal, pd.DataFrame):
                         # A veces es index, a veces columna
                         if 'Earnings Date' in cal.index:
                             val = cal.loc['Earnings Date']
                             nxt = val.iloc[0] if isinstance(val, pd.Series) else val
            except:
                pass # Calendar a veces falla en YF
            
            # Fallback info
            if nxt is None:
                # Try info fast
                # info = dat.fast_info # Not enough data
                pass
            
            # Sector/Industry (Solo si tenemos session cached o similar, info es lento)
            # Por ahora saltamos info para no alentar el scan masivo
            
            if nxt:
                # Convertir a Timestamp
                nxt_ts = pd.to_datetime(nxt).tz_localize(None)
                self.db.upsert_metadata(ticker, nxt_ts)

        except Exception as e:
            # No loggear error para no ensuciar, metadata es 'nice to have'
            pass
            
    def _map_tf_to_yf(self, tf: str) -> str:
        # Mapeo simple de nuestra config a YF API
        map_ = {
            "1d": "1d",
            "1h": "1h",
            "15m": "15m",
            "5m": "5m"
        }
        return map_.get(tf, None)

# Helper testing
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    db = Database()
    col = Collector(db)
    col.sync_tickers(["AAPL", "NVDA"], ["1d"])
