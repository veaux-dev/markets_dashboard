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
        Sincroniza tickers en lotes agrupados por fecha de inicio necesaria.
        """
        if not tickers:
            logging.warning("⚠️ Lista de tickers vacía.")
            return

        logging.info(f"📥 Iniciando Sync Batch de {len(tickers)} tickers en {timeframes}...")

        for tf in timeframes:
            self._sync_timeframe_batched(tickers, tf)

    def _sync_timeframe_batched(self, tickers: List[str], timeframe: str):
        yf_interval = self._map_tf_to_yf(timeframe)
        if not yf_interval:
            logging.error(f"❌ Timeframe no soportado: {timeframe}")
            return

        # 1. Analizar estado actual de la DB para agrupar
        # Query masiva para obtener max(timestamp) de todos los tickers solicitados
        logging.info(f"   🔎 Analizando fechas existentes para {timeframe}...")
        
        # Formatear lista para SQL
        tickers_sql = ",".join([f"'{t}'" for t in tickers])
        q = f"""
            SELECT ticker, MAX(timestamp) as last_ts
            FROM ohlcv 
            WHERE timeframe = '{timeframe}' AND ticker IN ({tickers_sql})
            GROUP BY ticker
        """
        try:
            existing_dates = self.db.conn.execute(q).df().set_index('ticker')['last_ts'].to_dict()
        except Exception as e:
            logging.error(f"Error consultando fechas: {e}")
            existing_dates = {}

        # 2. Agrupar por Start Date
        groups = {} # Key: start_date_str, Value: list(tickers)
        
        # Definir defaults
        overlap = timedelta(days=5) # Buffer seguro
        
        # Fecha default para nuevos (Full History)
        default_start = "2000-01-01"
        if yf_interval in ["60m", "1h", "30m", "15m"]: 
            # Intradía tiene límite en YF
            default_start = (datetime.now() - timedelta(days=59)).strftime('%Y-%m-%d') # Safe 60d limit for 15m
            if yf_interval in ["60m", "1h"]:
                 default_start = (datetime.now() - timedelta(days=720)).strftime('%Y-%m-%d') # Safe 2y limit

        for t in tickers:
            last_ts = existing_dates.get(t)
            if last_ts:
                # Incremental
                s_date = (pd.to_datetime(last_ts) - overlap).strftime('%Y-%m-%d')
            else:
                # Full
                s_date = default_start
            
            if s_date not in groups:
                groups[s_date] = []
            groups[s_date].append(t)

        logging.info(f"   ⚡ Se formaron {len(groups)} grupos de descarga.")

        # 3. Descargar por Grupo
        for start_date, batch_tickers in groups.items():
            logging.info(f"      -> Descargando {len(batch_tickers)} tickers desde {start_date}...")
            self._download_and_save_batch(batch_tickers, start_date, yf_interval, timeframe)

    def _download_and_save_batch(self, tickers: List[str], start_date: str, interval: str, timeframe: str):
        try:
            # yf.download devuelve MultiIndex (Price, Ticker) si hay mas de 1 ticker
            # auto_adjust=True para ajustar splits/divs
            # threads=False para evitar saturar conexiones y error 429/ConnectionRefused
            data = yf.download(tickers, start=start_date, interval=interval, auto_adjust=True, threads=False, progress=False)
            
            if data.empty:
                return

            # Normalizar estructura
            # Caso A: Un solo ticker (Index es fecha, columnas son Open, Close...)
            # Caso B: Multiples tickers (Columns Level 0: Price, Level 1: Ticker)
            
            if len(tickers) == 1:
                # Caso especial: Un solo ticker puede venir como MultiIndex o Flat
                # Si es MultiIndex (Price, Ticker), eliminamos el nivel de Ticker
                if isinstance(data.columns, pd.MultiIndex):
                    try:
                        # Intentar seleccionar por nivel de ticker si existe
                        data = data.xs(tickers[0], level=1, axis=1)
                    except:
                        # Fallback: drop level 1
                        data = data.droplevel(1, axis=1)

                df_to_process = data.copy()
                df_to_process['ticker'] = tickers[0]
                self._process_and_upsert(df_to_process, timeframe)
                return

            # Caso MultiIndex: Stackear para tener Ticker como columna
            # data.columns levels: (Price, Ticker) -> stack level 1 (Ticker)
            # Resultado: Index (Date, Ticker), Columns (Close, High, Low...)
            try:
                df_stacked = data.stack(level=1, future_stack=True)
            except TypeError:
                 # Compatibilidad pandas viejos
                 df_stacked = data.stack(level=1)
            
            df_stacked = df_stacked.reset_index()
            
            # Renombrar columnas a minúsculas
            # Stack convierte columnas a Index simple, str(c) es seguro aqui
            df_stacked.columns = [str(c).lower() for c in df_stacked.columns]
            
            # Normalizar nombres de columnas esenciales
            rename_map = {}
            if 'date' not in df_stacked.columns and 'datetime' in df_stacked.columns:
                rename_map['datetime'] = 'date'
            
            if rename_map:
                df_stacked.rename(columns=rename_map, inplace=True)
            
            # Convertir TZ a naive UTC
            if 'date' in df_stacked.columns:
                 # Check dtype (Modern Pandas)
                 if isinstance(df_stacked['date'].dtype, pd.DatetimeTZDtype):
                     df_stacked['date'] = df_stacked['date'].dt.tz_convert("UTC").dt.tz_localize(None)

            # Upsert masivo
            if 'date' in df_stacked.columns:
                self.db.upsert_ohlcv(df_stacked, timeframe)
            else:
                logging.error("❌ Batch ignorado: No se encontró columna 'date' tras procesar.")

        except Exception as e:
            logging.error(f"❌ Error batch download: {e}")

    def _process_and_upsert(self, df: pd.DataFrame, timeframe: str):
        """Helper para DF plano de un solo ticker."""
        df = df.reset_index()
        
        # Sanitizar columnas (pueden ser tuplas si venian de multiindex mal aplanado)
        new_cols = []
        for c in df.columns:
            if isinstance(c, tuple):
                new_cols.append(str(c[0]).lower())
            else:
                new_cols.append(str(c).lower())
        df.columns = new_cols

        if 'datetime' in df.columns: df.rename(columns={'datetime': 'date'}, inplace=True)
        
        if 'date' in df.columns and isinstance(df['date'].dtype, pd.DatetimeTZDtype):
             df['date'] = df['date'].dt.tz_convert("UTC").dt.tz_localize(None)
        
        if 'date' in df.columns:
            self.db.upsert_ohlcv(df, timeframe)
        else:
            logging.warning("⚠️ Ticker único ignorado: Falta columna 'date'")

    def _map_tf_to_yf(self, tf: str) -> str:
        map_ = { "1d": "1d", "1h": "1h", "15m": "15m", "5m": "5m" }
        return map_.get(tf, None)
    
    # Metadata sync separado (se llamará desde un Job semanal)
    def sync_metadata_batch(self, tickers: List[str]):
        """Baja metadata uno por uno (lento, usar con cuidado)."""
        pass # Implementar si se requiere un job específico
