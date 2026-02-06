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

        logging.info(f"📥 Iniciando Sync de {len(tickers)} activos en {timeframes}...")
        start_global = time.time()

        for tf in timeframes:
            self._sync_timeframe_batched(tickers, tf)
        
        logging.info(f"✅ Sync Finalizado. Tiempo total: {time.time() - start_global:.2f}s")

    def _sync_timeframe_batched(self, tickers: List[str], timeframe: str):
        yf_interval = self._map_tf_to_yf(timeframe)
        if not yf_interval:
            logging.error(f"❌ Timeframe no soportado: {timeframe}")
            return

        # 1. Analizar estado actual de la DB
        logging.info(f"   🔎 Buscando fechas existentes para [{timeframe}]...")
        
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
        groups = {}
        overlap = timedelta(days=5) 
        
        default_start = "2000-01-01"
        if yf_interval in ["60m", "1h", "30m", "15m"]: 
            default_start = (datetime.now() - timedelta(days=59)).strftime('%Y-%m-%d')
            if yf_interval in ["60m", "1h"]:
                 default_start = (datetime.now() - timedelta(days=720)).strftime('%Y-%m-%d')

        for t in tickers:
            last_ts = existing_dates.get(t)
            if last_ts:
                s_date = (pd.to_datetime(last_ts) - overlap).strftime('%Y-%m-%d')
            else:
                s_date = default_start
            
            if s_date not in groups:
                groups[s_date] = []
            groups[s_date].append(t)

        logging.info(f"   ⚡ {len(groups)} grupos de descarga detectados.")

        # 3. Descargar por Grupo
        for start_date, batch_tickers in groups.items():
            # CHUNKING (Smart Batching): Evita saturar YFinance y URLs demasiado largas
            # 50 tickers por llamada es el sweet spot para estabilidad.
            chunk_size = 50
            total_chunks = (len(batch_tickers) - 1) // chunk_size + 1
            
            logging.info(f"      📡 Descargando {len(batch_tickers)} activos desde {start_date} ({total_chunks} chunks)...")
            
            for i in range(0, len(batch_tickers), chunk_size):
                chunk = batch_tickers[i:i + chunk_size]
                chunk_num = i // chunk_size + 1
                
                logging.info(f"         -> Chunk {chunk_num}/{total_chunks}: Solicitando {len(chunk)} tickers...")
                t_start = time.time()
                self._download_and_save_batch(chunk, start_date, yf_interval, timeframe)
                logging.info(f"         ✅ Chunk {chunk_num} ok ({time.time() - t_start:.2f}s)")

    def _download_and_save_batch(self, tickers: List[str], start_date: str, interval: str, timeframe: str):
        try:
            data = yf.download(tickers, start=start_date, interval=interval, auto_adjust=True, threads=False, progress=False)
            
            if data.empty:
                logging.warning(f"            ⚠️ Batch vacío para {len(tickers)} tickers.")
                return

            logging.info(f"            📥 Recibidos {len(data)} registros temporales.")

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

                 # FIX DUPLICADOS 1D: Normalizar a medianoche
                 if timeframe == '1d':
                     df_stacked['date'] = df_stacked['date'].dt.normalize()

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

        # FIX DUPLICADOS 1D: Normalizar a medianoche
        if timeframe == '1d' and 'date' in df.columns:
             df['date'] = df['date'].dt.normalize()
        
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
