import pandas as pd
import pandas_ta as ta
import logging
from tqdm import tqdm
from svc_v2.db import Database
import numpy as np

class Analyzer:
    def __init__(self, db: Database):
        self.db = db

    def analyze_tickers(self, tickers: list, timeframes: list, force_full: bool = False):
        """
        Calcula indicadores técnicos y los guarda en DB.
        :param force_full: Si True, recalcula toda la historia. Si False, solo lo reciente.
        """
        if not tickers:
            return

        for tf in timeframes:
            self._analyze_batch(tickers, tf, force_full)

    def _analyze_batch(self, tickers: list, timeframe: str, force_full: bool):
        # Configuración de indicadores (podría venir de settings.yaml)
        # Por ahora hardcoded basándonos en el Manifiesto
        LOOKBACK_REQUIRED = 300 # Buffer seguro para EMA200 y Weinstein

        pbar = tqdm(tickers, desc=f"🧠 Analyzing {timeframe}")
        for ticker in pbar:
            try:
                # 1. Definir cuánta historia leer
                limit = None if force_full else LOOKBACK_REQUIRED
                
                # 2. Leer OHLCV de DB
                df = self.db.get_candles(ticker, timeframe, limit=limit)
                
                if df.empty or len(df) < 50: # Mínimo necesario para calc algo útil
                    continue

                # 3. Calcular Indicadores (Vectorizado con pandas_ta)
                df = self._compute_indicators(df)
                
                # 4. Guardar resultados
                # Si no es full history, solo guardamos las ultimas N velas calculadas
                # para evitar reescribir lo que no cambió.
                if not force_full:
                    # Guardamos solo las últimas 5 velas para manejar fines de semana/correcciones
                    df_to_save = df.tail(5)
                else:
                    df_to_save = df

                self._save_indicators(ticker, timeframe, df_to_save)

            except Exception as e:
                logging.error(f"❌ Error analizando {ticker} ({timeframe}): {e}")

    def _compute_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """Aplicación pura de indicadores sobre el DF."""
        # Copia ligera para no fragmentar
        # df tiene: timestamp, open, high, low, close, volume
        
        # --- Trend (EMAs) ---
        df['ema_20'] = ta.ema(df['close'], length=20)
        df['ema_50'] = ta.ema(df['close'], length=50)
        df['ema_200'] = ta.ema(df['close'], length=200)

        # --- Momentum (RSI, MACD, ADX) ---
        df['rsi'] = ta.rsi(df['close'], length=14)
        
        # MACD (12, 26, 9)
        macd = ta.macd(df['close'], fast=12, slow=26, signal=9)
        if macd is not None:
            df['macd'] = macd['MACD_12_26_9']
            df['macd_signal'] = macd['MACDs_12_26_9']
            df['macd_hist'] = macd['MACDh_12_26_9']
        
        # ADX (14)
        adx = ta.adx(df['high'], df['low'], df['close'], length=14)
        if adx is not None:
            df['adx'] = adx['ADX_14']

        # --- Volatility (Bollinger, Donchian) ---
        # BBands
        bb = ta.bbands(df['close'], length=20, std=2)
        if bb is not None:
            # Dynamic column mapping to avoid version suffix issues
            try:
                # Find columns starting with BBU, BBM, BBL
                c_upper = next(c for c in bb.columns if c.startswith('BBU'))
                c_mid   = next(c for c in bb.columns if c.startswith('BBM'))
                c_lower = next(c for c in bb.columns if c.startswith('BBL'))
                
                df['bb_upper'] = bb[c_upper]
                df['bb_mid'] = bb[c_mid]
                df['bb_lower'] = bb[c_lower]
            except StopIteration:
                logging.error(f"BBands columns missing required prefixes. Got: {bb.columns.tolist()}")
            except Exception as e:
                logging.error(f"Error mapping BBands: {e}")

        # Donchian (20 y 60 según manifiesto)
        # Pandas TA donchian retorna lower, mid, upper.
        donch20 = ta.donchian(df['high'], df['low'], lower_length=20, upper_length=20)
        if donch20 is not None:
             # Nombres por defecto de pandas_ta: DCL_20_20, DCU_20_20
            df['donchian_low'] = donch20['DCL_20_20']
            df['donchian_high'] = donch20['DCU_20_20']

        # --- Manifiesto Específicos ---
        
        # 1. GAP% = (Open_Hoy - Close_Ayer) / Close_Ayer
        # Shift(1) mueve el close una fila abajo (ayer alineado con hoy)
        prev_close = df['close'].shift(1)
        df['gap_pct'] = (df['open'] - prev_close) / prev_close * 100.0
        
        # 2. CHG% = (Close_Hoy - Close_Ayer) / Close_Ayer (Rendimiento Total Intradía)
        df['chg_pct'] = (df['close'] - prev_close) / prev_close * 100.0

        # 3. Vol K (Relativo) = Vol / AvgVol20
        vol_avg = ta.sma(df['volume'], length=20)
        df['vol_k'] = df['volume'] / (vol_avg + 1e-9) # Evitar div/0

        # 3. Reacelera MACD (macd_reaccel)
        # Logica: Histograma subiendo Y histograma positivo? 
        # O derivada positiva?
        # Manifiesto: "ReaceleraMACD (derivada de MACDh)"
        # Interpretación: Slope del hist > 0.
        # Mejor aún: slope > 0 y quizás MacdHist > 0 para confirmar tendencia.
        # Calculamos slope de 3 periodos para suavizar ruido
        macd_slope = df['macd_hist'].diff()
        # Lo guardaremos como booleano en logica de negocio, o el valor crudo en DB?
        # En DB guardemos indicadores numéricos. La decisión binaria va en el "Screener".
        # Aquí no hay columna específica en tabla indicators para "reaccel" (booleano),
        # pero tenemos macd_hist.
        # Si queremos guardar la slope, habría que añadir columna.
        # Por ahora con macd_hist podemos calcular slope en query: (h - lag(h)).

        return df

    def _save_indicators(self, ticker: str, timeframe: str, df: pd.DataFrame):
        """Prepara y guarda el DF en la tabla indicators."""
        # Mapear columnas del DF a nombres de la DB
        # DB espera: ticker, timeframe, timestamp, rsi, macd...
        
        # Inyectar claves
        df = df.copy()
        df['ticker'] = ticker
        df['timeframe'] = timeframe
        # Renombrar 'date' a 'timestamp' si es necesario, 
        # pero get_candles devuelve 'timestamp' ya.
        
        # Seleccionar solo columnas que existen en la tabla indicators
        # para evitar error de columnas extra (open, high...)
        target_cols = [
            'ticker', 'timeframe', 'timestamp',
            'rsi', 'macd', 'macd_signal', 'macd_hist', 'adx',
            'ema_20', 'ema_50', 'ema_200', 
            'donchian_high', 'donchian_low',
            'bb_upper', 'bb_mid', 'bb_lower',
            'vol_k', 'gap_pct', 'chg_pct'
        ]
        
        # Filtrar solo las que tenemos
        cols_to_save = [c for c in target_cols if c in df.columns]
        df_final = df[cols_to_save]
        
        # Upsert manual via SQL
        # DuckDB Python API tiene .insert() pero para upsert complejo usamos SQL
        self.db.conn.register('temp_ind', df_final)
        
        # Construir query dinámica según columnas disponibles
        cols_str = ", ".join(cols_to_save)
        # Para el UPDATE SET, excluimos las PKs
        update_set = [f"{c} = EXCLUDED.{c}" for c in cols_to_save if c not in ('ticker', 'timeframe', 'timestamp')]
        update_str = ", ".join(update_set)
        
        query = f"""
            INSERT INTO indicators ({cols_str})
            SELECT {cols_str} FROM temp_ind
            ON CONFLICT (ticker, timeframe, timestamp) DO UPDATE SET
            {update_str},
            updated_at = now();
        """
        
        self.db.conn.execute(query)

if __name__ == "__main__":
    # Test rápido
    db = Database()
    alz = Analyzer(db)
    # Asume que ya corriste collector para AAPL
    alz.analyze_tickers(["AAPL"], ["1d"], force_full=True)
    print("Analisis completado.")
