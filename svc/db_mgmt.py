import os
import duckdb
import pandas as pd

# 📌 Ruta base de la DB
DATA_DIR = "data"
DB_PATH = os.path.join(DATA_DIR, "markets.duckdb")

def get_connection():
    """Abre conexión con la DB (usar siempre este wrapper)."""
    os.makedirs(DATA_DIR, exist_ok=True)
    return duckdb.connect(DB_PATH)


def init_db():
    con = duckdb.connect(DB_PATH)

    # 1. Tabla prices (OHLCV)
    con.execute("""
    CREATE TABLE IF NOT EXISTS prices (
        ticker TEXT,
        timeframe TEXT,
        date TIMESTAMP,
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        UNIQUE (ticker, timeframe, date)
    );
    """)

    # 2. Tabla indicators (TA derivados)
    con.execute("""
    CREATE TABLE IF NOT EXISTS indicators (
        ticker      TEXT,
        timeframe   TEXT,
        date        TIMESTAMP,
        rsi         DOUBLE,
        macd        DOUBLE,
        macd_signal DOUBLE,
        macd_hist   DOUBLE,
        ema_short   DOUBLE,
        ema_long    DOUBLE,
        bb_upper    DOUBLE,
        bb_middle   DOUBLE,
        bb_lower    DOUBLE,
        bb_bandwidth DOUBLE,
        bb_percent DOUBLE,
        adx         DOUBLE,
        adxr        DOUBLE,
        di_plus     DOUBLE,
        di_minus    DOUBLE,
        donchian_high DOUBLE,
        donchian_low DOUBLE,
        donchian_mid DOUBLE,
        vol_sma20   DOUBLE,
        vol_ema20   DOUBLE,
        obv         DOUBLE,
        cmf         DOUBLE,
        mfi         DOUBLE,
        UNIQUE (ticker, timeframe, date)
    );
    """)

    # 3. Tabla screener_signals (reglas)
    con.execute("""
    CREATE TABLE IF NOT EXISTS screener_signals (
        ticker TEXT,
        timeframe TEXT,
        date TIMESTAMP,
        signal_type TEXT,
        signal_value TEXT,
        details JSON,
        PRIMARY KEY (ticker, timeframe, date, signal_type, signal_value)
    );
    """)

    con.close()

# ──────────────── WRITE ────────────────
def insert_prices(df: pd.DataFrame):
    """
    Inserta un DataFrame en la tabla prices.
    df debe tener columnas: ticker, timeframe, date, open, high, low, close, volume
    """
    con = get_connection()
    con.register("tmp_df", df)
    con.execute("BEGIN TRANSACTION")
    con.execute("INSERT OR REPLACE INTO prices SELECT * FROM tmp_df")
    con.execute("COMMIT")
    con.close()

def insert_indicators(df: pd.DataFrame):
    """Inserta indicadores calculados en la tabla indicators."""
    con = get_connection()
    con.register("tmp_df", df)
    con.execute("BEGIN TRANSACTION")
    con.execute("INSERT OR REPLACE INTO indicators SELECT * FROM tmp_df")
    con.execute("COMMIT")
    con.close()

def insert_signals(df: pd.DataFrame):
    """Inserta señales de screener en la tabla screener_signals."""
    con = get_connection()
    con.execute("BEGIN TRANSACTION")
    con.execute("INSERT OR REPLACE INTO screener_signals SELECT * FROM df")
    con.execute("COMMIT")
    con.close()

# ──────────────── READ ────────────────
def get_prices(ticker: str = None, timeframe: str='1d', start: str = None, end: str = None) -> pd.DataFrame:
    """Lee OHLCV de un ticker/timeframe, con rango opcional."""
    con = get_connection()
    query = f"""
        SELECT * FROM prices
        WHERE timeframe = '{timeframe}'
    """
    if ticker:  # solo aplica si se pasa explícitamente un ticker
        query += f" AND ticker = '{ticker}'"
    
    if start:
        query += f" AND date >= '{start}'"
    if end:
        query += f" AND date <= '{end}'"
    query += " ORDER BY date"
    df = con.execute(query).df()
    con.close()
    return df

def get_latest_signals(limit: int = 20) -> pd.DataFrame:
    """Devuelve las últimas señales generadas por el screener."""
    con = get_connection()
    query = f"""
        SELECT * FROM screener_signals
        ORDER BY date DESC
        LIMIT {limit}
    """
    df = con.execute(query).df()
    con.close()
    return df

def get_latest_indicators(timeframe: str = "1d") -> pd.DataFrame:
    with get_connection() as conn:
        return conn.execute(f"""
            SELECT *
            FROM indicators
            WHERE timeframe = '{timeframe}'
            QUALIFY ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY date DESC) = 1
        """).df()

def get_all_indicators(timeframe: str = "1d") -> pd.DataFrame:
    with get_connection() as conn:
        return conn.execute(f"""
            SELECT *
            FROM indicators
            WHERE timeframe = '{timeframe}'
        """).df()