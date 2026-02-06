import duckdb
import pandas as pd
import logging
from pathlib import Path
from threading import Lock
from typing import Optional

# Configuración por defecto (será sobreescrita por el config loader)
DEFAULT_DB_PATH = "data/markets.duckdb"

class Database:
    _instance = None
    _lock = Lock()

    def __new__(cls, db_path: str = DEFAULT_DB_PATH):
        with cls._lock:
            if cls._instance is None:
                cls._instance = super(Database, cls).__new__(cls)
                cls._instance._init_db(db_path)
            return cls._instance

    def _init_db(self, db_path: str):
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = duckdb.connect(str(self.db_path))
        self._create_tables()
        logging.info(f"🦆 DuckDB conectada en: {self.db_path}")

    def _create_tables(self):
        """Inicializa el esquema de tablas si no existen."""
        
        # 1. Tabla OHLCV (Datos crudos)
        # Partitioning implícito por Ticker/Timeframe via índice compuesto
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv (
                ticker VARCHAR,
                timeframe VARCHAR,
                timestamp TIMESTAMP,
                open DOUBLE,
                high DOUBLE,
                low DOUBLE,
                close DOUBLE,
                volume DOUBLE,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, timeframe, timestamp)
            );
        """)

        # 2. Tabla INDICATORS (Datos calculados)
        # Separada para permitir borrar y recalcular sin perder precios
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS indicators (
                ticker VARCHAR,
                timeframe VARCHAR,
                timestamp TIMESTAMP,
                
                -- Momentum
                rsi DOUBLE,
                macd DOUBLE,
                macd_signal DOUBLE,
                macd_hist DOUBLE,
                adx DOUBLE,
                
                -- Trend / Structure
                ema_20 DOUBLE,
                ema_50 DOUBLE,
                ema_200 DOUBLE,
                donchian_high DOUBLE,
                donchian_low DOUBLE,
                
                -- Volatility / Bands
                bb_upper DOUBLE,
                bb_mid DOUBLE,
                bb_lower DOUBLE,
                
                -- Custom Metrics (Manifiesto)
                vol_k DOUBLE,
                gap_pct DOUBLE,
                chg_pct DOUBLE,
                
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                PRIMARY KEY (ticker, timeframe, timestamp),
                FOREIGN KEY (ticker, timeframe, timestamp) REFERENCES ohlcv(ticker, timeframe, timestamp)
            );
        """)

        # 3. Tabla LOGS (Auditoría interna)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS system_logs (
                id INTEGER PRIMARY KEY, 
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                level VARCHAR,
                component VARCHAR,
                message VARCHAR
            );
            CREATE SEQUENCE IF NOT EXISTS log_id_seq;
            ALTER TABLE system_logs ALTER COLUMN id SET DEFAULT nextval('log_id_seq');
        """)

        # 4. Tabla METADATA (Earnings, Sector, etc.)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS ticker_metadata (
                ticker VARCHAR PRIMARY KEY,
                name VARCHAR,
                next_earnings TIMESTAMP,
                sector VARCHAR,
                industry VARCHAR,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 5. Tabla DYNAMIC WATCHLIST (El puente entre Broad y Detailed)
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS dynamic_watchlist (
                ticker VARCHAR PRIMARY KEY,
                reason VARCHAR,     -- Estrategia que lo detectó (ej. 'BUY_BOUNCE')
                added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                expires_at TIMESTAMP -- Hasta cuándo vigilarlo (ej. +3 días)
            );
        """)

        # 6. Tabla PORTFOLIO TRANSACTIONS (Ledger)
        self.conn.execute("""
            CREATE SEQUENCE IF NOT EXISTS txn_id_seq;
            CREATE TABLE IF NOT EXISTS portfolio_transactions (
                id INTEGER PRIMARY KEY DEFAULT nextval('txn_id_seq'),
                ticker VARCHAR,
                side VARCHAR, -- 'BUY', 'SELL', 'DIVIDEND', 'SPLIT'
                qty DOUBLE,
                price DOUBLE,
                fees DOUBLE DEFAULT 0.0,
                currency VARCHAR DEFAULT 'MXN',
                notes VARCHAR,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)

        # 7. Vista PORTFOLIO HOLDINGS (Calculada)
        # Calcula la posición neta actual sumando compras y restando ventas
        self.conn.execute("""
            CREATE OR REPLACE VIEW view_portfolio_holdings AS
            SELECT 
                ticker, 
                SUM(CASE WHEN side = 'BUY' THEN qty WHEN side = 'SELL' THEN -qty ELSE 0 END) as qty,
                SUM(CASE WHEN side = 'BUY' THEN qty * price ELSE 0 END) / NULLIF(SUM(CASE WHEN side = 'BUY' THEN qty ELSE 0 END), 0) as avg_buy_price
            FROM portfolio_transactions
            GROUP BY ticker
            HAVING SUM(CASE WHEN side = 'BUY' THEN qty WHEN side = 'SELL' THEN -qty ELSE 0 END) > 0;
        """)

    # --------------------------------------------------------------------------
    # WRITE OPERATIONS (Upserts)
    # --------------------------------------------------------------------------
    
    def add_transaction(self, ticker: str, side: str, qty: float, price: float, fees: float = 0.0, notes: str = None, timestamp: str = None, currency: str = 'MXN'):
        """Registra una nueva transacción (BUY/SELL). Timestamp opcional (YYYY-MM-DD HH:MM:SS)."""
        try:
            ts_val = f"'{timestamp}'" if timestamp else "now()"
            
            query = f"""
                INSERT INTO portfolio_transactions (ticker, side, qty, price, fees, notes, timestamp, currency)
                VALUES (?, ?, ?, ?, ?, ?, {ts_val}, ?)
            """
            self.conn.execute(query, [ticker, side.upper(), qty, price, fees, notes, currency.upper()])
            logging.info(f"💰 Transacción registrada: {side} {qty} {ticker} @ {price} {currency} ({ts_val})")
        except Exception as e:
            logging.error(f"DB Error adding transaction {ticker}: {e}")

    def add_to_dynamic_watchlist(self, ticker: str, reason: str, days_to_keep: int = 3):
        """Agrega un ticker a la watchlist dinámica o extiende su expiración acumulando razones."""
        try:
            # Si ya existe, concatenar la razón si es nueva
            query = f"""
                INSERT INTO dynamic_watchlist (ticker, reason, added_at, expires_at)
                VALUES ('{ticker}', '{reason}', now(), now() + INTERVAL {days_to_keep} DAY)
                ON CONFLICT (ticker) DO UPDATE SET
                    reason = CASE 
                        WHEN dynamic_watchlist.reason LIKE '%{reason}%' THEN dynamic_watchlist.reason 
                        ELSE dynamic_watchlist.reason || ', ' || EXCLUDED.reason 
                    END,
                    expires_at = GREATEST(dynamic_watchlist.expires_at, EXCLUDED.expires_at);
            """
            self.conn.execute(query)
        except Exception as e:
            logging.error(f"DB Error updating watchlist for {ticker}: {e}")

    def get_dynamic_watchlist(self) -> list:
        """Retorna lista de tickers activos en la watchlist dinámica."""
        try:
            res = self.conn.execute("""
                SELECT ticker FROM dynamic_watchlist 
                WHERE expires_at > now()
            """).fetchall()
            return [r[0] for r in res]
        except Exception as e:
            logging.error(f"DB Error reading dynamic watchlist: {e}")
            return []

    def upsert_metadata(self, ticker: str, next_earnings: pd.Timestamp = None, sector: str = None, industry: str = None, name: str = None):
        """Guarda metadatos del ticker."""
        try:
            # Upsert inteligente: actualiza solo lo que no sea None
            self.conn.execute("""
                INSERT INTO ticker_metadata (ticker, name, next_earnings, sector, industry, updated_at)
                VALUES (?, ?, ?, ?, ?, now())
                ON CONFLICT (ticker) DO UPDATE SET
                    name = COALESCE(EXCLUDED.name, ticker_metadata.name),
                    next_earnings = COALESCE(EXCLUDED.next_earnings, ticker_metadata.next_earnings),
                    sector = COALESCE(EXCLUDED.sector, ticker_metadata.sector),
                    industry = COALESCE(EXCLUDED.industry, ticker_metadata.industry),
                    updated_at = now();
            """, [ticker, name, next_earnings, sector, industry])
        except Exception as e:
            logging.error(f"DB Error upserting metadata {ticker}: {e}")

    def upsert_ohlcv(self, df: pd.DataFrame, timeframe: str):
        """
        Inserta o actualiza velas desde un DataFrame.
        El DF debe tener índice Datetime y columnas: open, high, low, close, volume.
        Opcional: columna 'ticker' si el DF tiene múltiples tickers.
        """
        if df.empty:
            return

        # Normalización básica
        df = df.copy()
        if 'ticker' not in df.columns and hasattr(df, 'name'):
             # Si es un DF de un solo ticker y tiene nombre
             pass 
        
        # Asegurar formato para DuckDB
        # DuckDB espera: ticker, timeframe, timestamp, o, h, l, c, v
        
        # Caso 1: Multi-index o columna Ticker presente
        if 'ticker' not in df.columns:
            # Asumimos que el caller maneja esto o es un single-ticker DF
            # Esto se refinará en el collector
            pass

        # Estrategia de inserción masiva eficiente:
        # 1. Registrar DF como vista temporal
        # 2. Insertar con ON CONFLICT REPLACE
        
        try:
            self.conn.register('temp_ohlcv_df', df)
            
            # El collector envía la columna de fecha como 'date' (minusculas)
            query = f"""
                INSERT INTO ohlcv (ticker, timeframe, timestamp, open, high, low, close, volume)
                SELECT 
                    ticker, 
                    '{timeframe}' as timeframe,
                    date as timestamp, 
                    open, high, low, close, volume
                FROM temp_ohlcv_df
                ON CONFLICT (ticker, timeframe, timestamp) DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    updated_at = now();
            """
            self.conn.execute(query)
            
        except Exception as e:
            logging.error(f"DB Error upserting OHLCV: {e}")
            raise

    # --------------------------------------------------------------------------
    # READ OPERATIONS
    # --------------------------------------------------------------------------

    def get_last_timestamp(self, ticker: str, timeframe: str) -> Optional[pd.Timestamp]:
        """Devuelve la fecha de la última vela guardada para un ticker/tf."""
        res = self.conn.execute("""
            SELECT MAX(timestamp) 
            FROM ohlcv 
            WHERE ticker = ? AND timeframe = ?
        """, [ticker, timeframe]).fetchone()
        
        return pd.Timestamp(res[0]) if res and res[0] else None

    def get_candles(self, ticker: str, timeframe: str, limit: int = None) -> pd.DataFrame:
        """Recupera velas históricas."""
        query = f"""
            SELECT timestamp, open, high, low, close, volume
            FROM ohlcv
            WHERE ticker = '{ticker}' AND timeframe = '{timeframe}'
            ORDER BY timestamp ASC
        """
        if limit:
            # Para indicadores, a veces necesitamos las ultimas N
            # Pero ojo, ORDER BY ASC + LIMIT te da las PRIMERAS N.
            # Necesitamos subquery para obtener las ÚLTIMAS N y luego reordenar.
            query = f"""
                SELECT * FROM (
                    SELECT timestamp, open, high, low, close, volume
                    FROM ohlcv
                    WHERE ticker = '{ticker}' AND timeframe = '{timeframe}'
                    ORDER BY timestamp DESC
                    LIMIT {limit}
                ) ORDER BY timestamp ASC
            """
        
        return self.conn.execute(query).df()

    def close(self):
        self.conn.close()

# Helper simple para testing
if __name__ == "__main__":
    db = Database()
    print("DB V2 Inicializada.")
