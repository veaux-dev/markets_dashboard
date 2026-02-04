import pandas as pd
import logging
from svc_v2.db import Database

class ScreenerEngine:
    def __init__(self, db: Database):
        self.db = db

    def run_screen(self, strategy_name: str, timeframe: str = "1d") -> pd.DataFrame:
        """
        Ejecuta una estrategia específica y devuelve los candidatos.
        """
        if strategy_name == "BUY_BOUNCE":
            return self._screen_buy_bounce(timeframe)
        elif strategy_name == "BUY_TREND":
            return self._screen_buy_trend(timeframe)
        elif strategy_name == "SELL_STRENGTH":
            return self._screen_sell_strength(timeframe)
        else:
            logging.error(f"Estrategia desconocida: {strategy_name}")
            return pd.DataFrame()

    def _screen_buy_bounce(self, tf: str) -> pd.DataFrame:
        """
        Estrategia: Rebote de Pánico
        """
        query = f"""
        WITH latest AS (
            SELECT i.*, o.close,
                   row_number() OVER (PARTITION BY i.ticker ORDER BY i.timestamp DESC) as rn
            FROM indicators i
            JOIN ohlcv o USING (ticker, timeframe, timestamp)
            WHERE i.timeframe = '{tf}'
        )
        SELECT 
            ticker, timestamp, close, gap_pct, rsi, vol_k, ema_200,
            (close / ema_50 - 1) * 100 as dist_ema50_pct
        FROM latest
        WHERE rn = 1
          AND gap_pct <= -6
          AND rsi BETWEEN 5 AND 60
          AND vol_k >= 0.6
        ORDER BY gap_pct ASC
        """
        return self.db.conn.execute(query).df()

    def _screen_buy_trend(self, tf: str) -> pd.DataFrame:
        """
        Estrategia: Continuación de Tendencia
        """
        query = f"""
        WITH latest AS (
            SELECT i.*, o.close,
                   row_number() OVER (PARTITION BY i.ticker ORDER BY i.timestamp DESC) as rn
            FROM indicators i
            JOIN ohlcv o USING (ticker, timeframe, timestamp)
            WHERE i.timeframe = '{tf}'
        )
        SELECT 
            ticker, timestamp, close, adx, ema_50, ema_200, macd_hist
        FROM latest
        WHERE rn = 1
          AND adx >= 25
          AND ema_50 > ema_200
          AND close > ema_50
          AND macd_hist > 0
        ORDER BY adx DESC
        """
        return self.db.conn.execute(query).df()

    def _screen_sell_strength(self, tf: str) -> pd.DataFrame:
        """
        Estrategia: Venta en Euforia
        """
        query = f"""
        WITH latest AS (
            SELECT i.*, o.close,
                   row_number() OVER (PARTITION BY i.ticker ORDER BY i.timestamp DESC) as rn
            FROM indicators i
            JOIN ohlcv o USING (ticker, timeframe, timestamp)
            WHERE i.timeframe = '{tf}'
        )
        SELECT 
            ticker, timestamp, close, rsi, vol_k
        FROM latest
        WHERE rn = 1
          AND rsi >= 70
        ORDER BY rsi DESC
        """
        return self.db.conn.execute(query).df()

if __name__ == "__main__":
    # Test simple
    db = Database()
    engine = ScreenerEngine(db)
    print("--- BOUNCE ---")
    print(engine.run_screen("BUY_BOUNCE").head())
    print("\n--- TREND ---")
    print(engine.run_screen("BUY_TREND").head())
