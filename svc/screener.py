import logging
import pandas as pd
import json
from svc import db_mgmt
from tqdm import tqdm

def run_screener(timeframe: str = "1d", all_data: bool = False) -> pd.DataFrame:
    """
    Corre screener de tendencia, momentum y volumen.
    Retorna un DataFrame con señales y también las guarda en la DB.
    """
    if all_data:
        df = db_mgmt.get_all_indicators(timeframe)
    else:
        df = db_mgmt.get_latest_indicators(timeframe)

    if df.empty:
        return pd.DataFrame()
    
    prices = db_mgmt.get_prices(timeframe=timeframe)
    df = df.merge(
        prices[["ticker", "timeframe", "date", "close","volume"]],
        on=["ticker", "timeframe", "date"],
        how="left"
        )
    signals = []

    for _, row in tqdm(df.iterrows(),total=len(df),desc=f"Screener {timeframe}"):
        t, date = row["ticker"], row["date"]

        # --- TENDENCIA ---
        if row["ema_short"] > row["ema_long"] and row["close"] > row["ema_long"]:
            details = {"ema_short": row["ema_short"], "ema_long": row["ema_long"], "close": row["close"]}
            signals.append((t, timeframe, date, "tendencia", "BULLISH", json.dumps(details)))
        elif row["ema_short"] < row["ema_long"] and row["close"] < row["ema_long"]:
            details = {"ema_short": row["ema_short"], "ema_long": row["ema_long"], "close": row["close"]}
            signals.append((t, timeframe, date, "tendencia", "BEARISH", json.dumps(details)))
        else:
            details = {"ema_short": row["ema_short"], "ema_long": row["ema_long"], "close": row["close"]}
            signals.append((t, timeframe, date, "tendencia", "SIDEWAYS", json.dumps(details)))

        # --- MOMENTUM ---
        if pd.notna(row.get("rsi")):
            if row["rsi"] > 70:
                details = {"rsi": row["rsi"], "threshold": 70}
                signals.append((t, timeframe, date, "momentum", "OVERBOUGHT", json.dumps(details)))
            elif row["rsi"] < 30:
                details = {"rsi": row["rsi"], "threshold": 30}
                signals.append((t, timeframe, date, "momentum", "OVERSOLD", json.dumps(details)))
            else:
                details = {"rsi": row["rsi"]}
                signals.append((t, timeframe, date, "momentum", "RSI_NORMAL", json.dumps(details)))

        if pd.notna(row.get("macd")) and pd.notna(row.get("macd_signal")):
            details = {"macd": row["macd"], "macd_signal": row["macd_signal"]}
            if row["macd"] > row["macd_signal"]:
                signals.append((t, timeframe, date, "momentum", "MACD_BULL", json.dumps(details)))
            elif row["macd"] < row["macd_signal"]:
                signals.append((t, timeframe, date, "momentum", "MACD_BEAR", json.dumps(details)))
            else:
                signals.append((t, timeframe, date, "momentum", "MACD_NEUTRAL", json.dumps(details)))

        # --- VOLUMEN ---
        if pd.notna(row.get("volume")) and pd.notna(row.get("vol_ema20")):
            details = {"volume": row["volume"], "avg20": row["vol_ema20"]}
            if row["volume"] > 1.5 * row["vol_ema20"]:
                signals.append((t, timeframe, date, "volumen", "HIGH_VOL", json.dumps(details)))
            else:
                signals.append((t, timeframe, date, "volumen", "NORMAL_VOL", json.dumps(details)))

    # Guardar resultados
    df_signals = pd.DataFrame(signals, columns=[
        "ticker", "timeframe", "date", "signal_type", "signal_value", "details"
    ])
    db_mgmt.insert_signals(df_signals)

    logging.info(f"✅ {len(signals)} señales insertadas en screener_signals")
    return df_signals
