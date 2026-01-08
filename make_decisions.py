import duckdb
import pandas as pd

DB_PATH = "/data/markets.duckdb"
START_DATE = "2025-01-01"
TIMEFRAME = "1d"

def load_signals(db_path=DB_PATH, start_date=START_DATE, timeframe=TIMEFRAME):
    con = duckdb.connect(db_path)
    df = con.execute(f"""
        SELECT ticker, timeframe, date, signal_type, signal_value
        FROM screener_signals
        WHERE timeframe = '{timeframe}'
          AND date >= '{start_date}'
        ORDER BY date, ticker
    """).df()
    con.close()
    return df

def decide_strategy_A(df_signals: pd.DataFrame) -> pd.DataFrame:
    rows = []
    # agrupamos exacto por lo tuyo: (ticker, timeframe, date)
    for (t, tf, d), g in df_signals.groupby(["ticker", "timeframe", "date"], sort=False):

        # --- TENDENCIA (uno de: BULLISH / BEARISH / SIDEWAYS) ---
        tend_set = set(g.loc[g["signal_type"]=="tendencia", "signal_value"])
        trend = next(iter(tend_set), None)  # toma el que haya

        # --- MOMENTUM: separar MACD y RSI explícitos ---
        macd_set = set(g.loc[
            (g["signal_type"]=="momentum") &
            (g["signal_value"].str.startswith("MACD")),
            "signal_value"
        ])
        rsi_set = set(g.loc[
            (g["signal_type"]=="momentum") &
            (g["signal_value"].isin(["RSI_OVERSOLD","RSI_OVERBOUGHT","OVERSOLD","OVERBOUGHT","RSI_NORMAL"])),
            "signal_value"
        ])
        # valores “simples” para inspección
        macd = ("MACD_BULL" if "MACD_BULL" in macd_set
                else "MACD_BEAR" if "MACD_BEAR" in macd_set
                else next(iter(macd_set), None))
        rsi = ("RSI_OVERSOLD" if ("RSI_OVERSOLD" in rsi_set or "OVERSOLD" in rsi_set)
               else "RSI_OVERBOUGHT" if ("RSI_OVERBOUGHT" in rsi_set or "OVERBOUGHT" in rsi_set)
               else "RSI_NORMAL" if "RSI_NORMAL" in rsi_set
               else None)

        # --- VOLUMEN (opcional, no afecta decisión A por ahora) ---
        vol_set = set(g.loc[g["signal_type"]=="volumen", "signal_value"])
        volume = ("HIGH_VOL" if "HIGH_VOL" in vol_set
                  else "NORMAL_VOL" if "NORMAL_VOL" in vol_set
                  else next(iter(vol_set), None))

        # --- Reglas Estrategia A ---
        decision = "HOLD"
        if trend == "BULLISH" and (macd == "MACD_BULL" or rsi in ["RSI_OVERSOLD","OVERSOLD"]):
            decision = "BUY"
        elif trend == "BEARISH" and (macd == "MACD_BEAR" or rsi in ["RSI_OVERBOUGHT","OVERBOUGHT"]):
            decision = "SELL"
        # SIDEWAYS => HOLD por diseño

        rows.append({
            "ticker": t,
            "timeframe": tf,
            "date": d,
            "trend": trend,
            "macd": macd,
            "rsi": rsi,
            "volume": volume,
            "decision": decision
        })

    return pd.DataFrame(rows).sort_values(["date","ticker"], kind="stable")

def save_decisions_to_db(df, db_path=DB_PATH):
    con = duckdb.connect(db_path)
    con.execute("CREATE TABLE IF NOT EXISTS decisions AS SELECT * FROM df LIMIT 0")  # esquema vacío
    con.register("df_view", df)
    con.execute("INSERT INTO decisions SELECT * FROM df_view")
    con.close()

if __name__ == "__main__":
    sigs = load_signals()
    decisions = decide_strategy_A(sigs)
    # inspección rápida (no guardamos todavía)
    print(decisions.head(20).to_string(index=False))
    # conteo de BUY/SELL por día (sanity check)
    print(
        decisions.groupby("date")["decision"]
        .value_counts()
        .unstack(fill_value=0)
        .head(10)
    )
    save_decisions_to_db(decisions)
