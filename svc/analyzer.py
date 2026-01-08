import pandas_ta as ta
import pandas as pd
import numpy as np
from svc.load_config import RSI_LENGTH, RSI_OB, RSI_OS, MACD_FAST, MACD_SLOW, MACD_SIGNAL, ADX_LENGTH, EMA_SHORT, EMA_LONG, BB_LENGTH, BB_STD
from tqdm import tqdm
from typing import Dict, List, cast

DictOfDict = Dict[str, Dict[str, pd.DataFrame]]
# --------------------------------------------------
# 9) Aplicar indicadores técnicos (pandas_ta)
# --------------------------------------------------
# - Ejemplo: MACD, RSI, EMA, etc.
# - Agregar columnas al DataFrame

def analyse_data(data_by_ticker) -> DictOfDict:

    macdfss=f"{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}"
    bblenst=f"{BB_LENGTH}_{BB_STD}_{BB_STD}"


    for ticker,tfs in tqdm(data_by_ticker.items(), total=len(data_by_ticker.items()), desc=f'🔎 Analizando',unit='ticker'):
        #print(f"🔎 Analizando {ticker} ... timeframes: {list(tfs.keys())}")
        for timeframe, df in tfs.items():
            #print(f"  -> {timeframe} antes: {df.columns.to_list()}")
            data_by_ticker[ticker][timeframe].ta.rsi(length=RSI_LENGTH,append=True)
            data_by_ticker[ticker][timeframe].ta.macd(fast=MACD_FAST,slow=MACD_SLOW,signal=MACD_SIGNAL,append=True)
            data_by_ticker[ticker][timeframe].ta.adx(length=ADX_LENGTH,append=True)
            data_by_ticker[ticker][timeframe].ta.ema(length=EMA_SHORT,append=True)
            data_by_ticker[ticker][timeframe].ta.ema(length=EMA_LONG,append=True)
            data_by_ticker[ticker][timeframe].ta.bbands(length=BB_LENGTH,std=BB_STD,append=True)
            data_by_ticker[ticker][timeframe].ta.donchian(length=20, append=True)
            #renombrando las columnas para facilitar su uso
            data_by_ticker[ticker][timeframe].rename(
                columns={
                    f"RSI_{RSI_LENGTH}":"rsi",
                    f"MACD_{macdfss}":"macd",
                    f"MACDh_{macdfss}":"macd_hist",
                    f"MACDs_{macdfss}":"macd_signal",
                    f"ADX_{ADX_LENGTH}":"adx",
                    f"ADXR_{ADX_LENGTH}_2":"adxr",
                    f"DMP_{ADX_LENGTH}":"di_plus",
                    f"DMN_{ADX_LENGTH}":"di_minus",
                    f"EMA_{EMA_SHORT}":"ema_short",
                    f"EMA_{EMA_LONG}":"ema_long",
                    f"BBL_{bblenst}":"bb_lower",
                    f"BBM_{bblenst}":"bb_middle",
                    f"BBU_{bblenst}":"bb_upper",
                    f"BBB_{bblenst}":"bb_bandwidth",
                    f"BBP_{bblenst}":"bb_percent",
                    f"DCU_20_20":"donchian_high",
                    f"DCL_20_20":"donchian_low",
                    f"DCM_20_20":"donchian_mid" 
                },
                inplace=True)
            # --- Indicadores de volumen ---
            data_by_ticker[ticker][timeframe].ta.sma(close="volume", length=20, append=True)
            data_by_ticker[ticker][timeframe].ta.ema(close="volume", length=20, append=True)
            data_by_ticker[ticker][timeframe].ta.obv(append=True)
            data_by_ticker[ticker][timeframe].ta.cmf(length=20, append=True)
            data_by_ticker[ticker][timeframe].ta.mfi(length=14, append=True)

            # --- Renombrar para evitar choques con EMA/SMA de precios ---
            data_by_ticker[ticker][timeframe].rename(columns={
                "SMA_20": "vol_sma20",
                "EMA_20": "vol_ema20",
                "OBV": "obv",
                "CMF_20": "cmf",
                "MFI_14": "mfi"
            }, inplace=True)
            
            # 🔒 Forzar todas las columnas a existir aunque no se calculen
            all_ind_cols = [
                "rsi", "macd", "macd_hist", "macd_signal",
                "adx", "adxr", "di_plus", "di_minus",
                "ema_short", "ema_long",
                "bb_lower", "bb_middle", "bb_upper", "bb_bandwidth", "bb_percent",
                "donchian_high", "donchian_low", "donchian_mid",
                "vol_sma20", "vol_ema20", "obv", "cmf", "mfi"]

            for col in all_ind_cols:
                if col not in df.columns:
                    df[col] = np.nan #pd.NA

            #print(f"  -> {timeframe} después: {df.columns.to_list()}")
    
    return data_by_ticker


#print(data_by_ticker["AAPL"]["1d"])

# --------------------------------------------------
# 10) Procesar señales BUY/SELL
# --------------------------------------------------
# - Usar lógica basada en indicadores calculados
# - Guardar resultados o exportar

def det_buy_sell(data_by_ticker: DictOfDict) -> DictOfDict:
    

    for ticker,tfs in data_by_ticker.items():
        for timeframe,df in tfs.items():

            # pd.DataFrame=data_by_ticker[ticker][timeframe]

        # definición de señales buy/sell .... basada en cruce de MACD y niveles de RSI .... son imediatas
            #contrucyendo el mask de filtro
            notna=data_by_ticker[ticker][timeframe][['rsi','macd']].notna().all(axis=1) #true = not na, para quitar los registros no validos
            cross_up=data_by_ticker[ticker][timeframe]['macd_hist'].gt(0)&data_by_ticker[ticker][timeframe]['macd_hist'].shift(1).le(0) #detecta cuando macdhist se vuelve negativo
            cross_down=data_by_ticker[ticker][timeframe]['macd_hist'].lt(0)&data_by_ticker[ticker][timeframe]['macd_hist'].shift(1).ge(0) #detecta cuando macdhist se vuelve positivo
            ok_buy = data_by_ticker[ticker][timeframe]['rsi'].lt(RSI_OB) #detecta cuando rsi dice sobreventa = va a subir
            ok_sell = data_by_ticker[ticker][timeframe]['rsi'].gt(RSI_OS) #detecta cuando rsi dice sobrecompra = va a bajar

            #definicion de las seniales buy sell
            signal='none'
            data_by_ticker[ticker][timeframe].loc[notna & cross_up & ok_buy,'signal']='buy' #busca las lineas que cumplen la condicion y asigna buy en la columna signal. si no existe la crea
            data_by_ticker[ticker][timeframe].loc[notna & cross_down & ok_sell,'signal']='sell'
        
        # definición de bias buy/sell/neutral .... basada en tendencia de MACD, EMAs y ADX .... es mas lenta
            bias_buy=(data_by_ticker[ticker][timeframe]['macd_hist'].gt(0))&(data_by_ticker[ticker][timeframe]['ema_short'].gt(data_by_ticker[ticker][timeframe]['ema_long']))&(data_by_ticker[ticker][timeframe]['adx'].gt(20))
            bias_sell=(data_by_ticker[ticker][timeframe]['macd_hist'].lt(0))&(data_by_ticker[ticker][timeframe]['ema_short'].lt(data_by_ticker[ticker][timeframe]['ema_long']))&(data_by_ticker[ticker][timeframe]['adx'].gt(20))
            
            data_by_ticker[ticker][timeframe].loc[bias_buy,'bias']='buy'
            data_by_ticker[ticker][timeframe].loc[bias_sell,'bias']='sell'
            data_by_ticker[ticker][timeframe]['bias'] = data_by_ticker[ticker][timeframe]['bias'].fillna('neutral')
    
    return data_by_ticker

def _pct(cur, base):
    '''Calculate % Variance between 2 numbers .... the return is alreayd multiplicated x 100'''
    try:
        if cur is None or base is None or base == 0:
            return 0.0
        return (cur / base - 1.0) * 100.0
    except Exception:
        return 0.0


def get_deltas(data_by_ticker :DictOfDict, ticker: str, timeframe: str = "1d", weekly_mode: str = "friday"):
    """
    Devuelve: d1, d2, d5, df1, df2  (todos float, en %)
      d1  = tf close current vs -1 día hábil
      d2  = tf close current vs -2 días hábiles
      d5  = tf close current vs -5 días hábiles
      df1 = tf close current vs último viernes
      df2 = tf close current vs viernes previo al último
    - tf close current se toma como el último close del dataframe del timeframe pedido (df_tf).
    - Si faltan datos, retorna 0.0 sin tronar.
    """
    tfs = data_by_ticker.get(ticker, {})
    df_tf = tfs.get(timeframe)
    df_1d = tfs.get("1d")

    # Guards
    if df_tf is None or df_tf.empty or "close" not in df_tf.columns:
        return 0.0, 0.0, 0.0, 0.0, 0.0
    if df_1d is None or df_1d.empty or "close" not in df_1d.columns:
        # sin diario no podemos calcular d1/d2/d5 ni viernes
        return 0.0, 0.0, 0.0, 0.0, 0.0

    # Precio "hoy" (último close del timeframe solicitado)
    p_now = pd.to_numeric(df_tf["close"].iloc[-1], errors="coerce")
    if pd.isna(p_now):
        return 0.0, 0.0, 0.0, 0.0, 0.0

    # Serie de cierres diarios
    closes = pd.to_numeric(df_1d["close"], errors="coerce")
    n = len(closes)

    p_1d = closes.iloc[-2] if n >= 2 else None
    p_2d = closes.iloc[-3] if n >= 3 else None
    p_5d = closes.iloc[-6] if n >= 6 else None  # 5 trading days back

    d1 = _pct(p_now, p_1d)
    d2 = _pct(p_now, p_2d)
    d5 = _pct(p_now, p_5d)

    # Viernes
    df1 = df2 = 0.0
    if weekly_mode == "friday":
        idx = pd.to_datetime(df_1d.index)
        fridays = closes[idx.weekday == 4].dropna() #get a list of fridays
        if len(fridays) >= 1:
            df1 = _pct(p_now, fridays.iloc[-1]) #timestamp of last friday
        if len(fridays) >= 2:
            df2 = _pct(p_now, fridays.iloc[-2]) #timestamp of the friday before last friday

    return float(d1), float(d2), float(d5), float(df1), float(df2)

def intraday_deltas(
    data_by_ticker: dict,
    ticker: str,
    tf_intraday: str,                 # ej. "1h", "30m", "15m"
    bar_offsets: list[int] = [1, 4, 12],   # vs -1, -4, -12 barras intradía
    day_offsets: list[int] = [1, 2, 5],    # vs -1d, -2d, -5d (trading days)
    friday_refs: bool = True               # vs último viernes y el anterior
) -> dict[str, float]:
    """
    Devuelve deltas % vs varias referencias para un timeframe intradía.
    Keys:
      - bar_-N  -> vs N barras atrás (intraday)
      - d_-N    -> vs N días hábiles atrás (cierre diario)
      - fri_1   -> vs último viernes
      - fri_2   -> vs viernes previo
    """
    tfs = data_by_ticker.get(ticker, {})
    dfi = tfs.get(tf_intraday)     # dataframe intradía
    dfd = tfs.get("1d")            # dataframe diario

    out = {}

    # Guardas
    if dfi is None or dfi.empty:
        return out  # nada intradía, devolvemos vacío
    if "close" not in dfi.columns:
        return out

    # Precio "ahora" = último close intradía
    p_now = float(pd.to_numeric(dfi["close"].iloc[-1], errors="coerce"))

    # ---------- 1) vs barras intradía ----------
    closes_i = pd.to_numeric(dfi["close"], errors="coerce")
    n_i = len(closes_i)
    for k in bar_offsets:
        key = f"bar_-{k}"
        base = float(closes_i.iloc[-(k+1)]) if n_i >= (k+1) else None
        out[key] = _pct(p_now, base)

    # ---------- 2) vs cierres diarios ----------
    if dfd is not None and not dfd.empty and "close" in dfd.columns:
        closes_d = pd.to_numeric(dfd["close"], errors="coerce")
        n_d = len(closes_d)
        for k in day_offsets:
            key = f"d_-{k}"
            base = float(closes_d.iloc[-(k+1)]) if n_d >= (k+1) else None
            out[key] = _pct(p_now, base)

        # ---------- 3) opcional: último viernes y el anterior ----------
        if friday_refs:
            dfd_idx = pd.to_datetime(dfd.index)
            fridays = closes_d[dfd_idx.weekday == 4].dropna()
            out["fri_1"] = _pct(p_now, float(fridays.iloc[-1])) if len(fridays) >= 1 else 0.0
            out["fri_2"] = _pct(p_now, float(fridays.iloc[-2])) if len(fridays) >= 2 else 0.0

        # Ejemplo:
        # r = intraday_deltas(reto_db, "AAPL.MX", "1h", bar_offsets=(1,4,12), day_offsets=(1,2,5), friday_refs=True)
        # print(r)  # {'bar_-1': ..., 'bar_-4': ..., 'bar_-12': ..., 'd_-1': ..., 'd_-2': ..., 'd_-5': ..., 'fri_1': ..., 'fri_2': ...}

    return out

# -----------------------------------------------------------------------------
# ℹ️ Nota importante sobre indicadores y timeframes
#
# Todos los indicadores (RSI, MACD, ADX, EMAs, Bollinger, etc.) se calculan
# en base a N "periodos". El periodo depende del timeframe que estemos usando.
#
# Ejemplos:
#   • RSI(14)
#       - En 1d  → usa 14 velas diarias = 14 días (~3 semanas hábiles)
#       - En 2h  → usa 14 velas de 2h   = 28 horas de trading (~3-4 días)
#       - En 15m → usa 14 velas de 15m  = 210 minutos (~3.5 horas)
#
#   • MACD (12,26,9)
#       - En 1d  → compara EMA12d vs EMA26d (≈ 12 y 26 días)
#       - En 2h  → compara EMA12(2h) vs EMA26(2h) (≈ 1 y 2.5 días)
#       - En 15m → compara EMA12(15m) vs EMA26(15m) (≈ 3 y 6.5 horas)
#
#   • ADX(14)
#       - En 1d  → mide fuerza de tendencia en los últimos 14 días
#       - En 2h  → mide fuerza en los últimos 28h
#       - En 15m → mide fuerza en las últimas 3.5h
#
#   • EMA50 / EMA200
#       - En 1d  → 50 y 200 días
#       - En 2h  → 50 y 200 velas de 2h (≈ 100h y 400h)
#       - En 15m → 50 y 200 velas de 15m (≈ 12.5h y 50h)
#
#   • Bollinger Bands (20,2)
#       - En 1d  → 20 días
#       - En 2h  → 20 velas de 2h (≈ 40h)
#       - En 15m → 20 velas de 15m (≈ 5h)
#
# 🚨 Conclusión:
# - El "14" o "20" siempre significa "14 o 20 velas del timeframe actual".
# - Por eso, RSI(14) en diario mide 3 semanas, pero en 15m mide solo 3.5h.
# - No da lo mismo aplicarlo en todos los timeframes: cada escala cuenta
#   una historia diferente. Para análisis robusto se recomienda usar
#   multi-timeframe (ej. tendencia en 1d, entradas en 15m).
# -----------------------------------------------------------------------------
