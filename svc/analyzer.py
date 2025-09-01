import pandas_ta as ta
import pandas as pd
from svc.load_config import RSI_LENGTH, RSI_OB, RSI_OS, MACD_FAST, MACD_SLOW, MACD_SIGNAL, ADX_LENGTH, EMA_SHORT, EMA_LONG, BB_LENGTH, BB_STD

# --------------------------------------------------
# 9) Aplicar indicadores técnicos (pandas_ta)
# --------------------------------------------------
# - Ejemplo: MACD, RSI, EMA, etc.
# - Agregar columnas al DataFrame

def analyse_data(data_by_ticker):

    tickers=list(data_by_ticker.keys())

    macdfss=f"{MACD_FAST}_{MACD_SLOW}_{MACD_SIGNAL}"
    bblenst=f"{BB_LENGTH}_{BB_STD}"


    for ticker in tickers:
        for timeframe in ["1d","1h","2h","15m"]:
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
                    f"DMP_{ADX_LENGTH}":"di+",
                    f"DMN_{ADX_LENGTH}":"di-",
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
    
    return data_by_ticker


#print(data_by_ticker["AAPL"]["1d"])

# --------------------------------------------------
# 10) Procesar señales BUY/SELL
# --------------------------------------------------
# - Usar lógica basada en indicadores calculados
# - Guardar resultados o exportar

def det_buy_sell(data_by_ticker):
    
    tickers=list(data_by_ticker.keys())


    for ticker in tickers:
        for timeframe in ["1d","2h","1h","15m"]:

            pd.DataFrame=data_by_ticker[ticker][timeframe]

        # definición de señales buy/sell .... basada en cruce de MACD y niveles de RSI .... son imediatas
            #contrucyendo el mask de filtro
            notna=data_by_ticker[ticker][timeframe][['rsi','macd']].notna().all(axis=1) #true = not na, para quitar los registros no validos
            cross_up=data_by_ticker[ticker][timeframe]['macd_hist'].gt(0)&data_by_ticker[ticker]["2h"]['macd_hist'].shift(1).le(0) #detecta cuando macdhist se vuelve negativo
            cross_down=data_by_ticker[ticker][timeframe]['macd_hist'].lt(0)&data_by_ticker[ticker]["2h"]['macd_hist'].shift(1).ge(0) #detecta cuando macdhist se vuelve positivo
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

def get_deltas(data_by_ticker, ticker, timeframe):

    df_tf = data_by_ticker[ticker][timeframe]
    df_1d = data_by_ticker[ticker]["1d"]

    if len(df_tf) >2:
        delta_prev = (df_tf['close'].iloc[-1] - df_tf['close'].iloc[-2]) / df_tf['close'].iloc[-2] * 100
    else:   
        delta_prev = 0.0

    if len(df_1d) >2:
        delta_lastday = (df_tf['close'].iloc[-1] - df_1d['close'].iloc[-2]) / df_1d['close'].iloc[-2] * 100
    else:   
        delta_lastday = 0.0
   
    return delta_prev, delta_lastday


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
