import pandas_ta as ta
import pandas as pd

# --------------------------------------------------
# 9) Aplicar indicadores técnicos (pandas_ta)
# --------------------------------------------------
# - Ejemplo: MACD, RSI, EMA, etc.
# - Agregar columnas al DataFrame

def analyse_data(data_by_ticker,rsi_length, macd_fast,macd_slow,macd_signal):

    tickers=list(data_by_ticker.keys())

    macdfss=f"{macd_fast}_{macd_slow}_{macd_signal}"


    for ticker in tickers:
        for timeframe in ["1d","1h","2h","15m"]:
            data_by_ticker[ticker][timeframe].ta.rsi(length=rsi_length,append=True)
            data_by_ticker[ticker][timeframe].ta.macd(fast=macd_fast,slow=macd_slow,signal=macd_signal,append=True)
            data_by_ticker[ticker][timeframe].rename(columns={f"RSI_{rsi_length}":"rsi", f"MACD_{macdfss}":"macd",f"MACDh_{macdfss}":"macd_hist",f"MACDs_{macdfss}":"macd_signal"},inplace=True)
    
    return data_by_ticker


#print(data_by_ticker["AAPL"]["1d"])

# --------------------------------------------------
# 10) (Opcional) Procesar señales BUY/SELL
# --------------------------------------------------
# - Usar lógica basada en indicadores calculados
# - Guardar resultados o exportar

def det_buy_sell(data_by_ticker,rsi_ob,rsi_os):
    
    tickers=list(data_by_ticker.keys())

    for ticker in tickers:
        for timeframe in ["1d","2h","1h","15m"]:

            pd.DataFrame=data_by_ticker[ticker][timeframe]
            #contrucyendo el mask de filtro
            notna=data_by_ticker[ticker][timeframe][['rsi','macd']].notna().all(axis=1) #true = not na, para quitar los registros no validos
            cross_up=data_by_ticker[ticker][timeframe]['macd_hist'].gt(0)&data_by_ticker[ticker]["2h"]['macd_hist'].shift(1).le(0) #detecta cuando macdhist se vuelve negativo
            cross_down=data_by_ticker[ticker][timeframe]['macd_hist'].lt(0)&data_by_ticker[ticker]["2h"]['macd_hist'].shift(1).ge(0) #detecta cuando macdhist se vuelve positivo
            ok_buy = data_by_ticker[ticker][timeframe]['rsi'].lt(rsi_ob) #detecta cuando rsi dice sobreventa = va a subir
            ok_sell = data_by_ticker[ticker][timeframe]['rsi'].gt(rsi_os) #detecta cuando rsi dice sobrecompra = va a bajar

            #definicion de las seniales buy sell
            signal='none'
            data_by_ticker[ticker][timeframe].loc[notna & cross_up & ok_buy,'signal']='buy' #busca las lineas que cumplen la condicion y asigna buy en la columna signal. si no existe la crea
            data_by_ticker[ticker][timeframe].loc[notna & cross_down & ok_sell,'signal']='sell'
        
    return data_by_ticker