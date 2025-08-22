
import os
import yaml
from dotenv import load_dotenv
import sys
from svc import collector, analyzer, notifier

def assign_ticker_market(tickers, default_market):
    """
    Determina el mercado de un ticker basado en la configuración.
    Si no se encuentra, usa el mercado por defecto.
    """
    ticker_market, market_tickers={}, {}

    for ticker in tickers:
        if ticker.endswith(tuple(suffix_map.keys())):
            result= suffix_map.get('.' + ticker.split('.')[-1])
            ticker_market[ticker]=result
            market_tickers.setdefault(result,[]).append(ticker)
             
        else:
            ticker_market[ticker]=default_market
            market_tickers.setdefault(default_market,[]).append(ticker)
       

    return ticker_market, market_tickers


# --------------------------------------------------
# MAIN
# --------------------------------------------------
if __name__ == "__main__":

    # --------------------------------------------------
    # 2) Cargar configuración (config.yaml)
    # --------------------------------------------------
    # - Usar ruta relativa al directorio raíz del proyecto
    # - Validar que el archivo exista
    # - Guardar en variable global CONFIG
    with open("config/config.yaml", "r") as f:
        config = yaml.safe_load(f)

    tickers=config["data"]["tickers"]
    
    save_directory=config["storage"]["path_data"]
    
    rsi_length=config["data"]["indicators"]["rsi"]["period"] 
    rsi_ob=config["data"]["indicators"]["rsi"]["overbought"]
    rsi_os=config["data"]["indicators"]["rsi"]["oversold"]

    macd_fast=config["data"]["indicators"]["macd"]["fast"]
    macd_slow=config["data"]["indicators"]["macd"]["slow"]
    macd_signal=config["data"]["indicators"]["macd"]["signal"]

    suffix_map=config["routing"]["by_suffix"]
    
    markets=config["markets"]

    default_market=config["routing"]["default"]

    ticker_to_market,market_to_ticker=assign_ticker_market(tickers, default_market)
 



    # --------------------------------------------------
    # 3) Cargar secretos (secrets.env)
    # --------------------------------------------------
    # - Usar dotenv para exponer en variables de entorno``
    # - Validar que se hayan cargado las claves necesarias
    # - Guardar en variables como API_KEY, API_SECRET, etc.
    load_dotenv("config/secrets.env")

    telegram_token = os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")



    # Validar que el token exista
    if not telegram_token:
        sys.exit("❌ ERROR: No se encontró TELEGRAM_TOKEN en config/secrets.env")


    dict=collector.download_tickers(tickers,ticker_to_market,market_to_ticker,markets)

    collector.save_local(dict,save_directory)

    analyzer.analyse_data(dict,rsi_length,macd_fast,macd_slow,macd_signal)

    analyzer.det_buy_sell(dict,rsi_ob,rsi_os)

    message=""
    urgentmessage="Urgent signals:\n"

    for ticker in tickers:
        lastrecord=dict[ticker]["2h"].tail(1)
 
        if lastrecord['signal'].iloc[-1] =="BUY":
                signalmsg="🟢"
        elif lastrecord['signal'].iloc[-1] =="SELL":
                signalmsg="🔴"
        else:
                signalmsg="⏸️"

        message+=f"<b>{ticker}</b> = Value:{lastrecord['close'].iloc[-1]:.2f} {signalmsg} RSI:{lastrecord['rsi'].iloc[-1]:.0f} MACD:{lastrecord['macd_hist'].iloc[-1]:.2f}" + "\n"

    notifier.send_msg(telegram_token, chat_id,message)

    for ticker in tickers:
        lastrecord=dict[ticker]["15m"].tail(1)
        if lastrecord['signal'].iloc[-1] =="BUY":
            signalmsg="🟢"
        elif lastrecord['signal'].iloc[-1] =="SELL":
            signalmsg="🔴"
        else:
            signalmsg="⏸️"
        if lastrecord['signal'].iloc[-1] in ("BUY","SELL"):
            urgentmessage+=f"<b>{ticker}</b> = Value:{lastrecord['close'].iloc[-1]:.2f} {signalmsg} RSI:{lastrecord['rsi'].iloc[-1]:.0f} MACD:{lastrecord['macd_hist'].iloc[-1]:.2f}"
            notifier.send_msg(telegram_token, chat_id,message)
    
