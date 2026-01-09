import sys
from svc import collector, analyzer, notifier, state, db_mgmt, screener
from svc.load_config import tickers, markets, suffix_map, default_market, telegram_token, chat_id, discord_webhook_url, save_directory
from svc import viz #EXPERIMENTAL NOT FOR PRODUCTION


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

    # Validar que el token exista
    if not telegram_token:
        sys.exit("❌ ERROR: No se encontró TELEGRAM_TOKEN en config/secrets.env")

    ticker_to_market,market_to_ticker=assign_ticker_market(tickers, default_market)

    snapshot_id=''
    alert_id={}
    last_state={'last_snapshot_ts':'', 'last_alerts':{}}
    new_state={'last_snapshot_ts':'', 'last_alerts':{}}
    

    last_state,snapshot_id,alert_id=state.load_state()
    
    working_db = collector.download_tickers(tickers, output_path="data")

    working_db= collector.save_local(working_db,'data')

    working_db = analyzer.analyse_data(working_db)

    working_db = analyzer.det_buy_sell(working_db)

    new_state=last_state #inicializo new_state con last_state para no perder alertas previas

    timestamps = [
        working_db[t]["2h"].index[-1]
        for t in tickers
        if "2h" in working_db.get(t, {}) and not working_db[t]["2h"].empty
    ]
    last_timestamp_2h = max(timestamps)



    send_snapshot=state.should_send_snapshot(last_timestamp_2h.isoformat(),last_state)

    message=""
    message_discord=""
    urgentmessage="Urgent signals:\n"

    if send_snapshot is True:
        message_discord=f"📊**Market Snapshot** - {last_timestamp_2h.isoformat()}\n\n"
        message_discord+=f"```{'TICKER':<12} {'CLOSE':>8} {'BIAS':^4} {'Δ-1c':>5}  {'Δ-1d':>5}  {'RSI':>3} {'MACD':>5} {'Support':>9} {'Resist.':>9}" + "\n"
        for ticker in tickers:
            lastrecord=working_db[ticker]["2h"].tail(1)
    
            if lastrecord['bias'].iloc[-1] =="buy":
                    signalmsg="🟢"
            elif lastrecord['bias'].iloc[-1] =="sell":
                    signalmsg="🔴"
            else:
                    signalmsg="⚪"

            dlc, dld, _, _, _ = analyzer.get_deltas(working_db,ticker,"2h")
            donch_low  = lastrecord['donchian_low'].iloc[-1]
            donch_high = lastrecord['donchian_high'].iloc[-1]
            
            message+=f"<b>{ticker}</b> = Value:{lastrecord['close'].iloc[-1]:.2f} {signalmsg} RSI:{lastrecord['rsi'].iloc[-1]:.0f} MACD:{lastrecord['macd_hist'].iloc[-1]:.2f}" + "\n"
            message_discord+=(
                 f"{ticker:<12} "
                 f"{lastrecord['close'].iloc[-1]:>8.2f} "
                 f"{signalmsg:^4} "
                 f"{dlc:>+5.1f}% {dld:>5.1f}% "
                 f"{lastrecord['rsi'].iloc[-1]:>3.0f} {lastrecord['macd_hist'].iloc[-1]:>+5.1f}" 
                 f"{donch_low:>9.2f} {donch_high:>9.2f}"
                 "\n"
            )

        message_discord+="```"
        notifier.send_msg(telegram_token, chat_id,message)
        notifier.send_discord(discord_webhook_url,message_discord)
        snapshot_id=last_timestamp_2h.isoformat()
    else:
        print("No new snapshot to send")

    missing_15m = []
    for ticker in tickers:
        if "15m" not in working_db.get(ticker, {}):
            missing_15m.append(ticker)
            continue
        lastrecord=working_db[ticker]["15m"].tail(1)
        if lastrecord['signal'].iloc[-1] =="BUY":
            signalmsg="🟢"
        elif lastrecord['signal'].iloc[-1] =="SELL":
            signalmsg="🔴"
        else:
            signalmsg="⏸️"
        if lastrecord['signal'].iloc[-1] in ("BUY","SELL"):
            urgentmessage+=f"<b>{ticker}</b> = Value:{lastrecord['close'].iloc[-1]:.2f} {signalmsg} RSI:{lastrecord['rsi'].iloc[-1]:.0f} MACD:{lastrecord['macd_hist'].iloc[-1]:.2f}"
            urgentmessage_discord=(
                f"🚨{lastrecord['signal'].iloc[-1]} Alert (15min)🚨\n\n"
                f"📈 **{ticker}**\n"
                f"Value: {lastrecord['close'].iloc[-1]:.2f} {signalmsg}{signalmsg}{signalmsg}\n"
                f"RSI: {lastrecord['rsi'].iloc[-1]:.0f}\n"
                f"MACD: {lastrecord['macd_hist'].iloc[-1]:.2f}\n"
               
            )
            notifier.send_msg(telegram_token, chat_id,message)
            notifier.send_discord(discord_webhook_url, urgentmessage_discord)
            alert_id[ticker]=lastrecord.index[-1]

    if missing_15m:
        print(f"⚠️ Missing 15m data for tickers: {', '.join(missing_15m)}")

    new_state={'last_snapshot_ts':snapshot_id,'last_alerts':alert_id}
    print(f'New Notification Timestamp to be loaded: {new_state}')

    state.save_state(new_state)

    # viz.quick_viz_triple_screen(dict["AAPL"], tick="AAPL") #EXPERIMENTAL NOT FOR PRODUCTION
    
