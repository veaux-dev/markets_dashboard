import logging
import os
import pandas as pd
from svc_v2.config_loader import load_settings
from svc_v2.db import Database
from svc_v2.collector import Collector
from svc_v2.analyzer import Analyzer
from svc_v2.screener import ScreenerEngine
from svc_v2.notifier import Notifier

# Configurar logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    print("\n🔬 MARKET DASHBOARD V2: Detailed Scan (Intraday) 🔬\n")

    # 1. Cargar Configuración
    try:
        cfg = load_settings()
    except Exception as e:
        logging.error(f"Fallo crítico cargando configuración: {e}")
        return

    # 2. Init System
    db = Database(f"data/{cfg.system.db_filename}")
    col = Collector(db)
    alz = Analyzer(db)
    eng = ScreenerEngine(db)
    notif = Notifier(db)

    # 3. Construir Universos
    print("🌌 Cargando Universos...")
    
    # A) Universo VIP (Holdings + Watchlist Manual + Dynamic) para Notificaciones
    try:
        my_holdings_df = db.conn.execute("SELECT ticker FROM view_portfolio_holdings").df()
        my_holdings = set(my_holdings_df['ticker'].tolist()) if not my_holdings_df.empty else set()
    except Exception:
        my_holdings = set()

    holding_tickers = [h.ticker if hasattr(h, 'ticker') else h for h in cfg.portfolios.holdings]
    all_holdings = my_holdings.union(set(holding_tickers))
    
    static_tickers = cfg.universe.watchlist
    dynamic_tickers = db.get_dynamic_watchlist()
    vip_tickers = list(all_holdings.union(set(static_tickers + dynamic_tickers)))
    
    # B) Universo Completo (Para Descarga y Análisis)
    from svc_v2.universe_loader import get_sp500_tickers, get_nasdaq100_tickers, get_key_etfs_indices
    
    sp500 = [t[0] for t in get_sp500_tickers()]
    ndx100 = [t[0] for t in get_nasdaq100_tickers()]
    etfs = [t[0] for t in get_key_etfs_indices()]
    full_universe = list(set(sp500 + ndx100 + etfs + vip_tickers))
    
    print(f"   -> Universo Completo: {len(full_universe)} activos.")
    print(f"   -> Activos VIP (Alertas): {len(vip_tickers)}")

    # Obtener mapa de nombres para el reporte
    try:
        q_names = f"SELECT ticker, name FROM ticker_metadata WHERE ticker IN ({','.join([f"'{t}'" for t in vip_tickers])})"
        name_map = db.conn.execute(q_names).df().set_index('ticker')['name'].to_dict()
    except Exception:
        name_map = {}

    # 4. Loop por Timeframe Intradía
    timeframes = [tf for tf in cfg.data.timeframes.get('detailed', ['1h', '15m']) if tf != '1d']
    
    for tf in timeframes:
        print(f"\n⏱️  Timeframe: {tf}")
        
        # A) Sync & Analyze TODO el universo
        col.sync_tickers(full_universe, [tf])
        alz.analyze_tickers(full_universe, [tf], force_full=(os.environ.get("FORCE_FULL_SCAN") == "1"))
        
        # B) Screen & Batch Notif
        print(f"   🔎 Evaluando Alertas VIP...")
        strategies = {
            "BUY_BOUNCE": "Rebote",
            "SELL_STRENGTH": "Euforia",
            "BUY_TREND": "Trend"
        }
        
        batch_holdings = []
        batch_market = []

        for strat_key, label in strategies.items():
            candidates = eng.run_screen(strat_key, timeframe=tf)
            # Solo VIPs
            candidates = candidates[candidates['ticker'].isin(vip_tickers)]
            
            for _, row in candidates.iterrows():
                signal_data = {
                    'ticker': row['ticker'],
                    'strategy': strat_key,
                    'price': row['close'],
                    'name': name_map.get(row['ticker'], row['ticker'])
                }
                
                if row['ticker'] in all_holdings:
                    batch_holdings.append(signal_data)
                else:
                    batch_market.append(signal_data)

        # Enviar Batch Consolidado
        if batch_holdings:
            print(f"   🚨 Enviando batch de {len(batch_holdings)} alertas de HOLDINGS...")
            notif.notify_batch(batch_holdings, title_prefix="🚨 MY HOLDINGS", timeframe=tf)
        
        if batch_market:
            print(f"   🔭 Enviando batch de {len(batch_market)} alertas de MARKET...")
            notif.notify_batch(batch_market, title_prefix="🔭 MARKET SCAN", timeframe=tf)

    print("\n✅ Detailed Scan Finalizado.")
    db.close()

if __name__ == "__main__":
    main()
