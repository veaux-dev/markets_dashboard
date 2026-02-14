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

    # ... (Carga de config)
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
    # Importamos loaders para replicar la lógica de broad_scan
    from svc_v2.universe_loader import get_sp500_tickers, get_nasdaq100_tickers, get_key_etfs_indices
    
    sp500 = [t[0] for t in get_sp500_tickers()]
    ndx100 = [t[0] for t in get_nasdaq100_tickers()]
    etfs = [t[0] for t in get_key_etfs_indices()]
    full_universe = list(set(sp500 + ndx100 + etfs + vip_tickers))
    
    print(f"   -> Universo Completo: {len(full_universe)} activos.")
    print(f"   -> Activos VIP (Alertas): {len(vip_tickers)}")

    # 4. Loop por Timeframe Intradía
    # Aseguramos que solo bajamos intradía aqui (1h, 15m)
    timeframes = [tf for tf in cfg.data.timeframes.get('detailed', ['1h', '15m']) if tf != '1d']
    
    for tf in timeframes:
        print(f"\n⏱️  Timeframe: {tf}")
        
        # A) Sync TODO el universo
        col.sync_tickers(full_universe, [tf])
        
        # B) Analyze TODO el universo
        force_full = os.environ.get("FORCE_FULL_SCAN") == "1"
        alz.analyze_tickers(full_universe, [tf], force_full=force_full)
        
        # C) Screen (Alertas solo para VIP)
        print(f"   🔎 Evaluando Alertas VIP:")
        strategies = {
            "BUY_BOUNCE": "Rebote / Sobrevendido",
            "SELL_STRENGTH": "Euforia / Sobrecompra",
            "BUY_TREND": "Tendencia Fuerte"
        }
        
        for strat_key, label in strategies.items():
            candidates = eng.run_screen(strat_key, timeframe=tf)
            # Solo nos interesan alertas de los que están en VIP
            candidates = candidates[candidates['ticker'].isin(vip_tickers)]
            
            if candidates.empty:
                continue

            # ... resto de la lógica de notificación ...
            
            if candidates.empty:
                continue

            # Enriquecer
            candidates['name'] = candidates['ticker'].map(name_map).fillna(candidates['ticker'])
            candidates['name'] = candidates['name'].astype(str).str.slice(0, 20)
            
            # Separar Holdings vs Resto
            is_holding_mask = candidates['ticker'].isin(all_holdings)
            df_holdings = candidates[is_holding_mask]
            df_market = candidates[~is_holding_mask]

            if not df_holdings.empty:
                print(f"\n   🚨 {label} [MY HOLDINGS] ({len(df_holdings)})")
                for _, row in df_holdings.iterrows():
                    notif.notify_strategy_hit(
                        ticker=row['ticker'], 
                        strategy=strat_key, 
                        timeframe=tf, 
                        price=row['close'],
                        extra_info=f"⚠️ Holding Position: {row['name']}"
                    )
            
            if not df_market.empty:
                print(f"\n   🔭 {label} [MARKET] ({len(df_market)})")
                for _, row in df_market.iterrows():
                    notif.notify_strategy_hit(
                        ticker=row['ticker'], 
                        strategy=strat_key, 
                        timeframe=tf, 
                        price=row['close'],
                        extra_info=f"Candidate from Watchlist: {row['name']}"
                    )

    print("\n✅ Detailed Scan Finalizado.")
    db.close()

if __name__ == "__main__":
    main()
