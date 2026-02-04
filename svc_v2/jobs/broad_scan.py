import logging
import pandas as pd
from svc_v2.config_loader import load_settings
from svc_v2.db import Database
from svc_v2.collector import Collector
from svc_v2.analyzer import Analyzer
from svc_v2.screener import ScreenerEngine
from svc_v2.universe_loader import get_sp500_tickers, get_nasdaq100_tickers, get_key_etfs_indices

# Configurar logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    print("\n⚔️ MARKET DASHBOARD V2: Broad Scan (Production) ⚔️\n")

    # 1. Cargar Configuración
    try:
        cfg = load_settings()
        logging.info(f"Configuración cargada: DB={cfg.system.db_filename}")
    except Exception as e:
        logging.error(f"Fallo crítico cargando configuración: {e}")
        return

    # 2. Init System
    # Usamos la ruta definida en settings (e.g., data/markets.duckdb)
    db_path = f"data/{cfg.system.db_filename}"
    db = Database(db_path)
    
    col = Collector(db)
    alz = Analyzer(db)
    eng = ScreenerEngine(db)

    # 3. Construir Universo (La Gran Fusión)
    print("🌌 Construyendo Universo...")
    universe_dict = {}

    # A) Listas Automáticas (S&P500 + NDX100 + ETFs Clave)
    print("   -> Descargando S&P 500, Nasdaq 100 y ETFs Clave...")
    sp500 = get_sp500_tickers()
    ndx100 = get_nasdaq100_tickers()
    etfs_key = get_key_etfs_indices()
    
    for t, n in sp500 + ndx100 + etfs_key:
        universe_dict[t] = n

    # B) Listas Manuales (Watchlist + Holdings del Config)
    # Extraer tickers de holdings (que pueden ser objetos HoldingConfig o strings)
    holding_tickers = [h.ticker if hasattr(h, 'ticker') else h for h in cfg.portfolios.holdings]
    manual_tickers = set(cfg.universe.watchlist + holding_tickers)
    
    print(f"   -> Agregando {len(manual_tickers)} tickers manuales (Watchlist + Holdings)...")
    for t in manual_tickers:
        if t not in universe_dict:
            universe_dict[t] = t # Nombre temporal = Ticker

    # Lista final deduplicada
    full_universe = list(universe_dict.keys())
    print(f"   -> Universo Total: {len(full_universe)} activos.")

    # 4. Guardar Metadatos (Nombres)
    # Importante para que el reporte salga bonito
    print("   -> Sincronizando metadatos...")
    for t, n in universe_dict.items():
        db.upsert_metadata(ticker=t, name=n)

    # 5. Sync Data (Collector)
    # Broad Scan siempre es Diario ('1d') según config default
    # Podríamos leer cfg.data.timeframes['broad'], pero asumimos '1d' por seguridad
    timeframes = cfg.data.timeframes.get('broad', ['1d'])
    col.sync_tickers(full_universe, timeframes)

    # 6. Analyze Data (Analyzer)
    # Force full=False para ser incrementales y rápidos
    alz.analyze_tickers(full_universe, timeframes, force_full=False)

    # 7. Execute Screeners (The Funnel)
    print("\n🔍 Ejecutando Filtros Tácticos...")
    
    strategies = {
        "🟢 BUY_BOUNCE (Pánico)": "BUY_BOUNCE",
        "🟡 BUY_TREND (U2)": "BUY_TREND",
        "🔴 SELL_STRENGTH (Euforia)": "SELL_STRENGTH"
    }
    
    for label, strat_key in strategies.items():
        candidates = eng.run_screen(strat_key)
        
        # Filtrar solo lo que está en nuestro universo actual (por si la DB tiene basura vieja)
        candidates = candidates[candidates['ticker'].isin(full_universe)]
        
        # Enriquecer con Nombre local (más rápido que JOIN en SQL si ya tenemos el dict)
        candidates['name'] = candidates['ticker'].map(universe_dict).fillna("Unknown")
        candidates['name'] = candidates['name'].astype(str).str.slice(0, 25) 
        
        print(f"\n{label}: {len(candidates)} candidatos")
        if not candidates.empty:
            # ---> GUARDAR EN DYNAMIC WATCHLIST <---
            print(f"   💾 Guardando {len(candidates)} candidatos para monitoreo intradía (3 días)...")
            for t in candidates['ticker'].tolist():
                db.add_to_dynamic_watchlist(t, reason=strat_key, days_to_keep=3)
            # --------------------------------------

            # Columnas dinámicas según estrategia
            base_cols = ['ticker', 'name', 'close']
            extra_cols = []
            if strat_key == "BUY_BOUNCE":
                extra_cols = ['gap_pct', 'chg_pct', 'rsi', 'vol_k']
            elif strat_key == "BUY_TREND":
                extra_cols = ['adx', 'ema_50', 'macd_hist']
            elif strat_key == "SELL_STRENGTH":
                extra_cols = ['rsi', 'vol_k']
            
            # Asegurar que existan
            final_cols = base_cols + [c for c in extra_cols if c in candidates.columns]
            print(candidates[final_cols].to_string(index=False))
        else:
            print("   (Ninguno)")

    # 8. Earnings Check
    print("\n📅 Próximos Earnings (< 5 días)...")
    q_earn = f"""
    SELECT ticker, name, next_earnings 
    FROM ticker_metadata 
    WHERE next_earnings BETWEEN now() AND now() + INTERVAL 5 DAY
    ORDER BY next_earnings ASC
    """
    try:
        earns = db.conn.execute(q_earn).df()
        # Filtrar universo
        earns = earns[earns['ticker'].isin(full_universe)]
        if not earns.empty:
            print(earns.to_string(index=False))
        else:
            print("   (Ninguno)")
    except Exception as e:
        print(f"Error checking earnings: {e}")

    print("\n✅ Broad Scan Finalizado.")
    db.close()

if __name__ == "__main__":
    main()