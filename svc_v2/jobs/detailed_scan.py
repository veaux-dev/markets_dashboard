import logging
import pandas as pd
from svc_v2.config_loader import load_settings
from svc_v2.db import Database
from svc_v2.collector import Collector
from svc_v2.analyzer import Analyzer
from svc_v2.screener import ScreenerEngine

# Configurar logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    print("\n🔬 MARKET DASHBOARD V2: Detailed Scan (Intraday) 🔬\n")

    # 1. Cargar Configuración
    try:
        cfg = load_settings()
        # Verificar Market Hours (Simple check, el Daemon lo hace pero doble seguridad)
        # Omitimos logica compleja de hora aqui, asumimos que si corre es porque toca.
    except Exception as e:
        logging.error(f"Fallo crítico cargando configuración: {e}")
        return

    # 2. Init System
    db = Database(f"data/{cfg.system.db_filename}")
    col = Collector(db)
    alz = Analyzer(db)
    eng = ScreenerEngine(db)

    # 3. Construir Universo (SOLO Watchlist + Holdings + Dynamic Candidates)
    print("🌌 Cargando Universo VIP (Watchlist + Holdings + Dynamic)...")
    
    # A) Estáticos (Config) - Extraer tickers si son objetos HoldingConfig
    holding_tickers = [h.ticker if hasattr(h, 'ticker') else h for h in cfg.portfolios.holdings]
    static_tickers = cfg.universe.watchlist + holding_tickers
    
    # B) Dinámicos (DB - Candidatos recientes del Broad Scan)
    dynamic_tickers = db.get_dynamic_watchlist()
    
    # Unificar y deduplicar
    vip_tickers = list(set(static_tickers + dynamic_tickers))
    
    if not vip_tickers:
        print("   ⚠️ No hay activos VIP para escanear.")
        return

    print(f"   -> Origen: {len(static_tickers)} Config + {len(dynamic_tickers)} Dynamic DB")
    
    # Recuperar nombres de la DB si existen (para el reporte)
    q_names = f"SELECT ticker, name FROM ticker_metadata WHERE ticker IN ({','.join([f"'{t}'" for t in vip_tickers])})"
    try:
        name_map = db.conn.execute(q_names).df().set_index('ticker')['name'].to_dict()
    except Exception as e:
        logging.debug(f"Could not load names: {e}")
        name_map = {}
        
    print(f"   -> Escaneando {len(vip_tickers)} activos VIP.")

    # 4. Loop por Timeframe Intradía
    # Por defecto ['1h', '15m']
    timeframes = cfg.data.timeframes.get('detailed', ['1h', '15m'])
    
    for tf in timeframes:
        if tf == '1d': continue # Broad scan ya hace esto
        
        print(f"\n⏱️  Timeframe: {tf}")
        
        # A) Sync (Incremental)
        # Collector ya maneja la lógica de pedir solo lo reciente si existe historia
        col.sync_tickers(vip_tickers, [tf])
        
        # B) Analyze (Incremental)
        alz.analyze_tickers(vip_tickers, [tf], force_full=False)
        
        # C) Screen
        print(f"   🔎 Buscando oportunidades en {tf}...")
        strategies = {
            "BUY_BOUNCE": "Rebote / Sobrevendido",
            "SELL_STRENGTH": "Euforia / Sobrecompra",
            "BUY_TREND": "Tendencia Fuerte"
        }
        
        for strat_key, label in strategies.items():
            candidates = eng.run_screen(strat_key, timeframe=tf)
            
            # Filtrar VIP
            candidates = candidates[candidates['ticker'].isin(vip_tickers)]
            
            if not candidates.empty:
                candidates['name'] = candidates['ticker'].map(name_map).fillna(candidates['ticker'])
                candidates['name'] = candidates['name'].astype(str).str.slice(0, 20)
                
                print(f"   👉 {label} ({strat_key}): {len(candidates)}")
                
                # Columnas dinámicas
                cols = ['ticker', 'name', 'close']
                if 'rsi' in candidates.columns: cols.append('rsi')
                if 'adx' in candidates.columns: cols.append('adx')
                if 'gap_pct' in candidates.columns: cols.append('gap_pct')
                
                print(candidates[cols].to_string(index=False))

    print("\n✅ Detailed Scan Finalizado.")
    db.close()

if __name__ == "__main__":
    main()
