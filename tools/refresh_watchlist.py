import logging
import sys
from pathlib import Path

# Ajustar path para importar módulos del proyecto
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from svc_v2.db import Database
from svc_v2.screener import ScreenerEngine
from svc_v2.config_loader import load_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    print("🚀 REFRESHING DYNAMIC WATCHLIST (SQL-ONLY) 🚀")
    
    cfg = load_settings()
    db = Database(f"data/{cfg.system.db_filename}")
    eng = ScreenerEngine(db)
    
    strategies = ["BUY_BOUNCE", "BUY_TREND", "SELL_STRENGTH"]
    total_added = 0

    # Limpiar lo que ya expiró antes de agregar nuevos
    print("   🧹 Limpiando candidatos expirados...")
    db.conn.execute("DELETE FROM dynamic_watchlist WHERE expires_at < now()")

    # Identificar holdings para SELL_STRENGTH
    # HoldingConfig puede ser objeto o string
    holdings = []
    for h in cfg.portfolios.holdings:
        if hasattr(h, 'ticker'): holdings.append(h.ticker)
        else: holdings.append(str(h))
    
    for strat in strategies:
        print(f"   🔍 Ejecutando {strat}...")
        
        candidates = eng.run_screen(strat)
        
        if candidates.empty:
            print("      (Sin candidatos)")
            continue

        # FILTER: SELL_STRENGTH only for holdings
        if strat == "SELL_STRENGTH":
            original_count = len(candidates)
            candidates = candidates[candidates['ticker'].isin(holdings)]
            print(f"      🛡️ Filtrado SELL_STRENGTH: {original_count} -> {len(candidates)} (Solo Holdings)")
        
        if not candidates.empty:
            print(f"      ✅ Encontrados {len(candidates)} candidatos.")
            for t in candidates['ticker'].tolist():
                # Agregar por 3 días por defecto
                db.add_to_dynamic_watchlist(t, reason=strat, days_to_keep=3)
                total_added += 1
        else:
            print("      (Ninguno tras filtro)")

    print(f"\n✨ Watchlist actualizada. Total candidatos activos: {total_added}")
    db.close()

if __name__ == "__main__":
    main()
