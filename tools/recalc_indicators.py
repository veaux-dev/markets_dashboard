import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from svc_v2.db import Database
from svc_v2.analyzer import Analyzer
from svc_v2.config_loader import load_settings

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    print("🧠 FORCING INDICATOR RECALCULATION (Full History)...")
    
    cfg = load_settings()
    db = Database(f"data/{cfg.system.db_filename}")
    alz = Analyzer(db)
    
    # Obtener todos los tickers que tienen datos en OHLCV
    print("   -> Identificando tickers con datos...")
    tickers = db.conn.execute("SELECT DISTINCT ticker FROM ohlcv").df()["ticker"].tolist()
    print(f"   -> {len(tickers)} activos a procesar.")

    # Timeframes
    timeframes = ["1d", "1h", "15m"]
    
    # Ejecutar con force_full=True
    alz.analyze_tickers(tickers, timeframes, force_full=True)
    
    print("\n✅ Recálculo completado.")

if __name__ == "__main__":
    main()

