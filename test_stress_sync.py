import logging
from svc_v2.db import Database
from svc_v2.collector import Collector
from svc_v2.universe_loader import get_sp500_tickers, get_nasdaq100_tickers, get_key_etfs_indices
import os

# Configurar logs para ver TODO
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def test_sync():
    # Usar una DB temporal local para no ensuciar nada
    db_path = "data/test_stress.duckdb"
    if os.path.exists(db_path): os.remove(db_path)
    
    db = Database(db_path)
    col = Collector(db)

    print("🌌 Cargando universo completo para la prueba...")
    sp500 = get_sp500_tickers()
    ndx100 = get_nasdaq100_tickers()
    etfs = get_key_etfs_indices()
    
    tickers = list(set([t for t, n in sp500 + ndx100 + etfs]))
    print(f"🚀 Iniciando sync de {len(tickers)} activos...")

    # Probar sync de 1d (Daily)
    try:
        col.sync_tickers(tickers, ["1d"])
        print("\n✅ Sync finalizado sin excepciones críticas.")
        
        # Verificar que la DB tenga datos
        count = db.conn.execute("SELECT count(*) FROM ohlcv").fetchone()[0]
        unique_tickers = db.conn.execute("SELECT count(distinct ticker) FROM ohlcv").fetchone()[0]
        print(f"📊 Resultado en DB: {count} filas, {unique_tickers} tickers procesados.")
        
    except Exception as e:
        print(f"❌ Fallo durante el sync: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    test_sync()
