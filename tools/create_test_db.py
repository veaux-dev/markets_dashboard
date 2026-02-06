import duckdb
import os
import shutil
from pathlib import Path

PROD_DB = "../../Data/Markets/Dashboard/data_v2/markets.duckdb"
TEST_DB = "data/test_markets.duckdb"
TICKERS_TO_KEEP = ['AAPL', 'NVDA', 'ALSEA.MX', '^GSPC', '^MXX', 'INTC']

def main():
    print(f"🚀 Creando DB de testeo (STRIPPED): {TEST_DB}")
    
    if not Path(PROD_DB).exists():
        print(f"❌ Error: No se encuentra la DB de producción en {PROD_DB}")
        return

    os.makedirs("data", exist_ok=True)
    
    # 1. Copia física del archivo (Clon exacto)
    print("📂 Clonando base de datos de producción...")
    shutil.copy2(PROD_DB, TEST_DB)
    
    # 2. Abrir la copia y borrar el exceso
    print("✂️ Stripping data (limpiando todo excepto favoritos)...")
    tickers_str = ", ".join([f"'{t}'" for t in TICKERS_TO_KEEP])
    
    with duckdb.connect(TEST_DB) as con:
        # Borrar filas de tablas pesadas
        con.execute(f"DELETE FROM indicators WHERE ticker NOT IN ({tickers_str})")
        con.execute(f"DELETE FROM ohlcv WHERE ticker NOT IN ({tickers_str})")
        
        # Limpiar logs viejos para bajar peso
        con.execute("DELETE FROM system_logs")
        
        # Optimizar espacio
        print("🧹 Optimizando espacio (CHECKPOINT)...")
        con.execute("CHECKPOINT")
        
    size_mb = os.path.getsize(TEST_DB) / (1024 * 1024)
    print(f"\n✅ DB Light (Stripped) lista: {size_mb:.2f} MB.")
    print(f"\n💡 Ejecuta la API localmente:")
    print(f"   DB_PATH_OVERRIDE={TEST_DB} uvicorn svc_v2.api:app --reload")

if __name__ == "__main__":
    main()