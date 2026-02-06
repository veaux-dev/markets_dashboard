import duckdb
import os
import sys
from pathlib import Path

# Configuración
PROD_DB = "../../Data/Markets/Dashboard/data_v2/markets.duckdb"
TEST_DB = "data/test_markets.duckdb"
TICKERS_TO_EXTRACT = ['AAPL', 'NVDA', 'ALSEA.MX', '^GSPC', '^MXX', 'INTC']

def main():
    print(f"🧪 Creando DB de testeo: {TEST_DB}")
    
    if not Path(PROD_DB).exists():
        print(f"❌ Error: No se encuentra la DB de producción en {PROD_DB}")
        return

    # Asegurar que el directorio data existe localmente
    os.makedirs("data", exist_ok=True)
    
    # Si ya existe la de test, borrarla para empezar limpio
    if Path(TEST_DB).exists():
        os.remove(TEST_DB)

    # Conectar a la nueva DB
    with duckdb.connect(TEST_DB) as con:
        print(f"🔗 Adjuntando base de datos de producción...")
        con.execute(f"ATTACH '{PROD_DB}' AS prod (READ_ONLY)")
        
        # 1. Copiar Tablas de Configuración/Metadatos (Completas)
        print("📋 Copiando metadatos y logs...")
        con.execute("CREATE TABLE ticker_metadata AS SELECT * FROM prod.ticker_metadata")
        con.execute("CREATE TABLE dynamic_watchlist AS SELECT * FROM prod.dynamic_watchlist")
        con.execute("CREATE TABLE portfolio_transactions AS SELECT * FROM prod.portfolio_transactions")
        con.execute("CREATE TABLE system_logs AS SELECT * FROM prod.system_logs")
        
        # 2. Copiar OHLCV e Indicators (Solo Tickers seleccionados)
        tickers_str = ", ".join([f"'{t}'" for t in TICKERS_TO_EXTRACT])
        
        print(f"📈 Extrayendo datos históricos para: {tickers_str}")
        con.execute(f"CREATE TABLE ohlcv AS SELECT * FROM prod.ohlcv WHERE ticker IN ({tickers_str})")
        con.execute(f"CREATE TABLE indicators AS SELECT * FROM prod.indicators WHERE ticker IN ({tickers_str})")
        
        # 3. Recrear Vistas
        print("🔭 Recreando vistas...")
        con.execute("""
            CREATE VIEW view_portfolio_holdings AS
            SELECT 
                ticker, 
                SUM(CASE WHEN side = 'BUY' THEN qty WHEN side = 'SELL' THEN -qty ELSE 0 END) as qty,
                SUM(CASE WHEN side = 'BUY' THEN qty * price ELSE 0 END) / NULLIF(SUM(CASE WHEN side = 'BUY' THEN qty ELSE 0 END), 0) as avg_buy_price
            FROM portfolio_transactions
            GROUP BY ticker
            HAVING SUM(CASE WHEN side = 'BUY' THEN qty WHEN side = 'SELL' THEN -qty ELSE 0 END) > 0;
        """)

        con.execute("DETACH prod")
        
    size_mb = os.path.getsize(TEST_DB) / (1024 * 1024)
    print(f"
✅ DB Light creada con éxito ({size_mb:.2f} MB).")
    print(f"
💡 Para usarla en local sin cambiar la config de producción, ejecuta la API así:")
    print(f"   DB_PATH_OVERRIDE={TEST_DB} python3 svc_v2/api.py")

if __name__ == "__main__":
    main()
