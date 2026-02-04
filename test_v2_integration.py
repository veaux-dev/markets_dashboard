import logging
import pandas as pd
from svc_v2.db import Database
from svc_v2.collector import Collector
from svc_v2.analyzer import Analyzer

# Configurar logs para ver qué pasa
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def run_test():
    print("🚀 Iniciando Test de Integración V2...")

    # 1. Inicializar DB (Singleton)
    # Usaremos una db temporal para no ensuciar la real si quisieras, 
    # pero como es dev, usemos la real 'data/markets.duckdb'
    db = Database("data/test_v2.duckdb") 
    
    # 2. Inicializar Servicios
    col = Collector(db)
    alz = Analyzer(db)

    # 3. Definir Universe de Prueba
    tickers = ["AAPL", "INTC", "NVDA"] # INTC es buen ejemplo para gaps bajistas recientes
    timeframe = "1d"

    # 4. Ejecutar Collector (Download)
    print("\n📥 1. Ejecutando Collector...")
    col.sync_tickers(tickers, [timeframe])

    # Verificar que haya datos crudos
    last_ts = db.get_last_timestamp("AAPL", timeframe)
    print(f"   ✅ Último dato AAPL en DB: {last_ts}")

    # 5. Ejecutar Analyzer (Compute)
    print("\n🧠 2. Ejecutando Analyzer...")
    alz.analyze_tickers(tickers, [timeframe], force_full=True)

    # 6. Consultar Resultados (Query estilo Screener)
    print("\n🔎 3. Consultando Indicadores (SQL)...")
    query = """
    SELECT 
        ticker, 
        timestamp, 
        close, 
        round(gap_pct, 2) as gap_pct, 
        round(rsi, 1) as rsi, 
        round(vol_k, 2) as vol_k,
        round(ema_200, 2) as ema200
    FROM indicators 
    JOIN ohlcv USING (ticker, timeframe, timestamp)
    WHERE timeframe = '1d'
    ORDER BY ticker, timestamp DESC
    LIMIT 10;
    """
    
    res = db.conn.execute(query).df()
    print(res)

    # Limpieza (opcional, cerramos conexión)
    db.close()
    print("\n✅ Test Finalizado.")

if __name__ == "__main__":
    run_test()
