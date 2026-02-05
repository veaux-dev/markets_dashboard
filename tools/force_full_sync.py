import logging
import sys
import pandas as pd
from pathlib import Path
import yfinance as yf

# Ajustar path para importar módulos del proyecto
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from svc_v2.db import Database
from svc_v2.collector import Collector
from svc_v2.universe_loader import get_sp500_tickers, get_nasdaq100_tickers, get_key_etfs_indices
from svc_v2.config_loader import load_settings

# Configuración
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    print("🚑 FORCING FULL HISTORY SYNC (Repair Mode)...")
    
    cfg = load_settings()
    db = Database(f"data/{cfg.system.db_filename}")
    
    # Construir universo
    print("   -> Construyendo universo...")
    sp500 = get_sp500_tickers()
    ndx100 = get_nasdaq100_tickers()
    etfs = get_key_etfs_indices()
    
    # Extraer holdings
    holding_tickers = [h.ticker if hasattr(h, 'ticker') else h for h in cfg.portfolios.holdings]
    
    # Unificar
    all_tickers = list(set([t for t, n in sp500 + ndx100 + etfs] + holding_tickers + cfg.universe.watchlist))
    print(f"   -> {len(all_tickers)} activos a reparar.")

    # Timeframes y periodos
    tf_map = {
        "1d": "10y",
        "1h": "730d", # Max YF hourly
        "15m": "60d"  # Max YF 15m
    }

    for tf, period in tf_map.items():
        print(f"\n⏳ Procesando Timeframe: {tf} (Periodo: {period})")
        yf_interval = tf # coinciden 1d, 1h, 15m
        
        chunk_size = 50 # Menor chunk para intradia para no saturar
        for i in range(0, len(all_tickers), chunk_size):
            chunk = all_tickers[i:i+chunk_size]
            print(f"   🔨 Batch {i}-{i+len(chunk)}: Descargando...")
            
            try:
                data = yf.download(chunk, period=period, interval=yf_interval, auto_adjust=True, threads=False, progress=False)
                
                if not data.empty:
                    # Stackear
                    if len(chunk) > 1:
                        # Stack level 1 (Ticker)
                        # A veces YF devuelve solo 1 nivel si fallaron todos menos 1
                        if isinstance(data.columns, pd.MultiIndex):
                            df = data.stack(level=1, future_stack=True).reset_index()
                        else:
                             # Fallback raro
                             df = data.reset_index()
                             # Asumir que es el primer ticker del chunk si no hay info
                             # Esto es riesgoso, mejor saltar si no es multi
                             continue 
                    else:
                        df = data.reset_index()
                        df['Ticker'] = chunk[0] 
                    
                    # Limpiar columnas
                    df.columns = [str(c).lower() for c in df.columns]
                    rename_map = {}
                    if 'date' not in df.columns and 'datetime' in df.columns: rename_map['datetime'] = 'date'
                    if rename_map: df.rename(columns=rename_map, inplace=True)
                    
                    # Fix TZ
                    if 'date' in df.columns:
                         # check if tz aware (modern pandas check)
                         if isinstance(df['date'].dtype, pd.DatetimeTZDtype):
                             df['date'] = df['date'].dt.tz_convert("UTC").dt.tz_localize(None)

                    # Upsert directo - Verificar columnas críticas
                    if 'date' not in df.columns:
                        print(f"      ⚠️ Batch {i} corrupto (falta 'date'). Activando Failover uno-por-uno...")
                        download_chunk_individually(chunk, period, yf_interval, db)
                        continue

                    db.upsert_ohlcv(df, tf)
                    print(f"      ✅ Guardado en DB ({len(df)} filas).")
                    
            except Exception as e:
                print(f"      ❌ Error en batch: {e}. Activando Failover uno-por-uno...")
                download_chunk_individually(chunk, period, yf_interval, db)

    print("\n✅ Reparación completada. Ahora corre el Analyzer para recalcular indicadores.")

def download_chunk_individually(tickers, period, interval, db):
    """Intenta descargar lista ticker por ticker para salvar lo que se pueda."""
    print(f"      🚑 Iniciando rescate de {len(tickers)} tickers...")
    for t in tickers:
        try:
            # Intentar descarga individual
            data = yf.download(t, period=period, interval=interval, auto_adjust=True, threads=False, progress=False)
            if data.empty:
                # Si falla con period largo (caso IPOs recientes en intradia), intentar periodo mas corto 'ytd'
                if period == '730d': 
                    data = yf.download(t, period="1y", interval=interval, auto_adjust=True, threads=False, progress=False)
            
            if data.empty:
                print(f"         ⚠️ {t}: Sin datos.")
                continue
                
            # Procesar single ticker
            df = data.reset_index()
            df['Ticker'] = t
            
            # Limpieza estándar robusta
            new_cols = []
            for c in df.columns:
                if isinstance(c, tuple):
                    new_cols.append(str(c[0]).lower())
                else:
                    new_cols.append(str(c).lower())
            df.columns = new_cols

            if 'datetime' in df.columns: df.rename(columns={'datetime': 'date'}, inplace=True)
            
            if 'date' in df.columns:
                if isinstance(df['date'].dtype, pd.DatetimeTZDtype):
                    df['date'] = df['date'].dt.tz_convert("UTC").dt.tz_localize(None)
                
                db.upsert_ohlcv(df, interval) # interval == timeframe aqui
            else:
                print(f"         ❌ {t}: Estructura inválida. Cols encontradas: {df.columns.tolist()}")

        except Exception as e:
            print(f"         ❌ {t}: Error {e}")

if __name__ == "__main__":
    main()

if __name__ == "__main__":
    main()
