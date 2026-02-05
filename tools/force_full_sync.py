import logging
import sys
import pandas as pd
from pathlib import Path
import yfinance as yf
from typing import List, Any, cast

# Ajustar path para importar módulos del proyecto
PROJECT_ROOT = Path(__file__).parent.parent
sys.path.append(str(PROJECT_ROOT))

from svc_v2.db import Database
from svc_v2.config_loader import load_settings, HoldingConfig

import argparse

# Configuración
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

def main():
    parser = argparse.ArgumentParser(description="Force full history sync")
    parser.add_argument("--clean", action="store_true", help="Delete existing data for the timeframe before syncing")
    args = parser.parse_args()

    print("🚑 FORCING FULL HISTORY SYNC (Repair Mode)...")
    if args.clean:
        print("   ⚠️ CLEAN MODE: Existing data will be deleted first!")
    
    cfg = load_settings()
    db = Database(f"data/{cfg.system.db_filename}")
    
    # Construir universo
    print("   -> Construyendo universo...")
    sp500 = get_sp500_tickers()
    ndx100 = get_nasdaq100_tickers()
    etfs = get_key_etfs_indices()
    
    # Extraer holdings con validación de tipo para Pylance
    holding_tickers: List[str] = []
    for h in cfg.portfolios.holdings:
        if isinstance(h, HoldingConfig):
            holding_tickers.append(h.ticker)
        else:
            holding_tickers.append(str(h))
    
    # Unificar
    watchlist_tickers = cfg.universe.watchlist
    all_tickers = list(set([t for t, n in sp500 + ndx100 + etfs] + holding_tickers + watchlist_tickers))
    print(f"   -> {len(all_tickers)} activos a reparar.")

    # Timeframes y periodos
    tf_map = {
        "1d": "10y",
        "1h": "730d", # Max YF hourly
        "15m": "60d"  # Max YF 15m
    }

    for tf, period in tf_map.items():
        print(f"\n⏳ Procesando Timeframe: {tf} (Periodo: {period})")
        
        if args.clean:
            print(f"   🗑️ Borrando datos existentes para {tf}...")
            try:
                # Borrar OHLCV
                db.conn.execute(f"DELETE FROM ohlcv WHERE timeframe = '{tf}'")
                # Borrar Indicators también para evitar huérfanos
                db.conn.execute(f"DELETE FROM indicators WHERE timeframe = '{tf}'")
                print("      ✅ Datos borrados.")
            except Exception as e:
                print(f"      ❌ Error borrando datos: {e}")

        yf_interval = tf 
        
        chunk_size = 50 
        for i in range(0, len(all_tickers), chunk_size):
            chunk = all_tickers[i:i+chunk_size]
            print(f"   🔨 Batch {i}-{i+len(chunk)}: Descargando...")
            
            try:
                # Forzar tipo Any para evitar que Pylance asuma None
                data: Any = yf.download(chunk, period=period, interval=yf_interval, auto_adjust=True, threads=False, progress=False)
                
                if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                    print(f"      ⚠️ Batch {i} vacío.")
                    continue

                df = cast(pd.DataFrame, data)
                
                # Stackear
                if len(chunk) > 1:
                    if isinstance(df.columns, pd.MultiIndex):
                        df_final = df.stack(level=1, future_stack=True).reset_index()
                    else:
                         continue 
                else:
                    df_final = df.reset_index()
                    df_final['Ticker'] = chunk[0] 
                
                # Limpiar columnas
                df_final.columns = [str(c).lower() for c in df_final.columns]
                if 'datetime' in df_final.columns: df_final.rename(columns={'datetime': 'date'}, inplace=True)
                
                # Fix TZ
                if 'date' in df_final.columns:
                     col_date = df_final['date']
                     if isinstance(col_date.dtype, pd.DatetimeTZDtype):
                         df_final['date'] = pd.to_datetime(col_date).dt.tz_convert("UTC").dt.tz_localize(None)
                     
                     # FIX DUPLICADOS 1D
                     if tf == '1d':
                         df_final['date'] = pd.to_datetime(df_final['date']).dt.normalize()

                # Upsert directo
                if 'date' in df_final.columns:
                    db.upsert_ohlcv(df_final, tf)
                    print(f"      ✅ Guardado en DB ({len(df_final)} filas).")
                else:
                    print(f"      ⚠️ Batch {i} corrupto (falta 'date'). Activando Failover...")
                    download_chunk_individually(chunk, period, yf_interval, db)
                    
            except Exception as e:
                print(f"      ❌ Error en batch: {e}. Activando Failover...")
                download_chunk_individually(chunk, period, yf_interval, db)

    print("\n✅ Reparación completada. Ahora corre el Analyzer para recalcular indicadores.")

def download_chunk_individually(tickers: List[str], period: str, interval: str, db: Database):
    """Intenta descargar lista ticker por ticker para salvar lo que se pueda."""
    print(f"      🚑 Iniciando rescate de {len(tickers)} tickers...")
    for t in tickers:
        try:
            data: Any = yf.download(t, period=period, interval=interval, auto_adjust=True, threads=False, progress=False)
            
            if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                if period == '730d': 
                    data = yf.download(t, period="1y", interval=interval, auto_adjust=True, threads=False, progress=False)
            
            if data is None or (isinstance(data, pd.DataFrame) and data.empty):
                print(f"         ⚠️ {t}: Sin datos.")
                continue
                
            df = cast(pd.DataFrame, data).reset_index()
            df['Ticker'] = t
            
            # Sanitización robusta de columnas
            new_cols = []
            for c in df.columns:
                if isinstance(c, tuple):
                    new_cols.append(str(c[0]).lower())
                else:
                    new_cols.append(str(c).lower())
            df.columns = new_cols

            if 'datetime' in df.columns: df.rename(columns={'datetime': 'date'}, inplace=True)
            
            if 'date' in df.columns:
                col_date = df['date']
                if isinstance(col_date.dtype, pd.DatetimeTZDtype):
                    df['date'] = pd.to_datetime(col_date).dt.tz_convert("UTC").dt.tz_localize(None)
                
                # FIX DUPLICADOS 1D
                if interval == '1d':
                    df['date'] = pd.to_datetime(df['date']).dt.normalize()

                db.upsert_ohlcv(df, interval)
            else:
                print(f"         ❌ {t}: Estructura inválida.")

        except Exception as e:
            print(f"         ❌ {t}: Error {e}")

# Helpers para que Pylance no se queje de imports circulares o missing functions
from svc_v2.universe_loader import get_sp500_tickers, get_nasdaq100_tickers, get_key_etfs_indices

if __name__ == "__main__":
    main()