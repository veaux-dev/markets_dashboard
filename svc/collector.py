# collector.py

# --------------------------------------------------
# 1) Importar librerías necesarias
# --------------------------------------------------
# - yaml (PyYAML) para leer config.yaml
# - dotenv para leer secrets.env
# - logging para logs
# - pandas y pandas_ta para análisis técnico
# - Cualquier API que vayamos a usar (yfinance, alpaca, etc.)
import os
import pandas as pd
import yfinance as yf
from collections import defaultdict

# # --------------------------------------------------
# # 2) Cargar configuración (config.yaml)
# # --------------------------------------------------
# # - Usar ruta relativa al directorio raíz del proyecto
# # - Validar que el archivo exista
# # - Guardar en variable global CONFIG
# with open("config/config.yaml", "r") as f:
#     config = yaml.safe_load(f)

# # --------------------------------------------------
# # 3) Cargar secretos (secrets.env)
# # --------------------------------------------------
# # - Usar dotenv para exponer en variables de entorno
# # - Validar que se hayan cargado las claves necesarias
# # - Guardar en variables como API_KEY, API_SECRET, etc.
# load_dotenv("config/secrets.env")

# telegram_token = os.getenv("TELEGRAM_TOKEN")

# # Validar que el token exista
# if not telegram_token:
#     sys.exit("❌ ERROR: No se encontró TELEGRAM_TOKEN en config/secrets.env")
    

# --------------------------------------------------
# 5) Inicializar parámetros de pandas-ta
# --------------------------------------------------
# - Configurar opciones generales
# - Asegurar que NaN se maneje de forma consistente
data_by_ticker={}
data_by_ticker=defaultdict(dict)


# --------------------------------------------------
# 6) Conectar a la fuente de datos
# --------------------------------------------------
# - Según CONFIG['data_source'], inicializar cliente
# - Por ejemplo: yfinance, alpaca, binance, etc.

# --------------------------------------------------
# 7) Descargar datos históricos
# --------------------------------------------------
# - Usar símbolos de CONFIG['symbols']
# - Usar fechas CONFIG['start_date'] y CONFIG['end_date']
# - Guardar en DataFrame principal
def download_tickers(tickers,ticker_to_market,market_to_ticker,markets):

    out_1d= yf.download(tickers,interval= "1d",period="max",auto_adjust=True,prepost=False)
    out_1h= yf.download(tickers,interval= "1h",period="60d",auto_adjust=True,prepost=False)
    out_15m= yf.download(tickers,interval= "15m",period="60d",auto_adjust=True,prepost=False)


    for ticker in tickers:
        extract_1d=out_1d.xs(ticker,axis=1,level=1).copy()
        extract_1d.columns.name = None
        extract_1d.index.name="date"
        extract_1d.rename(columns={"Close":"close", "High":"high","Low":"low","Open":"open","Volume":"volume"},inplace=True)
        extract_1d=extract_1d[['open', 'high', 'low', 'close', 'volume']]# Reorder columns
        extract_1d.dropna(subset=['open','high','low','close'],inplace=True)
        extract_1d.drop(extract_1d[extract_1d['volume']==0].index,inplace=True)
        extract_1d.sort_index(inplace=True)
        if extract_1d.index.tz is None: # Check if timezone is None
            extract_1d=extract_1d.tz_localize('UTC') # Localize to UTC if no timezone
        else: 
            extract_1d=extract_1d.tz_convert('UTC') # Convert to UTC if already localized
        data_by_ticker[ticker]["1d"]=extract_1d
        
        extract_1h=out_1h.xs(ticker,axis=1,level=1).copy()
        extract_1h.columns.name = None
        extract_1h.index.name="date"
        extract_1h.rename(columns={"Close":"close", "High":"high","Low":"low","Open":"open","Volume":"volume"},inplace=True)
        extract_1h=extract_1h[['open', 'high', 'low', 'close', 'volume']]
        extract_1h.dropna(subset=['open','high','low','close'],inplace=True)
        extract_1h.drop(extract_1h[extract_1h['volume']==0].index,inplace=True)
        extract_1h.sort_index(inplace=True)
        if extract_1h.index.tz is None: # Check if timezone is None
            extract_1h=extract_1h.tz_localize('UTC') #
        else: 
            extract_1h=extract_1h.tz_convert('UTC') # Convert to UTC if already localized
        data_by_ticker[ticker]["1h"]=extract_1h

        extract_1h=extract_1h.tz_convert('America/New_York')
        extract_2h=extract_1h.resample('2h', origin='start_day',offset='30min').agg({'open':'first','high':'max','low':'min','close':'last','volume':'sum'})
        extract_2h.dropna(subset=['open','high','low','close'],inplace=True)
        extract_2h.drop(extract_2h[extract_2h['volume']==0].index,inplace=True)
        extract_2h.sort_index(inplace=True)
        extract_2h=extract_2h.tz_convert('UTC')
        data_by_ticker[ticker]["2h"]=extract_2h

        extract_15m=out_15m.xs(ticker,axis=1,level=1).copy()
        extract_15m.columns.name = None
        extract_15m.index.name="date"
        extract_15m.rename(columns={"Close":"close", "High":"high","Low":"low","Open":"open","Volume":"volume"},inplace=True)
        extract_15m=extract_15m[['open', 'high', 'low', 'close', 'volume']]
        extract_15m.dropna(subset=['open','high','low','close'],inplace=True)
        extract_15m.drop(extract_1h[extract_1h['volume']==0].index,inplace=True)
        extract_15m.sort_index(inplace=True)
        if extract_15m.index.tz is None: # Check if timezone is None
            extract_15m=extract_15m.tz_localize('UTC') #
        else: 
            extract_15m=extract_15m.tz_convert('UTC') # Convert to UTC if already localized
        data_by_ticker[ticker]["15m"]=extract_15m

    return data_by_ticker

def save_local(dict,path):
    # --------------------------------------------------
    # 8) Guardar datos en caché local
    # --------------------------------------------------
    # - CSV o Parquet según CONFIG
    # - Ruta CONFIG['output_path']

    tickers=list(dict.keys())
    
    for ticker in tickers:
        dir_path=os.path.join(path, ticker)
        os.makedirs(dir_path,exist_ok=True)
        for timeframe in list(dict[ticker].keys()):
            file_path=os.path.normpath(os.path.join(dir_path, f"{timeframe}.parquet"))
            try: 
                print(repr(file_path))
                old=pd.read_parquet(file_path)
                new=data_by_ticker[ticker][timeframe]
                new=pd.concat([old,new])
                new = new.loc[~new.index.duplicated(keep='last')].sort_index()
                
            except: # (FileNotFoundError, OSError):
                new=data_by_ticker[ticker][timeframe]
            
            new.to_parquet(path=file_path,engine='pyarrow') 




