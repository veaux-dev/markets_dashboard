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
import logging
from datetime import timedelta
import pandas as pd
import yfinance as yf
from collections import defaultdict
from typing import Dict, List, cast, Any
from pandas import Timestamp
from pathlib import Path

DictOfDict = Dict[str, Dict[str, pd.DataFrame]]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# --------------------------------------------------
# Helpers
# --------------------------------------------------

def fetch_raw_data(tickers: list[str], intervals_cfg: List[Dict[str,str]], output_path: str, margin_days:int =7) -> DictOfDict:
    """
    Descarga datos crudos desde yfinance para varios tickers y timeframes.
    - Separa tickers existentes (ya tienen parquet) y nuevos (sin parquet).
    - Existentes → bulk incremental (desde min_last_date - margin_days).
    - Nuevos → bulk full period.
    - Devuelve dict[timeframe] = DataFrame combinado.
    """
    raw = {}
    for cfg in intervals_cfg:
        timeframe = cfg["name"]
        interval = cfg["interval"]
        period = cfg["period"]

        existing: List[str] = []
        new: List[str] = []
        last_dates: List[Timestamp] = []
        
        # Clasificar tickers (= DEFINIR CUALES YA TIENEN HISTORIA Y CUALES NO TIENEN)
        for ticker in tickers:
            file_path: Path = Path(output_path) / ticker / f"{timeframe}.parquet"
            if os.path.exists(file_path):
                try:
                    df: pd.DataFrame = pd.read_parquet(file_path)
                    if not df.empty:
                        last_dates.append(df.index[-1])
                        existing.append(ticker)
                    else:
                        new.append(ticker)
                except Exception as e:
                    logging.warning(f"⚠️ {ticker} parquet corrupto, se eliminará: {file_path} ({e})")
                    try:
                        os.remove(file_path)
                    except OSError as oe:
                        logging.error(f"❌ No se pudo borrar {file_path}: {oe}")
                    new.append(ticker)
            else:
                new.append(ticker)

        df_all = None

        # Incremental para existing
        if existing and last_dates:
            min_last_date = min(last_dates)
            start = (min_last_date - timedelta(days=margin_days)).strftime("%Y-%m-%d")
            logging.info(f"⏩ {timeframe}: incremental desde {start} ({len(existing)} tickers)")
            df_existing = yf.download(existing, interval=interval, start=start,auto_adjust=True, prepost=False, group_by="ticker",threads=True, progress=False)
            df_all = df_existing

        # Full para nuevos
        if new:
            logging.info(f"📥 {timeframe}: FULL period {period} para {len(new)} nuevos tickers")
            df_new = yf.download(new, interval=interval, period=period,auto_adjust=True, prepost=False, group_by="ticker",threads=True, progress=False)
            if df_all is not None:
                # Unir columnas de ambos
                df_all = pd.concat([df_all, df_new], axis=1)
            else:
                df_all = df_new

        # Guardar en raw
        raw[timeframe] = df_all

    return raw

def clean_data(df :pd.DataFrame, ticker :str, tz="UTC") -> pd.DataFrame:
    """
    Normaliza columnas, elimina basura y asegura index UTC.
    Retorna DataFrame limpio.
    """

    try:
        df = df.copy()
        df.columns.name = None
        df.index.name = "date"
        df = df.rename(columns={
            "Open": "open",
            "High": "high",
            "Low": "low",
            "Close": "close",
            "Volume": "volume"
        })
        df = df[['open', 'high', 'low', 'close', 'volume']]
        df = df.dropna(subset=['open', 'high', 'low', 'close'])
        df = df.sort_index()

        # Timezone
        if df.index.tz is None: # type: ignore
            df = df.tz_localize(tz)
        else:
            df = df.tz_convert(tz)

        return df

    except Exception as e:
        logging.error(f"❌ Error limpiando {ticker}: {e}")
        return pd.DataFrame()
    
def save_local(dict_data, path) -> DictOfDict:
    """
    Guarda los datos en parquet incremental por ticker/timeframe. ... PERO CARGA TODO EN MEMORIA
    """
    tickers = list(dict_data.keys())

    for ticker in tickers:
        dir_path = os.path.join(path, ticker)
        os.makedirs(dir_path, exist_ok=True)

        for timeframe, df in dict_data[ticker].items():
            file_path = os.path.normpath(os.path.join(dir_path, f"{timeframe}.parquet"))
            try:
                old = pd.read_parquet(file_path)
                new = pd.concat([old, df])
                new = new.loc[~new.index.duplicated(keep="last")].sort_index()
            except FileNotFoundError:
                new = df
            except Exception as e:
                logging.warning(f"⚠️ No se pudo leer {file_path}, sobrescribiendo. Error: {e}")
                new = df

            new.to_parquet(path=file_path, engine="pyarrow")
            logging.info(f"💾 Guardado {ticker} [{timeframe}] → {file_path}")

            # 👇 aquí viene la parte clave: cargar FULL parquet de vuelta
            dict_data[ticker][timeframe] = pd.read_parquet(file_path)

    return dict_data

def _asset_from_info(info: Dict[str, Any]) -> str:
    '''
    Devuelve el tipo de asset en funccion de la info bajada (Stock, Fibra, ETF, Fund).
    
    '''
    qt  = (info.get("quoteType") or "").upper()
    ind = (info.get("industry") or "").upper()
    nm  = ((info.get("displayName") or info.get("longName") or info.get("shortName") or "")).upper()

    if "REIT" in ind or "FIBRA" in nm:
        return "FIBRA"
    if qt == "ETF":
        return "ETF"
    if qt in ("MUTUALFUND", "FUND"):
        return "Fund"
    if qt == "EQUITY":
        return "Stock"
    return qt or "Unknown"


def GetTickerInfo(ticker:str) -> Dict[str, Any]:
    '''
    Trae y normaliza fundamentales de Yahoo Finance para un ticker.
    Devuelve claves listas para agregarse a df_out.
    Requiere: yfinance instalado (importado por el caller).
    
    '''
    try:
        info = yf.Ticker(ticker).get_info()
    except Exception as e:
        # En caso de error de red o ticker inválido, devuelve campos vacíos
        return {
            "Name": "", "Asset": "Unknown", "Sector": "", "Industry": "",
            "MarketCap": float("nan"), "TrailingPE": float("nan"), "ForwardPE": float("nan"),
            "PriceToBook": float("nan"), "EVtoEBITDA": float("nan"),
            "ProfitMargin%": float("nan"), "ROE%": float("nan"),
            "FreeCashFlow": float("nan"), "TotalDebt": float("nan"),
            "DividendYield%": float("nan"), "PayoutRatio%": float("nan"), "Beta": float("nan"),
            "ExpenseRatio%": float("nan"), "AUM": float("nan"), "Category": "",
            "FundYield%": float("nan"), "Ret3Y%": float("nan"), "Ret5Y%": float("nan"),
            "recommendationKey": "", "averageAnalystRating": "",
            "_error": str(e),
        }

    # helpers cortos
    num = lambda k: pd.to_numeric(k, errors='coerce')
    pct = lambda k: pd.to_numeric(k, errors='coerce') #* 100.0

    # Nombre amigable
    name_long = (info.get("displayName")
                 or info.get("longName")
                 or info.get("shortName")
                 or "").strip()

    # Campos base
    asset   = _asset_from_info(info)
    sector  = info.get("sector") or ""
    industry= info.get("industry") or ""

    # Valuación / métricas (floats)
    marketcap   = num(info.get("marketCap"))
    trailingPE  = num(info.get("trailingPE"))
    forwardPE   = num(info.get("forwardPE"))
    p_to_book   = num(info.get("priceToBook"))
    ev_ebitda   = num(info.get("enterpriseToEbitda"))

    # Rentabilidad / balance
    profit_margin_pct = pct(info.get("profitMargins"))
    roe_pct           = pct(info.get("returnOnEquity"))
    fcf               = num(info.get("freeCashflow"))
    total_debt        = num(info.get("totalDebt"))

    # Dividendos / riesgo
    div_yield_pct     = pct(info.get("dividendYield"))
    payout_pct        = pct(info.get("payoutRatio"))
    beta              = num(info.get("beta"))

    # ETF / Fondo
    expense_pct       = pct(info.get("annualReportExpenseRatio"))
    aum               = num(info.get("totalAssets"))
    category          = info.get("category") or ""
    fund_yield_pct    = pct(info.get("yield"))
    ret3y_pct         = pct(info.get("threeYearAverageReturn"))
    ret5y_pct         = pct(info.get("fiveYearAverageReturn"))

    # Analistas
    reco_key          = (info.get("recommendationKey") or "")
    avg_analyst       = (info.get("averageAnalystRating") or "")

    return {
        "Name": name_long, "Asset": asset, "Sector": sector, "Industry": industry,
        "MarketCap": marketcap, "TrailingPE": trailingPE, "ForwardPE": forwardPE,
        "PriceToBook": p_to_book, "EVtoEBITDA": ev_ebitda,
        "ProfitMargin%": profit_margin_pct, "ROE%": roe_pct,
        "FreeCashFlow": fcf, "TotalDebt": total_debt,
        "DividendYield%": div_yield_pct, "PayoutRatio%": payout_pct, "Beta": beta,
        "ExpenseRatio%": expense_pct, "AUM": aum, "Category": category,
        "FundYield%": fund_yield_pct, "Ret3Y%": ret3y_pct, "Ret5Y%": ret5y_pct,
        "recommendationKey": reco_key, "averageAnalystRating": avg_analyst,
    }


# --------------------------------------------------
# Orquestador
# --------------------------------------------------

def download_tickers(
    tickers,
    ticker_to_market=None,
    market_to_ticker=None,
    markets=None,
    output_path="data",
    intervals_cfg=None
)-> DictOfDict:
    """
    Orquestador:
    - Descarga datos crudos (1d, 1h, 15m) en bulk incremental
    - Limpia y normaliza
    - Genera también timeframe 2h resampleado desde 1h
    - Devuelve dict[ticker][timeframe] = DataFrame
    """
    results = defaultdict(dict)

    if intervals_cfg is None:
        intervals_cfg = [
            {"name": "1d", "interval": "1d", "period": "20y"},
            {"name": "1h", "interval": "1h", "period": "1y"},
            {"name": "15m", "interval": "15m", "period": "60d"},
        ]

    raw_all = fetch_raw_data(tickers, intervals_cfg, output_path, margin_days=7)

    for ticker in tickers:
        try:
            # Procesa solo lo que pediste en intervals_cfg
            for cfg in intervals_cfg:
                tf = cfg["name"]
                df_raw = raw_all[tf][ticker]
                df_clean = clean_data(df_raw, ticker)
                if not df_clean.empty:
                    results[ticker][tf] = df_clean

            # Si pediste 1h y quieres 2h → constrúyelo
            if "1h" in results[ticker]:
                df_1h = results[ticker]["1h"]
                df_ny = df_1h.tz_convert("America/New_York")
                df_2h = df_ny.resample(
                    "2h", origin="start_day", offset="30min"
                ).agg({
                    "open": "first",
                    "high": "max",
                    "low": "min",
                    "close": "last",
                    "volume": "sum"
                })
                df_2h = df_2h.dropna(subset=['open', 'high', 'low', 'close'])
                #df_2h = df_2h[df_2h['volume'] > 0]
                df_2h = df_2h.sort_index().tz_convert("UTC")
                results[ticker]["2h"] = df_2h

            logging.info(f"✅ {ticker} procesado OK")

        except Exception as e:
            logging.error(f"❌ Error procesando {ticker}: {e}")

        if not results[ticker]:
            del results[ticker]
            logging.warning(f"⚠️ {ticker} eliminado (sin datos válidos)")

    results=save_local(results,'data')

    return results

