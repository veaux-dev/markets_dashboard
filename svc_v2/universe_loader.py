import pandas as pd
import logging
from typing import List, Tuple

def get_sp500_tickers() -> List[Tuple[str, str]]:
    """Descarga lista de S&P 500 desde Wikipedia con Nombre."""
    try:
        url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
        # Fake User-Agent para evitar 403
        storage_options = {'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'}
        
        tables = pd.read_html(url, storage_options=storage_options)
        df = tables[0]
        
        # Extraer pares (Symbol, Security)
        # Limpieza: BRK.B -> BRK-B
        results = []
        for _, row in df.iterrows():
            ticker = row['Symbol'].replace('.', '-')
            name = row['Security']
            results.append((ticker, name))
            
        return results
    except Exception as e:
        logging.error(f"Error bajando S&P500: {e}")
        return []

def get_nasdaq100_tickers() -> List[Tuple[str, str]]:
    """Descarga lista de Nasdaq 100 con Nombre."""
    try:
        url = 'https://en.wikipedia.org/wiki/Nasdaq-100'
        storage_options = {'User-Agent': 'Mozilla/5.0'}
        
        tables = pd.read_html(url, storage_options=storage_options)
        for df in tables:
            if 'Ticker' in df.columns or 'Symbol' in df.columns:
                col_t = 'Ticker' if 'Ticker' in df.columns else 'Symbol'
                col_n = 'Company' if 'Company' in df.columns else 'Security' # Ajustar segun tabla
                
                results = []
                for _, row in df.iterrows():
                    ticker = str(row[col_t]).replace('.', '-')
                    name = str(row.get(col_n, 'Unknown'))
                    results.append((ticker, name))
                return results
        return []
    except Exception as e:
        logging.error(f"Error bajando Nasdaq100: {e}")
        return []

def get_key_etfs_indices() -> List[Tuple[str, str]]:
    """Retorna una lista curada de ETFs e Índices de Referencia Clave."""
    return [
        # --- Índices Principales ---
        ("^GSPC", "S&P 500 Index"),
        ("^NDX", "Nasdaq 100 Index"),
        ("^MXX", "IPC Mexico"),
        ("^VIX", "CBOE Volatility Index"),
        
        # --- ETFs Mercado USA ---
        ("SPY", "SPDR S&P 500 ETF Trust"),
        ("QQQ", "Invesco QQQ Trust"),
        ("IWM", "iShares Russell 2000 ETF"),
        ("DIA", "SPDR Dow Jones Industrial Average ETF"),
        
        # --- Sectoriales Clave ---
        ("SOXX", "iShares Semiconductor ETF"),
        ("SMH",  "VanEck Semiconductor ETF"),
        ("XLK",  "Technology Select Sector SPDR"),
        ("XLF",  "Financial Select Sector SPDR"),
        ("XLE",  "Energy Select Sector SPDR"),
        ("XLV",  "Health Care Select Sector SPDR"),
        
        # --- Leveraged / Volatilidad ---
        ("TQQQ", "ProShares UltraPro QQQ (3x)"),
        ("SQQQ", "ProShares UltraPro Short QQQ (-3x)"),
        ("SOXL", "Direxion Daily Semiconductor Bull 3x"),
        ("SOXS", "Direxion Daily Semiconductor Bear 3x"),
        ("TLT",  "iShares 20+ Year Treasury Bond ETF"),
        ("UVXY", "ProShares Ultra VIX Short-Term Futures"),
        
        # --- Crypto / Futuro ---
        ("BITO", "ProShares Bitcoin Strategy ETF"),
        ("ARKK", "ARK Innovation ETF")
    ]

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sp = get_sp500_tickers()
    print(f"S&P 500: {len(sp)} - Ejemplo: {sp[:3]}")
