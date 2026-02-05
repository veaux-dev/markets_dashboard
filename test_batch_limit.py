import yfinance as yf
import logging

logging.basicConfig(level=logging.INFO)

# Lista de ~20 tickers variados para probar
tickers = ["AAPL", "NVDA", "MSFT", "GOOGL", "AMZN", "META", "TSLA", "AMD", "INTC", "QCOM",
           "JPM", "BAC", "WFC", "C", "GS", "MS", "BLK", "SPY", "QQQ", "IWM"]

print(f"Probando descarga batch de {len(tickers)} tickers (Threads=False)...")
try:
    # Batch download SIN threads para evitar rate limiting excesivo
    data = yf.download(tickers, period="5d", interval="1d", auto_adjust=True, threads=False)
    print("✅ Descarga exitosa.")
    print(f"Shape: {data.shape}")
    print(data.head())
except Exception as e:
    print(f"❌ Error: {e}")
