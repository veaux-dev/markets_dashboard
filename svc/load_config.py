import os
import yaml
from dotenv import load_dotenv

# --------------------------------------------------
# 2) Cargar configuración (config.yaml)
# --------------------------------------------------
# - Usar ruta relativa al directorio raíz del proyecto
# - Validar que el archivo exista
# - Guardar en variable global CONFIG
with open("../config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

tickers=config["data"]["tickers"]

save_directory=config["storage"]["path_data"]

#CONSTANTS DEFINITION:
RSI_LENGTH=config["data"]["indicators"]["rsi"]["period"] 
RSI_OB=config["data"]["indicators"]["rsi"]["overbought"]
RSI_OS=config["data"]["indicators"]["rsi"]["oversold"]

MACD_FAST=config["data"]["indicators"]["macd"]["fast"]
MACD_SLOW=config["data"]["indicators"]["macd"]["slow"]
MACD_SIGNAL=config["data"]["indicators"]["macd"]["signal"]

ADX_LENGTH = config["data"]["indicators"]["adx"]["length"]
EMA_SHORT = config["data"]["indicators"]["ema"]["short"]
EMA_LONG = config["data"]["indicators"]["ema"]["long"]
BB_LENGTH = config["data"]["indicators"]["bollinger"]["length"]
BB_STD = config["data"]["indicators"]["bollinger"]["std"]

suffix_map=config["routing"]["by_suffix"]

markets=config["markets"]

default_market=config["routing"]["default"]


# --------------------------------------------------
# 3) Cargar secretos (secrets.env)
# --------------------------------------------------
# - Usar dotenv para exponer en variables de entorno``
# - Validar que se hayan cargado las claves necesarias
# - Guardar en variables como API_KEY, API_SECRET, etc.
load_dotenv("config/secrets.env")

telegram_token = os.getenv("TELEGRAM_TOKEN")
chat_id = os.getenv("TELEGRAM_CHAT_ID")
discord_webhook_url = os.getenv("DISCORD_WEBHOOK_URL")
