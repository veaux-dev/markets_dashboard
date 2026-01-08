# Markets Dashboard – Collector & Alerts

Sistema modular en Python para **recolección de datos de mercado, almacenamiento local en Parquet, análisis técnico y envío de alertas**.

---

## 🏗 Arquitectura

- **collector.py** → descarga datos desde Yahoo Finance (`yfinance`) y guarda caché local en `.parquet`.  
- **analyzer.py** → aplica indicadores técnicos (MACD, RSI, Bollinger Bands, etc.) y genera señales de BUY/SELL.  
- **notifier.py** → envía alertas y snapshots vía Discord/Telegram.  
- **state/** → guarda estado (`last_snapshot.json`, `new_snapshot.json`).  
- **00_mkt_db.py** → orquestador principal (coordinación de collector, analyzer y notifier).  
- **Dockerfile** y **docker-compose.yml** → para levantar todo en chinga con un solo comando.  

---

## ⚙️ Deploy rápido con Docker

### 1. Variables de entorno requeridas (`.env`)

- `DISCORD_TOKEN` / `DISCORD_CHANNEL_ID`  
- `TELEGRAM_TOKEN` / `TELEGRAM_CHAT_ID`  
- `OUTPUT_PATH` → carpeta donde se guardan los Parquet (ej. `./data`)  
- `CONFIG_PATH` → carpeta donde vive el `config.yaml`  

### 2. Levantar servicios

```bash
docker-compose up -d --build


Estructura de Carpetas
data/
 ├── AAPL/
 │    ├── 1d.parquet
 │    ├── 2d.parquet
 │    ├── 1h.parquet
 │    └── 15m.parquet
 ├── AMXB.MX/
 │    ├── 1d.parquet
 │    ├── 2d.parquet
 │    ├── 1h.parquet
 │    └── 15m.parquet
config/
 └── config.yaml
state/
 └── app.json

