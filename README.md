# Markets Dashboard – Collector, Alerts, Screener + Web

Sistema modular en Python para **recolección de datos de mercado, almacenamiento local en Parquet, análisis técnico, alertas** y **screener web**.

---

## 🧱 Componentes

- `svc/collector.py` → descarga datos (`yfinance`) + caché local Parquet.
- `svc/analyzer.py` → indicadores (MACD, RSI, Bollinger, etc.).
- `svc/notifier.py` → alertas Telegram/Discord.
- `svc/state.py` → estado para evitar spam.
- `main_mkt_db.py` → orquestador principal de alertas.
- `u2_screener_runner.py` → genera `u2_screener.json` + HTML embebido con loop.
- `u2_screener_FIJO.html` → template del screener (la versión embebida se genera).
- `triple_screen.html` → vista triple screen (usa `details/<ticker>.json`).

---

## 🔐 Secrets y `.env`

**No subas llaves a GitHub.**  
Usa un archivo `.env` local (ignorado) o variables en Portainer.

Ejemplo `.env` (no commitear):
```
TELEGRAM_TOKEN=xxxx
TELEGRAM_CHAT_ID=xxxx
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...

U2_TELEGRAM_TOKEN=xxxx
U2_TELEGRAM_CHAT_ID=xxxx
U2_DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
U2_WEB_PORT=8088

BASE_DIR=/mnt/MyPool/Data/Markets
PUID=1000
PGID=1000
```

Si usas Portainer: configura estas variables en el stack, **no** en el repo.

---

## 🐳 Docker Compose (multi‑servicio)

Este `docker-compose.yml` levanta:
- `market-dashboard` → proceso principal (alertas).
- `u2-screener-runner` → loop que regenera el screener.
- `u2-screener-web` → Nginx que sirve el HTML/JSON.

### Build + Up
```bash
docker compose up -d --build
```

### Forzar rebuild (sin CLI de Portainer)
Se usa `build_stamp.txt` como "cache bust".  
Cuando quieras forzar rebuild:
1) Cambia el contenido de `build_stamp.txt` (ej. fecha/hora).
2) Haz push.
3) Portainer hará rebuild porque cambia un archivo copiado antes de `pip install`.

### Config del screener
Archivo: `config/u2_screener_config.json` (se monta como read‑only en el runner).  
Ajusta:
- `tickers`
- `update_minutes`
- `intervals_cfg`
- `output_dir`, `data_dir`, `state_path`

### Puerto del web
Configura `U2_WEB_PORT` (default 8088):
```
U2_WEB_PORT=8099
```
URL final: `http://<nas-ip>:8099/u2_screener_FIJO_embedded.html`

---

## 🗂 Estructura de datos

```
data/
 ├── AAPL/
 │    ├── 1d.parquet
 │    ├── 2h.parquet
 │    ├── 1h.parquet
 │    └── 15m.parquet
config/
 └── config.yaml
state/
 └── app.json
out/
 ├── u2_screener.json
 └── u2_screener_FIJO_embedded.html
out/details/
 └── <TICKER>.json
```

### ✅ Carpetas requeridas (antes de levantar el stack)

Estos paths deben existir en el host (NAS):
```
${BASE_DIR}/Dashboard/data
${BASE_DIR}/config
${BASE_DIR}/Screener/out
${BASE_DIR}/Screener/data
${BASE_DIR}/Screener/state
```

Si no existen, créalos antes de `docker compose up`.

---

## 🧠 Notas rápidas

- `u2_screener_FIJO.html` es el template manual.
- `u2_screener_FIJO_embedded.html` se genera por el runner.
- Las alertas U2 se disparan cuando `U2_entry` es true y no se ha notificado antes (se guarda en `state/u2_alert_state.json`).
- El triple screen consume `details/<ticker>.json` y se abre desde el link en la columna Ticker.
