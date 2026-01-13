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
- `templates/u2_screener_FIJO.html` → template del screener (la versión embebida se genera).
- `templates/triple_screen.html` → vista triple screen (usa `details/<ticker>.json`).
- `out/` → salida generada (HTML/JSON/Details).
- `out/legacy/` → salidas legacy de scripts viejos.

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
out/legacy/
 ├── u2_screener.json
 ├── u2_screener_FIJO_embedded.html
 └── u2_screener.html
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

- `templates/u2_screener_FIJO.html` es el template manual.
- `u2_screener_FIJO_embedded.html` se genera por el runner.
- Las alertas U2 se disparan cuando `U2_entry` es true y no se ha notificado antes (se guarda en `state/u2_alert_state.json`).
- El triple screen consume `details/<ticker>.json` y se abre desde el link en la columna Ticker.

## 🧭 Próximos pasos (Roadmap)

### 1) Revisión profunda: Dashboard vs Screener (y plan de merge)

**Dashboard (main_mkt_db.py)**
- Ejecuta *batch* una sola vez (descarga + analiza + alerta).
- Usa `config/config.yaml` como fuente de verdad.
- Guarda datos en `data/` (Parquet) y estado en `state/app.json`.
- Alertas: Telegram/Discord con `TELEGRAM_*` y `DISCORD_WEBHOOK_URL`.

**Screener (u2_screener_runner.py)**
- Loop continuo con `update_minutes`.
- Usa `config/u2_screener_config.json` (tickers y ventanas propias).
- Genera `out/` (HTML/JSON) + `out/details/` para triple screen.
- Alertas U2 independientes (`U2_*`) y estado `state/u2_alert_state.json`.

**Diferencias clave**
- Doble configuración (dos fuentes de verdad).
- Doble pipeline de descarga y análisis (coste y latencia duplicados).
- Salidas y estados separados (difícil sincronizar alertas vs screener).
- Distintos criterios/intervalos (Dashboard 2h/15m vs Screener 1d/2h/15m).

**Plan de merge (propuesto)**
1. **Pipeline único de datos**: un *collector* central que actualice `data/` y entregue `working_db` a ambos flujos.
2. **Config unificada**: un solo `config.yaml` con bloques `dashboard` y `screener` (tickers, intervalos, alertas).
3. **Estado central**: consolidar `state/` en una sola estructura (snapshots, U2, alerts).
4. **Outputs desacoplados**: runner genera `out/` desde el `working_db` ya actualizado (sin redescargar).
5. **Scheduler común**: cron/loop que ejecute *tasks* (update, alertas, screener) con frecuencias distintas.

---

### 2) Mejoras UI/UX
- Indicadores extra por card (ej. etiquetas para earnings, volumen inusual, alertas activas).
- Tooltips explicativos en pills clave (Force, ADX, MACDh).
- Mejoras de performance en render (virtualización o fetch incremental).

### 3) Observabilidad
- Logs por task y métricas básicas (tiempo de update, tickers fallidos).
- Health endpoint simple para saber si el runner está vivo.

### 4) ML / Modelos (plan futuro)
- Dataset base: 200 tickers × 10–20 años (1D) como mínimo viable.
- Normalización por ticker (volatilidad, splits, gaps) para evitar sesgos.
- Splits por tiempo (ej. 2008–2018 train, 2019–2021 val, 2022–2026 test).
- Empezar con 1D; 2H/15m solo después de validar 1D.
- Baselines clásicos (LogReg / RandomForest / GradientBoosting) con `TimeSeriesSplit`.
- Métricas: hit rate, retorno promedio post‑señal, drawdown, precision/recall.

## 🗂️ Organización del repo

```
templates/    # HTML templates (screener + triple screen)
out/          # outputs generados por el runner
out/legacy/   # outputs legacy (RetoActinver_Stocks.py)
docs/         # docs y referencias
notebooks/    # notebooks exploratorios
assets/       # archivos fuente (xlsx)
scripts/      # utilidades sueltas
```
