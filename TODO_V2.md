# TODO V2 - MarketDashboard Revamp

## 📝 Estado Actual (Checkpoint: 2026-02-14)
**Última acción:** Implementación de **Trading Journal** avanzado, **Portfolio Editor** completo y optimización de **Notificaciones**.
**Logros:**
- ✅ **Portfolio Editor:** CRUD completo desde la web (Qty, Price, Fees, Date, Currency).
- ✅ **Trading Journal:** Análisis FIFO de trades cerrados, normalizado a MXN con FX histórico.
- ✅ **Alert Batching:** Notificaciones de Discord consolidadas por timeframe (menos spam).
- ✅ **Data Engine:** Descarga masiva de 1h/15m para todo el universo (cobertura total).
- ✅ **Infra:** Daemon con filtro de fines de semana y solución de concurrencia DuckDB.

---

## 🧹 Fase 1: Limpieza y Estructura (COMPLETA)
- [x] Crear carpeta `archive/` para scripts legacy.
- [x] Mover scripts huérfanos.
- [x] Crear estructura de directorios V2.

## ⚙️ Fase 2: Configuración Unificada (COMPLETA)
- [x] Diseñar `config/v2/settings.yaml` (con Scheduler).
- [x] Crear cargador `svc_v2/config_loader.py` (Pydantic).

## 🗄 Fase 3: Core Database (DuckDB) (COMPLETA)
- [x] Implementar `svc_v2/db.py` (Schema: OHLCV, Indicators, Metadata, Watchlist, **Signal History**).
- [x] Implementar `svc_v2/collector.py` (YFinance Caching + Incremental + **1D Normalize**).
- [x] Documentar esquema en `docs/SCHEMA_V2.md`.

## 🧠 Fase 4: Analítica Eficiente (COMPLETA)
- [x] Implementar `svc_v2/analyzer.py` (PandasTA Vectorizado).
- [x] Implementar `svc_v2/screener.py` (Estrategias SQL Multi-Timeframe).

## 🚀 Fase 5: Runners & Daemon (COMPLETA)
- [x] **Daemon Orchestrator (`main_v2.py`):** Loop, Scheduler, Subprocesos.
- [x] **Weekend Filter:** Omitir escaneos automáticos los fines de semana.
- [x] **Job: Broad Scan:** Diario (21:00), Batch Download, Dynamic Watchlist.
- [x] **Job: Detailed Scan:** Intradía, Cobertura Total (1h/15m para todo el universo).

## 🖥 Fase 6: Frontend & API (COMPLETA)
- [x] **API Layer:** FastAPI con conexiones transientes para alta concurrencia.
- [x] **Portfolio Editor:** CRUD de transacciones desde la UI web.
- [x] **Trading Journal:** Vista de rendimiento realizado y calendario de P&L.
- [x] **Triple Screen v2:** UI reactiva con cobertura intradía total.

## 📢 Fase 7: Notificaciones (COMPLETA)
- [x] **Alert Batching:** Notificaciones agrupadas en un solo mensaje por bloque.
- [x] **Smart Alerting:** Alertas segmentadas (Holdings vs Market) en tiempo real.

## 🅿️ Parking Lot / Backlog
- [x] **History Repair:** Script `force_full_sync.py` con option `--clean`.
- [x] **Portfolio CLI:** Gestión de transacciones vía terminal.
- [x] **Local Dev Tools:** `create_test_db.py` y `refresh_watchlist.py`.
- [ ] **Performance:** Virtualización de tablas si el universo crece > 1000 tickers.
- [ ] **Log Reader:** Pestaña en la UI para ver logs del daemon en tiempo real via API.
- [ ] **DB Explorer:** Vista de salud para inspeccionar conteos de tablas y últimos timestamps.
- [ ] **Sorting Rework:** Refinar el ordenamiento por defecto del screener (prioridad a Bias, Momentum).
