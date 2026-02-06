# TODO V2 - MarketDashboard Revamp

## 📝 Estado Actual (Checkpoint: 2026-02-05)
**Última acción:** Implementación completa del **Dashboard Interactivo V2** y **Notificaciones Inteligentes**.
**Logros:**
- ✅ **Frontend V2:** Screener compacto y Portfolio con P&L real-time.
- ✅ **Notificaciones:** Integración con Discord + Control de Spam (6h cooldown).
- ✅ **Base de Datos:** Limpieza de duplicados 1D y normalización de timestamps.
- ✅ **Agilidad:** Workflow local con DB Light y Auto-reload.

**Siguiente paso:** Pulir la edición de transacciones desde la UI y Journaling avanzado.

---

## 🧹 Fase 1: Limpieza y Estructura
- [x] Crear carpeta `archive/` para scripts legacy.
- [x] Mover scripts huérfanos.
- [x] Crear estructura de directorios V2.

## ⚙️ Fase 2: Configuración Unificada
- [x] Diseñar `config/v2/settings.yaml` (con Scheduler).
- [x] Crear cargador `svc_v2/config_loader.py` (Pydantic).

## 🗄 Fase 3: Core Database (DuckDB)
- [x] Implementar `svc_v2/db.py` (Schema: OHLCV, Indicators, Metadata, Watchlist, **Signal History**).
- [x] Implementar `svc_v2/collector.py` (YFinance Caching + Incremental + **1D Normalize**).
- [x] Documentar esquema en `docs/SCHEMA_V2.md`.

## 🧠 Fase 4: Analítica Eficiente
- [x] Implementar `svc_v2/analyzer.py` (PandasTA Vectorizado).
- [x] Implementar `svc_v2/screener.py` (Estrategias SQL Multi-Timeframe).

## 🚀 Fase 5: Runners & Daemon
- [x] **Daemon Orchestrator (`main_v2.py`):** Loop, Scheduler, Subprocesos.
- [x] **Job: Broad Scan:** Diario (21:00), Batch Download, Dynamic Watchlist.
- [x] **Job: Detailed Scan:** Intradía, Consume Watchlist, Reporte segmentado.

## 🖥 Fase 6: Frontend & API
- [x] **API Layer:** FastAPI sirviendo datos de DuckDB + Background Tasks.
- [x] **Triple Screen v2:** UI reactiva con fechas corregidas en 1D.
- [x] **Screener UI:** Tabla compacta sorteable con buscador manual y glosario.
- [x] **Portfolio View:** Visualización de holdings, P&L real-time y señales integradas.

## 📢 Fase 7: Notificaciones
- [x] **Notifier Module:** Discord integration con control de spam.
- [x] **Smart Alerting:** Alertas segmentadas (Holdings vs Market) en tiempo real.

## 🅿️ Parking Lot / Backlog
- [x] **History Repair:** Script `force_full_sync.py` con opción `--clean`.
- [x] **Portfolio CLI:** Gestión de transacciones vía terminal.
- [x] **Local Dev Tools:** `create_test_db.py` y `refresh_watchlist.py`.
- [ ] **Portfolio Editor:** CRUD de transacciones desde la UI web.
- [ ] **Performance:** Virtualización de tablas si el universo crece > 500 tickers.