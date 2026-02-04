# TODO V2 - MarketDashboard Revamp

## 📝 Estado Actual (Checkpoint: 2026-02-03)
**Última acción:** Implementación exitosa del **Daemon V2** (`main_v2.py`) y arquitectura de Jobs aislados.
**Logros:**
- ✅ Memory Leaks solucionados (Subprocesos para cada scan).
- ✅ Arquitectura "The Funnel" completa (Broad Scan -> Dynamic Watchlist -> Detailed Scan).
- ✅ Configuración unificada y "Hot Reloadable".
- ✅ Base de datos DuckDB estable y documentada (`docs/SCHEMA_V2.md`).

**Siguiente paso:** Dockerización final y despliegue del stack.

---

## 🧹 Fase 1: Limpieza y Estructura
- [x] Crear carpeta `archive/` para scripts legacy.
- [x] Mover scripts huérfanos.
- [x] Crear estructura de directorios V2.

## ⚙️ Fase 2: Configuración Unificada
- [x] Diseñar `config/v2/settings.yaml` (con Scheduler).
- [x] Crear cargador `svc_v2/config_loader.py` (Pydantic).

## 🗄 Fase 3: Core Database (DuckDB)
- [x] Implementar `svc_v2/db.py` (Schema: OHLCV, Indicators, Metadata, DynamicWatchlist).
- [x] Implementar `svc_v2/collector.py` (YFinance Caching + Incremental).
- [x] Documentar esquema en `docs/SCHEMA_V2.md`.

## 🧠 Fase 4: Analítica Eficiente
- [x] Implementar `svc_v2/analyzer.py` (PandasTA Vectorizado).
- [x] Implementar `svc_v2/screener.py` (Estrategias SQL Multi-Timeframe).

## 🚀 Fase 5: Runners & Daemon
- [x] **Daemon Orchestrator (`main_v2.py`):**
    - [x] Loop infinito y Scheduler.
    - [x] Ejecución aislada (Subprocesos).
    - [x] Hot Reload de configuración.
- [x] **Job: Broad Scan (`jobs/broad_scan.py`):**
    - [x] Diario (21:00).
    - [x] Alimenta `dynamic_watchlist`.
- [x] **Job: Detailed Scan (`jobs/detailed_scan.py`):**
    - [x] Intradía (Configurable).
    - [x] Consume Watchlist + Dynamic.

## 🅿️ Parking Lot / Backlog
- [x] **Holdings Metadata:** Migrado a DB (`portfolio_transactions` table + `view_portfolio_holdings`).
- [x] **Docker Stack V2:**
    - [x] `requirements_v2.txt` creado.
    - [x] `Dockerfile.v2` creado.
    - [x] `docker-compose.v2.yml` creado.
- [ ] **Notificaciones:** Integrar Discord Webhook en `run_job_subprocess` o dentro de los scripts.
- [ ] **Frontend:** TripleScreen UI (Fase 7).
