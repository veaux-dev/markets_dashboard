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
- [x] **Daemon Orchestrator (`main_v2.py`):** Loop, Scheduler, Subprocesos.
- [x] **Job: Broad Scan:** Diario (21:00), Batch Download, Dynamic Watchlist.
- [x] **Job: Detailed Scan:** Intradía, Consume Watchlist, Reporte segmentado.

## 🖥 Fase 6: Frontend & API (In Progress)
- [x] **API Layer:** FastAPI sirviendo datos de DuckDB.
- [x] **Triple Screen v2:** UI reactiva conectada a la API con 1500 velas de historial.
- [ ] **Screener UI:** Página principal para visualizar candidatos de la `dynamic_watchlist`.
- [ ] **Portfolio View:** Visualización web de tus holdings y P&L.

## 📢 Fase 7: Notificaciones
- [ ] **Notifier Module:** Discord/Telegram.
- [ ] **Smart Alerting:** Conectar alertas de `detailed_scan` al celular.

## 🅿️ Parking Lot / Backlog
- [x] **History Repair:** Script `force_full_sync.py` para recuperación de datos.
- [x] **Portfolio CLI:** Gestión de transacciones con soporte multi-moneda.
