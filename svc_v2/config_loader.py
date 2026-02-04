import yaml
from pydantic import BaseModel, field_validator
from typing import List, Dict, Any, Optional, Union
from pathlib import Path
import logging

# --- Modelos de Configuración (Schemas) ---

class SystemConfig(BaseModel):
    db_filename: str = "markets.duckdb"
    log_level: str = "INFO"
    timezone: str = "America/Mexico_City"

class HoldingConfig(BaseModel):
    ticker: str
    qty: float = 0.0
    avg_price: float = 0.0
    fees: float = 0.0
    notes: Optional[str] = None

class PortfoliosConfig(BaseModel):
    holdings: List[Union[str, HoldingConfig]] = []

    @field_validator('holdings', mode='before')
    @classmethod
    def parse_holdings(cls, v):
        parsed = []
        if not v:
            return []
        for item in v:
            if isinstance(item, str):
                parsed.append(HoldingConfig(ticker=item))
            else:
                parsed.append(item)
        return parsed

class UniverseConfig(BaseModel):
    watchlist: List[str] = [] 
    # Broad scan ya no es una lista explícita en config, se construye dinámicamente

class DataConfig(BaseModel):
    timeframes: Dict[str, List[str]]
    market_hours: Dict[str, str]

class IndicatorsConfig(BaseModel):
    rsi: Dict[str, Any]
    macd: Dict[str, Any]
    adx: Dict[str, Any]
    weinstein: Dict[str, Any]
    bollinger: Dict[str, Any]

class AlertsConfig(BaseModel):
    enable_discord: bool = False
    channels: Dict[str, str] = {}

class JournalConfig(BaseModel):
    enabled: bool = False
    storage_table: str = "trade_log"

class JobConfig(BaseModel):
    enabled: bool = True
    run_at: Optional[List[str]] = None
    interval_min: Optional[int] = None
    respect_market_hours: bool = False

class SchedulerConfig(BaseModel):
    loop_interval_sec: int = 60
    jobs: Dict[str, JobConfig]

class SettingsV2(BaseModel):
    system: SystemConfig
    portfolios: PortfoliosConfig
    universe: UniverseConfig
    data: DataConfig
    indicators: IndicatorsConfig
    alerts: AlertsConfig
    journal: JournalConfig
    scheduler: SchedulerConfig

# --- Cargador ---

def load_settings(config_path: str = "config/v2/settings.yaml") -> SettingsV2:
    """
    Carga, parsea y valida el archivo de configuración YAML.
    Busca inteligentemente el archivo si se ejecuta desde subdirectorios.
    """
    path = Path(config_path)
    
    # Búsqueda inteligente del archivo si no existe en ruta relativa directa
    if not path.exists():
        # Intentar desde root (si estamos en svc_v2/ o similar)
        candidates = [
            Path(__file__).parent.parent / config_path, # ../config/v2/settings.yaml
            Path.cwd() / config_path,
            Path("/mnt/Projects/MarketDashboard") / config_path
        ]
        for candidate in candidates:
            if candidate.exists():
                path = candidate
                break
        else:
            raise FileNotFoundError(f"No se encontró settings.yaml. Buscado en: {[str(c) for c in candidates]}")
    
    with open(path, "r") as f:
        try:
            raw_data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Error de sintaxis en YAML: {e}")
    
    try:
        settings = SettingsV2(**raw_data)
        return settings
    except Exception as e:
        raise ValueError(f"Error de validación de esquema (Pydantic): {e}")

if __name__ == "__main__":
    # Test rápido
    try:
        cfg = load_settings()
        print(f"✅ Configuración cargada correctamente.")
        print(f"   DB: {cfg.system.db_filename}")
        print(f"   Watchlist ({len(cfg.universe.watchlist)} items): {cfg.universe.watchlist}")
    except Exception as e:
        print(f"❌ Fallo cargando config: {e}")
