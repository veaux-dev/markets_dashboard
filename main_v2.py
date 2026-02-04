import time
import schedule
import logging
import subprocess
import sys
import signal
import os
from datetime import datetime
from pathlib import Path

# Ajustar path para encontrar módulos locales
sys.path.append(str(Path(__file__).parent))

from svc_v2.config_loader import load_settings

# Asegurar que directorio de logs exista
os.makedirs("logs", exist_ok=True)

# Configuración de Logging del Daemon
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [DAEMON] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/daemon_v2.log")
    ]
)

class Daemon:
    def __init__(self):
        self.running = True
        self.jobs_configured = False
        
        # Manejo de señales para salir elegante (Ctrl+C o Docker Stop)
        signal.signal(signal.SIGINT, self.shutdown)
        signal.signal(signal.SIGTERM, self.shutdown)

    def shutdown(self, signum, frame):
        logging.info("🛑 Recibida señal de parada. Cerrando Daemon...")
        self.running = False

    def run_job_subprocess(self, module_name: str, job_name: str):
        """
        Ejecuta un job en un subproceso aislado para garantizar
        limpieza total de memoria al terminar.
        """
        logging.info(f"🚀 Iniciando Job: {job_name} ({module_name})...")
        start_time = time.time()
        
        try:
            # Ejecutamos como módulo: python -m svc_v2.jobs.broad_scan
            # Usamos el mismo intérprete de python que está corriendo el daemon
            result = subprocess.run(
                [sys.executable, "-m", module_name],
                capture_output=False, # Dejar que imprima a stdout/stderr directo para ver logs en docker
                text=True
            )
            
            duration = time.time() - start_time
            if result.returncode == 0:
                logging.info(f"✅ Job {job_name} finalizado con éxito en {duration:.2f}s.")
            else:
                logging.error(f"❌ Job {job_name} falló con código {result.returncode}.")
                
        except Exception as e:
            logging.error(f"❌ Error crítico lanzando subproceso {job_name}: {e}")

    def refresh_schedule(self):
        """
        Recarga la configuración y regenera el calendario de tareas.
        Esto permite cambiar settings.yaml sin reiniciar el daemon.
        """
        try:
            cfg = load_settings()
            
            # Limpiar schedule anterior para evitar duplicados
            schedule.clear()
            
            logging.info("🔄 Configuración recargada. Actualizando Scheduler...")

            # 1. Broad Scan (Diario)
            if cfg.scheduler.jobs.get('broad_scan', {}).enabled:
                bs_cfg = cfg.scheduler.jobs['broad_scan']
                run_times = bs_cfg.run_at or ["16:15"]
                
                for t in run_times:
                    logging.info(f"   -> Programando Broad Scan a las {t}")
                    schedule.every().day.at(t).do(
                        self.run_job_subprocess, 
                        module_name="svc_v2.jobs.broad_scan", 
                        job_name="Broad Scan"
                    )

            # 2. Detailed Scan (Intradía) - Aún no existe el script, pero dejamos la lógica lista
            if cfg.scheduler.jobs.get('detailed_scan', {}).enabled:
                ds_cfg = cfg.scheduler.jobs['detailed_scan']
                interval = ds_cfg.interval_min or 15
                
                # Función wrapper para checar market hours
                def job_wrapper():
                    # Aquí iría la validación de Market Hours antes de lanzar el proceso
                    # Por ahora lo lanzamos directo y dejamos que el script decida si correr o no
                    self.run_job_subprocess("svc_v2.jobs.detailed_scan", "Detailed Scan")

                logging.info(f"   -> Programando Detailed Scan cada {interval} min")
                schedule.every(interval).minutes.do(job_wrapper)

            self.jobs_configured = True
            
        except Exception as e:
            logging.error(f"⚠️ Error recargando configuración: {e}")
            # Si falla la config, mantenemos el schedule anterior o reintentamos luego

    def start(self):
        logging.info("🔥 MarketDashboard V2 Daemon Iniciado")
        
        # Primera carga
        self.refresh_schedule()
        
        # Loop Principal
        while self.running:
            # 1. Ejecutar tareas pendientes
            schedule.run_pending()
            
            # 2. Hot Reload Check (Opcional: Re-leer config cada X ciclos)
            # Por simplicidad, recargamos cada hora para atrapar cambios en schedule
            # O podríamos observar el archivo. Por ahora simple.
            
            # 3. Sleep eficiente
            time.sleep(1)

if __name__ == "__main__":
    daemon = Daemon()
    daemon.start()
