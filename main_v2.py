import time
import schedule
import logging
import subprocess
import sys
import signal
import os
from datetime import datetime, timedelta
from pathlib import Path

# Ajustar path para encontrar módulos locales
sys.path.append(str(Path(__file__).parent))

from svc_v2.config_loader import load_settings
from svc_v2.db import Database

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
        self.db = Database()
        
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
            
            # Log next run time
            next_run = schedule.next_run()
            if next_run:
                delta = next_run - datetime.now()
                logging.info(f"⏳ Próxima ejecución programada en: {str(delta).split('.')[0]} (a las {next_run.strftime('%H:%M:%S')})")

        except Exception as e:
            logging.error(f"❌ Error crítico lanzando subproceso {job_name}: {e}")

    def check_staleness(self):
        """
        Verifica si los datos están muy viejos al inicio y corre los jobs si hace falta.
        """
        logging.info("🕵️ Verificando frescura de datos...")
        try:
            # 1. Broad Scan (Diario)
            # Checamos si hay datos de hoy (1d)
            res = self.db.conn.execute("SELECT max(timestamp) FROM ohlcv WHERE timeframe='1d'").fetchone()
            last_1d = res[0]
            
            should_run_broad = False
            if last_1d is None:
                should_run_broad = True
            else:
                # Si la última vela diaria es de hace más de 20 horas, asumimos que falta scan
                if (datetime.now() - last_1d).total_seconds() > 20 * 3600:
                    should_run_broad = True
            
            if should_run_broad:
                logging.warning("⚠️ Datos diarios obsoletos. Ejecutando Broad Scan de inmediato.")
                self.run_job_subprocess("svc_v2.jobs.broad_scan", "Broad Scan (Startup)")

            # 2. Detailed Scan (Intradía)
            # Checamos timestamp de indicators 1h
            res = self.db.conn.execute("SELECT max(timestamp) FROM indicators WHERE timeframe='1h'").fetchone()
            last_1h = res[0]
            
            should_run_detailed = False
            if last_1h is None:
                should_run_detailed = True
            else:
                # Si pasó más de 1 hora
                if (datetime.now() - last_1h).total_seconds() > 3600:
                    should_run_detailed = True
            
            if should_run_detailed:
                logging.warning("⚠️ Datos intradía obsoletos. Ejecutando Detailed Scan de inmediato.")
                self.run_job_subprocess("svc_v2.jobs.detailed_scan", "Detailed Scan (Startup)")

        except Exception as e:
            logging.error(f"Error verificando staleness: {e}")

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

            # 2. Detailed Scan (Intradía)
            if cfg.scheduler.jobs.get('detailed_scan', {}).enabled:
                ds_cfg = cfg.scheduler.jobs['detailed_scan']
                interval = ds_cfg.interval_min or 15
                
                # Función wrapper para checar market hours
                def job_wrapper():
                    # Aquí iría la validación de Market Hours
                    self.run_job_subprocess("svc_v2.jobs.detailed_scan", "Detailed Scan")

                logging.info(f"   -> Programando Detailed Scan cada {interval} min")
                schedule.every(interval).minutes.do(job_wrapper)

            self.jobs_configured = True
            
            # Log initial next run
            next_run = schedule.next_run()
            if next_run:
                logging.info(f"⏳ Primera ejecución programada: {next_run.strftime('%H:%M:%S')}")

        except Exception as e:
            logging.error(f"⚠️ Error recargando configuración: {e}")

    def start(self):
        logging.info("🔥 MarketDashboard V2 Daemon Iniciado")
        
        # Primera carga de schedule
        self.refresh_schedule()
        
        # Verificar si hay que correr YA
        self.check_staleness()
        
        # Loop Principal
        while self.running:
            schedule.run_pending()
            time.sleep(1)

if __name__ == "__main__":
    daemon = Daemon()
    daemon.start()
