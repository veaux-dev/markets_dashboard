import requests
import logging
import os
from datetime import datetime, timedelta
from svc_v2.db import Database
from svc_v2.config_loader import load_settings

class Notifier:
    def __init__(self, db: Database):
        self.db = db
        self.cfg = load_settings()
        
        # Cargar URLs desde env o config
        # Prioridad: ENV VAR (Docker) > settings.yaml
        self.discord_url = os.environ.get("DISCORD_WEBHOOK_URL") or self.cfg.alerts.channels.get("default")
        self.enabled = self.cfg.alerts.enable_discord and self.discord_url

    def should_notify(self, ticker: str, strategy: str, timeframe: str, cooldown_hours: int = 6) -> bool:
        """Verifica si ya se envió esta misma alerta recientemente."""
        try:
            query = f"""
                SELECT count(*) FROM signal_history
                WHERE ticker = '{ticker}' 
                  AND strategy = '{strategy}'
                  AND timeframe = '{timeframe}'
                  AND sent_at > now() - INTERVAL {cooldown_hours} HOUR
            """
            count = self.db.conn.execute(query).fetchone()[0]
            return count == 0
        except Exception as e:
            logging.error(f"Error checking signal history: {e}")
            return True # Notificar en caso de duda

    def log_notification(self, ticker: str, strategy: str, timeframe: str, price: float):
        """Registra el envío en la base de datos."""
        try:
            self.db.conn.execute("""
                INSERT INTO signal_history (ticker, strategy, timeframe, price)
                VALUES (?, ?, ?, ?)
            """, [ticker, strategy, timeframe, price])
        except Exception as e:
            logging.error(f"Error logging notification: {e}")

    def is_holding(self, ticker: str) -> bool:
        """Verifica si el ticker está actualmente en el portafolio."""
        try:
            res = self.db.conn.execute(
                "SELECT count(*) FROM view_portfolio_holdings WHERE ticker = ?", 
                [ticker]
            ).fetchone()
            return res[0] > 0
        except Exception as e:
            logging.error(f"Error checking holdings for {ticker}: {e}")
            return False

    def send_discord(self, message: str, ticker: str = None, strategy: str = None, timeframe: str = None, price: float = None):
        """Envía mensaje a Discord y lo registra."""
        if not self.enabled:
            return

        # Si tenemos metadatos, verificamos spam y reglas de negocio
        if ticker and strategy and timeframe:
            # Regla 1: No spamear (Cooldown)
            if not self.should_notify(ticker, strategy, timeframe):
                logging.info(f"🚫 Notificación silenciada (Spam Control): {ticker} {strategy}")
                return
            
            # Regla 2: SELL solo si tengo posición
            if "SELL" in strategy and not self.is_holding(ticker):
                logging.info(f"🚫 Notificación silenciada (No Holding): {ticker} {strategy}")
                return

        try:
            payload = {"content": message}
            response = requests.post(self.discord_url, json=payload, timeout=10)
            
            if response.status_code in [200, 204]:
                logging.info(f"✅ Notificación enviada a Discord: {message[:50]}...")
                if ticker and strategy and timeframe:
                    self.log_notification(ticker, strategy, timeframe, price)
            else:
                logging.error(f"❌ Error enviando a Discord ({response.status_code}): {response.text}")
                
        except Exception as e:
            logging.error(f"❌ Error crítico en Notifier: {e}")

    def notify_strategy_hit(self, ticker: str, strategy: str, timeframe: str, price: float, extra_info: str = ""):
        """Formatea y envía una alerta de estrategia."""
        emoji = "🟢" if "BUY" in strategy else "🔴" if "SELL" in strategy else "⚪"
        
        msg = f"{emoji} **SIGNAL DETECTED** {emoji}\n"
        msg += f"**Ticker:** `{ticker}`\n"
        msg += f"**Strategy:** `{strategy}`\n"
        msg += f"**Timeframe:** `{timeframe}`\n"
        msg += f"**Price:** `${price:.2f}`\n"
        if extra_info:
            msg += f"**Note:** {extra_info}\n"
        
        # Link a la Triple Screen (IP NAS)
        msg += f"[🔍 Ver en Dashboard](http://192.168.50.227:8000/static/triple_screen.html?ticker={ticker})"

        self.send_discord(msg, ticker, strategy, timeframe, price)
