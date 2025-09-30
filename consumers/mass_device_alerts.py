# consumers/mass_device_alerts.py
from __future__ import annotations
import os, smtplib, pathlib
from email.mime.text import MIMEText
from typing import Dict, Any

from utils.utils_logger import get_logger

# --- Explicit dotenv load ---
try:
    from dotenv import load_dotenv
    env_path = pathlib.Path("C:/Projects/mass_device/.env")
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
except Exception as e:
    print("Warning: could not load .env file:", e)

log = get_logger("mass_device_alerts")

# --- Global settings ---
ALERTS_ENABLED = os.getenv("ALERTS_ENABLED", "false").lower() == "true"

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")

ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", SMTP_USER or "")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "")

def _send_email(subject: str, body: str) -> None:
    if not (SMTP_HOST and ALERT_EMAIL_TO and ALERT_EMAIL_FROM):
        log.info("Email not configured; skipping send.")
        return

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = ALERT_EMAIL_FROM
    msg["To"] = ALERT_EMAIL_TO

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
            server.starttls()
            if SMTP_USER and SMTP_PASS:
                server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        log.info("Alert email sent to %s", ALERT_EMAIL_TO)
    except Exception as e:
        log.error("Email send failed: %s", e)

def maybe_send_alert(alert: Dict[str, Any]) -> None:
    """Send alert via email if enabled; otherwise just log."""
    if not ALERTS_ENABLED:
        log.info("Alerts disabled (ALERTS_ENABLED=false).")
        return

    reason = alert.get("reason", "Weather alert")
    subject = f"[M.A.S.S.] {reason}"
    body = (
        f"Time: {alert.get('ts_iso')}\n"
        f"Provider: {alert.get('provider')}\n"
        f"Location: ({alert.get('lat')}, {alert.get('lon')})\n"
        f"Temp: {alert.get('temp_c')} °C\n"
        f"Pressure: {alert.get('pressure_hpa')} hPa\n"
        f"Humidity: {alert.get('humidity_pct')} %\n"
        f"Wind: {alert.get('wind_mps')} m/s  Gust: {alert.get('gust_mps')} m/s\n"
        f"z-score: {alert.get('z_score')}\n"
        f"Thresholds: {alert.get('thresholds')}\n"
    )
    _send_email(subject, body)
