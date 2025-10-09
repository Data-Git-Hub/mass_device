# consumers/mass_device_alerts.py
from __future__ import annotations
import os, smtplib, pathlib
from email.mime.text import MIMEText
from typing import Dict, Any, Tuple, List

from utils.utils_logger import get_logger

# --- Explicit dotenv load (Windows path) ---
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
ALERT_EMAIL_TO = (os.getenv("ALERT_EMAIL_TO", "") or "").strip()

SMS_ENABLED = os.getenv("SMS_ENABLED", "false").lower() == "true"
SMS_TO = (os.getenv("SMS_TO", "") or "").strip()
SMS_CARRIER = (os.getenv("SMS_CARRIER", "tmobile") or "tmobile").strip().lower()

CARRIER_DOMAINS = {
    "tmobile": "tmomail.net",
    "att": "txt.att.net",          # mms: mms.att.net
    "verizon": "vtext.com",        # mms: vzwpix.com
    "uscellular": "email.uscc.net",
    "mint": "tmomail.net",         # MVNO on T-Mo
    "googlefi": "msg.fi.google.com"
}

def _smtp_send(to_addr: str, subject: str, body: str) -> bool:
    if not (SMTP_HOST and ALERT_EMAIL_FROM and to_addr):
        log.info("Email not configured; skipping send to %s.", to_addr)
        return False
    msg = MIMEText(body, "plain", "utf-8")
    # Keep the subject very short (or blank) for carrier gateways
    msg["Subject"] = subject
    msg["From"] = ALERT_EMAIL_FROM
    msg["To"] = to_addr
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=20) as server:
            server.starttls()
            if SMTP_USER and SMTP_PASS:
                server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        log.info("Alert sent to %s", to_addr)
        return True
    except Exception as e:
        log.error("Send failed to %s: %s", to_addr, e)
        return False

def _split_recipients() -> Tuple[List[str], List[str]]:
    """Return (normal_emails, sms_gateways)."""
    normals: List[str] = []
    if ALERT_EMAIL_TO:
        normals = [x.strip() for x in ALERT_EMAIL_TO.split(",") if x.strip()]
    sms: List[str] = []
    if SMS_ENABLED and SMS_TO:
        domain = CARRIER_DOMAINS.get(SMS_CARRIER, "tmomail.net")
        sms.append(f"{SMS_TO}@{domain}")
    return normals, sms

def _compose_email_text(alert: Dict[str, Any]) -> Tuple[str, str]:
    # Normal email: fuller content
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
    )
    return subject, body

def _compose_sms_text(alert: Dict[str, Any]) -> Tuple[str, str]:
    """
    Carrier gateways prefer tiny messages. Use an empty/1-char subject and a
    single short line. Avoid URLs/emoji. Keep <160 chars just to be safe.
    """
    # Extremely short subject; some gateways ignore it anyway
    subject = ""  # or "."
    # One tight line
    t_f = (float(alert.get("temp_c", 0.0)) * 9/5) + 32
    p = alert.get("pressure_hpa", "")
    w = alert.get("wind_mps", "")
    reason = alert.get("reason", "Alert")
    body = f"MASS {reason}: {t_f:.0f}F {p}hPa wind {w}m/s {alert.get('ts_iso','')}"
    # Hard cap to 160 chars for safety
    body = body[:160]
    return subject, body

def maybe_send_alert(alert: Dict[str, Any]) -> None:
    if not ALERTS_ENABLED:
        log.info("Alerts disabled (ALERTS_ENABLED=false).")
        return

    normals, sms_addrs = _split_recipients()

    # Send normals (regular email formatting)
    if normals:
        subj, body = _compose_email_text(alert)
        for to in normals:
            _smtp_send(to, subj, body)

    # Send SMS (ultra-minimal formatting)
    for to in sms_addrs:
        subj, body = _compose_sms_text(alert)
        _smtp_send(to, subj, body)
