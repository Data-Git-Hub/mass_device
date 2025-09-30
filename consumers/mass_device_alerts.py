# consumers/mass_device_alerts.py
from __future__ import annotations
import os, smtplib
from email.mime.text import MIMEText
from typing import Dict, Any, List
from utils.utils_logger import get_logger
from utils.utils_env import load_env

load_env()
log = get_logger("mass_device_alerts")

ALERTS_ENABLED = os.getenv("ALERTS_ENABLED", "false").lower() == "true"

# --- SMTP / Email env ---
SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")
ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", SMTP_USER or "")

# --- Comma-separated recipients (email + SMS gateways) ---
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "")

# --- Basic list of common SMS email gateways (auto-short body) ---
_SMS_GATEWAY_HINTS = (
    "@tmomail.net", "@vtext.com", "@txt.att.net", "@messaging.sprintpcs.com",
    "@email.uscc.net", "@pm.sprint.com", "@tmomail.net"
)

def _recipients() -> List[str]:
    return [r.strip() for r in ALERT_EMAIL_TO.split(",") if r.strip()]

def _is_sms_gateway(addr: str) -> bool:
    a = addr.lower()
    return any(h in a for h in _SMS_GATEWAY_HINTS)

def _build_email(subject: str, body: str, to_addr: str) -> MIMEText:
    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = ALERT_EMAIL_FROM
    msg["To"] = to_addr
    return msg

def _send_one(to_addr: str, subject: str, body: str) -> None:
    if not (SMTP_HOST and ALERT_EMAIL_FROM and to_addr):
        log.info("Email not configured; skipping send. (Set SMTP_* and ALERT_EMAIL_*)")
        return
    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=10) as server:
            server.starttls()
            if SMTP_USER and SMTP_PASS:
                server.login(SMTP_USER, SMTP_PASS)
            server.send_message(_build_email(subject, body, to_addr))
        log.info("Alert sent to %s", to_addr)
    except Exception as e:
        log.error("Send failed to %s: %s", to_addr, e)

def maybe_send_alert(alert: Dict[str, Any]) -> None:
    """Send alert via email/SMS gateways if enabled; otherwise just log."""
    if not ALERTS_ENABLED:
        log.info("Alerts disabled (ALERTS_ENABLED=false). Not sending.")
        return

    recips = _recipients()
    if not recips:
        log.info("No ALERT_EMAIL_TO configured; skipping send.")
        return

    # Build full email body
    reason = alert.get("reason", "Weather alert")
    subject_full = f"[M.A.S.S.] {reason}"
    body_full = (
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

    # Short SMS-friendly body (single line, trimmed)
    body_sms = (
        f"MASS {reason} | {alert.get('ts_iso')} "
        f"T:{alert.get('temp_c')}C P:{alert.get('pressure_hpa')}hPa "
        f"RH:{alert.get('humidity_pct')}% W:{alert.get('wind_mps')}m/s G:{alert.get('gust_mps')}m/s "
        f"z:{alert.get('z_score')}"
    )
    # Trim to ~150 chars for safety
    body_sms = (body_sms[:148] + "…") if len(body_sms) > 150 else body_sms

    # Send per recipient
    for to_addr in recips:
        use_sms = _is_sms_gateway(to_addr)
        subject = subject_full if not use_sms else f"MASS: {reason}"
        body = body_full if not use_sms else body_sms
        _send_one(to_addr, subject, body)
