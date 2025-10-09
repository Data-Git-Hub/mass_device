# consumers/mass_device_alerts.py
from __future__ import annotations
import os, smtplib, pathlib, re
from email.mime.text import MIMEText
from typing import Dict, Any, Iterable, Optional

from utils.utils_logger import get_logger

# --- Explicit dotenv load (Windows path you’re using) ---
try:
    from dotenv import load_dotenv
    env_path = pathlib.Path("C:/Projects/mass_device/.env")
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
except Exception as e:
    print("Warning: could not load .env file:", e)

log = get_logger("mass_device_alerts")

# --- Global toggles & SMTP creds ---
ALERTS_ENABLED = os.getenv("ALERTS_ENABLED", "false").lower() == "true"

SMTP_HOST = os.getenv("SMTP_HOST", "")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")

ALERT_EMAIL_FROM = os.getenv("ALERT_EMAIL_FROM", SMTP_USER or "")
ALERT_EMAIL_TO = os.getenv("ALERT_EMAIL_TO", "").strip()

# --- SMS settings (email-to-SMS gateway) ---
SMS_ENABLED = os.getenv("SMS_ENABLED", "false").lower() == "true"
SMS_TO = os.getenv("SMS_TO", "").strip()            # e.g. 4174701822 (digits only or with punctuation)
SMS_CARRIER = (os.getenv("SMS_CARRIER", "tmobile")  # tmobile, att, verizon, sprint, googlefi, uscellular
               .strip().lower())

# Carrier → gateway domain map
CARRIER_DOMAINS = {
    "tmobile":   "tmomail.net",            # SMS/MMS
    "att":       "txt.att.net",            # SMS  (MMS: mms.att.net)
    "verizon":   "vtext.com",              # SMS  (MMS: vzwpix.com)
    "sprint":    "messaging.sprintpcs.com",
    "googlefi":  "msg.fi.google.com",
    "uscellular":"email.uscc.net",
}

def _split_csv(s: str) -> list[str]:
    return [x.strip() for x in s.split(",") if x.strip()]

def _digits_only(num: str) -> str:
    return re.sub(r"\D+", "", num or "")

def _sms_address(number: str, carrier: str) -> Optional[str]:
    n = _digits_only(number)
    if len(n) != 10:
        log.error("SMS number must be 10 digits; got: %r", number)
        return None
    domain = CARRIER_DOMAINS.get(carrier)
    if not domain:
        log.error("Unknown SMS_CARRIER=%r. Supported: %s", carrier, ", ".join(CARRIER_DOMAINS))
        return None
    return f"{n}@{domain}"

def _send_email(to_addr: str, subject: str, body: str) -> None:
    """Send a single email (or email-to-SMS) to one recipient."""
    if not (SMTP_HOST and ALERT_EMAIL_FROM and to_addr):
        log.info("Email not configured; skipping send. (host/from/to missing)")
        return

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = ALERT_EMAIL_FROM
    msg["To"] = to_addr

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=25) as server:
            server.starttls()
            if SMTP_USER and SMTP_PASS:
                server.login(SMTP_USER, SMTP_PASS)
            server.send_message(msg)
        log.info("Alert sent to %s", to_addr)
    except Exception as e:
        log.error("Send failed to %s: %s", to_addr, e)

def _send_bulk(recipients: Iterable[str], subject: str, body: str) -> None:
    for r in recipients:
        _send_email(r, subject, body)

def maybe_send_alert(alert: Dict[str, Any]) -> None:
    """
    Sends alerts to:
      - normal email recipients (ALERT_EMAIL_TO)
      - optional SMS via carrier gateway (if SMS_ENABLED, SMS_TO)
    SMS gets a short subject/body to avoid carrier spam filters.
    """
    if not ALERTS_ENABLED:
        log.info("Alerts disabled (ALERTS_ENABLED=false).")
        return

    # --- Build recipient lists ---
    email_recipients = _split_csv(ALERT_EMAIL_TO)
    sms_recipient = _sms_address(SMS_TO, SMS_CARRIER) if SMS_ENABLED and SMS_TO else None

    # --- Compose messages ---
    # Full email (for inbox)
    reason = alert.get("reason", "Weather alert")
    subject_email = f"[MASS] {reason}"
    body_email = (
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

    # Minimal SMS (to reduce filtering):
    # keep ASCII, keep it short, avoid URLs/specials
    subject_sms = "[MASS]"  # short subject
    t_c = alert.get("temp_c")
    p = alert.get("pressure_hpa")
    z = alert.get("z_score")
    ts = alert.get("ts_iso")
    # Example: [MASS] Temp anomaly z=2.1, 22.5C, p=1019 hPa @ 06:12:40Z
    body_sms = f"{reason[:22]} z={z:.1f} {t_c:.1f}C p={p:.0f}hPa @{ts[-9:]}" if isinstance(z, (int,float)) else \
               f"{reason[:28]} {t_c:.1f}C p={p:.0f}hPa @{ts[-9:]}"

    # --- Send email(s) ---
    if email_recipients:
        _send_bulk(email_recipients, subject_email, body_email)

    # --- Send SMS (email->SMS) ---
    if sms_recipient:
        _send_email(sms_recipient, subject_sms, body_sms)
