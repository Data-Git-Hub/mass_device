# consumers/mass_device_alerts.py
from __future__ import annotations
from dataclasses import dataclass
from typing import Optional, Dict

@dataclass
class AlertPolicy:
    pressure_drop_hpa: float = 3.0
    wind_gust_mps: float = 15.0
    enabled: bool = False

def evaluate_message(msg: Dict, policy: AlertPolicy) -> Optional[str]:
    """Return a human message if an alert should fire, else None."""
    if not policy.enabled:
        return None
    try:
        if float(msg.get("gust_mps", 0)) >= policy.wind_gust_mps:
            return f"High wind gust: {msg['gust_mps']} m/s at {msg['ts_iso']}"
    except Exception:
        pass
    # TODO: implement rolling pressure drop logic here (needs window state)
    return None

def send_email(subject: str, body: str) -> None:
    """Wire up SMTP from .env here later."""
    # TODO: implement with smtplib using SMTP_HOST/PORT/USER/PASS
    pass

def send_sms(body: str) -> None:
    """Wire up SMS provider/gateway from .env here later."""
    pass
