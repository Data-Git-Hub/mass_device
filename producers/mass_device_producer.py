# producers/mass_device_producer.py
from __future__ import annotations
import os, json, time, pathlib
from datetime import datetime, timezone
from typing import Optional, Dict, Any

import requests

from utils.utils_env import load_env, env
from utils.utils_logger import get_logger
from utils.kafka_io import get_producer

# --- Fallback demo generator (Keeps the producer always runs) ---
from producers.demo_mass_device_producer import _synth_weather

load_env()
log = get_logger("mass_device_producer")

TOPIC = os.getenv("KAFKA_TOPIC", "weather_live")
POLL_SECONDS = float(env("POLL_SECONDS", "30", cast=float))

# --- Provider selection: "open-meteo" (default) or "openweather" ---
PROVIDER = os.getenv("WEATHER_PROVIDER", "open-meteo").strip().lower()

# --- Location (required for both providers) ---
LAT = float(env("LOCATION_LAT", "39.7392", cast=float))   # default: Denver, CO
LON = float(env("LOCATION_LON", "-104.9903", cast=float))

# --- OpenWeatherMap options (if you choose that provider) ---
OWM_API_KEY = os.getenv("OPENWEATHER_API_KEY")  # keep in .env only!
OWM_BASE = os.getenv("OPENWEATHER_BASE", "https://api.openweathermap.org/data/2.5/weather")
OWM_UNITS = os.getenv("OPENWEATHER_UNITS", "metric")  # metric => temp in °C, wind in m/s

# --- Filesystem fallback if Kafka is off ---
FALLBACK_FILE = pathlib.Path("data/demo_stream.jsonl")
FALLBACK_FILE.parent.mkdir(parents=True, exist_ok=True)

# --- Normalizers ---

def _normalize_open_meteo(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Open-Meteo current fields are under payload['current'] when requested with ?current=...
    Docs: https://open-meteo.com/en/docs
    """
    try:
        cur = payload["current"]
        msg = {
            "ts_iso": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "provider": "open-meteo",
            "lat": float(payload.get("latitude", LAT)),
            "lon": float(payload.get("longitude", LON)),
            "temp_c": float(cur.get("temperature_2m")),
            "pressure_hpa": float(cur.get("pressure_msl")),
            "humidity_pct": float(cur.get("relative_humidity_2m")),
            "wind_mps": float(cur.get("wind_speed_10m", 0.0)),
            "gust_mps": float(cur.get("wind_gusts_10m", 0.0)),
        }
        return msg
    except Exception as e:
        log.warning("Open-Meteo normalize failed: %s", e)
        return None

def _normalize_openweather(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    OpenWeatherMap /weather normalization
    Docs: https://openweathermap.org/current
    """
    try:
        main = payload["main"]
        wind = payload.get("wind", {}) or {}
        coord = payload.get("coord", {}) or {}
        msg = {
            "ts_iso": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "provider": "openweather",
            "lat": float(coord.get("lat", LAT)),
            "lon": float(coord.get("lon", LON)),
            "temp_c": float(main["temp"]),                # °C if units=metric
            "pressure_hpa": float(main["pressure"]),      # hPa
            "humidity_pct": float(main["humidity"]),      # %
            "wind_mps": float(wind.get("speed", 0.0)),    # m/s
            "gust_mps": float(wind.get("gust", 0.0)),     # m/s (may be missing)
        }
        return msg
    except Exception as e:
        log.warning("OpenWeather normalize failed: %s", e)
        return None

# --- Fetchers ---

def _fetch_open_meteo(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": ",".join([
            "temperature_2m",
            "relative_humidity_2m",
            "pressure_msl",
            "wind_speed_10m",
            "wind_gusts_10m"
        ]),
        # Can later add "precipitation", "rain", etc. if desired
        "timezone": "UTC"
    }
    try:
        r = requests.get(url, params=params, timeout=10)
        r.raise_for_status()
        return _normalize_open_meteo(r.json())
    except Exception as e:
        log.warning("Open-Meteo fetch failed: %s", e)
        return None

def _fetch_openweather(lat: float, lon: float) -> Optional[Dict[str, Any]]:
    if not OWM_API_KEY:
        log.warning("OPENWEATHER_API_KEY not set; cannot fetch OpenWeather live data.")
        return None
    params = {"lat": lat, "lon": lon, "appid": OWM_API_KEY, "units": OWM_UNITS}
    try:
        r = requests.get(OWM_BASE, params=params, timeout=10)
        try: r.raise_for_status()
        except Exception as e:
            log.warning("OpenWeather HTTP %s | URL=%s | body=%s", r.status_code, r.url, r.text[:300])
            return None
        msg = _normalize_openweather(r.json())
        if msg and OWM_UNITS.lower() == "imperial":
            msg["temp_c"] = (msg["temp_c"] - 32.0) * 5.0/9.0
        return msg
    except Exception as e:
        log.warning("OpenWeather fetch failed: %s", e)
        return None

def _fetch_live() -> Optional[Dict[str, Any]]:
    if PROVIDER == "openweather":
        return _fetch_openweather(LAT, LON)
    # default
    return _fetch_open_meteo(LAT, LON)

# --- Main loop ---

def main():
    prod = get_producer()
    log.info(
        "mass_device_producer start | Kafka=%s | topic=%s | provider=%s | lat=%.4f lon=%.4f",
        "ON" if prod else "OFF(file)", TOPIC, PROVIDER, LAT, LON
    )
    seq = 0
    try:
        while True:
            # Try live first
            msg = _fetch_live()
            # If live fails, use synthetic so the pipeline keeps running
            if not msg:
                msg = _synth_weather(seq, time.time())

            payload = json.dumps(msg).encode("utf-8")

            if prod:
                prod.send(TOPIC, payload)
                if seq % 20 == 0:
                    prod.flush(timeout=2)
            else:
                with FALLBACK_FILE.open("a", encoding="utf-8") as f:
                    f.write(payload.decode("utf-8") + "\n")

            log.info(
                "emitted #%d | %s | provider=%s | temp=%.2f°C p=%.1f hPa hum=%.0f%% wind=%.2f m/s",
                seq, msg["ts_iso"], msg.get("provider", "n/a"),
                msg.get("temp_c", float("nan")),
                msg.get("pressure_hpa", float("nan")),
                msg.get("humidity_pct", float("nan")),
                msg.get("wind_mps", float("nan")),
            )
            seq += 1
            time.sleep(POLL_SECONDS)
    except KeyboardInterrupt:
        log.info("producer stopped")
    finally:
        if prod:
            try:
                prod.flush(2)
                prod.close(2)
            except Exception:
                pass

if __name__ == "__main__":
    main()
