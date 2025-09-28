# producers/demo_mass_device_producer.py
from __future__ import annotations
import os, json, time, math, random, pathlib
from datetime import datetime, timezone

# ---- Optional utils (graceful fallback) ----
logger = None
def _get_logger():
    global logger
    if logger:
        return logger
    try:
        from utils.utils_logger import get_logger  # your helper
        logger = get_logger("demo_producer")
    except Exception:
        import logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        logger = logging.getLogger("demo_producer")
    return logger

def _load_env():
    """Load .env via your helper if available; fallback to plain env."""
    try:
        from utils.utils_env import load_env  # your helper
        load_env()
    except Exception:
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except Exception:
            pass

# ---- Config ----
_load_env()
TOPIC = os.getenv("KAFKA_TOPIC", "weather_demo")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")  # e.g. "localhost:9092"
POLL_SECONDS = float(os.getenv("DEMO_POLL_SECONDS", "1.0"))
OUTFILE = pathlib.Path("data/demo_stream.jsonl")
OUTFILE.parent.mkdir(parents=True, exist_ok=True)
log = _get_logger()

def _maybe_kafka_producer():
    """Return KafkaProducer if configured, else None."""
    if not BOOTSTRAP:
        return None
    try:
        from kafka import KafkaProducer  # kafka-python
        prod = KafkaProducer(
            bootstrap_servers=[s.strip() for s in BOOTSTRAP.split(",") if s.strip()],
            value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            linger_ms=50,
            retries=3,
        )
        return prod
    except Exception as e:
        log.warning("Kafka not available (%s). Falling back to file at %s", e, OUTFILE)
        return None

def _synth_weather(seq: int, t: float):
    """Generate plausible weather with cycles + noise + occasional 'storm'."""
    temp = 20 + 5*math.sin(t/300.0) + random.gauss(0, 0.3)
    pressure = 1013 + 3*math.sin(t/900.0) + random.gauss(0, 0.4)
    wind = max(0.0, 4 + 1.5*math.sin(t/200.0) + random.gauss(0, 0.5))
    gust = wind + abs(random.gauss(0, 1.2))
    precip = max(0.0, random.choice([0,0,0,0,0.2]) + random.random()*0.1)

    # Occasional event
    if random.random() < 0.02:
        pressure -= random.uniform(2.0, 5.0)
        gust += random.uniform(2.0, 5.0)
        precip += random.uniform(0.1, 1.0)

    return {
        "seq": seq,
        "ts_iso": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "station_id": "demo:sim",
        "lat": 38.5767,
        "lon": -92.1735,
        "temp_c": round(temp, 2),
        "apparent_c": round(temp + random.uniform(-0.5, 0.5), 2),
        "humidity_pct": round(min(100, max(20, 55 + random.gauss(0, 8))), 1),
        "wind_mps": round(wind, 2),
        "gust_mps": round(gust, 2),
        "pressure_hpa": round(pressure, 1),
        "precip_mmph": round(precip, 2),
        "precip_type": "none" if precip == 0 else "rain",
        "quality_flag": "ok",
        "source": "demo",
    }

def main():
    prod = _maybe_kafka_producer()
    log.info("Demo producer start | Kafka=%s | topic=%s", "ON" if prod else "OFF(file)", TOPIC)
    seq = 0
    try:
        while True:
            msg = _synth_weather(seq, time.time())
            if prod:
                prod.send(TOPIC, msg)
                if seq % 20 == 0:
                    prod.flush(timeout=2)
            else:
                with OUTFILE.open("a", encoding="utf-8") as f:
                    f.write(json.dumps(msg) + "\n")
            if seq % 10 == 0:
                log.info("tick #%d | %s | temp=%.2f°C p=%.1f hPa", seq, msg["ts_iso"], msg["temp_c"], msg["pressure_hpa"])
            seq += 1
            time.sleep(POLL_SECONDS)
    except KeyboardInterrupt:
        log.info("Demo producer stopped by user.")
    finally:
        if prod:
            try: prod.flush(2)
            except: pass
            try: prod.close(2)
            except: pass

if __name__ == "__main__":
    main()
