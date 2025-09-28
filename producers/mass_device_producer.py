# producers/mass_device_producer.py
from __future__ import annotations
import os, json, time, pathlib
from datetime import datetime, timezone

from utils.utils_env import load_env, env
from utils.utils_logger import get_logger
from utils.kafka_io import get_producer

# TEMP: reuse demo generator so this file runs now
from producers.demo_mass_device_producer import _synth_weather  # swap later

load_env()
log = get_logger("mass_device_producer")

TOPIC = os.getenv("KAFKA_TOPIC", "weather_live")
POLL_SECONDS = float(env("POLL_SECONDS", "30", cast=float))  # live API later
FALLBACK_FILE = pathlib.Path("data/demo_stream.jsonl")
FALLBACK_FILE.parent.mkdir(parents=True, exist_ok=True)

def main():
    prod = get_producer()
    log.info("mass_device_producer start | Kafka=%s | topic=%s", "ON" if prod else "OFF(file)", TOPIC)
    seq = 0
    try:
        while True:
            # TODO: replace with real API fetch + normalization
            msg = _synth_weather(seq, time.time())
            payload = json.dumps(msg).encode("utf-8")
            if prod:
                prod.send(TOPIC, payload)
                if seq % 20 == 0: prod.flush(timeout=2)
            else:
                with FALLBACK_FILE.open("a", encoding="utf-8") as f:
                    f.write(payload.decode("utf-8") + "\n")
            if seq % 10 == 0:
                log.info("emitted #%d | %s | temp=%.2f°C p=%.1f hPa",
                         seq, msg["ts_iso"], msg["temp_c"], msg["pressure_hpa"])
            seq += 1
            time.sleep(POLL_SECONDS)
    except KeyboardInterrupt:
        log.info("producer stopped")
    finally:
        if prod:
            try: prod.flush(2); prod.close(2)
            except Exception: pass

if __name__ == "__main__":
    main()
