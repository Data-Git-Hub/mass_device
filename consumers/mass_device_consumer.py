# consumers/mass_device_consumer.py
from __future__ import annotations
import os, json, time, pathlib, collections, sys
from datetime import datetime
from typing import Dict, Any
import numpy as np
import matplotlib.pyplot as plt

# --- Ensure project root import path ---
from pathlib import Path as _Path
_root = _Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from meteorological_theories.demo_calc import (
    zscores, dew_point_c, wind_chill_c
)
from utils.utils_jsondb import append_jsonl, ensure_parent

# Optional: email/SMS hooks (no-op if not configured)
try:
    from consumers.mass_device_alerts import maybe_send_alert
except Exception:
    def maybe_send_alert(_alert: Dict[str, Any]) -> None:
        pass

def _c_to_f(c: float) -> float:
    return (c * 9.0/5.0) + 32.0

# --- Logger / Env ---
logger = None
def _get_logger():
    global logger
    if logger:
        return logger
    try:
        from utils.utils_logger import get_logger
        logger = get_logger("mass_device_consumer")
    except Exception:
        import logging
        logging.basicConfig(level=logging.INFO,
                            format="%(asctime)s %(levelname)s %(message)s")
        logger = logging.getLogger("mass_device_consumer")
    return logger

def _load_env():
    try:
        from utils.utils_env import load_env
        load_env()
    except Exception:
        try:
            from dotenv import load_dotenv
            load_dotenv()
        except Exception:
            pass

_load_env()
log = _get_logger()

TOPIC = os.getenv("KAFKA_TOPIC", "weather_live")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
INFILE = pathlib.Path("data/demo_stream.jsonl")

WINDOW = int(os.getenv("WINDOW_POINTS", "180"))
ZTH = float(os.getenv("Z_THRESHOLD", "2.0"))
ALERTS_ENABLED = os.getenv("ALERTS_ENABLED", "false").lower() == "true"
ALERT_PRESS_DROP = float(os.getenv("ALERT_PRESSURE_DROP_HPA", "3"))
ALERT_WIND_GUST = float(os.getenv("ALERT_WIND_GUST_MPS", "15"))

# --- JSON database ---
JSON_DB_DIR = pathlib.Path("data/db")
CONSUMER_DB = JSON_DB_DIR / "consumer_stream.jsonl"
ALERTS_DB   = JSON_DB_DIR / "alerts.jsonl"
ensure_parent(CONSUMER_DB)
ensure_parent(ALERTS_DB)

# --- Kafka wiring ---
def _maybe_kafka_consumer():
    if not BOOTSTRAP:
        return None
    try:
        from kafka import KafkaConsumer
        return KafkaConsumer(
            TOPIC,
            bootstrap_servers=[s.strip() for s in BOOTSTRAP.split(",") if s.strip()],
            value_deserializer=lambda b: json.loads(b.decode("utf-8")),
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id=os.getenv("KAFKA_GROUP_ID", "mass_device"),
        )
    except Exception as e:
        log.warning("Kafka not available (%s). Falling back to file tail at %s", e, INFILE)
        return None

def _kafka_stream(consumer):
    while True:
        try:
            records = consumer.poll(timeout_ms=1000)
            if records:
                for _tp, msgs in records.items():
                    for m in msgs:
                        yield m.value
            else:
                time.sleep(0.1)
        except Exception as e:
            log.error("Kafka poll error: %s", e)
            time.sleep(1.0)

def _tail_jsonl(path: pathlib.Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)
    with path.open("r", encoding="utf-8") as f:
        f.seek(0, 2)
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.2)
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue

# --- Alert helpers ---
def _emit_alert(reason: str, ctx: Dict[str, Any]) -> None:
    alert = {
        "ts_iso": ctx.get("ts_iso"),
        "reason": reason,
        "provider": ctx.get("provider"),
        "lat": ctx.get("lat"),
        "lon": ctx.get("lon"),
        "temp_c": ctx.get("temp_c"),
        "pressure_hpa": ctx.get("pressure_hpa"),
        "humidity_pct": ctx.get("humidity_pct"),
        "wind_mps": ctx.get("wind_mps"),
        "gust_mps": ctx.get("gust_mps"),
        "z_score": ctx.get("z_score"),
        "thresholds": {
            "ZTH": ZTH,
            "ALERT_PRESSURE_DROP_HPA": ALERT_PRESS_DROP,
            "ALERT_WIND_GUST_MPS": ALERT_WIND_GUST,
        }
    }
    append_jsonl(ALERTS_DB, alert)
    log.warning("ALERT: %s | t=%.1f°C p=%.1f hPa wind=%.2f m/s gust=%.2f m/s",
                reason, ctx.get("temp_c", float("nan")),
                ctx.get("pressure_hpa", float("nan")),
                ctx.get("wind_mps", float("nan")),
                ctx.get("gust_mps", float("nan")))
    if ALERTS_ENABLED:
        try:
            maybe_send_alert(alert)
        except Exception as e:
            log.error("Alert send failed: %s", e)

# --- Main ---
def main():
    cons = _maybe_kafka_consumer()
    log.info("Consumer start | Kafka=%s | topic=%s | window=%d | zth=%.2f",
             "ON" if cons else "OFF(file)", TOPIC, WINDOW, ZTH)

    times = collections.deque(maxlen=WINDOW)
    temps_f = collections.deque(maxlen=WINDOW)
    press = collections.deque(maxlen=WINDOW)

    plt.ion()
    fig, ax1 = plt.subplots(figsize=(9, 4.8))
    ax2 = ax1.twinx()

    fig.suptitle("M.A.S.S. Device — Live Temp & Pressure with Anomalies", y=0.985)
    fig.tight_layout(rect=(0.06, 0.12, 0.98, 0.90))

    line_t, = ax1.plot([], [], lw=1.7, label="Temp (°F)")
    line_p, = ax2.plot([], [], lw=1.0, alpha=0.75, label="Pressure (hPa)")
    scat = ax1.scatter([], [], s=28, marker="o", edgecolors="none", alpha=0.9)

    ax1.set_xlabel("Time (HH:MM:SS)", labelpad=8)
    ax1.set_ylabel("Temp (°F)")
    ax2.set_ylabel("Pressure (hPa)")
    ax1.grid(True, alpha=0.3)

    h1, l1 = ax1.get_legend_handles_labels()
    h2, l2 = ax2.get_legend_handles_labels()
    ax1.legend(h1 + h2, l1 + l2, loc="lower right",
               bbox_to_anchor=(0.985, 0.04), borderaxespad=0.6, framealpha=0.9)

    ax1._metrics_box = ax1.text(
        0.01, 0.98, "",
        transform=ax1.transAxes,
        ha="left", va="top", fontsize=9,
        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="0.8", alpha=0.75)
    )

    def redraw():
        if not times:
            return
        xs = np.arange(len(times))
        t_arr_f = np.array(temps_f, dtype=float)
        p_arr = np.array(press, dtype=float)

        line_t.set_data(xs, t_arr_f)
        line_p.set_data(xs, p_arr)

        ax1.set_xlim(0, max(10, len(xs) - 1))
        if t_arr_f.size:
            ax1.set_ylim(t_arr_f.min() - 2.0, t_arr_f.max() + 2.0)
        if p_arr.size:
            ax2.set_ylim(p_arr.min() - 1.5, p_arr.max() + 1.5)

        if t_arr_f.size:
            t_arr_c = (t_arr_f - 32.0) * 5.0/9.0
            z = zscores(t_arr_c)
            mask = np.abs(z) >= ZTH
            scat.set_offsets(np.c_[xs[mask], t_arr_f[mask]])
        else:
            scat.set_offsets([])

        fig.canvas.draw_idle()
        fig.canvas.flush_events()

    source_iter = _kafka_stream(cons) if cons else _tail_jsonl(INFILE)
    last_pressure: float | None = None
    first = True

    try:
        for obj in source_iter:
            try:
                append_jsonl(CONSUMER_DB, obj)
            except Exception as e:
                log.warning("Failed to append consumer row: %s", e)

            try:
                ts = datetime.strptime(obj["ts_iso"], "%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                ts = datetime.fromisoformat(obj["ts_iso"].replace("Z", "+00:00")).replace(tzinfo=None)
            times.append(ts)

            t_c = float(obj.get("temp_c", np.nan))
            p_hpa = float(obj.get("pressure_hpa", np.nan))
            h_pct = float(obj.get("humidity_pct", 60.0))
            w_mps = float(obj.get("wind_mps", 3.0))
            g_mps = float(obj.get("gust_mps", 0.0))

            temps_f.append(_c_to_f(t_c))
            press.append(p_hpa)

            dp_f = _c_to_f(dew_point_c(t_c, h_pct))
            wc_f = _c_to_f(wind_chill_c(t_c, w_mps))

            n = len(temps_f)
            t_arr_c_now = (np.array(temps_f, dtype=float) - 32.0) * 5.0/9.0
            z_now = zscores(t_arr_c_now) if n else np.array([])
            anomalies = int((np.abs(z_now) >= ZTH).sum()) if z_now.size else 0

            ax1._metrics_box.set_text(
                f"n={n} | anomalies={anomalies} • dew≈{dp_f:.1f}°F • wind chill≈{wc_f:.1f}°F"
            )

            ctx = {
                "ts_iso": obj.get("ts_iso"),
                "provider": obj.get("provider"),
                "lat": obj.get("lat"),
                "lon": obj.get("lon"),
                "temp_c": t_c,
                "pressure_hpa": p_hpa,
                "humidity_pct": h_pct,
                "wind_mps": w_mps,
                "gust_mps": g_mps,
                "z_score": None
            }

            if z_now.size:
                last_z = float(z_now[-1])
                if abs(last_z) >= ZTH:
                    ctx["z_score"] = last_z
                    _emit_alert(f"Temperature anomaly |z|≥{ZTH} (z={last_z:.2f})", ctx)

            if last_pressure is not None and p_hpa == p_hpa:
                drop = last_pressure - p_hpa
                if drop >= ALERT_PRESS_DROP:
                    _emit_alert(f"Pressure drop ≥{ALERT_PRESS_DROP} hPa (Δ={drop:.1f})", ctx)
            last_pressure = p_hpa

            if g_mps >= ALERT_WIND_GUST:
                _emit_alert(f"Wind gust ≥{ALERT_WIND_GUST} m/s (gust={g_mps:.2f})", ctx)

            # Always log first record immediately
            if first:
                log.info("FIRST RECORD %s | provider=%s | temp=%.1f°F p=%.1f hPa hum=%.0f%% wind=%.2f m/s",
                         obj.get("ts_iso"), obj.get("provider", "n/a"),
                         temps_f[-1], press[-1], h_pct, w_mps)
                first = False
            elif n % 10 == 0:
                log.info("%s | provider=%s | temp=%.1f°F p=%.1f hPa hum=%.0f%% wind=%.2f m/s",
                         obj.get("ts_iso"), obj.get("provider", "n/a"),
                         temps_f[-1], press[-1], h_pct, w_mps)

            redraw()

    except KeyboardInterrupt:
        log.info("Consumer stopped by user.")
    finally:
        try:
            plt.ioff()
            plt.show(block=False)
        except Exception:
            pass
        if cons:
            try: cons.close()
            except: pass

if __name__ == "__main__":
    main()
