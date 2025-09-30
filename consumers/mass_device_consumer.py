# consumers/mass_device_consumer.py
from __future__ import annotations
import os, json, time, pathlib, collections, sys
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt

# --- ensure project root import path ---
from pathlib import Path as _Path
_root = _Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from meteorological_theories.demo_calc import (
    zscores, ewma, cusum, dew_point_c, heat_index_c, wind_chill_c
)

def _c_to_f(c: float) -> float:
    return (c * 9.0/5.0) + 32.0

# --- Optional utils ---
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
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
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
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")  # if not set
INFILE = pathlib.Path("data/demo_stream.jsonl")

WINDOW = int(os.getenv("WINDOW_POINTS", "180"))
ZTH = float(os.getenv("Z_THRESHOLD", "2.0"))

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
            consumer_timeout_ms=1000,
        )
    except Exception as e:
        log.warning("Kafka not available (%s). Falling back to file tail at %s", e, INFILE)
        return None

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

def main():
    cons = _maybe_kafka_consumer()
    log.info("Consumer start | Kafka=%s | topic=%s | window=%d | zth=%.2f",
             "ON" if cons else "OFF(file)", TOPIC, WINDOW, ZTH)

    times = collections.deque(maxlen=WINDOW)
    temps_f = collections.deque(maxlen=WINDOW)  # °F
    press = collections.deque(maxlen=WINDOW)    # hPa

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

        # z-score on °C (scale-invariant, keeps semantics clean)
        if t_arr_f.size:
            t_arr_c = (t_arr_f - 32.0) * 5.0/9.0
            z = zscores(t_arr_c)
            mask = np.abs(z) >= ZTH
            scat.set_offsets(np.c_[xs[mask], t_arr_f[mask]])
        else:
            mask = np.zeros(0, dtype=bool)

        ax1.set_title(f"Temp & Pressure (|z|≥{ZTH})", pad=10)

        # metrics box updates in the main loop (we pass computed dp/wc there)

        fig.canvas.draw_idle()
        plt.pause(0.05)

    def stream():
        if cons:
            for msg in cons:
                yield msg.value
        else:
            for obj in _tail_jsonl(INFILE):
                yield obj

    try:
        for obj in stream():
            # Parse timestamp
            try:
                ts = datetime.strptime(obj["ts_iso"], "%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                ts = datetime.fromisoformat(obj["ts_iso"].replace("Z", "+00:00")).replace(tzinfo=None)
            times.append(ts)

            # Append series values
            t_c = float(obj["temp_c"])
            temps_f.append(_c_to_f(t_c))
            press.append(float(obj.get("pressure_hpa", np.nan)))

            # Use real humidity/wind if present, else safe defaults
            hum = float(obj.get("humidity_pct", 60.0))
            wind = float(obj.get("wind_mps", 3.0))

            # Derived metrics in °F for display
            dp_f = _c_to_f(dew_point_c(t_c, hum))
            wc_f = _c_to_f(wind_chill_c(t_c, wind))

            # Update metrics box text
            n = len(temps_f)
            # recompute current anomaly count for display consistency
            t_arr_c_now = (np.array(temps_f, dtype=float) - 32.0) * 5.0/9.0
            z_now = zscores(t_arr_c_now) if n else np.array([])
            anomalies = int((np.abs(z_now) >= ZTH).sum()) if z_now.size else 0
            ax1._metrics_box.set_text(
                f"n={n} | anomalies={anomalies}  •  dew point≈{dp_f:.1f}°F  •  wind chill≈{wc_f:.1f}°F"
            )

            if n % 10 == 0:
                log.info(
                    "%s | provider=%s | temp=%.1f°F p=%.1f hPa hum=%.0f%% wind=%.2f m/s",
                    obj.get("ts_iso"), obj.get("provider", "n/a"),
                    temps_f[-1], press[-1], hum, wind
                )

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
            try:
                cons.close()
            except Exception:
                pass

if __name__ == "__main__":
    main()
