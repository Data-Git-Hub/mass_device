# consumers/demo_mass_device_consumer.py
from __future__ import annotations
import os, json, time, pathlib, collections, sys
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt

# --- ensure project root is importable (safe if already on sys.path) ---
from pathlib import Path as _Path
_root = _Path(__file__).resolve().parents[1]  # mass_device/
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

# --- formulas from your meteorological_theories package ---
from meteorological_theories.demo_calc import (
    zscores,        # replaces local _zscores
    ewma,           # available for later use
    cusum,          # available for later use
    dew_point_c,    # optional derived metric
    heat_index_c,   # optional derived metric
    wind_chill_c    # optional derived metric
)

# temperature conversion
def _c_to_f(c: float) -> float:
    return (c * 9.0/5.0) + 32.0

# ---- Optional utils (fallback logger + .env loader) ----
logger = None
def _get_logger():
    global logger
    if logger:
        return logger
    try:
        from utils.utils_logger import get_logger
        logger = get_logger("demo_consumer")
    except Exception:
        import logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        logger = logging.getLogger("demo_consumer")
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

TOPIC = os.getenv("KAFKA_TOPIC", "weather_demo")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")  # if not set → file tail
INFILE = pathlib.Path("data/demo_stream.jsonl")

WINDOW = int(os.getenv("DEMO_WINDOW_SIZE", "120"))  # points
ZTH = float(os.getenv("DEMO_Z_THRESHOLD", "2.0"))

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
            group_id=os.getenv("KAFKA_GROUP_ID", "mass_device_demo"),
            consumer_timeout_ms=1000,
        )
    except Exception as e:
        log.warning("Kafka not available (%s). Falling back to file tail at %s", e, INFILE)
        return None

def _tail_jsonl(path: pathlib.Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)
    with path.open("r", encoding="utf-8") as f:
        f.seek(0, 2)  # seek to end
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
    log.info("Demo consumer start | Kafka=%s | topic=%s | window=%d | zth=%.2f",
             "ON" if cons else "OFF(file)", TOPIC, WINDOW, ZTH)

    times = collections.deque(maxlen=WINDOW)
    temps = collections.deque(maxlen=WINDOW)   # store °F
    press = collections.deque(maxlen=WINDOW)   # store hPa

    plt.ion()
    fig, ax1 = plt.subplots(figsize=(9, 4.5))
    ax2 = ax1.twinx()

    # --- Title space & layout so nothing is squished ---
    fig.suptitle("M.A.S.S. Device — Live Temp & Pressure with Anomalies", y=0.985)
    fig.tight_layout(rect=(0, 0, 1, 0.93))

    # Lines & scatter
    line_t, = ax1.plot([], [], lw=1.7, label="Temp (°F)")
    line_p, = ax2.plot([], [], lw=1.0, alpha=0.75, label="Pressure (hPa)")
    scat = ax1.scatter([], [], s=28, marker="o", edgecolors="none", alpha=0.9)

    # Axes labels/grid
    ax1.set_xlabel("Time (HH:MM:SS)")
    ax1.set_ylabel("Temp (°F)")
    ax2.set_ylabel("Pressure (hPa)")
    ax1.grid(True, alpha=0.3)

    # Single legend INSIDE the axes (lower-right)
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax2.get_legend_handles_labels()
    legend = ax1.legend(
        handles1 + handles2,
        labels1 + labels2,
        loc="lower right",
        bbox_to_anchor=(0.985, 0.02),  # near bottom-right inside axes
        borderaxespad=0.6,
        framealpha=0.9
    )

    # Create a metrics textbox once; update its text on each redraw
    ax1._metrics_box = ax1.text(
        0.01, 0.98, "",
        transform=ax1.transAxes,
        ha="left", va="top",
        fontsize=9,
        bbox=dict(boxstyle="round,pad=0.3", fc="white", ec="0.8", alpha=0.75)
    )

    def redraw():
        if not times:
            return

        xlabels = [t.strftime("%H:%M:%S") for t in times]
        t_arr = np.array(temps, dtype=float)   # °F
        p_arr = np.array(press, dtype=float)   # hPa

        # update lines
        xs = np.arange(len(xlabels))
        line_t.set_data(xs, t_arr)
        line_p.set_data(xs, p_arr)

        # autoscale
        ax1.set_xlim(0, max(10, len(xs) - 1))
        if t_arr.size:
            ax1.set_ylim(t_arr.min() - 2.0, t_arr.max() + 2.0)
        if p_arr.size:
            ax2.set_ylim(p_arr.min() - 1.5, p_arr.max() + 1.5)

        # anomalies (z-score on temperature) — scale-invariant
        # for correctness use °C in z-score; convert back from °F:
        if t_arr.size:
            t_arr_c = (t_arr - 32.0) * 5.0/9.0
            z = zscores(t_arr_c)
            mask = np.abs(z) >= ZTH
            scat.set_offsets(np.c_[xs[mask], t_arr[mask]])
        else:
            mask = np.zeros(0, dtype=bool)

        # Short, clear axes title (no long strings that wrap)
        ax1.set_title(f"Temp & Pressure (|z|≥{ZTH})", pad=10)

        # Metrics box (compact summary)
        try:
            if t_arr.size:
                latest_t_f = float(t_arr[-1])
                latest_t_c = (latest_t_f - 32.0) * 5.0/9.0
                dp_f = _c_to_f(dew_point_c(latest_t_c, 60.0))   # replace 60.0 with obj["humidity_pct"] when available
                wc_f = _c_to_f(wind_chill_c(latest_t_c, 3.0))   # replace 3.0 with obj["wind_mps"] when available
                ax1._metrics_box.set_text(
                    f"n={len(t_arr)} | anomalies={int(mask.sum())}  •  "
                    f"dew point≈{dp_f:.1f}°F  •  wind chill≈{wc_f:.1f}°F"
                )
            else:
                ax1._metrics_box.set_text("n=0 | anomalies=0")
        except Exception:
            ax1._metrics_box.set_text(f"n={len(t_arr)} | anomalies={int(mask.sum())}")

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
            # tolerant timestamp parsing
            try:
                ts = datetime.strptime(obj["ts_iso"], "%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                ts = datetime.fromisoformat(obj["ts_iso"].replace("Z", "+00:00")).replace(tzinfo=None)

            times.append(ts)
            temps.append(_c_to_f(float(obj["temp_c"])))      # store °F
            press.append(float(obj["pressure_hpa"]))          # store hPa

            if len(times) % 10 == 0:
                log.info("%s | temp=%.1f°F p=%.1f hPa", obj["ts_iso"], temps[-1], press[-1])

            redraw()

    except KeyboardInterrupt:
        log.info("Demo consumer stopped by user.")
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
