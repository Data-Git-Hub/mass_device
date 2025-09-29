# consumers/demo_mass_device_consumer.py
from __future__ import annotations
import os, json, time, pathlib, collections
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt

# >>> added: import formulas from the new package
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

# ---- Optional utils (fallback) ----
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

# >>> removed: local _zscores (now using imported zscores)
# def _zscores(arr: np.ndarray):
#     ...

def main():
    cons = _maybe_kafka_consumer()
    log.info("Demo consumer start | Kafka=%s | topic=%s | window=%d | zth=%.2f",
             "ON" if cons else "OFF(file)", TOPIC, WINDOW, ZTH)

    times = collections.deque(maxlen=WINDOW)
    temps = collections.deque(maxlen=WINDOW)
    press = collections.deque(maxlen=WINDOW)

    plt.ion()
    fig, ax1 = plt.subplots(figsize=(9, 4.5))
    ax2 = ax1.twinx()
    fig.suptitle("M.A.S.S. Device — Live Temp & Pressure with Anomalies", y=0.98)
    fig.subplots_adjust(top=0.88)
    line_t, = ax1.plot([], [], lw=1.7, label="Temp (°F)")    # was °C
    line_p, = ax2.plot([], [], lw=1.0, alpha=0.75, label="Pressure (hPa)")
    scat = ax1.scatter([], [], s=28, marker="o", edgecolors="none", alpha=0.9)
    ax1.set_xlabel("Time (HH:MM:SS)")
    ax1.set_ylabel("Temp (°F)")                              # was °C
    ax2.set_ylabel("Pressure (hPa)")
    ax1.grid(True, alpha=0.3)
    fig.legend(loc="upper right")

    def redraw():
        if not times:
            return
        xlabels = [t.strftime("%H:%M:%S") for t in times]
        t_arr = np.array(temps, dtype=float)
        p_arr = np.array(press, dtype=float)

        # update lines
        line_t.set_data(range(len(xlabels)), t_arr)
        line_p.set_data(range(len(xlabels)), p_arr)

        # autoscale
        ax1.set_xlim(0, max(10, len(xlabels)-1))
        ax1.set_ylim(t_arr.min()-1.5, t_arr.max()+1.5)
        ax2.set_ylim(p_arr.min()-1.5, p_arr.max()+1.5)

        # anomalies (z-score on temp) using imported zscores
        z = zscores(t_arr)  # >>> changed
        mask = np.abs(z) >= ZTH
        xs = np.where(mask)[0]
        ys = t_arr[mask]
        scat.set_offsets(np.c_[xs, ys])

        # >>> optional: compute a derived metric and show it subtly
        try:
            # pick the latest sample to annotate
            latest_t = float(t_arr[-1])
            latest_p = float(p_arr[-1])
            # if humidity/wind available in stream, you can pass real values here
            # demo: assume 60% RH and 3 m/s wind for an illustrative annotation
            dp_f = _c_to_f(dew_point_c(latest_t, 60.0))
            wc_f = _c_to_f(wind_chill_c(latest_t, 3.0))
            ax1.set_title(
                f"Temp & Pressure | n={len(t_arr)} | anomalies={mask.sum()} (|z|≥{ZTH}) "
                f"| dew point≈{dp_f:.1f}°F, wind chill≈{wc_f:.1f}°F",
                pad=14
            )

        except Exception:
            ax1.set_title(f"Temp & Pressure | n={len(t_arr)} | anomalies={mask.sum()} (|z|≥{ZTH})")

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
            try:
                ts = datetime.strptime(obj["ts_iso"], "%Y-%m-%dT%H:%M:%SZ")
            except Exception:
                # tolerate slightly different formats
                ts = datetime.fromisoformat(obj["ts_iso"].replace("Z", "+00:00")).replace(tzinfo=None)
            times.append(ts)
            temps.append(_c_to_f(float(obj["temp_c"])))     # convert on ingest
            press.append(float(obj["pressure_hpa"]))
            if len(times) % 10 == 0:
                log.info("%s | temp=%.2f°C p=%.1f hPa", obj["ts_iso"], obj["temp_c"], obj["pressure_hpa"])
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
            try: cons.close()
            except: pass

if __name__ == "__main__":
    main()