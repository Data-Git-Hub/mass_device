# consumers/mass_device_consumer.py
from __future__ import annotations
import os, json, time, pathlib, collections
from datetime import datetime
import numpy as np
import matplotlib.pyplot as plt

from utils.utils_env import load_env, env
from utils.utils_logger import get_logger
from utils.kafka_io import get_consumer
from utils.rolling_stats import zscores, RollingDeque

load_env()
log = get_logger("mass_device_consumer")

TOPIC = os.getenv("KAFKA_TOPIC", "weather_live")
GROUP = os.getenv("KAFKA_GROUP_ID", "mass_device")
FALLBACK_FILE = pathlib.Path("data/demo_stream.jsonl")

WINDOW = int(env("WINDOW_POINTS", "180", cast=int))
ZTH = float(env("Z_THRESHOLD", "2.0", cast=float))

def _tail_jsonl(path: pathlib.Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)
    with path.open("r", encoding="utf-8") as f:
        f.seek(0, 2)
        while True:
            line = f.readline()
            if not line:
                time.sleep(0.2); continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue

def _stream():
    cons = get_consumer(TOPIC, group_id=GROUP)
    if cons:
        log.info("Kafka=ON topic=%s group=%s", TOPIC, GROUP)
        for m in cons:
            yield json.loads(m.value.decode("utf-8")) if isinstance(m.value, (bytes, bytearray)) else m.value
    else:
        log.info("Kafka=OFF(file) tail=%s", FALLBACK_FILE)
        yield from _tail_jsonl(FALLBACK_FILE)

def main():
    times = collections.deque(maxlen=WINDOW)
    temps = collections.deque(maxlen=WINDOW)
    press = collections.deque(maxlen=WINDOW)

    plt.ion()
    fig, ax1 = plt.subplots(figsize=(9, 4.5))
    ax2 = ax1.twinx()
    line_t, = ax1.plot([], [], lw=1.7, label="Temp (°C)")
    line_p, = ax2.plot([], [], lw=1.0, alpha=0.75, label="Pressure (hPa)")
    scat = ax1.scatter([], [], s=28, marker="o", edgecolors="none", alpha=0.9)
    fig.legend(loc="upper right")
    ax1.set_xlabel("Time (HH:MM:SS)"); ax1.set_ylabel("Temp (°C)"); ax2.set_ylabel("Pressure (hPa)")
    ax1.grid(True, alpha=0.3)

    try:
        for obj in _stream():
            ts = datetime.fromisoformat(obj["ts_iso"].replace("Z","+00:00"))
            times.append(ts)
            temps.append(float(obj["temp_c"]))
            press.append(float(obj["pressure_hpa"]))

            # redraw
            x = np.arange(len(times))
            t_arr = np.array(temps, dtype=float); p_arr = np.array(press, dtype=float)
            line_t.set_data(x, t_arr); line_p.set_data(x, p_arr)
            if len(x):
                ax1.set_xlim(0, max(10, len(x)-1))
                ax1.set_ylim(t_arr.min()-1.5, t_arr.max()+1.5)
                ax2.set_ylim(p_arr.min()-1.5, p_arr.max()+1.5)
            # anomalies on temp
            z = zscores(t_arr)
            mask = np.abs(z) >= ZTH
            xs = x[mask]; ys = t_arr[mask]
            scat.set_offsets(np.c_[xs, ys])

            ax1.set_title(f"Temp & Pressure | n={len(t_arr)} | anomalies={mask.sum()} (|z|≥{ZTH})")
            fig.canvas.draw_idle(); plt.pause(0.05)

            if len(times) % 10 == 0:
                log.info("%s temp=%.2f°C p=%.1f hPa", obj["ts_iso"], obj["temp_c"], obj["pressure_hpa"])
    except KeyboardInterrupt:
        log.info("consumer stopped")
    finally:
        try: plt.ioff(); plt.show(block=False)
        except Exception: pass

if __name__ == "__main__":
    main()
