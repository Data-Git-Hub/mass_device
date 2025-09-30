# consumers/mass_device_consumer.py
from __future__ import annotations
import os, json, time, pathlib, collections, sys, threading, queue
from datetime import datetime, timedelta
from typing import Dict, Any, Optional
import numpy as np

import matplotlib
try:
    if os.name == "nt":
        matplotlib.use("TkAgg")
except Exception:
    pass
import matplotlib.pyplot as plt
import matplotlib.animation as animation
from matplotlib.dates import DateFormatter, AutoDateLocator, date2num

from pathlib import Path as _Path
_root = _Path(__file__).resolve().parents[1]
if str(_root) not in sys.path:
    sys.path.insert(0, str(_root))

from meteorological_theories.demo_calc import zscores, dew_point_c, wind_chill_c
from utils.utils_jsondb import append_jsonl, ensure_parent

try:
    from consumers.mass_device_alerts import maybe_send_alert
except Exception:
    def maybe_send_alert(_alert: Dict[str, Any]) -> None: pass

def _c_to_f(c: float) -> float:
    return (c * 9.0/5.0) + 32.0

logger = None
def _get_logger():
    global logger
    if logger: return logger
    try:
        from utils.utils_logger import get_logger
        return get_logger("mass_device_consumer")
    except Exception:
        import logging
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        return logging.getLogger("mass_device_consumer")

def _load_env():
    try:
        from utils.utils_env import load_env; load_env()
    except Exception:
        try:
            from dotenv import load_dotenv; load_dotenv()
        except Exception: pass

_load_env()
log = _get_logger()

TOPIC = os.getenv("KAFKA_TOPIC", "weather_live")
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS")
INFILE = pathlib.Path("data/demo_stream.jsonl")

WINDOW = int(os.getenv("WINDOW_POINTS", "180"))
ZTH = float(os.getenv("Z_THRESHOLD", "2.0"))
ALERT_PRESS_DROP = float(os.getenv("ALERT_PRESSURE_DROP_HPA", "3"))
ALERT_WIND_GUST = float(os.getenv("ALERT_WIND_GUST_MPS", "15"))

JSON_DB_DIR = pathlib.Path("data/db")
CONSUMER_DB = JSON_DB_DIR / "consumer_stream.jsonl"
ALERTS_DB   = JSON_DB_DIR / "alerts.jsonl"
ensure_parent(CONSUMER_DB); ensure_parent(ALERTS_DB)

def _maybe_kafka_consumer():
    if not BOOTSTRAP: return None
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
                    for m in msgs: yield m.value
            else: time.sleep(0.05)
        except Exception as e:
            log.error("Kafka poll error: %s", e); time.sleep(1.0)

def _tail_jsonl(path: pathlib.Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.touch(exist_ok=True)
    with path.open("r", encoding="utf-8") as f:
        f.seek(0, 2)
        while True:
            line = f.readline()
            if not line: time.sleep(0.05); continue
            try: yield json.loads(line)
            except json.JSONDecodeError: continue

def _emit_alert(reason: str, ctx: Dict[str, Any]) -> None:
    alert = {
        "ts_iso": ctx.get("ts_iso"), "reason": reason, "provider": ctx.get("provider"),
        "lat": ctx.get("lat"), "lon": ctx.get("lon"),
        "temp_c": ctx.get("temp_c"), "pressure_hpa": ctx.get("pressure_hpa"),
        "humidity_pct": ctx.get("humidity_pct"), "wind_mps": ctx.get("wind_mps"),
        "gust_mps": ctx.get("gust_mps"), "z_score": ctx.get("z_score"),
        "thresholds": {"ZTH": ZTH, "ALERT_PRESSURE_DROP_HPA": ALERT_PRESS_DROP,
                       "ALERT_WIND_GUST_MPS": ALERT_WIND_GUST}
    }
    append_jsonl(ALERTS_DB, alert)
    log.warning("ALERT: %s | t=%.1f°C p=%.1f hPa wind=%.2f m/s gust=%.2f m/s",
                reason, ctx.get("temp_c", float("nan")), ctx.get("pressure_hpa", float("nan")),
                ctx.get("wind_mps", float("nan")), ctx.get("gust_mps", float("nan")))
    try: maybe_send_alert(alert)
    except Exception as e: log.error("Alert send failed: %s", e)

class ConsumerApp:
    def __init__(self):
        self.cons = _maybe_kafka_consumer()
        log.info("Consumer start | Kafka=%s | topic=%s | window=%d | zth=%.2f",
                 "ON" if self.cons else "OFF(file)", TOPIC, WINDOW, ZTH)

        self.times = collections.deque(maxlen=WINDOW)
        self.temps_f = collections.deque(maxlen=WINDOW)
        self.press = collections.deque(maxlen=WINDOW)
        self.last_pressure: Optional[float] = None
        self.first_logged = False

        self.q: queue.Queue[Dict[str, Any]] = queue.Queue(maxsize=1000)
        self.running = True

        self.fig, self.ax1 = plt.subplots(figsize=(9, 4.8))
        self.ax2 = self.ax1.twinx()
        self.fig.suptitle("M.A.S.S. Device — Live Temp & Pressure with Anomalies", y=0.985)
        self.fig.tight_layout(rect=(0.06, 0.12, 0.98, 0.90))

        (self.line_t,) = self.ax1.plot([], [], lw=1.7, label="Temp (°F)")
        (self.line_p,) = self.ax2.plot([], [], lw=1.0, alpha=0.75, label="Pressure (hPa)")
        self.scat = self.ax1.scatter([], [], s=28, marker="o", edgecolors="none", alpha=0.9)

        self.ax1.set_xlabel("Time (HH:MM:SS)", labelpad=8)
        self.ax1.set_ylabel("Temp (°F)"); self.ax2.set_ylabel("Pressure (hPa)")
        self.ax1.grid(True, alpha=0.3)

        h1,l1 = self.ax1.get_legend_handles_labels()
        h2,l2 = self.ax2.get_legend_handles_labels()
        self.ax1.legend(h1+h2, l1+l2, loc="lower right", bbox_to_anchor=(0.985,0.04),
                        borderaxespad=0.6, framealpha=0.9)

        self.ax1._metrics_box = self.ax1.text(
            0.01,0.98,"", transform=self.ax1.transAxes,
            ha="left",va="top",fontsize=9,
            bbox=dict(boxstyle="round,pad=0.3",fc="white",ec="0.8",alpha=0.75)
        )

        self.ax1.xaxis.set_major_locator(AutoDateLocator())
        self.ax1.xaxis.set_major_formatter(DateFormatter("%H:%M:%S"))

        self.thread = threading.Thread(target=self._ingest_loop, daemon=True); self.thread.start()
        self.ani = animation.FuncAnimation(self.fig, self._on_timer, interval=200,
                                           blit=False, cache_frame_data=False)

    def _source_iter(self): return _kafka_stream(self.cons) if self.cons else _tail_jsonl(INFILE)

    def _ingest_loop(self):
        try:
            for obj in self._source_iter():
                append_jsonl(CONSUMER_DB, obj)
                self.q.put(obj, timeout=1.0)
        except Exception as e: log.error("Ingest thread error: %s", e)
        finally: self.running=False

    def _process_one(self, obj: Dict[str, Any]):
        try: ts=datetime.strptime(obj["ts_iso"], "%Y-%m-%dT%H:%M:%SZ")
        except Exception: ts=datetime.fromisoformat(obj["ts_iso"].replace("Z","+00:00")).replace(tzinfo=None)
        self.times.append(ts)

        t_c=float(obj.get("temp_c",np.nan)); p_hpa=float(obj.get("pressure_hpa",np.nan))
        h_pct=float(obj.get("humidity_pct",60.0)); w_mps=float(obj.get("wind_mps",3.0))
        g_mps=float(obj.get("gust_mps",0.0))
        self.temps_f.append(_c_to_f(t_c)); self.press.append(p_hpa)

        dp_f=_c_to_f(dew_point_c(t_c,h_pct)); wc_f=_c_to_f(wind_chill_c(t_c,w_mps))
        n=len(self.temps_f)
        t_arr_c_now=(np.array(self.temps_f)-32.0)*5.0/9.0
        z_now=zscores(t_arr_c_now) if n else np.array([])
        anomalies=int((np.abs(z_now)>=ZTH).sum()) if z_now.size else 0
        self.ax1._metrics_box.set_text(f"n={n} | anomalies={anomalies} • dew≈{dp_f:.1f}°F • wind chill≈{wc_f:.1f}°F")

        ctx={"ts_iso":obj.get("ts_iso"),"provider":obj.get("provider"),
             "lat":obj.get("lat"),"lon":obj.get("lon"),"temp_c":t_c,"pressure_hpa":p_hpa,
             "humidity_pct":h_pct,"wind_mps":w_mps,"gust_mps":g_mps,"z_score":None}
        if z_now.size:
            last_z=float(z_now[-1])
            if abs(last_z)>=ZTH:
                ctx["z_score"]=last_z; _emit_alert(f"Temp anomaly |z|≥{ZTH} (z={last_z:.2f})",ctx)
        if self.last_pressure is not None and p_hpa==p_hpa:
            drop=self.last_pressure-p_hpa
            if drop>=ALERT_PRESS_DROP: _emit_alert(f"Pressure drop ≥{ALERT_PRESS_DROP} hPa (Δ={drop:.1f})",ctx)
        self.last_pressure=p_hpa
        if g_mps>=ALERT_WIND_GUST: _emit_alert(f"Wind gust ≥{ALERT_WIND_GUST} m/s (gust={g_mps:.2f})",ctx)

        # log first record immediately
        if not self.first_logged:
            log.info("FIRST %s | provider=%s | temp=%.1f°F p=%.1f hPa hum=%.0f%% wind=%.2f m/s",
                     obj.get("ts_iso"),obj.get("provider","n/a"),
                     self.temps_f[-1],self.press[-1],h_pct,w_mps)
            self.first_logged=True
        elif n%10==0:
            log.info("%s | provider=%s | temp=%.1f°F p=%.1f hPa hum=%.0f%% wind=%.2f m/s",
                     obj.get("ts_iso"),obj.get("provider","n/a"),
                     self.temps_f[-1],self.press[-1],h_pct,w_mps)

    def _redraw(self):
        if not self.times: return
        xs=date2num(list(self.times)); t_arr=np.array(self.temps_f); p_arr=np.array(self.press)
        self.line_t.set_data(xs,t_arr); self.line_p.set_data(xs,p_arr)

        if len(xs)==1:  # pad x-axis if only one point
            pad=60/86400
            self.ax1.set_xlim(xs[0]-pad,xs[0]+pad)
        else:
            self.ax1.set_xlim(xs.min(),xs.max())
        if t_arr.size: self.ax1.set_ylim(t_arr.min()-2,t_arr.max()+2)
        if p_arr.size: self.ax2.set_ylim(p_arr.min()-1.5,p_arr.max()+1.5)

        t_arr_c=(t_arr-32.0)*5.0/9.0
        z=zscores(t_arr_c) if t_arr.size else np.array([])
        mask=np.abs(z)>=ZTH if z.size else np.zeros_like(t_arr,bool)
        self.scat.set_offsets(np.c_[xs[mask],t_arr[mask]])
        self.ax1.set_title(f"Temp & Pressure (|z|≥{ZTH})",pad=10)

    def _on_timer(self,_): 
        processed=0
        while processed<200:
            try: obj=self.q.get_nowait()
            except queue.Empty: break
            self._process_one(obj); processed+=1
        self._redraw(); return []

    def run(self):
        try: plt.show(block=True)
        finally:
            self.running=False
            if self.cons:
                try:self.cons.close()
                except:pass

def main(): ConsumerApp().run()
if __name__=="__main__": main()
