# meteorological_theories/demo_calc.py
from __future__ import annotations
import numpy as np

# ---------- Streaming stats used by the consumer ----------

def zscores(arr: np.ndarray) -> np.ndarray:
    """Sample z-scores (ddof=1). Safe for small/constant arrays."""
    arr = np.asarray(arr, dtype=float)
    n = arr.size
    if n < 2:
        return np.zeros_like(arr)
    mu = float(np.mean(arr))
    sd = float(np.std(arr, ddof=1))
    return np.zeros_like(arr) if sd == 0 else (arr - mu) / sd

def ewma(arr: np.ndarray, alpha: float = 0.2) -> np.ndarray:
    """Exponentially Weighted Moving Average."""
    arr = np.asarray(arr, dtype=float)
    if arr.size == 0:
        return arr
    out = np.empty_like(arr, dtype=float)
    out[0] = arr[0]
    for i in range(1, arr.size):
        out[i] = alpha * arr[i] + (1 - alpha) * out[i - 1]
    return out

def cusum(arr: np.ndarray, k: float = 0.5, h: float = 5.0) -> tuple[np.ndarray, np.ndarray]:
    """
    One-sided CUSUM (pos, neg). 'k' is reference value (half the shift to detect),
    'h' is decision interval (used for thresholding).
    """
    arr = np.asarray(arr, dtype=float)
    n = arr.size
    if n == 0:
        return np.array([]), np.array([])
    mu = float(np.mean(arr))
    pos = np.zeros(n, dtype=float)
    neg = np.zeros(n, dtype=float)
    for i in range(1, n):
        pos[i] = max(0.0, pos[i - 1] + (arr[i] - (mu + k)))
        neg[i] = max(0.0, neg[i - 1] + ((mu - k) - arr[i]))
    return pos, neg

# ---------- Handy meteorological formulas (optional) ----------

def dew_point_c(temp_c: float, rh_pct: float) -> float:
    """
    Magnus formula approximation for dew point (°C).
    temp_c: air temperature (°C), rh_pct: relative humidity in percent.
    """
    a, b = 17.62, 243.12  # constants for water over liquid (°C)
    rh = max(1e-6, min(100.0, rh_pct)) / 100.0
    gamma = (a * temp_c) / (b + temp_c) + np.log(rh)
    return (b * gamma) / (a - gamma)

def heat_index_c(temp_c: float, rh_pct: float) -> float:
    """
    Simple heat index approximation (°C) for moderate ranges.
    For rigorous ranges, consider NWS/Steadman formulas.
    """
    t = temp_c
    r = rh_pct
    # crude approximation blending T and humidity effect
    return t + 0.33 * (r/100.0 * 6.105 * np.exp(17.27*t/(237.7+t)) - 10.0) - 0.70

def wind_chill_c(temp_c: float, wind_mps: float) -> float:
    """
    Wind chill (°C) using NWS formula adapted to m/s (converts to km/h).
    Valid roughly for T <= 10°C and wind >= 1.3 m/s.
    """
    v_kmh = max(0.0, wind_mps) * 3.6
    return 13.12 + 0.6215*temp_c - 11.37*(v_kmh**0.16) + 0.3965*temp_c*(v_kmh**0.16)
