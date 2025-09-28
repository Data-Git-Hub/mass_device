# utils/rolling_stats.py
from __future__ import annotations
import numpy as np
from collections import deque
from typing import Deque, Tuple

def zscores(arr: np.ndarray) -> np.ndarray:
    if arr.size < 2:
        return np.zeros_like(arr, dtype=float)
    mu = float(np.mean(arr))
    sd = float(np.std(arr, ddof=1))
    return np.zeros_like(arr) if sd == 0 else (arr - mu)/sd

def ewma(arr: np.ndarray, alpha: float=0.2) -> np.ndarray:
    if arr.size == 0: return arr
    out = np.empty_like(arr, dtype=float)
    out[0] = arr[0]
    for i in range(1, arr.size):
        out[i] = alpha*arr[i] + (1-alpha)*out[i-1]
    return out

def cusum(arr: np.ndarray, k: float=0.5, h: float=5.0) -> Tuple[np.ndarray, np.ndarray]:
    """Return (pos, neg) one-sided CUSUMs."""
    if arr.size == 0: 
        return np.array([]), np.array([])
    mu = float(np.mean(arr))
    pos = np.zeros_like(arr, dtype=float); neg = np.zeros_like(arr, dtype=float)
    for i, x in enumerate(arr):
        if i == 0: 
            pos[i] = max(0, x - (mu + k))
            neg[i] = max(0, (mu - k) - x)
        else:
            pos[i] = max(0, pos[i-1] + x - (mu + k))
            neg[i] = max(0, neg[i-1] + (mu - k) - x)
    return pos, neg

class RollingDeque:
    """Tiny helper: fixed-length rolling window."""
    def __init__(self, maxlen: int):
        self.t: Deque = deque(maxlen=maxlen)
        self.v: Deque = deque(maxlen=maxlen)
    def append(self, t, v): 
        self.t.append(t); self.v.append(v)