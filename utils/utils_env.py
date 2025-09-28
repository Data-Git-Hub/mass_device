# utils/utils_env.py
from __future__ import annotations
import os
from typing import Callable, Optional

def load_env() -> None:
    """Load .env if python-dotenv is available; otherwise no-op."""
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except Exception:
        pass

def _cast_bool(val: str) -> bool:
    return val.strip().lower() in {"1","true","t","yes","y","on"}

def env(name: str, default: Optional[str]=None, cast: Optional[Callable]=None):
    v = os.getenv(name, default)
    if v is None:
        return None
    if cast is None:
        return v
    if cast is bool:
        return _cast_bool(v)
    try:
        return cast(v)
    except Exception:
        return default

def require_env(name: str) -> str:
    v = os.getenv(name)
    if not v:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return v