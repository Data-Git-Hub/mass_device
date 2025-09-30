# utils/utils_jsondb.py
from __future__ import annotations
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import Any, Dict

def ensure_parent(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

def append_jsonl(path: Path, record: Dict[str, Any]) -> None:
    """Append a single JSON object as one line to a .jsonl file."""
    ensure_parent(path)
    with path.open("a", encoding="utf-8") as f:
        f.write(json.dumps(record, ensure_ascii=False) + "\n")

def utcnow_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
