# utils/utils_logger.py
from __future__ import annotations
import logging, logging.handlers, pathlib, sys

def get_logger(name: str="mass_device", level: int=logging.INFO) -> logging.Logger:
    log_dir = pathlib.Path("logs")
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{name}.log"

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(level)

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s - %(message)s")

    fh = logging.handlers.RotatingFileHandler(log_file, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(level)

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(fmt)
    ch.setLevel(level)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.propagate = False
    return logger