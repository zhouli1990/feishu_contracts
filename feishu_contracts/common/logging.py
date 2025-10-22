from __future__ import annotations
import logging
import os
from logging.handlers import RotatingFileHandler


def setup_logger(log_path: str, level: int = logging.INFO, max_bytes: int = 2_000_000, backup_count: int = 5) -> None:
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    logger = logging.getLogger()
    if logger.handlers:
        return
    logger.setLevel(level)
    fh = RotatingFileHandler(log_path, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8")
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
