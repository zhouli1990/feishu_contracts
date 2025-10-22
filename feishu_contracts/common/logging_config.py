from __future__ import annotations
import logging
import os
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler


CURRENT_LOGGING_CONFIG: dict = {}
CURRENT_RUN_ID: str = ""


class _ConsoleFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        if getattr(record, "is_progress", False):
            return True
        if record.name in ("pipeline",):
            return True
        if record.levelno >= logging.WARNING:
            return True
        return False


class _InjectDefaults(logging.Filter):
    def __init__(self, run_id: str):
        super().__init__()
        self._run_id = run_id

    def filter(self, record: logging.LogRecord) -> bool:
        if not hasattr(record, "run_id"):
            setattr(record, "run_id", self._run_id)
        if not hasattr(record, "contract_code"):
            setattr(record, "contract_code", "")
        if not hasattr(record, "contract_id"):
            setattr(record, "contract_id", "")
        if not hasattr(record, "attempt"):
            setattr(record, "attempt", "")
        if not hasattr(record, "is_progress"):
            setattr(record, "is_progress", False)
        return True


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        import json
        obj = {
            "timestamp": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "run_id": getattr(record, "run_id", ""),
            "contract_code": getattr(record, "contract_code", ""),
            "contract_id": getattr(record, "contract_id", ""),
            "attempt": getattr(record, "attempt", ""),
            "is_progress": getattr(record, "is_progress", False),
        }
        return json.dumps(obj, ensure_ascii=False)


def _level_from(cfg: dict, key: str, default: str = "INFO") -> int:
    val = str((cfg or {}).get(key, default)).upper()
    return getattr(logging, val, logging.INFO)


def _ensure_dir(p: str) -> None:
    d = p if p.endswith(os.sep) else os.path.dirname(p) or p
    if d:
        os.makedirs(d, exist_ok=True)


def init_logging(logging_cfg: dict | None, proj_root: str) -> str:
    cfg = logging_cfg or {}

    level = _level_from(cfg, "level", "INFO")
    console = bool(cfg.get("console", True))
    console_level = _level_from(cfg, "console_level", "INFO")
    use_json = bool(cfg.get("json", False))

    naming = cfg.get("naming", {}) or {}
    mode = str(naming.get("mode") or "timestamp").lower()
    pattern = naming.get("pattern") or "{stage}_{run_id}.log"
    run_id_fmt = naming.get("run_id_format") or "%Y%m%d_%H%M%S"
    run_id = datetime.now().strftime(run_id_fmt)

    rotation = cfg.get("rotation", {}) or {}
    rot_mode = str(rotation.get("mode") or "none").lower()
    when = rotation.get("when") or "D"
    interval = int(rotation.get("interval") or 1)
    backup_count = int(rotation.get("backup_count") or 0)

    files = cfg.get("files", {}) or {}

    global CURRENT_LOGGING_CONFIG, CURRENT_RUN_ID
    CURRENT_LOGGING_CONFIG = dict(cfg)
    CURRENT_LOGGING_CONFIG["progress_interval"] = int(cfg.get("progress_interval") or 100)
    CURRENT_LOGGING_CONFIG["run_id"] = run_id
    CURRENT_RUN_ID = run_id

    logging.captureWarnings(True)
    root = logging.getLogger()
    root.setLevel(level)
    for h in list(root.handlers):
        root.removeHandler(h)

    stages = ["pipeline", "fetch", "convert", "transform"]

    if use_json:
        file_fmt: logging.Formatter = _JsonFormatter()
    else:
        file_fmt = logging.Formatter("%(asctime)s [%(levelname)s] [%(name)s] run=%(run_id)s cc=%(contract_code)s cid=%(contract_id)s %(message)s")

    console_handler: logging.Handler | None = None
    if console:
        console_handler = logging.StreamHandler()
        console_handler.setLevel(console_level)
        console_handler.setFormatter(logging.Formatter("%(asctime)s [%(name)s] cc=%(contract_code)s cid=%(contract_id)s %(message)s", datefmt="%H:%M:%S"))
        console_handler.addFilter(_InjectDefaults(run_id))
        console_handler.addFilter(_ConsoleFilter())

    for stage in stages:
        logger = logging.getLogger(stage)
        logger.setLevel(level)
        logger.propagate = False
        for h in list(logger.handlers):
            logger.removeHandler(h)

        base = files.get(stage) or os.path.join(proj_root, "logs")
        if not base.endswith(os.sep):
            if os.path.splitext(base)[1]:
                # a file path was provided; keep directory of it
                base = os.path.dirname(base) or base
            else:
                base = base + os.sep
        _ensure_dir(base)
        file_path = os.path.join(base, pattern.format(stage=stage, run_id=run_id))

        if rot_mode == "time":
            fh: logging.Handler = TimedRotatingFileHandler(file_path, when=when, interval=interval, backupCount=backup_count, encoding="utf-8")
        else:
            fh = logging.FileHandler(file_path, encoding="utf-8")
        fh.setLevel(level)
        fh.setFormatter(file_fmt)
        fh.addFilter(_InjectDefaults(run_id))
        logger.addHandler(fh)

        if console_handler is not None:
            logger.addHandler(console_handler)

    return run_id


def get_progress_interval(default: int = 100) -> int:
    try:
        v = int(CURRENT_LOGGING_CONFIG.get("progress_interval", default))
        return v if v > 0 else default
    except Exception:
        return default


def get_run_id() -> str:
    return CURRENT_RUN_ID or ""
