from __future__ import annotations
import os
from typing import Any, Dict
import yaml


def project_root() -> str:
    # feishu_contracts/common/config.py -> feishu_contracts/common -> feishu_contracts -> project root
    return os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


def load_settings(path: str) -> Dict[str, Any]:
    if not path or not os.path.exists(path):
        return {}
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            if isinstance(data, dict):
                return data  # type: ignore[return-value]
            return {}
    except Exception:
        return {}


def ensure_parent_dir(p: str) -> None:
    d = os.path.dirname(p)
    if d:
        os.makedirs(d, exist_ok=True)


def resolve_path(p: str, base: str | None = None) -> str:
    if not isinstance(p, str) or not p:
        return p
    if os.path.isabs(p):
        return p
    base_dir = base or project_root()
    return os.path.normpath(os.path.join(base_dir, p))
