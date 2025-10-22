from __future__ import annotations
import os
from .config import project_root


def ensure_parent_dir(p: str) -> None:
    d = os.path.dirname(p)
    if d:
        os.makedirs(d, exist_ok=True)


def to_abs(p: str) -> str:
    if not isinstance(p, str) or not p:
        return p
    if os.path.isabs(p):
        return p
    return os.path.normpath(os.path.join(project_root(), p))
