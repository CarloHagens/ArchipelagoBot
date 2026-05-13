import re
from pathlib import Path

from config import VERSIONS_DIR


def get_installed_versions() -> list[str]:
    if not VERSIONS_DIR.exists():
        return []
    tags = [
        d.name for d in VERSIONS_DIR.iterdir()
        if d.is_dir() and (d / "Generate.py").exists()
    ]
    return sorted(tags, reverse=True)


def get_version_dir(tag: str) -> Path:
    return VERSIONS_DIR / tag


def parse_version(v: str) -> tuple[int, ...]:
    try:
        return tuple(int(x) for x in str(v).lstrip("v").split("."))
    except Exception:
        return (0,)


def _norm(s: str) -> str:
    return re.sub(r'[^a-z0-9]', '', s.lower())
