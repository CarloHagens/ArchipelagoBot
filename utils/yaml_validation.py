import functools
import io
import json
import re
import zipfile

import requests
import yaml

from config import ARCHIPELAGO_BASE
from utils.files import apworld_stem
from utils.versions import _norm, parse_version


HTML_TAG_RE    = re.compile(r'<[^>]+>')
CHECK_ROW_RE   = re.compile(r'<tr[^>]*>.*?<td[^>]*>(.*?)</td>.*?<td[^>]*>(.*?)</td>.*?</tr>', re.DOTALL)
WORLD_GAME_RE  = re.compile(r'game\s*(?::\s*\w+\s*)?=\s*["\']([^"\']+)["\']')


def check_yamls_on_server(yaml_files: dict[str, bytes]) -> dict[str, str]:
    files = [
        ("file", (name, data, "application/x-yaml"))
        for name, data in yaml_files.items()
    ]
    resp = requests.post(
        f"{ARCHIPELAGO_BASE}/check",
        files=files,
        timeout=60,
        headers={"User-Agent": "ArchipelagoDiscordBot/1.0"},
    )
    resp.raise_for_status()

    results = {}
    for match in CHECK_ROW_RE.finditer(resp.text):
        filename = HTML_TAG_RE.sub("", match.group(1)).strip()
        result   = HTML_TAG_RE.sub("", match.group(2)).strip()
        if filename.endswith((".yaml", ".yml")):
            results[filename] = result
    return results


@functools.lru_cache(maxsize=8)
def get_builtin_game_names(version_dir) -> frozenset[str]:
    games: set[str] = set()
    worlds_dir = version_dir / "worlds"
    if worlds_dir.exists():
        for py_file in worlds_dir.glob("*/*.py"):
            try:
                for match in WORLD_GAME_RE.finditer(
                    py_file.read_text(encoding="utf-8", errors="replace"),
                ):
                    games.add(match.group(1))
            except Exception:
                pass
    return frozenset(games)


def get_yaml_game(yaml_bytes: bytes) -> str | None:
    try:
        data = yaml.safe_load(yaml_bytes)
        if not isinstance(data, dict):
            return None
        game = data.get("game")
        if isinstance(game, dict):
            game = next(iter(game))
        return str(game).strip() if game else None
    except Exception:
        return None


def get_yaml_requires(yaml_bytes: bytes) -> tuple[str | None, dict[str, str]]:
    try:
        data = yaml.safe_load(yaml_bytes)
        if not isinstance(data, dict):
            return None, {}
        req = data.get("requires")
        if not isinstance(req, dict):
            return None, {}
        ap_version = req.get("version")
        ap_version = str(ap_version).strip() if ap_version is not None else None
        game_reqs  = req.get("game") or {}
        if not isinstance(game_reqs, dict):
            game_reqs = {}
        return ap_version, {str(k): str(v) for k, v in game_reqs.items()}
    except Exception:
        return None, {}


def get_apworld_info(apworld_bytes: bytes) -> dict:
    info: dict = {"game": None, "world_version": None, "minimum_ap_version": None}
    try:
        with zipfile.ZipFile(io.BytesIO(apworld_bytes)) as zf:
            for entry in zf.namelist():
                if entry.endswith("archipelago.json"):
                    try:
                        manifest = json.loads(zf.read(entry).decode("utf-8", errors="replace"))
                        if isinstance(manifest, dict):
                            if manifest.get("game"):
                                info["game"] = str(manifest["game"])
                            if manifest.get("world_version"):
                                info["world_version"] = str(manifest["world_version"])
                            if manifest.get("minimum_ap_version"):
                                info["minimum_ap_version"] = str(manifest["minimum_ap_version"])
                            return info
                    except Exception:
                        pass

            for entry in zf.namelist():
                if not entry.endswith(".py"):
                    continue
                try:
                    text = zf.read(entry).decode("utf-8", errors="replace")
                    if info["world_version"] is None:
                        m = re.search(
                            r'apworld_version\s*=\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)',
                            text,
                        )
                        if m:
                            info["world_version"] = f"{m.group(1)}.{m.group(2)}.{m.group(3)}"
                    if info["game"] is None:
                        m = WORLD_GAME_RE.search(text)
                        if m:
                            info["game"] = m.group(1)
                    if info["world_version"] is not None and info["game"] is not None:
                        return info
                except Exception:
                    pass
    except Exception:
        pass
    return info


def get_min_ap_version(
    yaml_data: dict[str, bytes],
    apworld_data: dict[str, bytes] | None = None,
    apworld_infos: dict[str, dict] | None = None,
) -> str | None:
    max_ver: tuple[int, ...] | None = None
    max_str: str | None = None

    def _update(v_str: str | None) -> None:
        nonlocal max_ver, max_str
        if v_str:
            v = parse_version(v_str)
            if max_ver is None or v > max_ver:
                max_ver = v
                max_str = v_str

    for data in yaml_data.values():
        ap_ver, _ = get_yaml_requires(data)
        _update(ap_ver)

    if apworld_infos is not None:
        for info in apworld_infos.values():
            _update(info.get("minimum_ap_version"))
    else:
        for data in (apworld_data or {}).values():
            _update(get_apworld_info(data).get("minimum_ap_version"))

    return max_str
