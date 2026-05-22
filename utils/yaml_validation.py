import functools
import io
import json
import os
import re
import subprocess
import sys
import zipfile

import requests
import yaml

from config import ARCHIPELAGO_BASE, log

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
    try:
        env = {
            **{k: v for k, v in os.environ.items() if k not in ("BOT_TOKEN", "SERVER_PASSWORD")},
            "PYTHONPATH": str(version_dir),
            "PYTHONUSERBASE": f"/archipelago/pyenv/{version_dir.name}",
        }
        _script = (
            "import sys\n"
            "sys.path.insert(0, sys.argv[1])\n"
            "import worlds\n"
            "from worlds.AutoWorld import AutoWorldRegister\n"
            "import inspect\n"
            "names = []\n"
            "for n, c in AutoWorldRegister.world_types.items():\n"
            "    try:\n"
            "        f = inspect.getfile(c)\n"
            "    except Exception:\n"
            "        f = ''\n"
            "    if '.apworld' not in f:\n"
            "        names.append(n)\n"
            "print('\\n'.join(names))\n"
        )
        result = subprocess.run(
            [sys.executable, "-c", _script, str(version_dir)],
            capture_output=True,
            text=True,
            timeout=30,
            env=env,
        )
        if result.returncode == 0:
            return frozenset(line.strip() for line in result.stdout.splitlines() if line.strip())
        log.warning(f"AutoWorldRegister query failed (rc={result.returncode}):\n{result.stderr.strip()}")
    except Exception as e:
        log.warning(f"AutoWorldRegister query exception: {e}")
    log.warning(f"Could not query AutoWorldRegister for {version_dir.name} — returning empty set.")
    return frozenset()


def _iter_yaml_docs(yaml_bytes: bytes):
    try:
        for doc in yaml.safe_load_all(yaml_bytes):
            if isinstance(doc, dict):
                yield doc
    except Exception:
        pass


def get_yaml_names(yaml_bytes: bytes) -> list[str]:
    names = []
    for doc in _iter_yaml_docs(yaml_bytes):
        name = doc.get("name")
        if name:
            names.append(str(name).strip())
    return names


def get_yaml_name(yaml_bytes: bytes) -> str | None:
    names = get_yaml_names(yaml_bytes)
    return names[0] if names else None


def get_yaml_games(yaml_bytes: bytes) -> list[str]:
    games = []
    for doc in _iter_yaml_docs(yaml_bytes):
        game = doc.get("game")
        if isinstance(game, dict):
            games.extend(str(g).strip() for g in game if g)
        elif game:
            games.append(str(game).strip())
    return games


def get_yaml_game(yaml_bytes: bytes) -> str | None:
    games = get_yaml_games(yaml_bytes)
    return games[0] if games else None


def get_yaml_requires(yaml_bytes: bytes) -> tuple[str | None, dict[str, str]]:
    max_ap_version: str | None = None
    all_game_reqs: dict[str, str] = {}
    try:
        for doc in _iter_yaml_docs(yaml_bytes):
            req = doc.get("requires")
            if not isinstance(req, dict):
                continue
            ap_version = req.get("version")
            if ap_version is not None:
                v = str(ap_version).strip()
                if max_ap_version is None or parse_version(v) > parse_version(max_ap_version):
                    max_ap_version = v
            game_reqs = req.get("game") or {}
            if isinstance(game_reqs, dict):
                all_game_reqs.update({str(k): str(v) for k, v in game_reqs.items()})
    except Exception:
        pass
    return max_ap_version, all_game_reqs


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
