import io
import zipfile

import requests

import shutil

from config import APWORLD_CACHE_DIR, APWORLD_CACHE_MAX, MAX_APWORLD_BYTES, MAX_ZIP_BYTES
from utils.files import safe_filename


def _cache_path(owner: str, repo: str, tag: str) -> "Path":
    from pathlib import Path
    return APWORLD_CACHE_DIR / owner / repo / tag


def _load_from_cache(owner: str, repo: str, tag: str) -> tuple[str, bytes] | None:
    cache_dir = _cache_path(owner, repo, tag)
    if cache_dir.is_dir():
        for f in cache_dir.iterdir():
            if f.suffix == ".apworld":
                return f.name, f.read_bytes()
    return None


def _evict_cache() -> None:
    try:
        tag_dirs = [
            p for owner in APWORLD_CACHE_DIR.iterdir() if owner.is_dir()
            for repo in owner.iterdir() if repo.is_dir()
            for p in repo.iterdir() if p.is_dir()
        ]
        if len(tag_dirs) <= APWORLD_CACHE_MAX:
            return
        tag_dirs.sort(key=lambda p: p.stat().st_mtime)
        for old in tag_dirs[:len(tag_dirs) - APWORLD_CACHE_MAX]:
            shutil.rmtree(old, ignore_errors=True)
    except Exception:
        pass


def _save_to_cache(owner: str, repo: str, tag: str, filename: str, data: bytes) -> None:
    cache_dir = _cache_path(owner, repo, tag)
    try:
        cache_dir.mkdir(parents=True, exist_ok=True)
        (cache_dir / filename).write_bytes(data)
        _evict_cache()
    except Exception:
        pass  # cache write failure is non-fatal


def download_apworld_from_github(owner: str, repo: str, tag: str) -> tuple[str, bytes]:
    cached = _load_from_cache(owner, repo, tag)
    if cached:
        return cached

    api_url = f"https://api.github.com/repos/{owner}/{repo}/releases/tags/{tag}"
    resp = requests.get(api_url, timeout=30, headers={"Accept": "application/vnd.github+json"})
    resp.raise_for_status()

    assets = resp.json().get("assets", [])

    apworld_assets = [a for a in assets if a["name"].endswith(".apworld")]
    if apworld_assets:
        asset = apworld_assets[0]
        download = requests.get(asset["browser_download_url"], timeout=120)
        download.raise_for_status()
        filename, data = safe_filename(asset["name"]), download.content
        _save_to_cache(owner, repo, tag, filename, data)
        return filename, data

    zip_assets = [a for a in assets if a["name"].endswith(".zip")]
    for zip_asset in zip_assets:
        if zip_asset["size"] > MAX_ZIP_BYTES:
            raise RuntimeError(
                f"Zip asset `{zip_asset['name']}` is too large ({zip_asset['size'] // 1024 // 1024} MB). "
                f"Max is {MAX_ZIP_BYTES // 1024 // 1024} MB."
            )
        download = requests.get(zip_asset["browser_download_url"], timeout=120)
        download.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(download.content)) as zf:
            apworld_names = [n for n in zf.namelist() if n.endswith(".apworld")]
            if apworld_names:
                data = zf.read(apworld_names[0])
                if len(data) > MAX_APWORLD_BYTES:
                    raise RuntimeError(
                        f".apworld inside zip is too large ({len(data) // 1024 // 1024} MB). "
                        f"Max is {MAX_APWORLD_BYTES // 1024 // 1024} MB."
                    )
                filename = safe_filename(apworld_names[0])
                _save_to_cache(owner, repo, tag, filename, data)
                return filename, data

    raise RuntimeError(f"No .apworld asset (or zip containing one) found in {owner}/{repo} release {tag}")
