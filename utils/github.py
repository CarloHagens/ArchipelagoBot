import io
import zipfile

import requests

from config import MAX_APWORLD_BYTES, MAX_ZIP_BYTES
from utils.files import safe_filename


def download_apworld_from_github(owner: str, repo: str, tag: str) -> tuple[str, bytes]:
    api_url = f"https://api.github.com/repos/{owner}/{repo}/releases/tags/{tag}"
    resp = requests.get(api_url, timeout=30, headers={"Accept": "application/vnd.github+json"})
    resp.raise_for_status()

    assets = resp.json().get("assets", [])

    apworld_assets = [a for a in assets if a["name"].endswith(".apworld")]
    if apworld_assets:
        asset = apworld_assets[0]
        download = requests.get(asset["browser_download_url"], timeout=120)
        download.raise_for_status()
        return safe_filename(asset["name"]), download.content

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
                return safe_filename(apworld_names[0]), data

    raise RuntimeError(f"No .apworld asset (or zip containing one) found in {owner}/{repo} release {tag}")
