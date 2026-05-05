import logging
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(name)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger('version_manager')

VERSIONS_DIR     = Path("/archipelago/versions")
ROMS_DIR         = Path("/roms")
GITHUB_API       = "https://api.github.com/repos/ArchipelagoMW/Archipelago/releases"
RELEASES_TO_KEEP = 2
CHECK_INTERVAL   = 12 * 60 * 60  # 12 hours


# ── GitHub ────────────────────────────────────────────────────────────────────

def fetch_latest_tags(n: int = RELEASES_TO_KEEP) -> list[str]:
    resp = requests.get(
        GITHUB_API,
        params={"per_page": 20},  # fetch extra to account for pre-releases
        headers={"Accept": "application/vnd.github+json"},
        timeout=30,
    )
    resp.raise_for_status()
    stable = [r["tag_name"] for r in resp.json() if not r["prerelease"] and not r["draft"]]
    return stable[:n]


# ── Version state ─────────────────────────────────────────────────────────────

def get_installed_versions() -> list[str]:
    if not VERSIONS_DIR.exists():
        return []
    tags = [
        d.name for d in VERSIONS_DIR.iterdir()
        if d.is_dir() and (d / "Generate.py").exists()
    ]
    return sorted(tags, reverse=True)


# ── ROMs ──────────────────────────────────────────────────────────────────────

def symlink_roms(version_dir: Path) -> None:
    if not ROMS_DIR.exists():
        return
    count = 0
    for rom in ROMS_DIR.iterdir():
        if rom.is_file():
            link = version_dir / rom.name
            if not link.exists():
                link.symlink_to(rom)
                count += 1
    if count:
        log.info(f"Symlinked {count} ROM(s) into {version_dir.name}")


# ── Cloning ───────────────────────────────────────────────────────────────────

def install_requirements(tag: str, version_dir: Path) -> None:
    """Install core requirements for a version into its isolated pyenv directory."""
    marker = version_dir / ".requirements_installed"
    if marker.exists():
        return

    log.info(f"Installing requirements for {tag}...")
    result = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--quiet",
         "-r", str(version_dir / "requirements.txt")],
        capture_output=True,
        env={**os.environ, "PYTHONUSERBASE": f"/archipelago/pyenv/{tag}", "PIP_USER": "1"},
    )
    if result.returncode == 0:
        marker.touch()
        log.info(f"Requirements installed successfully for {tag}.")
    else:
        log.error(f"pip install failed for {tag}: {result.stderr.decode()}")
        log.warning(f"Requirements failed to install for {tag} — will retry next check.")


def clone_version(tag: str) -> bool:
    version_dir = VERSIONS_DIR / tag
    if (version_dir / "Generate.py").exists():
        symlink_roms(version_dir)  # pick up any newly added ROMs
        install_requirements(tag, version_dir)  # no-op if marker present
        return False  # already installed

    log.info(f"Cloning Archipelago {tag}...")
    version_dir.mkdir(parents=True, exist_ok=True)

    result = subprocess.run(
        ["git", "clone", "--depth", "1", "--branch", tag,
         "https://github.com/ArchipelagoMW/Archipelago.git", str(version_dir)],
        capture_output=True,
    )
    if result.returncode != 0:
        log.error(f"Failed to clone {tag}: {result.stderr.decode()}")
        shutil.rmtree(version_dir, ignore_errors=True)
        return False

    install_requirements(tag, version_dir)
    symlink_roms(version_dir)
    log.info(f"Archipelago {tag} ready.")
    return True


# ── Culling ───────────────────────────────────────────────────────────────────

def cull_old_versions(keep_tags: list[str]) -> None:
    if not VERSIONS_DIR.exists():
        return
    for version_dir in VERSIONS_DIR.iterdir():
        if not version_dir.is_dir() or version_dir.name in keep_tags:
            continue
        if (version_dir / ".generating").exists():
            log.warning(f"Skipping cull of {version_dir.name} — generation in progress.")
            continue
        log.info(f"Removing old version {version_dir.name}...")
        shutil.rmtree(version_dir, ignore_errors=True)


# ── Main check ────────────────────────────────────────────────────────────────

def check_and_update() -> None:
    log.info("Checking for new Archipelago releases...")
    try:
        latest_tags = fetch_latest_tags()
    except Exception as e:
        log.error(f"Failed to fetch release tags: {e}")
        return

    log.info(f"Latest stable releases: {latest_tags}")

    for tag in latest_tags:
        try:
            clone_version(tag)
        except Exception as e:
            log.error(f"Error processing {tag}: {e}")

    cull_old_versions(latest_tags)
    log.info(f"Installed versions: {get_installed_versions()}")


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("Version manager starting up...")
    VERSIONS_DIR.mkdir(parents=True, exist_ok=True)
    check_and_update()
    while True:
        log.info(f"Next check in {CHECK_INTERVAL // 3600} hours.")
        time.sleep(CHECK_INTERVAL)
        check_and_update()
