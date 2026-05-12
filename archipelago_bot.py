import asyncio
from dataclasses import dataclass, field
import functools
import html
import io
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import uuid
import zipfile
from datetime import datetime, timezone, timedelta
from pathlib import Path

import dateparser
import discord
import requests
import yaml
from discord import app_commands

# ── Configuration ─────────────────────────────────────────────────────────────

BOT_TOKEN       = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
SERVER_PASSWORD = os.environ.get("SERVER_PASSWORD", "archipelago")

VERSIONS_DIR     = Path("/archipelago/versions")
ROMS_DIR         = Path("/roms")
ARCHIPELAGO_BASE = "https://archipelago.gg"

GITHUB_RELEASE_RE = re.compile(
    r'https://github\.com/([^/]+)/([^/]+)/releases/tag/([^\s>]+)'
)

VALID_RELEASE_COLLECT_MODES = ["disabled", "enabled", "auto", "auto-enabled", "goal"]
VALID_REMAINING_MODES       = ["disabled", "enabled", "goal"]
SPOILER_MODES               = {"none": 0, "basic": 1, "playthrough": 2, "full": 3}

MAX_PARALLEL_GENERATIONS = 4
MAX_SEEDS_PER_RUN        = 20
MAX_YAML_FILES           = 200
MAX_APWORLD_FILES        = 200
MAX_YAML_BYTES           = 1 * 1024 * 1024    # 1 MB per yaml
MAX_APWORLD_BYTES        = 30 * 1024 * 1024   # 30 MB per apworld
MAX_ZIP_BYTES            = 100 * 1024 * 1024  # 100 MB zip containing an apworld
MAX_GENERATION_MEMORY    = 3 * 1024 * 1024 * 1024  # 3 GB across all active commands
MSG_MEMORY_FULL          = f"⚠️ The bot is currently holding too much data in memory ({MAX_GENERATION_MEMORY // 1024 // 1024 // 1024} GB limit). Please try again shortly."
RUNS_FILE                = Path("/archipelago/runs.json")
MONITORS_FILE            = Path("/archipelago/monitors.json")
SCHEDULED_FILE           = Path("/archipelago/scheduled.json")
MAX_RUNS                 = 50
TIMEZONE                 = os.environ.get("TIMEZONE", "UTC")

COMMON_TIMEZONES = [
    "UTC",
    "Europe/London", "Europe/Amsterdam", "Europe/Paris", "Europe/Berlin",
    "Europe/Madrid", "Europe/Rome", "Europe/Stockholm", "Europe/Helsinki",
    "America/New_York", "America/Chicago", "America/Denver", "America/Los_Angeles",
    "America/Toronto", "America/Vancouver", "America/Sao_Paulo",
    "Australia/Sydney", "Australia/Melbourne", "Australia/Perth",
    "Asia/Tokyo", "Asia/Seoul", "Asia/Shanghai", "Asia/Singapore",
    "Asia/Kolkata", "Asia/Dubai", "Pacific/Auckland",
]

_locks: dict[str, asyncio.Lock] = {}
_monitor_pending: set[int] = set()  # thread_ids that need a re-check after the current one finishes
_generation_sem: asyncio.Semaphore | None = None
_memory_in_use: int = 0
_monitors: dict = {}    # thread_id_str → {"known_issue_keys": [...]}
_scheduled: list = []   # list of scheduled job dicts
_checker_task: asyncio.Task | None = None


@dataclass
class ScanResult:
    yaml_data:        dict = field(default_factory=dict)
    apworld_data:     dict = field(default_factory=dict)
    yaml_uploaders:   dict = field(default_factory=dict)
    apworld_uploaders: dict = field(default_factory=dict)
    reserved_bytes:   int  = 0
    had_error:        bool = False
    issues:           list = field(default_factory=list)  # list[tuple[str, str]] — (key, display_msg)


def _get_lock(key: str) -> asyncio.Lock:
    if key not in _locks:
        _locks[key] = asyncio.Lock()
    return _locks[key]


def get_monitor_lock(thread_id: int) -> asyncio.Lock:
    return _get_lock(f"monitor:{thread_id}")


def get_setup_lock(version_dir: Path) -> asyncio.Lock:
    return _get_lock(f"setup:{version_dir}")


def get_generation_sem() -> asyncio.Semaphore:
    global _generation_sem
    if _generation_sem is None:
        _generation_sem = asyncio.Semaphore(MAX_PARALLEL_GENERATIONS)
    return _generation_sem

NUMBERED_LINE_PREFIXES = tuple(f"{i}." for i in range(1, 20))
UTF8_BOM               = b'\xef\xbb\xbf'

SERVER_KEYS    = {"release_mode", "collect_mode", "remaining_mode", "password", "server_password"}
GENERATOR_KEYS = {"race", "spoiler"}

DEFAULT_HOST_YAML = {
    "server_options": {
        "release_mode": "auto", "collect_mode": "auto",
        "remaining_mode": "goal", "password": None, "server_password": None,
    },
    "generator": {"race": 0, "spoiler": 3},
}

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-8s %(name)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger('bot')

intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
intents.guilds = True

client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)


# ── Version helpers ───────────────────────────────────────────────────────────

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
    """Parse "1.2.3" or "v1.2.3" → (1, 2, 3). Returns (0,) on failure."""
    try:
        return tuple(int(x) for x in str(v).lstrip("v").split("."))
    except Exception:
        return (0,)


def _norm(s: str) -> str:
    """Normalise a string to lowercase alphanumeric for fuzzy game-name matching."""
    return re.sub(r'[^a-z0-9]', '', s.lower())


# ── File helpers ──────────────────────────────────────────────────────────────

def normalise_yaml_bytes(raw: bytes) -> bytes:
    if raw.startswith(UTF8_BOM):
        raw = raw[len(UTF8_BOM):]
    return raw.replace(b'\r\n', b'\n')


def apworld_stem(filename: str) -> str:
    return Path(filename).stem.lower()


def safe_filename(filename: str) -> str:
    """Strip any path components from a filename to prevent directory traversal."""
    return Path(filename).name


# ── GitHub apworld download ───────────────────────────────────────────────────

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


# ── Thread file collection ────────────────────────────────────────────────────

async def handle_github_link(
    match, thread, author, message_id: int, seen_stems: set, seen_repos: dict,
    audit: bool = False, issues: list | None = None,
) -> tuple[str, bytes] | None | bool:
    owner, repo, tag = match.group(1), match.group(2), match.group(3)
    repo_key  = f"{owner}/{repo}".lower()
    tag_lower = tag.lower()
    mention   = author.mention
    link      = match.group(0)

    if repo_key in seen_repos:
        if seen_repos[repo_key] == tag_lower:
            return None  # same link seen twice, skip silently
        msg = (
            f"{mention} ⚠️ Multiple releases linked for **{owner}/{repo}**: "
            f"`{seen_repos[repo_key]}` and `{tag}` — remove one."
        )
        if audit:
            if issues is not None:
                # Keyed on message_id so edits to the same message don't re-report
                issues.append((f"multi_release:{message_id}", msg))
            return None
        await thread.send(msg)
        return False

    seen_repos[repo_key] = tag_lower

    try:
        loop = asyncio.get_running_loop()
        filename, data = await loop.run_in_executor(
            None, download_apworld_from_github, owner, repo, tag
        )
    except Exception as e:
        msg = f"{mention} ⚠️ **{link}**: could not download apworld — `{e}`"
        if audit:
            if issues is not None:
                issues.append((f"download_failed:{message_id}", msg))
            return None
        await thread.send(msg)
        return None

    stem = apworld_stem(filename)
    if stem in seen_stems:
        msg = f"{mention} ⚠️ **{stem}**: duplicate apworld — GitHub link conflicts with an already posted file."
        if audit:
            if issues is not None:
                issues.append((f"dup_apworld_github:{message_id}:{stem}", msg))
            return None
        await thread.send(msg)
        return False

    seen_stems.add(stem)
    return filename, data


async def collect_files_from_thread(thread, audit: bool = False) -> ScanResult:
    """Collect yaml and apworld files from thread history into memory.

    audit=False (default): post errors inline with author tag, abort on first problem.
    audit=True: accumulate all issues silently without aborting or posting.

    Caller must subtract result.reserved_bytes from _memory_in_use when done."""
    global _memory_in_use
    result              = ScanResult()
    seen_apworld_stems  = set()
    seen_repos: dict    = {}

    def abort() -> ScanResult:
        global _memory_in_use
        _memory_in_use        -= result.reserved_bytes
        result.reserved_bytes  = 0
        result.had_error       = True
        return result

    async for message in thread.history(limit=500, oldest_first=True):
        if message.author == client.user:
            continue
        mention = message.author.mention

        for attachment in message.attachments:
            name      = attachment.filename.lower()
            safe_name = safe_filename(attachment.filename)

            if name.endswith(".yaml") or name.endswith(".yml"):
                if len(result.yaml_data) >= MAX_YAML_FILES:
                    if not audit:
                        await thread.send(f"⚠️ Too many YAML files (max {MAX_YAML_FILES}). Only the first {MAX_YAML_FILES} will be used.")
                    return result  # truncate — caller releases memory via try/finally

                if attachment.size > MAX_YAML_BYTES:
                    msg = f"{mention} ⚠️ **{safe_name}**: too large ({attachment.size // 1024} KB, max {MAX_YAML_BYTES // 1024} KB)."
                    if audit:
                        result.issues.append((f"{message.id}:yaml_too_large:{safe_name}", msg))
                        continue
                    await thread.send(msg)
                    return abort()

                if _memory_in_use + attachment.size > MAX_GENERATION_MEMORY:
                    if audit:
                        result.issues.append(("memory_full", MSG_MEMORY_FULL))
                        return result
                    await thread.send(MSG_MEMORY_FULL)
                    return abort()

                # Reserve memory atomically before downloading (no await between check and increment)
                _memory_in_use        += attachment.size
                result.reserved_bytes += attachment.size
                result.yaml_data[safe_name]      = normalise_yaml_bytes(await attachment.read())
                result.yaml_uploaders[safe_name] = message.author

            elif name.endswith(".apworld"):
                if len(result.apworld_data) >= MAX_APWORLD_FILES:
                    if not audit:
                        await thread.send(f"⚠️ Too many apworld files (max {MAX_APWORLD_FILES}).")
                        return abort()
                    continue

                if attachment.size > MAX_APWORLD_BYTES:
                    msg = f"{mention} ⚠️ **{safe_name}**: too large ({attachment.size // 1024 // 1024} MB, max {MAX_APWORLD_BYTES // 1024 // 1024} MB)."
                    if audit:
                        result.issues.append((f"{message.id}:apworld_too_large:{safe_name}", msg))
                        continue
                    await thread.send(msg)
                    return abort()

                if _memory_in_use + attachment.size > MAX_GENERATION_MEMORY:
                    if audit:
                        result.issues.append(("memory_full", MSG_MEMORY_FULL))
                        return result
                    await thread.send(MSG_MEMORY_FULL)
                    return abort()

                stem = apworld_stem(attachment.filename)
                if stem in seen_apworld_stems:
                    msg = f"{mention} ⚠️ **{safe_name}**: duplicate apworld — remove one."
                    if audit:
                        result.issues.append((f"{message.id}:dup_apworld:{stem}", msg))
                        continue
                    await thread.send(msg)
                    return abort()

                # Reserve memory atomically before downloading
                _memory_in_use        += attachment.size
                result.reserved_bytes += attachment.size
                result.apworld_data[safe_name]      = await attachment.read()
                result.apworld_uploaders[safe_name] = message.author
                seen_apworld_stems.add(stem)

        for match in GITHUB_RELEASE_RE.finditer(message.content or ""):
            gh = await handle_github_link(
                match, thread, message.author, message.id, seen_apworld_stems, seen_repos,
                audit=audit, issues=result.issues,
            )
            if gh is False:
                if audit:
                    continue  # issue already appended by handle_github_link
                return abort()
            if gh is not None:
                filename, data = gh
                file_size = len(data)
                if _memory_in_use + file_size > MAX_GENERATION_MEMORY:
                    if audit:
                        result.issues.append(("memory_full", MSG_MEMORY_FULL))
                        return result
                    await thread.send(MSG_MEMORY_FULL)
                    return abort()
                _memory_in_use        += file_size
                result.reserved_bytes += file_size
                result.apworld_data[filename]      = data
                result.apworld_uploaders[filename] = message.author

    return result


# ── Host YAML management ──────────────────────────────────────────────────────

def load_host_yaml(host_yaml_path: Path) -> dict:
    if host_yaml_path.exists():
        return yaml.safe_load(host_yaml_path.read_text(encoding="utf-8"))
    return DEFAULT_HOST_YAML.copy()


def save_host_yaml(config: dict, host_yaml_path: Path) -> None:
    host_yaml_path.write_text(
        yaml.dump(config, default_flow_style=False, allow_unicode=True),
        encoding="utf-8",
    )


def apply_host_yaml_options(opts: dict, host_yaml_path: Path) -> dict:
    config = load_host_yaml(host_yaml_path)
    originals = {}
    for key, value in opts.items():
        if key in SERVER_KEYS:
            originals[("server_options", key)] = config["server_options"].get(key)
            config["server_options"][key] = value
        elif key in GENERATOR_KEYS:
            originals[("generator", key)] = config["generator"].get(key)
            config["generator"][key] = value
    save_host_yaml(config, host_yaml_path)
    return originals


def restore_host_yaml(originals: dict, host_yaml_path: Path) -> None:
    config = load_host_yaml(host_yaml_path)
    for (section, key), value in originals.items():
        config[section][key] = value
    save_host_yaml(config, host_yaml_path)


# ── Generation log parsing ────────────────────────────────────────────────────

def parse_generation_error(log_text: str) -> str | tuple:
    lines    = log_text.splitlines()
    stripped = [line.strip() for line in lines]

    no_world = next((line for line in stripped if line.startswith("Exception: No world found")), None)
    if no_world:
        return no_world.split(":", 1)[1].strip().split(".")[0] + "."

    rom_missing = next((line for line in stripped if "does not exist, but" in line and "rom_file" in line), None)
    if rom_missing:
        fname = rom_missing.split("FileNotFoundError:")[-1].strip().split(" does not exist")[0].strip()
        return f"Missing ROM file: {fname}. This game requires a ROM to generate and cannot run on the server."

    invalid_lines = [line for line in stripped if line.startswith(NUMBERED_LINE_PREFIXES) and "is invalid" in line]
    if invalid_lines:
        msg, filenames = _parse_invalid_files(invalid_lines)
        # Append any Exception/ValueError detail lines for context (e.g. "No functional world found")
        friendly = [line for line in stripped if line.startswith(("Exception:", "ValueError:"))]
        detail = _parse_friendly_errors(friendly)
        if detail:
            msg = f"{msg}\n{detail}"
        return msg, filenames

    friendly = [line for line in stripped if line.startswith(("Exception:", "ValueError:"))]
    if friendly:
        return _parse_friendly_errors(friendly)

    error_lines = [
        line for line in lines
        if any(kw in line for kw in ("Exception", "Error", "invalid", "failed"))
        and "logging initialized" not in line
    ]
    return "\n".join(error_lines[-10:]) if error_lines else log_text[-1500:]


def _parse_invalid_files(invalid_lines: list) -> tuple:
    msgs, filenames = [], []
    for line in invalid_lines:
        body = line.split(".", 1)[1].strip()
        if "is invalid" not in body:
            continue
        short = body[:body.index("is invalid.") + len("is invalid.")]
        msgs.append(short)
        try:
            filenames.append(short.split("File ")[1].split(" is invalid")[0])
        except IndexError:
            pass
    return "\n".join(msgs), filenames


def _parse_friendly_errors(friendly_lines: list) -> str:
    seen, messages = set(), []
    for line in friendly_lines:
        msg = line.split(":", 1)[1].strip()
        if msg in seen or msg.startswith("Encountered"):
            continue
        seen.add(msg)
        if msg.startswith("No world found"):
            msg = msg.split(".")[0] + "."
        messages.append(msg)
    return "\n".join(messages)


# ── Generation runner ─────────────────────────────────────────────────────────

async def setup_and_launch(
    version_dir: Path,
    yaml_data: dict[str, bytes],
    apworld_data: dict[str, bytes],
    output_dir: Path,
) -> subprocess.Popen:
    """Acquire the per-version setup lock, write files, launch Generate.py, release lock."""
    players_dir = version_dir / "Players"
    worlds_dir  = version_dir / "custom_worlds"

    def write_files() -> None:
        players_dir.mkdir(parents=True, exist_ok=True)
        for f in players_dir.glob("*.yaml"): f.unlink(missing_ok=True)
        for f in players_dir.glob("*.yml"):  f.unlink(missing_ok=True)
        for name, data in yaml_data.items():
            (players_dir / name).write_bytes(data)

        worlds_dir.mkdir(parents=True, exist_ok=True)
        for f in worlds_dir.glob("*.apworld"): f.unlink(missing_ok=True)
        for name, data in apworld_data.items():
            (worlds_dir / name).write_bytes(data)

        output_dir.mkdir(parents=True, exist_ok=True)

    loop = asyncio.get_running_loop()
    async with get_setup_lock(version_dir):
        # Run all NAS file I/O in a thread so the event loop stays responsive
        await loop.run_in_executor(None, write_files)

        proc = subprocess.Popen(
            [sys.executable, str(version_dir / "Generate.py"),
             "--outputpath", str(output_dir)],
            cwd=str(version_dir),
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env={
                # Pass full environment minus secrets — exclusion list is safer than a whitelist
                # since we can't predict what Archipelago or apworlds need.
                **{k: v for k, v in os.environ.items() if k not in ("BOT_TOKEN", "SERVER_PASSWORD")},
                "PYTHONUSERBASE": f"/archipelago/pyenv/{version_dir.name}",
            },
        )
        proc.stdin.write(b"\n" * 20)
        proc.stdin.close()

    return proc  # lock released; generation runs freely


async def _run_one_generation(
    version_dir: Path,
    yaml_data: dict[str, bytes],
    apworld_data: dict[str, bytes],
) -> tuple[int, list[Path]]:
    """Acquire the global generation slot, launch in an isolated output dir, and wait.
    Returns (returncode, list of zip paths moved to the main output dir)."""
    main_output = version_dir / "output"
    temp_output = main_output / f"run_{uuid.uuid4().hex[:8]}"

    async with get_generation_sem():
        proc = await setup_and_launch(version_dir, yaml_data, apworld_data, temp_output)
        loop = asyncio.get_running_loop()
        returncode = await loop.run_in_executor(None, proc.wait)
        if returncode != 0 and proc.stderr:
            stderr_out = proc.stderr.read().decode("utf-8", errors="replace")
            # Strip known-harmless Python UserWarning lines (pkg_resources deprecation, etc.)
            # so they don't obscure the real error in the logs.
            filtered = "\n".join(
                line for line in stderr_out.splitlines()
                if "UserWarning" not in line and "warnings.warn(" not in line
            ).strip()
            if filtered:
                log.warning(f"Generate.py stderr:\n{filtered}")

    # Move zips to main output dir and clean up temp dir
    main_output.mkdir(parents=True, exist_ok=True)
    moved = []
    for zip_path in temp_output.glob("AP_*.zip"):
        dest = main_output / zip_path.name
        zip_path.rename(dest)
        moved.append(dest)
    shutil.rmtree(temp_output, ignore_errors=True)

    return returncode, moved


def _find_missing_module(log_text: str) -> str | None:
    """Return the top-level package name from a ModuleNotFoundError in a generation log."""
    for line in log_text.splitlines():
        m = re.match(r"ModuleNotFoundError: No module named '([^'.]+)", line.strip())
        if m:
            return m.group(1)
    return None


async def _install_missing_module(module_name: str, version_dir: Path) -> bool:
    """pip install a missing apworld dependency into the version's persistent pyenv.
    Returns True if installation succeeded."""
    log.info(f"Installing missing module '{module_name}'...")
    env = {
        **{k: v for k, v in os.environ.items() if k not in ("BOT_TOKEN", "SERVER_PASSWORD")},
        "PYTHONUSERBASE": str(Path("/archipelago/pyenv") / version_dir.name),
    }
    loop = asyncio.get_running_loop()
    try:
        result = await loop.run_in_executor(
            None,
            lambda: subprocess.run(
                [sys.executable, "-m", "pip", "install", "--quiet", module_name],
                env=env,
                capture_output=True,
                timeout=120,
            ),
        )
        if result.returncode == 0:
            log.info(f"Installed '{module_name}' successfully.")
            return True
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        log.warning(f"Failed to install '{module_name}': {stderr}")
    except Exception as e:
        log.warning(f"Error installing '{module_name}': {e}")
    return False


async def run_generation(
    opts: dict,
    version_dir: Path,
    yaml_data: dict[str, bytes],
    apworld_data: dict[str, bytes],
) -> tuple[bool, str, list[Path]]:
    """Run a single generation. Returns (success, error, new_zips)."""
    host_yaml_path = version_dir / "host.yaml"
    logs_dir       = version_dir / "logs"
    lock_file      = version_dir / ".generating"

    loop = asyncio.get_running_loop()
    originals = await loop.run_in_executor(None, apply_host_yaml_options, opts, host_yaml_path)
    logs_dir.mkdir(exist_ok=True)
    before_logs = set(logs_dir.glob("Generate_*.txt"))

    log.info(f"Running Generate.py for version {version_dir.name}...")
    lock_file.touch()
    try:
        returncode, new_zips = await _run_one_generation(version_dir, yaml_data, apworld_data)
    finally:
        lock_file.unlink(missing_ok=True)

    await loop.run_in_executor(None, restore_host_yaml, originals, host_yaml_path)

    if returncode == 0:
        log.info("Generation succeeded.")
        return True, "", new_zips

    log.warning("Generation failed, parsing logs...")
    after_logs = set(logs_dir.glob("Generate_*.txt"))
    new_logs   = sorted(after_logs - before_logs, key=lambda p: p.stat().st_mtime, reverse=True)
    if not new_logs:
        return False, "Generation failed but no log file was found.", []

    log_text = new_logs[0].read_text(encoding="utf-8", errors="replace")
    for line in log_text.splitlines():
        stripped = line.strip()
        if stripped.startswith(("ModuleNotFoundError:", "ImportError:")):
            log.warning(f"Apworld import error: {stripped}")

    # Auto-install a missing apworld dependency and retry once
    missing = _find_missing_module(log_text)
    if missing and await _install_missing_module(missing, version_dir):
        log.info("Retrying generation...")
        originals = await loop.run_in_executor(None, apply_host_yaml_options, opts, host_yaml_path)
        before_retry_logs = set(logs_dir.glob("Generate_*.txt"))
        lock_file.touch()
        try:
            returncode, new_zips = await _run_one_generation(version_dir, yaml_data, apworld_data)
        finally:
            lock_file.unlink(missing_ok=True)
        await loop.run_in_executor(None, restore_host_yaml, originals, host_yaml_path)
        if returncode == 0:
            log.info("Retry succeeded.")
            return True, "", new_zips
        after_retry_logs = set(logs_dir.glob("Generate_*.txt"))
        retry_new_logs = sorted(after_retry_logs - before_retry_logs, key=lambda p: p.stat().st_mtime, reverse=True)
        if retry_new_logs:
            log_text = retry_new_logs[0].read_text(encoding="utf-8", errors="replace")

    return False, parse_generation_error(log_text), []


async def run_generations(
    count: int,
    opts: dict,
    version_dir: Path,
    yaml_data: dict[str, bytes],
    apworld_data: dict[str, bytes],
) -> tuple[int, list[Path], list[str]]:
    """Run `count` generations with at most MAX_PARALLEL_GENERATIONS concurrent.
    Returns (succeeded, new_zip_paths, error_messages)."""
    host_yaml_path = version_dir / "host.yaml"
    logs_dir       = version_dir / "logs"
    lock_file      = version_dir / ".generating"

    loop = asyncio.get_running_loop()
    originals   = await loop.run_in_executor(None, apply_host_yaml_options, opts, host_yaml_path)
    logs_dir.mkdir(exist_ok=True)
    before_logs = set(logs_dir.glob("Generate_*.txt"))

    async def one_run() -> tuple[int, list[Path]]:
        return await _run_one_generation(version_dir, yaml_data, apworld_data)

    log.info(f"Running {count} generation(s) for version {version_dir.name} (max {MAX_PARALLEL_GENERATIONS} parallel)...")
    lock_file.touch()
    try:
        results = await asyncio.gather(*[one_run() for _ in range(count)])
    finally:
        lock_file.unlink(missing_ok=True)

    await loop.run_in_executor(None, restore_host_yaml, originals, host_yaml_path)

    succeeded = sum(1 for rc, _ in results if rc == 0)
    new_zips  = sorted(
        (p for _, zips in results for p in zips),
        key=lambda p: p.stat().st_mtime,
    )

    after_logs = set(logs_dir.glob("Generate_*.txt"))
    new_logs   = sorted(after_logs - before_logs, key=lambda p: p.stat().st_mtime)
    errors     = [parse_generation_error(p.read_text(encoding="utf-8", errors="replace")) for p in new_logs]

    log.info(f"{succeeded}/{count} generation(s) succeeded, {len(new_zips)} zip(s) produced.")
    return succeeded, new_zips, errors


def parse_sphere_count(zip_path: Path) -> int | None:
    """Read the spoiler log inside a zip and return the number of playthrough spheres (1+)."""
    try:
        with zipfile.ZipFile(zip_path) as zf:
            spoiler_name = next((n for n in zf.namelist() if n.endswith("_Spoiler.txt")), None)
            if not spoiler_name:
                return None
            text = zf.read(spoiler_name).decode("utf-8", errors="replace")
    except Exception:
        return None

    playthrough = re.search(r'Playthrough:(.*?)(?:\n\n[A-Z]|\Z)', text, re.DOTALL)
    if not playthrough:
        return None
    return len(re.findall(r'^[1-9]\d*: \{', playthrough.group(1), re.MULTILINE)) or None


# ── Run history ───────────────────────────────────────────────────────────────

def _load_json_file(path: Path, default):
    """Read a JSON file and return its contents, or `default` on any error."""
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning(f"Could not load {path.name}: {e}")
        return default


def load_runs() -> list[dict]:
    return _load_json_file(RUNS_FILE, [])


def save_runs(runs: list[dict]) -> None:
    dropped = runs[MAX_RUNS:]
    for run in dropped:
        for seed in run.get("seeds", []):
            p = Path(seed["path"])
            if p.exists():
                p.unlink()
                log.info(f"Deleted zip from expired run {run['id']}: {p.name}")
    RUNS_FILE.write_text(
        json.dumps(runs[:MAX_RUNS], indent=2),
        encoding="utf-8",
    )


def record_run(thread_id: int, thread_name: str, version: str, zips_with_counts: list[tuple[Path, int | None]]) -> dict:
    run = {
        "id":          datetime.now().strftime("%Y%m%d_%H%M%S"),
        "timestamp":   datetime.now().isoformat(),
        "thread_id":   thread_id,
        "thread_name": thread_name,
        "version":     version,
        "seeds":       [{"path": str(p), "spheres": c} for p, c in zips_with_counts],
        "uploaded":    None,
    }
    runs = load_runs()
    runs.insert(0, run)
    save_runs(runs)
    log.info(f"Recorded run {run['id']} with {len(zips_with_counts)} seed(s).")
    return run


def mark_run_uploaded(run_id: str, zip_path: Path) -> None:
    runs = load_runs()
    for run in runs:
        if run["id"] == run_id:
            run["uploaded"] = str(zip_path)
            for seed in run.get("seeds", []):
                p = Path(seed["path"])
                if p != zip_path and p.exists():
                    p.unlink()
                    log.info(f"Deleted losing seed from run {run_id}: {p.name}")
            break
    save_runs(runs)


# ── YAML validation ───────────────────────────────────────────────────────────

HTML_TAG_RE  = re.compile(r'<[^>]+>')
CHECK_ROW_RE = re.compile(r'<tr[^>]*>.*?<td[^>]*>(.*?)</td>.*?<td[^>]*>(.*?)</td>.*?</tr>', re.DOTALL)


def check_yamls_on_server(yaml_files: dict[str, bytes]) -> dict[str, str]:
    """POST yaml files to archipelago.gg/check and return {filename: result}."""
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


# ── Thread audit ─────────────────────────────────────────────────────────────

@functools.lru_cache(maxsize=8)
def get_builtin_game_names(version_dir: Path) -> frozenset[str]:
    """Return game names baked into this Archipelago release.
    Searches all .py files one level deep in worlds/ since some worlds
    define their game name outside of __init__.py.
    Cached — a version's worlds never change once installed."""
    games: set[str] = set()
    worlds_dir = version_dir / "worlds"
    if worlds_dir.exists():
        for py_file in worlds_dir.glob("*/*.py"):
            try:
                for match in re.finditer(
                    r'game\s*(?::\s*\w+\s*)?=\s*["\']([^"\']+)["\']',
                    py_file.read_text(encoding="utf-8", errors="replace"),
                ):
                    games.add(match.group(1))
            except Exception:
                pass
    return frozenset(games)


def get_yaml_game(yaml_bytes: bytes) -> str | None:
    """Parse the game name from a YAML player file."""
    try:
        data = yaml.safe_load(yaml_bytes)
        if not isinstance(data, dict):
            return None
        game = data.get("game")
        if isinstance(game, dict):
            game = next(iter(game))  # weighted game — pick first key
        return str(game).strip() if game else None
    except Exception:
        return None


def get_yaml_requires(yaml_bytes: bytes) -> tuple[str | None, dict[str, str]]:
    """Parse the 'requires' block from a YAML player file.

    Returns (min_ap_version, {game_name: min_world_version}).
    Both values default to None / {} if absent or unparseable."""
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
    """Extract version metadata from an apworld (zip) file.

    Tries archipelago.json manifest first (Archipelago 0.7.0+), then falls back
    to grepping apworld_version = (x, y, z) from Python files inside the zip.

    Returns {"game": str|None, "world_version": str|None, "minimum_ap_version": str|None}."""
    info: dict = {"game": None, "world_version": None, "minimum_ap_version": None}
    try:
        with zipfile.ZipFile(io.BytesIO(apworld_bytes)) as zf:
            # Prefer archipelago.json manifest (0.7.0+)
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

            # Fallback: grep apworld_version = (x, y, z) from any Python file
            for entry in zf.namelist():
                if not entry.endswith(".py"):
                    continue
                try:
                    text = zf.read(entry).decode("utf-8", errors="replace")
                    m = re.search(
                        r'apworld_version\s*=\s*\(\s*(\d+)\s*,\s*(\d+)\s*,\s*(\d+)\s*\)',
                        text,
                    )
                    if m:
                        info["world_version"] = f"{m.group(1)}.{m.group(2)}.{m.group(3)}"
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
    """Return the highest minimum AP version required across all YAMLs and apworld manifests.

    apworld_infos may be pre-computed to avoid re-reading apworld bytes (e.g. in audit_thread)."""
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


async def audit_thread(thread) -> ScanResult:
    """Full silent audit: collect files + validate YAMLs against archipelago.gg.
    Only YAMLs for games baked into the latest Archipelago release are validated —
    custom-apworld games are skipped since the server won't know them.
    Caller must subtract result.reserved_bytes from _memory_in_use when done."""
    global _memory_in_use
    result = await collect_files_from_thread(thread, audit=True)

    versions      = get_installed_versions()
    builtin_games = get_builtin_game_names(get_version_dir(versions[0])) if versions else set()

    # Pre-compute apworld metadata once — reused for version checks and min-AP-version
    apworld_infos = {name: get_apworld_info(data) for name, data in result.apworld_data.items()}

    # Check requires.version: find the highest minimum AP version across YAMLs and apworld manifests
    min_ap_ver = get_min_ap_version(result.yaml_data, apworld_infos=apworld_infos)
    if min_ap_ver:
        min_ap_parsed = parse_version(min_ap_ver)
        satisfying = [v for v in versions if parse_version(v) >= min_ap_parsed]
        if not satisfying:
            latest = versions[0] if versions else "none"
            result.issues.append((
                f"ap_version_too_old:{min_ap_ver}",
                f"⚠️ Your YAMLs require Archipelago `{min_ap_ver}` or newer, "
                f"but the latest installed version is `{latest}`.",
            ))

    # Pre-normalise apworld stems once — reused for both yaml-matching and version checks below
    apworld_stems_norm = {_norm(apworld_stem(name)): name for name in result.apworld_data}

    # Parse each YAML's game name once — reused in both the apworld-matching and validation loops
    yaml_games_by_name = {name: get_yaml_game(data) for name, data in result.yaml_data.items()}

    # Check for apworlds with no matching yaml (runs even when yaml_data is empty)
    yaml_games_normalised = {_norm(game or "") for game in yaml_games_by_name.values()}
    for norm_stem, apworld_name in apworld_stems_norm.items():
        has_yaml = any(
            norm_stem in game_norm or game_norm in norm_stem
            for game_norm in yaml_games_normalised
            if game_norm
        )
        if not has_yaml:
            uploader = result.apworld_uploaders.get(apworld_name)
            mention  = uploader.mention if uploader else ""
            result.issues.append((
                f"missing_yaml:{apworld_name}",
                f"{mention} ⚠️ **{apworld_name}**: apworld provided but no matching YAML found — please post a YAML for this game.",
            ))

    if result.yaml_data:
        yamls_to_validate = {}
        for name, data in result.yaml_data.items():
            game = yaml_games_by_name[name]
            if builtin_games and game not in builtin_games:
                # Custom game — check an apworld was provided
                norm_game   = _norm(game or "")
                has_apworld = any(
                    ns in norm_game or norm_game in ns
                    for ns in apworld_stems_norm
                )
                if not has_apworld:
                    uploader = result.yaml_uploaders.get(name)
                    mention  = uploader.mention if uploader else ""
                    result.issues.append((
                        f"missing_apworld:{name}",
                        f"{mention} ⚠️ **{name}**: game \"{game}\" is not a built-in world — please provide a `.apworld` file for it.",
                    ))
            else:
                yamls_to_validate[name] = data

        # Check requires.game: verify the custom apworld version meets each YAML's minimum
        for yaml_name, yaml_bytes in result.yaml_data.items():
            _, game_reqs = get_yaml_requires(yaml_bytes)
            for req_game, req_ver in game_reqs.items():
                # Skip builtin games — they have no separate apworld to version-check
                if builtin_games and req_game in builtin_games:
                    continue
                norm_req = _norm(req_game)
                matching_stem = next(
                    (s for s in apworld_stems_norm if s in norm_req or norm_req in s),
                    None,
                )
                if matching_stem is None:
                    continue  # no apworld present — covered by missing_apworld check
                apworld_name = apworld_stems_norm[matching_stem]
                info = apworld_infos[apworld_name]
                if info["world_version"] and req_ver:
                    if parse_version(info["world_version"]) < parse_version(req_ver):
                        uploader = result.yaml_uploaders.get(yaml_name)
                        mention  = uploader.mention if uploader else ""
                        result.issues.append((
                            f"apworld_version_too_old:{yaml_name}:{req_game}",
                            f"{mention} ⚠️ **{yaml_name}**: requires `{req_game}` version "
                            f"`{req_ver}` or newer, but the provided `.apworld` is version "
                            f"`{info['world_version']}`.",
                        ))

        if yamls_to_validate:
            loop = asyncio.get_running_loop()
            try:
                validation = await loop.run_in_executor(None, check_yamls_on_server, yamls_to_validate)
                for filename, verdict in validation.items():
                    if verdict != "Valid":
                        uploader = result.yaml_uploaders.get(filename)
                        mention  = uploader.mention if uploader else ""
                        result.issues.append((
                            f"yaml_invalid:{filename}",
                            f"{mention} ⚠️ **{filename}**: {html.unescape(verdict)}",
                        ))
            except Exception as e:
                result.issues.append((
                    "validation_unreachable",
                    f"⚠️ Could not reach archipelago.gg/check: `{e}`",
                ))

    return result


# ── Monitor helpers ───────────────────────────────────────────────────────────

_MENTION_RE = re.compile(r'<@!?\d+>\s*')

def format_resolved(content: str) -> str:
    """Format a warning message as resolved: strip mention and ⚠️, wrap in strikethrough."""
    clean = _MENTION_RE.sub('', content)
    clean = clean.replace('⚠️ ', '', 1)
    return f"✅ ~~{clean.strip()}~~"


def is_monitored(channel) -> bool:
    return isinstance(channel, discord.Thread) and str(channel.id) in _monitors


def load_monitors() -> dict:
    return _load_json_file(MONITORS_FILE, {})


def save_monitors() -> None:
    MONITORS_FILE.write_text(json.dumps(_monitors, indent=2), encoding="utf-8")


def unregister_monitor(thread_id: int) -> None:
    if str(thread_id) in _monitors:
        del _monitors[str(thread_id)]
        save_monitors()
        log.info(f"Stopped monitoring thread {thread_id}.")


async def check_monitored_thread(thread: discord.Thread) -> None:
    """Run a full audit and post any issues not previously reported.

    New issues are posted as individual messages so each can be edited in-place
    when resolved. Resolved issues get their warning message edited to strikethrough.

    If a check is already in progress for this thread, mark it as pending and return.
    The running check will re-run once after it finishes, picking up any messages
    that arrived while it was scanning."""
    lock = get_monitor_lock(thread.id)
    if lock.locked():
        log.info(f"[monitor #{thread.name}] Scan already in progress — queuing re-check.")
        _monitor_pending.add(thread.id)
        return
    async with lock:
        while True:
            _monitor_pending.discard(thread.id)
            log.info(f"[monitor #{thread.name}] Starting scan.")
            global _memory_in_use
            result = await audit_thread(thread)
            try:
                current     = {key: msg for key, msg in result.issues}
                entry       = _monitors.setdefault(str(thread.id), {})
                known       = set(entry.get("known_issue_keys", []))
                warning_ids = entry.get("warning_messages", {})  # issue_key → message_id

                new_keys      = [key for key in current if key not in known]
                resolved_keys = known - current.keys()

                for key in new_keys:
                    sent = await thread.send(current[key])
                    warning_ids[key] = sent.id
                if new_keys:
                    log.info(f"[monitor #{thread.name}] {len(new_keys)} new issue(s) found — posting.")

                for key in resolved_keys:
                    msg_id = warning_ids.pop(key, None)
                    if msg_id:
                        try:
                            warning_msg = await thread.fetch_message(msg_id)
                            await warning_msg.edit(content=format_resolved(warning_msg.content))
                        except Exception:
                            pass  # message was deleted or no longer accessible
                if resolved_keys:
                    log.info(f"[monitor #{thread.name}] {len(resolved_keys)} issue(s) resolved — editing warnings.")

                if not new_keys and not resolved_keys:
                    log.info(f"[monitor #{thread.name}] Scan complete — no changes.")

                entry["known_issue_keys"] = list(current.keys())
                entry["warning_messages"] = warning_ids
                if new_keys or resolved_keys:
                    save_monitors()
            finally:
                _memory_in_use -= result.reserved_bytes
            if thread.id not in _monitor_pending:
                break
            log.info(f"[monitor #{thread.name}] Re-check was queued during scan — running again.")


# ── Schedule helpers ─────────────────────────────────────────────────────────

def load_scheduled() -> list:
    return _load_json_file(SCHEDULED_FILE, [])


def save_scheduled() -> None:
    SCHEDULED_FILE.write_text(json.dumps(_scheduled, indent=2), encoding="utf-8")


def get_scheduled_job(thread_id: int) -> dict | None:
    return next((j for j in _scheduled if j["thread_id"] == thread_id), None)


def remove_scheduled_job(thread_id: int) -> bool:
    before = len(_scheduled)
    _scheduled[:] = [j for j in _scheduled if j["thread_id"] != thread_id]
    if len(_scheduled) < before:
        save_scheduled()
        return True
    return False


def parse_schedule_time(time_str: str, tz_name: str | None = None) -> datetime | None:
    """Parse a natural-language time string and return a UTC-aware datetime, or None if unparseable."""
    settings = {
        "TIMEZONE":              tz_name or TIMEZONE,
        "RETURN_AS_TIMEZONE_AWARE": True,
        "PREFER_DATES_FROM":    "future",
        "TO_TIMEZONE":          "UTC",
    }
    try:
        dt = dateparser.parse(time_str, settings=settings)
        return dt if dt and dt > datetime.now(timezone.utc) else None
    except Exception:
        return None


async def _execute_generation(
    thread: discord.Thread,
    opts: dict,
    version_dir: Path,
    yaml_data: dict[str, bytes],
    apworld_data: dict[str, bytes],
    yaml_uploaders: dict,
    count: int,
    dry_run: bool = False,
) -> None:
    """Run generation(s), record, and post results to the thread.
    Enforces requires.version. Memory management is the caller's responsibility."""
    version = version_dir.name
    loop    = asyncio.get_running_loop()

    # Enforce requires.version: abort if selected version is older than any YAML or apworld demands
    min_ap_ver = get_min_ap_version(yaml_data, apworld_data)
    if min_ap_ver and parse_version(version) < parse_version(min_ap_ver):
        await thread.send(
            f"❌ Version `{version}` is too old — your YAMLs require Archipelago "
            f"`{min_ap_ver}` or newer. Please install a newer version or remove the "
            f"`requires` field from the relevant YAML(s)."
        )
        return

    if count == 1:
        success, error, new_zips = await run_generation(opts, version_dir, yaml_data, apworld_data)
        if not success:
            if isinstance(error, tuple):
                msg, bad_files = error
                mentions = " ".join(yaml_uploaders[f].mention for f in bad_files if f in yaml_uploaders)
                await thread.send(f"❌ Generation failed{' ' + mentions if mentions else ''}:\n```\n{msg}\n```")
            else:
                await thread.send(f"❌ Generation failed:\n```\n{error}\n```")
            return
        if not new_zips:
            await thread.send("✅ Generator finished, but no new zip found in output/. Check the logs.")
            return
        zips_with_counts = [(p, parse_sphere_count(p)) for p in new_zips]
        run = record_run(thread.id, thread.name, version, zips_with_counts)
        if dry_run:
            await thread.send("✅ Dry run complete!")
            return
        unregister_monitor(thread.id)
        await thread.send("✅ Generation complete! Uploading to archipelago.gg…")
        try:
            room_url = await loop.run_in_executor(None, upload_and_create_room, new_zips[0])
            mark_run_uploaded(run["id"], new_zips[0])
            await thread.send(f"🎉 Room is ready! <{room_url}>")
        except Exception as e:
            await thread.send(f"⚠️ Generation succeeded but upload failed: `{e}`\nThe zip is saved at: `{new_zips[0]}`")
    else:
        succeeded, new_zips, errors = await run_generations(count, opts, version_dir, yaml_data, apworld_data)
        if not new_zips:
            error_detail = "\n".join(str(e) for e in errors if e) if errors else "Check the logs."
            await thread.send(f"❌ All {count} generations failed:\n```\n{error_detail}\n```")
            return
        zips_with_counts = [(p, parse_sphere_count(p)) for p in new_zips]
        run = record_run(thread.id, thread.name, version, zips_with_counts)
        lines = [f"🎲 `{p.name}` — {f'{c} spheres' if c is not None else 'no spoiler'}" for p, c in zips_with_counts]
        failed_line = f"\n❌ {count - succeeded} seed(s) failed." if succeeded < count else ""
        await thread.send(f"✅ {succeeded}/{count} seeds generated:\n" + "\n".join(lines) + failed_line)
        if dry_run:
            return
        unregister_monitor(thread.id)
        view = SeedSelectView(zips_with_counts, thread, run["id"])
        await thread.send("Pick a seed to upload:", view=view)


async def _run_scheduled_generate(thread: discord.Thread, job: dict) -> None:
    """Execute a scheduled generation job — mirrors /generate but posts directly to the thread."""
    global _memory_in_use
    reserved_bytes = 0
    try:
        versions = get_installed_versions()
        if not versions:
            await thread.send("⚠️ Scheduled generation failed: no Archipelago versions installed.")
            return

        version = job.get("version") or versions[0]
        version_dir = get_version_dir(version)
        if not version_dir.exists():
            await thread.send(f"⚠️ Scheduled generation failed: version `{version}` is no longer installed.")
            return

        count = job.get("count", 1)
        await thread.send(f"⏰ Running scheduled generation with Archipelago `{version}`…")

        scan = await collect_files_from_thread(thread)
        reserved_bytes = scan.reserved_bytes

        if not scan.yaml_data:
            if not scan.had_error:
                await thread.send("⚠️ Scheduled generation: no YAML files found in this thread.")
            return

        seed_label = "seed" if count == 1 else f"{count} seeds"
        await thread.send(f"⚙️ Found **{len(scan.yaml_data)}** yaml(s) and **{len(scan.apworld_data)}** apworld(s). Generating {seed_label}… this may take a minute.")

        await _execute_generation(thread, job.get("opts", {}), version_dir, scan.yaml_data, scan.apworld_data, scan.yaml_uploaders, count)

    except Exception:
        log.exception(f"Error in scheduled generation for thread {thread.id}")
        try:
            await thread.send("⚠️ Scheduled generation encountered an unexpected error. Check the bot logs.")
        except Exception:
            pass
    finally:
        _memory_in_use -= reserved_bytes


async def check_due_schedules() -> None:
    """Fire any scheduled jobs whose time has passed."""
    if not _scheduled:
        return
    now = datetime.now(timezone.utc)
    due = [j for j in _scheduled if datetime.fromisoformat(j["scheduled_utc"]) <= now]
    for job in due:
        remove_scheduled_job(job["thread_id"])
        log.info(f"Firing scheduled generation for thread {job['thread_id']} ({job['thread_name']})")
        try:
            thread = await client.fetch_channel(job["thread_id"])
            await _run_scheduled_generate(thread, job)
        except Exception:
            log.exception(f"Failed to fire scheduled job for thread {job['thread_id']}")


# ── Gather zip builder ───────────────────────────────────────────────────────

def _build_gather_zip(yaml_data: dict[str, bytes], apworld_data: dict[str, bytes]) -> bytes:
    """Build an in-memory zip with Players/ and custom_worlds/ subdirectories."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in yaml_data.items():
            zf.writestr(f"Players/{name}", data)
        for name, data in apworld_data.items():
            zf.writestr(f"custom_worlds/{name}", data)
    return buf.getvalue()


# ── Room upload ───────────────────────────────────────────────────────────────

def upload_and_create_room(zip_path: Path) -> str:
    log.info(f"Uploading {zip_path.name} to archipelago.gg...")
    session = requests.Session()
    session.headers.update({"User-Agent": "ArchipelagoDiscordBot/1.0"})

    with open(zip_path, "rb") as f:
        upload = session.post(
            f"{ARCHIPELAGO_BASE}/uploads",
            files={"file": (zip_path.name, f, "application/zip")},
            allow_redirects=True,
            timeout=120,
        )
    upload.raise_for_status()

    if "/seed/" not in upload.url:
        raise RuntimeError(f"Unexpected redirect after upload: {upload.url}")
    seed_id = upload.url.rstrip("/").split("/seed/")[-1]

    room = session.get(f"{ARCHIPELAGO_BASE}/new_room/{seed_id}", allow_redirects=True, timeout=30)
    room.raise_for_status()

    if "/room/" not in room.url:
        raise RuntimeError(f"Unexpected redirect after room creation: {room.url}")
    log.info(f"Room created: {room.url}")
    return room.url


# ── Discord UI views ─────────────────────────────────────────────────────────

class SeedSelect(discord.ui.Select):
    """Dropdown that lets the user pick one seed to upload."""

    def __init__(self, zips_with_counts: list[tuple[Path, int | None]], thread, run_id: str):
        self.thread  = thread
        self.run_id  = run_id
        options = [
            discord.SelectOption(
                label=p.name[:100],
                description=f"{c} spheres" if c is not None else "sphere count unavailable",
                value=str(p),
            )
            for p, c in zips_with_counts
        ]
        super().__init__(placeholder="Pick a seed to upload…", options=options)

    async def callback(self, interaction: discord.Interaction):
        log.info(f"Seed select callback invoked by {interaction.user}")
        await interaction.response.defer()
        for item in self.view.children:
            item.disabled = True
        await interaction.message.edit(view=self.view)
        self.view.stop()

        zip_path = Path(self.values[0])
        await self.thread.send("⬆️ Uploading to archipelago.gg…")
        try:
            loop = asyncio.get_running_loop()
            room_url = await loop.run_in_executor(None, upload_and_create_room, zip_path)
            mark_run_uploaded(self.run_id, zip_path)
            await self.thread.send(f"🎉 Room is ready! <{room_url}>")
        except Exception as e:
            log.exception("Upload failed")
            await self.thread.send(f"⚠️ Upload failed: `{e}`")


class SeedSelectView(discord.ui.View):
    """View wrapper for SeedSelect."""

    def __init__(self, zips_with_counts: list[tuple[Path, int | None]], thread, run_id: str):
        super().__init__(timeout=300)
        self.add_item(SeedSelect(zips_with_counts, thread, run_id))

    async def on_error(self, interaction: discord.Interaction, error: Exception, item: discord.ui.Item):
        log.exception(f"Error in SeedSelectView item {item}: {error}")
        if not interaction.response.is_done():
            await interaction.response.send_message("⚠️ Something went wrong.", ephemeral=True)


# ── Slash commands ────────────────────────────────────────────────────────────

def is_thread(interaction: discord.Interaction) -> bool:
    return isinstance(interaction.channel, discord.Thread)


async def _get_thread_min_ap_version(thread: discord.Thread) -> str | None:
    """Scan YAML and apworld attachments in the thread and return the highest minimum AP version found,
    considering both YAML requires.version and apworld archipelago.json minimum_ap_version."""
    yaml_data:    dict[str, bytes] = {}
    apworld_data: dict[str, bytes] = {}
    async for message in thread.history(limit=500, oldest_first=True):
        if message.author.bot:
            continue
        for attachment in message.attachments:
            name = attachment.filename.lower()
            try:
                if name.endswith((".yaml", ".yml")) and attachment.size <= MAX_YAML_BYTES:
                    yaml_data[attachment.filename] = normalise_yaml_bytes(await attachment.read())
                elif name.endswith(".apworld") and attachment.size <= MAX_APWORLD_BYTES:
                    apworld_data[attachment.filename] = await attachment.read()
            except Exception:
                pass
    return get_min_ap_version(yaml_data, apworld_data)


async def time_autocomplete(interaction: discord.Interaction, current: str):
    now = datetime.now(timezone.utc)
    suggestions = [
        "in 30 minutes", "in 1 hour", "in 2 hours", "in 4 hours", "in 8 hours",
        "tomorrow 8pm",
    ] + [
        f"{(now + timedelta(days=i)).strftime('%A').lower()} 8pm"
        for i in range(1, 8)
    ]
    # Deduplicate (e.g. if today is Friday, "friday 8pm" might appear twice)
    seen: set[str] = set()
    unique = [s for s in suggestions if not (s in seen or seen.add(s))]
    if current:
        unique = [s for s in unique if current.lower() in s.lower()]
    return [app_commands.Choice(name=s, value=s) for s in unique[:25]]


async def timezone_autocomplete(interaction: discord.Interaction, current: str):
    matches = [tz for tz in COMMON_TIMEZONES if current.lower() in tz.lower()] if current else COMMON_TIMEZONES
    return [app_commands.Choice(name=tz, value=tz) for tz in matches[:25]]


async def version_autocomplete(interaction: discord.Interaction, current: str):
    versions = get_installed_versions()
    if isinstance(interaction.channel, discord.Thread):
        try:
            min_ap_ver = await _get_thread_min_ap_version(interaction.channel)
            if min_ap_ver:
                min_ap_parsed = parse_version(min_ap_ver)
                versions = [v for v in versions if parse_version(v) >= min_ap_parsed]
        except Exception:
            pass  # fall back to showing all versions
    return [
        app_commands.Choice(name=v, value=v)
        for v in versions
        if current.lower() in v.lower()
    ]


async def run_autocomplete(interaction: discord.Interaction, current: str):
    runs = load_runs()
    choices = []
    for run in runs:
        uploaded  = "✅ " if run.get("uploaded") else ""
        count     = len(run.get("seeds", []))
        thread    = run.get("thread_name", "unknown")
        time      = datetime.fromisoformat(run["timestamp"]).strftime("%m-%d %H:%M")
        label     = f"{uploaded}{count} seed{'s' if count != 1 else ''} — #{thread} — {time}"
        if current.lower() in label.lower():
            choices.append(app_commands.Choice(name=label[:100], value=run["id"]))
        if len(choices) == 25:
            break
    return choices


async def seed_autocomplete(interaction: discord.Interaction, current: str):
    run_id = interaction.namespace.run
    if not run_id:
        return []
    runs = load_runs()
    run  = next((r for r in runs if r["id"] == run_id), None)
    if not run:
        return []
    choices = []
    for seed in run.get("seeds", []):
        path = seed["path"]
        if not Path(path).exists():
            continue
        name     = Path(path).name
        spheres  = seed.get("spheres")
        suffix   = f" — {spheres} spheres" if spheres is not None else ""
        label    = f"{name}{suffix}"
        if current.lower() in label.lower():
            choices.append(app_commands.Choice(name=label[:100], value=path))
    return choices


@tree.command(name="status", description="List files found in this thread and validate YAMLs against archipelago.gg")
async def status(interaction: discord.Interaction):
    if not is_thread(interaction):
        await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
        return

    log.info(f"/status invoked by {interaction.user} in #{interaction.channel.name}")
    await interaction.response.send_message("🔍 Scanning and validating thread…")
    thread = interaction.channel

    result = await audit_thread(thread)
    try:
        yaml_list    = ", ".join(f"`{f}`" for f in result.yaml_data)    or "none"
        apworld_list = ", ".join(f"`{f}`" for f in result.apworld_data) or "none"
        msg = (
            f"**Files found in this thread:**\n"
            f"📄 **YAMLs ({len(result.yaml_data)}):** {yaml_list}\n"
            f"🌍 **APworlds ({len(result.apworld_data)}):** {apworld_list}"
        )
        if result.issues:
            issue_lines = "\n".join(display for _, display in result.issues)
            msg += f"\n\n⚠️ **Issues:**\n{issue_lines}"
        else:
            msg += "\n\n✅ No issues found."
        await thread.send(msg)
    finally:
        global _memory_in_use
        _memory_in_use -= result.reserved_bytes


@tree.command(name="output", description="Attach a previously generated seed to this thread")
@app_commands.describe(run="Generation run to pick from", seed="Seed to attach")
@app_commands.autocomplete(run=run_autocomplete, seed=seed_autocomplete)
async def output(interaction: discord.Interaction, run: str, seed: str):
    if not is_thread(interaction):
        await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
        return

    log.info(f"/output invoked by {interaction.user} in #{interaction.channel.name} (run={run})")

    runs     = load_runs()
    run_data = next((r for r in runs if r["id"] == run), None)
    if not run_data:
        await interaction.response.send_message("⚠️ Run not found.", ephemeral=True)
        return

    zip_path = Path(seed)
    if not zip_path.exists():
        await interaction.response.send_message("⚠️ Zip file no longer exists on disk.", ephemeral=True)
        return

    await interaction.response.send_message(f"📦 Attaching `{zip_path.name}`…")
    try:
        await interaction.channel.send(file=discord.File(zip_path))
    except discord.HTTPException as e:
        if e.status == 413:
            await interaction.channel.send("⚠️ File is too large to attach for this server.")
        else:
            await interaction.channel.send(f"⚠️ Failed to attach file: `{e}`")


@tree.command(name="gather", description="Collect all YAMLs and apworlds from this thread and attach them as a zip")
async def gather(interaction: discord.Interaction):
    if not is_thread(interaction):
        await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
        return

    log.info(f"/gather invoked by {interaction.user} in #{interaction.channel.name}")
    await interaction.response.send_message("📦 Gathering files…")
    thread = interaction.channel

    scan = await collect_files_from_thread(thread, audit=True)
    try:
        if not scan.yaml_data and not scan.apworld_data:
            msg = "⚠️ No files found in this thread."
            if scan.issues:
                msg += " Run `/status` for details on skipped files."
            await thread.send(msg)
            return

        summary = f"📄 {len(scan.yaml_data)} YAML(s)  🌍 {len(scan.apworld_data)} apworld(s)"
        if scan.issues:
            summary += " — ⚠️ some files were skipped, run `/status` for details."

        safe_name = re.sub(r'[^\w\-. ]', '_', thread.name)[:50].strip()

        # Try to send everything in one zip
        combined = _build_gather_zip(scan.yaml_data, scan.apworld_data)
        try:
            await thread.send(summary, file=discord.File(io.BytesIO(combined), filename=f"{safe_name}.zip"))
            return
        except discord.HTTPException as e:
            if e.status != 413:
                raise

        # Too large — split into separate players and custom_worlds zips
        await thread.send(f"{summary}\n📦 Combined zip too large — sending as separate files:")

        async def send_or_split(zip_name: str, yaml_d: dict[str, bytes], apworld_d: dict[str, bytes]) -> None:
            """Try sending as a zip; if still too large, fall back to individual files."""
            data = _build_gather_zip(yaml_d, apworld_d)
            try:
                await thread.send(file=discord.File(io.BytesIO(data), filename=zip_name))
                return
            except discord.HTTPException as e:
                if e.status != 413:
                    raise
            # Zip still too large — send each file individually
            individual_files = {**yaml_d, **apworld_d}
            await thread.send(f"📦 `{zip_name}` too large — sending {len(individual_files)} file(s) individually:")
            for fname, fdata in individual_files.items():
                try:
                    await thread.send(file=discord.File(io.BytesIO(fdata), filename=fname))
                except discord.HTTPException as e:
                    if e.status == 413:
                        await thread.send(f"⚠️ `{fname}` is too large to attach.")
                    else:
                        raise

        if scan.yaml_data:
            await send_or_split("players.zip", scan.yaml_data, {})
        if scan.apworld_data:
            await send_or_split("custom_worlds.zip", {}, scan.apworld_data)
    finally:
        global _memory_in_use
        _memory_in_use -= scan.reserved_bytes


@tree.command(name="schedule", description="Schedule a generation for this thread — uses whatever files are posted when the time comes")
@app_commands.describe(
    time="When to generate — e.g. 'friday 8pm', 'in 2 hours', '2026-05-15 20:00'",
    timezone="Your timezone, e.g. 'Europe/London'. Overrides server default. Current default: " + TIMEZONE,
    cancel="Cancel the scheduled generation for this thread",
    release="When players can release remaining items (default: auto)",
    collect="When players can collect remaining items (default: auto)",
    remaining="When players can query remaining items (default: goal)",
    spoiler="Spoiler log detail level (default: full)",
    race="Enable race mode",
    password="Server join password, only visible to you (optional)",
    server_password="Admin password, overrides default, only visible to you (optional)",
    version="Archipelago version to generate with (default: latest)",
    count=f"Number of seeds to generate (default: 1, max: {MAX_SEEDS_PER_RUN})",
)
@app_commands.choices(
    cancel=[app_commands.Choice(name="yes", value="yes")],
    release=[app_commands.Choice(name=m, value=m) for m in VALID_RELEASE_COLLECT_MODES],
    collect=[app_commands.Choice(name=m, value=m) for m in VALID_RELEASE_COLLECT_MODES],
    remaining=[app_commands.Choice(name=m, value=m) for m in VALID_REMAINING_MODES],
    spoiler=[app_commands.Choice(name=name, value=str(val)) for name, val in SPOILER_MODES.items()],
    race=[app_commands.Choice(name="yes", value="yes")],
)
@app_commands.autocomplete(time=time_autocomplete, timezone=timezone_autocomplete, version=version_autocomplete)
async def schedule(
    interaction: discord.Interaction,
    time: str = None,
    timezone: str = None,
    cancel: str = None,
    release: app_commands.Choice[str] = None,
    collect: app_commands.Choice[str] = None,
    remaining: app_commands.Choice[str] = None,
    spoiler: app_commands.Choice[str] = None,
    race: str = None,
    password: str = None,
    server_password: str = None,
    version: str = None,
    count: int = 1,
):
    if not is_thread(interaction):
        await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
        return

    thread = interaction.channel

    if cancel == "yes":
        if remove_scheduled_job(thread.id):
            log.info(f"/schedule cancel in #{thread.name} by {interaction.user}")
            await interaction.response.send_message("🗓️ Scheduled generation cancelled.", ephemeral=True)
        else:
            await interaction.response.send_message("⚠️ No scheduled generation found for this thread.", ephemeral=True)
        return

    if not time:
        existing = get_scheduled_job(thread.id)
        if existing:
            dt = datetime.fromisoformat(existing["scheduled_utc"])
            ts = int(dt.timestamp())
            await interaction.response.send_message(
                f"🗓️ Generation scheduled for <t:{ts}:F> (<t:{ts}:R>). Use `cancel: yes` to remove it.",
                ephemeral=True,
            )
        else:
            await interaction.response.send_message(
                "⚠️ No time provided. Pass a `time` to schedule, or `cancel: yes` to cancel.",
                ephemeral=True,
            )
        return

    dt = parse_schedule_time(time, timezone)
    if not dt:
        tz_hint = f" (timezone: `{timezone}`)" if timezone else f" (server timezone: `{TIMEZONE}`)"
        await interaction.response.send_message(
            f"⚠️ Couldn't parse `{time}` as a future date/time{tz_hint}.",
            ephemeral=True,
        )
        return

    versions = get_installed_versions()
    if version and version not in versions:
        await interaction.response.send_message(f"⚠️ Version `{version}` is not installed.", ephemeral=True)
        return

    opts: dict = {"server_password": server_password or SERVER_PASSWORD}
    if release:   opts["release_mode"]   = release.value
    if collect:   opts["collect_mode"]   = collect.value
    if remaining: opts["remaining_mode"] = remaining.value
    if spoiler:   opts["spoiler"]        = int(spoiler.value)
    if race:      opts["race"]           = 1
    if password:  opts["password"]       = password

    job = {
        "thread_id":     thread.id,
        "thread_name":   thread.name,
        "scheduled_utc": dt.isoformat(),
        "version":       version,
        "count":         max(1, min(count, MAX_SEEDS_PER_RUN)),
        "opts":          opts,
    }

    # Replace any existing job for this thread
    replaced = remove_scheduled_job(thread.id)
    _scheduled.append(job)
    save_scheduled()

    ts = int(dt.timestamp())
    tz_used = timezone or TIMEZONE
    log.info(f"/schedule in #{thread.name} by {interaction.user}: {dt.isoformat()} (tz={tz_used})")
    suffix = " (replaced previous schedule)" if replaced else ""
    await interaction.response.send_message(
        f"🗓️ Generation scheduled for <t:{ts}:F> (<t:{ts}:R>){suffix}.",
        ephemeral=True,
    )


@tree.command(name="monitor", description="Start monitoring this thread for issues, or stop if already monitoring")
async def monitor(interaction: discord.Interaction):
    if not is_thread(interaction):
        await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
        return

    thread     = interaction.channel
    thread_key = str(thread.id)

    if thread_key in _monitors:
        unregister_monitor(thread.id)
        log.info(f"/monitor: stopped monitoring #{thread.name}")
        await interaction.response.send_message("🔕 Monitoring stopped for this thread.")
        return

    _monitors[thread_key] = {"known_issue_keys": []}
    save_monitors()
    log.info(f"/monitor: started monitoring #{thread.name}")
    await interaction.response.send_message("🔔 Monitoring started — I'll flag any issues as files are posted.")
    await check_monitored_thread(thread)


@tree.command(name="generate", description="Generate and host an Archipelago multiworld from this thread's files")
@app_commands.describe(
    release="When players can release remaining items from their world (default: auto)",
    collect="When players can collect remaining items into their world (default: auto)",
    remaining="When players can query remaining items via !remaining (default: goal)",
    spoiler="Spoiler log detail level (default: full)",
    race="Enable race mode",
    password="Server join password, only visible to you (optional)",
    server_password="Admin password, overrides default, only visible to you (optional)",
    version="Archipelago version to generate with (default: latest)",
    dry_run="Generate locally without uploading to archipelago.gg",
    count=f"Number of seeds to generate (default: 1, max: {MAX_SEEDS_PER_RUN})",
)
@app_commands.choices(
    release=[app_commands.Choice(name=m, value=m) for m in VALID_RELEASE_COLLECT_MODES],
    collect=[app_commands.Choice(name=m, value=m) for m in VALID_RELEASE_COLLECT_MODES],
    remaining=[app_commands.Choice(name=m, value=m) for m in VALID_REMAINING_MODES],
    spoiler=[app_commands.Choice(name=name, value=str(val)) for name, val in SPOILER_MODES.items()],
    race=[app_commands.Choice(name="yes", value="yes")],
    dry_run=[app_commands.Choice(name="yes", value="yes")],
)
@app_commands.autocomplete(version=version_autocomplete)
async def generate(
    interaction: discord.Interaction,
    release: app_commands.Choice[str] = None,
    collect: app_commands.Choice[str] = None,
    remaining: app_commands.Choice[str] = None,
    spoiler: app_commands.Choice[str] = None,
    race: str = None,
    password: str = None,
    server_password: str = None,
    version: str = None,
    dry_run: str = None,
    count: int = 1,
):
    if not is_thread(interaction):
        await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
        return

    # Defer immediately — must happen within 3 s before any I/O
    await interaction.response.defer(ephemeral=True)

    log.info(f"/generate invoked by {interaction.user} in #{interaction.channel.name} (version={version or 'latest'}, count={count}, dry_run={dry_run})")
    reserved_bytes = 0
    try:
        versions = get_installed_versions()
        if not versions:
            await interaction.followup.send("⚠️ No Archipelago versions are installed yet. Please wait for the version manager to finish.", ephemeral=True)
            return

        version     = version or versions[0]
        version_dir = get_version_dir(version)
        if not version_dir.exists():
            await interaction.followup.send(f"⚠️ Version `{version}` is not installed.", ephemeral=True)
            return

        count = max(1, min(count, MAX_SEEDS_PER_RUN))
        await interaction.followup.send(f"⏳ Starting {'generation' if count == 1 else f'{count} generations'} with Archipelago `{version}`…", ephemeral=True)
        thread = interaction.channel
        await thread.send("🔍 Scanning thread history for files...")

        try:
            scan = await collect_files_from_thread(thread)
        except discord.HTTPException as e:
            log.warning(f"Discord API error while scanning thread history: {e}")
            try:
                await thread.send(f"⚠️ Discord API error while scanning thread history: `{e}`. Please try again.")
            except discord.HTTPException:
                pass
            return
        reserved_bytes = scan.reserved_bytes
        yaml_data, apworld_data, yaml_uploaders = scan.yaml_data, scan.apworld_data, scan.yaml_uploaders
        log.info(f"Collected {len(yaml_data)} yaml(s) and {len(apworld_data)} apworld(s) from thread.")
        if not yaml_data:
            if not scan.had_error:
                await thread.send("⚠️ No YAML files found in this thread — nothing to generate.")
            return

        seed_label = "seed" if count == 1 else f"{count} seeds"
        await thread.send(f"⚙️ Found **{len(yaml_data)}** yaml(s) and **{len(apworld_data)}** apworld(s). Generating {seed_label}… this may take a minute.")

        gen_opts: dict = {"server_password": server_password or SERVER_PASSWORD}
        if release:   gen_opts["release_mode"]   = release.value
        if collect:   gen_opts["collect_mode"]   = collect.value
        if remaining: gen_opts["remaining_mode"] = remaining.value
        if spoiler:   gen_opts["spoiler"]        = int(spoiler.value)
        if race:      gen_opts["race"]           = 1
        if password:  gen_opts["password"]       = password

        await _execute_generation(thread, gen_opts, version_dir, yaml_data, apworld_data, yaml_uploaders, count, dry_run=dry_run == "yes")

    finally:
        global _memory_in_use
        _memory_in_use -= reserved_bytes


# ── Startup & events ─────────────────────────────────────────────────────────

async def _schedule_checker_loop() -> None:
    await client.wait_until_ready()
    while not client.is_closed():
        try:
            await check_due_schedules()
        except Exception:
            log.exception("Error in schedule checker loop")
        await asyncio.sleep(30)


@client.event
async def on_ready():
    global _checker_task
    await tree.sync()
    _monitors.clear()
    _monitors.update(load_monitors())
    _scheduled[:] = load_scheduled()
    versions = get_installed_versions()
    log.info(f"Logged in as {client.user} — slash commands synced.")
    log.info(f"Installed Archipelago versions: {versions if versions else 'none yet'}")
    log.info(f"Monitoring {len(_monitors)} thread(s). {len(_scheduled)} generation(s) scheduled.")
    log.info(f"Server timezone: {TIMEZONE}")
    # Cancel any previous loop (on_ready fires again on every Discord reconnect).
    # Await it so the old loop is fully stopped before the new one starts.
    if _checker_task and not _checker_task.done():
        _checker_task.cancel()
        try:
            await _checker_task
        except asyncio.CancelledError:
            pass
    _checker_task = asyncio.create_task(_schedule_checker_loop())


@client.event
async def on_message(message: discord.Message):
    if message.author == client.user:
        return
    if not is_monitored(message.channel):
        return
    try:
        await check_monitored_thread(message.channel)
    except Exception:
        log.exception(f"Error in on_message monitor check for thread {message.channel.id}")


@client.event
async def on_message_delete(message: discord.Message):
    if not is_monitored(message.channel):
        return
    try:
        await check_monitored_thread(message.channel)
    except Exception:
        log.exception(f"Error in on_message_delete monitor check for thread {message.channel.id}")


@client.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if after.author == client.user:
        return
    # Discord fires on_message_edit a second time when it resolves URL embeds.
    # If content and attachments are unchanged, it's an embed-only update — skip it.
    if before.content == after.content and before.attachments == after.attachments:
        return
    if not is_monitored(after.channel):
        return
    try:
        await check_monitored_thread(after.channel)
    except Exception:
        log.exception(f"Error in on_message_edit monitor check for thread {after.channel.id}")


client.run(BOT_TOKEN, log_handler=None)
