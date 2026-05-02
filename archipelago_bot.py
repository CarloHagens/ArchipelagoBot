import asyncio
import os
import re
import subprocess
import sys
from pathlib import Path

import discord
import requests
import yaml
from discord import app_commands

# ── Configuration ─────────────────────────────────────────────────────────────

BOT_TOKEN       = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
SERVER_PASSWORD = os.environ.get("SERVER_PASSWORD", "archipelago")

ARCHIPELAGO_DIR = Path("/archipelago")
PLAYERS_DIR     = ARCHIPELAGO_DIR / "Players"
WORLDS_DIR      = ARCHIPELAGO_DIR / "custom_worlds"
OUTPUT_DIR      = ARCHIPELAGO_DIR / "output"
HOST_YAML_PATH  = ARCHIPELAGO_DIR / "host.yaml"
LOGS_DIR        = ARCHIPELAGO_DIR / "logs"

ARCHIPELAGO_BASE = "https://archipelago.gg"

GITHUB_RELEASE_RE = re.compile(
    r'https://github\.com/([^/]+)/([^/]+)/releases/tag/([^\s>]+)'
)

VALID_RELEASE_COLLECT_MODES = ["disabled", "enabled", "auto", "auto-enabled", "goal"]
VALID_REMAINING_MODES       = ["disabled", "enabled", "goal"]
VALID_SPOILER_MODES         = ["0", "1", "2", "3"]

NUMBERED_LINE_PREFIXES = tuple(f"{i}." for i in range(1, 20))
UTF8_BOM               = b'\xef\xbb\xbf'

SERVER_KEYS   = {"release_mode", "collect_mode", "remaining_mode", "password", "server_password"}
GENERATOR_KEYS = {"race", "spoiler"}

DEFAULT_HOST_YAML = {
    "server_options": {
        "release_mode": "auto", "collect_mode": "auto",
        "remaining_mode": "goal", "password": None, "server_password": None,
    },
    "generator": {"race": 0, "spoiler": 3},
}

intents = discord.Intents.default()
intents.message_content = True
intents.messages = True
intents.guilds = True

client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)


# ── File helpers ──────────────────────────────────────────────────────────────

def normalise_yaml_bytes(raw: bytes) -> bytes:
    if raw.startswith(UTF8_BOM):
        raw = raw[len(UTF8_BOM):]
    return raw.replace(b'\r\n', b'\n')


def apworld_stem(filename: str) -> str:
    return Path(filename).stem.lower()


def zip_snapshot() -> set:
    return set(OUTPUT_DIR.glob("AP_*.zip")) if OUTPUT_DIR.exists() else set()


# ── GitHub apworld download ───────────────────────────────────────────────────

def download_apworld_from_github(owner: str, repo: str, tag: str) -> Path:
    api_url = f"https://api.github.com/repos/{owner}/{repo}/releases/tags/{tag}"
    resp = requests.get(api_url, timeout=30, headers={"Accept": "application/vnd.github+json"})
    resp.raise_for_status()

    apworld_assets = [a for a in resp.json().get("assets", []) if a["name"].endswith(".apworld")]
    if not apworld_assets:
        raise RuntimeError(f"No .apworld asset found in {owner}/{repo} release {tag}")

    asset = apworld_assets[0]
    download = requests.get(asset["browser_download_url"], timeout=120)
    download.raise_for_status()

    dest = WORLDS_DIR / asset["name"]
    dest.write_bytes(download.content)
    return dest


# ── Thread file collection ────────────────────────────────────────────────────

async def save_yaml_attachment(attachment, thread_dir: Path) -> Path:
    dest = thread_dir / attachment.filename
    await attachment.save(dest)
    dest.write_bytes(normalise_yaml_bytes(dest.read_bytes()))
    return dest


async def save_apworld_attachment(attachment, dest_dir: Path, thread, seen_stems: set) -> Path | None:
    stem = apworld_stem(attachment.filename)
    if stem in seen_stems:
        await thread.send(f"⚠️ Duplicate apworld for **{stem}**: attached file conflicts with a previously seen apworld. Please post only one.")
        return None
    dest = dest_dir / attachment.filename
    await attachment.save(dest)
    seen_stems.add(stem)
    return dest


async def handle_github_link(match, thread, seen_stems: set, seen_repos: dict) -> Path | None | bool:
    owner, repo, tag = match.group(1), match.group(2), match.group(3)
    repo_key  = f"{owner}/{repo}".lower()
    tag_lower = tag.lower()

    if repo_key in seen_repos:
        if seen_repos[repo_key] == tag_lower:
            return None  # same link seen twice, skip silently
        await thread.send(
            f"⚠️ Multiple releases linked for **{owner}/{repo}**: "
            f"`{seen_repos[repo_key]}` and `{tag}`. Please link only one release."
        )
        return False

    seen_repos[repo_key] = tag_lower

    try:
        loop = asyncio.get_running_loop()
        dest = await loop.run_in_executor(None, download_apworld_from_github, owner, repo, tag)
    except Exception as e:
        await thread.send(f"⚠️ Could not download apworld from {match.group(0)}: `{e}`")
        return None

    stem = apworld_stem(dest.name)
    if stem in seen_stems:
        await thread.send(f"⚠️ Duplicate apworld for **{stem}**: GitHub link conflicts with an already posted apworld. Please post only one.")
        return False

    seen_stems.add(stem)
    return dest


async def collect_files_from_thread(thread):
    PLAYERS_DIR.mkdir(parents=True, exist_ok=True)
    WORLDS_DIR.mkdir(parents=True, exist_ok=True)

    yaml_files, apworld_files = [], []
    yaml_uploaders: dict  = {}
    seen_apworld_stems: set = set()
    seen_repos: dict = {}

    async for message in thread.history(limit=500, oldest_first=True):
        for attachment in message.attachments:
            name = attachment.filename.lower()
            if name.endswith(".yaml") or name.endswith(".yml"):
                dest = await save_yaml_attachment(attachment, PLAYERS_DIR)
                yaml_files.append(dest)
                yaml_uploaders[attachment.filename] = message.author
            elif name.endswith(".apworld"):
                dest = await save_apworld_attachment(attachment, WORLDS_DIR, thread, seen_apworld_stems)
                if dest is None:
                    return [], [], {}
                apworld_files.append(dest)

        for match in GITHUB_RELEASE_RE.finditer(message.content or ""):
            result = await handle_github_link(match, thread, seen_apworld_stems, seen_repos)
            if result is False:
                return [], [], {}
            if isinstance(result, Path):
                apworld_files.append(result)

    return yaml_files, apworld_files, yaml_uploaders


# ── Host YAML management ──────────────────────────────────────────────────────

def load_host_yaml() -> dict:
    if HOST_YAML_PATH.exists():
        return yaml.safe_load(HOST_YAML_PATH.read_text(encoding="utf-8"))
    return DEFAULT_HOST_YAML.copy()


def save_host_yaml(config: dict) -> None:
    HOST_YAML_PATH.write_text(
        yaml.dump(config, default_flow_style=False, allow_unicode=True),
        encoding="utf-8",
    )


def apply_host_yaml_options(opts: dict) -> dict:
    config = load_host_yaml()
    originals = {}
    for key, value in opts.items():
        if key in SERVER_KEYS:
            originals[("server_options", key)] = config["server_options"].get(key)
            config["server_options"][key] = value
        elif key in GENERATOR_KEYS:
            originals[("generator", key)] = config["generator"].get(key)
            config["generator"][key] = value
    save_host_yaml(config)
    return originals


def restore_host_yaml(originals: dict) -> None:
    config = load_host_yaml()
    for (section, key), value in originals.items():
        config[section][key] = value
    save_host_yaml(config)


# ── Generation log parsing ────────────────────────────────────────────────────

def parse_generation_error(log_text: str) -> str | tuple:
    lines   = log_text.splitlines()
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
        return _parse_invalid_files(invalid_lines)

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

def run_generation(opts: dict) -> tuple[bool, str]:
    originals = apply_host_yaml_options(opts)
    LOGS_DIR.mkdir(exist_ok=True)
    before_logs = set(LOGS_DIR.glob("Generate_*.txt"))

    result = subprocess.run(
        [sys.executable, str(ARCHIPELAGO_DIR / "Generate.py")],
        cwd=str(ARCHIPELAGO_DIR),
        input=b"\n" * 20,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    restore_host_yaml(originals)

    if result.returncode == 0:
        return True, ""

    after_logs = set(LOGS_DIR.glob("Generate_*.txt"))
    new_logs   = sorted(after_logs - before_logs, key=lambda p: p.stat().st_mtime, reverse=True)
    if not new_logs:
        return False, "Generation failed but no log file was found."

    log_text = new_logs[0].read_text(encoding="utf-8", errors="replace")
    return False, parse_generation_error(log_text)


# ── Room upload ───────────────────────────────────────────────────────────────

def upload_and_create_room(zip_path: Path) -> str:
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
    return room.url


# ── Slash commands ────────────────────────────────────────────────────────────

def is_thread(interaction: discord.Interaction) -> bool:
    return isinstance(interaction.channel, discord.Thread)


@tree.command(name="status", description="List yaml and apworld files found in this thread")
async def status(interaction: discord.Interaction):
    if not is_thread(interaction):
        await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
        return

    await interaction.response.send_message("🔍 Scanning thread history for files...")
    yaml_files, apworld_files, _ = await collect_files_from_thread(interaction.channel)

    yaml_list    = ", ".join(f"`{f.name}`" for f in yaml_files) or "none"
    apworld_list = ", ".join(f"`{f.name}`" for f in apworld_files) or "none"
    await interaction.channel.send(
        f"**Files found in this thread:**\n"
        f"📄 **YAMLs ({len(yaml_files)}):** {yaml_list}\n"
        f"🌍 **APworlds ({len(apworld_files)}):** {apworld_list}"
    )


@tree.command(name="generate", description="Generate and host an Archipelago multiworld from this thread's files")
@app_commands.describe(
    release="When players can release remaining items from their world (default: auto)",
    collect="When players can collect remaining items into their world (default: auto)",
    remaining="When players can query remaining items via !remaining (default: goal)",
    spoiler="Spoiler log detail level: 0=none 1=basic 2=playthrough 3=full (default: 3)",
    race="Enable race mode (default: false)",
    password="Server join password, only visible to you (optional)",
    server_password="Admin password, overrides default, only visible to you (optional)",
)
@app_commands.choices(
    release=[app_commands.Choice(name=m, value=m) for m in VALID_RELEASE_COLLECT_MODES],
    collect=[app_commands.Choice(name=m, value=m) for m in VALID_RELEASE_COLLECT_MODES],
    remaining=[app_commands.Choice(name=m, value=m) for m in VALID_REMAINING_MODES],
    spoiler=[app_commands.Choice(name=m, value=m) for m in VALID_SPOILER_MODES],
)
async def generate(
    interaction: discord.Interaction,
    release: app_commands.Choice[str] = None,
    collect: app_commands.Choice[str] = None,
    remaining: app_commands.Choice[str] = None,
    spoiler: app_commands.Choice[str] = None,
    race: bool = False,
    password: str = None,
    server_password: str = None,
):
    if not is_thread(interaction):
        await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
        return

    await interaction.response.send_message("⏳ Starting generation…", ephemeral=True)
    thread = interaction.channel
    await thread.send("🔍 Scanning thread history for files...")

    if PLAYERS_DIR.exists():
        for f in PLAYERS_DIR.glob("*.yaml"):
            f.unlink(missing_ok=True)
        for f in PLAYERS_DIR.glob("*.yml"):
            f.unlink(missing_ok=True)
    if WORLDS_DIR.exists():
        for f in WORLDS_DIR.glob("*.apworld"):
            f.unlink(missing_ok=True)

    yaml_files, apworld_files, yaml_uploaders = await collect_files_from_thread(thread)
    if not yaml_files:
        await thread.send("⚠️ No YAML files found in this thread — nothing to generate.")
        return

    await thread.send(f"⚙️ Found **{len(yaml_files)}** yaml(s) and **{len(apworld_files)}** apworld(s). Generating… this may take a minute.")

    gen_opts: dict = {"server_password": server_password or SERVER_PASSWORD}
    if release:   gen_opts["release_mode"]   = release.value
    if collect:   gen_opts["collect_mode"]   = collect.value
    if remaining: gen_opts["remaining_mode"] = remaining.value
    if spoiler:   gen_opts["spoiler"]        = int(spoiler.value)
    if race:      gen_opts["race"]           = 1
    if password:  gen_opts["password"]       = password

    before = zip_snapshot()
    loop   = asyncio.get_running_loop()
    success, error = await loop.run_in_executor(None, run_generation, gen_opts)

    if not success:
        if isinstance(error, tuple):
            msg, bad_files = error
            mentions = " ".join(yaml_uploaders[f].mention for f in bad_files if f in yaml_uploaders)
            await thread.send(f"❌ Generation failed{' ' + mentions if mentions else ''}:\n```\n{msg}\n```")
        else:
            await thread.send(f"❌ Generation failed:\n```\n{error}\n```")
        return

    new_zips = sorted(zip_snapshot() - before, key=lambda p: p.stat().st_mtime, reverse=True)
    if not new_zips:
        await thread.send("✅ Generator finished, but no new zip found in output/. Check the logs.")
        return

    await thread.send("✅ Generation complete! Uploading to archipelago.gg…")
    try:
        room_url = await loop.run_in_executor(None, upload_and_create_room, new_zips[0])
    except Exception as e:
        await thread.send(f"⚠️ Generation succeeded but upload failed: `{e}`\nThe zip is saved at: `{new_zips[0]}`")
        return

    await thread.send(f"🎉 Room is ready! <{room_url}>")


# ── Startup ───────────────────────────────────────────────────────────────────

@client.event
async def on_ready():
    await tree.sync()
    print(f"Logged in as {client.user} — slash commands synced.")


client.run(BOT_TOKEN)
