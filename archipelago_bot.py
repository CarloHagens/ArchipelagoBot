
import discord
from discord import app_commands
import asyncio
import requests
import os
import re
from pathlib import Path

# ============================================================
# CONFIG
# ============================================================

BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")

ARCHIPELAGO_DIR = Path("/archipelago")
PLAYERS_DIR     = ARCHIPELAGO_DIR / "Players"
WORLDS_DIR      = ARCHIPELAGO_DIR / "custom_worlds"
OUTPUT_DIR      = ARCHIPELAGO_DIR / "output"

ARCHIPELAGO_BASE = "https://archipelago.gg"
SERVER_PASSWORD  = os.environ.get("SERVER_PASSWORD", "archipelago")

# ============================================================

GITHUB_RELEASE_RE = re.compile(
    r'https://github\.com/([^/]+)/([^/]+)/releases/tag/([^\s>]+)'
)

VALID_RELEASE_MODES  = ["disabled", "enabled", "auto", "auto-enabled", "goal"]
VALID_COLLECT_MODES  = ["disabled", "enabled", "auto", "auto-enabled", "goal"]
VALID_REMAINING_MODES = ["disabled", "enabled", "goal"]
VALID_SPOILER_MODES  = ["0", "1", "2", "3"]

intents = discord.Intents.default()
intents.message_content = True  # still needed to scan thread history for file attachments
intents.messages = True
intents.guilds = True

client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)


# ============================================================
# Core logic (unchanged)
# ============================================================

def download_apworld_from_github(owner, repo, tag):
    api_url = f"https://api.github.com/repos/{owner}/{repo}/releases/tags/{tag}"
    resp = requests.get(api_url, timeout=30, headers={"Accept": "application/vnd.github+json"})
    resp.raise_for_status()
    assets = resp.json().get("assets", [])
    apworld_assets = [a for a in assets if a["name"].endswith(".apworld")]
    if not apworld_assets:
        raise RuntimeError(f"No .apworld asset found in {owner}/{repo} release {tag}")
    asset = apworld_assets[0]
    download = requests.get(asset["browser_download_url"], timeout=120)
    download.raise_for_status()
    dest = WORLDS_DIR / asset["name"]
    dest.write_bytes(download.content)
    return dest


async def collect_files_from_thread(thread):
    PLAYERS_DIR.mkdir(parents=True, exist_ok=True)
    WORLDS_DIR.mkdir(parents=True, exist_ok=True)
    yaml_files, apworld_files = [], []
    yaml_uploaders = {}
    seen_repos = {}
    seen_apworld_stems = set()
    loop = asyncio.get_event_loop()

    async for message in thread.history(limit=None, oldest_first=True):
        for attachment in message.attachments:
            name = attachment.filename.lower()
            if name.endswith(".yaml") or name.endswith(".yml"):
                dest = PLAYERS_DIR / attachment.filename
                await attachment.save(dest)
                # Strip UTF-8 BOM and normalise CRLF -> LF for Linux compatibility
                raw = dest.read_bytes()
                bom = bytes([0xef, 0xbb, 0xbf])
                if raw.startswith(bom):
                    raw = raw[3:]
                raw = raw.replace(bytes([0x0d, 0x0a]), bytes([0x0a]))
                dest.write_bytes(raw)
                yaml_files.append(dest)
                yaml_uploaders[attachment.filename] = message.author
            elif name.endswith(".apworld"):
                stem = attachment.filename[:-len(".apworld")].lower()
                if stem in seen_apworld_stems:
                    await thread.send(
                        f"⚠️ Duplicate apworld for **{stem}**: attached file conflicts with a previously seen apworld. Please post only one."
                    )
                    return [], [], {}
                seen_apworld_stems.add(stem)
                dest = WORLDS_DIR / attachment.filename
                await attachment.save(dest)
                apworld_files.append(dest)

        for match in GITHUB_RELEASE_RE.finditer(message.content):
            owner, repo, tag = match.group(1), match.group(2), match.group(3)
            repo_key = f"{owner}/{repo}".lower()
            if repo_key in seen_repos:
                existing_tag = seen_repos[repo_key]
                if existing_tag == tag.lower():
                    continue
                else:
                    await thread.send(
                        f"⚠️ Multiple releases linked for **{owner}/{repo}**: "
                        f"`{existing_tag}` and `{tag}`. Please link only one release."
                    )
                    return [], [], {}
            seen_repos[repo_key] = tag.lower()
            try:
                dest = await loop.run_in_executor(None, download_apworld_from_github, owner, repo, tag)
                stem = dest.name[:-len(".apworld")].lower()
                if stem in seen_apworld_stems:
                    await thread.send(
                        f"⚠️ Duplicate apworld for **{stem}**: GitHub link conflicts with an already posted apworld. Please post only one."
                    )
                    return [], [], {}
                seen_apworld_stems.add(stem)
                apworld_files.append(dest)
            except Exception as e:
                await thread.send(f"⚠️ Could not download apworld from {match.group(0)}: `{e}`")

    return yaml_files, apworld_files, yaml_uploaders


def apply_host_yaml_options(opts):
    import yaml
    host_yaml_path = ARCHIPELAGO_DIR / "host.yaml"
    if host_yaml_path.exists():
        with open(host_yaml_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)
    else:
        config = {
            "server_options": {
                "release_mode": "auto", "collect_mode": "auto",
                "remaining_mode": "goal", "password": None, "server_password": None,
            },
            "generator": {"race": 0, "spoiler": 3},
        }
    originals = {}
    server_keys   = {"release_mode", "collect_mode", "remaining_mode", "password", "server_password"}
    generator_keys = {"race", "spoiler"}
    for key, value in opts.items():
        if key in server_keys:
            originals[("server_options", key)] = config["server_options"].get(key)
            config["server_options"][key] = value
        elif key in generator_keys:
            originals[("generator", key)] = config["generator"].get(key)
            config["generator"][key] = value
    with open(host_yaml_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True)
    return originals


def restore_host_yaml(originals):
    import yaml
    host_yaml_path = ARCHIPELAGO_DIR / "host.yaml"
    with open(host_yaml_path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    for (section, key), value in originals.items():
        config[section][key] = value
    with open(host_yaml_path, "w", encoding="utf-8") as f:
        yaml.dump(config, f, default_flow_style=False, allow_unicode=True)


def run_generation(opts):
    import subprocess
    originals = apply_host_yaml_options(opts)
    logs_dir = ARCHIPELAGO_DIR / "logs"
    logs_dir.mkdir(exist_ok=True)
    before_logs = set(logs_dir.glob("Generate_*.txt"))

    import sys
    result = subprocess.run(
        [sys.executable, str(ARCHIPELAGO_DIR / "Generate.py")],
        cwd=str(ARCHIPELAGO_DIR),
        input=b"\n" * 20,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )

    restore_host_yaml(originals)

    after_logs = set(logs_dir.glob("Generate_*.txt"))
    new_logs = sorted(after_logs - before_logs, key=lambda p: p.stat().st_mtime, reverse=True)

    if result.returncode != 0:
        if new_logs:
            log_text = new_logs[0].read_text(encoding="utf-8", errors="replace")
            lines = log_text.splitlines()

            no_world = [l.strip() for l in lines if l.strip().startswith("Exception: No world found")]
            if no_world:
                msg = no_world[0].split(":", 1)[1].strip().split(".")[0] + "."
                return False, msg

            rom_missing = [l.strip() for l in lines if "does not exist, but" in l and "rom_file" in l]
            if rom_missing:
                fname = rom_missing[0].split("FileNotFoundError:")[-1].strip().split(" does not exist")[0].strip()
                return False, "Missing ROM file: {}. This game requires a ROM to generate and cannot run on the server.".format(fname)


            invalid_files = [
                l.strip() for l in lines
                if (l.strip().startswith(tuple(str(i)+"." for i in range(1, 20))) and "is invalid" in l)
            ]
            if invalid_files:
                msgs, filenames = [], []
                for l in invalid_files:
                    body = l.split(".", 1)[1].strip()
                    if "is invalid" in body:
                        short = body[:body.index("is invalid.") + len("is invalid.")]
                        msgs.append(short)
                        try:
                            fname = short.split("File ")[1].split(" is invalid")[0]
                            filenames.append(fname)
                        except IndexError:
                            pass
                return False, ("\n".join(msgs), filenames)

            friendly = [l.strip() for l in lines if l.strip().startswith(("Exception:", "ValueError:"))]
            if friendly:
                seen = set()
                messages = []
                for l in friendly:
                    msg = l.split(":", 1)[1].strip()
                    if msg in seen or msg.startswith("Encountered"):
                        continue
                    seen.add(msg)
                    if msg.startswith("No world found"):
                        msg = msg.split(".")[0] + "."
                    messages.append(msg)
                return False, "\n".join(messages)
            else:
                error_lines = [
                    l for l in lines
                    if any(kw in l for kw in ("Exception", "Error", "invalid", "failed"))
                    and "logging initialized" not in l
                ]
                return False, "\n".join(error_lines[-10:]) if error_lines else log_text[-1500:]
        return False, "Generation failed but no log file was found."
    return True, ""


def upload_and_create_room(zip_path):
    session = requests.Session()
    session.headers.update({"User-Agent": "ArchipelagoDiscordBot/1.0"})
    with open(zip_path, "rb") as f:
        response = session.post(
            f"{ARCHIPELAGO_BASE}/uploads",
            files={"file": (zip_path.name, f, "application/zip")},
            allow_redirects=True,
            timeout=120,
        )
    response.raise_for_status()
    seed_url = response.url
    if "/seed/" not in seed_url:
        raise RuntimeError(f"Unexpected redirect after upload: {seed_url}")
    seed_id = seed_url.rstrip("/").split("/seed/")[-1]
    room_response = session.get(
        f"{ARCHIPELAGO_BASE}/new_room/{seed_id}",
        allow_redirects=True,
        timeout=30,
    )
    room_response.raise_for_status()
    room_url = room_response.url
    if "/room/" not in room_url:
        raise RuntimeError(f"Unexpected redirect after room creation: {room_url}")
    return room_url


# ============================================================
# Slash commands
# ============================================================

@tree.command(name="status", description="List yaml and apworld files found in this thread")
async def status(interaction: discord.Interaction):
    if not isinstance(interaction.channel, discord.Thread):
        await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
        return
    await interaction.response.send_message("🔍 Scanning thread history for files...")
    yaml_files, apworld_files, _ = await collect_files_from_thread(interaction.channel)
    lines = ["**Files found in this thread:**"]
    lines.append("📄 **YAMLs ({}):** {}".format(len(yaml_files), ", ".join(f"`{f.name}`" for f in yaml_files) or "none"))
    lines.append("🌍 **APworlds ({}):** {}".format(len(apworld_files), ", ".join(f"`{f.name}`" for f in apworld_files) or "none"))
    await interaction.channel.send("\n".join(lines))


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
    release=[app_commands.Choice(name=m, value=m) for m in VALID_RELEASE_MODES],
    collect=[app_commands.Choice(name=m, value=m) for m in VALID_COLLECT_MODES],
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
    if not isinstance(interaction.channel, discord.Thread):
        await interaction.response.send_message("⚠️ This command must be used inside a thread.", ephemeral=True)
        return

    # Acknowledge ephemerally so passwords are only visible to the invoker
    await interaction.response.send_message("⏳ Starting generation…", ephemeral=True)

    thread = interaction.channel
    await thread.send("🔍 Scanning thread history for files...")

    for f in list(PLAYERS_DIR.glob("*.yaml")) + list(PLAYERS_DIR.glob("*.yml")):
        f.unlink(missing_ok=True)
    for f in WORLDS_DIR.glob("*.apworld"):
        f.unlink(missing_ok=True)

    yaml_files, apworld_files, yaml_uploaders = await collect_files_from_thread(thread)
    if not yaml_files:
        await thread.send("⚠️ No YAML files found in this thread — nothing to generate.")
        return

    await thread.send("⚙️ Found **{}** yaml(s) and **{}** apworld(s). Generating… this may take a minute.".format(
        len(yaml_files), len(apworld_files)
    ))

    gen_opts = {"server_password": server_password if server_password else SERVER_PASSWORD}
    if release:
        gen_opts["release_mode"] = release.value
    if collect:
        gen_opts["collect_mode"] = collect.value
    if remaining:
        gen_opts["remaining_mode"] = remaining.value
    if spoiler:
        gen_opts["spoiler"] = int(spoiler.value)
    if race:
        gen_opts["race"] = 1
    if password:
        gen_opts["password"] = password

    before = set(OUTPUT_DIR.glob("AP_*.zip")) if OUTPUT_DIR.exists() else set()
    loop = asyncio.get_event_loop()
    success, error = await loop.run_in_executor(None, run_generation, gen_opts)

    if not success:
        if isinstance(error, tuple):
            msg, bad_files = error
            mentions = " ".join(yaml_uploaders[f].mention for f in bad_files if f in yaml_uploaders)
            tag = " " + mentions if mentions else ""
            await thread.send("❌ Generation failed{}:\n```\n{}\n```".format(tag, msg))
        else:
            await thread.send("❌ Generation failed:\n```\n{}\n```".format(error))
        return

    after = set(OUTPUT_DIR.glob("AP_*.zip")) if OUTPUT_DIR.exists() else set()
    new_zips = sorted(after - before, key=lambda p: p.stat().st_mtime, reverse=True)
    if not new_zips:
        await thread.send("✅ Generator finished, but no new zip found in output/. Check the logs.")
        return

    zip_path = new_zips[0]
    await thread.send("✅ Generation complete! Uploading to archipelago.gg…")

    try:
        room_url = await loop.run_in_executor(None, upload_and_create_room, zip_path)
    except Exception as e:
        await thread.send("⚠️ Generation succeeded but upload failed: `{}`\nThe zip is saved at: `{}`".format(e, zip_path))
        return

    await thread.send("🎉 Room is ready! <{}>".format(room_url))


# ============================================================
# Startup
# ============================================================

@client.event
async def on_ready():
    await tree.sync()
    print(f"Logged in as {client.user} — slash commands synced.")


client.run(BOT_TOKEN)
