from datetime import datetime, timedelta, timezone

import discord
from discord import app_commands

from config import COMMON_TIMEZONES, MAX_APWORLD_BYTES, MAX_YAML_BYTES
from utils.files import normalise_yaml_bytes
from utils.runs import load_runs
from utils.versions import get_installed_versions, parse_version
from utils.yaml_validation import get_min_ap_version


async def _get_thread_min_ap_version(thread: discord.Thread) -> str | None:
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
    unique = list(dict.fromkeys(suggestions))
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
            pass
    return [
        app_commands.Choice(name=v, value=v)
        for v in versions
        if current.lower() in v.lower()
    ]


async def run_autocomplete(interaction: discord.Interaction, current: str):
    runs = load_runs()
    choices = []
    for run in runs:
        uploaded = "✅ " if run.get("uploaded") else ""
        count    = len(run.get("seeds", []))
        thread   = run.get("thread_name", "unknown")
        time     = datetime.fromisoformat(run["timestamp"]).strftime("%m-%d %H:%M")
        label    = f"{uploaded}{count} seed{'s' if count != 1 else ''} — #{thread} — {time}"
        if current.lower() in label.lower():
            choices.append(app_commands.Choice(name=label[:100], value=run["id"]))
        if len(choices) == 25:
            break
    return choices


async def seed_autocomplete(interaction: discord.Interaction, current: str):
    from pathlib import Path
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
        name    = Path(path).name
        spheres = seed.get("spheres")
        suffix  = f" — {spheres} spheres" if spheres is not None else ""
        label   = f"{name}{suffix}"
        if current.lower() in label.lower():
            choices.append(app_commands.Choice(name=label[:100], value=path))
    return choices
