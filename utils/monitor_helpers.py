import json
import re

import discord

import state
from config import MONITORS_FILE, log
from utils.files import load_json_file


_MENTION_RE = re.compile(r'<@!?\d+>\s*')


def format_resolved(content: str) -> str:
    clean = _MENTION_RE.sub('', content)
    clean = clean.replace('⚠️ ', '', 1)
    return f"✅ ~~{clean.strip()}~~"


def is_monitored(channel) -> bool:
    return isinstance(channel, discord.Thread) and str(channel.id) in state.monitors


def load_monitors() -> dict:
    return load_json_file(MONITORS_FILE, {})


def save_monitors() -> None:
    MONITORS_FILE.write_text(json.dumps(state.monitors, indent=2), encoding="utf-8")


def unregister_monitor(thread_id: int) -> None:
    if str(thread_id) in state.monitors:
        del state.monitors[str(thread_id)]
        save_monitors()
        log.info(f"Stopped monitoring thread {thread_id}.")
