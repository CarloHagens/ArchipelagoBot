import json
from datetime import datetime, timezone

import dateparser

import state
from config import SCHEDULED_FILE, TIMEZONE, log


def load_scheduled() -> list:
    try:
        return json.loads(SCHEDULED_FILE.read_text(encoding="utf-8"))
    except Exception as e:
        log.warning(f"Could not load {SCHEDULED_FILE.name}: {e}")
        return []


def save_scheduled() -> None:
    SCHEDULED_FILE.write_text(json.dumps(state.scheduled, indent=2), encoding="utf-8")


def get_scheduled_job(thread_id: int) -> dict | None:
    return next((j for j in state.scheduled if j["thread_id"] == thread_id), None)


def remove_scheduled_job(thread_id: int) -> bool:
    before = len(state.scheduled)
    state.scheduled[:] = [j for j in state.scheduled if j["thread_id"] != thread_id]
    if len(state.scheduled) < before:
        save_scheduled()
        return True
    return False


def parse_schedule_time(time_str: str, tz_name: str | None = None) -> datetime | None:
    settings = {
        "TIMEZONE":                 tz_name or TIMEZONE,
        "RETURN_AS_TIMEZONE_AWARE": True,
        "PREFER_DATES_FROM":        "future",
        "TO_TIMEZONE":              "UTC",
    }
    try:
        dt = dateparser.parse(time_str, settings=settings)
        return dt if dt and dt > datetime.now(timezone.utc) else None
    except Exception:
        return None
