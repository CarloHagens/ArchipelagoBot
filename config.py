import logging
import os
import re
from pathlib import Path

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
MAX_YAML_BYTES           = 1 * 1024 * 1024
MAX_APWORLD_BYTES        = 30 * 1024 * 1024
MAX_ZIP_BYTES            = 100 * 1024 * 1024
MAX_GENERATION_MEMORY    = 3 * 1024 * 1024 * 1024
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
