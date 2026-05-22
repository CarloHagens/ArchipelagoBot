import io
import json
import zipfile
from pathlib import Path

from config import UTF8_BOM, log


def load_json_file(path: Path, default):
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        log.warning(f"Could not load {path.name}: {e}")
        return default


def normalise_yaml_bytes(raw: bytes) -> bytes:
    if raw.startswith(UTF8_BOM):
        raw = raw[len(UTF8_BOM):]
    raw = raw.replace(b'\r\n', b'\n')
    # Strip trailing whitespace (including tabs) from each line — PyYAML is
    # strict about tabs and will raise a ScannerError on trailing tabs.
    raw = b'\n'.join(line.rstrip() for line in raw.split(b'\n'))
    return raw


def apworld_stem(filename: str) -> str:
    return Path(filename).stem.lower()


def safe_filename(filename: str) -> str:
    return Path(filename).name


def _build_gather_zip(yaml_data: dict[str, bytes], apworld_data: dict[str, bytes]) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for name, data in yaml_data.items():
            zf.writestr(f"Players/{name}", data)
        for name, data in apworld_data.items():
            zf.writestr(f"custom_worlds/{name}", data)
    return buf.getvalue()
