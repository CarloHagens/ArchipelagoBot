import asyncio
import os
import re
import shutil
import subprocess
import sys
import uuid
import zipfile
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import discord

import state
from config import log, NUMBERED_LINE_PREFIXES, MAX_PARALLEL_GENERATIONS
from utils.host_yaml import apply_host_yaml_options, restore_host_yaml
from state import get_generation_sem, get_setup_lock


def _filtered_env(tag: str) -> dict:
    return {
        **{k: v for k, v in os.environ.items() if k not in ("BOT_TOKEN", "SERVER_PASSWORD")},
        "PYTHONUSERBASE": f"/archipelago/pyenv/{tag}",
    }


async def setup_and_launch(
    version_dir: Path,
    yaml_data: dict[str, bytes],
    apworld_data: dict[str, bytes],
    output_dir: Path,
) -> subprocess.Popen:
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
        await loop.run_in_executor(None, write_files)

        proc = subprocess.Popen(
            [sys.executable, str(version_dir / "Generate.py"),
             "--outputpath", str(output_dir)],
            cwd=str(version_dir),
            stdin=subprocess.PIPE,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            env=_filtered_env(version_dir.name),
        )
        proc.stdin.write(b"\n" * 20)
        proc.stdin.close()

    return proc


async def _run_one_generation(
    version_dir: Path,
    yaml_data: dict[str, bytes],
    apworld_data: dict[str, bytes],
) -> tuple[int, list[Path]]:
    main_output = version_dir / "output"
    temp_output = main_output / f"run_{uuid.uuid4().hex[:8]}"

    async with get_generation_sem():
        proc = await setup_and_launch(version_dir, yaml_data, apworld_data, temp_output)
        loop = asyncio.get_running_loop()
        _, stderr_bytes = await loop.run_in_executor(None, proc.communicate)
        returncode = proc.returncode

    main_output.mkdir(parents=True, exist_ok=True)
    moved = []
    for zip_path in temp_output.glob("AP_*.zip"):
        dest = main_output / zip_path.name
        zip_path.rename(dest)
        moved.append(dest)
    shutil.rmtree(temp_output, ignore_errors=True)

    return returncode, moved


def _find_missing_module(log_text: str) -> str | None:
    for line in log_text.splitlines():
        m = re.match(r"ModuleNotFoundError: No module named '([^'.]+)", line.strip())
        if m:
            return m.group(1)
    return None


async def _install_missing_module(module_name: str, version_dir: Path) -> bool:
    log.info(f"Installing missing module '{module_name}'...")
    env = _filtered_env(version_dir.name)
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

    _log_generation_failure(log_text)
    return False, parse_generation_error(log_text), []


async def run_generations(
    count: int,
    opts: dict,
    version_dir: Path,
    yaml_data: dict[str, bytes],
    apworld_data: dict[str, bytes],
) -> tuple[int, list[Path], list[str]]:
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

    if succeeded < count:
        after_logs = set(logs_dir.glob("Generate_*.txt"))
        new_logs   = sorted(after_logs - before_logs, key=lambda p: p.stat().st_mtime)
        errors     = [parse_generation_error(p.read_text(encoding="utf-8", errors="replace")) for p in new_logs]
    else:
        errors = []

    log.info(f"{succeeded}/{count} generation(s) succeeded, {len(new_zips)} zip(s) produced.")
    return succeeded, new_zips, errors


def parse_sphere_count(zip_path: Path) -> int | None:
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


def _log_generation_failure(log_text: str) -> None:
    log.warning("Generation failure log:\n" + log_text)


def parse_generation_error(log_text: str) -> str | tuple:
    lines    = log_text.splitlines()
    stripped = [line.strip() for line in lines]

    fill_error = next((line for line in stripped if line.startswith("Fill.FillError:")), None)
    if fill_error:
        return fill_error.split("Missing:")[0].strip()

    rom_missing = next((line for line in stripped if "does not exist, but" in line and "rom_file" in line), None)
    if rom_missing:
        fname = rom_missing.split("FileNotFoundError:")[-1].strip().split(" does not exist")[0].strip()
        return f"Missing ROM file: {fname}. This game requires a ROM to generate and cannot run on the server."

    invalid_lines = [line for line in stripped if line.startswith(NUMBERED_LINE_PREFIXES) and "is invalid" in line]
    if invalid_lines:
        msg, filenames = _parse_invalid_files(invalid_lines)
        friendly = [
            line for line in stripped
            if line.startswith(("Exception:", "ValueError:", "AssertionError:"))
            and not line.startswith(("Exception: No world found", "Exception: No functional world found"))
        ]
        detail = _parse_friendly_errors(friendly)
        if detail:
            msg = f"{msg}\n{detail}"
        return msg, filenames

    no_world = next(
        (line for line in stripped if line.startswith(("Exception: No world found", "Exception: No functional world found"))),
        None,
    )
    if no_world:
        has_traceback = any("Traceback (most recent call last)" in line for line in lines)
        if has_traceback:
            return "An error occurred during generation."
        return no_world.split(":", 1)[1].strip().split(".")[0] + "."

    friendly = [line for line in stripped if line.startswith(("Exception:", "ValueError:", "AssertionError:"))]
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


async def execute_generation(
    bot_user: "discord.User",
    thread: "discord.Thread",
    opts: dict,
    version_dir: Path,
    yaml_data: dict[str, bytes],
    apworld_data: dict[str, bytes],
    yaml_uploaders: dict,
    count: int,
    dry_run: bool = False,
    host: str | None = None,
) -> None:
    """Run generation(s), record, and post results to the thread.
    Memory management is the caller's responsibility."""
    import discord as _discord
    from utils.monitor_helpers import unregister_monitor
    from utils.room_upload import upload_and_create_room
    from utils.runs import mark_run_uploaded, record_run
    from utils.versions import parse_version
    from utils.yaml_validation import get_min_ap_version
    from views import SeedSelectView

    version = version_dir.name
    loop    = asyncio.get_running_loop()

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
        if not success and isinstance(error, str) and "FillError" in error:
            await thread.send("⚠️ Fill failed due to randomness — retrying with a new seed…")
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
        from config import ARCHIPELAGO_BASE
        upload_host = host or ARCHIPELAGO_BASE
        await thread.send(f"✅ Generation complete! Uploading to {upload_host}…")
        try:
            room_url = await loop.run_in_executor(None, upload_and_create_room, new_zips[0], upload_host)
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
            await thread.send("✅ Dry run complete!")
            return
        unregister_monitor(thread.id)
        view = SeedSelectView(zips_with_counts, thread, run["id"], host=host)
        await thread.send("Pick a seed to upload:", view=view)
