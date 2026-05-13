import json
from datetime import datetime
from pathlib import Path

from config import MAX_RUNS, RUNS_FILE, log
from utils.files import load_json_file


def load_runs() -> list[dict]:
    return load_json_file(RUNS_FILE, [])


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
    now = datetime.now()
    run = {
        "id":          now.strftime("%Y%m%d_%H%M%S"),
        "timestamp":   now.isoformat(),
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
