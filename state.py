import asyncio

from config import MAX_PARALLEL_GENERATIONS

locks:           dict[str, asyncio.Lock]  = {}
monitor_pending: set[int]                 = set()
generation_sem:  asyncio.Semaphore | None = None
memory_in_use:   int                      = 0
monitors:        dict                     = {}
scheduled:       list                     = []
checker_task:    asyncio.Task | None      = None


def _get_lock(key: str) -> asyncio.Lock:
    if key not in locks:
        locks[key] = asyncio.Lock()
    return locks[key]


def get_audit_lock(thread_id: int) -> asyncio.Lock:
    return _get_lock(f"audit:{thread_id}")


def get_setup_lock(version_dir) -> asyncio.Lock:
    return _get_lock(f"setup:{version_dir}")


def get_generation_sem() -> asyncio.Semaphore:
    global generation_sem
    if generation_sem is None:
        generation_sem = asyncio.Semaphore(MAX_PARALLEL_GENERATIONS)
    return generation_sem
