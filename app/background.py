"""Running work after the webhook has already answered Telegram.

Telegram gives a webhook roughly a minute to return 200, and re-delivers
the same update if it doesn't. Anything that touches Trilium can blow past
that on its own: listing books is one search plus a fetch per book, each
with a 15-second timeout, so a slow or unreachable Trilium turns a /reading
into a minute and a half — and the retry then starts the whole flow a
second time.

So the webhook's job is only to decide what to do; the doing happens here,
after 200 is already on its way back.
"""

import asyncio
import traceback

from app import errors

# asyncio keeps only a weak reference to a task nothing else holds, so an
# unreferenced fire-and-forget task can be garbage-collected mid-flight
# (documented behavior) — these strong references are what keep it alive
# until it finishes.
_tasks: set[asyncio.Task] = set()


async def _guard(coro, label: str) -> None:
    """A background task's exception has nobody to propagate to, and would
    otherwise surface only as asyncio's "Task exception was never
    retrieved" long after the fact, detached from what caused it."""
    try:
        await coro
    except Exception as exc:
        print(f"background task {label!r} failed:", flush=True)
        traceback.print_exc()
        errors.record(label, exc)


def spawn(coro, label: str) -> None:
    task = asyncio.create_task(_guard(coro, label))
    _tasks.add(task)
    task.add_done_callback(_tasks.discard)
