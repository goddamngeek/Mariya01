import asyncio
import traceback

from app.config import OUTGOING_DEDUP_DAYS
from app.db import (
    close_question,
    get_open_question,
    insert_incoming_message,
    mark_reply_sent,
    pick_outgoing_message,
    utcnow,
)
from app.ingest import handle_active_message
from app.telegram import send_message

# asyncio only holds a weak reference to a task with no other referrer, so an
# unreferenced fire-and-forget task is eligible for GC before it completes
# (documented asyncio behavior) — keep a strong reference here until it's done.
_background_tasks: set[asyncio.Task] = set()


async def process_incoming_message(user_id: int, text: str, reply_to_text: str | None = None) -> None:
    # A pending question at arrival time means this message is the user's
    # answer to it (passive) rather than a spontaneous question (active) —
    # see app/ingest.py for how each kind is handled downstream.
    open_question = await get_open_question(user_id)
    kind = "passive" if open_question is not None else "active"
    received_at = utcnow()

    message_id = await insert_incoming_message(user_id, text, kind, reply_to_text)

    if open_question is not None:
        await close_question(open_question["id"])

    if kind == "active":
        # A real, formulated answer is coming from Odysseus shortly — the
        # generic pool "reply" (принял / понял, разберусь) would just be
        # redundant noise ahead of it.
        task = asyncio.create_task(
            handle_active_message(message_id, user_id, text, received_at, reply_to_text)
        )
        _background_tasks.add(task)
        task.add_done_callback(_background_tasks.discard)
    else:
        await _send_reply(user_id)


async def _send_reply(user_id: int) -> None:
    try:
        reply = await pick_outgoing_message("reply", OUTGOING_DEDUP_DAYS, user_id)
        if reply is None:
            return

        if await send_message(user_id, reply["text"]):
            await mark_reply_sent(reply["id"])
        else:
            print(f"failed to send reply id={reply['id']} user={user_id}", flush=True)
    except Exception:
        # A bug here must not turn into a webhook 500 — Telegram retries
        # failed webhook deliveries, which would re-run process_incoming_message
        # and re-insert the same incoming message. Print the full traceback
        # (not just the exception) so a real bug is still diagnosable.
        traceback.print_exc()
