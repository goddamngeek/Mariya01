import asyncio
import json
import re
import traceback

from app.config import OUTGOING_DEDUP_DAYS
from app.db import (
    ack_incoming_messages,
    advance_ezhednevnik_step,
    close_ezhednevnik_prompt,
    close_question,
    get_open_ezhednevnik_prompt,
    get_open_question,
    insert_incoming_message,
    mark_reply_sent,
    pick_outgoing_message,
    utcnow,
)
from app.ingest import handle_active_message
from app.odysseus_client import fill_ezhednevnik_direct
from app.people import USER_NAMES
from app.prompts import EZHEDNEVNIK_STEPS, ezhednevnik_step_text
from app.telegram import send_message

# asyncio only holds a weak reference to a task with no other referrer, so an
# unreferenced fire-and-forget task is eligible for GC before it completes
# (documented asyncio behavior) — keep a strong reference here until it's done.
_background_tasks: set[asyncio.Task] = set()

_SCORE_RE = re.compile(r"-?\d+")


async def process_incoming_message(user_id: int, text: str, reply_to_text: str | None = None) -> None:
    # A pending question/check-in at arrival time means this message is the
    # user's answer to it (passive-style) rather than a spontaneous message
    # (active) — see app/ingest.py for how each kind is handled downstream.
    # The old random daily question and the new ежедневник check-ins are
    # mutually exclusive in practice (the former is being phased out), but
    # checked in this order regardless — a question takes priority if both
    # were somehow open at once.
    open_question = await get_open_question(user_id)
    open_ezhednevnik = None if open_question is not None else await get_open_ezhednevnik_prompt(user_id)

    # Every ежедневник slot is now a strict one-question-at-a-time sequence
    # (see EZHEDNEVNIK_STEPS in prompts.py) — entirely deterministic, never
    # touches Odysseus/the LLM at all, since each reply maps to exactly one
    # known field with nothing left to interpret.
    if open_question is None and open_ezhednevnik is not None:
        await _handle_ezhednevnik_reply(user_id, text, reply_to_text, open_ezhednevnik)
        return

    if open_question is not None:
        kind = "passive"
    else:
        kind = "active"
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


async def _handle_ezhednevnik_reply(
    user_id: int, text: str, reply_to_text: str | None, prompt,
) -> None:
    """Advance one step in the current slot's question sequence. The reply
    just received answers `prompt["step"]`'s field — store it (a *_score
    field is parsed as a number, best-effort; no number found just means no
    score, never invent one) into `collected`. If more steps remain, ask
    the next one. If that was the last step, write everything gathered to
    Trilium via a direct non-LLM call and close the prompt out. Every
    message here still gets logged to incoming_messages for the audit
    trail, then immediately acked — this never enters the normal
    ingest_incoming()/handle_active_message() pipelines at all."""
    slot = prompt["slot"]
    step = prompt["step"]
    steps = EZHEDNEVNIK_STEPS[slot]
    field = steps[step][2]

    kind = f"ezhednevnik_{slot}_{step}"
    message_id = await insert_incoming_message(user_id, text, kind, reply_to_text)

    collected = json.loads(prompt["collected"] or "{}")
    if field.endswith("_score"):
        match = _SCORE_RE.search(text)
        if match:
            collected[field] = max(0, min(100, int(match.group())))
        # no number found -> field just stays unset, never invented
    else:
        collected[field] = text.strip()

    next_step = step + 1
    if next_step < len(steps):
        await advance_ezhednevnik_step(prompt["id"], next_step, collected)
        await send_message(user_id, ezhednevnik_step_text(slot, next_step))
        await ack_incoming_messages([message_id])
        return

    person_name = USER_NAMES.get(user_id, str(user_id))
    fields = {"person_name": person_name, "slot": slot, **collected}

    await close_ezhednevnik_prompt(prompt["id"])
    try:
        await fill_ezhednevnik_direct(fields)
        await send_message(user_id, "Записал, спасибо.")
    except Exception as exc:
        print(f"fill_ezhednevnik_direct failed for user={user_id}:", flush=True)
        traceback.print_exc()
        # No server log access to the Northflank deployment — surface the
        # real reason directly in chat (temporary, until this class of
        # failure is understood) instead of a silent no-op the person has
        # no way to notice, let alone diagnose.
        await send_message(user_id, f"Не получилось записать: {type(exc).__name__}: {exc}"[:500])
    await ack_incoming_messages([message_id])


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
