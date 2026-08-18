import asyncio
import json
import re
import traceback
from datetime import timedelta

from app.config import TIMEZONE
from app.db import (
    ack_incoming_messages,
    advance_activity_prompt_step,
    advance_ezhednevnik_step,
    close_activity_prompt,
    close_ezhednevnik_prompt,
    create_activity_prompt,
    get_open_activity_prompt,
    get_open_ezhednevnik_prompt,
    insert_incoming_message,
    utcnow,
)
from app.ingest import handle_active_message
from app.people import USER_NAMES
from app.prompts import ACTIVITY_STEPS, EZHEDNEVNIK_STEPS, activity_step_text, ezhednevnik_step_text
from app.telegram import send_message
from app.trilium_client import extract_duration, fill_ezhednevnik, log_activity

# asyncio only holds a weak reference to a task with no other referrer, so an
# unreferenced fire-and-forget task is eligible for GC before it completes
# (documented asyncio behavior) — keep a strong reference here until it's done.
_background_tasks: set[asyncio.Task] = set()

_SCORE_RE = re.compile(r"-?\d+")

# How long an open activity_prompt (yoga/chinese/trading) can sit unanswered
# before it's treated as abandoned rather than still "in flight" — unlike
# ежедневник (re-checked at each fixed AM/PM/evening cron tick), nothing
# else ever re-visits this, so without a timeout a forgotten reply would
# silently gate every future message from that person forever, same class
# of bug already fixed once for ежедневник (see close_stale_ezhednevnik_prompts).
_ACTIVITY_PROMPT_TIMEOUT = timedelta(hours=3)

_ACTIVITY_VERBS = (
    "позанимал", "занимал", "делал", "сделал", "провел", "провёл",
    "отработал", "потрейдил", "затрейдил",
)
_ACTIVITY_KEYWORDS = {
    "yoga": ("йог",),
    "chinese": ("китайск",),
    "trading": ("трейдинг", "трейд"),
}


def _looks_like_activity_log(text: str) -> str | None:
    """"позанималась йогой" / "позанимался китайским" / "потрейдил
    сегодня" — self-initiated, no schedule involved (unlike ежедневник).
    Requires both an activity keyword AND a completion-signal verb, a bit
    stricter than most other loose keyword heuristics in this codebase,
    since a false positive here starts a real multi-step conversation
    (asking "как тебе?"/"чему научился?") rather than just costing an
    unneeded retry."""
    lowered = text.lower()
    for activity, keywords in _ACTIVITY_KEYWORDS.items():
        if any(kw in lowered for kw in keywords) and any(v in lowered for v in _ACTIVITY_VERBS):
            return activity
    return None


async def process_incoming_message(user_id: int, text: str, reply_to_text: str | None = None) -> None:
    # A pending ежедневник check-in at arrival time means this message is
    # the answer to it, not a spontaneous message — see app/ingest.py for
    # how a spontaneous ("active") message is handled downstream. Every
    # ежедневник slot is a strict one-question-at-a-time sequence (see
    # EZHEDNEVNIK_STEPS in prompts.py) — entirely deterministic, never
    # touches Odysseus/the LLM at all, since each reply maps to exactly one
    # known field with nothing left to interpret.
    open_ezhednevnik = await get_open_ezhednevnik_prompt(user_id)
    if open_ezhednevnik is not None:
        await _handle_ezhednevnik_reply(user_id, text, reply_to_text, open_ezhednevnik)
        return

    # Same shape as ежедневник, but user-initiated (see _looks_like_activity_log
    # below) instead of scheduled — an open prompt here means this message
    # is the answer to "как тебе?"/"чему научился?"/"баллы", not a new
    # spontaneous message.
    open_activity = await get_open_activity_prompt(user_id)
    if open_activity is not None:
        if utcnow() - open_activity["sent_at"] > _ACTIVITY_PROMPT_TIMEOUT:
            await close_activity_prompt(open_activity["id"])
            open_activity = None
    if open_activity is not None:
        await _handle_activity_reply(user_id, text, reply_to_text, open_activity)
        return

    activity = _looks_like_activity_log(text)
    if activity is not None:
        await _start_activity_flow(user_id, text, reply_to_text, activity)
        return

    received_at = utcnow()
    message_id = await insert_incoming_message(user_id, text, "active", reply_to_text)

    task = asyncio.create_task(
        handle_active_message(message_id, user_id, text, received_at, reply_to_text)
    )
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


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
    trail, then immediately acked — this never enters handle_active_message()
    at all."""
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
    # Dated to when the question was actually SENT (its calendar day in
    # Moscow time), not whenever the reply happens to land — confirmed
    # live: a prompt left open overnight and answered the next morning was
    # otherwise stamped with that morning's date, landing the previous
    # day's retrospective in the wrong row entirely.
    entry_date = prompt["sent_at"].astimezone(TIMEZONE).date().isoformat()
    fields = {"person_name": person_name, "slot": slot, "date": entry_date, **collected}

    # Close only on SUCCESS — confirmed live (the odysseus.61d1.online DNS
    # outage, back when this went through Odysseus): closing unconditionally
    # BEFORE attempting the write meant a failed write lost that day's entry
    # outright, with no way to retry — the prompt was already gone. Leaving
    # it open on failure means the person can just answer again (any
    # message) once Trilium is reachable again; `collected` already has
    # every earlier answer intact either way.
    try:
        await fill_ezhednevnik(fields)
        await close_ezhednevnik_prompt(prompt["id"])
        await send_message(user_id, "Записал, спасибо.")
    except Exception as exc:
        print(f"fill_ezhednevnik failed for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(
            user_id,
            f"Не получилось записать (Trilium недоступен): {type(exc).__name__}. "
            "Напиши что-нибудь ещё раз чуть позже — я попробую снова.",
        )
    await ack_incoming_messages([message_id])


async def _start_activity_flow(
    user_id: int, text: str, reply_to_text: str | None, activity: str,
) -> None:
    """The trigger message itself ("позанималась йогой") carries no data —
    just log it for the audit trail, open the prompt, and ask the first
    question."""
    message_id = await insert_incoming_message(user_id, text, f"activity_{activity}_start", reply_to_text)
    await create_activity_prompt(user_id, activity)
    await send_message(user_id, activity_step_text(activity, 0))
    await ack_incoming_messages([message_id])


async def _handle_activity_reply(
    user_id: int, text: str, reply_to_text: str | None, prompt,
) -> None:
    """Advance one step in the activity's 2-step sequence (feedback, then
    score) — same shape as _handle_ezhednevnik_reply. The feedback step's
    answer is also scanned for a mentioned duration (see
    app/trilium_client.extract_duration) — there's no separate duration
    question by explicit choice."""
    activity = prompt["activity"]
    step = prompt["step"]
    steps = ACTIVITY_STEPS[activity]
    field = steps[step][2]

    kind = f"activity_{activity}_{step}"
    message_id = await insert_incoming_message(user_id, text, kind, reply_to_text)

    collected = json.loads(prompt["collected"] or "{}")
    if field == "score":
        match = _SCORE_RE.search(text)
        if not match:
            # Never invent a score — re-ask instead of defaulting to 0 or
            # completing with a missing value.
            await send_message(user_id, "Не расслышал число — " + activity_step_text(activity, step))
            await ack_incoming_messages([message_id])
            return
        collected[field] = max(0, min(100, int(match.group())))
    else:
        collected[field] = text.strip()
        duration = extract_duration(text)
        if duration:
            collected["duration"] = duration

    next_step = step + 1
    if next_step < len(steps):
        await advance_activity_prompt_step(prompt["id"], next_step, collected)
        await send_message(user_id, activity_step_text(activity, next_step))
        await ack_incoming_messages([message_id])
        return

    person_name = USER_NAMES.get(user_id, str(user_id))
    # Close only on SUCCESS, same reasoning as ежедневник — a failed write
    # must not lose the answers already collected.
    try:
        await log_activity(
            person_name, activity,
            collected.get("feedback", ""), collected["score"],
            collected.get("duration"),
        )
        await close_activity_prompt(prompt["id"])
        await send_message(user_id, "Записал, спасибо.")
    except Exception as exc:
        print(f"log_activity failed for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(
            user_id,
            f"Не получилось записать (Trilium недоступен): {type(exc).__name__}. "
            "Напиши что-нибудь ещё раз чуть позже — я попробую снова.",
        )
    await ack_incoming_messages([message_id])


