import asyncio
import json
import re
import traceback
from datetime import timedelta

from app.config import TIMEZONE
from app.db import (
    ack_incoming_messages,
    advance_activity_prompt_step,
    advance_book_quote_prompt_step,
    advance_ezhednevnik_step,
    append_book_quote_prompt_message,
    close_activity_prompt,
    close_book_quote_prompt,
    close_ezhednevnik_prompt,
    create_activity_prompt,
    create_book_quote_prompt,
    get_book_quote_prompt,
    get_open_activity_prompt,
    get_open_book_quote_prompt,
    get_open_ezhednevnik_prompt,
    insert_incoming_message,
    set_book_quote_prompt_book,
    utcnow,
)
from app.ingest import TRILIUM_UNAVAILABLE_TEXT, handle_active_message
from app.people import USER_NAMES
from app.prompts import (
    ACTIVITY_STEPS,
    EZHEDNEVNIK_STEPS,
    activity_step_text,
    ezhednevnik_step_text,
    quote_step_text,
)
from app.telegram import (
    answer_callback_query,
    send_message,
    send_message_get_id,
    send_message_with_buttons,
)
from app.trilium_client import (
    add_book_quote,
    extract_duration,
    fill_ezhednevnik,
    get_active_reading_books,
    log_activity,
)

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

# Much shorter than ежедневник/activity, per request — this is a quick
# on-the-spot action, not something meant to sit half-answered for hours.
# The real cleanup (deleting the dangling messages, not just closing the
# prompt) happens proactively in scheduler.py's release_stale_quote_prompts
# (polled every 60s); this is only a same-value fallback so a reply that
# slips in right at the boundary is never treated as answering a prompt
# that's about to be wiped out from under it.
_QUOTE_PROMPT_TIMEOUT = timedelta(minutes=5)

_ACTIVITY_VERBS = (
    "позанимал", "занимал", "делал", "сделал", "провел", "провёл",
    "отработал", "потрейдил", "затрейдил",
)
_ACTIVITY_KEYWORDS = {
    "yoga": ("йог",),
    "chinese": ("китайск",),
    "trading": ("трейдинг", "трейд"),
}


def _looks_like_quote_request(text: str) -> bool:
    """Matches any Russian word-form of "цитата" (цитату, цитаты, цитате,
    ...) — they all share the "цитат" stem, so a plain substring check
    covers every inflection without needing a word list."""
    return "цитат" in text.lower()


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

    # Same shape again — an open book_quote_prompt with step >= 1 means this
    # message answers the quote or the impression question. Step 0 (book not
    # picked yet) is driven by a button press instead (see
    # handle_quote_book_selected below, called from app/callbacks.py), so a
    # stray text message while step 0 is still open just gets a nudge to use
    # the button rather than being consumed as an answer.
    open_quote = await get_open_book_quote_prompt(user_id)
    if open_quote is not None:
        if utcnow() - open_quote["updated_at"] > _QUOTE_PROMPT_TIMEOUT:
            await close_book_quote_prompt(open_quote["id"])
            open_quote = None
    if open_quote is not None:
        if open_quote["step"] == 0:
            await send_message(user_id, "Выбери книгу, нажав на кнопку в сообщении выше.")
            return
        await _handle_quote_reply(user_id, text, reply_to_text, open_quote)
        return

    if _looks_like_quote_request(text):
        await start_quote_flow(user_id, text, reply_to_text)
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


async def start_quote_flow(user_id: int, text: str = "цитата", reply_to_text: str | None = None) -> None:
    """Look up books currently in active reading (readingStart set, no
    readingEnd — see get_active_reading_books) and offer them as buttons.
    Shared by the "цитата" text trigger above and the /quote command
    (app/main.py). The actual quote/impression answers are logged by
    _handle_quote_reply below; only the trigger message itself is logged
    here."""
    message_id = await insert_incoming_message(user_id, text, "quote_start", reply_to_text)
    try:
        books = await get_active_reading_books()
    except Exception:
        traceback.print_exc()
        await send_message(user_id, TRILIUM_UNAVAILABLE_TEXT)
        await ack_incoming_messages([message_id])
        return

    if not books:
        await send_message(user_id, "Нет книг в активном чтении (без даты окончания).")
        await ack_incoming_messages([message_id])
        return

    prompt_id = await create_book_quote_prompt(user_id, books)
    buttons = [(book["title"], f"bq:{prompt_id}:{i}") for i, book in enumerate(books)]
    sent_id = await send_message_with_buttons(user_id, "Какую книгу?", buttons)
    if sent_id is not None:
        await append_book_quote_prompt_message(prompt_id, sent_id)
    await ack_incoming_messages([message_id])


async def handle_quote_book_selected(callback_query: dict) -> None:
    """Routes a "bq:{prompt_id}:{index}" button press from start_quote_flow
    above back to the matching book — called from app/callbacks.py."""
    query_id = callback_query["id"]
    data = callback_query.get("data") or ""
    chat_id = callback_query.get("message", {}).get("chat", {}).get("id")

    try:
        _prefix, prompt_id_str, index_str = data.split(":")
        prompt_id, index = int(prompt_id_str), int(index_str)
    except ValueError:
        await answer_callback_query(query_id)
        return

    prompt = await get_book_quote_prompt(prompt_id)
    if prompt is None or not prompt["is_open"] or prompt["step"] != 0 or prompt["user_id"] != chat_id:
        await answer_callback_query(query_id, "Этот выбор уже неактуален.")
        return

    candidates = json.loads(prompt["collected"] or "{}").get("candidates", [])
    if index < 0 or index >= len(candidates):
        await answer_callback_query(query_id, "Этот выбор уже неактуален.")
        return

    book = candidates[index]
    await set_book_quote_prompt_book(prompt_id, book["note_id"], book["title"])
    await answer_callback_query(query_id, f"Книга: {book['title']}")
    sent_id = await send_message_get_id(chat_id, quote_step_text(0))
    if sent_id is not None:
        await append_book_quote_prompt_message(prompt_id, sent_id)


async def _handle_quote_reply(
    user_id: int, text: str, reply_to_text: str | None, prompt,
) -> None:
    """step 1 = awaiting the quote text, step 2 = awaiting the impression —
    same shape as _handle_activity_reply."""
    step = prompt["step"]
    kind = f"quote_{step}"
    message_id = await insert_incoming_message(user_id, text, kind, reply_to_text)

    collected = json.loads(prompt["collected"] or "{}")
    if step == 1:
        collected["quote"] = text.strip()
        await advance_book_quote_prompt_step(prompt["id"], 2, collected)
        sent_id = await send_message_get_id(user_id, quote_step_text(1))
        if sent_id is not None:
            await append_book_quote_prompt_message(prompt["id"], sent_id)
        await ack_incoming_messages([message_id])
        return

    collected["impression"] = text.strip()
    # Close only on SUCCESS, same reasoning as ежедневник/activity — a
    # failed write must not lose the quote already collected.
    try:
        await add_book_quote(prompt["book_note_id"], collected["quote"], collected["impression"])
        await close_book_quote_prompt(prompt["id"])
        await send_message(user_id, "Записал, спасибо.")
    except Exception as exc:
        print(f"add_book_quote failed for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(
            user_id,
            f"Не получилось записать (Trilium недоступен): {type(exc).__name__}. "
            "Напиши что-нибудь ещё раз чуть позже — я попробую снова.",
        )
    await ack_incoming_messages([message_id])


