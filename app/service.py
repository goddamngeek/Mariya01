import asyncio
import html
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
    append_book_add_prompt_message,
    append_book_quote_prompt_message,
    close_activity_prompt,
    close_book_add_prompt,
    close_book_quote_prompt,
    close_ezhednevnik_prompt,
    create_activity_prompt,
    create_book_add_prompt,
    create_book_quote_prompt,
    finalize_book_add_prompt,
    get_book_add_prompt,
    get_book_add_prompt_by_template_message,
    get_book_quote_prompt,
    get_incoming_message_by_telegram_id,
    get_open_activity_prompt,
    get_open_book_add_prompt,
    get_open_book_quote_prompt,
    get_open_ezhednevnik_prompt,
    insert_incoming_message,
    set_book_add_title,
    set_book_quote_prompt_book,
    update_incoming_message_text,
    utcnow,
)
from app.ingest import TRILIUM_UNAVAILABLE_TEXT, handle_active_message
from app.people import USER_NAMES
from app.prompts import (
    ACTIVITY_STEPS,
    BOOK_DETAILS_TEMPLATE,
    EZHEDNEVNIK_STEPS,
    activity_step_text,
    book_add_step_text,
    ezhednevnik_step_text,
    quote_step_text,
)
from app.telegram import (
    answer_callback_query,
    delete_message,
    send_message,
    send_message_get_id,
    send_message_with_buttons,
)
from app.trilium_client import (
    BOOK_DETAIL_HEADERS,
    add_book,
    add_book_quote,
    extract_duration,
    fill_book_details,
    fill_ezhednevnik,
    get_active_reading_books,
    get_book_details,
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

# Stage 1 only (title/author collection) — same reasoning as
# _ACTIVITY_PROMPT_TIMEOUT. Once stage 1 finishes and the "расскажи
# подробнее" template is sent, the row is finalized (see
# finalize_book_add_prompt) and stays addressable forever via a reply —
# that part has no timeout at all, per explicit request.
_BOOK_ADD_PROMPT_TIMEOUT = timedelta(hours=3)

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


def _looks_like_reading_status(text: str) -> bool:
    """"что я сейчас читаю" / "что читаю" — present tense only ("читаю"),
    so it never collides with _looks_like_book_add's "начал читать"/"хочу
    почитать" (infinitive forms)."""
    return "читаю" in text.lower()


def _looks_like_book_add(text: str) -> bool:
    """"добавь книгу ..." / "хочу почитать ..." / "начал читать ..." —
    ported from the old (now removed) ingest.py heuristic of the same name,
    just moved here now that adding a book is a deterministic 2-question
    flow (title, then author) instead of a single narrow LLM extraction.
    "добавь"/"добавить" need "книг" alongside them (too generic alone —
    collides with the plain note-request "добавь"), but "хочу почитать X"/
    "начал читать X" are distinctive enough to stand alone."""
    lowered = text.lower()
    if "книг" in lowered and any(kw in lowered for kw in ("добавь", "добавить")):
        return True
    return any(kw in lowered for kw in ("хочу почитать", "буду читать", "начал читать", "начала читать"))


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


async def process_incoming_message(
    user_id: int, text: str, reply_to_text: str | None = None, telegram_message_id: int | None = None,
) -> None:
    # A pending ежедневник check-in at arrival time means this message is
    # the answer to it, not a spontaneous message — see app/ingest.py for
    # how a spontaneous ("active") message is handled downstream. Every
    # ежедневник slot is a strict one-question-at-a-time sequence (see
    # EZHEDNEVNIK_STEPS in prompts.py) — entirely deterministic, never
    # touches Odysseus/the LLM at all, since each reply maps to exactly one
    # known field with nothing left to interpret.
    open_ezhednevnik = await get_open_ezhednevnik_prompt(user_id)
    if open_ezhednevnik is not None:
        await _handle_ezhednevnik_reply(user_id, text, reply_to_text, open_ezhednevnik, telegram_message_id)
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

    if _looks_like_reading_status(text):
        await show_reading_status(user_id)
        return

    # Same shape as activity for steps 0/1 (title, then author). Step 2 is
    # different: once the book note exists and the "расскажи подробнее"
    # template is sent (finalize_book_add_prompt), the row STAYS open for a
    # short window so the very next plain message is still auto-captured as
    # the details answer — same "normal continuation" behavior as every
    # other flow here. That window is 5 minutes, matching
    # release_due_book_add_notices (scheduler.py), which closes it and
    # sends a one-time "Добавил книгу" notice once it lapses; from then on
    # only a reply to that exact template message still works (see
    # app/main.py — that's the whole point of the reply mechanism, to
    # answer after this normal window has expired), matched independently
    # of is_open, so it's unaffected by the close here.
    open_book_add = await get_open_book_add_prompt(user_id)
    if open_book_add is not None:
        timeout = _QUOTE_PROMPT_TIMEOUT if open_book_add["step"] == 2 else _BOOK_ADD_PROMPT_TIMEOUT
        if utcnow() - open_book_add["updated_at"] > timeout:
            await close_book_add_prompt(open_book_add["id"])
            open_book_add = None
    if open_book_add is not None:
        await _handle_book_add_reply(user_id, text, reply_to_text, open_book_add, telegram_message_id)
        return

    if _looks_like_book_add(text):
        await start_book_add_flow(user_id, text, reply_to_text, telegram_message_id)
        return

    received_at = utcnow()
    message_id = await insert_incoming_message(user_id, text, "active", reply_to_text)

    task = asyncio.create_task(
        handle_active_message(message_id, user_id, text, received_at, reply_to_text)
    )
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


async def _handle_ezhednevnik_reply(
    user_id: int, text: str, reply_to_text: str | None, prompt, telegram_message_id: int | None = None,
) -> None:
    """Advance one step in the current slot's question sequence. The reply
    just received answers `prompt["step"]`'s field — store it (a *_score
    field is parsed as a number, best-effort; no number found just means no
    score, never invent one) into `collected`. If more steps remain, ask
    the next one. If that was the last step, write everything gathered to
    Trilium via a direct non-LLM call and close the prompt out. Every
    message here still gets logged to incoming_messages for the audit
    trail, then immediately acked — this never enters handle_active_message()
    at all.

    Tagged with telegram_message_id + entry_date so a later edit to this
    exact Telegram message can retroactively patch just this one cell — see
    handle_message_edit below."""
    slot = prompt["slot"]
    step = prompt["step"]
    steps = EZHEDNEVNIK_STEPS[slot]
    field = steps[step][2]

    # Dated to when the question was actually SENT (its calendar day in
    # Moscow time), not whenever the reply happens to land — confirmed
    # live: a prompt left open overnight and answered the next morning was
    # otherwise stamped with that morning's date, landing the previous
    # day's retrospective in the wrong row entirely. Computed here (not just
    # at the final step) so every step's row carries the same entry_date,
    # needed for handle_message_edit to work on any step, not only the last.
    entry_date = prompt["sent_at"].astimezone(TIMEZONE).date()

    kind = f"ezhednevnik_{slot}_{step}"
    message_id = await insert_incoming_message(
        user_id, text, kind, reply_to_text, telegram_message_id, entry_date,
    )

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
    fields = {"person_name": person_name, "slot": slot, "date": entry_date.isoformat(), **collected}

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


async def handle_message_edit(user_id: int, telegram_message_id: int, new_text: str) -> None:
    """Telegram's edited_message webhook update — see app/main.py. If the
    edited message was a tracked ежедневник answer (has an entry_date, only
    set by _handle_ezhednevnik_reply above), retroactively patch just that
    one cell via fill_ezhednevnik's existing partial-update support (it
    only overwrites the fields actually passed in, leaving the rest of that
    day's row untouched). Anything else (an untracked message, an activity/
    quote reply, a random passive message) is silently ignored — no
    dedicated per-message-type support outside ежедневник, per request."""
    row = await get_incoming_message_by_telegram_id(user_id, telegram_message_id)
    if row is None or row["entry_date"] is None or not row["kind"].startswith("ezhednevnik_"):
        return

    _prefix, slot, step_str = row["kind"].split("_")
    field = EZHEDNEVNIK_STEPS[slot][int(step_str)][2]

    if field.endswith("_score"):
        match = _SCORE_RE.search(new_text)
        if not match:
            await send_message(user_id, "Не расслышал число в правке — запись не обновил.")
            return
        value = max(0, min(100, int(match.group())))
    else:
        value = new_text.strip()

    person_name = USER_NAMES.get(user_id, str(user_id))
    fields = {"person_name": person_name, "slot": slot, "date": row["entry_date"].isoformat(), field: value}
    try:
        await fill_ezhednevnik(fields)
        await update_incoming_message_text(row["id"], new_text)
        await send_message(user_id, "Обновил запись, спасибо.")
    except Exception as exc:
        print(f"handle_message_edit failed for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(
            user_id, f"Не получилось обновить запись (Trilium недоступен): {type(exc).__name__}.",
        )


_SKIP_AUTHOR_WORDS = ("нет", "не знаю", "неизвестно", "не помню")


async def start_book_add_flow(
    user_id: int, text: str, reply_to_text: str | None = None, telegram_message_id: int | None = None,
) -> None:
    """Shared by the free-text trigger (_looks_like_book_add above) and the
    /addbook command (app/main.py). Every message belonging to this
    exchange (starting with the trigger itself) is tracked via
    append_book_add_prompt_message, so it can all be deleted in one shot
    once both stages finish successfully — see _cleanup_book_add_messages."""
    message_id = await insert_incoming_message(user_id, text, "book_add_start", reply_to_text)
    prompt_id = await create_book_add_prompt(user_id)
    if telegram_message_id is not None:
        await append_book_add_prompt_message(prompt_id, telegram_message_id)
    sent_id = await send_message_get_id(user_id, book_add_step_text(0))
    if sent_id is not None:
        await append_book_add_prompt_message(prompt_id, sent_id)
    await ack_incoming_messages([message_id])


async def _cleanup_book_add_messages(user_id: int, prompt_id: int) -> None:
    """Deletes every message tagged onto this /addbook exchange — only
    called once both stages have actually finished successfully (see
    _apply_book_details), per explicit request: unlike /quote's stale-flow
    cleanup, this is never a "you took too long" wipe."""
    prompt = await get_book_add_prompt(prompt_id)
    if prompt is None:
        return
    for message_id in prompt["message_ids"]:
        await delete_message(user_id, message_id)


async def _apply_book_details(user_id: int, prompt_id: int, note_id: str, text: str) -> bool:
    """Shared by both ways of answering the "расскажи подробнее" template —
    the immediate plain-message continuation (step 2 in
    _handle_book_add_reply below) and a later reply (handle_book_details_reply).
    No header matching — text is just split into paragraphs (blank-line
    separated), taken by POSITION in the same order as the template
    (Об Авторе / Аннотация / Жанр / Похожие книги), since the answer always
    mirrors that same paragraph structure. Each paragraph's own first line
    is dropped (the echoed-back header) if it has more than one line.
    Returns whether it succeeded — the caller only deletes the exchange's
    messages on True."""
    raw_paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text.strip()) if p.strip()]
    paragraphs = []
    for block in raw_paragraphs:
        lines = block.split("\n")
        paragraphs.append("\n".join(lines[1:]).strip() if len(lines) > 1 else lines[0].strip())
    values = (paragraphs + [None, None, None, None])[:4]

    try:
        await fill_book_details(note_id, values)
        confirm_id = await send_message_get_id(user_id, "Спасибо, добавил книгу!")
        if confirm_id is not None:
            await append_book_add_prompt_message(prompt_id, confirm_id)
        await _cleanup_book_add_messages(user_id, prompt_id)
        return True
    except Exception as exc:
        print(f"fill_book_details failed for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(
            user_id, f"Не получилось записать подробности (Trilium недоступен): {type(exc).__name__}.",
        )
        return False


async def _handle_book_add_reply(
    user_id: int, text: str, reply_to_text: str | None, prompt, telegram_message_id: int | None = None,
) -> None:
    """step 0 = awaiting title, step 1 = awaiting author (or a skip word
    like "нет"/"не знаю" — author is optional). step 2 = the book already
    exists and the "расскажи подробнее" template was just sent — this is
    the IMMEDIATE-continuation path (the very next plain message, within
    the 5-minute window — see process_incoming_message); a later reply to
    that same template, after the window's closed, is handled separately
    by handle_book_details_reply below."""
    step = prompt["step"]
    kind = f"book_add_{step}"
    message_id = await insert_incoming_message(user_id, text, kind, reply_to_text, telegram_message_id)
    if telegram_message_id is not None:
        await append_book_add_prompt_message(prompt["id"], telegram_message_id)

    if step == 0:
        await set_book_add_title(prompt["id"], text.strip())
        sent_id = await send_message_get_id(user_id, book_add_step_text(1))
        if sent_id is not None:
            await append_book_add_prompt_message(prompt["id"], sent_id)
        await ack_incoming_messages([message_id])
        return

    if step == 2:
        await _apply_book_details(user_id, prompt["id"], prompt["book_note_id"], text)
        await close_book_add_prompt(prompt["id"])
        await ack_incoming_messages([message_id])
        return

    title = prompt["title"]
    author = "" if text.strip().lower() in _SKIP_AUTHOR_WORDS else text.strip()
    person_name = USER_NAMES.get(user_id, str(user_id))
    try:
        note_id = await add_book(person_name, title, author)
        template_message_id = await send_message_get_id(
            user_id, BOOK_DETAILS_TEMPLATE.format(title=title, author=author),
        )
        if template_message_id is not None:
            await finalize_book_add_prompt(prompt["id"], author, note_id, template_message_id)
            await append_book_add_prompt_message(prompt["id"], template_message_id)
        else:
            await close_book_add_prompt(prompt["id"])
    except Exception as exc:
        print(f"add_book failed for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(
            user_id,
            f"Не получилось добавить книгу (Trilium недоступен): {type(exc).__name__}. "
            "Напиши что-нибудь ещё раз чуть позже — я попробую снова.",
        )
    await ack_incoming_messages([message_id])


async def handle_book_details_reply(
    user_id: int, telegram_message_id: int, reply_to_message_id: int,
    text: str, reply_to_text: str | None,
) -> bool:
    """A Telegram reply to a /addbook "расскажи подробнее" template — see
    app/main.py, checked before any other routing since it works no matter
    how much time has passed (get_book_add_prompt_by_template_message
    ignores is_open entirely — this is the "answer after the normal
    5-minute window expired" path; the immediate case is
    _handle_book_add_reply's step==2 above). Returns False (do nothing
    further — let normal routing continue) if this reply doesn't match any
    book_add_prompts row."""
    prompt = await get_book_add_prompt_by_template_message(user_id, reply_to_message_id)
    if prompt is None or prompt["book_note_id"] is None:
        return False

    message_id = await insert_incoming_message(
        user_id, text, "book_add_details", reply_to_text, telegram_message_id,
    )
    await append_book_add_prompt_message(prompt["id"], telegram_message_id)
    await _apply_book_details(user_id, prompt["id"], prompt["book_note_id"], text)
    await close_book_add_prompt(prompt["id"])
    await ack_incoming_messages([message_id])
    return True


async def show_reading_status(user_id: int) -> None:
    """"что я сейчас читаю" / /reading — same book list as /quote
    (get_active_reading_books), but here the button just shows that book's
    description on click (handle_reading_book_selected) instead of
    starting a multi-step flow. Entirely stateless — the note_id is
    encoded directly in the button's callback_data, no DB row needed."""
    try:
        books = await get_active_reading_books()
    except Exception:
        traceback.print_exc()
        await send_message(user_id, TRILIUM_UNAVAILABLE_TEXT)
        return

    if not books:
        await send_message(user_id, "Нет книг в активном чтении (без даты окончания).")
        return

    buttons = [(book["title"], f"rb:{book['note_id']}") for book in books]
    await send_message_with_buttons(user_id, "Какую книгу показать?", buttons)


async def handle_reading_book_selected(callback_query: dict) -> None:
    """Routes a "rb:{note_id}" button press from show_reading_status above
    — called from app/callbacks.py."""
    query_id = callback_query["id"]
    data = callback_query.get("data") or ""
    chat_id = callback_query.get("message", {}).get("chat", {}).get("id")
    note_id = data[len("rb:"):]

    await answer_callback_query(query_id)
    try:
        details = await get_book_details(note_id)
    except Exception as exc:
        print(f"get_book_details failed for user={chat_id}:", flush=True)
        traceback.print_exc()
        await send_message(chat_id, f"Не получилось прочитать описание: {type(exc).__name__}.")
        return

    sections = "\n\n".join(
        f"<b>{html.escape(header)}</b>\n{html.escape(details[header]) if details[header] else '—'}"
        for header in BOOK_DETAIL_HEADERS
    )
    await send_message(chat_id, sections, parse_mode="HTML")


