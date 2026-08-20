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
    close_activity_prompt,
    close_book_add_prompt,
    close_book_quote_prompt,
    close_book_review_prompt,
    close_ezhednevnik_prompt,
    create_activity_prompt,
    create_book_add_prompt,
    create_book_quote_prompt,
    create_book_review_prompt,
    finalize_book_add_prompt,
    get_book_add_prompt_by_template_message,
    get_book_quote_prompt,
    get_incoming_message_by_telegram_id,
    get_message_thread,
    get_open_activity_prompt,
    get_open_book_add_prompt,
    get_open_book_quote_prompt,
    get_open_book_review_prompt,
    get_open_ezhednevnik_prompt,
    insert_incoming_message,
    set_book_add_title,
    set_book_quote_prompt_book,
    set_book_review_rating,
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
    book_review_step_text,
    ezhednevnik_step_text,
    quote_step_text,
)
from app.telegram import answer_callback_query, clear_reply_markup, send_message
from app import threads
from app.trilium_client import (
    BOOK_DETAIL_HEADERS,
    add_book,
    add_book_quote,
    create_book_review_note,
    extract_duration,
    fill_book_details,
    fill_ezhednevnik,
    get_active_reading_books,
    get_book_details,
    get_finished_books,
    log_activity,
    set_reading_end,
)

# asyncio only holds a weak reference to a task with no other referrer, so an
# unreferenced fire-and-forget task is eligible for GC before it completes
# (documented asyncio behavior) — keep a strong reference here until it's done.
_background_tasks: set[asyncio.Task] = set()

_SCORE_RE = re.compile(r"-?\d+")

# Mirrors threads.TTL_DIALOG deliberately. The thread's own staleness job
# is what actually tears an abandoned dialogue down; this inline check just
# makes sure a reply landing right at the boundary is never treated as
# answering a prompt that is about to be wiped out from under it.
_PROMPT_TIMEOUT = timedelta(minutes=threads.TTL_DIALOG)

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


def _looks_like_finished_books(text: str) -> bool:
    """"прочитанные" / "прочитанных книг" — the "прочитанн" stem covers
    every inflection and doesn't overlap with "читаю" (_looks_like_reading_status)
    or the infinitive forms _looks_like_book_add matches."""
    return "прочитанн" in text.lower()


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
        if utcnow() - open_activity["sent_at"] > _PROMPT_TIMEOUT:
            await close_activity_prompt(open_activity["id"])
            open_activity = None
    if open_activity is not None:
        await _handle_activity_reply(user_id, text, reply_to_text, open_activity, telegram_message_id)
        return

    activity = _looks_like_activity_log(text)
    if activity is not None:
        await _start_activity_flow(user_id, text, reply_to_text, activity, telegram_message_id)
        return

    # Same shape again — an open book_quote_prompt with step >= 1 means this
    # message answers the quote or the impression question. Step 0 (book not
    # picked yet) is driven by a button press instead (see
    # handle_quote_book_selected below, called from app/callbacks.py), so a
    # stray text message while step 0 is still open just gets a nudge to use
    # the button rather than being consumed as an answer.
    open_quote = await get_open_book_quote_prompt(user_id)
    if open_quote is not None:
        if utcnow() - open_quote["updated_at"] > _PROMPT_TIMEOUT:
            await close_book_quote_prompt(open_quote["id"])
            open_quote = None
    if open_quote is not None:
        if open_quote["step"] == 0:
            await send_message(user_id, "Выбери книгу, нажав на кнопку в сообщении выше.")
            return
        await _handle_quote_reply(user_id, text, reply_to_text, open_quote, telegram_message_id)
        return

    if _looks_like_quote_request(text):
        await start_quote_flow(user_id, text, reply_to_text, telegram_message_id)
        return

    if _looks_like_reading_status(text):
        await show_reading_status(user_id, telegram_message_id)
        return

    if _looks_like_finished_books(text):
        await show_finished_books(user_id, telegram_message_id)
        return

    # "Я дочитал" — started by a button under a book's description (see
    # handle_book_finished), then two plain steps: rating, then free text.
    # Same 5-minute staleness rule as /quote.
    open_review = await get_open_book_review_prompt(user_id)
    if open_review is not None:
        if utcnow() - open_review["updated_at"] > _PROMPT_TIMEOUT:
            await close_book_review_prompt(open_review["id"])
            open_review = None
    if open_review is not None:
        await _handle_book_review_reply(user_id, text, reply_to_text, open_review, telegram_message_id)
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
        if utcnow() - open_book_add["updated_at"] > _PROMPT_TIMEOUT:
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
        handle_active_message(message_id, user_id, text, received_at, reply_to_text, telegram_message_id)
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
    telegram_message_id: int | None = None,
) -> None:
    """The trigger message itself ("позанималась йогой") carries no data —
    just log it for the audit trail, open the prompt, and ask the first
    question."""
    message_id = await insert_incoming_message(user_id, text, f"activity_{activity}_start", reply_to_text)
    thread_id = await threads.open_thread(user_id, threads.TTL_DIALOG, telegram_message_id)
    await create_activity_prompt(user_id, activity, thread_id)
    await threads.send(thread_id, user_id, activity_step_text(activity, 0))
    await ack_incoming_messages([message_id])


async def _handle_activity_reply(
    user_id: int, text: str, reply_to_text: str | None, prompt,
    telegram_message_id: int | None = None,
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
    thread_id = prompt["thread_id"]
    message_id = await insert_incoming_message(user_id, text, kind, reply_to_text, telegram_message_id)
    await threads.track(thread_id, telegram_message_id)

    collected = json.loads(prompt["collected"] or "{}")
    if field == "score":
        match = _SCORE_RE.search(text)
        if not match:
            # Never invent a score — re-ask instead of defaulting to 0 or
            # completing with a missing value.
            await threads.send(thread_id, user_id, "Не расслышал число — " + activity_step_text(activity, step))
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
        await threads.send(thread_id, user_id, activity_step_text(activity, next_step))
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
        await threads.send(thread_id, user_id, "Записал, спасибо.")
        await _dismiss_thread_by_id(thread_id)
    except Exception as exc:
        print(f"log_activity failed for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(
            user_id,
            f"Не получилось записать (Trilium недоступен): {type(exc).__name__}. "
            "Напиши что-нибудь ещё раз чуть позже — я попробую снова.",
        )
    await ack_incoming_messages([message_id])


async def start_quote_flow(
    user_id: int, text: str = "цитата", reply_to_text: str | None = None,
    telegram_message_id: int | None = None,
) -> None:
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

    thread_id = await threads.open_thread(user_id, threads.TTL_DIALOG, telegram_message_id)
    prompt_id = await create_book_quote_prompt(user_id, books, thread_id)
    buttons = [(book["title"], f"bq:{prompt_id}:{i}") for i, book in enumerate(books)]
    await threads.send(thread_id, user_id, "Какую книгу?", buttons=buttons)
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
    list_message_id = (callback_query.get("message") or {}).get("message_id")
    if list_message_id is not None:
        await clear_reply_markup(chat_id, list_message_id)
    await threads.send(prompt["thread_id"], chat_id, quote_step_text(0))


async def _handle_quote_reply(
    user_id: int, text: str, reply_to_text: str | None, prompt,
    telegram_message_id: int | None = None,
) -> None:
    """step 1 = awaiting the quote text, step 2 = awaiting the impression —
    same shape as _handle_activity_reply."""
    step = prompt["step"]
    thread_id = prompt["thread_id"]
    kind = f"quote_{step}"
    message_id = await insert_incoming_message(user_id, text, kind, reply_to_text, telegram_message_id)
    await threads.track(thread_id, telegram_message_id)

    collected = json.loads(prompt["collected"] or "{}")
    if step == 1:
        collected["quote"] = text.strip()
        await advance_book_quote_prompt_step(prompt["id"], 2, collected)
        await threads.send(thread_id, user_id, quote_step_text(1))
        await ack_incoming_messages([message_id])
        return

    collected["impression"] = text.strip()
    # Close only on SUCCESS, same reasoning as ежедневник/activity — a
    # failed write must not lose the quote already collected.
    try:
        await add_book_quote(prompt["book_note_id"], collected["quote"], collected["impression"])
        await close_book_quote_prompt(prompt["id"])
        await threads.send(thread_id, user_id, "Записал, спасибо.")
        await _dismiss_thread_by_id(thread_id)
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
    # closing_text: if the "расскажи подробнее" template goes unanswered,
    # the thread signs off with this before tearing itself down, so the
    # book being added isn't left unconfirmed.
    thread_id = await threads.open_thread(
        user_id, threads.TTL_DIALOG, telegram_message_id, closing_text="Добавил книгу",
    )
    await create_book_add_prompt(user_id, thread_id)
    await threads.send(thread_id, user_id, book_add_step_text(0))
    await ack_incoming_messages([message_id])


async def _apply_book_details(user_id: int, thread_id: int | None, note_id: str, text: str) -> bool:
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
        await threads.send(thread_id, user_id, "Спасибо, добавил книгу!")
        await _dismiss_thread_by_id(thread_id)
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
    thread_id = prompt["thread_id"]
    kind = f"book_add_{step}"
    message_id = await insert_incoming_message(user_id, text, kind, reply_to_text, telegram_message_id)
    await threads.track(thread_id, telegram_message_id)

    if step == 0:
        await set_book_add_title(prompt["id"], text.strip())
        await threads.send(thread_id, user_id, book_add_step_text(1))
        await ack_incoming_messages([message_id])
        return

    if step == 2:
        await _apply_book_details(user_id, thread_id, prompt["book_note_id"], text)
        await close_book_add_prompt(prompt["id"])
        await ack_incoming_messages([message_id])
        return

    title = prompt["title"]
    author = "" if text.strip().lower() in _SKIP_AUTHOR_WORDS else text.strip()
    person_name = USER_NAMES.get(user_id, str(user_id))
    try:
        note_id = await add_book(person_name, title, author)
        template_message_id = await threads.send(
            thread_id, user_id, BOOK_DETAILS_TEMPLATE.format(title=title, author=author),
        )
        if template_message_id is not None:
            await finalize_book_add_prompt(prompt["id"], author, note_id, template_message_id)
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
    await threads.track(prompt["thread_id"], telegram_message_id)
    await _apply_book_details(user_id, prompt["thread_id"], prompt["book_note_id"], text)
    await close_book_add_prompt(prompt["id"])
    await ack_incoming_messages([message_id])
    return True


async def _show_book_list(
    user_id: int, fetch, prefix: str, empty_text: str, trigger_message_id: int | None,
) -> None:
    """Shared by /reading and /finished — both just list books as buttons,
    differing only in which set they fetch and what the buttons do on click.
    Opens a message_thread covering the whole interaction (the trigger, this
    list, the description, and any review dialogue that follows), so it can
    all be cleared away together later."""
    try:
        books = await fetch()
    except Exception:
        traceback.print_exc()
        await send_message(user_id, TRILIUM_UNAVAILABLE_TEXT)
        return

    if not books:
        await send_message(user_id, empty_text)
        return

    thread_id = await threads.open_thread(user_id, threads.TTL_DIALOG, trigger_message_id)
    buttons = [(book["title"], f"{prefix}:{book['note_id']}") for book in books]
    await threads.send(thread_id, user_id, "Какую книгу показать?", buttons=buttons)


async def show_reading_status(user_id: int, trigger_message_id: int | None = None) -> None:
    """"что я сейчас читаю" / /reading — same book list as /quote
    (get_active_reading_books), but here the button just shows that book's
    description on click (handle_reading_book_selected) instead of
    starting a multi-step flow."""
    await _show_book_list(
        user_id, get_active_reading_books, "rb",
        "Нет книг в активном чтении (без даты окончания).", trigger_message_id,
    )


async def show_finished_books(user_id: int, trigger_message_id: int | None = None) -> None:
    """"прочитанные" / /finished — the mirror of show_reading_status, for
    books that already have a readingEnd date. Same description on click
    (handle_finished_book_selected), minus the "Я дочитал" button, which
    would be meaningless on an already-finished book."""
    await _show_book_list(
        user_id, get_finished_books, "pb", "Пока нет прочитанных книг.", trigger_message_id,
    )


async def _handle_book_list_press(callback_query: dict, prefix: str, with_finish_button: bool) -> None:
    """Shared by both lists' button handlers: strip the list's keyboard (so
    a second book can't be picked from the same message), then send that
    book's description — always leading with the book's own title (per
    request), then the 4 template sections."""
    await answer_callback_query(callback_query["id"])
    data = callback_query.get("data") or ""
    message = callback_query.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    list_message_id = message.get("message_id")
    note_id = data[len(prefix) + 1:]

    thread = None
    if list_message_id is not None:
        await clear_reply_markup(chat_id, list_message_id)
        thread = await threads.thread_for_message(chat_id, list_message_id)
    thread_id = thread["id"] if thread is not None else None

    try:
        title, details = await get_book_details(note_id)
    except Exception as exc:
        print(f"get_book_details failed for user={chat_id}:", flush=True)
        traceback.print_exc()
        await send_message(chat_id, f"Не получилось прочитать описание: {type(exc).__name__}.")
        return

    sections = "\n\n".join(
        f"<b>{html.escape(header)}</b>\n{html.escape(details[header]) if details[header] else '—'}"
        for header in BOOK_DETAIL_HEADERS
    )
    body = f"<b>{html.escape(title)}</b>\n\n{sections}"
    buttons = [("Я дочитал", f"fd:{note_id}")] if with_finish_button else None
    await threads.send(thread_id, chat_id, body, parse_mode="HTML", buttons=buttons)


async def handle_reading_book_selected(callback_query: dict) -> None:
    """Routes a "rb:{note_id}" button press from show_reading_status above
    — called from app/callbacks.py."""
    await _handle_book_list_press(callback_query, "rb", with_finish_button=True)


async def handle_finished_book_selected(callback_query: dict) -> None:
    """Routes a "pb:{note_id}" button press from show_finished_books above."""
    await _handle_book_list_press(callback_query, "pb", with_finish_button=False)


async def handle_message_reaction(user_id: int, message_id: int) -> None:
    """A reaction on any message in an open /reading or /finished thread
    dismisses the whole thread on the spot — the deliberate shortcut for
    "I'm done looking at this", instead of waiting out the 5-minute
    staleness window (see scheduler.py's release_stale_message_threads,
    which does exactly the same thing on a timer). A reaction anywhere
    else is ignored."""
    thread = await threads.thread_for_message(user_id, message_id)
    if thread is None:
        return
    await threads.dismiss(thread)


async def _dismiss_thread_by_id(thread_id: int | None) -> None:
    """The success path into a thread's terminal state — the flow finished
    and wrote what it collected, so the conversation has served its purpose.
    No closing_text here: the flow just sent its own confirmation."""
    if thread_id is None:
        return
    thread = await get_message_thread(thread_id)
    if thread is not None and thread["is_open"]:
        await threads.dismiss(thread)


async def handle_book_finished(callback_query: dict) -> None:
    """"Я дочитал" — the button under a book's description (see
    handle_reading_book_selected above). Stamps readingEnd on the book
    IMMEDIATELY, before asking anything (per explicit request), so an
    abandoned review dialogue still leaves the book correctly marked as
    finished — it just won't get a review note. Then opens the two-step
    rating/review flow (_handle_book_review_reply below)."""
    query_id = callback_query["id"]
    data = callback_query.get("data") or ""
    message = callback_query.get("message") or {}
    chat_id = (message.get("chat") or {}).get("id")
    description_message_id = message.get("message_id")
    note_id = data[len("fd:"):]

    await answer_callback_query(query_id)

    thread = None
    if description_message_id is not None:
        await clear_reply_markup(chat_id, description_message_id)
        thread = await threads.thread_for_message(chat_id, description_message_id)

    try:
        books = await get_active_reading_books()
        title = next((b["title"] for b in books if b["note_id"] == note_id), None)
        if title is None:
            await send_message(chat_id, "Эта книга уже отмечена как прочитанная.")
            return
        await set_reading_end(note_id)
    except Exception as exc:
        print(f"set_reading_end failed for user={chat_id}:", flush=True)
        traceback.print_exc()
        await send_message(chat_id, f"Не получилось отметить книгу: {type(exc).__name__}.")
        return

    thread_id = thread["id"] if thread is not None else None
    await create_book_review_prompt(chat_id, note_id, title, thread_id)
    await threads.send(thread_id, chat_id, book_review_step_text(0))


async def _handle_book_review_reply(
    user_id: int, text: str, reply_to_text: str | None, prompt, telegram_message_id: int | None = None,
) -> None:
    """step 0 = awaiting a 1-10 rating, step 1 = awaiting the free-text
    review. Out-of-range or non-numeric ratings re-ask rather than being
    clamped or invented, same as the activity tracker's score step."""
    step = prompt["step"]
    thread_id = prompt["thread_id"]
    message_id = await insert_incoming_message(
        user_id, text, f"book_review_{step}", reply_to_text, telegram_message_id,
    )
    await threads.track(thread_id, telegram_message_id)

    if step == 0:
        match = _SCORE_RE.search(text)
        rating = int(match.group()) if match else None
        if rating is None or not 1 <= rating <= 10:
            await threads.send(
                thread_id, user_id, "Нужно число от 1 до 10 — " + book_review_step_text(0),
            )
            await ack_incoming_messages([message_id])
            return
        await set_book_review_rating(prompt["id"], rating)
        await threads.send(thread_id, user_id, book_review_step_text(1))
        await ack_incoming_messages([message_id])
        return

    person_name = USER_NAMES.get(user_id, str(user_id))
    try:
        await create_book_review_note(
            prompt["book_note_id"], prompt["book_title"], prompt["rating"], text.strip(), person_name,
        )
        await close_book_review_prompt(prompt["id"])
        await threads.send(thread_id, user_id, "Спасибо, записал отзыв!")
        await _dismiss_thread_by_id(thread_id)
    except Exception as exc:
        print(f"create_book_review_note failed for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(
            user_id,
            f"Не получилось записать отзыв (Trilium недоступен): {type(exc).__name__}. "
            "Напиши что-нибудь ещё раз чуть позже — я попробую снова.",
        )
    await ack_incoming_messages([message_id])


