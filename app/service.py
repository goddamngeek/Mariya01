import asyncio
import html
import json
import re
import traceback
from datetime import datetime, timedelta

from app.config import TIMEZONE
from app.db import (
    ack_incoming_messages,
    advance_activity_prompt_step,
    advance_book_quote_prompt_step,
    advance_ezhednevnik_step,
    append_ezhednevnik_question,
    close_activity_prompt,
    close_book_add_prompt,
    close_book_quote_prompt,
    close_book_review_prompt,
    close_ezhednevnik_prompt,
    close_prompt,
    create_activity_prompt,
    create_book_add_prompt,
    create_book_quote_prompt,
    create_book_review_prompt,
    filter_new_clippings,
    finalize_book_add_prompt,
    get_book_add_prompt_by_template_message,
    get_book_quote_prompt,
    get_ezhednevnik_prompt_by_question_message,
    get_incoming_message_by_telegram_id,
    get_open_ezhednevnik_prompt,
    get_message_thread,
    get_open_prompt,
    get_prompt,
    insert_incoming_message,
    mark_clippings_imported,
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
from app import clippings, humanize, threads, triggers
from app.telegram import (
    answer_callback_query,
    clear_reply_markup,
    send_message,
    send_message_get_id,
)
from app.trilium_client import (
    BOOK_DETAIL_HEADERS,
    add_book,
    add_book_quote,
    create_book_review_note,
    extract_duration,
    fill_book_details,
    fill_ezhednevnik,
    find_book_note_id,
    get_active_reading_books,
    get_book_details,
    get_finished_books,
    get_note_labels,
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

async def process_incoming_message(
    user_id: int, text: str, reply_to_text: str | None = None, telegram_message_id: int | None = None,
) -> None:
    # At most one dialogue can be open at a time, so this is a single
    # lookup across all five kinds rather than one query per kind (see
    # get_open_prompt) — the common case, nothing open at all, used to cost
    # five round trips to establish.
    #
    # An open dialogue always wins over a trigger word: typing "позанималась
    # йогой" while a /quote question is pending answers the question rather
    # than abandoning it half-finished.
    open_prompt = await get_open_prompt(user_id)
    if open_prompt is not None:
        kind = open_prompt["kind"]
        # ежедневник has no inline timeout — the next slot tick closes it
        # (close_open_ezhednevnik_prompts), and even then it stays resumable
        # by replying to one of its questions.
        if kind != "ezhednevnik" and utcnow() - open_prompt["updated_at"] > _PROMPT_TIMEOUT:
            await close_prompt(kind, open_prompt["id"])
        else:
            prompt = await get_prompt(kind, open_prompt["id"])
            if prompt is not None:
                await _PROMPT_HANDLERS[kind](
                    user_id, text, reply_to_text, prompt, telegram_message_id,
                )
                return

    # Which of the dialogue starters this is, if any — precedence across
    # every trigger in the bot lives in app/triggers.py, not here. Anything
    # this stage doesn't own falls through to handle_active_message below,
    # which handles the rest of the names classify() can return.
    trigger = triggers.classify(text)
    if trigger == "activity":
        activity = triggers.activity_kind(text)
        await _start_activity_flow(user_id, text, reply_to_text, activity, telegram_message_id)
        return
    if trigger == "quote":
        await start_quote_flow(user_id, text, reply_to_text, telegram_message_id)
        return
    if trigger == "reading_status":
        await show_reading_status(user_id, telegram_message_id)
        return
    if trigger == "finished_books":
        await show_finished_books(user_id, telegram_message_id)
        return
    if trigger == "book_add":
        await start_book_add_flow(user_id, text, reply_to_text, telegram_message_id)
        return

    received_at = utcnow()
    message_id = await insert_incoming_message(user_id, text, "active", reply_to_text)

    task = asyncio.create_task(
        handle_active_message(message_id, user_id, text, received_at, reply_to_text, telegram_message_id)
    )
    _background_tasks.add(task)
    task.add_done_callback(_background_tasks.discard)


def _recorded_text(entry_date) -> str:
    """Names the day when it isn't today. A check-in answered the next
    morning is filed under the day it was ASKED about, and a bare "Записал,
    спасибо" gave no hint of that — confirmed live: an answer to the
    previous evening's question looked like it had gone missing, when it
    had simply landed in yesterday's row."""
    if entry_date == datetime.now(TIMEZONE).date():
        return "Записал, спасибо."
    return f"Записал за {humanize.format_date(entry_date)}."


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
        question_id = await send_message_get_id(user_id, ezhednevnik_step_text(slot, next_step))
        if question_id is None:
            await close_ezhednevnik_prompt(prompt["id"])
            print(f"failed to send ezhednevnik {slot} step {next_step} to user={user_id}", flush=True)
        else:
            # Tracked so a reply to this exact question can resume the
            # check-in later — see handle_ezhednevnik_question_reply.
            await append_ezhednevnik_question(prompt["id"], question_id)
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
        # step past the last index marks the slot as fully filled in — an
        # abandoned check-in can otherwise sit at that same last index, and
        # a later reply has to tell the two apart.
        await advance_ezhednevnik_step(prompt["id"], len(steps), collected)
        await close_ezhednevnik_prompt(prompt["id"])
        await send_message(user_id, _recorded_text(entry_date))
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
    prompt_id = await create_activity_prompt(user_id, activity, thread_id)
    if await threads.send(thread_id, user_id, activity_step_text(activity, 0)) is None:
        await close_activity_prompt(prompt_id)
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
    if await threads.send(thread_id, user_id, "Какую книгу?", buttons=buttons) is None:
        await close_book_quote_prompt(prompt_id)
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
    if step == 0:
        # Step 0 is answered by a button, not by typing (see
        # handle_quote_book_selected) — nudge rather than swallow the text.
        await send_message(user_id, "Выбери книгу, нажав на кнопку в сообщении выше.")
        return
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


async def resend_ezhednevnik_question(user_id: int) -> bool:
    """/checkin — re-ask the open check-in's current question, tracking the
    new message so replying to it resumes the check-in like any other
    question. Returns False when nothing is open."""
    open_prompt = await get_open_ezhednevnik_prompt(user_id)
    if open_prompt is None:
        return False
    # A "pool"-kind step (see EZHEDNEVNIK_STEPS) picks fresh wording each
    # call rather than re-showing the exact original, which isn't stored
    # anywhere — functionally equivalent either way.
    message_id = await send_message_get_id(
        user_id, ezhednevnik_step_text(open_prompt["slot"], open_prompt["step"]),
    )
    if message_id is not None:
        await append_ezhednevnik_question(open_prompt["id"], message_id)
    return True


async def handle_ezhednevnik_question_reply(
    user_id: int, telegram_message_id: int, reply_to_message_id: int,
    text: str, reply_to_text: str | None,
) -> bool:
    """Replying to any question of a check-in resumes THAT check-in at
    whatever step it stopped on, however long ago and whether or not it is
    still the open one — which is what lets each slot close the previous one
    without anything being lost (see app/db.py's close_open_ezhednevnik_prompts).

    Unambiguous even for the six-step evening slot, because questions are
    asked one at a time: at any moment exactly one of them is unanswered.

    Returns False if this reply isn't to a check-in question at all, so
    normal routing continues."""
    prompt = await get_ezhednevnik_prompt_by_question_message(user_id, reply_to_message_id)
    if prompt is None:
        return False

    if prompt["step"] >= len(EZHEDNEVNIK_STEPS[prompt["slot"]]):
        await send_message(user_id, "Этот чек-ин уже заполнен.")
        return True

    await _handle_ezhednevnik_reply(user_id, text, reply_to_text, prompt, telegram_message_id)
    return True


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
    """Shared by the free-text trigger (triggers.is_book_add) and the
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
    prompt_id = await create_book_add_prompt(user_id, thread_id)
    if await threads.send(thread_id, user_id, book_add_step_text(0)) is None:
        await close_book_add_prompt(prompt_id)
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
        # One fetch of this exact note, not a listing of every active book
        # just to find its title in the result.
        title, labels = await get_note_labels(note_id)
        if labels.get("readingEnd"):
            await send_message(chat_id, "Эта книга уже отмечена как прочитанная.")
            return
        await set_reading_end(note_id)
    except Exception as exc:
        print(f"set_reading_end failed for user={chat_id}:", flush=True)
        traceback.print_exc()
        await send_message(chat_id, f"Не получилось отметить книгу: {type(exc).__name__}.")
        return

    thread_id = thread["id"] if thread is not None else None
    prompt_id = await create_book_review_prompt(chat_id, note_id, title, thread_id)
    if await threads.send(thread_id, chat_id, book_review_step_text(0)) is None:
        await close_book_review_prompt(prompt_id)


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


# Filled in down here because every handler has to be defined first. They
# share one signature — (user_id, text, reply_to_text, prompt,
# telegram_message_id) — so process_incoming_message can dispatch on kind
# without knowing anything else about the flow.
_PROMPT_HANDLERS = {
    "ezhednevnik": _handle_ezhednevnik_reply,
    "activity": _handle_activity_reply,
    "quote": _handle_quote_reply,
    "review": _handle_book_review_reply,
    "book_add": _handle_book_add_reply,
}


async def _offer_book_details(user_id: int, note_id: str, title: str, author: str) -> None:
    """Книга только что заведена по данным из «My Clippings.txt» — шлём тот
    же шаблон «расскажи подробнее», что и /addbook, и подключаем его к той
    же машинерии, чтобы ответ разложился по разделам заметки как обычно."""
    thread_id = await threads.open_thread(
        user_id, threads.TTL_DIALOG, closing_text="Добавил книгу",
    )
    prompt_id = await create_book_add_prompt(user_id, thread_id)
    template_message_id = await threads.send(
        thread_id, user_id, BOOK_DETAILS_TEMPLATE.format(title=title, author=author),
    )
    if template_message_id is not None:
        await finalize_book_add_prompt(prompt_id, author, note_id, template_message_id)
    else:
        await close_book_add_prompt(prompt_id)


async def handle_clippings_file(user_id: int, raw: str) -> None:
    """«My Clippings.txt» с читалки: разобрать, отбросить уже
    импортированное, разложить по книгам в Trilium.

    Комментарий («что понравилось в этом моменте») здесь не спрашивается —
    за один файл приезжает и десяток выделений сразу, и допрос на десять
    сообщений превратил бы удобство в повинность. Прокомментировать
    отдельную цитату всегда можно через /quote."""
    items = clippings.parse(raw)
    if not items:
        await send_message(user_id, "Не нашёл в файле ни одного выделения.")
        return

    fingerprints = [clippings.fingerprint(c) for c in items]
    fresh = await filter_new_clippings(fingerprints)
    new_items = [c for c, fp in zip(items, fingerprints) if fp in fresh]
    if not new_items:
        await send_message(user_id, f"Все {len(items)} выделений уже добавлены.")
        return

    by_book: dict[tuple[str, str], list] = {}
    for c in new_items:
        by_book.setdefault((c.book_title, c.book_author), []).append(c)

    person_name = USER_NAMES.get(user_id, str(user_id))
    imported, per_book, created_books, failed = [], [], [], []

    for (title, author), group in by_book.items():
        try:
            note_id = await find_book_note_id(title)
            is_new_book = note_id is None
            if is_new_book:
                note_id = await add_book(person_name, title, author)

            for c in group:
                await add_book_quote(note_id, c.text, location=c.location)
                imported.append((clippings.fingerprint(c), title))

            per_book.append(f"«{title}» — {len(group)}")
            if is_new_book:
                created_books.append((note_id, title, author))
        except Exception as exc:
            print(f"clippings import failed for {title!r}, user={user_id}:", flush=True)
            traceback.print_exc()
            failed.append(f"«{title}» ({type(exc).__name__})")

    # Отмечаем только то, что реально доехало до Trilium: упавшая книга
    # должна приехать снова при следующей отправке файла, а не потеряться.
    await mark_clippings_imported(imported)

    lines = []
    if imported:
        lines.append(f"Добавил {len(imported)} цитат:")
        lines.extend(f"— {entry}" for entry in per_book)
    if failed:
        lines.append("Не получилось: " + "; ".join(failed))
    if not lines:
        lines.append("Ничего не добавил.")
    await send_message(user_id, "\n".join(lines))

    # Про новые книги спрашиваем подробности — по одной, после отчёта.
    for note_id, title, author in created_books:
        await _offer_book_details(user_id, note_id, title, author)
