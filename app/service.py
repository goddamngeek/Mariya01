import html
import json
import re
import traceback
from datetime import date as _date, datetime, timedelta
from typing import Optional

from app.config import (
    LEDGER_ACCOUNTS,
    LEDGER_CATEGORIES,
    LEDGER_DEFAULT_CATEGORY,
    TIMEZONE,
    ledger_label,
)
from app.db import (
    ack_incoming_messages,
    create_inbox_session,
    get_inbox_session,
    mark_inbox_handled,
    touch_message_thread,
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
    _PROMPT_TABLES,
    open_link_add_prompt,
    open_task_add_prompt,
    create_activity_prompt,
    create_book_add_prompt,
    create_book_quote_prompt,
    create_book_review_prompt,
    create_media_add_prompt,
    create_media_review_prompt,
    close_media_add_prompt,
    get_media_add_prompt,
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
from app.press import Press
from app.people import USER_NAMES, dative
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
from app import background, clippings, errors, humanize, ledger, media, threads, triggers
from app import tmdb_client
from app.channel import (
    answer_callback_query,
    clear_reply_markup,
    edit_message,
    send_message,
    send_message_get_id,
)
from app.trilium_client import (
    add_media,
    fill_media,
    find_media_by_title,
    create_media_review_note,
    get_media_details,
    list_media,
    mark_media,
    BOOK_DETAIL_HEADERS,
    SEED_LINKS,
    add_book,
    add_book_quote,
    add_book_quotes,
    add_kanban_card,
    add_link,
    KANBAN_DONE,
    set_card_label,
    create_book_review_note,
    extract_duration,
    fill_book_details,
    fill_ezhednevnik,
    get_active_reading_books,
    get_book_details,
    get_book_note_ids,
    get_book_quotes,
    get_finished_books,
    get_links,
    get_planner_cards,
    get_note_labels,
    log_activity,
    normalize_book_title,
    set_reading_end,
)

_RETRY_HINT = " Напиши что-нибудь ещё раз чуть позже — я попробую снова."


async def _report_failure(
    user_id: int, label: str, text: str, exc: Exception, retry: bool = False,
) -> None:
    """Every flow fails the same way and used to say so in its own copy of
    these four lines: the traceback goes to the log for us, the person gets
    the plain reason it broke, and — where the prompt is still open, so
    answering again really does retry the write — the hint that it will.

    retry=False for the paths where nothing is left open to answer into: a
    read that failed, or an edit that patches an already-closed entry."""
    print(f"{label} failed for user={user_id}:", flush=True)
    traceback.print_exc()
    errors.record(label, exc)
    await send_message(user_id, f"{text}: {type(exc).__name__}." + (_RETRY_HINT if retry else ""))


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
    if trigger == "watch":
        # Фильм или сериал решает слово «сериал» в самой фразе: по названию
        # их не различить, а спрашивать каждый раз — лишний вопрос.
        kind = media.SERIES if triggers.is_series_text(text) else media.FILM
        await start_watch_intent(
            user_id, kind, triggers.watch_intent(text), triggers.watch_title(text),
            telegram_message_id,
        )
        return
    if trigger == "expense":
        await start_expense_flow(user_id, text, telegram_message_id)
        return
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

    background.spawn(
        handle_active_message(
            message_id, user_id, text, received_at, reply_to_text, telegram_message_id,
        ),
        "active",
    )


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
        await _report_failure(
            user_id, "fill_ezhednevnik",
            "Не получилось записать (Trilium недоступен)", exc, retry=True,
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
        await _report_failure(
            user_id, "log_activity",
            "Не получилось записать (Trilium недоступен)", exc, retry=True,
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


async def handle_quote_book_selected(press: Press) -> None:
    """Routes a "bq:{prompt_id}:{index}" button press from start_quote_flow
    above back to the matching book — called from app/callbacks.py."""
    query_id = press.id
    data = press.data
    chat_id = press.chat_id

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
    list_message_id = press.message_id
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
        await _report_failure(
            user_id, "add_book_quote",
            "Не получилось записать (Trilium недоступен)", exc, retry=True,
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
        await _report_failure(
            user_id, "handle_message_edit",
            "Не получилось обновить запись (Trilium недоступен)", exc,
        )


_SKIP_AUTHOR_WORDS = ("нет", "не знаю", "неизвестно", "не помню")


async def start_book_add_flow(
    user_id: int, text: str, reply_to_text: str | None = None, telegram_message_id: int | None = None,
) -> None:
    """Shared by the free-text trigger (triggers.is_book_add) and the
    /addbook command (app/main.py). Every message belonging to this
    exchange (starting with the trigger itself) is tracked on its thread
    (app/threads.py), so it all goes away in one shot when the thread
    reaches its terminal state."""
    message_id = await insert_incoming_message(user_id, text, "book_add_start", reply_to_text)
    # closing_text: if the "расскажи подробнее" template goes unanswered,
    # the thread signs off with this before tearing itself down, so the
    # book being added isn't left unconfirmed.
    thread_id = await threads.open_thread(
        user_id, threads.TTL_BOOK_DETAILS, telegram_message_id, closing_text="Добавил книгу",
    )
    prompt_id = await create_book_add_prompt(user_id, thread_id)
    if await threads.send(thread_id, user_id, book_add_step_text(0)) is None:
        await close_book_add_prompt(prompt_id)
    await ack_incoming_messages([message_id])


_DETAIL_HEADER_RE = re.compile(
    r"^\s*[*_#>\-\s]*(Об\s+Авторе|Аннотация|Жанр|Похожие\s+книги)\s*:?\s*[*_]*\s*$",
    re.IGNORECASE,
)


def _normalize_header(line: str) -> Optional[str]:
    """Which of the four sections this line announces, if it is a bare
    header line and nothing else. Tolerant of the shapes people and LLMs
    actually produce around a heading — a trailing colon, **bold**, a
    leading «#» or «-» — but deliberately not of a header with text on the
    same line, which is content, not a boundary."""
    match = _DETAIL_HEADER_RE.match(line)
    if match is None:
        return None
    collapsed = re.sub(r"\s+", " ", match.group(1)).strip().lower()
    return next(
        (h for h in BOOK_DETAIL_HEADERS if h.lower() == collapsed), None,
    )


def split_book_details(text: str) -> list[Optional[str]]:
    """The person's answer to the "расскажи подробнее" template, cut into
    the four sections in template order.

    Splitting on the headers comes first, and positional paragraphs are the
    fallback. That ordering is the opposite of what it was, because the
    original positional-only rule assumed the answer would arrive as four
    blank-line-separated paragraphs — and in practice it doesn't. The
    template itself lists its four headers on four consecutive lines with
    no blank line between them, so that's the shape it invites back: bare
    header lines with the text under each, single newlines throughout. Such
    an answer is one paragraph, and the whole thing landed under «Об
    Авторе» while the other three kept their placeholders.

    Headers are only trusted as boundaries when at least two of them show
    up on their own lines; one stray line matching a header word inside
    otherwise free-form prose shouldn't get to restructure the answer. Below
    that bar we fall back to the original behaviour exactly: paragraphs by
    position, each one's echoed-back header line dropped."""
    lines = text.strip().splitlines()
    found = [(i, h) for i, line in enumerate(lines) if (h := _normalize_header(line))]

    if len(found) >= 2:
        sections: dict[str, str] = {}
        for pos, (line_index, header) in enumerate(found):
            end = found[pos + 1][0] if pos + 1 < len(found) else len(lines)
            body = "\n".join(lines[line_index + 1:end]).strip()
            # First header wins a duplicate: re-stating one usually means
            # the answer wandered back to it, not that it should be replaced.
            if body and header not in sections:
                sections[header] = body
        return [sections.get(header) for header in BOOK_DETAIL_HEADERS]

    raw_paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text.strip()) if p.strip()]
    paragraphs = []
    for block in raw_paragraphs:
        block_lines = block.split("\n")
        # Drop the paragraph's own first line only when it's the echoed-back
        # header — the old rule dropped it whenever a paragraph had more than
        # one line, which silently ate the first line of any multi-line answer.
        if len(block_lines) > 1 and _normalize_header(block_lines[0]):
            paragraphs.append("\n".join(block_lines[1:]).strip())
        else:
            paragraphs.append(block.strip())
    return (paragraphs + [None, None, None, None])[:4]


async def _apply_book_details(user_id: int, thread_id: int | None, note_id: str, text: str) -> bool:
    """Shared by both ways of answering the "расскажи подробнее" template —
    the plain-message continuation (step 2 in _handle_book_add_reply below)
    and a reply to the template itself (handle_book_details_reply).
    Returns whether it succeeded — the caller only deletes the exchange's
    messages on True."""
    values = split_book_details(text)

    try:
        await fill_book_details(note_id, values)
        await threads.send(thread_id, user_id, "Спасибо, добавил книгу!")
        await _dismiss_thread_by_id(thread_id)
        return True
    except Exception as exc:
        await _report_failure(
            user_id, "fill_book_details",
            "Не получилось записать подробности (Trilium недоступен)", exc,
        )
        return False


async def _handle_book_add_reply(
    user_id: int, text: str, reply_to_text: str | None, prompt, telegram_message_id: int | None = None,
) -> None:
    """step 0 = awaiting title, step 1 = awaiting author (or a skip word
    like "нет"/"не знаю" — author is optional). step 2 = the book already
    exists and the "расскажи подробнее" template was just sent — this is
    the IMMEDIATE-continuation path (the very next plain message, within
    the 5-minute window — see process_incoming_message); a Telegram reply to
    that same template goes through handle_book_details_reply below instead,
    which is what keeps several outstanding templates apart."""
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
        await _report_failure(
            user_id, "add_book",
            "Не получилось добавить книгу (Trilium недоступен)", exc, retry=True,
        )
    await ack_incoming_messages([message_id])


async def handle_book_details_reply(
    user_id: int, telegram_message_id: int, reply_to_message_id: int,
    text: str, reply_to_text: str | None,
) -> bool:
    """A Telegram reply to a /addbook "расскажи подробнее" template — see
    app/main.py, checked before any other routing.

    What this path is FOR, now that the thread deletes the template after
    five minutes: telling several outstanding templates apart. The clippings
    import can add a few books at once and sends a template for each (see
    _offer_book_details), and a plain message only ever answers the most
    recently touched prompt — the replied-to message_id is what says which
    book is meant. With a single book open it is just a longer way round to
    _handle_book_add_reply's step==2.

    get_book_add_prompt_by_template_message ignores is_open, which still
    earns its keep: a details write that failed closes the row without
    dismissing the thread, so replying to the template again retries it.

    Returns False (do nothing further — let normal routing continue) if this
    reply doesn't match any book_add_prompts row."""
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


# Telegram rejects a sendMessage over this many characters outright.
_TELEGRAM_TEXT_LIMIT = 4096


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
    buttons = [(_book_label(book), f"{prefix}:{book['note_id']}") for book in books]
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


def _reading_dates(labels: dict) -> str:
    """«Читаю с 12.03.2026» или «12.03.2026 — 28.08.2026» под названием книги.

    Даты лежат лейблами readingStart/readingEnd в формате Trilium
    (ГГГГ-ММ-ДД). Проверяем значение на истинность, а не наличие метки:
    очищенная в интерфейсе дата остаётся пустой строкой, а не исчезает.

    Год показываем всегда: книгу читают месяцами, и «12 марта» без года
    здесь двусмысленно, в отличие от подтверждений внутри одного дня."""
    def fmt(raw: str) -> str:
        try:
            return datetime.strptime(raw, "%Y-%m-%d").strftime("%d.%m.%Y")
        except ValueError:
            return raw

    start, end = labels.get("readingStart") or "", labels.get("readingEnd") or ""
    if start and end:
        return f"{fmt(start)} — {fmt(end)}"
    if start:
        return f"Читаю с {fmt(start)}"
    if end:
        return f"Дочитал {fmt(end)}"
    return ""


def _book_label(book: dict) -> str:
    """«Автор — Название», или просто название, если автора нет."""
    author = (book.get("author") or "").strip()
    return f"{author} — {book['title']}" if author else book["title"]


def _ratings(reviews: list[tuple[str, str]]) -> str:
    """«8/10» или «Остап 8/10 · Маша 9/10», если книгу оценили оба.

    Имя подписывается только когда оценок больше одной: у своей книги
    подпись «Остап» лишняя, а у общей без неё непонятно, чья цифра."""
    if not reviews:
        return ""
    if len(reviews) == 1:
        return f"{reviews[0][1]}/10"
    return " · ".join(
        f"{owner.capitalize()} {rating}/10" if owner else f"{rating}/10"
        for owner, rating in reviews
    )


async def _handle_book_list_press(press: Press, prefix: str, with_finish_button: bool) -> None:
    """Shared by both lists' button handlers: strip the list's keyboard (so
    a second book can't be picked from the same message), then send that
    book's description — always leading with the book's own title (per
    request), then the 4 template sections."""
    await answer_callback_query(press.id)
    data = press.data
    chat_id = press.chat_id
    list_message_id = press.message_id
    note_id = data[len(prefix) + 1:]

    thread = None
    if list_message_id is not None:
        await clear_reply_markup(chat_id, list_message_id)
        thread = await threads.thread_for_message(chat_id, list_message_id)
    thread_id = thread["id"] if thread is not None else None

    try:
        title, details, quotes, labels, reviews = await get_book_details(note_id)
    except Exception as exc:
        await _report_failure(chat_id, "get_book_details", "Не получилось прочитать описание", exc)
        return

    sections = "\n\n".join(
        f"<b>{html.escape(header)}</b>\n{html.escape(details[header]) if details[header] else '—'}"
        for header in BOOK_DETAIL_HEADERS
    )
    header = f"<b>{html.escape(_book_label({'title': title, 'author': labels.get('author', '')}))}</b>"
    subtitle = " · ".join(p for p in (_reading_dates(labels), _ratings(reviews)) if p)
    if subtitle:
        header += f"\n<i>{html.escape(subtitle)}</i>"
    body = f"{header}\n\n{sections}"
    # Sections are free text someone typed, so nothing bounds their length.
    # Over Telegram's limit the send fails outright and the button press
    # looks like it did nothing at all; a visibly cut description is the
    # better failure. Cut on a line boundary so no HTML tag is split in half.
    if len(body) > _TELEGRAM_TEXT_LIMIT:
        keep = body[:_TELEGRAM_TEXT_LIMIT - 40]
        body = keep[:keep.rfind("\n")] + "\n\n<i>…описание не поместилось целиком.</i>"
    # The moments go behind a button rather than into this message: a book
    # with a dozen highlights runs several times longer than its whole
    # description, and burying the description under them helps nobody. The
    # count is on the button so it's worth pressing knowingly.
    buttons = []
    if quotes:
        buttons.append((f"Интересные моменты ({len(quotes)})", f"im:{note_id}"))
    # Заполнить или переписать четыре раздела шаблона можно в любой момент,
    # а не только сразу после /addbook: диалог там живёт десять минут, и
    # раньше не успевший его дозаполнить оставался с книгой в прочерках
    # навсегда.
    buttons.append(("Описание", f"bd:{note_id}"))
    if with_finish_button:
        buttons.append(("Я дочитал", f"fd:{note_id}"))
    await threads.send(thread_id, chat_id, body, parse_mode="HTML", buttons=buttons or None)


_URL_RE = re.compile(r"https?://\S+")
# Separators people put between a name and its address, left dangling once
# the URL is cut out of the line: "DNS — https://…", "Хостинг: https://…".
_DANGLING_RE = re.compile(r"^[\s\-–—:,|]+|[\s\-–—:,|]+$")

ADDLINK_USAGE = "Пришли название и ссылку следующим сообщением."


def parse_link(text: str) -> tuple[str, str] | None:
    """A name and a URL out of one line, in either order. The URL is found
    by its scheme rather than by position, so «DNS — https://…» and
    «https://… — DNS» both work and no separator needs agreeing on. Without
    a name the host stands in for one, which is worth more than refusing."""
    match = _URL_RE.search(text)
    if match is None:
        return None
    url = match.group(0).rstrip(".,;)")
    label = _DANGLING_RE.sub("", text.replace(match.group(0), " ")).strip()
    if not label:
        label = url.split("//", 1)[-1].split("/", 1)[0]
    return label, url


def _render_links(links) -> str:
    return "\n".join(f"{label}: {url}" for label, url in links)


async def show_links(user_id: int, trigger_message_id: int | None = None) -> None:
    """/links — read out of the ССЫЛКИ note in Trilium.

    Falls back to the seeded infrastructure links when Trilium can't be
    reached, since the single most likely reason someone wants this list is
    that Trilium is the thing that's broken."""
    thread_id = await threads.open_thread(user_id, threads.TTL_INFO, trigger_message_id)
    try:
        links = await get_links()
        text = _render_links(links) if links else "Пока ни одной ссылки. Добавить: /addlink"
    except Exception:
        traceback.print_exc()
        text = "Trilium недоступен, показываю сохранённые:\n\n" + _render_links(SEED_LINKS)
    await threads.send(thread_id, user_id, text)


TASK_USAGE = "Что за задача? Напиши следующим сообщением."


async def _write_task(user_id: int, thread_id: int | None, title: str) -> bool:
    """Завести карточку. False — если текста нет."""
    title = title.strip()
    if not title:
        return False
    try:
        await add_kanban_card(USER_NAMES.get(user_id, str(user_id)), title)
    except Exception:
        traceback.print_exc()
        await threads.send(thread_id, user_id, TRILIUM_UNAVAILABLE_TEXT)
        return True
    await threads.send(thread_id, user_id, "Записал в инбокс. Разобрать: /inbox")
    return True


async def add_task_from_command(
    user_id: int, text: str, trigger_message_id: int | None = None,
) -> None:
    """/task купить молоко — карточка сразу, без модели.

    Всегда в БЭКЛОГ, то есть в инбокс: день назначается разбором, а не при
    заведении. В этом весь смысл инбокса — кидать не думая."""
    thread_id = await threads.open_thread(user_id, threads.TTL_DIALOG, trigger_message_id)
    if await _write_task(user_id, thread_id, text[len("/task"):]):
        await _dismiss_thread_by_id(thread_id)
        return
    await open_task_add_prompt(user_id, thread_id)
    await threads.send(thread_id, user_id, TASK_USAGE)


async def _handle_task_add_reply(
    user_id: int, text: str, reply_to_text: str | None, prompt, telegram_message_id: int | None,
) -> None:
    thread_id = prompt["thread_id"]
    await threads.track(thread_id, telegram_message_id)
    if not await _write_task(user_id, thread_id, text):
        await threads.send(thread_id, user_id, TASK_USAGE)
        return
    await close_prompt("task_add", prompt["id"])
    await _dismiss_thread_by_id(thread_id)


async def _write_link(user_id: int, thread_id: int | None, text: str) -> bool:
    """Разобрать и записать. False — если ссылки в тексте не нашлось."""
    parsed = parse_link(text)
    if parsed is None:
        return False
    label, url = parsed
    try:
        added = await add_link(label, url)
    except Exception:
        traceback.print_exc()
        await threads.send(thread_id, user_id, TRILIUM_UNAVAILABLE_TEXT)
        return True
    await threads.send(
        thread_id, user_id, "Записал" if added else f"Такая ссылка уже есть: {url}",
    )
    return True


async def add_link_from_command(
    user_id: int, text: str, trigger_message_id: int | None = None,
) -> None:
    """/addlink Название https://… — одним сообщением.

    Без аргументов команда не отвечает подсказкой в пустоту, а начинает
    ждать следующее сообщение: подсказка сама просит его прислать, и
    отвечать на неё — ровно то, что человек делает. Раньше ожидания не
    было, и присланное следом уходило в Odysseus и оседало заметкой в
    журнале вместо списка ссылок."""
    thread_id = await threads.open_thread(user_id, threads.TTL_DIALOG, trigger_message_id)
    if await _write_link(user_id, thread_id, text[len("/addlink"):]):
        await _dismiss_thread_by_id(thread_id)
        return
    await open_link_add_prompt(user_id, thread_id)
    await threads.send(thread_id, user_id, ADDLINK_USAGE)


async def _handle_link_add_reply(
    user_id: int, text: str, reply_to_text: str | None, prompt, telegram_message_id: int | None,
) -> None:
    """Ответ на подсказку /addlink. Ссылки в тексте нет — переспрашиваем, не
    закрывая ожидание: человек мог просто промахнуться."""
    thread_id = prompt["thread_id"]
    await threads.track(thread_id, telegram_message_id)
    if not await _write_link(user_id, thread_id, text):
        await threads.send(thread_id, user_id, "Не вижу здесь ссылки. " + ADDLINK_USAGE)
        return
    await close_prompt("link_add", prompt["id"])
    await _dismiss_thread_by_id(thread_id)


# --- планировщик дня поверх канбан-доски ---------------------------------
#
# Колонка отвечает на вопрос «на какой стадии задача», дата — на вопрос
# «когда я ей займусь». Это разные оси: задача может быть одновременно
# запланированной на сегодня и уже в работе. Поэтому «сегодняшность» живёт
# меткой #due, а доска не меняется вовсе — ни новых колонок, ни
# переименований (см. PLAN-planner.md).
#
# Единственное движение колонок, которое делает бот, — «Потом» уводит
# карточку в БУДУЩИЕ ЗАДАЧИ, то есть ровно туда, что эта колонка означает.

_PLAN_TODAY = "pt"
_PLAN_TOMORROW = "pm"
_PLAN_LATER = "pl"
_PLAN_HAND = "ph"
_PLAN_DONE = "pd"


def _mine(card: dict, person_name: str) -> bool:
    """Своя задача или общая. Карточка без владельца видна обоим — так
    ведут себя все, что были заведены до появления этой метки, и это же
    оказалось удобным для совместных дел."""
    return card["owner"] in ("", person_name)


def _slices(cards: list[dict], person_name: str, today) -> tuple[list, list, list]:
    """Инбокс, план на сегодня и просроченное — три среза одного списка.

    Инбокс — это «без даты и не сделано», а НЕ «лежит в колонке БЭКЛОГ».
    Разница оказалась принципиальной: карточка, перенесённая в «БУДУЩИЕ
    ЗАДАЧИ» руками или старой кнопкой «Потом», выпадала из инбокса (не
    бэклог) и из /plan (нет даты) — то есть исчезала из бота насовсем. На
    живой доске так потерялось семь задач. Сюда же попадают карточки вообще
    без метки статуса: их заводит Trilium, когда дочернюю заметку создают
    напрямую, а не перетаскиванием в колонку.

    Иначе говоря, у задачи есть ровно два способа не спрашивать о себе:
    получить день или быть сделанной."""
    mine = [c for c in cards if _mine(c, person_name)]
    inbox = [c for c in mine if c["due"] is None and c["status"] != KANBAN_DONE]
    live = [c for c in mine if c["due"] is not None and c["status"] != KANBAN_DONE]
    return (
        inbox,
        [c for c in live if c["due"] == today],
        sorted([c for c in live if c["due"] < today], key=lambda c: c["due"]),
    )


def _other_person(person_name: str) -> str | None:
    return next((n for n in USER_NAMES.values() if n != person_name), None)


def _inbox_buttons(session_id: int, index: int, person_name: str) -> list[tuple[str, str]]:
    """Кнопка адресует не карточку, а место в снимке: сессия плюс индекс.
    Так «следующая» берётся из зафиксированного списка, а не из заново
    прочитанной доски."""
    ref = f"{session_id}:{index}"
    buttons = [
        ("Сегодня", f"{_PLAN_TODAY}:{ref}"),
        ("Завтра", f"{_PLAN_TOMORROW}:{ref}"),
        ("Потом", f"{_PLAN_LATER}:{ref}"),
    ]
    other = _other_person(person_name)
    if other:
        buttons.append((dative(other), f"{_PLAN_HAND}:{ref}"))
    return buttons


def _inbox_card_text(card: dict, remaining: int) -> str:
    return f"<b>{html.escape(card['title'])}</b>\n\nВ инбоксе: {remaining}"


async def show_inbox(user_id: int, trigger_message_id: int | None = None) -> None:
    """/inbox — разбор по одной задаче.

    Доска читается ОДИН раз, и порядок замораживается снимком в
    inbox_sessions. Раньше следующая карточка вычислялась каждый раз из
    свежепрочитанной доски: одно нажатие стоило полутора десятков запросов
    к Trilium, а разбор одиннадцати задач — полутора сотен. Плюс порядок мог
    съехать между нажатиями, и карточка показывалась дважды или пропускалась.

    Снимок стареет — и это честно: разбор есть срез момента, заведённое
    после приедет следующим заходом."""
    person_name = USER_NAMES.get(user_id, str(user_id))
    thread_id = await threads.open_thread(user_id, threads.TTL_DIALOG, trigger_message_id)
    try:
        cards = await get_planner_cards()
    except Exception:
        traceback.print_exc()
        await threads.send(thread_id, user_id, TRILIUM_UNAVAILABLE_TEXT)
        return

    inbox, _today, _overdue = _slices(cards, person_name, datetime.now(TIMEZONE).date())
    if not inbox:
        await threads.send(thread_id, user_id, "Инбокс пуст.")
        return

    snapshot = [{"note_id": c["note_id"], "title": c["title"]} for c in inbox]
    session_id = await create_inbox_session(user_id, snapshot, thread_id)
    await threads.send(
        thread_id, user_id, _inbox_card_text(snapshot[0], len(snapshot)),
        parse_mode="HTML", buttons=_inbox_buttons(session_id, 0, person_name), row_width=2,
    )


async def show_plan(user_id: int, trigger_message_id: int | None = None) -> None:
    """/plan — что назначено на сегодня, плюс всё просроченное отдельно.
    Просроченное показывается вместе с сегодняшним намеренно: задача,
    которую вчера не сделал, никуда не делась, и прятать её значит
    потерять."""
    person_name = USER_NAMES.get(user_id, str(user_id))
    today = datetime.now(TIMEZONE).date()
    thread_id = await threads.open_thread(user_id, threads.TTL_DIALOG, trigger_message_id)
    try:
        cards = await get_planner_cards()
    except Exception:
        traceback.print_exc()
        await threads.send(thread_id, user_id, TRILIUM_UNAVAILABLE_TEXT)
        return

    inbox, planned, overdue = _slices(cards, person_name, today)
    if not planned and not overdue:
        hint = f" В инбоксе {len(inbox)} — разобрать: /inbox" if inbox else ""
        await threads.send(thread_id, user_id, f"На сегодня ничего не запланировано.{hint}")
        return

    lines = [f"<b>На сегодня, {humanize.format_date(today)}</b>"]
    lines += [f"{i}. {html.escape(c['title'])}" for i, c in enumerate(planned, 1)]
    if overdue:
        lines.append("")
        lines.append("<b>Просрочено:</b>")
        lines += [
            f"{i}. {html.escape(c['title'])} <i>({humanize.format_date(c['due'])})</i>"
            for i, c in enumerate(overdue, len(planned) + 1)
        ]
    buttons = [(f"✓ {c['title']}", f"{_PLAN_DONE}:{c['note_id']}") for c in planned + overdue]
    await threads.send(
        thread_id, user_id, "\n".join(lines), parse_mode="HTML", buttons=buttons,
    )


async def handle_planner_action(press: Press) -> None:
    """Любая кнопка планировщика. После действия карточка перерисовывается
    на месте — при разборе это следующая задача в том же сообщении, а не
    новое сообщение поверх отвеченного."""
    data = press.data
    action, _, ref = data.partition(":")
    # Две формы ссылки: «сессия:индекс» из разбора инбокса и голый note_id из
    # /plan, где список короткий и снимок не нужен.
    session = None
    index = 0
    if ":" in ref:
        session = await get_inbox_session(int(ref.split(":")[0]))
        if session is None:
            await answer_callback_query(press.id, "Этот разбор уже неактуален.")
            return
        cards = json.loads(session["cards"]) if isinstance(session["cards"], str) else session["cards"]
        index = int(ref.split(":")[1])
        if index >= len(cards):
            await answer_callback_query(press.id)
            return
        note_id = cards[index]["note_id"]
    else:
        note_id = ref
    chat_id = press.chat_id
    message_id = press.message_id
    person_name = USER_NAMES.get(chat_id, str(chat_id))
    today = datetime.now(TIMEZONE).date()

    try:
        if action == _PLAN_TODAY:
            await set_card_label(note_id, "due", today.isoformat())
            note = "Сегодня"
        elif action == _PLAN_TOMORROW:
            await set_card_label(note_id, "due", (today + timedelta(days=1)).isoformat())
            note = "Завтра"
        elif action == _PLAN_LATER:
            # «Потом» НИЧЕГО не меняет в задаче — только листает дальше.
            # Раньше она ставила статус «БУДУЩИЕ ЗАДАЧИ», и задача пропадала
            # из планировщика насовсем: ни инбокс, ни /plan таких не
            # показывают никогда. То есть кнопка, которую жмут со смыслом
            # «решу позже», означала «больше не спрашивай».
            await answer_callback_query(press.id, "Потом")
            await touch_message_thread(session["thread_id"] if session else None)
            await _redraw_inbox(chat_id, message_id, person_name, session, index)
            return
        elif action == _PLAN_HAND:
            other = _other_person(person_name)
            if other is None:
                await answer_callback_query(press.id, "Некому передать.")
                return
            await set_card_label(note_id, "owner", other)
            note = f"Передал {dative(other)}"
        elif action == _PLAN_DONE:
            await set_card_label(note_id, "status", KANBAN_DONE)
            note = "Готово"
        else:
            await answer_callback_query(press.id)
            return
    except Exception:
        traceback.print_exc()
        await answer_callback_query(press.id, "Trilium недоступен.")
        return

    await answer_callback_query(press.id, note)

    if action == _PLAN_DONE or session is None:
        await _redraw_plan(chat_id, message_id, person_name, today)
        return
    await mark_inbox_handled(session["id"], note_id)
    await touch_message_thread(session["thread_id"])
    session = await get_inbox_session(session["id"])
    await _redraw_inbox(chat_id, message_id, person_name, session, index)


async def _redraw_inbox(chat_id: int, message_id: int, person_name: str, session, index: int) -> None:
    """Следующая неразобранная карточка снимка, по кругу. Ни одного запроса
    к Trilium: список уже лежит в сессии."""
    cards = json.loads(session["cards"]) if isinstance(session["cards"], str) else session["cards"]
    handled = set(session["handled"] or [])
    order = [i for i in range(1, len(cards) + 1)]
    nxt = next(
        (i % len(cards) for i in (index + o for o in order)
         if cards[i % len(cards)]["note_id"] not in handled),
        None,
    )
    if nxt is None:
        await edit_message(chat_id, message_id, "Инбокс разобран.")
        return
    await edit_message(
        chat_id, message_id,
        _inbox_card_text(cards[nxt], len(cards) - len(handled)),
        buttons=_inbox_buttons(session["id"], nxt, person_name), parse_mode="HTML", row_width=2,
    )


async def _redraw_plan(chat_id: int, message_id: int, person_name: str, today) -> None:
    try:
        _inbox, planned, overdue = _slices(await get_planner_cards(), person_name, today)
    except Exception:
        traceback.print_exc()
        return
    if not planned and not overdue:
        await edit_message(chat_id, message_id, "На сегодня всё сделано.")
        return
    lines = [f"<b>На сегодня, {humanize.format_date(today)}</b>"]
    lines += [f"{i}. {html.escape(c['title'])}" for i, c in enumerate(planned, 1)]
    if overdue:
        lines.append("")
        lines.append("<b>Просрочено:</b>")
        lines += [
            f"{i}. {html.escape(c['title'])} <i>({humanize.format_date(c['due'])})</i>"
            for i, c in enumerate(overdue, len(planned) + 1)
        ]
    buttons = [(f"✓ {c['title']}", f"{_PLAN_DONE}:{c['note_id']}") for c in planned + overdue]
    await edit_message(chat_id, message_id, "\n".join(lines), buttons=buttons, parse_mode="HTML")


def _chunk(entries: list[str], header: str) -> list[str]:
    """Entries packed into as few messages as fit under Telegram's limit,
    never splitting an entry across two. The header leads the first one."""
    messages, current = [], header
    for entry in entries:
        # A single highlight longer than a whole message can't be packed with
        # anything — cut it, or the send fails and the moment vanishes.
        if len(entry) > _TELEGRAM_TEXT_LIMIT - len(header) - 40:
            entry = entry[:_TELEGRAM_TEXT_LIMIT - len(header) - 60].rstrip() + " <i>…</i>"
        candidate = f"{current}\n\n{entry}" if current else entry
        if len(candidate) > _TELEGRAM_TEXT_LIMIT and current:
            messages.append(current)
            current = entry
        else:
            current = candidate
    if current:
        messages.append(current)
    return messages


async def handle_book_quotes_selected(press: Press) -> None:
    """"Интересные моменты" under a book's description — sends everything
    collected for that book, from /quote and from the reader import alike.
    Stays in the same thread as the description, so one reaction still
    clears the whole conversation."""
    await answer_callback_query(press.id)
    data = press.data
    chat_id = press.chat_id
    note_id = data[len("im:"):]

    thread = await threads.thread_for_message(chat_id, press.message_id)
    thread_id = thread["id"] if thread is not None else None

    try:
        quotes = await get_book_quotes(note_id)
    except Exception as exc:
        await _report_failure(
            chat_id, "get_book_quotes",
            "Не получилось прочитать интересные моменты", exc,
        )
        return

    if not quotes:
        await threads.send(thread_id, chat_id, "Пока ничего не отмечено.")
        return

    for part in _chunk(quotes, "<b>Интересные моменты</b>"):
        await threads.send(thread_id, chat_id, part, parse_mode="HTML")


async def handle_reading_book_selected(press: Press) -> None:
    """Routes a "rb:{note_id}" button press from show_reading_status above
    — called from app/callbacks.py."""
    await _handle_book_list_press(press, "rb", with_finish_button=True)


async def handle_finished_book_selected(press: Press) -> None:
    """Routes a "pb:{note_id}" button press from show_finished_books above."""
    await _handle_book_list_press(press, "pb", with_finish_button=False)


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


async def handle_book_finished(press: Press) -> None:
    """"Я дочитал" — the button under a book's description (see
    handle_reading_book_selected above). Stamps readingEnd on the book
    IMMEDIATELY, before asking anything (per explicit request), so an
    abandoned review dialogue still leaves the book correctly marked as
    finished — it just won't get a review note. Then opens the two-step
    rating/review flow (_handle_book_review_reply below)."""
    query_id = press.id
    data = press.data
    chat_id = press.chat_id
    description_message_id = press.message_id
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
        await _report_failure(chat_id, "set_reading_end", "Не получилось отметить книгу", exc)
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
    # Таблица одна на книги и кино, различает их колонка kind. Отзыв
    # ложится в разные места дерева, поэтому и функции разные, но диалог —
    # оценка, потом текст — у них дословно один и тот же, и разводить его на
    # две копии было бы незачем.
    write = (
        create_book_review_note if prompt["kind"] == "book" else create_media_review_note
    )
    try:
        await write(
            prompt["book_note_id"], prompt["book_title"], prompt["rating"], text.strip(), person_name,
        )
        await close_book_review_prompt(prompt["id"])
        await threads.send(thread_id, user_id, "Спасибо, записал отзыв!")
        await _dismiss_thread_by_id(thread_id)
    except Exception as exc:
        await _report_failure(
            user_id, write.__name__,
            "Не получилось записать отзыв (Trilium недоступен)", exc, retry=True,
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
    "link_add": _handle_link_add_reply,
    "task_add": _handle_task_add_reply,
}


async def _offer_book_details(user_id: int, note_id: str, title: str, author: str) -> None:
    """Книга только что заведена по данным из «My Clippings.txt» — шлём тот
    же шаблон «расскажи подробнее», что и /addbook, и подключаем его к той
    же машинерии, чтобы ответ разложился по разделам заметки как обычно."""
    thread_id = await threads.open_thread(
        user_id, threads.TTL_BOOK_DETAILS, closing_text="Добавил книгу",
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

    # Вся библиотека одним запросом, а не поиск на каждую книгу из файла:
    # раньше каждый find_book_note_id обходил всех детей КНИГИ заново.
    try:
        known_books = await get_book_note_ids()
    except Exception:
        print(f"clippings import: cannot read КНИГИ for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(user_id, TRILIUM_UNAVAILABLE_TEXT)
        return

    for (title, author), group in by_book.items():
        try:
            note_id = known_books.get(normalize_book_title(title))
            is_new_book = note_id is None
            if is_new_book:
                note_id = await add_book(person_name, title, author)
                # Чтобы вторая группа с тем же названием (другой автор в
                # метаданных) попала в только что созданную заметку, а не
                # завела ещё одну — так же, как делал поиск по названию.
                known_books[normalize_book_title(title)] = note_id

            # Все цитаты книги одной перезаписью заметки: по одной это было
            # чтение и запись всего содержимого на каждую цитату.
            await add_book_quotes(note_id, [(c.text, "", c.location) for c in group])
            imported.extend((clippings.fingerprint(c), title) for c in group)

            per_book.append(f"«{title}» — {len(group)}")
            if is_new_book:
                created_books.append((note_id, title, author))
        except Exception as exc:
            print(f"clippings import failed for {title!r}, user={user_id}:", flush=True)
            traceback.print_exc()
            failed.append(f"«{title}» ({type(exc).__name__})")

    # Отмечаем только то, что реально доехало до Trilium: упавшая книга
    # должна приехать снова при следующей отправке файла, а не потеряться.
    # Теперь это вся книга целиком — раз запись одна на книгу, то и «доехало»
    # у неё одно на всех, а не по цитате.
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


# --- траты -----------------------------------------------------------------
#
# Из строки надёжно достаётся ТОЛЬКО число. Всё остальное зависит от того,
# как человек сформулировал, — «659 за креатин», «на креатин 659 с
# рассрочки», — и разбор такого либо диктует человеку язык, либо молча
# ошибается. Поэтому спрашиваем: описание сообщением, остальное кнопками из
# того, что уже заведено, с возможностью написать своё.


def _money(amount: str) -> str:
    rubles = f"{int(float(amount)):,}".replace(",", " ")
    kopecks = round(float(amount) % 1 * 100)
    return f"{rubles}{f',{kopecks:02d}' if kopecks else ''} ₽"


async def start_expense_flow(
    user_id: int, text: str, telegram_message_id: int | None = None,
) -> None:
    """Записать трату сразу, не спрашивая ничего.

    Раньше здесь начинался диалог из пяти вопросов — описание, счёт,
    получатель, категория, тэг, — и не потому, что столько нужно знать, а
    потому что Firefly не принимал проводку без них. Форма отчётности
    диктовала, что спрашивают у человека, и учёт умирал не от лени, а от
    пяти касаний на каждый чек.

    beancount не требует ничего, кроме суммы и двух счетов. Поэтому:
    записываем немедленно, а уточняем кнопками под подтверждением — и почти
    всегда не уточняем вовсе. Записать покупку можно только сейчас, пока
    помнишь; разложить по полкам — когда угодно потом.

    Счёт и категория берутся из прошлой такой же траты (ledger, по тексту
    описания), а если её не было — первый счёт человека и «Прочее»."""
    amount = triggers.expense_amount(text)
    if amount is None:
        return
    person = USER_NAMES.get(user_id, "")
    accounts = LEDGER_ACCOUNTS.get(person, [])
    if not accounts:
        await send_message(user_id, "Счета не настроены — см. LEDGER_ACCOUNTS в app/config.py.")
        return

    note = triggers.expense_note(text)
    account = await ledger.account_for_note(user_id, note)
    category = await ledger.category_for_note(user_id, note)

    # Ветка, как у всех остальных потоков: карточка подтверждения и само
    # сообщение человека уезжают вместе через TTL, по реакции или по
    # «Отменить». Без неё карточка висела бы в чате вечно, а реакция —
    # документированный жест «свернуть это» — молча ничего не делала.
    thread_id = await threads.open_thread(user_id, threads.TTL_DIALOG, telegram_message_id)

    entry_id, created = await ledger.add_expense(
        user_id, amount,
        account=account or accounts[0],
        category=category or LEDGER_DEFAULT_CATEGORY,
        narration=note,
        external_id=f"tg:{telegram_message_id}" if telegram_message_id else None,
    )
    if not created:
        # Тот же апдейт приехал второй раз (вебхук передоставляет, если бот
        # молчал минуту). Строка уже есть — второе подтверждение было бы
        # враньём про две покупки.
        print(f"трата tg:{telegram_message_id} уже записана", flush=True)
        return

    await _send_expense_card(user_id, entry_id, category is None, thread_id)


_MONTHS = (
    "январь", "февраль", "март", "апрель", "май", "июнь",
    "июль", "август", "сентябрь", "октябрь", "ноябрь", "декабрь",
)
_MONTHS_IN = (
    "январе", "феврале", "марте", "апреле", "мае", "июне",
    "июле", "августе", "сентябре", "октябре", "ноябре", "декабре",
)


def _month_bounds(text: str) -> tuple[_date, _date, str]:
    """Период, о котором спрашивают. Без аргумента — текущий месяц.

    Названия месяцев принимаются и в именительном, и в предложном падеже,
    потому что спрашивают обоими: «/spent август» и «сколько ушло в
    августе»."""
    today = datetime.now(TIMEZONE).date()
    wanted = text.strip().lower()
    month = today.month
    for i, (nom, prep) in enumerate(zip(_MONTHS, _MONTHS_IN), start=1):
        if wanted and (nom in wanted or prep in wanted):
            month = i
            break
    year = today.year if month <= today.month else today.year - 1
    start = today.replace(year=year, month=month, day=1)
    end = (start + timedelta(days=32)).replace(day=1) - timedelta(days=1)
    return start, min(end, today), _MONTHS[month - 1]


async def show_spent(user_id: int, argument: str = "") -> None:
    """Сколько ушло за месяц и на что.

    Ради этого всё и затевалось: у Firefly ответ на такой вопрос жил только
    внутри его интерфейса, куда надо было зайти. Здесь это обычный SQL по
    своей же таблице, поэтому ответ приходит в чат за миллисекунды и
    работает, даже когда VPS недоступен."""
    since, until, name = _month_bounds(argument)
    rows = await ledger.spent(user_id, since, until)
    if not rows:
        await send_message(user_id, f"За {name} ничего не записано.")
        return
    total = sum(amount for _c, amount in rows)
    lines = [
        f"<b>{_money(str(total))}</b> за {name}",
        "",
    ] + [
        f"{html.escape(ledger_label(category))} — {_money(str(amount))}"
        for category, amount in rows
    ]
    await send_message(user_id, "\n".join(lines), parse_mode="HTML")


def _expense_card(row, offer_category: bool) -> tuple[str, list[tuple[str, str]], int]:
    """Текст подтверждения и кнопки под ним.

    Кнопки категорий показываются только когда категорию не угадали: если
    прошлая такая же трата уже разложена, спрашивать не о чем. Счёт спрятан
    за одну кнопку, а не разложен рядом — он угадывается почти всегда, и
    занимать им экран на каждой покупке незачем."""
    entry_id = row["id"]
    text = (
        f"Записал: <b>{_money(str(row['amount']))}</b>"
        + (f" · {html.escape(row['narration'])}" if row["narration"] else "")
        + f"\n<code>{html.escape(row['account'])}</code>"
        f" → <code>{html.escape(row['category'])}</code>"
    )
    buttons: list[tuple[str, str]] = []
    if offer_category:
        buttons += [
            (ledger_label(c), f"el:{entry_id}:c:{i}") for i, c in enumerate(LEDGER_CATEGORIES)
        ]
    buttons.append(("Счёт", f"el:{entry_id}:A"))
    buttons.append(("Отменить", f"el:{entry_id}:x"))
    return text, buttons, 2


async def _send_expense_card(
    user_id: int, entry_id: int, offer_category: bool, thread_id: int | None,
) -> None:
    row = await ledger.get(entry_id, user_id)
    if row is None:
        return
    text, buttons, width = _expense_card(row, offer_category)
    await threads.send(
        thread_id, user_id, text, parse_mode="HTML", buttons=buttons, row_width=width,
    )


async def handle_ledger_choice(press: Press) -> None:
    """Кнопки под записанной тратой: категория, счёт, отмена.

    Правка меняет уже лежащую в леджере строку — и заодно учит бота: в
    следующий раз ledger.account_for_note найдёт именно её и не спросит."""
    await answer_callback_query(press.id)
    user_id = press.chat_id
    parts = press.data.split(":")
    entry_id = int(parts[1])
    action = parts[2]

    row = await ledger.get(entry_id, user_id)
    if row is None:
        return

    thread = await threads.thread_for_message(user_id, press.message_id)

    if action == "x":
        await ledger.forget(entry_id, user_id)
        # Не «Отменил» в карточке, а снос ветки целиком: отмена значит «этого
        # не было», и оставлять в чате след о ненаписанной трате незачем.
        if thread is not None:
            await threads.dismiss(thread)
        else:
            await edit_message(user_id, press.message_id, "Отменил, трата не записана.")
        return

    # Правка — это активность: без продления уборщик снёс бы карточку прямо
    # под руками через пять минут после сообщения (см. touch_message_thread).
    await touch_message_thread(thread["id"] if thread else None)

    if action == "A":
        # Показать счета вместо категорий, не пересылая карточку заново.
        accounts = LEDGER_ACCOUNTS.get(USER_NAMES.get(user_id, ""), [])
        buttons = [(ledger_label(a), f"el:{entry_id}:a:{i}") for i, a in enumerate(accounts)]
        buttons.append(("Отменить", f"el:{entry_id}:x"))
        text, _b, _w = _expense_card(row, offer_category=False)
        await edit_message(user_id, press.message_id, text, buttons, parse_mode="HTML", row_width=2)
        return

    index = int(parts[3])
    if action == "c":
        if index >= len(LEDGER_CATEGORIES):
            return
        await ledger.reclassify(entry_id, user_id, category=LEDGER_CATEGORIES[index])
    elif action == "a":
        accounts = LEDGER_ACCOUNTS.get(USER_NAMES.get(user_id, ""), [])
        if index >= len(accounts):
            return
        await ledger.reclassify(entry_id, user_id, account=accounts[index])

    row = await ledger.get(entry_id, user_id)
    text, _b, _w = _expense_card(row, offer_category=False)
    await edit_message(
        user_id, press.message_id, text,
        [("Счёт", f"el:{entry_id}:A"), ("Отменить", f"el:{entry_id}:x")],
        parse_mode="HTML", row_width=2,
    )


# Траты определены ниже карты, поэтому дописываются сюда отдельно.


async def handle_book_edit_details(press: Press) -> None:
    """«Описание» под книгой — открыть тот же шаблон «расскажи подробнее»,
    что и /addbook, для уже существующей книги.

    Нужен, потому что первый заход можно не успеть: диалог живёт десять
    минут и уносит с собой шаблон. Раньше после этого книга оставалась с
    прочерками навсегда — дописать было нечем."""
    await answer_callback_query(press.id)
    chat_id = press.chat_id
    note_id = press.data[len("bd:"):]
    try:
        title, labels = await get_note_labels(note_id)
    except Exception as exc:
        await _report_failure(chat_id, "get_note_labels", "Не получилось прочитать книгу", exc)
        return
    await _offer_book_details(chat_id, note_id, title, labels.get("author", ""))




# --- кино (см. app/media.py) ------------------------------------------------
#
# Отличие от книг, из которого следует всё остальное: заметка ОБЩАЯ, а
# состояние личное. Поэтому каждый список спрашивается применительно к
# человеку, и «посмотрел» у Маши не убирает фильм из «хочу» у Остапа.


def _media_person(user_id: int) -> str:
    return USER_NAMES.get(user_id, str(user_id))


async def start_watch_intent(
    user_id: int, kind, intent: str, title: str, telegram_message_id: int | None = None,
) -> None:
    """Разводит три фразы про кино по действиям.

    «хочу посмотреть X» — просто завести, это и есть состояние «хочу».
    «начал смотреть X» и «посмотрел X» — сначала найти уже заведённое, и
    только если его нет, завести. Иначе фильм, который лежал в «хочу
    посмотреть», раздвоился бы: один в списке, второй просмотренный."""
    if not title:
        return
    if intent == "watch_want":
        await start_media_add_flow(user_id, kind, title, telegram_message_id)
        return

    state = media.IN_PROGRESS if intent == "watch_start" else media.DONE
    thread_id = await threads.open_thread(user_id, threads.TTL_DIALOG, telegram_message_id)
    person = _media_person(user_id)

    try:
        note_id = await find_media_by_title(kind, title)
    except Exception as exc:
        await _report_failure(user_id, "find_media_by_title", "Не получилось найти", exc)
        return

    if note_id is None:
        # Не заводили заранее — заводим сейчас и сразу помечаем. Поиск в
        # TMDb пропускаем: человек уже посмотрел, спрашивать «что из этого?»
        # поздно и незачем.
        try:
            note_id = await add_media(kind, title, None)
        except Exception as exc:
            await _report_failure(
                user_id, "add_media", f"Не получилось добавить {kind.one}", exc, retry=True,
            )
            return

    try:
        await mark_media(kind, note_id, person, state)
    except Exception as exc:
        await _report_failure(user_id, "mark_media", "Не получилось отметить", exc, retry=True)
        return

    if state == media.IN_PROGRESS:
        await threads.send(thread_id, user_id, f"Отметил, что смотришь: {title}")
        await _dismiss_thread_by_id(thread_id)
        return

    prompt_id = await create_media_review_prompt(user_id, note_id, title, kind.slug, thread_id)
    if await threads.send(thread_id, user_id, book_review_step_text(0)) is None:
        await close_book_review_prompt(prompt_id)


async def _ask_media_title(user_id: int, kind, thread_id, text: str) -> None:
    """Спросить название сообщением — тот же ход, что у /addbook."""
    prompt_id = await create_media_add_prompt(user_id, kind.slug, "", [], thread_id)
    if await threads.send(thread_id, user_id, text) is None:
        await close_media_add_prompt(prompt_id)


async def start_media_add_flow(
    user_id: int, kind, query: str, telegram_message_id: int | None = None,
    retry: bool = False, note_id: str | None = None,
) -> None:
    """«хочу посмотреть Дюну» — найти в TMDb и предложить выбрать.

    Ничего не нашлось — СПРАШИВАЕМ название, как это делает /addbook, а не
    заводим заметку по тому, что написано. Причина живая: по-русски пишут в
    винительном падеже («посмотреть Дюну»), а в базе именительный («Дюна»),
    и поиск такого не находит. Раньше здесь молча заводилась заметка с
    названием «Дюну» — то есть кривым навсегда.

    Угадывать падеж отсечением окончания я пробовал: «Дюн» действительно
    находит «Дюну», но заодно «Воины дюн» и «В тени дюн». Спросить — честнее
    и короче, чем угадывать.

    retry=True приходит из ответа на этот самый вопрос: второй раз не
    переспрашиваем, а заводим по написанному. Иначе человек, у которого
    фильма в TMDb просто нет, попал бы в бесконечный круг."""
    thread_id = await threads.open_thread(user_id, threads.TTL_DIALOG, telegram_message_id)
    query = query.strip()

    if not query:
        await _ask_media_title(user_id, kind, thread_id, f"Какой {kind.one}?")
        return

    # note_id задан — значит заполняем уже заведённую руками заметку, а не
    # создаём новую. Тогда «не нашли» это тупик, а не повод спрашивать
    # название: название и так взято из самой заметки.

    # Ищем В ОБОИХ видах сразу, а не только в запрошенном.
    #
    # Вид угадывается словом «сериал» во фразе, и люди его опускают: «хочу
    # посмотреть Разделение» — это сериал Apple TV+, но среди фильмов
    # находятся пять посторонних «Разделений», и человек не понимает, куда
    # делся нужный. Показать оба списка честнее, чем настаивать на своей
    # догадке; сериалы подписаны, так что выбор очевиден.
    #
    # Заполнение уже заведённой заметки (note_id) — исключение: у неё вид
    # уже определён тем, где она лежит, и менять его нельзя.
    searched, found = False, []
    kinds = [kind] if note_id is not None else (
        [kind, media.SERIES if kind is media.FILM else media.FILM]
    )
    try:
        for candidate_kind in kinds:
            hits = await tmdb_client.search(candidate_kind, query)
            searched = True
            found.extend((candidate_kind, hit) for hit in hits)
    except tmdb_client.TmdbNotConfiguredError:
        pass  # ключа нет — заводим по названию, это штатный путь
    except Exception as exc:
        print(f"tmdb search failed: {exc!r}", flush=True)

    if not found:
        # Не искали вовсе (нет ключа, TMDb недоступен) или уже спрашивали —
        # заводим по написанному: переспрашивать без работающего поиска
        # бессмысленно.
        if note_id is not None:
            await threads.send(
                thread_id, user_id,
                f"Не нашёл «{query}» в TMDb. Переименуй заметку точнее и попробуй снова.",
            )
            await _dismiss_thread_by_id(thread_id)
            return
        if not searched or retry:
            await _create_media(user_id, kind, thread_id, query, None)
            return
        # Пустая карточка — только осознанным выбором, кнопкой. Раньше она
        # получалась сама: «I love killing flies» (на деле «I Like Killing
        # Flies») не нашлось, и бот молча завёл заметку без года, режиссёра
        # и описания. Человек узнавал об этом через неделю, открыв Trilium.
        prompt_id = await create_media_add_prompt(
            user_id, kind.slug, query, [], thread_id,
        )
        text = (
            f"Не нашёл «{query}» ни среди фильмов, ни среди сериалов.\n"
            f"Напиши название точнее — или заведу как есть, без описания."
        )
        if await threads.send(
            thread_id, user_id, text,
            buttons=[("Завести как есть", f"ma:{prompt_id}:-1")],
        ) is None:
            await close_media_add_prompt(prompt_id)
        return

    candidates = [
        {"tmdb_id": f.tmdb_id, "title": f.title, "year": f.year, "kind": k.slug}
        for k, f in found
    ]
    prompt_id = await create_media_add_prompt(
        user_id, kind.slug, query, candidates, thread_id, note_id,
    )
    buttons = [
        (f.label() + ("" if k is kind else f" · {k.one}"), f"ma:{prompt_id}:{i}")
        for i, (k, f) in enumerate(found)
    ]
    # Последней кнопкой — «ничего из этого»: выдача TMDb отсортирована по
    # популярности, и нужного там может не быть вовсе.
    buttons.append(("Ничего из этого", f"ma:{prompt_id}:-1"))
    if await threads.send(
        thread_id, user_id, "Что из этого?", buttons=buttons,
    ) is None:
        await close_media_add_prompt(prompt_id)


async def _create_media(user_id: int, kind, thread_id, title: str, tmdb_id: int | None) -> None:
    """Завести заметку и подтвердить. Подробности тянем вторым запросом
    только когда есть что тянуть."""
    meta = None
    if tmdb_id is not None:
        try:
            meta = await tmdb_client.details(kind, tmdb_id)
            title = meta.get("title") or title
        except Exception as exc:
            print(f"tmdb details failed: {exc!r}", flush=True)

    try:
        await add_media(kind, title, meta)
    except Exception as exc:
        await _report_failure(
            user_id, "add_media", f"Не получилось добавить {kind.one}", exc, retry=True,
        )
        return

    year = (meta or {}).get("year", "")
    await threads.send(
        thread_id, user_id,
        f"Добавил в «хочу посмотреть»: <b>{html.escape(title)}</b>"
        + (f" ({year})" if year else ""),
        parse_mode="HTML",
    )
    await _dismiss_thread_by_id(thread_id)


async def _handle_media_add_reply(
    user_id: int, text: str, reply_to_text: str | None, prompt, telegram_message_id: int | None = None,
) -> None:
    """Название сообщением — когда кнопки не подошли или их не было.

    Текст чистим от глагола, если он там есть. На «Какой фильм?» отвечают и
    голым названием, и целой фразой «хочу посмотреть Дюна» — а раз открытый
    диалог перехватывает сообщение раньше разбора триггеров, без этой чистки
    в Trilium заводилась заметка с названием «хочу посмотреть Дюна»
    (поймано на живом проде)."""
    await threads.track(prompt["thread_id"], telegram_message_id)
    kind = media.by_slug(prompt["kind"])
    if kind is None:
        await close_media_add_prompt(prompt["id"])
        return
    await close_media_add_prompt(prompt["id"])
    title = triggers.watch_title(text) if triggers.watch_intent(text) else text.strip()
    # retry=True: если и по уточнённому названию ничего не найдётся, заводим
    # по нему, а не спрашиваем в третий раз.
    await start_media_add_flow(user_id, kind, title, telegram_message_id, retry=True)


async def handle_media_add_choice(press: Press) -> None:
    """Нажата находка TMDb — или «ничего из этого»."""
    await answer_callback_query(press.id)
    _prefix, prompt_id_raw, index_raw = press.data.split(":", 2)
    prompt = await get_media_add_prompt(int(prompt_id_raw))
    if prompt is None or not prompt["is_open"] or prompt["user_id"] != press.chat_id:
        return
    kind = media.by_slug(prompt["kind"])
    if kind is None:
        return

    if press.message_id is not None:
        await clear_reply_markup(press.chat_id, press.message_id)
    await close_media_add_prompt(prompt["id"])

    index = int(index_raw)
    if index < 0:
        # «Ничего из этого» — заводим по тому, что человек написал сам.
        await _create_media(press.chat_id, kind, prompt["thread_id"], prompt["query"], None)
        return

    candidates = json.loads(prompt["candidates"] or "[]")
    if index >= len(candidates):
        return
    chosen = candidates[index]
    # Вид берём из самой находки: список смешанный, и «Разделение» может
    # оказаться сериалом, хотя спрашивали про фильм.
    kind = media.by_slug(chosen.get("kind") or kind.slug) or kind
    if prompt["note_id"]:
        await _fill_existing(press.chat_id, kind, prompt["thread_id"],
                             prompt["note_id"], chosen["tmdb_id"])
        return
    await _create_media(
        press.chat_id, kind, prompt["thread_id"], chosen["title"], chosen["tmdb_id"],
    )


async def _fill_existing(user_id: int, kind, thread_id, note_id: str, tmdb_id: int) -> None:
    """Дописать карточку заметке, которую человек завёл в Trilium руками."""
    try:
        meta = await tmdb_client.details(kind, tmdb_id)
        await fill_media(note_id, meta)
    except Exception as exc:
        await _report_failure(user_id, "fill_media", "Не получилось заполнить", exc, retry=True)
        return
    await threads.send(
        thread_id, user_id,
        f"Заполнил: <b>{html.escape(meta.get('title', ''))}</b>"
        + (f" ({meta['year']})" if meta.get("year") else ""),
        parse_mode="HTML",
    )
    await _dismiss_thread_by_id(thread_id)


async def show_media_menu(user_id: int, kind, trigger_message_id: int | None = None) -> None:
    """/films и /series — не список, а выбор списка.

    Плоскими командами это было бы пять пунктов в меню телеграма, где и так
    семнадцать. Здесь два, а состояния разводятся кнопками."""
    thread_id = await threads.open_thread(user_id, threads.TTL_DIALOG, trigger_message_id)
    buttons = [("Хочу посмотреть", f"ml:{kind.slug}:{media.WANT}")]
    if kind.has_in_progress:
        buttons.append(("Смотрю", f"ml:{kind.slug}:{media.IN_PROGRESS}"))
    buttons.append(("Посмотрел", f"ml:{kind.slug}:{media.DONE}"))
    await threads.send(
        thread_id, user_id, f"{kind.one.capitalize()}ы — что показать?",
        buttons=buttons, row_width=2,
    )


_MEDIA_EMPTY = {
    media.WANT: lambda k: k.want_empty,
    media.IN_PROGRESS: lambda k: k.progress_empty,
    media.DONE: lambda k: k.done_empty,
}


_MEDIA_LIST_NAMES = {
    media.WANT: "Хочу посмотреть",
    media.IN_PROGRESS: "Смотрю",
    media.DONE: "Посмотрел",
}


async def _render_media_list(user_id: int, kind, state: str, thread_id) -> None:
    """Список одним сообщением: сам список кнопками, а под ним переходы в
    остальные списки. Так любой из них в одном нажатии, и при этом не
    приходится начинать с выбора."""
    try:
        items = await list_media(kind, state, _media_person(user_id))
    except Exception:
        traceback.print_exc()
        await send_message(user_id, TRILIUM_UNAVAILABLE_TEXT)
        return

    buttons = [
        (_media_label(item), f"ms:{kind.slug}:{item['note_id']}") for item in items
    ]
    # Переходы в соседние списки — всегда, даже когда текущий пуст: пустой
    # список это и есть момент, когда человек ищет, где же остальное.
    others = [
        (name, f"ml:{kind.slug}:{other}")
        for other, name in _MEDIA_LIST_NAMES.items()
        if other != state and (kind.has_in_progress or other != media.IN_PROGRESS)
    ]
    text = (
        f"{_MEDIA_LIST_NAMES[state]} · {kind.one}ы"
        if items else _MEDIA_EMPTY[state](kind)
    )
    await threads.send(thread_id, user_id, text, buttons=buttons + others, row_width=2)


async def handle_media_list(press: Press) -> None:
    """Нажата кнопка перехода в другой список."""
    await answer_callback_query(press.id)
    _prefix, kind_slug, state = press.data.split(":", 2)
    kind = media.by_slug(kind_slug)
    if kind is None:
        return

    thread = None
    if press.message_id is not None:
        await clear_reply_markup(press.chat_id, press.message_id)
        thread = await threads.thread_for_message(press.chat_id, press.message_id)
    await _render_media_list(
        press.chat_id, kind, state, thread["id"] if thread is not None else None,
    )


def _media_label(item: dict) -> str:
    """«Дюна (2021)» — год различает переснятое, как автор различает книги."""
    year = (item.get("year") or "").strip()
    return f"{item['title']} ({year})" if year else item["title"]


async def handle_media_selected(press: Press) -> None:
    """Карточка выбранного кино плюс кнопки перехода в другое состояние."""
    await answer_callback_query(press.id)
    _prefix, kind_slug, note_id = press.data.split(":", 2)
    kind = media.by_slug(kind_slug)
    if kind is None:
        return
    chat_id = press.chat_id
    person = _media_person(chat_id)

    thread = None
    if press.message_id is not None:
        await clear_reply_markup(chat_id, press.message_id)
        thread = await threads.thread_for_message(chat_id, press.message_id)
    thread_id = thread["id"] if thread is not None else None

    try:
        title, content, labels, reviews = await get_media_details(note_id)
    except Exception as exc:
        await _report_failure(chat_id, "get_media_details", "Не получилось прочитать карточку", exc)
        return

    state = _media_state_of(labels, person)
    rating = _ratings(reviews)
    text = f"<b>{html.escape(title)}</b>"
    if rating:
        text += f"\n{html.escape(rating)}"
    if content:
        text += f"\n\n{html.escape(content)}"

    # Кнопки — только переходы, осмысленные из текущего состояния. Иначе под
    # уже просмотренным фильмом висело бы «Не буду смотреть», а под тем, что
    # ещё не начинали, — «Бросил».
    def go(label: str, to: str) -> tuple[str, str]:
        return label, f"mm:{kind.slug}:{to}:{note_id}"

    # Заметку могли завести руками прямо в Trilium — тогда у неё нет ни
    # описания, ни года, ни режиссёра. Предлагаем дозаполнить, но кнопкой, а
    # не молча: по названию «Дюна» находится пять разных фильмов, и угадать
    # за человека значит однажды угадать неверно.
    fill = [] if labels.get("tmdbId") else [("Заполнить из TMDb", f"mF:{kind.slug}:{note_id}")]

    if state == media.WANT:
        buttons = [go("Посмотрел", media.DONE)]
        if kind.has_in_progress:
            buttons.append(go("Начал смотреть", media.IN_PROGRESS))
        buttons.append(go("Не буду смотреть", media.DROPPED))
    elif state == media.IN_PROGRESS:
        buttons = [go("Посмотрел", media.DONE), go("Бросил", media.DROPPED)]
    else:
        # Посмотрел или бросил — оба состояния конечные, и единственное
        # осмысленное действие это откатить, если нажали не то.
        buttons = [go("Вернуть в «хочу»", media.WANT)]

    await threads.send(
        thread_id, chat_id, text[:4000], parse_mode="HTML",
        buttons=fill + buttons, row_width=2,
    )


def _media_state_of(labels: dict, person_name: str) -> str:
    """То же правило, что в trilium_client._media_state, но по уже
    прочитанным лейблам — второй раз в сеть за ними не ходим."""
    if labels.get(media.label_for(media.WATCHED, person_name)):
        return media.DONE
    if labels.get(media.label_for(media.DROPPED, person_name)):
        return media.DROPPED
    if labels.get(media.label_for(media.WATCHING, person_name)):
        return media.IN_PROGRESS
    return media.WANT


async def handle_media_fill(press: Press) -> None:
    """«Заполнить из TMDb» под карточкой без данных. Ищем по заголовку самой
    заметки — его человек уже написал, спрашивать нечего."""
    await answer_callback_query(press.id)
    _prefix, kind_slug, note_id = press.data.split(":", 2)
    kind = media.by_slug(kind_slug)
    if kind is None:
        return
    if press.message_id is not None:
        await clear_reply_markup(press.chat_id, press.message_id)
    try:
        title, _content, _labels, _reviews = await get_media_details(note_id)
    except Exception as exc:
        await _report_failure(press.chat_id, "get_media_details", "Не получилось прочитать", exc)
        return
    await start_media_add_flow(press.chat_id, kind, title, press.message_id, note_id=note_id)


async def handle_media_mark(press: Press) -> None:
    """Переставить состояние. Для «посмотрел» следом идёт диалог оценки —
    но метка ставится ДО вопросов, как у книг: брошенный диалог оставляет
    фильм корректно отмеченным, просто без отзыва."""
    await answer_callback_query(press.id)
    _prefix, kind_slug, state, note_id = press.data.split(":", 3)
    kind = media.by_slug(kind_slug)
    if kind is None:
        return
    chat_id = press.chat_id
    person = _media_person(chat_id)

    thread = None
    if press.message_id is not None:
        await clear_reply_markup(chat_id, press.message_id)
        thread = await threads.thread_for_message(chat_id, press.message_id)
    thread_id = thread["id"] if thread is not None else None

    try:
        title, _content, _labels, _reviews = await get_media_details(note_id)
        await mark_media(kind, note_id, person, state)
    except Exception as exc:
        await _report_failure(chat_id, "mark_media", "Не получилось отметить", exc, retry=True)
        return

    if state != media.DONE:
        said = {
            media.WANT: "Вернул в «хочу посмотреть»",
            media.IN_PROGRESS: "Отметил, что смотришь",
            media.DROPPED: "Убрал из списка",
        }[state]
        await threads.send(thread_id, chat_id, f"{said}: {title}")
        await _dismiss_thread_by_id(thread_id)
        return

    prompt_id = await create_media_review_prompt(chat_id, note_id, title, kind.slug, thread_id)
    if await threads.send(thread_id, chat_id, book_review_step_text(0)) is None:
        await close_book_review_prompt(prompt_id)


_PROMPT_HANDLERS["media_add"] = _handle_media_add_reply


# Каждый вид диалога должен быть и в карте таблиц (app/db.py), и в карте
# обработчиков. Забыть одно из двух легко, и это не падает при импорте — оно
# ждёт живого человека: get_open_prompt находит открытый вопрос, а получить
# строку или позвать обработчик уже нечем, и бот молча зависает посреди
# диалога. Так и случилось с тратами. Проверка на импорте ловит это до пуша.
assert set(_PROMPT_TABLES) == set(_PROMPT_HANDLERS), (
    f"виды диалогов разошлись: {set(_PROMPT_TABLES) ^ set(_PROMPT_HANDLERS)}"
)
