import json
from datetime import datetime, timedelta, timezone

import asyncpg

from app.config import DATABASE_URL

SCHEMA = """
CREATE TABLE IF NOT EXISTS incoming_messages (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    confirmed_at TIMESTAMPTZ,
    kind TEXT NOT NULL DEFAULT 'passive',
    reply_to_text TEXT
);

CREATE TABLE IF NOT EXISTS registered_users (
    chat_id BIGINT PRIMARY KEY,
    registered_at TIMESTAMPTZ NOT NULL,
    odysseus_session_id TEXT
);

CREATE TABLE IF NOT EXISTS reminders (
    id SERIAL PRIMARY KEY,
    target_chat_id BIGINT NOT NULL,
    sender_chat_id BIGINT NOT NULL,
    message TEXT NOT NULL,
    run_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    sent_at TIMESTAMPTZ,
    anonymous BOOLEAN NOT NULL DEFAULT FALSE
);

-- DB-backed like outgoing_messages, unlike the old purely
-- in-memory APScheduler date-jobs this replaces: a row's due_at survives
-- any number of process restarts untouched, so a redeploy can never again
-- silently reroll an already-past slot and drop the day's reminder (see
-- scheduler.py's release_due_water_reminders / ensure_today_water_reminders
-- for the confirmed-live bug this fixes). One row per (user, window, day).
CREATE TABLE IF NOT EXISTS water_reminders (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    window_index INTEGER NOT NULL,
    for_date DATE NOT NULL,
    due_at TIMESTAMPTZ NOT NULL,
    sent_at TIMESTAMPTZ,
    UNIQUE (user_id, window_index, for_date)
);

-- Replaces the old random-pool daily question (see scheduler.py) — three
-- fixed check-ins per day (am ~12:30 / pm ~18:00 / evening ~21:30) instead
-- of one random question, each a strict one-question-at-a-time sequence
-- (see app/prompts.py's EZHEDNEVNIK_STEPS) — step is the 0-based index into
-- that slot's step list, collected holds every answer gathered so far this
-- round as {field_name: value}. is_open gates "don't send a second one
-- while the first is still unanswered".
CREATE TABLE IF NOT EXISTS ezhednevnik_prompts (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    slot TEXT NOT NULL CHECK (slot IN ('am', 'pm', 'evening')),
    sent_at TIMESTAMPTZ NOT NULL,
    is_open BOOLEAN NOT NULL DEFAULT TRUE,
    step INTEGER NOT NULL DEFAULT 0,
    collected JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- User-initiated activity logging (yoga / китайский / трейдинг) — same
-- one-question-at-a-time shape as ezhednevnik_prompts (feedback, then
-- score), but started by the person's own message ("позанималась йогой")
-- rather than a scheduled slot, and written to that person's own ТРЕКЕР
-- РУТИНЫ {ИМЯ} note instead of ЕЖЕДНЕВНИК.
CREATE TABLE IF NOT EXISTS activity_prompts (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    activity TEXT NOT NULL CHECK (activity IN ('yoga', 'chinese', 'trading')),
    sent_at TIMESTAMPTZ NOT NULL,
    is_open BOOLEAN NOT NULL DEFAULT TRUE,
    step INTEGER NOT NULL DEFAULT 0,
    collected JSONB NOT NULL DEFAULT '{}'::jsonb
);

-- User-initiated "add an interesting book moment" flow (/quote or the word
-- "цитата") — same one-question-at-a-time shape as activity_prompts, but
-- with an extra button-driven step 0 (pick which book, from the notes
-- under КНИГИ that have readingStart set and no readingEnd) before the two
-- text steps (quote, then impression). collected holds {"candidates": [...]}
-- while step 0 is still open (so the callback handler can resolve a button
-- press back to a note_id/title without re-querying Trilium), then the
-- quote/impression answers once step advances past 0. Short-lived by
-- design (unlike ежедневник/activity) — updated_at tracks the last real
-- step so release_stale_quote_prompts (scheduler.py) can delete the whole
-- exchange (message_ids) and close it out 5 minutes after the person goes
-- quiet, rather than leaving a half-answered "Какую книгу?" sitting in the
-- chat forever.
CREATE TABLE IF NOT EXISTS book_quote_prompts (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    is_open BOOLEAN NOT NULL DEFAULT TRUE,
    step INTEGER NOT NULL DEFAULT 0,
    book_note_id TEXT,
    book_title TEXT,
    collected JSONB NOT NULL DEFAULT '{}'::jsonb,
    message_ids INTEGER[] NOT NULL DEFAULT '{}'
);

-- One /reading or /finished interaction, start to finish: the triggering
-- command, the book list, the description, and (if "Я дочитал" is pressed)
-- the whole rating/review exchange. Unlike the per-flow message_ids columns
-- elsewhere, this spans several flows, because from the person's point of
-- view it's all one conversation that should disappear together — either
-- 5 minutes after the last activity (see scheduler.py's
-- release_stale_message_threads) or as soon as they put a reaction on any
-- message in it (see app/service.py's handle_message_reaction).
CREATE TABLE IF NOT EXISTS message_threads (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    message_ids INTEGER[] NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL,
    is_open BOOLEAN NOT NULL DEFAULT TRUE
);

-- "Я дочитал" flow (the button on /reading's book description — see
-- app/service.py's handle_book_finished) — two steps, rating then free-text
-- review, which together become a review note cloned into both the book
-- and ОТЗЫВЫ НА КНИГИ. readingEnd is stamped on the book immediately at
-- button press, before this row is even created, so an abandoned dialogue
-- still leaves the book correctly marked as finished.
CREATE TABLE IF NOT EXISTS book_review_prompts (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    book_note_id TEXT NOT NULL,
    book_title TEXT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    is_open BOOLEAN NOT NULL DEFAULT TRUE,
    step INTEGER NOT NULL DEFAULT 0,
    rating INTEGER,
    thread_id INTEGER
);

-- /addbook flow (title, then author — see app/service.py) — stage 1 is the
-- normal is_open/step shape, closed (is_open=FALSE) once the book note is
-- created and the "расскажи подробнее" template is sent. Unlike every other
-- prompt table, the row is never deleted or made unreachable after that:
-- template_message_id stays valid forever so a reply to that message —
-- Telegram's native reply, matched by its message_id — can be found and
-- applied at any later time (stage 2, filling in author/annotation/genre/
-- similar-books), even days later, per explicit request. template_sent_at +
-- details_notified only drive the one-time "Добавил книгу" courtesy
-- message if nothing comes back within 5 minutes — they don't gate or
-- expire the reply itself.
CREATE TABLE IF NOT EXISTS book_add_prompts (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    is_open BOOLEAN NOT NULL DEFAULT TRUE,
    step INTEGER NOT NULL DEFAULT 0,
    title TEXT,
    author TEXT,
    book_note_id TEXT,
    template_message_id BIGINT,
    template_sent_at TIMESTAMPTZ,
    details_notified BOOLEAN NOT NULL DEFAULT FALSE,
    message_ids INTEGER[] NOT NULL DEFAULT '{}'
);

-- Every message sent to/from a chat, purely so the nightly cleanup job
-- (scheduler.py's clear_chat_history) knows which Telegram message_ids to
-- delete. Telegram gives bots no "list messages in this chat" API — the
-- only way to delete a message is to already know its id, so every send
-- (telegram.py) and every incoming webhook update (main.py) logs itself
-- here; the cleanup job deletes each one, then clears the rows it handled.
CREATE TABLE IF NOT EXISTS chat_messages_log (
    id SERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

-- DB-backed like water_reminders, unlike the in-memory APScheduler
-- trigger="date" jobs this replaces: confirmed live, a 3-minute delayed
-- delete job scheduled purely in process memory silently vanished when a
-- redeploy landed inside that window, leaving the message stuck forever
-- with no trace it was ever supposed to be cleaned up. A row here survives
-- any number of restarts untouched.
CREATE TABLE IF NOT EXISTS pending_message_deletions (
    id SERIAL PRIMARY KEY,
    chat_id BIGINT NOT NULL,
    message_id BIGINT NOT NULL,
    due_at TIMESTAMPTZ NOT NULL
);

ALTER TABLE registered_users ADD COLUMN IF NOT EXISTS odysseus_session_id TEXT;
ALTER TABLE incoming_messages ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'passive';
ALTER TABLE incoming_messages ADD COLUMN IF NOT EXISTS reply_to_text TEXT;
-- Lets an edited Telegram message ("я поторопился, дай поправлю") retroactively
-- patch the one ЕЖЕДНЕВНИК cell it originally answered — see
-- app/service.py's handle_message_edit. telegram_message_id is populated on
-- every insert going forward; entry_date only on ezhednevnik_* answer rows
-- (the day the question was SENT, same value fill_ezhednevnik already uses),
-- since that's the only flow this supports. Both NULL for everything else.
ALTER TABLE incoming_messages ADD COLUMN IF NOT EXISTS telegram_message_id BIGINT;
ALTER TABLE incoming_messages ADD COLUMN IF NOT EXISTS entry_date DATE;
ALTER TABLE reminders ADD COLUMN IF NOT EXISTS anonymous BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE ezhednevnik_prompts DROP CONSTRAINT IF EXISTS ezhednevnik_prompts_slot_check;
ALTER TABLE ezhednevnik_prompts ADD CONSTRAINT ezhednevnik_prompts_slot_check CHECK (slot IN ('am', 'pm', 'evening'));
ALTER TABLE ezhednevnik_prompts DROP COLUMN IF EXISTS stage;
ALTER TABLE ezhednevnik_prompts DROP COLUMN IF EXISTS pending_text;
ALTER TABLE ezhednevnik_prompts ADD COLUMN IF NOT EXISTS step INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ezhednevnik_prompts ADD COLUMN IF NOT EXISTS collected JSONB NOT NULL DEFAULT '{}'::jsonb;
-- Every message belonging to a /addbook exchange (trigger, both questions
-- and answers, the template, the details answer, the final confirmation)
-- — cleared once both stages finish successfully, see
-- app/service.py's _apply_book_details.
ALTER TABLE book_add_prompts ADD COLUMN IF NOT EXISTS message_ids INTEGER[] NOT NULL DEFAULT '{}';
-- book_quote_prompts.updated_at/message_ids were added straight into the
-- CREATE TABLE above (for the 5-minute-timeout-with-deletion redesign),
-- but the table already existed live at that point, so CREATE TABLE IF NOT
-- EXISTS silently never added them — confirmed live: every /quote attempt
-- since that deploy 500'd on "column updated_at does not exist" with no
-- reply ever reaching the person, since create_book_quote_prompt is the
-- very first write in the flow. message_ids has a DEFAULT so it backfills
-- on its own; updated_at doesn't, so it's backfilled from sent_at (a
-- reasonable stand-in — these rows are short-lived anyway) before being
-- made NOT NULL.
ALTER TABLE book_quote_prompts ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
UPDATE book_quote_prompts SET updated_at = sent_at WHERE updated_at IS NULL;
ALTER TABLE book_quote_prompts ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE book_quote_prompts ADD COLUMN IF NOT EXISTS message_ids INTEGER[] NOT NULL DEFAULT '{}';
ALTER TABLE book_review_prompts ADD COLUMN IF NOT EXISTS thread_id INTEGER;
-- Review-flow messages are tracked on the /reading thread that spawned
-- them (message_threads), which was double-counting them here.
ALTER TABLE book_review_prompts DROP COLUMN IF EXISTS message_ids;
-- Flashcard feature removed entirely (unused, per explicit confirmation
-- there was nothing worth keeping in it) — drops the tables outright
-- rather than leaving them as dead weight nothing references anymore.
DROP TABLE IF EXISTS cards;
DROP TABLE IF EXISTS card_sessions;
DROP TABLE IF EXISTS card_reminders;
-- Old random-pool daily question/reply system removed entirely — replaced
-- by the fixed-time ежедневник check-ins (see ezhednevnik_prompts above).
DROP TABLE IF EXISTS outgoing_messages;
"""

_pool: asyncpg.Pool | None = None


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


async def get_pool() -> asyncpg.Pool:
    global _pool
    if _pool is None:
        _pool = await asyncpg.create_pool(DATABASE_URL)
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


async def init_db() -> None:
    pool = await get_pool()
    await pool.execute(SCHEMA)


# --- registration -----------------------------------------------------

async def is_registered(chat_id: int) -> bool:
    pool = await get_pool()
    row = await pool.fetchval("SELECT 1 FROM registered_users WHERE chat_id = $1", chat_id)
    return row is not None


async def count_registered() -> int:
    pool = await get_pool()
    return await pool.fetchval("SELECT COUNT(*) FROM registered_users")


async def register_user(chat_id: int) -> None:
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO registered_users (chat_id, registered_at) VALUES ($1, $2) "
        "ON CONFLICT (chat_id) DO NOTHING",
        chat_id, utcnow(),
    )


async def get_registered_user_ids() -> list[int]:
    pool = await get_pool()
    rows = await pool.fetch("SELECT chat_id FROM registered_users")
    return [row["chat_id"] for row in rows]


async def get_odysseus_session_id(chat_id: int) -> str | None:
    pool = await get_pool()
    return await pool.fetchval(
        "SELECT odysseus_session_id FROM registered_users WHERE chat_id = $1", chat_id
    )


async def set_odysseus_session_id(chat_id: int, session_id: str) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE registered_users SET odysseus_session_id = $1 WHERE chat_id = $2",
        session_id, chat_id,
    )


# --- incoming messages -----------------------------------------------------

async def insert_incoming_message(
    user_id: int, text: str, kind: str = "passive", reply_to_text: str | None = None,
    telegram_message_id: int | None = None, entry_date=None,
) -> int:
    pool = await get_pool()
    return await pool.fetchval(
        "INSERT INTO incoming_messages "
        "(user_id, text, created_at, kind, reply_to_text, telegram_message_id, entry_date) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING id",
        user_id, text, utcnow(), kind, reply_to_text, telegram_message_id, entry_date,
    )


async def get_incoming_message_by_telegram_id(user_id: int, telegram_message_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow(
        "SELECT * FROM incoming_messages WHERE user_id = $1 AND telegram_message_id = $2 "
        "ORDER BY id DESC LIMIT 1",
        user_id, telegram_message_id,
    )


async def update_incoming_message_text(message_id: int, new_text: str) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE incoming_messages SET text = $1 WHERE id = $2", new_text, message_id)


async def pull_unconfirmed_incoming() -> list[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch(
        "SELECT id, user_id, text, created_at, kind, reply_to_text FROM incoming_messages "
        "WHERE confirmed_at IS NULL ORDER BY id"
    )


async def get_recent_incoming_messages(user_id: int, limit: int = 10) -> list[asyncpg.Record]:
    """Diagnostic: newest incoming_messages rows for one user, including
    telegram_message_id/entry_date — for confirming handle_message_edit
    (app/service.py) can actually find and tag the row it needs to."""
    pool = await get_pool()
    return await pool.fetch(
        "SELECT * FROM incoming_messages WHERE user_id = $1 ORDER BY id DESC LIMIT $2",
        user_id, limit,
    )


async def ack_incoming_messages(ids: list[int]) -> None:
    if not ids:
        return
    pool = await get_pool()
    await pool.execute(
        "UPDATE incoming_messages SET confirmed_at = $1 WHERE id = ANY($2::int[])",
        utcnow(), ids,
    )


# --- ежедневник check-ins (replaces the random-pool daily question) -------

async def has_open_ezhednevnik(user_id: int) -> bool:
    pool = await get_pool()
    row = await pool.fetchval(
        "SELECT 1 FROM ezhednevnik_prompts WHERE user_id = $1 AND is_open = TRUE LIMIT 1",
        user_id,
    )
    return row is not None


async def close_stale_ezhednevnik_prompts(user_id: int, before: datetime) -> int:
    """Auto-recovery: an open prompt nobody ever answers must not block
    every future check-in forever. Confirmed live: a PM prompt sent
    2026-08-03 stayed open (unanswered) and silently blocked every single
    AM/PM send for that user for the next two days, since has_open_
    ezhednevnik only checks "is ANY row open", with no expiry at all —
    unlike card sessions (10min idle timeout) or the old daily question
    (deferral with eventual reset). Closes anything still open from before
    `before` (start of today) — the person can still reply to it if they
    want (it just won't be "the open one" get_open_ezhednevnik_prompt picks
    up anymore), it just stops blocking new ones. Returns how many closed."""
    pool = await get_pool()
    result = await pool.execute(
        "UPDATE ezhednevnik_prompts SET is_open = FALSE "
        "WHERE user_id = $1 AND is_open = TRUE AND sent_at < $2",
        user_id, before,
    )
    return int(result.split()[-1]) if result else 0


async def create_ezhednevnik_prompt(user_id: int, slot: str) -> int:
    pool = await get_pool()
    return await pool.fetchval(
        "INSERT INTO ezhednevnik_prompts (user_id, slot, sent_at, is_open) "
        "VALUES ($1, $2, $3, TRUE) RETURNING id",
        user_id, slot, utcnow(),
    )


async def get_open_ezhednevnik_prompt(user_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow(
        "SELECT * FROM ezhednevnik_prompts WHERE user_id = $1 AND is_open = TRUE "
        "ORDER BY sent_at DESC LIMIT 1",
        user_id,
    )


async def advance_ezhednevnik_step(prompt_id: int, step: int, collected: dict) -> None:
    """Move to the next question in the slot's step sequence (see
    app/prompts.py's EZHEDNEVNIK_STEPS) — the prompt stays open, collected
    accumulates every answer gathered so far this round. service.py sends
    the next question itself without ever involving Odysseus for this."""
    pool = await get_pool()
    await pool.execute(
        "UPDATE ezhednevnik_prompts SET step = $1, collected = $2::jsonb WHERE id = $3",
        step, json.dumps(collected), prompt_id,
    )


async def get_recent_reminders(user_id: int, limit: int = 15) -> list[asyncpg.Record]:
    """Diagnostic: most recent reminders/relays targeting a user, newest
    first — for tracing an unexpected/duplicate delivery back to its
    original created_at, run_at and sent_at."""
    pool = await get_pool()
    return await pool.fetch(
        "SELECT * FROM reminders WHERE target_chat_id = $1 ORDER BY created_at DESC LIMIT $2",
        user_id, limit,
    )


async def get_ezhednevnik_prompts_for_user(user_id: int) -> list[asyncpg.Record]:
    """Diagnostic: every ezhednevnik_prompts row for a user, newest first —
    for confirming why a trigger did or didn't actually send (has_open_
    ezhednevnik gates on ANY open row regardless of slot, so an unanswered
    PM prompt silently blocks a same-day AM trigger too)."""
    pool = await get_pool()
    return await pool.fetch(
        "SELECT * FROM ezhednevnik_prompts WHERE user_id = $1 ORDER BY sent_at DESC LIMIT 5",
        user_id,
    )


async def close_ezhednevnik_prompt(prompt_id: int) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE ezhednevnik_prompts SET is_open = FALSE WHERE id = $1", prompt_id)


# --- activity logging (yoga / chinese / trading) ----------------------------

async def get_open_activity_prompt(user_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow(
        "SELECT * FROM activity_prompts WHERE user_id = $1 AND is_open = TRUE "
        "ORDER BY sent_at DESC LIMIT 1",
        user_id,
    )


async def create_activity_prompt(user_id: int, activity: str) -> int:
    pool = await get_pool()
    return await pool.fetchval(
        "INSERT INTO activity_prompts (user_id, activity, sent_at, is_open) "
        "VALUES ($1, $2, $3, TRUE) RETURNING id",
        user_id, activity, utcnow(),
    )


async def advance_activity_prompt_step(prompt_id: int, step: int, collected: dict) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE activity_prompts SET step = $1, collected = $2::jsonb WHERE id = $3",
        step, json.dumps(collected), prompt_id,
    )


async def close_activity_prompt(prompt_id: int) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE activity_prompts SET is_open = FALSE WHERE id = $1", prompt_id)


# --- book quotes ("interesting moments") -------------------------------------

async def get_open_book_quote_prompt(user_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow(
        "SELECT * FROM book_quote_prompts WHERE user_id = $1 AND is_open = TRUE "
        "ORDER BY sent_at DESC LIMIT 1",
        user_id,
    )


async def get_book_quote_prompt(prompt_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow("SELECT * FROM book_quote_prompts WHERE id = $1", prompt_id)


async def create_book_quote_prompt(user_id: int, candidates: list[dict]) -> int:
    pool = await get_pool()
    now = utcnow()
    return await pool.fetchval(
        "INSERT INTO book_quote_prompts (user_id, sent_at, updated_at, is_open, collected) "
        "VALUES ($1, $2, $2, TRUE, $3::jsonb) RETURNING id",
        user_id, now, json.dumps({"candidates": candidates}),
    )


async def set_book_quote_prompt_book(prompt_id: int, note_id: str, title: str) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE book_quote_prompts SET step = 1, book_note_id = $1, book_title = $2, updated_at = $3 "
        "WHERE id = $4",
        note_id, title, utcnow(), prompt_id,
    )


async def advance_book_quote_prompt_step(prompt_id: int, step: int, collected: dict) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE book_quote_prompts SET step = $1, collected = $2::jsonb, updated_at = $3 WHERE id = $4",
        step, json.dumps(collected), utcnow(), prompt_id,
    )


async def append_book_quote_prompt_message(prompt_id: int, message_id: int) -> None:
    """Records a message sent as part of this flow (the book-choice buttons,
    or one of the two follow-up questions) so release_stale_quote_prompts
    (scheduler.py) can delete the whole exchange if the person goes quiet."""
    pool = await get_pool()
    await pool.execute(
        "UPDATE book_quote_prompts SET message_ids = array_append(message_ids, $1) WHERE id = $2",
        message_id, prompt_id,
    )


QUOTE_PROMPT_STALE_AFTER = timedelta(minutes=5)


async def get_stale_book_quote_prompts() -> list[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch(
        "SELECT * FROM book_quote_prompts WHERE is_open = TRUE AND updated_at < $1",
        utcnow() - QUOTE_PROMPT_STALE_AFTER,
    )


async def close_book_quote_prompt(prompt_id: int) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE book_quote_prompts SET is_open = FALSE WHERE id = $1", prompt_id)


# --- /addbook -----------------------------------------------------------

async def get_open_book_add_prompt(user_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow(
        "SELECT * FROM book_add_prompts WHERE user_id = $1 AND is_open = TRUE "
        "ORDER BY sent_at DESC LIMIT 1",
        user_id,
    )


async def get_book_add_prompt(prompt_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow("SELECT * FROM book_add_prompts WHERE id = $1", prompt_id)


async def create_book_add_prompt(user_id: int) -> int:
    pool = await get_pool()
    now = utcnow()
    return await pool.fetchval(
        "INSERT INTO book_add_prompts (user_id, sent_at, updated_at, is_open) "
        "VALUES ($1, $2, $2, TRUE) RETURNING id",
        user_id, now,
    )


async def set_book_add_title(prompt_id: int, title: str) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE book_add_prompts SET step = 1, title = $1, updated_at = $2 WHERE id = $3",
        title, utcnow(), prompt_id,
    )


async def close_book_add_prompt(prompt_id: int) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE book_add_prompts SET is_open = FALSE WHERE id = $1", prompt_id)


async def finalize_book_add_prompt(
    prompt_id: int, author: str, book_note_id: str, template_message_id: int,
) -> None:
    """Marks stage 1 done (book note created, template sent) and advances to
    step 2 — LEAVES is_open TRUE, so the very next plain message is still
    auto-captured as the details answer, same as every other flow's normal
    continuation (see app/service.py's process_incoming_message). That
    immediate window closes 5 minutes later (release_due_book_add_notices,
    scheduler.py, which also calls close_book_add_prompt) — after that,
    only a reply to this exact template_message_id still works (see
    get_book_add_prompt_by_template_message, which ignores is_open
    entirely) — that's the "answer after the normal window expired" case."""
    pool = await get_pool()
    now = utcnow()
    await pool.execute(
        "UPDATE book_add_prompts SET step = 2, author = $1, book_note_id = $2, "
        "template_message_id = $3, template_sent_at = $4, updated_at = $4 WHERE id = $5",
        author, book_note_id, template_message_id, now, prompt_id,
    )


async def get_book_add_prompt_by_template_message(user_id: int, template_message_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow(
        "SELECT * FROM book_add_prompts WHERE user_id = $1 AND template_message_id = $2",
        user_id, template_message_id,
    )


async def get_due_book_add_notices() -> list[asyncpg.Record]:
    """Rows whose template was sent 5+ minutes ago with no reply yet and no
    courtesy notice sent — see scheduler.py's release_due_book_add_notices.
    Does NOT close/expire anything — a reply is still accepted afterward."""
    pool = await get_pool()
    return await pool.fetch(
        "SELECT * FROM book_add_prompts WHERE template_message_id IS NOT NULL "
        "AND details_notified = FALSE AND template_sent_at < $1",
        utcnow() - timedelta(minutes=5),
    )


async def mark_book_add_notified(prompt_id: int) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE book_add_prompts SET details_notified = TRUE WHERE id = $1", prompt_id)


async def append_book_add_prompt_message(prompt_id: int, message_id: int) -> None:
    """Records one message belonging to this /addbook exchange (either
    direction) — see _apply_book_details in app/service.py, which deletes
    every recorded message once both stages finish successfully."""
    pool = await get_pool()
    await pool.execute(
        "UPDATE book_add_prompts SET message_ids = array_append(message_ids, $1) WHERE id = $2",
        message_id, prompt_id,
    )


# --- book reviews ("я дочитал") ---------------------------------------------

async def get_open_book_review_prompt(user_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow(
        "SELECT * FROM book_review_prompts WHERE user_id = $1 AND is_open = TRUE "
        "ORDER BY sent_at DESC LIMIT 1",
        user_id,
    )


async def create_book_review_prompt(
    user_id: int, book_note_id: str, book_title: str, thread_id: int | None = None,
) -> int:
    pool = await get_pool()
    now = utcnow()
    return await pool.fetchval(
        "INSERT INTO book_review_prompts "
        "(user_id, book_note_id, book_title, sent_at, updated_at, is_open, thread_id) "
        "VALUES ($1, $2, $3, $4, $4, TRUE, $5) RETURNING id",
        user_id, book_note_id, book_title, now, thread_id,
    )


async def set_book_review_rating(prompt_id: int, rating: int) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE book_review_prompts SET step = 1, rating = $1, updated_at = $2 WHERE id = $3",
        rating, utcnow(), prompt_id,
    )


async def close_book_review_prompt(prompt_id: int) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE book_review_prompts SET is_open = FALSE WHERE id = $1", prompt_id)


async def get_book_review_prompt(prompt_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow("SELECT * FROM book_review_prompts WHERE id = $1", prompt_id)


async def get_stale_book_review_prompts() -> list[asyncpg.Record]:
    """Same 5-minute staleness rule as book_quote_prompts — see
    scheduler.py's release_stale_book_review_prompts."""
    pool = await get_pool()
    return await pool.fetch(
        "SELECT * FROM book_review_prompts WHERE is_open = TRUE AND updated_at < $1",
        utcnow() - QUOTE_PROMPT_STALE_AFTER,
    )


# --- message threads (/reading & /finished conversations) --------------------

async def create_message_thread(user_id: int) -> int:
    pool = await get_pool()
    return await pool.fetchval(
        "INSERT INTO message_threads (user_id, updated_at, is_open) VALUES ($1, $2, TRUE) RETURNING id",
        user_id, utcnow(),
    )


async def append_thread_message(thread_id: int, message_id: int) -> None:
    """Also bumps updated_at — the 5-minute staleness window is measured
    from the last thing that happened in the thread, not from its start."""
    pool = await get_pool()
    await pool.execute(
        "UPDATE message_threads SET message_ids = array_append(message_ids, $1), updated_at = $2 "
        "WHERE id = $3",
        message_id, utcnow(), thread_id,
    )


async def get_thread_by_message(user_id: int, message_id: int) -> asyncpg.Record | None:
    """Which open thread (if any) a given Telegram message belongs to — for
    resolving both a button press and a reaction back to its conversation."""
    pool = await get_pool()
    return await pool.fetchrow(
        "SELECT * FROM message_threads WHERE user_id = $1 AND is_open = TRUE "
        "AND $2 = ANY(message_ids) ORDER BY id DESC LIMIT 1",
        user_id, message_id,
    )


async def get_message_thread(thread_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow("SELECT * FROM message_threads WHERE id = $1", thread_id)


async def close_message_thread(thread_id: int) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE message_threads SET is_open = FALSE WHERE id = $1", thread_id)


async def get_stale_message_threads() -> list[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch(
        "SELECT * FROM message_threads WHERE is_open = TRUE AND updated_at < $1",
        utcnow() - QUOTE_PROMPT_STALE_AFTER,
    )


async def close_book_review_prompts_for_thread(thread_id: int) -> None:
    """A thread going away takes any half-finished review dialogue with it —
    its questions are among the messages just deleted, so leaving the prompt
    open would silently swallow the person's next unrelated message."""
    pool = await get_pool()
    await pool.execute(
        "UPDATE book_review_prompts SET is_open = FALSE WHERE thread_id = $1 AND is_open = TRUE",
        thread_id,
    )


# --- chat message log (nightly cleanup) -------------------------------------

async def log_chat_message(chat_id: int, message_id: int) -> None:
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO chat_messages_log (chat_id, message_id, created_at) VALUES ($1, $2, $3)",
        chat_id, message_id, utcnow(),
    )


async def pop_logged_messages_except(skip_chat_ids: set[int]) -> list[asyncpg.Record]:
    """Fetch and clear the log in one atomic step, EXCLUDING any chat_id in
    skip_chat_ids — used once a night by clear_chat_history(), which skips
    a person's chat entirely while they have an open ежедневник/activity
    question (clearing mid check-in would delete the bot's own pending
    question along with everything else, losing where things left off).
    Rows for a skipped chat_id are left untouched in the log for a future
    night's attempt, once that question is answered or auto-closed.
    Deleting the popped rows here (not after the Telegram calls) means a
    message that's already gone from the log never gets retried forever
    even if its own deleteMessage call happens to fail (harmless either
    way — Telegram just 400s on an already-deleted or too-old message)."""
    pool = await get_pool()
    skip_list = list(skip_chat_ids)
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                "SELECT * FROM chat_messages_log WHERE NOT (chat_id = ANY($1::bigint[])) ORDER BY id",
                skip_list,
            )
            await conn.execute(
                "DELETE FROM chat_messages_log WHERE NOT (chat_id = ANY($1::bigint[]))",
                skip_list,
            )
            return rows


async def peek_logged_messages(limit: int = 20) -> list[asyncpg.Record]:
    """Diagnostic: newest logged (not-yet-cleared) messages, without
    popping them — for confirming logging is actually happening ahead of
    tonight's real clear_chat_history run."""
    pool = await get_pool()
    return await pool.fetch("SELECT * FROM chat_messages_log ORDER BY id DESC LIMIT $1", limit)


async def schedule_pending_message_deletion(chat_id: int, message_id: int, due_at: datetime) -> None:
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO pending_message_deletions (chat_id, message_id, due_at) VALUES ($1, $2, $3)",
        chat_id, message_id, due_at,
    )


async def pop_due_message_deletions() -> list[asyncpg.Record]:
    """Same atomic fetch-and-clear shape as pop_all_logged_messages — a
    failed deleteMessage call (already gone, too old, etc.) must not retry
    forever, just be dropped either way."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch(
                "SELECT * FROM pending_message_deletions WHERE due_at <= $1", utcnow(),
            )
            if rows:
                await conn.execute(
                    "DELETE FROM pending_message_deletions WHERE id = ANY($1::int[])",
                    [r["id"] for r in rows],
                )
            return rows


async def peek_pending_message_deletions(limit: int = 20) -> list[asyncpg.Record]:
    """Diagnostic: everything currently queued for deletion, without
    popping it — for confirming scheduling is actually happening ahead of
    the next 60s release_due_message_deletions tick."""
    pool = await get_pool()
    return await pool.fetch(
        "SELECT * FROM pending_message_deletions ORDER BY id DESC LIMIT $1", limit,
    )


# --- reminders (schedule_send tool) -----------------------------------------

async def insert_reminder(
    target_chat_id: int, sender_chat_id: int, message: str, run_at: datetime,
    anonymous: bool = False,
) -> int:
    pool = await get_pool()
    return await pool.fetchval(
        "INSERT INTO reminders (target_chat_id, sender_chat_id, message, run_at, created_at, anonymous) "
        "VALUES ($1, $2, $3, $4, $5, $6) RETURNING id",
        target_chat_id, sender_chat_id, message, run_at, utcnow(), anonymous,
    )


async def get_due_reminders() -> list[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch(
        "SELECT * FROM reminders WHERE sent_at IS NULL AND run_at <= $1", utcnow()
    )


# --- water reminders (DB-backed due_at, same shape as card_reminders' release
# side — no is_open/deferral here since there's nothing to defer, just a
# fire-once slot per user/window/day) -----------------------------------

async def ensure_water_reminder(user_id: int, window_index: int, for_date, due_at: datetime) -> None:
    """Idempotent — ON CONFLICT DO NOTHING means calling this again for a
    (user, window, day) that already has a row (e.g. every restart re-runs
    the day's setup) is always a harmless no-op, unlike the old in-memory
    APScheduler jobs it replaces which got wiped and re-rolled on every
    restart."""
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO water_reminders (user_id, window_index, for_date, due_at) "
        "VALUES ($1, $2, $3, $4) ON CONFLICT (user_id, window_index, for_date) DO NOTHING",
        user_id, window_index, for_date, due_at,
    )


async def get_due_water_reminders() -> list[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch(
        "SELECT * FROM water_reminders WHERE sent_at IS NULL AND due_at <= $1", utcnow(),
    )


async def mark_water_reminder_sent(reminder_id: int) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE water_reminders SET sent_at = $1 WHERE id = $2", utcnow(), reminder_id)


async def get_water_reminders_for_date(user_id: int, for_date) -> list[asyncpg.Record]:
    """Diagnostic: today's rows for a user regardless of send state — for
    confirming ensure_today_water_reminders() actually created them, unlike
    get_due_water_reminders() which only surfaces still-unsent ones."""
    pool = await get_pool()
    return await pool.fetch(
        "SELECT window_index, due_at, sent_at FROM water_reminders "
        "WHERE user_id = $1 AND for_date = $2 ORDER BY window_index",
        user_id, for_date,
    )


async def claim_reminder(reminder_id: int) -> bool:
    """Atomically mark a reminder as sent, but only if nobody has claimed it
    yet. The immediate ("now") delivery check in app/sync.py and the 60s
    release_due_reminders() poll can both see the same unsent row — this
    single conditional UPDATE is what guarantees only one of them actually
    sends it, instead of both racing past a separate read-then-write check."""
    pool = await get_pool()
    result = await pool.fetchval(
        "UPDATE reminders SET sent_at = $1 WHERE id = $2 AND sent_at IS NULL RETURNING id",
        utcnow(), reminder_id,
    )
    return result is not None
