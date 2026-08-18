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

ALTER TABLE registered_users ADD COLUMN IF NOT EXISTS odysseus_session_id TEXT;
ALTER TABLE incoming_messages ADD COLUMN IF NOT EXISTS kind TEXT NOT NULL DEFAULT 'passive';
ALTER TABLE incoming_messages ADD COLUMN IF NOT EXISTS reply_to_text TEXT;
ALTER TABLE reminders ADD COLUMN IF NOT EXISTS anonymous BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE ezhednevnik_prompts DROP CONSTRAINT IF EXISTS ezhednevnik_prompts_slot_check;
ALTER TABLE ezhednevnik_prompts ADD CONSTRAINT ezhednevnik_prompts_slot_check CHECK (slot IN ('am', 'pm', 'evening'));
ALTER TABLE ezhednevnik_prompts DROP COLUMN IF EXISTS stage;
ALTER TABLE ezhednevnik_prompts DROP COLUMN IF EXISTS pending_text;
ALTER TABLE ezhednevnik_prompts ADD COLUMN IF NOT EXISTS step INTEGER NOT NULL DEFAULT 0;
ALTER TABLE ezhednevnik_prompts ADD COLUMN IF NOT EXISTS collected JSONB NOT NULL DEFAULT '{}'::jsonb;
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
) -> int:
    pool = await get_pool()
    return await pool.fetchval(
        "INSERT INTO incoming_messages (user_id, text, created_at, kind, reply_to_text) "
        "VALUES ($1, $2, $3, $4, $5) RETURNING id",
        user_id, text, utcnow(), kind, reply_to_text,
    )


async def pull_unconfirmed_incoming() -> list[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch(
        "SELECT id, user_id, text, created_at, kind, reply_to_text FROM incoming_messages "
        "WHERE confirmed_at IS NULL ORDER BY id"
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


# --- chat message log (nightly cleanup) -------------------------------------

async def log_chat_message(chat_id: int, message_id: int) -> None:
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO chat_messages_log (chat_id, message_id, created_at) VALUES ($1, $2, $3)",
        chat_id, message_id, utcnow(),
    )


async def pop_all_logged_messages() -> list[asyncpg.Record]:
    """Fetch and clear the whole log in one atomic step — used once a night
    by clear_chat_history(). Deleting the rows here (not after the Telegram
    calls) means a message that's already gone from the log never gets
    retried forever even if its own deleteMessage call happens to fail
    (harmless either way — Telegram just 400s on an already-deleted or
    too-old message)."""
    pool = await get_pool()
    async with pool.acquire() as conn:
        async with conn.transaction():
            rows = await conn.fetch("SELECT * FROM chat_messages_log ORDER BY id")
            await conn.execute("DELETE FROM chat_messages_log")
            return rows


async def peek_logged_messages(limit: int = 20) -> list[asyncpg.Record]:
    """Diagnostic: newest logged (not-yet-cleared) messages, without
    popping them — for confirming logging is actually happening ahead of
    tonight's real clear_chat_history run."""
    pool = await get_pool()
    return await pool.fetch("SELECT * FROM chat_messages_log ORDER BY id DESC LIMIT $1", limit)


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
