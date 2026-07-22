from datetime import datetime, timedelta, timezone

import asyncpg

from app.config import DATABASE_URL, DEFERRAL_DELAY_HOURS, MAX_DEFERRALS

SCHEMA = """
CREATE TABLE IF NOT EXISTS incoming_messages (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    confirmed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS outgoing_messages (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    category TEXT NOT NULL CHECK (category IN ('question', 'reply')),
    text TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    sent_at TIMESTAMPTZ,
    is_open BOOLEAN NOT NULL DEFAULT FALSE,
    deferral_count INTEGER NOT NULL DEFAULT 0,
    due_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS registered_users (
    chat_id BIGINT PRIMARY KEY,
    registered_at TIMESTAMPTZ NOT NULL
);
"""

SEED_QUESTIONS = [
    "как прошёл день?",
    "как ты вообще?",
    "что было интересного сегодня?",
    "как настроение?",
    "чем занят был?",
]

SEED_REPLIES = [
    "принял",
    "услышал тебя",
    "ловлю",
    "окей",
    "понял, разберусь",
]

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


async def seed_defaults(user_ids: set[int]) -> None:
    pool = await get_pool()
    for user_id in user_ids:
        for category, seeds in (("question", SEED_QUESTIONS), ("reply", SEED_REPLIES)):
            count = await pool.fetchval(
                "SELECT COUNT(*) FROM outgoing_messages WHERE user_id = $1 AND category = $2",
                user_id, category,
            )
            if count == 0:
                created_at = utcnow()
                await pool.executemany(
                    "INSERT INTO outgoing_messages "
                    "(user_id, category, text, created_at, is_open, deferral_count) "
                    "VALUES ($1, $2, $3, $4, FALSE, 0)",
                    [(user_id, category, text, created_at) for text in seeds],
                )


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


# --- incoming messages -----------------------------------------------------

async def insert_incoming_message(user_id: int, text: str) -> int:
    pool = await get_pool()
    return await pool.fetchval(
        "INSERT INTO incoming_messages (user_id, text, created_at) VALUES ($1, $2, $3) "
        "RETURNING id",
        user_id, text, utcnow(),
    )


async def pull_unconfirmed_incoming() -> list[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch(
        "SELECT id, user_id, text, created_at FROM incoming_messages "
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


# --- open question / deferral ----------------------------------------------

async def get_open_question(user_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow(
        "SELECT * FROM outgoing_messages "
        "WHERE category = 'question' AND user_id = $1 AND is_open = TRUE "
        "ORDER BY sent_at DESC LIMIT 1",
        user_id,
    )


async def has_pending_question(user_id: int) -> bool:
    """True if the user has a question that is open, or deferred and not
    yet due — i.e. "in flight" and shouldn't be joined by a second one."""
    pool = await get_pool()
    row = await pool.fetchval(
        "SELECT 1 FROM outgoing_messages WHERE category = 'question' AND user_id = $1 "
        "AND (is_open = TRUE OR (due_at IS NOT NULL AND due_at > $2)) LIMIT 1",
        user_id, utcnow(),
    )
    return row is not None


async def get_outgoing_by_id(message_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow("SELECT * FROM outgoing_messages WHERE id = $1", message_id)


async def close_question(question_id: int) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE outgoing_messages SET is_open = FALSE WHERE id = $1", question_id
    )


async def apply_deferral(question_id: int, user_id: int, deferral_count: int) -> None:
    """Defer the open question by a fixed delay. On the (MAX_DEFERRALS + 1)-th
    consecutive deferral, swap in a different question for this user instead
    of repeating the same text, and reset the counter on the new row."""
    due_at = utcnow() + timedelta(hours=DEFERRAL_DELAY_HOURS)
    new_count = deferral_count + 1
    pool = await get_pool()

    if new_count <= MAX_DEFERRALS:
        await pool.execute(
            "UPDATE outgoing_messages SET is_open = FALSE, due_at = $1, deferral_count = $2 "
            "WHERE id = $3",
            due_at, new_count, question_id,
        )
        return

    async with pool.acquire() as conn:
        async with conn.transaction():
            replacement = await conn.fetchrow(
                "SELECT * FROM outgoing_messages WHERE category = 'question' AND user_id = $1 "
                "AND id != $2 ORDER BY RANDOM() LIMIT 1",
                user_id, question_id,
            )

            target_id = replacement["id"] if replacement is not None else question_id
            if replacement is not None:
                await conn.execute(
                    "UPDATE outgoing_messages SET is_open = FALSE, due_at = NULL WHERE id = $1",
                    question_id,
                )
            await conn.execute(
                "UPDATE outgoing_messages SET is_open = FALSE, due_at = $1, deferral_count = 0 "
                "WHERE id = $2",
                due_at, target_id,
            )


async def get_due_deferred_questions() -> list[asyncpg.Record]:
    pool = await get_pool()
    return await pool.fetch(
        "SELECT * FROM outgoing_messages WHERE category = 'question' AND is_open = FALSE "
        "AND due_at IS NOT NULL AND due_at <= $1",
        utcnow(),
    )


async def mark_question_sent(question_id: int) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE outgoing_messages SET sent_at = $1, is_open = TRUE, due_at = NULL WHERE id = $2",
        utcnow(), question_id,
    )


# --- reply -------------------------------------------------------------

async def get_last_reply_sent_at(user_id: int) -> datetime | None:
    pool = await get_pool()
    return await pool.fetchval(
        "SELECT sent_at FROM outgoing_messages WHERE category = 'reply' AND user_id = $1 "
        "AND sent_at IS NOT NULL ORDER BY sent_at DESC LIMIT 1",
        user_id,
    )


async def mark_reply_sent(reply_id: int) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE outgoing_messages SET sent_at = $1 WHERE id = $2", utcnow(), reply_id
    )


# --- shared outgoing selection ------------------------------------------

async def pick_outgoing_message(category: str, dedup_days: int, user_id: int) -> asyncpg.Record | None:
    """Fresh (never sent) message first; otherwise a random one not sent
    within the last `dedup_days` days. Scoped to a single user's pool."""
    pool = await get_pool()

    row = await pool.fetchrow(
        "SELECT * FROM outgoing_messages WHERE category = $1 AND user_id = $2 "
        "AND sent_at IS NULL ORDER BY RANDOM() LIMIT 1",
        category, user_id,
    )
    if row is not None:
        return row

    cutoff = utcnow() - timedelta(days=dedup_days)
    return await pool.fetchrow(
        "SELECT * FROM outgoing_messages WHERE category = $1 AND user_id = $2 "
        "AND sent_at IS NOT NULL AND sent_at < $3 ORDER BY RANDOM() LIMIT 1",
        category, user_id, cutoff,
    )


async def insert_outgoing_messages(items: list[tuple[int, str, str]]) -> None:
    """items: list of (user_id, category, text)"""
    if not items:
        return
    pool = await get_pool()
    created_at = utcnow()
    await pool.executemany(
        "INSERT INTO outgoing_messages "
        "(user_id, category, text, created_at, is_open, deferral_count) "
        "VALUES ($1, $2, $3, $4, FALSE, 0)",
        [(user_id, category, text, created_at) for user_id, category, text in items],
    )
