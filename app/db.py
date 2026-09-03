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
    registered_at TIMESTAMPTZ NOT NULL
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

-- Three fixed check-ins per day (am ~12:30 / pm ~18:00 / evening ~21:30),
-- each a strict one-question-at-a-time sequence (see app/prompts.py's
-- EZHEDNEVNIK_STEPS) — step is the 0-based index into that slot's step
-- list, and equals len(steps) once the whole slot is filled in; collected
-- holds every answer gathered.
--
-- is_open means "this is the slot a plain message answers", nothing more.
-- Each new slot closes the previous one whether or not it was finished,
-- because the old rule — skip the new slot while an old one is unanswered —
-- silently cost whole check-ins: one unanswered pm on 21.08 meant the
-- evening retrospective (six fields, the richest slot of the day) was never
-- asked at all.
--
-- Nothing is lost by closing early, because question_message_ids keeps
-- every question the bot asked for this prompt: replying to any of them
-- (Telegram's native reply) resumes that check-in at whatever step it
-- stopped on, no matter how long ago or whether it's still "open" — see
-- app/service.py's handle_ezhednevnik_question_reply. So a plain message
-- answers the current slot, a reply answers the slot you replied to, and
-- editing a past answer patches the cell it already wrote.
CREATE TABLE IF NOT EXISTS ezhednevnik_prompts (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    slot TEXT NOT NULL CHECK (slot IN ('am', 'pm', 'evening')),
    sent_at TIMESTAMPTZ NOT NULL,
    is_open BOOLEAN NOT NULL DEFAULT TRUE,
    step INTEGER NOT NULL DEFAULT 0,
    collected JSONB NOT NULL DEFAULT '{}'::jsonb,
    question_message_ids INTEGER[] NOT NULL DEFAULT '{}'
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
    updated_at TIMESTAMPTZ NOT NULL,
    is_open BOOLEAN NOT NULL DEFAULT TRUE,
    step INTEGER NOT NULL DEFAULT 0,
    collected JSONB NOT NULL DEFAULT '{}'::jsonb,
    thread_id INTEGER
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
-- step, and the thread this prompt hangs on (thread_id) is what actually
-- clears it: release_stale_message_threads (scheduler.py) deletes the whole
-- exchange and closes the prompt with it 5 minutes after the person goes
-- quiet, rather than leaving a half-answered "Какую книгу?" sitting in the
-- chat forever.
CREATE TABLE IF NOT EXISTS task_add_prompts (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    is_open BOOLEAN NOT NULL DEFAULT TRUE,
    thread_id INTEGER
);

CREATE TABLE IF NOT EXISTS link_add_prompts (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    sent_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    is_open BOOLEAN NOT NULL DEFAULT TRUE,
    thread_id INTEGER
);

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
    thread_id INTEGER
);

-- One interaction, start to finish — the triggering command, every question
-- and answer, and the final confirmation. THE single mechanism for making
-- messages go away (see app/threads.py); it replaced four overlapping ones
-- that each had their own rule for when a conversation disappeared, which
-- together were impossible to predict.
--
-- A thread has exactly one terminal state, "завершена", reachable three
-- ways: the flow succeeded, ttl_minutes elapsed with no activity, or the
-- person reacted to one of its messages. Reaching it ALWAYS wipes every
-- message the thread owns — a reaction doesn't delete anything by itself,
-- it just ends the thread early and the usual teardown follows.
--
-- closing_text, if set, is sent on the timeout path only, as the thread's
-- final act before teardown (see /addbook's "Добавил книгу").
--
-- The one flow with no thread at all is ежедневник: its messages stay until
-- an explicit /clear, so an answer can still be edited after the fact (see
-- handle_message_edit).
CREATE TABLE IF NOT EXISTS message_threads (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    message_ids INTEGER[] NOT NULL DEFAULT '{}',
    updated_at TIMESTAMPTZ NOT NULL,
    is_open BOOLEAN NOT NULL DEFAULT TRUE,
    ttl_minutes INTEGER NOT NULL DEFAULT 5,
    closing_text TEXT
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

-- /addbook flow (title, then author — see app/service.py). Stage 1 collects
-- those two answers and creates the book note; stage 2 is the "расскажи
-- подробнее" template and the answer to it, filling in author/annotation/
-- genre/similar-books.
--
-- The row stays open (is_open=TRUE) once the template goes out, so the very
-- next plain message is captured as the details answer, like any other
-- flow's continuation. template_message_id makes a Telegram reply to the
-- template work as well, and that is not a nicety: the clippings import can
-- add several books at once and sends a template for each (see
-- app/service.py's _offer_book_details), and with more than one prompt open
-- the replied-to message_id is the ONLY thing saying which book is being
-- answered — a plain message can only ever reach the most recently touched
-- one.
--
-- Both paths live exactly as long as the template is still in the chat: the
-- thread owning this exchange (thread_id) tears it down 5 minutes after the
-- person goes quiet, deleting the template and closing this row with it.
-- Replying days later used to work and no longer does — the unification of
-- message teardown onto threads took the template along with everything
-- else, and there is nothing left to reply to.
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
    thread_id INTEGER
);

-- Отпечатки уже разобранных выделений с читалки (см. app/clippings.py).
-- «My Clippings.txt» копится вечно и никогда не чистится, так что файл
-- можно присылать хоть каждый день: новым считается то, чего здесь нет.
CREATE TABLE IF NOT EXISTS imported_clippings (
    fingerprint TEXT PRIMARY KEY,
    book_title TEXT NOT NULL,
    imported_at TIMESTAMPTZ NOT NULL
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

-- Trilium hands out stable noteIds that never change, but the bot has
-- always looked notes up by their TITLE — which is user-editable in the
-- UI, so renaming "КНИГИ" would silently break every book operation with
-- nothing to point at. (Already bitten once: one real note's title turned
-- out to have a double space in it.) Resolving a title to its noteId once
-- and remembering it makes a later rename a non-event.
CREATE TABLE IF NOT EXISTS trilium_note_ids (
    lookup_key TEXT PRIMARY KEY,
    note_id TEXT NOT NULL,
    resolved_at TIMESTAMPTZ NOT NULL
);

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
ALTER TABLE ezhednevnik_prompts ADD COLUMN IF NOT EXISTS question_message_ids INTEGER[] NOT NULL DEFAULT '{}';
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
-- Unifying every flow onto message_threads (see app/threads.py): each
-- prompt table now just points at the thread that owns its messages,
-- instead of keeping a parallel list of its own.
ALTER TABLE message_threads ADD COLUMN IF NOT EXISTS ttl_minutes INTEGER NOT NULL DEFAULT 5;
ALTER TABLE message_threads ADD COLUMN IF NOT EXISTS closing_text TEXT;
ALTER TABLE activity_prompts ADD COLUMN IF NOT EXISTS thread_id INTEGER;
-- Was timing out from sent_at, i.e. from when the dialogue STARTED, so a
-- slow answer could be judged stale while the person was still typing it —
-- every other flow (and the thread's own TTL) measures from the last step.
ALTER TABLE activity_prompts ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ;
UPDATE activity_prompts SET updated_at = sent_at WHERE updated_at IS NULL;
ALTER TABLE activity_prompts ALTER COLUMN updated_at SET NOT NULL;
ALTER TABLE book_quote_prompts ADD COLUMN IF NOT EXISTS thread_id INTEGER;
ALTER TABLE book_add_prompts ADD COLUMN IF NOT EXISTS thread_id INTEGER;
ALTER TABLE book_quote_prompts DROP COLUMN IF EXISTS message_ids;
ALTER TABLE book_add_prompts DROP COLUMN IF EXISTS message_ids;
ALTER TABLE book_add_prompts DROP COLUMN IF EXISTS details_notified;
-- Only ever written, never read: it dated the template for a courtesy
-- message the thread's own closing_text replaced long ago.
ALTER TABLE book_add_prompts DROP COLUMN IF EXISTS template_sent_at;
DROP TABLE IF EXISTS pending_message_deletions;

-- Indexes for the lookups that run per incoming message or per background
-- tick. With two people the tables are tiny and a sequential scan is free,
-- but incoming_messages is the one that only ever grows, and it's scanned
-- on every message edit.
CREATE INDEX IF NOT EXISTS incoming_messages_unconfirmed_idx
    ON incoming_messages (id) WHERE confirmed_at IS NULL;
CREATE INDEX IF NOT EXISTS incoming_messages_telegram_idx
    ON incoming_messages (user_id, telegram_message_id);
CREATE INDEX IF NOT EXISTS ezhednevnik_prompts_open_idx
    ON ezhednevnik_prompts (user_id) WHERE is_open;
CREATE INDEX IF NOT EXISTS ezhednevnik_prompts_questions_idx
    ON ezhednevnik_prompts USING GIN (question_message_ids);
CREATE INDEX IF NOT EXISTS activity_prompts_open_idx ON activity_prompts (user_id) WHERE is_open;
CREATE INDEX IF NOT EXISTS book_quote_prompts_open_idx ON book_quote_prompts (user_id) WHERE is_open;
CREATE INDEX IF NOT EXISTS book_review_prompts_open_idx ON book_review_prompts (user_id) WHERE is_open;
CREATE INDEX IF NOT EXISTS book_add_prompts_open_idx ON book_add_prompts (user_id) WHERE is_open;
CREATE INDEX IF NOT EXISTS book_add_prompts_template_idx ON book_add_prompts (template_message_id);
CREATE INDEX IF NOT EXISTS message_threads_open_idx ON message_threads (user_id) WHERE is_open;
CREATE INDEX IF NOT EXISTS message_threads_messages_idx ON message_threads USING GIN (message_ids);
CREATE INDEX IF NOT EXISTS chat_messages_log_chat_idx ON chat_messages_log (chat_id);
-- Flashcard feature removed entirely (unused, per explicit confirmation
-- there was nothing worth keeping in it) — drops the tables outright
-- rather than leaving them as dead weight nothing references anymore.
DROP TABLE IF EXISTS cards;
DROP TABLE IF EXISTS card_sessions;
DROP TABLE IF EXISTS card_reminders;
-- Old random-pool daily question/reply system removed entirely — replaced
-- by the fixed-time ежедневник check-ins (see ezhednevnik_prompts above).
DROP TABLE IF EXISTS outgoing_messages;
-- Odysseus убран целиком: свободный разговор им не пользовались,
-- а всё остальное бот давно делает сам. Сессии хранить не для кого.
ALTER TABLE registered_users DROP COLUMN IF EXISTS odysseus_session_id;
-- Счёт, с которого человек платил в прошлый раз. Обычная трата уходит с
-- него молча — выбор появляется только когда он реально нужен.
ALTER TABLE registered_users ADD COLUMN IF NOT EXISTS firefly_account_id TEXT;

-- Одна трата, записанная строкой. Живёт ради двух вещей: кнопки «другой
-- счёт» (нужен id уже созданной транзакции, чтобы её поправить) и первого
-- раза, когда счёт по умолчанию ещё не выбран и трату надо где-то
-- подержать, пока человек его назовёт.
-- Снимок инбокса на момент /inbox. Раньше «следующая карточка» считалась
-- каждый раз из свежепрочитанной доски — то есть каждое нажатие кнопки
-- стоило чтения всей доски целиком (полтора десятка запросов к Trilium), а
-- порядок мог съехать между нажатиями, если второй человек разбирал свой
-- инбокс параллельно. Снимок замораживает и то и другое: разбор — это срез
-- момента, новое приедет следующим заходом.
CREATE TABLE IF NOT EXISTS inbox_sessions (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    cards JSONB NOT NULL,
    handled TEXT[] NOT NULL DEFAULT '{}',
    thread_id INTEGER,
    created_at TIMESTAMPTZ NOT NULL
);

-- Трата, собираемая по шагам: сумма из сообщения, дальше «на что», счёт,
-- получатель и категория. Из строки надёжно достаётся только число —
-- остальное зависит от формулировки, поэтому спрашивается, а не угадывается.
-- На каждом шаге кнопки с тем, что уже заведено, и можно написать своё.
CREATE TABLE IF NOT EXISTS expense_prompts (
    id SERIAL PRIMARY KEY,
    user_id BIGINT NOT NULL,
    amount TEXT NOT NULL,
    description TEXT NOT NULL DEFAULT '',
    external_id TEXT,
    transaction_id TEXT,
    step INTEGER NOT NULL DEFAULT 0,
    is_open BOOLEAN NOT NULL DEFAULT TRUE,
    account_id TEXT,
    destination TEXT,
    category TEXT,
    collected JSONB NOT NULL DEFAULT '{}'::jsonb,
    thread_id INTEGER,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at TIMESTAMPTZ NOT NULL
);
ALTER TABLE expense_prompts ALTER COLUMN description SET DEFAULT '';
ALTER TABLE expense_prompts ADD COLUMN IF NOT EXISTS step INTEGER NOT NULL DEFAULT 0;
ALTER TABLE expense_prompts ADD COLUMN IF NOT EXISTS is_open BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE expense_prompts ADD COLUMN IF NOT EXISTS account_id TEXT;
ALTER TABLE expense_prompts ADD COLUMN IF NOT EXISTS destination TEXT;
ALTER TABLE expense_prompts ADD COLUMN IF NOT EXISTS category TEXT;
ALTER TABLE expense_prompts ADD COLUMN IF NOT EXISTS collected JSONB NOT NULL DEFAULT '{}'::jsonb;
ALTER TABLE expense_prompts ADD COLUMN IF NOT EXISTS thread_id INTEGER;
ALTER TABLE expense_prompts ADD COLUMN IF NOT EXISTS updated_at TIMESTAMPTZ NOT NULL DEFAULT now();
-- Строки, заведённые до пошагового диалога: у них описание уже заполнено, а
-- шаг нулевой, потому что колонки тогда не было. is_open по умолчанию TRUE
-- сделал бы их «открытым вопросом», и первое же сообщение человека уехало
-- бы в них ответом. Закрываем — писать в Firefly им всё равно нечем.
UPDATE expense_prompts SET is_open = FALSE WHERE step = 0 AND description <> '';
CREATE INDEX IF NOT EXISTS expense_prompts_open_idx ON expense_prompts (user_id) WHERE is_open;
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


async def close_open_ezhednevnik_prompts(user_id: int) -> int:
    """Called by each slot tick before it sends: whatever was open stops
    being "the slot a plain message answers", so the new slot can take over.
    Age doesn't matter — an unanswered pm from three hours ago blocks the
    evening retrospective just as badly as one from last week did.

    The closed check-in stays resumable: see question_message_ids and
    handle_ezhednevnik_question_reply. Returns how many were closed."""
    pool = await get_pool()
    result = await pool.execute(
        "UPDATE ezhednevnik_prompts SET is_open = FALSE WHERE user_id = $1 AND is_open = TRUE",
        user_id,
    )
    return int(result.split()[-1])


async def append_ezhednevnik_question(prompt_id: int, message_id: int) -> None:
    """Remember a question the bot just asked, so a reply to it can be
    traced back to this check-in later."""
    pool = await get_pool()
    await pool.execute(
        "UPDATE ezhednevnik_prompts SET question_message_ids = array_append(question_message_ids, $1) "
        "WHERE id = $2",
        message_id, prompt_id,
    )


async def get_ezhednevnik_prompt_by_question_message(
    user_id: int, message_id: int,
) -> asyncpg.Record | None:
    """Deliberately ignores is_open — resuming a closed check-in by replying
    to one of its questions is the whole point."""
    pool = await get_pool()
    return await pool.fetchrow(
        "SELECT * FROM ezhednevnik_prompts WHERE user_id = $1 AND $2 = ANY(question_message_ids)",
        user_id, message_id,
    )


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


async def create_activity_prompt(user_id: int, activity: str, thread_id: int | None = None) -> int:
    pool = await get_pool()
    now = utcnow()
    return await pool.fetchval(
        "INSERT INTO activity_prompts (user_id, activity, sent_at, updated_at, is_open, thread_id) "
        "VALUES ($1, $2, $3, $3, TRUE, $4) RETURNING id",
        user_id, activity, now, thread_id,
    )


async def advance_activity_prompt_step(prompt_id: int, step: int, collected: dict) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE activity_prompts SET step = $1, collected = $2::jsonb, updated_at = $3 WHERE id = $4",
        step, json.dumps(collected), utcnow(), prompt_id,
    )


async def close_activity_prompt(prompt_id: int) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE activity_prompts SET is_open = FALSE WHERE id = $1", prompt_id)


# --- book quotes ("interesting moments") -------------------------------------

async def get_book_quote_prompt(prompt_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow("SELECT * FROM book_quote_prompts WHERE id = $1", prompt_id)


async def create_book_quote_prompt(
    user_id: int, candidates: list[dict], thread_id: int | None = None,
) -> int:
    pool = await get_pool()
    now = utcnow()
    return await pool.fetchval(
        "INSERT INTO book_quote_prompts (user_id, sent_at, updated_at, is_open, collected, thread_id) "
        "VALUES ($1, $2, $2, TRUE, $3::jsonb, $4) RETURNING id",
        user_id, now, json.dumps({"candidates": candidates}), thread_id,
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


async def close_book_quote_prompt(prompt_id: int) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE book_quote_prompts SET is_open = FALSE WHERE id = $1", prompt_id)


# --- /addbook -----------------------------------------------------------

async def create_book_add_prompt(user_id: int, thread_id: int | None = None) -> int:
    pool = await get_pool()
    now = utcnow()
    return await pool.fetchval(
        "INSERT INTO book_add_prompts (user_id, sent_at, updated_at, is_open, thread_id) "
        "VALUES ($1, $2, $2, TRUE, $3) RETURNING id",
        user_id, now, thread_id,
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
    continuation (see app/service.py's process_incoming_message).

    That window, and the reply-to-the-template one alongside it, both close
    5 minutes after the last activity, when release_stale_message_threads
    (scheduler.py) tears the thread down: the template is deleted and
    close_prompts_for_thread closes this row."""
    pool = await get_pool()
    now = utcnow()
    await pool.execute(
        "UPDATE book_add_prompts SET step = 2, author = $1, book_note_id = $2, "
        "template_message_id = $3, updated_at = $4 WHERE id = $5",
        author, book_note_id, template_message_id, now, prompt_id,
    )


async def get_book_add_prompt_by_template_message(user_id: int, template_message_id: int) -> asyncpg.Record | None:
    """Deliberately ignores is_open, which still earns its keep even though
    the row no longer outlives its thread: a details write that failed closes
    this row without dismissing the thread, so the template is still sitting
    in the chat and replying to it again retries the write."""
    pool = await get_pool()
    return await pool.fetchrow(
        "SELECT * FROM book_add_prompts WHERE user_id = $1 AND template_message_id = $2",
        user_id, template_message_id,
    )


# --- book reviews ("я дочитал") ---------------------------------------------

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


# --- выделения с читалки ------------------------------------------------

async def filter_new_clippings(fingerprints: list[str]) -> set[str]:
    """Какие из этих отпечатков ещё не импортированы."""
    if not fingerprints:
        return set()
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT fingerprint FROM imported_clippings WHERE fingerprint = ANY($1::text[])",
        fingerprints,
    )
    seen = {r["fingerprint"] for r in rows}
    return {f for f in fingerprints if f not in seen}


async def mark_clippings_imported(items: list[tuple[str, str]]) -> None:
    """items — пары (отпечаток, название книги)."""
    if not items:
        return
    pool = await get_pool()
    now = utcnow()
    await pool.executemany(
        "INSERT INTO imported_clippings (fingerprint, book_title, imported_at) "
        "VALUES ($1, $2, $3) ON CONFLICT (fingerprint) DO NOTHING",
        [(fp, title, now) for fp, title in items],
    )


# --- Trilium note id cache --------------------------------------------------

async def get_cached_note_id(lookup_key: str) -> str | None:
    pool = await get_pool()
    return await pool.fetchval(
        "SELECT note_id FROM trilium_note_ids WHERE lookup_key = $1", lookup_key
    )


async def cache_note_id(lookup_key: str, note_id: str) -> None:
    pool = await get_pool()
    await pool.execute(
        "INSERT INTO trilium_note_ids (lookup_key, note_id, resolved_at) VALUES ($1, $2, $3) "
        "ON CONFLICT (lookup_key) DO UPDATE SET note_id = $2, resolved_at = $3",
        lookup_key, note_id, utcnow(),
    )


async def forget_note_id(lookup_key: str) -> None:
    """Drop a mapping whose note no longer exists, so the next lookup falls
    back to searching by title again."""
    pool = await get_pool()
    await pool.execute("DELETE FROM trilium_note_ids WHERE lookup_key = $1", lookup_key)


# --- open prompts (one lookup across every dialogue kind) -------------------

# The five one-question-at-a-time dialogues all answer the same question on
# every incoming message — "is this a reply to something I asked?" — and at
# most one of them can be open at a time. Asking each table separately meant
# five round trips per message to learn there was nothing open at all, which
# is the common case.
_PROMPT_TABLES = {
    "ezhednevnik": "ezhednevnik_prompts",
    "activity": "activity_prompts",
    "quote": "book_quote_prompts",
    "review": "book_review_prompts",
    "book_add": "book_add_prompts",
    "link_add": "link_add_prompts",
    "task_add": "task_add_prompts",
}

# ежедневник outranks everything (priority 0): it's the one scheduled
# check-in, and a command can open another dialogue on top of it without
# ever passing through the reply routing. Among the rest, most recently
# active wins — if you started something a moment ago, your next message
# is answering THAT, not whatever was left hanging earlier.
_OPEN_PROMPT_QUERY = """
SELECT 'ezhednevnik' AS kind, id, sent_at AS updated_at, 0 AS priority
    FROM ezhednevnik_prompts WHERE user_id = $1 AND is_open
UNION ALL
SELECT 'activity', id, updated_at, 1 FROM activity_prompts WHERE user_id = $1 AND is_open
UNION ALL
SELECT 'quote', id, updated_at, 1 FROM book_quote_prompts WHERE user_id = $1 AND is_open
UNION ALL
SELECT 'review', id, updated_at, 1 FROM book_review_prompts WHERE user_id = $1 AND is_open
UNION ALL
SELECT 'book_add', id, updated_at, 1 FROM book_add_prompts WHERE user_id = $1 AND is_open
UNION ALL
SELECT 'expense', id, updated_at, 1 FROM expense_prompts WHERE user_id = $1 AND is_open
UNION ALL
SELECT 'link_add', id, updated_at, 1 FROM link_add_prompts WHERE user_id = $1 AND is_open
UNION ALL
SELECT 'task_add', id, updated_at, 1 FROM task_add_prompts WHERE user_id = $1 AND is_open
ORDER BY priority, updated_at DESC
LIMIT 1
"""


async def open_task_add_prompt(user_id: int, thread_id: int | None) -> int:
    """Ждём следующим сообщением текст задачи."""
    pool = await get_pool()
    now = utcnow()
    return await pool.fetchval(
        "INSERT INTO task_add_prompts (user_id, sent_at, updated_at, is_open, thread_id) "
        "VALUES ($1, $2, $2, TRUE, $3) RETURNING id",
        user_id, now, thread_id,
    )


async def open_link_add_prompt(user_id: int, thread_id: int | None) -> int:
    """Ждём следующим сообщением название и ссылку. Отдельная таблица, а не
    просто ветка: ветка решает, когда сообщения убрать, а не кому
    адресован следующий ответ."""
    pool = await get_pool()
    now = utcnow()
    return await pool.fetchval(
        "INSERT INTO link_add_prompts (user_id, sent_at, updated_at, is_open, thread_id) "
        "VALUES ($1, $2, $2, TRUE, $3) RETURNING id",
        user_id, now, thread_id,
    )


async def get_open_prompt(user_id: int) -> asyncpg.Record | None:
    """Which dialogue, if any, this person's next message is answering —
    returns just kind/id/updated_at; the caller fetches the full row with
    get_prompt() only once it knows there is one to handle."""
    pool = await get_pool()
    return await pool.fetchrow(_OPEN_PROMPT_QUERY, user_id)


async def get_prompt(kind: str, prompt_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow(f"SELECT * FROM {_PROMPT_TABLES[kind]} WHERE id = $1", prompt_id)


async def close_prompt(kind: str, prompt_id: int) -> None:
    pool = await get_pool()
    await pool.execute(f"UPDATE {_PROMPT_TABLES[kind]} SET is_open = FALSE WHERE id = $1", prompt_id)


# --- message threads (/reading & /finished conversations) --------------------

async def create_message_thread(
    user_id: int, ttl_minutes: int = 5, closing_text: str | None = None,
) -> int:
    pool = await get_pool()
    return await pool.fetchval(
        "INSERT INTO message_threads (user_id, updated_at, is_open, ttl_minutes, closing_text) "
        "VALUES ($1, $2, TRUE, $3, $4) RETURNING id",
        user_id, utcnow(), ttl_minutes, closing_text,
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
    """Per-row ttl_minutes, not one global cutoff — a water reminder is
    stale after 2 minutes while a half-answered dialogue gets 5."""
    pool = await get_pool()
    return await pool.fetch(
        "SELECT * FROM message_threads WHERE is_open = TRUE "
        "AND updated_at < now() - (ttl_minutes * interval '1 minute')"
    )


_THREADED_PROMPT_TABLES = (
    "activity_prompts", "book_quote_prompts", "book_add_prompts", "book_review_prompts",
    "link_add_prompts", "task_add_prompts",
)


async def close_prompts_for_thread(thread_id: int) -> None:
    """A thread going away takes any half-finished dialogue with it — the
    questions are among the messages just deleted, so leaving a prompt open
    would silently swallow the person's next unrelated message as if it
    were an answer."""
    pool = await get_pool()
    for table in _THREADED_PROMPT_TABLES:
        await pool.execute(
            f"UPDATE {table} SET is_open = FALSE WHERE thread_id = $1 AND is_open = TRUE",
            thread_id,
        )


# --- chat message log (nightly cleanup) -------------------------------------

# Telegram refuses deleteMessage for anything sent more than 48 hours ago,
# so a log row older than that can never be acted on — it would only ever
# produce a failed delete on the next /clear. Trimmed with a day of slack.
CHAT_LOG_RETENTION_HOURS = 72


async def trim_chat_message_log() -> int:
    pool = await get_pool()
    result = await pool.execute(
        "DELETE FROM chat_messages_log WHERE created_at < $1",
        utcnow() - timedelta(hours=CHAT_LOG_RETENTION_HOURS),
    )
    return int(result.split()[-1])


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


# --- траты (Firefly) --------------------------------------------------------

async def get_firefly_account(user_id: int) -> str | None:
    pool = await get_pool()
    return await pool.fetchval(
        "SELECT firefly_account_id FROM registered_users WHERE chat_id = $1", user_id
    )


async def set_firefly_account(user_id: int, account_id: str) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE registered_users SET firefly_account_id = $1 WHERE chat_id = $2",
        account_id, user_id,
    )


async def create_expense_prompt(
    user_id: int, amount: str, external_id: str | None, thread_id: int | None = None,
) -> int:
    pool = await get_pool()
    now = utcnow()
    return await pool.fetchval(
        "INSERT INTO expense_prompts (user_id, amount, external_id, thread_id, created_at, updated_at) "
        "VALUES ($1, $2, $3, $4, $5, $5) RETURNING id",
        user_id, amount, external_id, thread_id, now,
    )


async def advance_expense_prompt(prompt_id: int, step: int, **fields) -> None:
    """Шаг вперёд плюс любое из собранных полей — по одному на шаг."""
    sets = ", ".join(f"{k} = ${i + 3}" for i, k in enumerate(fields))
    pool = await get_pool()
    await pool.execute(
        f"UPDATE expense_prompts SET step = $2, updated_at = now()"
        f"{', ' + sets if sets else ''} WHERE id = $1",
        prompt_id, step, *fields.values(),
    )


async def set_expense_candidates(prompt_id: int, candidates: list) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE expense_prompts SET collected = $1::jsonb, updated_at = now() WHERE id = $2",
        json.dumps({"candidates": candidates}), prompt_id,
    )


async def close_expense_prompt(prompt_id: int) -> None:
    pool = await get_pool()
    await pool.execute("UPDATE expense_prompts SET is_open = FALSE WHERE id = $1", prompt_id)


async def get_expense_prompt(prompt_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow("SELECT * FROM expense_prompts WHERE id = $1", prompt_id)


async def set_expense_transaction(prompt_id: int, transaction_id: str) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE expense_prompts SET transaction_id = $1 WHERE id = $2", transaction_id, prompt_id
    )


# --- разбор инбокса ---------------------------------------------------------

async def create_inbox_session(user_id: int, cards: list[dict], thread_id: int | None) -> int:
    pool = await get_pool()
    return await pool.fetchval(
        "INSERT INTO inbox_sessions (user_id, cards, thread_id, created_at) "
        "VALUES ($1, $2::jsonb, $3, $4) RETURNING id",
        user_id, json.dumps(cards), thread_id, utcnow(),
    )


async def get_inbox_session(session_id: int) -> asyncpg.Record | None:
    pool = await get_pool()
    return await pool.fetchrow("SELECT * FROM inbox_sessions WHERE id = $1", session_id)


async def mark_inbox_handled(session_id: int, note_id: str) -> None:
    pool = await get_pool()
    await pool.execute(
        "UPDATE inbox_sessions SET handled = array_append(handled, $1) WHERE id = $2",
        note_id, session_id,
    )


async def touch_message_thread(thread_id: int | None) -> None:
    """Продлить ветку, ничего в неё не добавляя.

    Разбор инбокса правит одно и то же сообщение через editMessageText, а не
    шлёт новые, поэтому append_thread_message ему не подходит — а без
    продления ветка считается заброшенной и уборщик сносит сообщение прямо
    посреди разбора, через пять минут после /inbox."""
    if thread_id is None:
        return
    pool = await get_pool()
    await pool.execute(
        "UPDATE message_threads SET updated_at = $1 WHERE id = $2", utcnow(), thread_id
    )
