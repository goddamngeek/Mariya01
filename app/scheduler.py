import random
from datetime import datetime, time, timedelta, timezone

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import TIMEZONE, WATER_REMINDER_TEXTS, WATER_REMINDER_WINDOWS
from app.db import (
    claim_reminder,
    close_book_add_prompt,
    close_book_quote_prompt,
    close_stale_ezhednevnik_prompts,
    create_ezhednevnik_prompt,
    ensure_water_reminder,
    get_due_book_add_notices,
    get_due_reminders,
    get_due_water_reminders,
    get_open_activity_prompt,
    get_registered_user_ids,
    get_stale_book_quote_prompts,
    has_open_ezhednevnik,
    mark_book_add_notified,
    mark_water_reminder_sent,
    pop_due_message_deletions,
    pop_logged_messages_except,
    schedule_pending_message_deletion,
)
from app.prompts import ezhednevnik_step_text
from app.reminders import deliver_reminder
from app.telegram import delete_message, send_message, send_message_get_id

scheduler = AsyncIOScheduler(timezone=TIMEZONE)


def _random_time_in_window(start: time, end: time) -> datetime:
    """Used by ensure_today_water_reminders() to re-pick today's random
    slot from scratch — not just at the 00:05 cron tick, but on EVERY
    process restart too (it also runs from lifespan() on every deploy).
    Confirmed live: with the window's lower bound fixed at `start` regardless of the
    current time, a restart landing partway through an already-open window
    could roll a slot that's already in the past — and the caller's
    `run_date <= now: skip` then silently dropped that window's reminder for
    the rest of the day, with no retry at the time (water reminders now
    have their own due_at/release-job safety net — see below).
    Clamping the lower bound to `now` guarantees a fresh restart always rolls
    a still-future slot for any window that hasn't fully elapsed yet."""
    now = datetime.now(TIMEZONE)
    start_dt = now.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)
    end_dt = now.replace(hour=end.hour, minute=end.minute, second=0, microsecond=0)
    lower = max(start_dt, now)
    if lower >= end_dt:
        return lower  # window already fully elapsed — caller's run_date<=now skip still applies
    window_seconds = int((end_dt - lower).total_seconds())
    return lower + timedelta(seconds=random.randint(0, window_seconds))


async def send_ezhednevnik_prompts(slot: str) -> None:
    """Fixed-time daily journal check-in — replaces the old random-pool
    daily question system entirely (removed). Three fixed times,
    each matched to when the relevant period just ended so it's still
    fresh: 'am' ~12:30 (before-lunch feeling), 'pm' ~18:00 (after-lunch
    feeling, right as the work day ends), 'evening' ~21:30 (the full-day
    retrospective — needs the whole day to have actually happened first).
    Each slot is a one-question-at-a-time sequence (EZHEDNEVNIK_STEPS in
    prompts.py) — this only ever sends the FIRST question; every step after
    that is driven entirely by app/service.py as replies come in."""
    text = ezhednevnik_step_text(slot, 0)
    today_start = datetime.now(TIMEZONE).replace(hour=0, minute=0, second=0, microsecond=0)
    for user_id in await get_registered_user_ids():
        closed = await close_stale_ezhednevnik_prompts(user_id, today_start)
        if closed:
            print(f"user={user_id}: auto-closed {closed} stale unanswered ezhednevnik prompt(s)", flush=True)
        if await has_open_ezhednevnik(user_id):
            print(f"user={user_id} already has an open ezhednevnik check-in, skipping {slot}", flush=True)
            continue
        await create_ezhednevnik_prompt(user_id, slot)
        if not await send_message(user_id, text):
            print(f"failed to send ezhednevnik {slot} prompt to user={user_id}", flush=True)


MARKET_REVIEW_TEXT = (
    "Привет, тебе необходимо проанализировать рынок и подготовиться к "
    "неделе, заполнив план-таблицу."
)


async def send_market_review_reminder() -> None:
    """Fixed weekly nudge — Saturday 10:00 MSK, both registered users, no
    reply expected (a plain heads-up, not a check-in flow like
    ежедневник)."""
    for user_id in await get_registered_user_ids():
        if not await send_message(user_id, MARKET_REVIEW_TEXT):
            print(f"failed to send market review reminder to user={user_id}", flush=True)


async def release_due_reminders() -> None:
    for row in await get_due_reminders():
        # claim_reminder() is the atomic gate — if an immediate ("now")
        # delivery in app/sync.py already claimed this row moments ago, we
        # skip it here instead of sending it a second time.
        if not await claim_reminder(row["id"]):
            continue
        await deliver_reminder(
            row["id"], row["sender_chat_id"], row["target_chat_id"], row["message"], row["anonymous"],
        )


async def ensure_today_water_reminders() -> None:
    """DB-backed (water_reminders table), unlike the old in-memory
    APScheduler date-jobs this replaces — ensure_water_reminder() is an
    idempotent INSERT ... ON CONFLICT DO NOTHING, so re-running this on
    every process restart (not just the 00:05 cron tick) is always safe:
    a row already created for today is left completely untouched, so a
    redeploy can no longer reroll or drop an already-scheduled slot the way
    the old in-memory version confirmed-live could. Actual sending happens
    from release_due_water_reminders() below, polled every 60s regardless
    of how many times the process has restarted since this ran."""
    now = datetime.now(TIMEZONE)
    today = now.date()
    for user_id in await get_registered_user_ids():
        for i, (start, end) in enumerate(WATER_REMINDER_WINDOWS):
            due_at = _random_time_in_window(start, end)
            if due_at <= now:
                continue  # window already fully elapsed today — nothing to schedule
            await ensure_water_reminder(user_id, i, today, due_at)


WATER_REMINDER_DELETE_DELAY_MINUTES = 2
TEMP_MESSAGE_DELETE_DELAY_MINUTES = 3


async def schedule_message_deletion(
    chat_id: int, message_id: int, delay_minutes: float = TEMP_MESSAGE_DELETE_DELAY_MINUTES,
) -> None:
    """Delete an already-sent (or already-received) message after a delay.
    Bots can delete their own outgoing messages in any chat, and — in a
    private chat specifically — the other person's incoming messages too
    (Telegram Bot API's own documented behavior, no special rights needed
    there, unlike groups/channels which need admin/can_delete_messages).

    DB-backed (pending_message_deletions table), unlike the in-memory
    APScheduler trigger="date" jobs this replaces — confirmed live: a
    delayed delete scheduled purely in process memory silently vanished
    when a redeploy landed inside the delay window, leaving the message
    stuck in the chat forever with no trace it was ever meant to be
    cleaned up. Actual deletion happens from release_due_message_deletions
    below, polled every 60s regardless of how many times the process has
    restarted since this was scheduled. Used both by send_temporary_message
    below (its own reply), main.py (the person's own triggering message),
    and release_due_water_reminders (its own reminder text)."""
    due_at = datetime.now(TIMEZONE) + timedelta(minutes=delay_minutes)
    await schedule_pending_message_deletion(chat_id, message_id, due_at.astimezone(timezone.utc))


async def release_due_message_deletions() -> None:
    for row in await pop_due_message_deletions():
        await delete_message(row["chat_id"], row["message_id"])


async def release_stale_quote_prompts() -> None:
    """Cleans up a /quote flow (see app/service.py's start_quote_flow) that
    the person walked away from mid-conversation — 5 minutes since the last
    step (get_stale_book_quote_prompts' updated_at cutoff), unlike
    ежедневник/activity which just get silently closed on timeout, this
    actually deletes every message the bot sent as part of that exchange
    (message_ids), per request — a half-answered "Какую книгу?" shouldn't
    just sit in the chat forever."""
    for row in await get_stale_book_quote_prompts():
        for message_id in row["message_ids"]:
            await delete_message(row["user_id"], message_id)
        await close_book_quote_prompt(row["id"])


async def release_due_book_add_notices() -> None:
    """Closes out the "normal dialogue" window for a /addbook flow (see
    app/service.py's start_book_add_flow) — 5 minutes with no answer to the
    "расскажи подробнее" template means the very-next-plain-message
    auto-capture no longer applies (see finalize_book_add_prompt), so this
    both sends a one-time "Добавил книгу" courtesy notice AND closes the
    prompt (is_open=FALSE). template_message_id stays valid regardless —
    get_book_add_prompt_by_template_message ignores is_open entirely — so a
    reply to that same message still works any time after this, per the
    original point of the reply mechanism: answering once the normal
    window has already expired."""
    for row in await get_due_book_add_notices():
        await send_message(row["user_id"], "Добавил книгу")
        await mark_book_add_notified(row["id"])
        await close_book_add_prompt(row["id"])


async def release_due_water_reminders() -> None:
    for row in await get_due_water_reminders():
        text = random.choice(WATER_REMINDER_TEXTS)
        message_id = await send_message_get_id(row["user_id"], text)
        if message_id is None:
            print(f"failed to send water reminder id={row['id']} user={row['user_id']}, will retry", flush=True)
            continue
        await mark_water_reminder_sent(row["id"])
        # Self-cleaning nudge — not worth leaving in the chat forever and
        # cluttering it, per request.
        await schedule_message_deletion(row["user_id"], message_id, WATER_REMINDER_DELETE_DELAY_MINUTES)


async def send_temporary_message(chat_id: int, text: str, parse_mode: str | None = None) -> None:
    """Send-and-forget message that deletes itself after a few minutes —
    for on-demand dumps (kanban board, /week summary, /links) that would
    otherwise sit in the chat forever cluttering it, same self-cleaning
    idea as water reminders. Silently does nothing if the send itself
    fails — the caller already logs that."""
    message_id = await send_message_get_id(chat_id, text, parse_mode=parse_mode)
    if message_id is None:
        return
    await schedule_message_deletion(chat_id, message_id)


async def clear_chat_history(only_chat_id: int | None = None) -> None:
    """Wipe a chat's logged conversation — every message logged since the
    last run (both the bot's own and the person's own; see
    app/telegram.py's _log_sent and app/main.py's webhook handler) gets
    deleted from Telegram. No longer runs on its own schedule (the nightly
    4am cron job was removed per request) — triggered on-demand instead,
    either by the person themselves via /clear (see app/main.py) or for
    testing via app/sync.py's /trigger_chat_clear. Skips a person's chat
    entirely while they have an open ежедневник or activity (yoga/chinese/
    trading) question — wiping mid check-in would delete the bot's own
    pending question along with everything else, losing where things left
    off; that chat's messages just stay logged for a future attempt instead.

    only_chat_id restricts this run to a single chat (everyone else's
    messages stay untouched in the log)."""
    skip_chat_ids = set()
    for user_id in await get_registered_user_ids():
        if only_chat_id is not None and user_id != only_chat_id:
            skip_chat_ids.add(user_id)
            continue
        if await has_open_ezhednevnik(user_id) or await get_open_activity_prompt(user_id) is not None:
            print(f"clear_chat_history: skipping user={user_id}, has an open question", flush=True)
            skip_chat_ids.add(user_id)

    rows = await pop_logged_messages_except(skip_chat_ids)
    for row in rows:
        await delete_message(row["chat_id"], row["message_id"])


async def start_scheduler() -> None:
    scheduler.add_job(
        send_ezhednevnik_prompts,
        trigger=CronTrigger(hour=12, minute=30, timezone=TIMEZONE),
        args=["am"],
        id="ezhednevnik_am",
    )
    scheduler.add_job(
        send_ezhednevnik_prompts,
        trigger=CronTrigger(hour=18, minute=0, timezone=TIMEZONE),
        args=["pm"],
        id="ezhednevnik_pm",
    )
    scheduler.add_job(
        send_ezhednevnik_prompts,
        trigger=CronTrigger(hour=21, minute=30, timezone=TIMEZONE),
        args=["evening"],
        id="ezhednevnik_evening",
    )
    scheduler.add_job(
        ensure_today_water_reminders,
        trigger=CronTrigger(hour=0, minute=5, timezone=TIMEZONE),
        id="schedule_daily_water_reminders",
    )
    scheduler.add_job(
        send_market_review_reminder,
        trigger=CronTrigger(day_of_week="sat", hour=10, minute=0, timezone=TIMEZONE),
        id="market_review_reminder",
    )
    scheduler.add_job(
        release_due_reminders,
        trigger="interval",
        seconds=60,
        id="release_due_reminders",
    )
    scheduler.add_job(
        release_due_water_reminders,
        trigger="interval",
        seconds=60,
        id="release_due_water_reminders",
    )
    scheduler.add_job(
        release_due_message_deletions,
        trigger="interval",
        seconds=60,
        id="release_due_message_deletions",
    )
    scheduler.add_job(
        release_stale_quote_prompts,
        trigger="interval",
        seconds=60,
        id="release_stale_quote_prompts",
    )
    scheduler.add_job(
        release_due_book_add_notices,
        trigger="interval",
        seconds=60,
        id="release_due_book_add_notices",
    )
    scheduler.start()
    await ensure_today_water_reminders()
