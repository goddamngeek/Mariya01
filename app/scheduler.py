import random
from datetime import datetime, time, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import TIMEZONE, WATER_REMINDER_TEXTS, WATER_REMINDER_WINDOWS
from app.db import (
    claim_reminder,
    close_stale_ezhednevnik_prompts,
    create_ezhednevnik_prompt,
    ensure_water_reminder,
    get_due_reminders,
    get_due_water_reminders,
    get_registered_user_ids,
    has_open_ezhednevnik,
    mark_water_reminder_sent,
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


async def _delete_scheduled_message(chat_id: int, message_id: int) -> None:
    await delete_message(chat_id, message_id)


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
        scheduler.add_job(
            _delete_scheduled_message,
            trigger="date",
            run_date=datetime.now(TIMEZONE) + timedelta(minutes=WATER_REMINDER_DELETE_DELAY_MINUTES),
            args=[row["user_id"], message_id],
            id=f"delete_water_msg_{message_id}",
            replace_existing=True,
        )


TEMP_MESSAGE_DELETE_DELAY_MINUTES = 3


async def send_temporary_message(chat_id: int, text: str, parse_mode: str | None = None) -> None:
    """Send-and-forget message that deletes itself after a few minutes —
    for on-demand dumps (kanban board, /week summary, /links) that would
    otherwise sit in the chat forever cluttering it, same self-cleaning
    idea as water reminders (see _delete_scheduled_message above). Silently
    does nothing if the send itself fails — the caller already logs that."""
    message_id = await send_message_get_id(chat_id, text, parse_mode=parse_mode)
    if message_id is None:
        return
    scheduler.add_job(
        _delete_scheduled_message,
        trigger="date",
        run_date=datetime.now(TIMEZONE) + timedelta(minutes=TEMP_MESSAGE_DELETE_DELAY_MINUTES),
        args=[chat_id, message_id],
        id=f"delete_temp_msg_{message_id}",
        replace_existing=True,
    )


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
    scheduler.start()
    await ensure_today_water_reminders()
