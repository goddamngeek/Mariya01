import random
from datetime import datetime, time, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import (
    CARD_REMINDER_WINDOW_END,
    CARD_REMINDER_WINDOW_START,
    DEFERRAL_DELAY_HOURS,
    OUTGOING_DEDUP_DAYS,
    PROACTIVE_WINDOW_END,
    PROACTIVE_WINDOW_START,
    TIMEZONE,
    WATER_REMINDER_TEXTS,
    WATER_REMINDER_WINDOWS,
)
from app.db import (
    claim_reminder,
    close_stale_ezhednevnik_prompts,
    create_card_reminder,
    create_ezhednevnik_prompt,
    ensure_water_reminder,
    get_due_card_reminders,
    get_due_cards,
    get_due_deferred_questions,
    get_due_reminders,
    get_due_water_reminders,
    get_idle_card_sessions,
    get_registered_user_ids,
    has_open_ezhednevnik,
    has_pending_card_reminder,
    has_pending_question,
    mark_question_sent,
    mark_water_reminder_sent,
    pick_outgoing_message,
    release_card_reminder,
)
from app.flashcard_session import close_idle_session
from app.ingest import ingest_incoming
from app.prompts import EZHEDNEVNIK_AM_POOL, EZHEDNEVNIK_QUESTION_TEXT
from app.reminders import deliver_reminder
from app.telegram import send_message, send_message_with_button, send_message_with_buttons

CARD_SESSION_IDLE_MINUTES = 10
CARD_REMINDER_TEXT = "Готовы карточки на повторение — сейчас или попозже?"

DEFER_BUTTON_TEXT = "Спросить позже"

scheduler = AsyncIOScheduler(timezone=TIMEZONE)


def _random_time_in_window(start: time, end: time) -> datetime:
    """Every schedule_today_*() call re-picks today's random slot from
    scratch — not just at the 00:05 cron tick, but on EVERY process restart
    too (schedule_today_* runs from lifespan() on every deploy). Confirmed
    live: with the window's lower bound fixed at `start` regardless of the
    current time, a restart landing partway through an already-open window
    could roll a slot that's already in the past — and the caller's
    `run_date <= now: skip` then silently dropped that window's reminder for
    the rest of the day, with no retry (water reminders have no due_at/
    release-job safety net at all, unlike card reminders and questions).
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


def _random_time_today() -> datetime:
    return _random_time_in_window(PROACTIVE_WINDOW_START, PROACTIVE_WINDOW_END)


async def _send_question(user_id: int, question) -> None:
    if await send_message_with_button(
        user_id, question["text"], DEFER_BUTTON_TEXT, str(question["id"])
    ):
        await mark_question_sent(question["id"])
    else:
        print(f"failed to send question id={question['id']} user={user_id}, will retry", flush=True)


async def send_daily_question(user_id: int) -> None:
    if await has_pending_question(user_id):
        print(
            f"user={user_id} already has a question in flight (open or deferred), skipping today",
            flush=True,
        )
        return

    question = await pick_outgoing_message("question", OUTGOING_DEDUP_DAYS, user_id)
    if question is None:
        print(f"no question available for user={user_id} today", flush=True)
        return

    await _send_question(user_id, question)


async def schedule_today_question() -> None:
    now = datetime.now(TIMEZONE)
    for user_id in await get_registered_user_ids():
        run_date = _random_time_today()
        if run_date <= now:
            continue
        scheduler.add_job(
            send_daily_question,
            trigger="date",
            run_date=run_date,
            args=[user_id],
            id=f"daily_question_today_{user_id}",
            replace_existing=True,
        )
        print(f"scheduled today's question for user={user_id} at {run_date.isoformat()}", flush=True)


async def release_due_questions() -> None:
    for row in await get_due_deferred_questions():
        await _send_question(row["user_id"], row)


async def send_ezhednevnik_prompts(slot: str) -> None:
    """Fixed-time daily journal check-in — replaces the old random-pool
    daily question above (schedule_today_question/send_daily_question are
    no longer scheduled in start_scheduler(), left in place only in case
    a question was already in flight at deploy time). Three fixed times,
    each matched to when the relevant period just ended so it's still
    fresh: 'am' ~12:30 (before-lunch feeling — a random casual question
    from EZHEDNEVNIK_AM_POOL; the score follow-up is asked separately by
    app/service.py once this one's answered, not here), 'pm' ~18:00
    (after-lunch feeling, right as the work day ends), 'evening' ~21:30
    (the full-day retrospective — events/WDIS/WDIL/mistakes, needs the
    whole day to have actually happened first)."""
    text = random.choice(EZHEDNEVNIK_AM_POOL) if slot == "am" else EZHEDNEVNIK_QUESTION_TEXT[slot]
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


async def _send_card_reminder(user_id: int, reminder_id: int) -> None:
    message_id = await send_message_with_buttons(
        user_id, CARD_REMINDER_TEXT, [("Сейчас", f"cardrem:{reminder_id}:now"), ("Позже", f"cardrem:{reminder_id}:later")]
    )
    if message_id is None:
        print(f"failed to send card reminder id={reminder_id} user={user_id}, will retry", flush=True)


async def send_daily_card_reminder(user_id: int) -> None:
    if await has_pending_card_reminder(user_id):
        print(f"user={user_id} already has a card reminder in flight, skipping today", flush=True)
        return
    # Confirmed live: a user with zero flashcards still got prompted
    # ("Готовы карточки на повторение?") and reasonably tried to engage with
    # an offer that didn't actually apply to them. Mirrors send_daily_question
    # skipping when pick_outgoing_message finds nothing to send.
    if not await get_due_cards(user_id):
        print(f"no due cards for user={user_id} today, skipping card reminder", flush=True)
        return
    reminder_id = await create_card_reminder(user_id)
    await _send_card_reminder(user_id, reminder_id)


async def schedule_today_card_reminder() -> None:
    now = datetime.now(TIMEZONE)
    for user_id in await get_registered_user_ids():
        run_date = _random_time_in_window(CARD_REMINDER_WINDOW_START, CARD_REMINDER_WINDOW_END)
        if run_date <= now:
            continue
        scheduler.add_job(
            send_daily_card_reminder,
            trigger="date",
            run_date=run_date,
            args=[user_id],
            id=f"card_reminder_today_{user_id}",
            replace_existing=True,
        )
        print(f"scheduled today's card reminder for user={user_id} at {run_date.isoformat()}", flush=True)


async def release_due_card_reminders() -> None:
    for row in await get_due_card_reminders():
        if not await get_due_cards(row["user_id"]):
            continue
        await release_card_reminder(row["id"])
        await _send_card_reminder(row["user_id"], row["id"])


async def close_idle_card_sessions() -> None:
    for session in await get_idle_card_sessions(CARD_SESSION_IDLE_MINUTES):
        await close_idle_session(session)


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


async def release_due_water_reminders() -> None:
    for row in await get_due_water_reminders():
        text = random.choice(WATER_REMINDER_TEXTS)
        if await send_message(row["user_id"], text):
            await mark_water_reminder_sent(row["id"])
        else:
            print(f"failed to send water reminder id={row['id']} user={row['user_id']}, will retry", flush=True)


async def start_scheduler() -> None:
    # schedule_today_question / send_daily_question above are no longer
    # scheduled here — replaced by the fixed-time ezhednevnik check-ins
    # below, per explicit choice over keeping the old random daily question.
    # release_due_questions stays registered (harmless) purely to still
    # release any question that was already deferred at the moment this
    # shipped; nothing new will ever create one going forward.
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
        schedule_today_card_reminder,
        trigger=CronTrigger(hour=0, minute=5, timezone=TIMEZONE),
        id="schedule_daily_card_reminder",
    )
    scheduler.add_job(
        release_due_questions,
        trigger="interval",
        seconds=60,
        id="release_due_questions",
    )
    scheduler.add_job(
        release_due_card_reminders,
        trigger="interval",
        seconds=60,
        id="release_due_card_reminders",
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
        ingest_incoming,
        trigger="interval",
        seconds=60,
        id="ingest_incoming_to_odysseus",
    )
    scheduler.add_job(
        close_idle_card_sessions,
        trigger="interval",
        seconds=60,
        id="close_idle_card_sessions",
    )
    scheduler.start()
    await ensure_today_water_reminders()
    await schedule_today_card_reminder()
