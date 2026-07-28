import random
from datetime import datetime, time, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import (
    OUTGOING_DEDUP_DAYS,
    PROACTIVE_WINDOW_END,
    PROACTIVE_WINDOW_START,
    TIMEZONE,
    WATER_REMINDER_TEXTS,
    WATER_REMINDER_WINDOWS,
    format_reminder_message,
)
from app.db import (
    claim_reminder,
    get_due_deferred_questions,
    get_due_reminders,
    get_registered_user_ids,
    has_pending_question,
    mark_question_sent,
    pick_outgoing_message,
)
from app.ingest import ingest_incoming
from app.telegram import send_message, send_message_with_button

DEFER_BUTTON_TEXT = "Спросить позже"

scheduler = AsyncIOScheduler(timezone=TIMEZONE)


def _random_time_in_window(start: time, end: time) -> datetime:
    now = datetime.now(TIMEZONE)
    start_dt = now.replace(hour=start.hour, minute=start.minute, second=0, microsecond=0)
    end_dt = now.replace(hour=end.hour, minute=end.minute, second=0, microsecond=0)
    window_seconds = int((end_dt - start_dt).total_seconds())
    return start_dt + timedelta(seconds=random.randint(0, window_seconds))


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


async def release_due_reminders() -> None:
    for row in await get_due_reminders():
        # claim_reminder() is the atomic gate — if an immediate ("now")
        # delivery in app/sync.py already claimed this row moments ago, we
        # skip it here instead of sending it a second time.
        if not await claim_reminder(row["id"]):
            continue
        text = format_reminder_message(row["sender_chat_id"], row["target_chat_id"], row["message"])
        if not await send_message(row["target_chat_id"], text):
            print(f"failed to send reminder id={row['id']}", flush=True)


async def send_water_reminder(user_id: int) -> None:
    text = random.choice(WATER_REMINDER_TEXTS)
    if not await send_message(user_id, text):
        print(f"failed to send water reminder to user={user_id}", flush=True)


async def schedule_today_water_reminders() -> None:
    now = datetime.now(TIMEZONE)
    for user_id in await get_registered_user_ids():
        for i, (start, end) in enumerate(WATER_REMINDER_WINDOWS):
            run_date = _random_time_in_window(start, end)
            if run_date <= now:
                continue
            scheduler.add_job(
                send_water_reminder,
                trigger="date",
                run_date=run_date,
                args=[user_id],
                id=f"water_reminder_{user_id}_{i}",
                replace_existing=True,
            )
            print(
                f"scheduled water reminder #{i} for user={user_id} at {run_date.isoformat()}",
                flush=True,
            )


async def start_scheduler() -> None:
    scheduler.add_job(
        schedule_today_question,
        trigger=CronTrigger(hour=0, minute=5, timezone=TIMEZONE),
        id="schedule_daily_question",
    )
    scheduler.add_job(
        schedule_today_water_reminders,
        trigger=CronTrigger(hour=0, minute=5, timezone=TIMEZONE),
        id="schedule_daily_water_reminders",
    )
    scheduler.add_job(
        release_due_questions,
        trigger="interval",
        seconds=60,
        id="release_due_questions",
    )
    scheduler.add_job(
        release_due_reminders,
        trigger="interval",
        seconds=60,
        id="release_due_reminders",
    )
    scheduler.add_job(
        ingest_incoming,
        trigger="interval",
        seconds=60,
        id="ingest_incoming_to_odysseus",
    )
    scheduler.start()
    await schedule_today_question()
    await schedule_today_water_reminders()
