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
    create_card_reminder,
    get_due_card_reminders,
    get_due_cards,
    get_due_deferred_questions,
    get_due_reminders,
    get_idle_card_sessions,
    get_registered_user_ids,
    has_pending_card_reminder,
    has_pending_question,
    mark_question_sent,
    pick_outgoing_message,
    release_card_reminder,
)
from app.flashcard_session import close_idle_session
from app.ingest import ingest_incoming
from app.reminders import deliver_reminder
from app.telegram import send_message, send_message_with_button, send_message_with_buttons

CARD_SESSION_IDLE_MINUTES = 10
CARD_REMINDER_TEXT = "Готовы карточки на повторение — сейчас или попозже?"

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
    await schedule_today_question()
    await schedule_today_water_reminders()
    await schedule_today_card_reminder()
