import logging
import random
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import (
    OUTGOING_DEDUP_DAYS,
    PROACTIVE_WINDOW_END,
    PROACTIVE_WINDOW_START,
    TIMEZONE,
)
from app.db import (
    get_due_deferred_questions,
    get_registered_user_ids,
    has_pending_question,
    mark_question_sent,
    pick_outgoing_message,
)
from app.telegram import send_message_with_button

logger = logging.getLogger("queue_bot.scheduler")

DEFER_BUTTON_TEXT = "Спросить позже"

scheduler = AsyncIOScheduler(timezone=TIMEZONE)


def _random_time_today() -> datetime:
    now = datetime.now(TIMEZONE)
    start = now.replace(
        hour=PROACTIVE_WINDOW_START.hour,
        minute=PROACTIVE_WINDOW_START.minute,
        second=0,
        microsecond=0,
    )
    end = now.replace(
        hour=PROACTIVE_WINDOW_END.hour,
        minute=PROACTIVE_WINDOW_END.minute,
        second=0,
        microsecond=0,
    )
    window_seconds = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, window_seconds))


async def _send_question(user_id: int, question) -> None:
    if await send_message_with_button(
        user_id, question["text"], DEFER_BUTTON_TEXT, str(question["id"])
    ):
        await mark_question_sent(question["id"])
    else:
        logger.warning(
            "failed to send question id=%s user=%s, will retry", question["id"], user_id
        )


async def send_daily_question(user_id: int) -> None:
    if await has_pending_question(user_id):
        logger.info(
            "user=%s already has a question in flight (open or deferred), skipping today",
            user_id,
        )
        return

    question = await pick_outgoing_message("question", OUTGOING_DEDUP_DAYS, user_id)
    if question is None:
        logger.info("no question available for user=%s today", user_id)
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
        logger.info("scheduled today's question for user=%s at %s", user_id, run_date.isoformat())


async def release_due_questions() -> None:
    for row in await get_due_deferred_questions():
        await _send_question(row["user_id"], row)


async def start_scheduler() -> None:
    scheduler.add_job(
        schedule_today_question,
        trigger=CronTrigger(hour=0, minute=5, timezone=TIMEZONE),
        id="schedule_daily_question",
    )
    scheduler.add_job(
        release_due_questions,
        trigger="interval",
        seconds=60,
        id="release_due_questions",
    )
    scheduler.start()
    await schedule_today_question()
