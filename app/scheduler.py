import random
import traceback
from datetime import datetime, time, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from app.config import (
    THOUGHT_CHANNEL,
    TIMEZONE,
    WATER_REMINDER_TEXTS,
    WATER_REMINDER_WINDOWS,
)
from app.db import (
    claim_reminder,
    append_ezhednevnik_question,
    close_ezhednevnik_prompt,
    close_open_ezhednevnik_prompts,
    create_ezhednevnik_prompt,
    ensure_water_reminder,
    get_due_reminders,
    get_due_water_reminders,
    get_open_activity_prompt,
    get_registered_user_ids,
    get_stale_message_threads,
    has_open_ezhednevnik,
    mark_water_reminder_sent,
    pop_logged_messages_except,
    trim_chat_message_log,
    trim_journal,
)
from app import parables, threads
from app.trilium_client import archive_done_cards
from app.prompts import ezhednevnik_step_text
from app.reminders import deliver_reminder
from app.telegram import delete_messages, send_message, send_message_get_id

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
    Those three are weekdays only; Saturday and Sunday get the evening
    retrospective alone, at 23:00 (see start_scheduler).
    Each slot is a one-question-at-a-time sequence (EZHEDNEVNIK_STEPS in
    prompts.py) — this only ever sends the FIRST question; every step after
    that is driven entirely by app/service.py as replies come in."""
    text = ezhednevnik_step_text(slot, 0)
    for user_id in await get_registered_user_ids():
        # Whatever was open stops being the slot a plain message answers, so
        # this one can take over. It is NOT skipped when something is still
        # unanswered — that rule silently cost whole check-ins (an
        # unanswered pm meant the evening retrospective was never asked at
        # all). The closed one stays resumable by replying to any of its
        # questions; see app/service.py's handle_ezhednevnik_question_reply.
        closed = await close_open_ezhednevnik_prompts(user_id)
        if closed:
            print(f"user={user_id}: closed {closed} unfinished ezhednevnik check-in(s) before {slot}", flush=True)
        prompt_id = await create_ezhednevnik_prompt(user_id, slot)
        message_id = await send_message_get_id(user_id, text)
        if message_id is None:
            # Close it again rather than leaving a check-in open whose
            # question nobody ever saw — otherwise the person's next
            # ordinary message gets swallowed as the answer to an invisible
            # question.
            await close_ezhednevnik_prompt(prompt_id)
            print(f"failed to send ezhednevnik {slot} prompt to user={user_id}", flush=True)
            continue
        await append_ezhednevnik_question(prompt_id, message_id)


async def send_thought_of_the_day(part: int = 0) -> None:
    """Мысль дня из «Круга чтения» — see app/parables.py. One day is sent in
    three portions (9:00 / 14:00 / 20:00) that continue each other, rather
    than one 900-character sample standing in for the whole day.

    Уходит только в канал, в личку — нет. Раньше шло и туда, и туда, и в
    личке это была третья за день рассылка, которую никто не просил; в
    канале же посты копятся в архив и никому не мешают. Прочитать мысль в
    личке по-прежнему можно, но по своей воле — командой /thought.

    Не в ветке и с log=False: смысл ровно обратный уборке, посты должны
    остаться. Это про chat_messages_log; в историю (chat_journal) пост
    попадает, её никто не выметает.

    A portion that isn't there is normal, not a failure: days differ in
    size and a short one runs out before the evening slot."""
    if not THOUGHT_CHANNEL:
        print("thought of the day: THOUGHT_CHANNEL not set, nothing to post to", flush=True)
        return
    thought = parables.compose_for(datetime.now(TIMEZONE).date(), part)
    if thought is None:
        print(f"thought of the day: nothing left for part {part} today, skipping", flush=True)
        return
    if not await send_message(THOUGHT_CHANNEL, thought, parse_mode="HTML", log=False):
        print(f"failed to post thought of the day to {THOUGHT_CHANNEL}", flush=True)


GRANDMA_CALL_TEXT = "Позвони бабушкам — они ждут звонка."


async def send_grandma_reminder() -> None:
    """Weekly nudge, Sunday midday — the kind of thing that slips not
    because it's hard but because nothing ever asks. No reply expected;
    lives a day and clears itself, so an unanswered one doesn't sit in the
    chat as a reproach."""
    for user_id in await get_registered_user_ids():
        thread_id = await threads.open_thread(user_id, threads.TTL_DAY)
        await threads.send(thread_id, user_id, GRANDMA_CALL_TEXT)


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


async def trim_logs() -> None:
    """Hourly housekeeping: drop chat-log rows Telegram would refuse to
    delete anyway (see CHAT_LOG_RETENTION_HOURS) — otherwise the table only
    ever grows, and /clear spends its time on messages too old to remove."""
    trimmed = await trim_chat_message_log()
    if trimmed:
        print(f"trimmed {trimmed} chat-log row(s) past Telegram's delete window", flush=True)
    # Журнал разговора (chat_journal) — история, а не рабочая таблица: его не
    # трогает ни /clear, ни удаление сообщений, поэтому подрезать его больше
    # некому. Полгода — это тысячи строк на двоих, не гигабайты.
    trimmed = await trim_journal()
    if trimmed:
        print(f"trimmed {trimmed} journal row(s) older than 180 days", flush=True)


async def release_stale_message_threads() -> None:
    """The timeout path into a thread's terminal state — nothing has
    happened in it for its ttl_minutes, so it gets torn down. The other two
    paths are the flow succeeding and the person reacting to one of its
    messages; all three end in threads.dismiss(). Only this one passes
    send_closing, since only an abandoned thread has anything left to say
    (see /addbook's "Добавил книгу")."""
    for thread in await get_stale_message_threads():
        await threads.dismiss(thread, send_closing=True)


async def release_due_water_reminders() -> None:
    for row in await get_due_water_reminders():
        text = random.choice(WATER_REMINDER_TEXTS)
        # A self-clearing nudge: a thread of exactly one message, so the
        # same teardown as every other flow takes it away (app/threads.py).
        thread_id = await threads.open_thread(row["user_id"], threads.TTL_WATER)
        message_id = await threads.send(thread_id, row["user_id"], text)
        if message_id is None:
            print(f"failed to send water reminder id={row['id']} user={row['user_id']}, will retry", flush=True)
            continue
        await mark_water_reminder_sent(row["id"])


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
    await delete_messages([(row["chat_id"], row["message_id"]) for row in rows])


async def archive_done_tasks() -> None:
    """Еженедельная уборка доски: сделанное уезжает в архив.

    Раз в неделю, а не при каждом «Готово», потому что карточку помечают
    сделанной и руками в Trilium — сметать надо всё, а не только то, что
    прошло через бота. Молча: это гигиена, а не событие, о котором стоит
    писать людям."""
    try:
        moved = await archive_done_cards()
    except Exception:
        print("archive_done_tasks failed:", flush=True)
        traceback.print_exc()
        return
    if moved:
        print(f"архив задач: перенесено {len(moved)} — {'; '.join(moved)}", flush=True)


async def start_scheduler() -> None:
    # Weekdays keep all three slots at their work-shaped times. Weekends get
    # the evening retrospective only, and later: asking "как проходит день"
    # twice on a Saturday landed on people who were out and not looking at
    # the phone, so the questions piled up unanswered and turned into debt
    # by Monday.
    scheduler.add_job(
        send_ezhednevnik_prompts,
        trigger=CronTrigger(day_of_week="mon-fri", hour=12, minute=30, timezone=TIMEZONE),
        args=["am"],
        id="ezhednevnik_am",
    )
    scheduler.add_job(
        send_ezhednevnik_prompts,
        trigger=CronTrigger(day_of_week="mon-fri", hour=18, minute=0, timezone=TIMEZONE),
        args=["pm"],
        id="ezhednevnik_pm",
    )
    scheduler.add_job(
        send_ezhednevnik_prompts,
        trigger=CronTrigger(day_of_week="mon-fri", hour=21, minute=30, timezone=TIMEZONE),
        args=["evening"],
        id="ezhednevnik_evening",
    )
    scheduler.add_job(
        send_ezhednevnik_prompts,
        trigger=CronTrigger(day_of_week="sat,sun", hour=23, minute=0, timezone=TIMEZONE),
        args=["evening"],
        id="ezhednevnik_evening_weekend",
    )
    # One day, three portions. 14:00 and 20:00 sit clear of the check-ins
    # (12:30 / 18:00 / 21:30) on purpose: a thought landing on top of a
    # question would push the question up out of sight.
    scheduler.add_job(
        send_thought_of_the_day,
        trigger=CronTrigger(hour=9, minute=0, timezone=TIMEZONE),
        args=[0],
        id="thought_of_the_day",
    )
    scheduler.add_job(
        send_thought_of_the_day,
        trigger=CronTrigger(hour=14, minute=0, timezone=TIMEZONE),
        args=[1],
        id="thought_of_the_day_midday",
    )
    scheduler.add_job(
        send_thought_of_the_day,
        trigger=CronTrigger(hour=20, minute=0, timezone=TIMEZONE),
        args=[2],
        id="thought_of_the_day_evening",
    )
    scheduler.add_job(
        ensure_today_water_reminders,
        trigger=CronTrigger(hour=0, minute=5, timezone=TIMEZONE),
        id="schedule_daily_water_reminders",
    )
    scheduler.add_job(
        send_grandma_reminder,
        trigger=CronTrigger(day_of_week="sun", hour=12, minute=0, timezone=TIMEZONE),
        id="grandma_reminder",
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
    # Понедельник, раннее утро: неделя начинается с чистой доски, и в это
    # время ничего другого не запускается.
    scheduler.add_job(
        archive_done_tasks,
        trigger=CronTrigger(day_of_week="mon", hour=5, minute=0, timezone=TIMEZONE),
        id="archive_done_tasks",
    )
    scheduler.add_job(
        trim_logs,
        trigger="interval",
        hours=1,
        id="trim_logs",
    )
    scheduler.add_job(
        release_stale_message_threads,
        trigger="interval",
        seconds=60,
        id="release_stale_message_threads",
    )
    scheduler.start()
    await ensure_today_water_reminders()
