"""Handles every real (ACTIVE) message the bot receives — see
handle_active_message(), called straight from app/service.py as soon as the
webhook receives it. Kanban reads, relay/reminders, Chinese words and
sales are all fully deterministic (or a single narrow LLM extraction at
most — see app/odysseus_client.extract_fields_via_llm) and answered
directly, never through Odysseus's agent loop. Only general journal
notes/finance logging and open-ended Q&A still go through Odysseus, since
those genuinely need search + reasoning over existing entries.

Ежедневник (daily journal check-in), activity tracking, /addbook and book
reviews are all handled entirely in app/service.py instead —
deterministically, no LLM involved at all, since each is a fixed
one-question-at-a-time sequence.
"""

import re
import traceback
from datetime import datetime, timedelta

from app.config import TIMEZONE
from app.db import ack_incoming_messages, utcnow
from app.people import NAME_TO_USER_ID, USER_NAMES
from app.reminder_time import parse_reminder_time
from app import humanize, threads, triggers
from app.reminders import schedule_reminder
from app.channel import send_message
from app.trilium_client import (
    add_kanban_card,
    get_day_summary,
    get_week_summary,
    log_reminder_to_calendar,
    read_kanban_status,
)

# Same stem prefixes as the person-name canonicalization this used to rely
# on in Odysseus: "ОСТАП" covers all its declensions (Остапа/Остапу/...),
# while МАША needs both "МАШ" (Маша/Маши/Маше...) and the formal "МАРИ"
# (Мария/Марии...), since case endings differ between the two.
_OTHER_PERSON_PREFIXES = {
    "МАША": ("ОСТАП",),
    "ОСТАП": ("МАШ", "МАРИ"),
}
_ANONYMOUS_RE = re.compile(
    r"не говори.{0,20}(от меня|кто|что это я)|анонимн|без подписи|не подписыва",
    re.IGNORECASE,
)


def _detect_relay_target(sender_name: str, text: str) -> str:
    """The OTHER registered person if their name is mentioned anywhere in
    the text (a relay), otherwise the sender themself (a self-reminder)."""
    other_name, other_prefixes = next(
        (name, prefixes) for name, prefixes in _OTHER_PERSON_PREFIXES.items() if name != sender_name
    )
    mentions_other = any(re.search(p, text, re.IGNORECASE) for p in other_prefixes)
    return other_name if mentions_other else sender_name


def _detect_anonymous(text: str) -> bool:
    """"не говори что от меня" / "анонимно" / "не подписывай" etc — loose,
    risk-accepting keyword approach: a false positive just drops
    attribution unnecessarily, a false negative reveals a sender who asked
    not to be revealed — only worth catching the explicit, unambiguous
    phrasing."""
    return bool(_ANONYMOUS_RE.search(text))


async def _handle_relay_or_reminder(message_id: int, user_id: int, text: str) -> None:
    """A self-reminder or a message relayed to the other registered
    person — fully handled here, never through Odysseus/an LLM: the bot
    already knows the sender (user_id) and the target/anonymity are
    decided by the same lightweight keyword rules Odysseus's deterministic
    fallback used to apply after the model failed to call schedule_send
    (which it did most of the time — see git history). The one thing that
    genuinely benefited from a model was recovering a specific requested
    time from free text, so that's the only piece still allowed to ask an
    LLM — and only a narrow single-shot one (see app/reminder_time.py /
    parse_time_via_llm), never the full agent loop."""
    sender_name = USER_NAMES.get(user_id, str(user_id))
    sender_id = user_id
    target_name = _detect_relay_target(sender_name, text)
    target_id = NAME_TO_USER_ID.get(target_name, user_id)
    anonymous = _detect_anonymous(text)

    # Разбор времени — только регулярками (app/reminder_time.py). Запасной
    # вызов модели убран вместе с Odysseus: фраза со словом-признаком
    # времени, но не подошедшая ни под один шаблон, теперь просто означает
    # «сейчас» — как и любая другая фраза без времени.
    run_at, _needs_llm = parse_reminder_time(text)
    if run_at is None:
        run_at = utcnow()

    await schedule_reminder(sender_id, target_id, text, run_at, anonymous)
    await log_reminder_to_calendar(sender_name, target_name, text, run_at.astimezone(TIMEZONE))

    # Confirm, or the request vanishes into silence: scheduling something
    # for tomorrow used to produce no reply at all, leaving no way to tell
    # whether it had been understood. (Nothing to confirm when it's due
    # right now — schedule_reminder has already delivered it.)
    if run_at > utcnow() + timedelta(seconds=30):
        when = humanize.format_when(run_at)
        if target_id == sender_id:
            await send_message(user_id, f"Напомню {when}.")
        else:
            await send_message(user_id, f"Передам {target_name.capitalize()} {when}.")
    elif target_id != sender_id:
        await send_message(user_id, f"Передал {target_name.capitalize()}.")

    await ack_incoming_messages([message_id])

async def _handle_kanban_add(message_id: int, user_id: int, text: str) -> None:
    """«добавь в канбан купить молоко» — заголовок отрезается регуляркой
    (см. triggers.strip_task_prefix), без обращения к модели.

    Колонку больше не выбираем: всё падает в БЭКЛОГ, то есть в инбокс, и
    день ему назначается разбором в /inbox. Раньше модель угадывала
    колонку из фразы — лишняя степень свободы, которая ничего не решала:
    «в работе» осмысленно только когда ты и правда начал."""
    person_name = USER_NAMES.get(user_id, str(user_id))
    title = triggers.strip_task_prefix(text)
    try:
        await add_kanban_card(person_name, title)
        await send_message(user_id, f"Добавил «{title}» в инбокс.")
    except Exception:
        print(f"_handle_kanban_add failed for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(user_id, TRILIUM_UNAVAILABLE_TEXT)
    await ack_incoming_messages([message_id])

TRILIUM_UNAVAILABLE_TEXT = "Не могу сейчас связаться с Trilium. Попробуй чуть позже."


async def send_kanban_status(user_id: int, trigger_message_id: int | None = None) -> None:
    """Direct trigger for the /kanban command — a pure read with zero
    content judgment, so it goes straight to Trilium (see
    app/trilium_client.py), never through Odysseus/an LLM at all. Wrapped
    in try/except since this is called directly from main.py's webhook
    handler with nothing else in between — confirmed live (back when this
    went through Odysseus): an outage turned an unhandled exception here
    into a bare 500 to Telegram and total silence to the user."""
    try:
        answer = await read_kanban_status()
    except Exception:
        print(f"send_kanban_status failed for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(user_id, TRILIUM_UNAVAILABLE_TEXT)
        return

    thread_id = await threads.open_thread(user_id, threads.TTL_INFO, trigger_message_id)
    await threads.send(thread_id, user_id, answer, parse_mode="HTML")


async def send_week_summary(user_id: int, trigger_message_id: int | None = None) -> None:
    """/week — sibling of send_kanban_status above: a pure Trilium read,
    shown in a self-clearing info thread, wrapped so an outage can't turn
    into silence."""
    person_name = USER_NAMES.get(user_id, str(user_id))
    try:
        summary = await get_week_summary(person_name)
    except Exception:
        print(f"send_week_summary failed for user={user_id}:", flush=True)
        traceback.print_exc()
        summary = TRILIUM_UNAVAILABLE_TEXT

    thread_id = await threads.open_thread(user_id, threads.TTL_INFO, trigger_message_id)
    await threads.send(thread_id, user_id, summary, parse_mode="HTML")


async def send_day_summary(user_id: int, trigger_message_id: int | None = None) -> None:
    """/today — the same shape as send_week_summary, but one day's own row
    rather than the week's aggregates."""
    person_name = USER_NAMES.get(user_id, str(user_id))
    try:
        summary = await get_day_summary(person_name)
    except Exception:
        print(f"send_day_summary failed for user={user_id}:", flush=True)
        traceback.print_exc()
        summary = TRILIUM_UNAVAILABLE_TEXT

    thread_id = await threads.open_thread(user_id, threads.TTL_INFO, trigger_message_id)
    await threads.send(thread_id, user_id, summary, parse_mode="HTML")


async def handle_active_message(
    message_id: int, user_id: int, text: str, received_at: datetime,
    reply_to_text: str | None = None, telegram_message_id: int | None = None,
) -> None:
    """Answer a real question right away — called as a fire-and-forget task
    from app/service.py as soon as the webhook receives it."""
    try:
        # Precedence across every trigger in the bot is decided in one
        # place (app/triggers.py); by the time a message reaches here,
        # app/service.py has already taken the dialogue starters it owns.
        trigger = triggers.classify(text)

        if trigger == "kanban_add":
            await _handle_kanban_add(message_id, user_id, text)
            return

        # Kanban is a pure read with zero content judgment, so it's handled
        # entirely before ever reaching Odysseus/an LLM — same reasoning as
        # the ежедневник flow, see app/trilium_client.py.
        if trigger == "kanban_status":
            try:
                answer = await read_kanban_status()
            except Exception:
                print(f"handle_active_message: kanban read failed for incoming id={message_id}:", flush=True)
                traceback.print_exc()
                answer = TRILIUM_UNAVAILABLE_TEXT
            thread_id = await threads.open_thread(user_id, threads.TTL_INFO, telegram_message_id)
            await threads.send(thread_id, user_id, answer, parse_mode="HTML")
            await ack_incoming_messages([message_id])
            return

        # Same reasoning as kanban above — sender, target and anonymity are
        # all decided by lightweight keyword rules with no real judgment
        # needed, so this never reaches Odysseus except for a narrow,
        # single-shot time-parsing fallback (see _handle_relay_or_reminder).
        if trigger == "relay_or_reminder":
            await _handle_relay_or_reminder(message_id, user_id, text)
            return

        # Сюда доходит то, что не подошло ни под один триггер. Раньше это
        # уходило в свободный разговор с Odysseus; им не пользовались, и
        # ветку убрали вместе с ним. Молчать нельзя: сообщение уже
        # отправлено человеку в пустоту, и без ответа непонятно, дошло ли
        # оно вообще.
        await send_message(
            user_id,
            "Не понял, что с этим делать. Посмотри /help — там всё, что я умею.",
        )
        await ack_incoming_messages([message_id])
    except Exception:
        print(f"handle_active_message failed for incoming id={message_id}:", flush=True)
        traceback.print_exc()
        # Подтверждено на живом: сбой здесь молча съедал сообщение —
        # вебхук отвечал 200, а человек не получал ничего и не знал, что
        # что-то сломалось. Намеренно не подтверждаем сообщение: если сбой
        # был временным, его можно переиграть через /sync/reprocess_active.
        await send_message(user_id, "Что-то сломалось на моей стороне. Попробуй ещё раз чуть позже.")
