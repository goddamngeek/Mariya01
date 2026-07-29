"""Flashcard review sessions — orchestrates db.py + telegram.py around the
scheduling math in app/flashcards.py. One card = one new Telegram message
(never edited); all of a session's card messages get deleted when it closes,
leaving only the message that started it and a closing line.
"""

import random

from app.db import (
    add_session_message,
    close_card_session,
    create_card_session,
    get_due_cards,
    get_open_card_session,
    increment_session_reviewed,
    update_card_schedule,
)
from app.flashcards import compute_next_schedule
from app.telegram import clear_message_buttons, delete_message, send_card_message, send_message

PRAISE_PHRASES = [
    "Отлично! Ты справился со всеми карточками 🎉",
    "Так держать — память работает на все 100%!",
    "Ещё один шаг к идеальному запоминанию 💪",
    "Браво! Повторение — мать учения, и ты это доказал.",
    "Красавчик! Мозг качается лучше мышц 🧠",
    "Ты сегодня герой карточек!",
    "Прогресс налицо — продолжай в том же духе.",
    "Вот это результат! Гордимся тобой.",
    "Твои нейроны довольны — они только что укрепили связи 🔗",
    "Ещё чуть-чуть, и ты знаешь это лучше, чем родной язык.",
    "Класс! Повторение прошло на отлично.",
    "Ты справился — можно выдохнуть и похвалить себя.",
    "Каждая карточка — маленькая победа. Сегодня их было много!",
    "Огонь! Так и запоминают чемпионы.",
    "Молодец! Завтра будет ещё легче — ты уже заложил фундамент.",
]

POSTPONE_PHRASES = [
    "Это со мной что-то не так или с тобой? 👓",
    "Коридор 🐡",
    "Что то шевелится. Не сейчас 🧗‍♂️",
    "Кто-то стучит в трубе ‼️",
    "Пол гудит. Позже 🆒",
    "Часы идут назад 💱",
    "Радио шепчет твоё имя 📉",
    "Дверь была закрыта 🫟",
    "ахахааххахахахах 🔢",
    "Ладно, дадим себе передышку, пока пингвин осваивает алгебру 🦤",
    "Кто здесь? 🧟‍♂️",
    "Тени длиннее, чем должны 👨‍👦",
    "хватит 🛞",
    "Ок, сделаем паузу — где-то там кальмар пишет твою карму 🚸",
    "Тишина слишком громкая 👨‍👧‍👧",
]


def _grade_buttons(card_id: int) -> list[tuple[str, str]]:
    return [("✅", f"card:{card_id}:know"), ("❌", f"card:{card_id}:dontknow")]


async def _send_next_card(user_id: int, session_id: int) -> bool:
    """Send the next due card as a new message (chat_id == user_id — this
    bot is always a private 1:1 chat). Returns False if none left."""
    due = await get_due_cards(user_id)
    if not due:
        return False
    card = due[0]
    message_id = await send_card_message(user_id, card["front"], card["back"], _grade_buttons(card["id"]))
    if message_id is not None:
        await add_session_message(session_id, message_id)
    return True


async def start_review_session(user_id: int, start_message_id: int | None) -> str:
    """Returns "started", "already_open", or "no_cards" — distinct outcomes,
    not a bare bool, since a caller (Odysseus's require_tool fallback) needs
    to tell the user WHY nothing happened rather than staying silent.
    Confirmed live: a user with zero flashcards asked to start review and
    got no response at all — the model's own text came back empty and
    nothing else explained the silence."""
    if await get_open_card_session(user_id) is not None:
        return "already_open"

    due = await get_due_cards(user_id)
    if not due:
        return "no_cards"

    session_id = await create_card_session(user_id, start_message_id, total_count=len(due))
    await _send_next_card(user_id, session_id)
    return "started"


async def handle_card_grade(user_id: int, clicked_message_id: int, card_id: int, know: bool) -> None:
    session = await get_open_card_session(user_id)
    if session is None:
        return  # session already closed (timeout race) — ignore a late click

    # Guard against double-grading: only accept a click on a message that's
    # still part of this open session (i.e. hasn't already been cleared).
    if clicked_message_id not in session["message_ids"]:
        return

    card_rows = await get_due_cards(user_id)
    card = next((c for c in card_rows if c["id"] == card_id), None)
    if card is None:
        return  # not due (already graded via a stale/duplicate click) — ignore

    new_ease, new_interval, new_reps, next_at = compute_next_schedule(
        card["ease_factor"], card["interval_days"], card["repetitions"], know,
    )
    await update_card_schedule(card_id, new_ease, new_interval, new_reps, next_at)
    await increment_session_reviewed(session["id"])
    await clear_message_buttons(user_id, clicked_message_id)

    sent_next = await _send_next_card(user_id, session["id"])
    if not sent_next:
        # Re-fetch: increment_session_reviewed() above changed reviewed_count.
        session = await get_open_card_session(user_id)
        await _close_session(session, completed=True)


async def _close_session(session, completed: bool) -> None:
    chat_id = session["user_id"]
    await close_card_session(session["id"])
    for mid in session["message_ids"]:
        await delete_message(chat_id, mid)

    if completed:
        text = f"Готово: {session['reviewed_count']}/{session['total_count']}. {random.choice(PRAISE_PHRASES)}"
    else:
        text = random.choice(POSTPONE_PHRASES)
    await send_message(chat_id, text)


async def close_idle_session(session) -> None:
    """Called by the 60s idle-sweep job (scheduler.py) for a session that's
    been quiet >10min — treated as abandoned, not a grade of any kind, so
    the card mid-review just keeps its existing schedule and resurfaces
    whenever it's next due."""
    await _close_session(session, completed=False)
