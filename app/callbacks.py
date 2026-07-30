from app.config import DEFERRAL_DELAY_HOURS
from app.db import apply_deferral, consume_card_reminder, defer_card_reminder, get_outgoing_by_id, is_registered
from app.flashcard_session import handle_card_grade, start_review_session
from app.telegram import answer_callback_query, clear_message_buttons, delete_message, send_message


async def process_callback_query(callback_query: dict) -> None:
    callback_id = callback_query["id"]
    from_user_id = callback_query["from"]["id"]
    data = callback_query.get("data")
    message = callback_query.get("message") or {}
    message_id = message.get("message_id")

    await answer_callback_query(callback_id)

    if not await is_registered(from_user_id):
        return
    if data is None:
        return

    if data.isdigit():
        await _handle_question_defer(from_user_id, int(data))
    elif data.startswith("card:") and message_id is not None:
        await _handle_card_callback(from_user_id, message_id, data)
    elif data.startswith("cardrem:") and message_id is not None:
        await _handle_card_reminder_callback(from_user_id, message_id, data)


async def _handle_question_defer(user_id: int, question_id: int) -> None:
    question = await get_outgoing_by_id(question_id)
    if question is None:
        return
    if question["user_id"] != user_id or not question["is_open"]:
        return

    await apply_deferral(question["id"], question["user_id"], question["deferral_count"])


async def _handle_card_callback(user_id: int, message_id: int, data: str) -> None:
    # "card:{card_id}:know" or "card:{card_id}:dontknow"
    parts = data.split(":")
    if len(parts) != 3 or not parts[1].isdigit() or parts[2] not in ("know", "dontknow"):
        return
    await handle_card_grade(user_id, message_id, int(parts[1]), know=(parts[2] == "know"))


async def _handle_card_reminder_callback(user_id: int, message_id: int, data: str) -> None:
    # "cardrem:{reminder_id}:now" or "cardrem:{reminder_id}:later"
    parts = data.split(":")
    if len(parts) != 3 or not parts[1].isdigit() or parts[2] not in ("now", "later"):
        return
    reminder_id = int(parts[1])

    if parts[2] == "later":
        # Unlike "Сейчас" (below), there's no session starting here to
        # explain the message's continued presence — confirmed live: leaving
        # it in place (buttons-only cleared) read as the tap having done
        # nothing at all, and nothing told the user it was actually deferred.
        await defer_card_reminder(reminder_id, DEFERRAL_DELAY_HOURS)
        await delete_message(user_id, message_id)
        await send_message(user_id, f"Хорошо, напомню через {DEFERRAL_DELAY_HOURS} ч.")
        return

    await clear_message_buttons(user_id, message_id)
    await consume_card_reminder(reminder_id)
    # The reminder message itself is "the message that started the session"
    # and stays put per spec — only the per-card messages get cleaned up.
    await start_review_session(user_id, start_message_id=message_id)
