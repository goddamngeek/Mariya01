from datetime import datetime, timezone

from app.config import ANTI_SPAM_SECONDS, OUTGOING_DEDUP_DAYS
from app.db import (
    close_question,
    get_last_reply_sent_at,
    get_open_question,
    insert_incoming_message,
    mark_reply_sent,
    pick_outgoing_message,
)
from app.telegram import send_message


async def process_incoming_message(user_id: int, text: str) -> None:
    await insert_incoming_message(user_id, text)

    open_question = await get_open_question(user_id)
    if open_question is not None:
        await close_question(open_question["id"])

    await _maybe_send_reply(user_id)


async def _maybe_send_reply(user_id: int) -> None:
    last_sent = await get_last_reply_sent_at(user_id)
    if last_sent is not None:
        elapsed = (datetime.now(timezone.utc) - last_sent).total_seconds()
        if elapsed < ANTI_SPAM_SECONDS:
            return

    reply = await pick_outgoing_message("reply", OUTGOING_DEDUP_DAYS, user_id)
    if reply is None:
        return

    if await send_message(user_id, reply["text"]):
        await mark_reply_sent(reply["id"])
