from app.config import OUTGOING_DEDUP_DAYS
from app.db import (
    close_question,
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

    await _send_reply(user_id)


async def _send_reply(user_id: int) -> None:
    reply = await pick_outgoing_message("reply", OUTGOING_DEDUP_DAYS, user_id)
    if reply is None:
        return

    if await send_message(user_id, reply["text"]):
        await mark_reply_sent(reply["id"])
