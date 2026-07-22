from app.config import OUTGOING_DEDUP_DAYS
from app.db import (
    close_question,
    get_open_question,
    insert_incoming_message,
    mark_reply_sent,
    pick_outgoing_message,
)
from app.telegram import send_message_debug


async def process_incoming_message(user_id: int, text: str) -> None:
    await insert_incoming_message(user_id, text)

    open_question = await get_open_question(user_id)
    if open_question is not None:
        await close_question(open_question["id"])

    await _send_reply(user_id)


async def _send_reply(user_id: int) -> None:
    try:
        reply = await pick_outgoing_message("reply", OUTGOING_DEDUP_DAYS, user_id)
        print(
            "pick_outgoing_message(reply) ->",
            {"id": reply["id"], "text": reply["text"]} if reply is not None else None,
        )
        if reply is None:
            return

        ok, status_code, response_text = await send_message_debug(user_id, reply["text"])
        print(f"telegram send status={status_code} response={response_text}")
        if ok:
            await mark_reply_sent(reply["id"])
    except Exception as e:
        print(f"REPLY ERROR: {repr(e)}", flush=True)
