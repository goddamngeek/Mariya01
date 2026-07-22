from app.db import apply_deferral, get_outgoing_by_id, is_registered
from app.telegram import answer_callback_query


async def process_callback_query(callback_query: dict) -> None:
    callback_id = callback_query["id"]
    from_user_id = callback_query["from"]["id"]
    data = callback_query.get("data")

    await answer_callback_query(callback_id)

    if not await is_registered(from_user_id):
        return
    if data is None or not data.isdigit():
        return

    question = await get_outgoing_by_id(int(data))
    if question is None:
        return
    if question["user_id"] != from_user_id or not question["is_open"]:
        return

    await apply_deferral(question["id"], question["user_id"], question["deferral_count"])
