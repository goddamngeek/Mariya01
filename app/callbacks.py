from app.db import apply_deferral, get_outgoing_by_id, is_registered
from app.telegram import answer_callback_query


async def process_callback_query(callback_query: dict) -> None:
    callback_id = callback_query["id"]
    from_user_id = callback_query["from"]["id"]
    data = callback_query.get("data")

    await answer_callback_query(callback_id)

    if not await is_registered(from_user_id):
        return
    if data is None:
        return

    if data.isdigit():
        await _handle_question_defer(from_user_id, int(data))


async def _handle_question_defer(user_id: int, question_id: int) -> None:
    question = await get_outgoing_by_id(question_id)
    if question is None:
        return
    if question["user_id"] != user_id or not question["is_open"]:
        return

    await apply_deferral(question["id"], question["user_id"], question["deferral_count"])
