from app.db import is_registered
from app.service import handle_quote_book_selected
from app.telegram import answer_callback_query


async def process_callback_query(callback_query: dict) -> None:
    """Routes button presses. Currently only "bq:{prompt_id}:{index}" (book
    choice for the /quote flow, see app/service.py's start_quote_flow) is a
    real button — anything else just gets answered so the user's client
    doesn't show a spinner forever."""
    data = callback_query.get("data") or ""
    chat_id = callback_query.get("message", {}).get("chat", {}).get("id")

    if data.startswith("bq:") and chat_id is not None and await is_registered(chat_id):
        await handle_quote_book_selected(callback_query)
        return

    await answer_callback_query(callback_query["id"])
