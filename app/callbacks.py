from app.db import is_registered
from app.service import handle_quote_book_selected, handle_reading_book_selected
from app.telegram import answer_callback_query


async def process_callback_query(callback_query: dict) -> None:
    """Routes button presses: "bq:{prompt_id}:{index}" (book choice for the
    /quote flow, see app/service.py's start_quote_flow) and "rb:{note_id}"
    (book choice for "что я сейчас читаю", see show_reading_status) are the
    real ones — anything else just gets answered so the user's client
    doesn't show a spinner forever."""
    data = callback_query.get("data") or ""
    chat_id = callback_query.get("message", {}).get("chat", {}).get("id")

    if chat_id is not None and await is_registered(chat_id):
        if data.startswith("bq:"):
            await handle_quote_book_selected(callback_query)
            return
        if data.startswith("rb:"):
            await handle_reading_book_selected(callback_query)
            return

    await answer_callback_query(callback_query["id"])
