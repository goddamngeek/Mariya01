from app.db import is_registered
from app.service import (
    handle_book_finished,
    handle_book_quotes_selected,
    handle_expense_account_selected,
    handle_expense_change_account,
    handle_planner_action,
    handle_finished_book_selected,
    handle_quote_book_selected,
    handle_reading_book_selected,
)
from app.telegram import answer_callback_query


async def process_callback_query(callback_query: dict) -> None:
    """Routes button presses: "bq:{prompt_id}:{index}" (book choice for the
    /quote flow, see app/service.py's start_quote_flow), "rb:{note_id}"
    (book choice for "что я сейчас читаю", see show_reading_status),
    "pb:{note_id}" (same for "прочитанные", see show_finished_books) and
    "fd:{note_id}" ("Я дочитал", see handle_book_finished) and
    "im:{note_id}" ("Интересные моменты", see handle_book_quotes_selected)
    — anything else
    just gets answered so the user's client doesn't show a spinner
    forever."""
    data = callback_query.get("data") or ""
    chat_id = callback_query.get("message", {}).get("chat", {}).get("id")

    if chat_id is not None and await is_registered(chat_id):
        if data.startswith("bq:"):
            await handle_quote_book_selected(callback_query)
            return
        if data.startswith("rb:"):
            await handle_reading_book_selected(callback_query)
            return
        if data.startswith("pb:"):
            await handle_finished_book_selected(callback_query)
            return
        if data.startswith("fd:"):
            await handle_book_finished(callback_query)
            return
        if data.startswith("im:"):
            await handle_book_quotes_selected(callback_query)
            return
        # Планировщик дня: pt/pm/pl/ph — разбор инбокса, pd — «готово» в
        # /plan (см. handle_planner_action).
        if data[:2] in ("pt", "pm", "pl", "ph", "pd") and data[2:3] == ":":
            await handle_planner_action(callback_query)
            return

        # Траты: fx — «другой счёт» под подтверждением, fx2 — выбранный.
        if data.startswith("fx2:"):
            await handle_expense_account_selected(callback_query)
            return
        if data.startswith("fx:"):
            await handle_expense_change_account(callback_query)
            return

    await answer_callback_query(callback_query["id"])
