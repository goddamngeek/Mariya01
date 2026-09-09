from app.db import is_registered
from app.service import (
    handle_book_finished,
    handle_book_started,
    handle_wanted_book_selected,
    handle_wanted_film_selected,
    handle_watched_film_selected,
    handle_film_watched,
    handle_book_quotes_selected,
    handle_book_edit_details,
    handle_ledger_choice,
    handle_planner_action,
    handle_finished_book_selected,
    handle_quote_book_selected,
    handle_reading_book_selected,
)
from app.channel import answer_callback_query
from app.press import Press


async def handle_press(press: Press) -> None:
    """Routes button presses: "bq:{prompt_id}:{index}" (book choice for the
    /quote flow, see app/service.py's start_quote_flow), "rb:{note_id}"
    (book choice for "что я сейчас читаю", see show_reading_status),
    "pb:{note_id}" (same for "прочитанные", see show_finished_books) and
    "fd:{note_id}" ("Я дочитал", see handle_book_finished) and
    "im:{note_id}" ("Интересные моменты", see handle_book_quotes_selected)
    — anything else
    just gets answered so the user's client doesn't show a spinner
    forever."""
    data = press.data
    chat_id = press.chat_id

    if chat_id is not None and await is_registered(chat_id):
        if data.startswith("bq:"):
            await handle_quote_book_selected(press)
            return
        if data.startswith("rb:"):
            await handle_reading_book_selected(press)
            return
        if data.startswith("wb:"):
            await handle_wanted_book_selected(press)
            return
        if data.startswith("sb:"):
            await handle_book_started(press)
            return
        if data.startswith("pb:"):
            await handle_finished_book_selected(press)
            return
        if data.startswith("fw:"):
            await handle_wanted_film_selected(press)
            return
        if data.startswith("fp:"):
            await handle_watched_film_selected(press)
            return
        if data.startswith("fm:"):
            await handle_film_watched(press)
            return
        if data.startswith("fd:"):
            await handle_book_finished(press)
            return
        if data.startswith("bd:"):
            await handle_book_edit_details(press)
            return
        if data.startswith("im:"):
            await handle_book_quotes_selected(press)
            return
        # Планировщик дня: pt/pm/pl/ph — разбор инбокса, pd — «готово» в
        # /plan (см. handle_planner_action).
        if data[:2] in ("pt", "pm", "pl", "ph", "pd") and data[2:3] == ":":
            await handle_planner_action(press)
            return

        # Траты в леджере (app/ledger.py): категория, счёт, отмена под уже
        # записанной строкой.
        if data.startswith("el:"):
            await handle_ledger_choice(press)
            return

    await answer_callback_query(press.id)
