from app.telegram import answer_callback_query


async def process_callback_query(callback_query: dict) -> None:
    """No inline-keyboard buttons are sent by the bot anymore (the old
    daily-question "Спросить позже" button was the last one) — still
    answers any stray callback Telegram delivers from a button on an
    older message, so the user's client doesn't show a spinner forever."""
    await answer_callback_query(callback_query["id"])
