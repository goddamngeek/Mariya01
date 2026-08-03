import httpx

from app.config import TELEGRAM_BOT_TOKEN

_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    """Shared, keep-alive client — reused across calls instead of paying a
    fresh TCP+TLS handshake to api.telegram.org on every single message."""
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=10)
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


async def send_message(chat_id: int | str, text: str) -> bool:
    """One attempt, no retries here — callers decide what happens on failure."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = await get_client().post(url, json={"chat_id": chat_id, "text": text})
        resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        print(f"telegram sendMessage failed: {exc}", flush=True)
        return False


async def set_bot_commands() -> None:
    """Registers /cards in Telegram's own native bot command menu (the "/"
    menu button next to the message input, built into every Telegram chat
    with a bot) — this is Telegram's built-in mechanism, not a bot-specific
    workaround like a custom reply keyboard. Global across all chats with
    this bot (not per-user), so this only needs calling once at startup;
    re-calling on every restart is harmless (pure idempotent registration,
    no message sent to anyone)."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setMyCommands"
    commands = [
        {"command": "start", "description": "Активировать бота"},
        {"command": "cards", "description": "Начать повторение карточек"},
        {"command": "kanban", "description": "Показать канбан-доску задач"},
        {"command": "stop", "description": "Остановить сессию повторения карточек"},
        {"command": "checkin", "description": "Повторить текущий вопрос ежедневника"},
        {"command": "help", "description": "Что умеет бот"},
    ]
    try:
        resp = await get_client().post(url, json={"commands": commands})
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        print(f"telegram setMyCommands failed: {exc}", flush=True)


async def send_message_with_button(
    chat_id: int | str, text: str, button_text: str, callback_data: str
) -> bool:
    """One attempt, no retries here — callers decide what happens on failure."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "reply_markup": {
            "inline_keyboard": [[{"text": button_text, "callback_data": callback_data}]]
        },
    }
    try:
        resp = await get_client().post(url, json=payload)
        resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        print(f"telegram sendMessage (with button) failed: {exc}", flush=True)
        return False


async def send_message_with_buttons(
    chat_id: int | str, text: str, buttons: list[tuple[str, str]]
) -> int | None:
    """buttons: list of (button_text, callback_data), all in one row.
    Returns the sent message_id on success, None on failure — callers that
    need to track/delete the message later (flashcard sessions) require the
    id, unlike send_message_with_button's bool-only contract."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "reply_markup": {
            "inline_keyboard": [[{"text": t, "callback_data": d} for t, d in buttons]]
        },
    }
    try:
        resp = await get_client().post(url, json=payload)
        resp.raise_for_status()
        return resp.json()["result"]["message_id"]
    except httpx.HTTPError as exc:
        print(f"telegram sendMessage (with buttons) failed: {exc}", flush=True)
        return None


async def clear_message_buttons(chat_id: int | str, message_id: int) -> bool:
    """Remove the inline keyboard from a message — used right after a
    flashcard is graded, so an already-answered card can't be graded twice
    by clicking it again before the session's messages get cleaned up."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageReplyMarkup"
    payload = {"chat_id": chat_id, "message_id": message_id, "reply_markup": {"inline_keyboard": []}}
    try:
        resp = await get_client().post(url, json=payload)
        resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        print(f"telegram editMessageReplyMarkup failed: {exc}", flush=True)
        return False


async def delete_message(chat_id: int | str, message_id: int) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteMessage"
    try:
        resp = await get_client().post(url, json={"chat_id": chat_id, "message_id": message_id})
        resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        print(f"telegram deleteMessage failed: {exc}", flush=True)
        return False


def _utf16_len(s: str) -> int:
    """Telegram message-entity offset/length is in UTF-16 code units, not
    Python codepoints — matters here because card fronts/backs come from
    arbitrary Trilium note text that may contain non-BMP characters (emoji,
    etc.) needing a surrogate pair (2 units) instead of 1."""
    return len(s.encode("utf-16-le")) // 2


async def send_card_message(
    chat_id: int | str, front: str, back: str, buttons: list[tuple[str, str]]
) -> int | None:
    """Front is plain text; back is hidden behind a native Telegram spoiler
    entity (tap to reveal client-side — no round trip to the bot, so both
    grading buttons are shown from the start rather than after a reveal
    step). Entities avoid MarkdownV2 escaping, which arbitrary note text
    would otherwise routinely break."""
    prefix = f"🃏 {front}\n\n"
    text = prefix + back
    entity = {"type": "spoiler", "offset": _utf16_len(prefix), "length": _utf16_len(back)}

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "entities": [entity],
        "reply_markup": {
            "inline_keyboard": [[{"text": t, "callback_data": d} for t, d in buttons]]
        },
    }
    try:
        resp = await get_client().post(url, json=payload)
        resp.raise_for_status()
        return resp.json()["result"]["message_id"]
    except httpx.HTTPError as exc:
        print(f"telegram sendMessage (card) failed: {exc}", flush=True)
        return None


async def answer_callback_query(callback_query_id: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    try:
        resp = await get_client().post(url, json={"callback_query_id": callback_query_id})
        resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        print(f"telegram answerCallbackQuery failed: {exc}", flush=True)
        return False
