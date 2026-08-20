import httpx

from app.config import TELEGRAM_BOT_TOKEN
from app.db import log_chat_message

_client: httpx.AsyncClient | None = None


async def _log_sent(chat_id: int | str, message_id: int) -> None:
    """Best-effort — a logging failure here must never take down message
    sending itself, only make that one message invisible to the nightly
    chat-history cleanup (see scheduler.py's clear_chat_history)."""
    try:
        await log_chat_message(int(chat_id), message_id)
    except Exception as exc:
        print(f"log_chat_message failed (non-fatal): {exc!r}", flush=True)


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


async def send_message(chat_id: int | str, text: str, parse_mode: str | None = None) -> bool:
    """One attempt, no retries here — callers decide what happens on failure."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        resp = await get_client().post(url, json=payload)
        resp.raise_for_status()
        await _log_sent(chat_id, resp.json()["result"]["message_id"])
        return True
    except httpx.HTTPError as exc:
        print(f"telegram sendMessage failed: {exc}", flush=True)
        return False


async def send_message_get_id(chat_id: int | str, text: str, parse_mode: str | None = None) -> int | None:
    """Same as send_message, but returns the sent message_id (or None on
    failure) — for callers that need to act on this specific message later,
    e.g. scheduling its deletion (see scheduler.py's water reminders and
    send_temporary_message)."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        resp = await get_client().post(url, json=payload)
        resp.raise_for_status()
        message_id = resp.json()["result"]["message_id"]
        await _log_sent(chat_id, message_id)
        return message_id
    except httpx.HTTPError as exc:
        print(f"telegram sendMessage failed: {exc}", flush=True)
        return None


async def send_message_with_buttons(
    chat_id: int | str, text: str, buttons: list[tuple[str, str]],
) -> int | None:
    """Send a message with an inline keyboard, one button per row —
    buttons is [(label, callback_data), ...]. Returns the sent message_id
    (or None on failure), same as send_message_get_id."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "reply_markup": {"inline_keyboard": [[{"text": label, "callback_data": data}] for label, data in buttons]},
    }
    try:
        resp = await get_client().post(url, json=payload)
        resp.raise_for_status()
        message_id = resp.json()["result"]["message_id"]
        await _log_sent(chat_id, message_id)
        return message_id
    except httpx.HTTPError as exc:
        print(f"telegram sendMessage (with buttons) failed: {exc}", flush=True)
        return None


async def set_bot_commands() -> None:
    """Registers commands in Telegram's own native bot command menu (the "/"
    menu button next to the message input, built into every Telegram chat
    with a bot) — this is Telegram's built-in mechanism, not a bot-specific
    workaround like a custom reply keyboard. Global across all chats with
    this bot (not per-user), so this only needs calling once at startup;
    re-calling on every restart is harmless (pure idempotent registration,
    no message sent to anyone)."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setMyCommands"
    commands = [
        # /start itself still works (handled in app/main.py) — just left out
        # of the visible menu since both real users are already registered
        # and Telegram prompts /start on its own for any unstarted chat.
        {"command": "kanban", "description": "Показать канбан-доску задач"},
        {"command": "week", "description": "Сводка по ежедневнику за неделю"},
        {"command": "checkin", "description": "Повторить текущий вопрос ежедневника"},
        {"command": "links", "description": "Ссылки на Odysseus и Trilium"},
        {"command": "clear", "description": "Очистить историю чата"},
        {"command": "quote", "description": "Добавить интересный момент из книги"},
        {"command": "addbook", "description": "Добавить новую книгу"},
        {"command": "reading", "description": "Что я сейчас читаю"},
        {"command": "help", "description": "Что умеет бот"},
    ]
    try:
        resp = await get_client().post(url, json={"commands": commands})
        resp.raise_for_status()
    except httpx.HTTPError as exc:
        print(f"telegram setMyCommands failed: {exc}", flush=True)




async def delete_message(chat_id: int | str, message_id: int) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/deleteMessage"
    try:
        resp = await get_client().post(url, json={"chat_id": chat_id, "message_id": message_id})
        resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        print(f"telegram deleteMessage failed: {exc}", flush=True)
        return False


async def answer_callback_query(callback_query_id: str, text: str | None = None) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    payload = {"callback_query_id": callback_query_id}
    if text:
        payload["text"] = text
    try:
        resp = await get_client().post(url, json=payload)
        resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        print(f"telegram answerCallbackQuery failed: {exc}", flush=True)
        return False
