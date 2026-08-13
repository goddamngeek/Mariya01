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


async def send_message(chat_id: int | str, text: str, parse_mode: str | None = None) -> bool:
    """One attempt, no retries here — callers decide what happens on failure."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    try:
        resp = await get_client().post(url, json=payload)
        resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        print(f"telegram sendMessage failed: {exc}", flush=True)
        return False


async def send_message_get_id(chat_id: int | str, text: str) -> int | None:
    """Same as send_message, but returns the sent message_id (or None on
    failure) — for callers that need to act on this specific message later,
    e.g. scheduling its deletion (see scheduler.py's water reminders)."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        resp = await get_client().post(url, json={"chat_id": chat_id, "text": text})
        resp.raise_for_status()
        return resp.json()["result"]["message_id"]
    except httpx.HTTPError as exc:
        print(f"telegram sendMessage failed: {exc}", flush=True)
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
        {"command": "start", "description": "Активировать бота"},
        {"command": "kanban", "description": "Показать канбан-доску задач"},
        {"command": "week", "description": "Сводка по ежедневнику за неделю"},
        {"command": "checkin", "description": "Повторить текущий вопрос ежедневника"},
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


async def answer_callback_query(callback_query_id: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    try:
        resp = await get_client().post(url, json={"callback_query_id": callback_query_id})
        resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        print(f"telegram answerCallbackQuery failed: {exc}", flush=True)
        return False
