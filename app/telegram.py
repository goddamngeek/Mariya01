import asyncio

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


def _keyboard(buttons: list[tuple[str, str]], row_width: int) -> list[list[dict]]:
    """Buttons laid out row_width per row. One per row reads best for a list
    of choices of differing length (books, tasks); two per row keeps a fixed
    set of short verbs on one screen without scrolling."""
    cells = [{"text": label, "callback_data": data} for label, data in buttons]
    return [cells[i:i + row_width] for i in range(0, len(cells), row_width)]


async def _send(
    chat_id: int | str, text: str, parse_mode: str | None = None,
    buttons: list[tuple[str, str]] | None = None, log: bool = True,
    row_width: int = 1,
) -> int | None:
    """The one sendMessage call — one attempt, no retries here; callers
    decide what happens on failure. Returns the sent message_id, or None.

    log=False keeps the message out of chat_messages_log, and that matters
    for exactly one caller: posts to the public channel. /clear deletes
    everything the log holds for any chat that isn't a registered person's,
    so a logged channel post would eventually be wiped along with the
    chat — taking the archive with it."""
    payload: dict = {"chat_id": chat_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    if buttons:
        payload["reply_markup"] = {"inline_keyboard": _keyboard(buttons, row_width)}
    try:
        resp = await get_client().post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json=payload,
        )
        resp.raise_for_status()
        message_id = resp.json()["result"]["message_id"]
        if log:
            await _log_sent(chat_id, message_id)
        return message_id
    except httpx.HTTPError as exc:
        print(f"telegram sendMessage failed: {exc}", flush=True)
        return None


async def send_message(
    chat_id: int | str, text: str, parse_mode: str | None = None, log: bool = True,
) -> bool:
    """Whether it went out — for callers with no use for the message id."""
    return await _send(chat_id, text, parse_mode, log=log) is not None


async def send_message_get_id(chat_id: int | str, text: str, parse_mode: str | None = None) -> int | None:
    """The sent message_id (or None on failure) — for callers that need to
    act on this specific message later, above all app/threads.py, which
    tracks it so the whole conversation can be cleared at once."""
    return await _send(chat_id, text, parse_mode)


async def send_message_with_buttons(
    chat_id: int | str, text: str, buttons: list[tuple[str, str]], parse_mode: str | None = None,
    row_width: int = 1,
) -> int | None:
    """Same, plus an inline keyboard — buttons is [(label, callback_data),
    ...], laid out row_width per row."""
    return await _send(chat_id, text, parse_mode, buttons=buttons, row_width=row_width)


async def edit_message(
    chat_id: int | str, message_id: int, text: str,
    buttons: list[tuple[str, str]] | None = None, parse_mode: str | None = None,
    row_width: int = 1,
) -> bool:
    """Replace a message's text and buttons in place. Lets a one-at-a-time
    flow (the inbox triage) flip through cards inside a single message
    instead of leaving a wall of answered ones behind."""
    payload: dict = {"chat_id": chat_id, "message_id": message_id, "text": text}
    if parse_mode:
        payload["parse_mode"] = parse_mode
    payload["reply_markup"] = {"inline_keyboard": _keyboard(buttons, row_width) if buttons else []}
    try:
        resp = await get_client().post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText", json=payload,
        )
        resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        print(f"telegram editMessageText failed: {exc}", flush=True)
        return False


# Телеграм отдаёт боту файлы не больше 20 МБ, но «My Clippings.txt» — это
# десятки килобайт даже за годы чтения. Всё, что сильно больше, к делу не
# относится и качать незачем.
MAX_DOCUMENT_BYTES = 2 * 1024 * 1024


async def download_document(file_id: str) -> str | None:
    """Содержимое присланного документа как текст, или None. Двухшаговый
    путь: getFile отдаёт временный путь, файл забирается по нему."""
    try:
        info = await get_client().get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getFile",
            params={"file_id": file_id},
        )
        info.raise_for_status()
        result = info.json().get("result") or {}
        path = result.get("file_path")
        if not path:
            return None
        if (result.get("file_size") or 0) > MAX_DOCUMENT_BYTES:
            print(f"document too large to fetch: {result.get('file_size')} bytes", flush=True)
            return None

        resp = await get_client().get(
            f"https://api.telegram.org/file/bot{TELEGRAM_BOT_TOKEN}/{path}"
        )
        resp.raise_for_status()
        # utf-8-sig: файл пишет устройство, и BOM в начале вполне вероятен.
        return resp.content.decode("utf-8-sig", errors="replace")
    except httpx.HTTPError as exc:
        print(f"telegram getFile/download failed: {exc}", flush=True)
        return None


async def clear_reply_markup(chat_id: int | str, message_id: int) -> bool:
    """Strip the inline keyboard off an already-sent message, leaving its
    text alone — used right after a button is pressed so the same list
    can't be answered twice (see app/service.py's book-list handlers)."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageReplyMarkup"
    try:
        resp = await get_client().post(
            url, json={"chat_id": chat_id, "message_id": message_id, "reply_markup": {"inline_keyboard": []}},
        )
        resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        print(f"telegram editMessageReplyMarkup failed: {exc}", flush=True)
        return False


# Telegram does NOT send reaction updates unless they're explicitly listed
# in the webhook's allowed_updates — "message_reaction" is one of the few
# update types excluded from the default set, so without this the reaction
# shortcut for dismissing a thread (see app/service.py's
# handle_message_reaction) would silently never fire.
ALLOWED_UPDATES = ["message", "edited_message", "callback_query", "message_reaction"]


async def ensure_webhook_allowed_updates() -> None:
    """Re-register the EXISTING webhook URL with our allowed_updates list.
    Reads the current URL from getWebhookInfo rather than hardcoding it, so
    this stays correct across deploys/URL changes; a no-op if no webhook is
    set yet. Idempotent — safe to run on every startup."""
    try:
        info_resp = await get_client().get(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/getWebhookInfo",
        )
        info_resp.raise_for_status()
        result = info_resp.json().get("result") or {}
        url = result.get("url")
        if not url:
            print("ensure_webhook_allowed_updates: no webhook set, skipping", flush=True)
            return
        if set(result.get("allowed_updates") or []) == set(ALLOWED_UPDATES):
            return
        set_resp = await get_client().post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setWebhook",
            json={"url": url, "allowed_updates": ALLOWED_UPDATES},
        )
        set_resp.raise_for_status()
        print(f"webhook allowed_updates set to {ALLOWED_UPDATES}", flush=True)
    except httpx.HTTPError as exc:
        print(f"ensure_webhook_allowed_updates failed (non-fatal): {exc}", flush=True)


async def set_bot_commands() -> None:
    """Registers commands in Telegram's own native bot command menu (the "/"
    menu button next to the message input, built into every Telegram chat
    with a bot) — this is Telegram's built-in mechanism, not a bot-specific
    workaround like a custom reply keyboard. Global across all chats with
    this bot (not per-user), so this only needs calling once at startup;
    re-calling on every restart is harmless (pure idempotent registration,
    no message sent to anyone)."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/setMyCommands"
    # Порядок и префиксы — единственная группировка, доступная в родном
    # меню Telegram: setMyCommands принимает плоский список, без
    # разделителей, заголовков и подменю. Поэтому группа выносится в текст
    # описания, а команды стоят подряд внутри своей группы — по убыванию
    # того, как часто их жмут.
    commands = [
        # /start сам по себе работает (см. app/main.py) — просто не показывается в
        # меню: оба живых пользователя давно зарегистрированы, а незнакомому
        # чату Telegram и так предлагает /start сам.
        {"command": "today", "description": "Ежедневник · Что записано за сегодня"},
        {"command": "week", "description": "Ежедневник · Сводка за неделю"},
        {"command": "checkin", "description": "Ежедневник · Повторить текущий вопрос"},

        {"command": "reading", "description": "Книги · Что я сейчас читаю"},
        {"command": "quote", "description": "Книги · Добавить интересный момент"},
        {"command": "addbook", "description": "Книги · Добавить новую"},
        {"command": "finished", "description": "Книги · Прочитанные"},

        {"command": "task", "description": "Задачи · Добавить в инбокс"},
        {"command": "plan", "description": "Задачи · Что на сегодня"},
        {"command": "inbox", "description": "Задачи · Разобрать новые"},
        {"command": "kanban", "description": "Задачи · Доска целиком"},

        {"command": "links", "description": "Ссылки · Сохранённые"},
        {"command": "addlink", "description": "Ссылки · Добавить"},

        {"command": "thought", "description": "Мысль дня из «Круга чтения»"},
        {"command": "clear", "description": "Очистить историю чата"},
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


# Telegram rate-limits a bot at roughly 30 calls a second and answers 429
# past that, so a clear-out goes a few at a time rather than all at once.
_MAX_PARALLEL_DELETES = 8


async def delete_messages(entries: list[tuple[int | str, int]]) -> None:
    """Delete many messages — (chat_id, message_id) pairs. One at a time
    this was a full round trip each: clearing a chat holding a few hundred
    logged messages spent minutes doing nothing but waiting."""
    semaphore = asyncio.Semaphore(_MAX_PARALLEL_DELETES)

    async def one(chat_id: int | str, message_id: int) -> None:
        async with semaphore:
            await delete_message(chat_id, message_id)

    await asyncio.gather(*(one(chat_id, message_id) for chat_id, message_id in entries))


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
