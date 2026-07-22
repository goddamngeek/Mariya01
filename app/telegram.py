import logging

import httpx

from app.config import TELEGRAM_BOT_TOKEN

logger = logging.getLogger("queue_bot.telegram")


async def send_message(chat_id: int | str, text: str) -> bool:
    """One attempt, no retries here — callers decide what happens on failure."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json={"chat_id": chat_id, "text": text})
            resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        logger.warning("telegram sendMessage failed: %s", exc)
        return False


async def send_message_debug(chat_id: int | str, text: str) -> tuple[bool, int | None, str]:
    """Temporary variant of send_message() that also returns the raw
    Telegram API response for debug logging. Remove once debugging is done."""
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json={"chat_id": chat_id, "text": text})
            return resp.status_code < 400, resp.status_code, resp.text
    except httpx.HTTPError as exc:
        logger.warning("telegram sendMessage failed: %s", exc)
        return False, None, str(exc)


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
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json=payload)
            resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        logger.warning("telegram sendMessage (with button) failed: %s", exc)
        return False


async def answer_callback_query(callback_query_id: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            resp = await client.post(url, json={"callback_query_id": callback_query_id})
            resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        logger.warning("telegram answerCallbackQuery failed: %s", exc)
        return False
