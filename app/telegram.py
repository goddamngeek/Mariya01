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


async def answer_callback_query(callback_query_id: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/answerCallbackQuery"
    try:
        resp = await get_client().post(url, json={"callback_query_id": callback_query_id})
        resp.raise_for_status()
        return True
    except httpx.HTTPError as exc:
        print(f"telegram answerCallbackQuery failed: {exc}", flush=True)
        return False
