"""Periodic job (see scheduler.py): forward each unconfirmed incoming
message to Odysseus so it can process/remember it. Ported from
mac_sync/sync.py's ingest mechanism — this version calls the bot's own db.py
directly instead of going over HTTP to its own /sync/* endpoints, since it
now runs inside the same process. The /sync/* endpoints stay for external/
manual use (see app/sync.py), just no longer self-called from here.
"""

from datetime import datetime

import httpx

from app.config import TIMEZONE
from app.db import (
    ack_incoming_messages,
    get_odysseus_session_id,
    pull_unconfirmed_incoming,
    set_odysseus_session_id,
)
from app.odysseus_client import SessionNotFoundError, chat, get_active_endpoint


def _tag_with_received_time(text: str, received_at: datetime) -> str:
    local_time = received_at.astimezone(TIMEZONE)
    return f"[Сообщение получено {local_time.strftime('%d.%m.%Y %H:%M')} МСК] {text}"


async def _chat_with_session(user_id: int, message: str, base_url: str, model: str) -> dict:
    session_id = await get_odysseus_session_id(user_id)
    try:
        result = await chat(message, base_url, model, session=session_id)
    except SessionNotFoundError:
        result = await chat(message, base_url, model, session=None)  # сервер сам создаст новую сессию

    new_session_id = result.get("session_id")
    if new_session_id and new_session_id != session_id:
        await set_odysseus_session_id(user_id, new_session_id)
    return result


async def ingest_incoming() -> None:
    incoming = await pull_unconfirmed_incoming()
    if not incoming:
        return

    base_url, model = await get_active_endpoint()

    confirmed_ids = []
    for message in incoming:
        try:
            tagged_text = _tag_with_received_time(message["text"], message["created_at"])
            await _chat_with_session(message["user_id"], tagged_text, base_url, model)
        except httpx.HTTPError as exc:
            print(f"ingest failed for incoming id={message['id']}: {exc!r}", flush=True)
            continue

        confirmed_ids.append(message["id"])

    await ack_incoming_messages(confirmed_ids)
