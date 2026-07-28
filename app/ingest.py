"""Periodic job (see scheduler.py): forward each unconfirmed PASSIVE incoming
message to Odysseus so it can analyze/log it. Ported from mac_sync/sync.py's
ingest mechanism — this version calls the bot's own db.py directly instead of
going over HTTP to its own /sync/* endpoints, since it now runs inside the
same process. The /sync/* endpoints stay for external/manual use (see
app/sync.py), just no longer self-called from here.

ACTIVE messages (real questions) are handled separately and immediately —
see handle_active_message(), called straight from app/service.py rather than
waiting for this poll — so this module only ever processes kind='passive'.
"""

import traceback
from datetime import datetime

from app.config import TIMEZONE
from app.db import (
    ack_incoming_messages,
    get_odysseus_session_id,
    pull_unconfirmed_incoming,
    set_odysseus_session_id,
)
from app.odysseus_client import SessionNotFoundError, agent_chat, get_active_endpoint
from app.people import USER_NAMES
from app.prompts import INGEST_PROMPT_TEMPLATE
from app.telegram import send_message


def _tag_message(user_id: int, text: str, received_at: datetime) -> str:
    name = USER_NAMES.get(user_id, str(user_id))
    local_time = received_at.astimezone(TIMEZONE)
    return f"[{name} {local_time.strftime('%d.%m.%Y %H:%M')} МСК] {text}"


def _build_prompt(user_id: int, kind: str) -> str:
    name = USER_NAMES.get(user_id, str(user_id))
    return INGEST_PROMPT_TEMPLATE.replace("__NAME__", name).replace("__KIND__", kind)


async def _chat_with_session(
    user_id: int, message: str, base_url: str, model: str, system_prompt: str,
    require_tool: bool = False,
) -> dict:
    session_id = await get_odysseus_session_id(user_id)
    try:
        result = await agent_chat(
            message, base_url, model, session=session_id,
            system_prompt=system_prompt, require_tool=require_tool,
        )
    except SessionNotFoundError:
        result = await agent_chat(
            message, base_url, model, session=None,
            system_prompt=system_prompt, require_tool=require_tool,
        )

    new_session_id = result.get("session_id")
    if new_session_id and new_session_id != session_id:
        await set_odysseus_session_id(user_id, new_session_id)
    return result


async def ingest_incoming() -> None:
    incoming = [m for m in await pull_unconfirmed_incoming() if m["kind"] == "passive"]
    if not incoming:
        return

    try:
        base_url, model = await get_active_endpoint()
    except Exception:
        # Must not crash the 60s scheduler job — e.g. get_active_endpoint()
        # raises RuntimeError if Odysseus has no enabled model configured.
        print("ingest_incoming: could not resolve active endpoint:", flush=True)
        traceback.print_exc()
        return

    confirmed_ids = []
    for message in incoming:
        try:
            tagged_text = _tag_message(message["user_id"], message["text"], message["created_at"])
            system_prompt = _build_prompt(message["user_id"], "пассивное")
            # Every passive message must end in a trilium_notes append per
            # INGEST_PROMPT_TEMPLATE's passive branch — never optional here,
            # unlike handle_active_message() below which covers general Q&A too.
            result = await _chat_with_session(
                message["user_id"], tagged_text, base_url, model, system_prompt,
                require_tool=True,
            )
            # forced_fallback is only present when the model never called a
            # tool even after correction; False means the deterministic
            # fallback in Odysseus ALSO couldn't act (unexpected message
            # shape) — nothing was actually logged despite the HTTP 200, so
            # this must not be acked, or the message is lost for good.
            if result.get("forced_fallback") is False:
                print(f"ingest: nothing was logged for incoming id={message['id']} "
                      f"(require_tool fallback failed) — will retry", flush=True)
                continue
        except Exception:
            print(f"ingest failed for incoming id={message['id']}:", flush=True)
            traceback.print_exc()
            continue

        confirmed_ids.append(message["id"])

    await ack_incoming_messages(confirmed_ids)


async def handle_active_message(message_id: int, user_id: int, text: str, received_at: datetime) -> None:
    """Answer a real question right away — called as a fire-and-forget task
    from app/service.py as soon as the webhook receives it, not from the 60s
    ingest_incoming() poll, since a real answer shouldn't wait up to a minute."""
    try:
        base_url, model = await get_active_endpoint()
        tagged_text = _tag_message(user_id, text, received_at)
        system_prompt = _build_prompt(user_id, "активное")
        result = await _chat_with_session(user_id, tagged_text, base_url, model, system_prompt)

        answer = (result.get("response") or "").strip()
        if answer and not await send_message(user_id, answer):
            print(f"failed to deliver active answer to user={user_id}", flush=True)

        await ack_incoming_messages([message_id])
    except Exception:
        print(f"handle_active_message failed for incoming id={message_id}:", flush=True)
        traceback.print_exc()
