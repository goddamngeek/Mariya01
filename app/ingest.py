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


def _tag_message(
    user_id: int, text: str, received_at: datetime, reply_to_text: str | None = None,
) -> str:
    name = USER_NAMES.get(user_id, str(user_id))
    local_time = received_at.astimezone(TIMEZONE)
    tag = f"[{name} {local_time.strftime('%d.%m.%Y %H:%M')} МСК]"
    if reply_to_text:
        # Telegram's native "reply" feature — without this, a reply like
        # "напомни об этом Маше" loses which earlier message "этом" refers
        # to entirely, since only the new text ever reached Odysseus.
        quoted = reply_to_text.strip().replace("\n", " ")[:300]
        return f'{tag} (в ответ на сообщение: "{quoted}") {text}'
    return f"{tag} {text}"


def _build_prompt(user_id: int, kind: str) -> str:
    name = USER_NAMES.get(user_id, str(user_id))
    return INGEST_PROMPT_TEMPLATE.replace("__NAME__", name).replace("__KIND__", kind)


_RELAY_OR_REMINDER_KEYWORDS = ("передай", "передать", "скажи", "напомни")


def _looks_like_relay_or_reminder(text: str) -> bool:
    """Lightweight heuristic gate for require_tool on ACTIVE messages —
    unlike passive messages (always require trilium_notes), active covers
    general Q&A too, so this can't be unconditional. Confirmed live: a real
    relay request ("передай Остапу...") silently failed to reach him because
    the model's schedule_send attempt came out malformed in a different,
    unparseable way each time. A false positive here just costs an unneeded
    retry/fallback message; a false negative silently drops a real relay —
    the keyword check is intentionally loose in the risk-accepting direction."""
    lowered = text.lower()
    return any(kw in lowered for kw in _RELAY_OR_REMINDER_KEYWORDS)


def _looks_like_card_generation(text: str) -> bool:
    """Same reasoning as _looks_like_relay_or_reminder, but for save_flashcard
    generation — confirmed live the model can skip the required trilium_notes
    search step entirely and just claim "no access" to the note instead of
    trying. There's no deterministic fallback for this (require_tool_type=
    "none" — see webhook_routes.py), but the corrective retry alone still
    gives it one more real chance to actually search. Checked BEFORE
    _looks_like_start_review (below) since "сделай карточки для повторения
    из заметки X" contains both "карточ" and "повтор" — generation-specific
    words (сделай/создай/заметк) win the ambiguity."""
    lowered = text.lower()
    return "карточ" in lowered and any(kw in lowered for kw in ("сделай", "создай", "заметк"))


def _looks_like_start_review(text: str) -> bool:
    """"го повторим карточки" / "хочу повторить карточки" / "давай карточки"
    — start a review session. Unlike card generation this IS fully
    deterministic (start_review_session is a pure trigger, no content
    judgment needed) and so CAN have a real fallback. Confirmed live: asked
    to start review, the model just narrated a fake session ("Давай начнём
    с первой карточки...") with zero tool calls — the user never got a real
    card message with buttons, just a hallucinated conversation."""
    lowered = text.lower()
    return "карточ" in lowered and "повтор" in lowered


async def _chat_with_session(
    user_id: int, message: str, base_url: str, model: str, system_prompt: str,
    require_tool: bool = False, require_tool_type: str | None = None,
) -> dict:
    session_id = await get_odysseus_session_id(user_id)
    try:
        result = await agent_chat(
            message, base_url, model, session=session_id,
            system_prompt=system_prompt, require_tool=require_tool,
            require_tool_type=require_tool_type,
        )
    except SessionNotFoundError:
        result = await agent_chat(
            message, base_url, model, session=None,
            system_prompt=system_prompt, require_tool=require_tool,
            require_tool_type=require_tool_type,
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
            tagged_text = _tag_message(
                message["user_id"], message["text"], message["created_at"], message["reply_to_text"],
            )
            system_prompt = _build_prompt(message["user_id"], "пассивное")
            # Every passive message must end in a trilium_notes append per
            # INGEST_PROMPT_TEMPLATE's passive branch — never optional here,
            # unlike handle_active_message() below which covers general Q&A too.
            result = await _chat_with_session(
                message["user_id"], tagged_text, base_url, model, system_prompt,
                require_tool=True, require_tool_type="trilium_notes",
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


async def handle_active_message(
    message_id: int, user_id: int, text: str, received_at: datetime,
    reply_to_text: str | None = None,
) -> None:
    """Answer a real question right away — called as a fire-and-forget task
    from app/service.py as soon as the webhook receives it, not from the 60s
    ingest_incoming() poll, since a real answer shouldn't wait up to a minute."""
    try:
        base_url, model = await get_active_endpoint()
        tagged_text = _tag_message(user_id, text, received_at, reply_to_text)
        system_prompt = _build_prompt(user_id, "активное")

        relay_intent = _looks_like_relay_or_reminder(text)
        # Order matters: card_gen must be checked before start_review, since
        # "сделай карточки для повторения из заметки X" matches both.
        card_gen_intent = _looks_like_card_generation(text)
        start_review_intent = (not card_gen_intent) and _looks_like_start_review(text)
        if relay_intent:
            require_tool_type = "schedule_send"
        elif start_review_intent:
            require_tool_type = "start_flashcard_session"
        elif card_gen_intent:
            require_tool_type = "none"
        else:
            require_tool_type = None
        result = await _chat_with_session(
            user_id, tagged_text, base_url, model, system_prompt,
            require_tool=relay_intent or start_review_intent or card_gen_intent,
            require_tool_type=require_tool_type,
        )
        if (relay_intent or start_review_intent) and result.get("forced_fallback") is False:
            # Both the model and the deterministic fallback failed to act —
            # unlike passive messages there's no 60s retry poll for active
            # ones, so this is a real, visible loss, not just a delayed retry.
            print(f"handle_active_message: {require_tool_type} not delivered for "
                  f"incoming id={message_id} (require_tool fallback failed)", flush=True)

        answer = (result.get("response") or "").strip()
        if answer and not await send_message(user_id, answer):
            print(f"failed to deliver active answer to user={user_id}", flush=True)

        await ack_incoming_messages([message_id])
    except Exception:
        print(f"handle_active_message failed for incoming id={message_id}:", flush=True)
        traceback.print_exc()
