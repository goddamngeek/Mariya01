from datetime import datetime, timedelta, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from app.config import NAME_TO_USER_ID, SYNC_BEARER_TOKEN, TIMEZONE, format_reminder_message
from app.db import (
    ack_incoming_messages,
    claim_reminder,
    get_odysseus_session_id,
    insert_outgoing_messages,
    insert_reminder,
    pull_unconfirmed_incoming,
    set_odysseus_session_id,
    utcnow,
)
from app.telegram import send_message

router = APIRouter(prefix="/sync")


def require_bearer(authorization: str = Header(default="")) -> None:
    if not SYNC_BEARER_TOKEN or authorization != f"Bearer {SYNC_BEARER_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")


class AckRequest(BaseModel):
    ids: list[int]


class PushItem(BaseModel):
    user_id: int
    category: Literal["question", "reply"]
    text: str


class PushRequest(BaseModel):
    items: list[PushItem]


class SetSessionRequest(BaseModel):
    user_id: int
    session_id: str


@router.get("/pull", dependencies=[Depends(require_bearer)])
async def pull():
    rows = await pull_unconfirmed_incoming()
    return [
        {"id": r["id"], "user_id": r["user_id"], "text": r["text"], "created_at": r["created_at"]}
        for r in rows
    ]


@router.post("/ack", dependencies=[Depends(require_bearer)])
async def ack(body: AckRequest):
    await ack_incoming_messages(body.ids)
    return {"ok": True}


@router.get("/session", dependencies=[Depends(require_bearer)])
async def get_session(user_id: int):
    return {"session_id": await get_odysseus_session_id(user_id)}


@router.post("/session", dependencies=[Depends(require_bearer)])
async def set_session(body: SetSessionRequest):
    await set_odysseus_session_id(body.user_id, body.session_id)
    return {"ok": True}


@router.post("/push", dependencies=[Depends(require_bearer)])
async def push(body: PushRequest):
    await insert_outgoing_messages(
        [(item.user_id, item.category, item.text) for item in body.items]
    )
    return {"ok": True}


class ScheduleReminderRequest(BaseModel):
    sender_name: str
    target_name: str
    message: str
    run_at: Optional[str] = None  # ISO 8601 (Moscow local time) or "now"/empty for immediate


def _resolve_name(name: str) -> Optional[int]:
    return NAME_TO_USER_ID.get(name.strip().upper())


@router.post("/schedule_reminder", dependencies=[Depends(require_bearer)])
async def schedule_reminder(body: ScheduleReminderRequest):
    sender_id = _resolve_name(body.sender_name)
    target_id = _resolve_name(body.target_name)
    if sender_id is None or target_id is None:
        raise HTTPException(400, f"Unknown person name(s): sender={body.sender_name!r} target={body.target_name!r}")

    raw = (body.run_at or "").strip().lower()
    if raw in ("", "now", "сейчас"):
        run_at = utcnow()
    else:
        try:
            parsed = datetime.fromisoformat(body.run_at.strip())
        except ValueError:
            raise HTTPException(400, "run_at must be ISO 8601 (e.g. 2026-08-02T10:00:00) or 'now'")
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=TIMEZONE)
        run_at = parsed.astimezone(timezone.utc)

    reminder_id = await insert_reminder(target_id, sender_id, body.message, run_at)

    # Due right now (or already past) — don't make the user wait for the next
    # 60s poll tick. claim_reminder() is the atomic gate: if release_due_
    # reminders() happens to tick at the same moment and wins the claim first,
    # we just skip — it's already being sent, no double delivery.
    if run_at <= utcnow() + timedelta(seconds=30) and await claim_reminder(reminder_id):
        text = format_reminder_message(sender_id, target_id, body.message)
        if not await send_message(target_id, text):
            print(f"failed to send reminder id={reminder_id} immediately", flush=True)

    return {"ok": True, "id": reminder_id}
