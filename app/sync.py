from datetime import datetime, timedelta, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from app.config import OUTGOING_DEDUP_DAYS, SYNC_BEARER_TOKEN, TIMEZONE
from app.db import (
    ack_incoming_messages,
    claim_reminder,
    close_question,
    count_open_questions,
    dedupe_cards,
    get_due_cards,
    get_odysseus_session_id,
    get_open_question,
    get_water_reminders_for_date,
    insert_card,
    insert_outgoing_messages,
    insert_reminder,
    pick_outgoing_message,
    pull_unconfirmed_incoming,
    set_odysseus_session_id,
    utcnow,
)
from app.flashcard_session import start_review_session
from app.ingest import handle_active_message
from app.people import NAME_TO_USER_ID
from app.reminders import deliver_reminder
from app.scheduler import _send_question, send_ezhednevnik_prompts

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
    anonymous: bool = False  # deliver with no "X просил передать" attribution


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

    reminder_id = await insert_reminder(target_id, sender_id, body.message, run_at, body.anonymous)

    # Due right now (or already past) — don't make the user wait for the next
    # 60s poll tick. claim_reminder() is the atomic gate: if release_due_
    # reminders() happens to tick at the same moment and wins the claim first,
    # we just skip — it's already being sent, no double delivery.
    if run_at <= utcnow() + timedelta(seconds=30) and await claim_reminder(reminder_id):
        await deliver_reminder(reminder_id, sender_id, target_id, body.message, body.anonymous)

    return {"ok": True, "id": reminder_id}


class StartFlashcardSessionRequest(BaseModel):
    person_name: str


@router.post("/start_flashcard_session", dependencies=[Depends(require_bearer)])
async def start_flashcard_session_endpoint(body: StartFlashcardSessionRequest):
    """Called by Odysseus's start_flashcard_session tool when the user asks
    (in a normal message) to review flashcards. No start_message_id — this
    path has no bot-sent message to keep around, unlike the button-based
    daily reminder in scheduler.py."""
    user_id = _resolve_name(body.person_name)
    if user_id is None:
        raise HTTPException(400, f"Unknown person name: {body.person_name!r}")

    status = await start_review_session(user_id, start_message_id=None)
    return {"ok": True, "started": status == "started", "status": status}


class SaveFlashcardRequest(BaseModel):
    person_name: str
    front: str
    back: str


@router.post("/save_flashcard", dependencies=[Depends(require_bearer)])
async def save_flashcard_endpoint(body: SaveFlashcardRequest):
    user_id = _resolve_name(body.person_name)
    if user_id is None:
        raise HTTPException(400, f"Unknown person name: {body.person_name!r}")
    if not body.front.strip() or not body.back.strip():
        raise HTTPException(400, "front and back must both be non-empty")

    card_id = await insert_card(user_id, None, body.front.strip(), body.back.strip())
    return {"ok": True, "card_id": card_id}


class DedupeCardsRequest(BaseModel):
    person_name: str


@router.post("/dedupe_cards", dependencies=[Depends(require_bearer)])
async def dedupe_cards_endpoint(body: DedupeCardsRequest):
    """Remove duplicate flashcards (identical front+back) for a person,
    keeping the earliest of each — for cleaning up after a repeated
    save_flashcard batch (e.g. a retried card-generation request)."""
    user_id = _resolve_name(body.person_name)
    if user_id is None:
        raise HTTPException(400, f"Unknown person name: {body.person_name!r}")
    removed = await dedupe_cards(user_id)
    return {"ok": True, "removed": removed}


class ResendQuestionRequest(BaseModel):
    user_id: int


@router.post("/resend_question", dependencies=[Depends(require_bearer)])
async def resend_question(body: ResendQuestionRequest):
    """Manual on-demand trigger, reusing the exact same code path as the
    00:05 MSK daily job — for testing the passive-message flow without
    waiting for the scheduled time. Unlike send_daily_question(), this isn't
    gated by has_pending_question() (the whole point is to bypass the
    schedule) — so it must close any question already open for this user
    itself, or that older row would never get closed by anything and
    has_pending_question() would silently block every future daily question
    for them forever."""
    existing = await get_open_question(body.user_id)
    if existing is not None:
        await close_question(existing["id"])

    question = await pick_outgoing_message("question", OUTGOING_DEDUP_DAYS, body.user_id)
    if question is None:
        raise HTTPException(404, "no question available for this user right now")
    await _send_question(body.user_id, question)
    return {"ok": True, "question_id": question["id"]}


@router.get("/open_questions", dependencies=[Depends(require_bearer)])
async def open_questions():
    """Diagnostic: count of currently-open questions per known user. Should
    always be 0 or 1 — anything higher means has_pending_question() will
    silently block that user's daily question forever (see resend_question's
    docstring)."""
    return {name: await count_open_questions(uid) for name, uid in NAME_TO_USER_ID.items()}


@router.get("/due_cards", dependencies=[Depends(require_bearer)])
async def due_cards():
    """Diagnostic: count of currently-due flashcards per known user — for
    confirming whether a daily card reminder that was sent was actually
    warranted (send_daily_card_reminder/release_due_card_reminders both gate
    on this being non-empty before sending)."""
    return {name: len(await get_due_cards(uid)) for name, uid in NAME_TO_USER_ID.items()}


@router.get("/water_reminders_today", dependencies=[Depends(require_bearer)])
async def water_reminders_today():
    """Diagnostic: today's water_reminders rows per known user, regardless
    of send state — for confirming ensure_today_water_reminders() actually
    created today's slots (and which ones have fired) after the migration
    off in-memory APScheduler date-jobs."""
    today = datetime.now(TIMEZONE).date()
    return {
        name: [
            {"window": r["window_index"], "due_at": r["due_at"].isoformat(),
             "sent_at": r["sent_at"].isoformat() if r["sent_at"] else None}
            for r in await get_water_reminders_for_date(uid, today)
        ]
        for name, uid in NAME_TO_USER_ID.items()
    }


class TriggerEzhednevnikRequest(BaseModel):
    slot: Literal["am", "pm"]


@router.post("/trigger_ezhednevnik", dependencies=[Depends(require_bearer)])
async def trigger_ezhednevnik(body: TriggerEzhednevnikRequest):
    """Manual on-demand trigger, reusing the exact same code path as the
    12:00/18:00 cron ticks — for testing the check-in flow without waiting
    for the scheduled time (same reasoning as /resend_question)."""
    await send_ezhednevnik_prompts(body.slot)
    return {"ok": True}


class ReprocessActiveRequest(BaseModel):
    id: int


@router.post("/reprocess_active", dependencies=[Depends(require_bearer)])
async def reprocess_active(body: ReprocessActiveRequest):
    """Re-run handle_active_message() for a still-unconfirmed incoming row —
    for recovering an active message that failed before ever reaching
    Odysseus (e.g. an outage), since unlike passive messages it isn't retried
    by the 60s ingest poll. Looks the row up by id rather than trusting
    caller-supplied text/user_id, so this can't be used to inject arbitrary
    messages into the pipeline."""
    rows = await pull_unconfirmed_incoming()
    row = next((r for r in rows if r["id"] == body.id), None)
    if row is None:
        raise HTTPException(404, "no such unconfirmed message")
    await handle_active_message(row["id"], row["user_id"], row["text"], row["created_at"], row["reply_to_text"])
    return {"ok": True}
