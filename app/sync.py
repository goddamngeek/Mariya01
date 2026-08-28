from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from app.config import SYNC_BEARER_TOKEN, TIMEZONE
from app.db import (
    ack_incoming_messages,
    get_ezhednevnik_prompts_for_user,
    get_odysseus_session_id,
    get_open_prompt,
    get_recent_incoming_messages,
    get_recent_reminders,
    get_water_reminders_for_date,
    peek_logged_messages,
    pull_unconfirmed_incoming,
    set_odysseus_session_id,
    utcnow,
)
from app.ingest import handle_active_message
from app.people import NAME_TO_USER_ID
from app.reminders import schedule_reminder as schedule_reminder_now
from app.scheduler import clear_chat_history, send_ezhednevnik_prompts
from app.trilium_client import (
    BOOK_DETAIL_HEADERS,
    fill_book_details,
    get_active_reading_books,
    get_book_details,
    inspect_note,
)
from app.service import split_book_details

router = APIRouter(prefix="/sync")


def require_bearer(authorization: str = Header(default="")) -> None:
    if not SYNC_BEARER_TOKEN or authorization != f"Bearer {SYNC_BEARER_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")


class AckRequest(BaseModel):
    ids: list[int]


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

    reminder_id = await schedule_reminder_now(sender_id, target_id, body.message, run_at, body.anonymous)
    return {"ok": True, "id": reminder_id}


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


@router.get("/recent_reminders", dependencies=[Depends(require_bearer)])
async def recent_reminders():
    """Diagnostic: most recent reminders/relays per known user, newest
    first — for tracing an unexpected/duplicate delivery back to its
    real created_at/run_at/sent_at."""
    result = {}
    for name, uid in NAME_TO_USER_ID.items():
        result[name] = [
            {
                "id": r["id"], "sender_chat_id": r["sender_chat_id"],
                "message": r["message"], "run_at": r["run_at"].isoformat(),
                "created_at": r["created_at"].isoformat(),
                "sent_at": r["sent_at"].isoformat() if r["sent_at"] else None,
                "anonymous": r["anonymous"],
            }
            for r in await get_recent_reminders(uid)
        ]
    return result


@router.get("/ezhednevnik_state", dependencies=[Depends(require_bearer)])
async def ezhednevnik_state():
    """Diagnostic: last 5 ezhednevnik_prompts rows per known user, newest
    first — for confirming whether/why a trigger actually sent a message."""
    result = {}
    for name, uid in NAME_TO_USER_ID.items():
        result[name] = [
            {
                "id": r["id"], "slot": r["slot"], "sent_at": r["sent_at"].isoformat(),
                "is_open": r["is_open"], "step": r["step"], "collected": r["collected"],
            }
            for r in await get_ezhednevnik_prompts_for_user(uid)
        ]
    return result


class TriggerEzhednevnikRequest(BaseModel):
    slot: Literal["am", "pm", "evening"]


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


@router.get("/open_prompt", dependencies=[Depends(require_bearer)])
async def open_prompt():
    """Diagnostic: which dialogue, if any, each person's next message would
    be answering — the same single lookup process_incoming_message uses
    (get_open_prompt), across all five kinds rather than just one."""
    result = {}
    for name, uid in NAME_TO_USER_ID.items():
        row = await get_open_prompt(uid)
        result[name] = (
            {"kind": row["kind"], "id": row["id"], "updated_at": row["updated_at"].isoformat()}
            if row is not None else None
        )
    return result


@router.get("/chat_log", dependencies=[Depends(require_bearer)])
async def chat_log():
    """Diagnostic: newest logged (not-yet-cleared) messages — for
    confirming the nightly clear_chat_history job has something real to
    work with, without waiting for 04:00 MSK."""
    return [
        {"id": r["id"], "chat_id": r["chat_id"], "message_id": r["message_id"], "created_at": r["created_at"]}
        for r in await peek_logged_messages()
    ]


class TriggerChatClearRequest(BaseModel):
    user_id: Optional[int] = None


@router.post("/trigger_chat_clear", dependencies=[Depends(require_bearer)])
async def trigger_chat_clear(body: TriggerChatClearRequest):
    """Manual on-demand trigger, reusing the exact same code path as the
    04:00 MSK nightly job — for testing without waiting for the scheduled
    time (same reasoning as /trigger_ezhednevnik). user_id restricts it to
    a single chat; omit to clear everyone (same as the real cron call)."""
    await clear_chat_history(only_chat_id=body.user_id)
    return {"ok": True}


@router.get("/active_books", dependencies=[Depends(require_bearer)])
async def active_books():
    """Diagnostic: books get_active_reading_books() (see /quote flow in
    app/service.py) currently considers "in active reading" — for
    confirming it resolves the real КНИГИ note/readingStart/readingEnd
    labels correctly without going through the whole Telegram flow."""
    return await get_active_reading_books()


@router.get("/note", dependencies=[Depends(require_bearer)])
async def note(title: str):
    """Диагностика: что реально лежит в заметке — заголовок, лейблы и
    содержимое. Для проверки того, что бот записал."""
    return await inspect_note(title)


class RepairBookRequest(BaseModel):
    title: str


@router.post("/repair_book_sections", dependencies=[Depends(require_bearer)])
async def repair_book_sections(body: RepairBookRequest):
    """Одноразовый ремонт книги, заполненной до исправления разбора: весь
    ответ тогда уезжал в «Об Авторе» одним куском, а три остальных раздела
    оставались с заглушками шаблона. Читает этот кусок обратно, прогоняет
    через нынешний split_book_details и раскладывает как надо. Безопасно
    повторять: на уже правильной заметке заголовков внутри текста нет,
    разбор вернёт её же содержимое в первый раздел."""
    note = await inspect_note(body.title)
    _title, sections = await get_book_details(note["note_id"])
    stuck = sections.get(BOOK_DETAIL_HEADERS[0], "")
    values = split_book_details(f"{BOOK_DETAIL_HEADERS[0]}\n{stuck}")
    await fill_book_details(note["note_id"], values)
    return {"ok": True, "filled": {h: bool(v) for h, v in zip(BOOK_DETAIL_HEADERS, values)}}


@router.get("/recent_incoming", dependencies=[Depends(require_bearer)])
async def recent_incoming(user_id: int):
    """Diagnostic: newest incoming_messages rows for one user, including
    telegram_message_id/entry_date — for debugging handle_message_edit
    (app/service.py), e.g. confirming a given Telegram message actually got
    tagged the way it should have."""
    return [
        {
            "id": r["id"], "text": r["text"], "kind": r["kind"],
            "telegram_message_id": r["telegram_message_id"],
            "entry_date": r["entry_date"].isoformat() if r["entry_date"] else None,
            "created_at": r["created_at"].isoformat(),
        }
        for r in await get_recent_incoming_messages(user_id)
    ]

