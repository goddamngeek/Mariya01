from datetime import datetime, timezone
from typing import Literal, Optional

from fastapi import APIRouter, Depends, Header, HTTPException
from pydantic import BaseModel

from app import errors
from app.config import SYNC_BEARER_TOKEN, TIMEZONE
from app.db import (
    ack_incoming_messages,
    get_ezhednevnik_prompts_for_user,
    get_open_prompt,
    get_recent_incoming_messages,
    get_recent_reminders,
    get_water_reminders_for_date,
    peek_logged_messages,
    pull_unconfirmed_incoming,
    utcnow,
)
from app.ingest import handle_active_message
from app.people import NAME_TO_USER_ID
from app.reminders import schedule_reminder as schedule_reminder_now
from app.scheduler import archive_done_tasks, clear_chat_history, send_ezhednevnik_prompts
from app.firefly_client import list_asset_accounts, list_categories, recent_transactions
from app.trilium_client import get_active_reading_books, get_planner_cards, inspect_note

router = APIRouter(prefix="/sync")


def require_bearer(authorization: str = Header(default="")) -> None:
    if not SYNC_BEARER_TOKEN or authorization != f"Bearer {SYNC_BEARER_TOKEN}":
        raise HTTPException(status_code=401, detail="unauthorized")


class AckRequest(BaseModel):
    ids: list[int]

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


@router.get("/planner", dependencies=[Depends(require_bearer)])
async def planner():
    """Диагностика: как планировщик видит доску — инбокс, сегодня и
    просроченное для каждого человека (см. show_plan в app/service.py).
    Чтобы проверять срезы на живых карточках, не проходя весь путь через
    Telegram."""
    from app.service import _slices
    today = datetime.now(TIMEZONE).date()
    cards = await get_planner_cards()
    result = {}
    for name, _uid in NAME_TO_USER_ID.items():
        inbox, planned, overdue = _slices(cards, name, today)
        result[name] = {
            "inbox": [c["title"] for c in inbox],
            "today": [c["title"] for c in planned],
            "overdue": [f"{c['title']} ({c['due']})" for c in overdue],
        }
    result["_всего карточек"] = len(cards)
    return result


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


@router.get("/firefly_accounts", dependencies=[Depends(require_bearer)])
async def firefly_accounts():
    """Диагностика: счета каждого человека так, как их видит бот — это и
    есть будущие кнопки при записи траты. Первое, что стоит проверить
    после выдачи токена: ошибка тут означает неверный токен или адрес, а не
    что-то в потоке диалога.

    Ошибка одного человека не роняет ответ целиком: у Маши токена может
    ещё не быть, и это не повод не показать счета Остапа."""
    result = {}
    for name, uid in NAME_TO_USER_ID.items():
        try:
            result[name] = await list_asset_accounts(uid)
        except Exception as exc:
            result[name] = {"error": f"{type(exc).__name__}: {exc}"}
    return result


@router.get("/firefly_recent", dependencies=[Depends(require_bearer)])
async def firefly_recent(user_id: int, limit: int = 10):
    """Диагностика: последние операции одного человека — чтобы видеть, что
    бот записал на самом деле, не открывая Firefly."""
    return await recent_transactions(user_id, limit)


@router.get("/firefly_categories", dependencies=[Depends(require_bearer)])
async def firefly_categories():
    """Диагностика: категории каждого человека — будущие кнопки первого
    шага. Пустой список значит, что человек их ещё не завёл."""
    result = {}
    for name, uid in NAME_TO_USER_ID.items():
        try:
            result[name] = await list_categories(uid)
        except Exception as exc:
            result[name] = {"error": f"{type(exc).__name__}: {exc}"}
    return result


@router.post("/archive_done", dependencies=[Depends(require_bearer)])
async def archive_done():
    """Ручной запуск еженедельной уборки доски — тем же кодом, что и
    понедельничный тик, чтобы не ждать понедельника ради проверки."""
    await archive_done_tasks()
    return {"ok": True}


@router.get("/errors", dependencies=[Depends(require_bearer)])
async def recent_errors(limit: int = 10):
    """Последние ошибки с трейсбеками — чтобы не деплоить ради того, чтобы
    увидеть, что именно упало."""
    return errors.recent(limit)
