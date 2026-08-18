from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.callbacks import process_callback_query
from app.config import MAX_REGISTERED_USERS
from app.db import (
    close_pool,
    count_registered,
    get_open_ezhednevnik_prompt,
    init_db,
    is_registered,
    register_user,
)
from app.ingest import TRILIUM_UNAVAILABLE_TEXT, send_kanban_status
from app.odysseus_client import close_client as close_odysseus_client
from app.people import USER_NAMES
from app.prompts import ezhednevnik_step_text
from app.scheduler import scheduler, send_temporary_message, start_scheduler
from app.service import process_incoming_message
from app.sync import router as sync_router
from app.telegram import close_client as close_telegram_client, send_message, set_bot_commands
from app.trilium_client import get_week_summary

LINKS_TEXT = (
    "Odysseus: https://odysseus.61d1.online\n"
    "Trilium: https://trilium.61d1.online"
)

HELP_TEXT = (
    "Команды:\n"
    "/kanban — показать канбан-доску задач\n"
    "/week — сводка по ежедневнику за последние 7 дней\n"
    "/checkin — повторить текущий вопрос ежедневника, если он ещё открыт\n"
    "/links — ссылки на Odysseus и Trilium\n"
    "\n"
    "Просто напиши мне:\n"
    "— напомнить о чём-то себе или передать другому человеку\n"
    "— зафиксировать мысль или заметку (траты уходят в финансы отдельно)\n"
    "— добавить книгу, отзыв на книгу, китайское слово, продажу или задачу в канбан\n"
    "— позанималась йогой / китайским / трейдингом — спрошу, как прошло, и запишу"
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await set_bot_commands()
    await start_scheduler()
    yield
    scheduler.shutdown(wait=False)
    await close_telegram_client()
    await close_odysseus_client()
    await close_pool()


app = FastAPI(lifespan=lifespan)
app.include_router(sync_router)


async def handle_start(chat_id: int) -> None:
    if await is_registered(chat_id):
        await send_message(chat_id, "бот активен")
        return

    if await count_registered() >= MAX_REGISTERED_USERS:
        await send_message(chat_id, "мест нет")
        return

    await register_user(chat_id)
    await send_message(chat_id, "бот активен, ты зарегистрирован")


@app.post("/webhook/telegram")
async def telegram_webhook(request: Request):
    update = await request.json()

    if "callback_query" in update:
        await process_callback_query(update["callback_query"])
        return {"ok": True}

    message = update.get("message")
    text = message.get("text") if message else None
    chat = message.get("chat") if message else None
    chat_id = chat.get("id") if chat else None

    if text is None or chat_id is None:
        return {"ok": True}

    if text.strip() == "/start":
        await handle_start(chat_id)
        return {"ok": True}

    if not await is_registered(chat_id):
        return {"ok": True}

    if text.strip() == "/kanban":
        # A guaranteed-reliable direct trigger — reads straight from
        # Trilium (see app/trilium_client.py), no LLM involved at all.
        await send_kanban_status(chat_id)
        return {"ok": True}

    if text.strip() == "/week":
        person_name = USER_NAMES.get(chat_id, str(chat_id))
        try:
            summary = await get_week_summary(person_name)
        except Exception:
            summary = TRILIUM_UNAVAILABLE_TEXT
        await send_temporary_message(chat_id, summary, parse_mode="HTML")
        return {"ok": True}

    if text.strip() == "/checkin":
        open_prompt = await get_open_ezhednevnik_prompt(chat_id)
        if open_prompt is None:
            await send_message(chat_id, "Сейчас нет открытого чек-ина ежедневника.")
        else:
            # A "pool"-kind step (see EZHEDNEVNIK_STEPS) picks fresh each
            # call rather than re-showing the exact original wording, which
            # isn't stored anywhere — functionally equivalent either way.
            await send_message(chat_id, ezhednevnik_step_text(open_prompt["slot"], open_prompt["step"]))
        return {"ok": True}

    if text.strip() == "/help":
        await send_message(chat_id, HELP_TEXT)
        return {"ok": True}

    if text.strip() == "/links":
        await send_temporary_message(chat_id, LINKS_TEXT)
        return {"ok": True}

    # Telegram's native "reply" feature attaches the full replied-to message
    # here — without this, a reply like "напомни об этом Маше" loses which
    # earlier message "этом" refers to entirely, since only the new text
    # ever reached Odysseus.
    reply_to = message.get("reply_to_message") or {}
    reply_to_text = reply_to.get("text")

    await process_incoming_message(chat_id, text, reply_to_text=reply_to_text)

    return {"ok": True}


@app.get("/health")
async def health():
    return {"ok": True}
