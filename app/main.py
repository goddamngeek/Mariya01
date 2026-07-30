from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.callbacks import process_callback_query
from app.config import MAX_REGISTERED_USERS
from app.db import close_pool, count_registered, init_db, is_registered, register_user, seed_defaults
from app.flashcard_session import start_review_session
from app.odysseus_client import close_client as close_odysseus_client
from app.scheduler import scheduler, start_scheduler
from app.service import process_incoming_message
from app.sync import router as sync_router
from app.telegram import (
    REVIEW_BUTTON_TEXT,
    close_client as close_telegram_client,
    send_message,
    send_message_with_persistent_keyboard,
)

_REVIEW_STATUS_TEXT = {
    "already_open": "Сессия повторения уже идёт.",
    "no_cards": "Пока нет карточек для повторения.",
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
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
        await send_message_with_persistent_keyboard(chat_id, "бот активен")
        return

    if await count_registered() >= MAX_REGISTERED_USERS:
        await send_message(chat_id, "мест нет")
        return

    await register_user(chat_id)
    await seed_defaults({chat_id})
    await send_message_with_persistent_keyboard(chat_id, "бот активен, ты зарегистрирован")


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

    if text.strip() in (REVIEW_BUTTON_TEXT, "/cards"):
        # Direct trigger, entirely bypassing Odysseus and its NL heuristics —
        # requested as a simpler, guaranteed-reliable alternative after
        # several rounds of hardening "хочу повторить карточки"-style intent
        # detection. A pure DB lookup + button tap, nothing to misinterpret.
        status = await start_review_session(chat_id, start_message_id=None)
        reply = _REVIEW_STATUS_TEXT.get(status)
        if reply:
            await send_message(chat_id, reply)
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
