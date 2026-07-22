from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.callbacks import process_callback_query
from app.config import MAX_REGISTERED_USERS
from app.db import close_pool, count_registered, init_db, is_registered, register_user, seed_defaults
from app.scheduler import scheduler, start_scheduler
from app.service import process_incoming_message
from app.sync import router as sync_router
from app.telegram import send_message


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await start_scheduler()
    yield
    scheduler.shutdown(wait=False)
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
    await seed_defaults({chat_id})
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

    await process_incoming_message(chat_id, text)

    return {"ok": True}


@app.get("/health")
async def health():
    return {"ok": True}
