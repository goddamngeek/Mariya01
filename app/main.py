import os
import traceback
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Header, Request, Response

from app.callbacks import handle_press
from app.router import handle_incoming
from app.config import MAX_REGISTERED_USERS, TELEGRAM_WEBHOOK_SECRET, TIMEZONE
from app.db import (
    close_pool,
    count_registered,
    init_db,
    is_registered,
    journal_message,
    log_chat_message,
    register_user,
)
from app.firefly_client import close_client as close_firefly_client
from app.trilium_client import close_client as close_trilium_client
from app import background, errors
from app.scheduler import scheduler, start_scheduler
from app.service import (
    handle_clippings_file,
    handle_message_edit,
    handle_message_reaction,
)
from app.sync import router as sync_router
from app.channel import send_message
from app.telegram import (
    close_client as close_telegram_client,
    download_document,
    ensure_webhook_allowed_updates,
    press_from_update,
    set_bot_commands,
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    await init_db()
    await set_bot_commands()
    await ensure_webhook_allowed_updates()
    await start_scheduler()
    yield
    scheduler.shutdown(wait=False)
    await close_telegram_client()
    await close_trilium_client()
    await close_firefly_client()
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


async def import_clippings_document(chat_id: int, document: dict) -> None:
    """Любой присланный документ пробуем прочитать как файл выделений.
    Отдельной команды нет намеренно: файл с читалки узнаётся по
    содержимому, и просить человека помнить команду ради этого незачем."""
    raw = await download_document(document.get("file_id", ""))
    if raw is None:
        await send_message(chat_id, "Не смог скачать файл.")
        return
    await handle_clippings_file(chat_id, raw)


@app.post("/webhook/telegram")
async def telegram_webhook(
    request: Request,
    x_telegram_bot_api_secret_token: str = Header(default=""),
):
    # Телеграм подписывает каждый запрос этим заголовком (см.
    # ensure_webhook_allowed_updates). Без проверки сюда мог постучаться
    # кто угодно: адрес бота не секрет, а дальше всё решает chat_id ИЗ
    # САМОГО ЗАПРОСА — то есть подделать сообщение от любого из двоих было
    # делом одного curl. Отвечаем 403, а не 200: подделке незачем знать,
    # что её приняли, а настоящий телеграм этот путь никогда не проходит.
    if TELEGRAM_WEBHOOK_SECRET and x_telegram_bot_api_secret_token != TELEGRAM_WEBHOOK_SECRET:
        print("webhook: отклонён запрос без верного секрета", flush=True)
        return Response(status_code=403)

    update = await request.json()

    if "callback_query" in update:
        # Телеграмный словарь разбирается здесь и дальше не идёт: за ним
        # живёт нейтральное нажатие (app/press.py).
        await handle_press(press_from_update(update["callback_query"]))
        return {"ok": True}

    if "message_reaction" in update:
        # Putting a reaction on any message of an open /reading or
        # /finished thread dismisses that whole thread immediately (see
        # app/service.py's handle_message_reaction). Requires
        # "message_reaction" in the webhook's allowed_updates — Telegram
        # excludes it from the default set, see ensure_webhook_allowed_updates.
        reaction = update["message_reaction"]
        reaction_chat_id = (reaction.get("chat") or {}).get("id")
        reaction_message_id = reaction.get("message_id")
        if reaction_chat_id is not None and reaction_message_id is not None:
            if await is_registered(reaction_chat_id):
                await handle_message_reaction(reaction_chat_id, reaction_message_id)
        return {"ok": True}

    if "edited_message" in update:
        # "я поторопился, дай поправлю" — retroactively patches the one
        # ежедневник cell that message originally answered, if any (see
        # app/service.py's handle_message_edit); silently a no-op for
        # anything else (untracked messages, activity/quote replies).
        edited = update["edited_message"]
        edited_chat_id = (edited.get("chat") or {}).get("id")
        edited_message_id = edited.get("message_id")
        edited_text = edited.get("text")
        if edited_chat_id is not None and edited_message_id is not None and edited_text is not None:
            if await is_registered(edited_chat_id):
                await handle_message_edit(edited_chat_id, edited_message_id, edited_text)
        return {"ok": True}

    message = update.get("message")
    text = message.get("text") if message else None
    document = message.get("document") if message else None
    chat = message.get("chat") if message else None
    chat_id = chat.get("id") if chat else None
    message_id = message.get("message_id") if message else None

    if chat_id is None or (text is None and document is None):
        return {"ok": True}

    if message_id is not None:
        # Logged unconditionally, before /start or registration checks —
        # the nightly cleanup (see scheduler.py's clear_chat_history) needs
        # every real incoming message_id, not just ones the bot goes on to
        # act on. Best-effort: a logging failure here must not break the
        # actual webhook handling.
        try:
            await log_chat_message(chat_id, message_id)
        except Exception as exc:
            print(f"log_chat_message failed (non-fatal): {exc!r}", flush=True)

    # Реплика человека — в тот же журнал, куда telegram.py кладёт ответы бота
    # (chat_journal), иначе в истории останется монолог. Документ текстом не
    # опишешь, кладём имя файла: важно, что в этот момент в разговоре
    # что-то произошло. Best-effort, как и лог выше.
    try:
        await journal_message(
            chat_id, "user",
            text if text is not None else f"[файл: {(document or {}).get('file_name', 'без имени')}]",
            telegram_message_id=message_id,
        )
    except Exception as exc:
        print(f"journal_message failed (non-fatal): {exc!r}", flush=True)

    if text is not None and text.strip() == "/start":
        await handle_start(chat_id)
        return {"ok": True}

    if not await is_registered(chat_id):
        return {"ok": True}

    if document is not None:
        # «My Clippings.txt» с читалки — выделения из книг (см.
        # app/clippings.py). Скачивание и запись в Trilium уходят в фон:
        # книг в файле может быть много, а Telegram ждёт ответа минуту.
        background.spawn(import_clippings_document(chat_id, document), "clippings")
        return {"ok": True}

    # Телеграмная форма ответа-реплая разбирается здесь; дальше идёт уже
    # нейтральный вызов (см. app/router.py).
    reply_to = message.get("reply_to_message") or {}
    await handle_incoming(
        chat_id, text, message_id,
        reply_to_text=reply_to.get("text"),
        reply_to_message_id=reply_to.get("message_id"),
    )
    return {"ok": True}


# Хеш коммита, если платформа его прокидывает, и время старта процесса.
# started_at меняется на каждом деплое даже без переменных окружения —
# этого достаточно, чтобы отличить новую сборку от старой. Без такого
# маркера отладка превращалась в гадание: /health отвечает одинаково на
# обеих, и можно двадцать раз подряд щупать старую версию, ища баг в уже
# исправленном коде.
_REV = next(
    (os.environ[k] for k in ("BUILD_REV", "GIT_COMMIT", "NF_GIT_COMMIT", "SOURCE_COMMIT")
     if os.environ.get(k)),
    "unknown",
)
_STARTED_AT = datetime.now(TIMEZONE).isoformat(timespec="seconds")


@app.get("/health")
async def health():
    return {"ok": True, "rev": _REV, "started_at": _STARTED_AT}


@app.exception_handler(Exception)
async def _unhandled(request: Request, exc: Exception):
    errors.record(f"{request.method} {request.url.path}", exc)
    traceback.print_exc()
    return Response(status_code=500)
