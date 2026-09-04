"""Что делать с пришедшим текстом.

Раньше это жило прямо в теле вебхука, вперемешку с разбором телеграмного
апдейта. Разделено, чтобы второй вход мог позвать то же самое, разобрав свой
формат по-своему: сюда приходят уже готовые chat_id, текст и ссылка на
сообщение, на которое отвечают, — и ничего телеграмного.

Снаружи остались две вещи, и намеренно: /start с регистрацией (это про то,
как транспорт узнаёт человека) и присланный файл (его надо скачать, а
скачивание у каждого транспорта своё).
"""

from datetime import datetime

from app import background, parables, threads
from app.channel import send_message
from app.config import TIMEZONE
from app.db import get_open_activity_prompt, get_open_ezhednevnik_prompt
from app.ingest import send_day_summary, send_kanban_status, send_week_summary
from app.scheduler import clear_chat_history
from app.service import (
    add_link_from_command,
    add_task_from_command,
    handle_book_details_reply,
    handle_ezhednevnik_question_reply,
    process_incoming_message,
    resend_ezhednevnik_question,
    show_accounts,
    show_finished_books,
    show_inbox,
    show_links,
    show_plan,
    show_reading_status,
    start_book_add_flow,
    start_quote_flow,
)


HELP_TEXT = (
    "ЕЖЕДНЕВНИК\n"
    "/today — что уже записано за сегодня\n"
    "/week — сводка за текущую неделю\n"
    "/checkin — повторить текущий вопрос, если он ещё открыт\n"
    "\n"
    "КНИГИ\n"
    "/reading — что я сейчас читаю\n"
    "/quote — добавить интересный момент\n"
    "/addbook — добавить новую книгу\n"
    "/finished — прочитанные книги\n"
    "\n"
    "ЗАДАЧИ\n"
    "/task Текст — добавить задачу в инбокс\n"
    "/plan — что на сегодня\n"
    "/inbox — разобрать новые задачи по дням\n"
    "/kanban — канбан-доска целиком\n"
    "\n"
    "ССЫЛКИ\n"
    "/links — сохранённые\n"
    "/addlink Название https://… — добавить\n"
    "\n"
    "ПРОЧЕЕ\n"
    "/thought — мысль дня из «Круга чтения» Толстого\n"
    "/clear — очистить историю чата\n"
    "\n"
    "ПРОСТО НАПИШИ МНЕ\n"
    "— напомнить о чём-то себе или передать другому человеку\n"
    "— добавь книгу / хочу почитать X — спрошу название и автора\n"
    "— задачу в канбан\n"
    "— позанималась йогой / китайским / трейдингом — спрошу, как прошло, и запишу\n"
    "— цитата — добавлю интересный момент из книги, которую сейчас читаешь\n"
    "— что я сейчас читаю — покажу активные книги и их описание\n"
    "  (там же кнопки «Интересные моменты» и «Я дочитал»)\n"
    "— прочитанные — покажу книги, которые уже прочитал\n"
    "\n"
    "ЖЕСТЫ\n"
    "— ответить (reply) на вопрос ежедневника — вернёт тебя в тот чек-ин, "
    "даже если он был вчера\n"
    "— отредактировать свой ответ — поправит уже записанное в Trilium\n"
    "— поставить реакцию на сообщение — свернёт всю эту ветку сразу, "
    "не дожидаясь таймера"
)


async def handle_incoming(
    chat_id: int, text: str, message_id: int | None,
    reply_to_text: str | None = None, reply_to_message_id: int | None = None,
) -> None:
    # Everything below that reaches Trilium is spawned rather than awaited:
    # see app/background.py for why answering Telegram first matters.
    if text.strip() == "/kanban":
        # A guaranteed-reliable direct trigger — reads straight from
        # Trilium (see app/trilium_client.py), no LLM involved at all.
        background.spawn(send_kanban_status(chat_id, trigger_message_id=message_id), "/kanban")
        return

    if text.strip() == "/thought":
        # Reads a local data file, not Trilium, so this one is fast enough
        # to answer inline. Same day-long thread as the 9:00 job, so asking
        # twice doesn't leave two copies lying around.
        thought = parables.compose_for(datetime.now(TIMEZONE).date())
        if thought is None:
            await send_message(chat_id, "На сегодня в «Круге чтения» ничего нет.")
            return
        thread_id = await threads.open_thread(chat_id, threads.TTL_DAY, message_id)
        await threads.send(thread_id, chat_id, thought, parse_mode="HTML")
        return

    if text.strip() == "/today":
        background.spawn(send_day_summary(chat_id, message_id), "/today")
        return

    if text.strip() == "/week":
        background.spawn(send_week_summary(chat_id, message_id), "/week")
        return

    if text.strip() == "/checkin":
        if not await resend_ezhednevnik_question(chat_id):
            await send_message(chat_id, "Сейчас нет открытого чек-ина ежедневника.")
        return

    if text.strip() == "/clear":
        has_open = (
            await get_open_ezhednevnik_prompt(chat_id) is not None
            or await get_open_activity_prompt(chat_id) is not None
        )
        await clear_chat_history(only_chat_id=chat_id)
        note = "Есть открытый вопрос — почищу чат, когда ответишь на него." if has_open else "Чат очищен."
        thread_id = await threads.open_thread(chat_id, threads.TTL_INFO)
        await threads.send(thread_id, chat_id, note)
        return

    if text.strip() == "/quote":
        background.spawn(start_quote_flow(chat_id), "/quote")
        return

    if text.strip() == "/addbook":
        background.spawn(
            start_book_add_flow(chat_id, text, telegram_message_id=message_id), "/addbook",
        )
        return

    if text.strip() == "/accounts":
        background.spawn(show_accounts(chat_id, message_id), "/accounts")
        return

    if text.strip() == "/reading":
        background.spawn(
            show_reading_status(chat_id, trigger_message_id=message_id), "/reading",
        )
        return

    if text.strip() == "/finished":
        background.spawn(
            show_finished_books(chat_id, trigger_message_id=message_id), "/finished",
        )
        return

    if text.strip() == "/help":
        await send_message(chat_id, HELP_TEXT)
        return

    if text.strip().startswith("/task"):
        background.spawn(add_task_from_command(chat_id, text, message_id), "/task")
        return

    if text.strip() == "/inbox":
        background.spawn(show_inbox(chat_id, message_id), "/inbox")
        return

    if text.strip() == "/plan":
        background.spawn(show_plan(chat_id, message_id), "/plan")
        return

    if text.strip() == "/links":
        background.spawn(show_links(chat_id, message_id), "/links")
        return

    if text.strip().startswith("/addlink"):
        background.spawn(add_link_from_command(chat_id, text, message_id), "/addlink")
        return

    # A reply to a /addbook "расскажи подробнее" template — checked before
    # any other routing, since it's identified purely by which message it
    # replies to (see handle_book_details_reply). That is what tells several
    # outstanding templates apart when the clippings import has just added
    # more than one book at once. A no-match (any other reply, or none at
    # all) just falls through to normal handling below.
    if reply_to_message_id is not None and message_id is not None:
        handled = await handle_book_details_reply(
            chat_id, message_id, reply_to_message_id, text, reply_to_text,
        )
        if handled:
            return

        # Replying to a check-in question resumes that check-in, even one
        # closed days ago (see handle_ezhednevnik_question_reply) — the
        # counterpart to each slot closing the previous one.
        handled = await handle_ezhednevnik_question_reply(
            chat_id, message_id, reply_to_message_id, text, reply_to_text,
        )
        if handled:
            return

    background.spawn(
        process_incoming_message(
            chat_id, text, reply_to_text=reply_to_text, telegram_message_id=message_id,
        ),
        "incoming",
    )

    return
