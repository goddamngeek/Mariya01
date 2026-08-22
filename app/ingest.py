"""Handles every real (ACTIVE) message the bot receives — see
handle_active_message(), called straight from app/service.py as soon as the
webhook receives it. Kanban reads, relay/reminders, Chinese words and
sales are all fully deterministic (or a single narrow LLM extraction at
most — see app/odysseus_client.extract_fields_via_llm) and answered
directly, never through Odysseus's agent loop. Only general journal
notes/finance logging and open-ended Q&A still go through Odysseus, since
those genuinely need search + reasoning over existing entries.

Ежедневник (daily journal check-in), activity tracking, /addbook and book
reviews are all handled entirely in app/service.py instead —
deterministically, no LLM involved at all, since each is a fixed
one-question-at-a-time sequence.
"""

import re
import traceback
from datetime import datetime, timedelta, timezone

from app.config import TIMEZONE
from app.db import (
    ack_incoming_messages,
    get_odysseus_session_id,
    set_odysseus_session_id,
    utcnow,
)
from app.odysseus_client import (
    SessionNotFoundError,
    agent_chat,
    extract_fields_via_llm,
    get_active_endpoint,
    parse_time_via_llm,
)
from app.people import NAME_TO_USER_ID, USER_NAMES
from app.prompts import INGEST_PROMPT_TEMPLATE
from app.reminder_time import parse_reminder_time
from app import humanize, threads, triggers
from app.reminders import schedule_reminder
from app.telegram import send_message
from app.trilium_client import (
    KANBAN_COLUMNS,
    add_chinese_word,
    add_kanban_card,
    get_day_summary,
    get_week_summary,
    log_reminder_to_calendar,
    log_sale,
    read_kanban_status,
)


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


# Same stem prefixes as the person-name canonicalization this used to rely
# on in Odysseus: "ОСТАП" covers all its declensions (Остапа/Остапу/...),
# while МАША needs both "МАШ" (Маша/Маши/Маше...) and the formal "МАРИ"
# (Мария/Марии...), since case endings differ between the two.
_OTHER_PERSON_PREFIXES = {
    "МАША": ("ОСТАП",),
    "ОСТАП": ("МАШ", "МАРИ"),
}
_ANONYMOUS_RE = re.compile(
    r"не говори.{0,20}(от меня|кто|что это я)|анонимн|без подписи|не подписыва",
    re.IGNORECASE,
)


def _detect_relay_target(sender_name: str, text: str) -> str:
    """The OTHER registered person if their name is mentioned anywhere in
    the text (a relay), otherwise the sender themself (a self-reminder)."""
    other_name, other_prefixes = next(
        (name, prefixes) for name, prefixes in _OTHER_PERSON_PREFIXES.items() if name != sender_name
    )
    mentions_other = any(re.search(p, text, re.IGNORECASE) for p in other_prefixes)
    return other_name if mentions_other else sender_name


def _detect_anonymous(text: str) -> bool:
    """"не говори что от меня" / "анонимно" / "не подписывай" etc — loose,
    risk-accepting keyword approach: a false positive just drops
    attribution unnecessarily, a false negative reveals a sender who asked
    not to be revealed — only worth catching the explicit, unambiguous
    phrasing."""
    return bool(_ANONYMOUS_RE.search(text))


async def _handle_relay_or_reminder(message_id: int, user_id: int, text: str) -> None:
    """A self-reminder or a message relayed to the other registered
    person — fully handled here, never through Odysseus/an LLM: the bot
    already knows the sender (user_id) and the target/anonymity are
    decided by the same lightweight keyword rules Odysseus's deterministic
    fallback used to apply after the model failed to call schedule_send
    (which it did most of the time — see git history). The one thing that
    genuinely benefited from a model was recovering a specific requested
    time from free text, so that's the only piece still allowed to ask an
    LLM — and only a narrow single-shot one (see app/reminder_time.py /
    parse_time_via_llm), never the full agent loop."""
    sender_name = USER_NAMES.get(user_id, str(user_id))
    sender_id = user_id
    target_name = _detect_relay_target(sender_name, text)
    target_id = NAME_TO_USER_ID.get(target_name, user_id)
    anonymous = _detect_anonymous(text)

    run_at, needs_llm = parse_reminder_time(text)  # already UTC-aware, or None for "now"
    if run_at is None and needs_llm:
        now_iso = datetime.now(TIMEZONE).replace(microsecond=0).isoformat()
        llm_result = await parse_time_via_llm(text, now_iso)
        if llm_result:
            try:
                naive = datetime.fromisoformat(llm_result)
                run_at = naive.replace(tzinfo=TIMEZONE).astimezone(timezone.utc)
            except ValueError:
                run_at = None
    if run_at is None:
        run_at = utcnow()

    await schedule_reminder(sender_id, target_id, text, run_at, anonymous)
    await log_reminder_to_calendar(sender_name, target_name, text, run_at.astimezone(TIMEZONE))

    # Confirm, or the request vanishes into silence: scheduling something
    # for tomorrow used to produce no reply at all, leaving no way to tell
    # whether it had been understood. (Nothing to confirm when it's due
    # right now — schedule_reminder has already delivered it.)
    if run_at > utcnow() + timedelta(seconds=30):
        when = humanize.format_when(run_at)
        if target_id == sender_id:
            await send_message(user_id, f"Напомню {when}.")
        else:
            await send_message(user_id, f"Передам {target_name.capitalize()} {when}.")
    elif target_id != sender_id:
        await send_message(user_id, f"Передал {target_name.capitalize()}.")

    await ack_incoming_messages([message_id])


NOT_UNDERSTOOD_TEXT = "Не понял детали — напиши ещё раз, что именно записать."


async def _handle_chinese_word(message_id: int, user_id: int, text: str) -> None:
    """"добавь иероглиф 你好, пиньинь ni hao, тон 3, значение привет" — the
    person_name is already known (user_id); everything else needs reading
    free text, so a narrow LLM extraction call replaces the full agent loop."""
    person_name = USER_NAMES.get(user_id, str(user_id))
    fields = await extract_fields_via_llm(
        text,
        "Пользователь добавляет китайское слово в словарь. Извлеки из его "
        "сообщения поля JSON: hieroglyph (иероглиф), pinyin (транскрипция "
        "пиньинь), tone (тон, число), translation (перевод на русский). "
        "translation и hieroglyph обязательны.",
    )
    if not fields or not fields.get("hieroglyph") or not fields.get("translation"):
        await send_message(user_id, NOT_UNDERSTOOD_TEXT)
        await ack_incoming_messages([message_id])
        return

    try:
        await add_chinese_word(
            person_name, fields["hieroglyph"], fields.get("pinyin", ""),
            str(fields.get("tone") or ""), fields["translation"],
        )
        await send_message(user_id, f"Добавил «{fields['hieroglyph']}» в словарь.")
    except Exception:
        print(f"_handle_chinese_word failed for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(user_id, TRILIUM_UNAVAILABLE_TEXT)
    await ack_incoming_messages([message_id])



async def _handle_sale(message_id: int, user_id: int, text: str) -> None:
    """"продал куртку за 3000"."""
    person_name = USER_NAMES.get(user_id, str(user_id))
    fields = await extract_fields_via_llm(
        text,
        "Пользователь сообщает о продаже. Извлеки из его сообщения поля "
        "JSON: item (что продано) и status (цена и/или дата, как есть в "
        "сообщении, произвольный текст, можно пусто). item обязателен.",
    )
    if not fields or not fields.get("item"):
        await send_message(user_id, NOT_UNDERSTOOD_TEXT)
        await ack_incoming_messages([message_id])
        return

    try:
        await log_sale(person_name, fields["item"], fields.get("status", ""))
        await send_message(user_id, f"Записал продажу «{fields['item']}».")
    except Exception:
        print(f"_handle_sale failed for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(user_id, TRILIUM_UNAVAILABLE_TEXT)
    await ack_incoming_messages([message_id])


async def _handle_kanban_add(message_id: int, user_id: int, text: str) -> None:
    """"добавь в канбан купить молоко" / "закинь задачу в работу"."""
    person_name = USER_NAMES.get(user_id, str(user_id))
    fields = await extract_fields_via_llm(
        text,
        "Пользователь добавляет задачу на канбан-доску. Извлеки из его "
        "сообщения поля JSON: title (короткое название задачи) и column "
        "(ровно одно из: 'БЭКЛОГ // BACKLOG', 'БУДУЩИЕ ЗАДАЧИ // FUTURE "
        "TASKS', 'В РАБОТЕ // IN PROGRESS', 'СДЕЛАНО // DONE' — если явно "
        "не указано, в какую колонку класть, оставь пустым). title обязателен.",
    )
    if not fields or not fields.get("title"):
        await send_message(user_id, NOT_UNDERSTOOD_TEXT)
        await ack_incoming_messages([message_id])
        return

    column = fields.get("column") or "БЭКЛОГ // BACKLOG"
    if column not in KANBAN_COLUMNS:
        column = "БЭКЛОГ // BACKLOG"

    try:
        await add_kanban_card(person_name, fields["title"], column)
        await send_message(user_id, f"Добавил «{fields['title']}» в {column}.")
    except Exception:
        print(f"_handle_kanban_add failed for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(user_id, TRILIUM_UNAVAILABLE_TEXT)
    await ack_incoming_messages([message_id])


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


ODYSSEUS_UNAVAILABLE_TEXT = "Не могу сейчас связаться с Odysseus. Попробуй чуть позже."
TRILIUM_UNAVAILABLE_TEXT = "Не могу сейчас связаться с Trilium. Попробуй чуть позже."


async def send_kanban_status(user_id: int, trigger_message_id: int | None = None) -> None:
    """Direct trigger for the /kanban command — a pure read with zero
    content judgment, so it goes straight to Trilium (see
    app/trilium_client.py), never through Odysseus/an LLM at all. Wrapped
    in try/except since this is called directly from main.py's webhook
    handler with nothing else in between — confirmed live (back when this
    went through Odysseus): an outage turned an unhandled exception here
    into a bare 500 to Telegram and total silence to the user."""
    try:
        answer = await read_kanban_status()
    except Exception:
        print(f"send_kanban_status failed for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(user_id, TRILIUM_UNAVAILABLE_TEXT)
        return

    thread_id = await threads.open_thread(user_id, threads.TTL_INFO, trigger_message_id)
    await threads.send(thread_id, user_id, answer, parse_mode="HTML")


async def send_week_summary(user_id: int, trigger_message_id: int | None = None) -> None:
    """/week — sibling of send_kanban_status above: a pure Trilium read,
    shown in a self-clearing info thread, wrapped so an outage can't turn
    into silence."""
    person_name = USER_NAMES.get(user_id, str(user_id))
    try:
        summary = await get_week_summary(person_name)
    except Exception:
        print(f"send_week_summary failed for user={user_id}:", flush=True)
        traceback.print_exc()
        summary = TRILIUM_UNAVAILABLE_TEXT

    thread_id = await threads.open_thread(user_id, threads.TTL_INFO, trigger_message_id)
    await threads.send(thread_id, user_id, summary, parse_mode="HTML")


async def send_day_summary(user_id: int, trigger_message_id: int | None = None) -> None:
    """/today — the same shape as send_week_summary, but one day's own row
    rather than the week's aggregates."""
    person_name = USER_NAMES.get(user_id, str(user_id))
    try:
        summary = await get_day_summary(person_name)
    except Exception:
        print(f"send_day_summary failed for user={user_id}:", flush=True)
        traceback.print_exc()
        summary = TRILIUM_UNAVAILABLE_TEXT

    thread_id = await threads.open_thread(user_id, threads.TTL_INFO, trigger_message_id)
    await threads.send(thread_id, user_id, summary, parse_mode="HTML")


async def handle_active_message(
    message_id: int, user_id: int, text: str, received_at: datetime,
    reply_to_text: str | None = None, telegram_message_id: int | None = None,
) -> None:
    """Answer a real question right away — called as a fire-and-forget task
    from app/service.py as soon as the webhook receives it."""
    try:
        # Precedence across every trigger in the bot is decided in one
        # place (app/triggers.py); by the time a message reaches here,
        # app/service.py has already taken the dialogue starters it owns.
        trigger = triggers.classify(text)

        if trigger == "kanban_add":
            await _handle_kanban_add(message_id, user_id, text)
            return

        # Kanban is a pure read with zero content judgment, so it's handled
        # entirely before ever reaching Odysseus/an LLM — same reasoning as
        # the ежедневник flow, see app/trilium_client.py.
        if trigger == "kanban_status":
            try:
                answer = await read_kanban_status()
            except Exception:
                print(f"handle_active_message: kanban read failed for incoming id={message_id}:", flush=True)
                traceback.print_exc()
                answer = TRILIUM_UNAVAILABLE_TEXT
            thread_id = await threads.open_thread(user_id, threads.TTL_INFO, telegram_message_id)
            await threads.send(thread_id, user_id, answer, parse_mode="HTML")
            await ack_incoming_messages([message_id])
            return

        # Same reasoning as kanban above — sender, target and anonymity are
        # all decided by lightweight keyword rules with no real judgment
        # needed, so this never reaches Odysseus except for a narrow,
        # single-shot time-parsing fallback (see _handle_relay_or_reminder).
        if trigger == "relay_or_reminder":
            await _handle_relay_or_reminder(message_id, user_id, text)
            return

        # Chinese word and sale both need a model to read free text, but
        # only ever a single narrow extraction — never the full agent loop,
        # sessions, or Odysseus's require_tool retry dance (see
        # extract_fields_via_llm).
        if trigger == "chinese_word":
            await _handle_chinese_word(message_id, user_id, text)
            return
        if trigger == "sale":
            await _handle_sale(message_id, user_id, text)
            return

        base_url, model = await get_active_endpoint()
        tagged_text = _tag_message(user_id, text, received_at, reply_to_text)
        system_prompt = _build_prompt(user_id, "активное")

        note_intent = trigger == "note_request"
        finance_intent = note_intent and triggers.is_finance(text)

        # require_tool_type is now ALSO how Odysseus scopes which tools the
        # model is even offered (see _TOOLS_BY_REQUIRE_TYPE in
        # webhook_routes.py) — not just which fallback to run. Confirmed
        # live: a message with NO matching intent ("это на какую дату ты
        # спрашиваешь") still had schedule_send available (the old
        # relevant_tools set was static, every tool offered on every single
        # call regardless of relevance) and the model called it anyway,
        # relaying a week-old test message to Masha completely unprompted.
        # So every distinct intent still routed through Odysseus gets its
        # own specific string, and the unclassified case sends an explicit
        # "general" instead of omitting the field, so general chat never
        # has schedule_send or any other write tool available at all.
        if finance_intent:
            require_tool_type = "trilium_notes_finance"
        elif note_intent:
            require_tool_type = "trilium_notes"
        else:
            require_tool_type = "general"

        result = await _chat_with_session(
            user_id, tagged_text, base_url, model, system_prompt,
            require_tool=note_intent,
            require_tool_type=require_tool_type,
        )
        # finance_intent can't be true without note_intent, so note_intent
        # alone is the whole condition.
        if note_intent and result.get("forced_fallback") is False:
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
        # Confirmed live: an Odysseus outage silently swallowed every active
        # message here — this didn't crash the webhook (200 still went back
        # to Telegram), but the person got total silence with no indication
        # anything went wrong. Not acking on purpose: if this really was a
        # transient failure, /sync/reprocess_active can still recover it.
        await send_message(user_id, ODYSSEUS_UNAVAILABLE_TEXT)
