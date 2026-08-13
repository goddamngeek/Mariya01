"""Periodic job (see scheduler.py): forward each unconfirmed PASSIVE incoming
message to Odysseus so it can analyze/log it. Ported from mac_sync/sync.py's
ingest mechanism — this version calls the bot's own db.py directly instead of
going over HTTP to its own /sync/* endpoints, since it now runs inside the
same process. The /sync/* endpoints stay for external/manual use (see
app/sync.py), just no longer self-called from here.

ACTIVE messages (real questions) are handled separately and immediately —
see handle_active_message(), called straight from app/service.py rather than
waiting for this poll — so this module's ingest_incoming() only ever
processes kind='passive'. Ежедневник (daily journal check-in) replies are
handled entirely in app/service.py instead — deterministically, no LLM
involved at all — since EZHEDNEVNIK_STEPS in app/prompts.py asks one
question at a time, so every reply maps to exactly one known field.
"""

import re
import traceback
from datetime import datetime, timezone

from app.config import TIMEZONE
from app.db import (
    ack_incoming_messages,
    get_odysseus_session_id,
    pull_unconfirmed_incoming,
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
from app.reminders import schedule_reminder
from app.telegram import send_message
from app.trilium_client import (
    TriliumNoteNotFoundError,
    add_book,
    add_book_review,
    add_chinese_word,
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


_RELAY_OR_REMINDER_KEYWORDS = ("передай", "передать", "скажи", "напомни")


def _looks_like_relay_or_reminder(text: str) -> bool:
    """Lightweight heuristic gate for require_tool on ACTIVE messages —
    unlike passive messages (always require trilium_notes), active covers
    general Q&A too, so this can't be unconditional. Confirmed live: a real
    relay request ("передай Остапу...") silently failed to reach him because
    the model's schedule_send attempt came out malformed in a different,
    unparseable way each time. A false positive here just costs an unneeded
    retry/fallback message; a false negative silently drops a real relay —
    the keyword check is intentionally loose in the risk-accepting direction."""
    lowered = text.lower()
    return any(kw in lowered for kw in _RELAY_OR_REMINDER_KEYWORDS)


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


async def _handle_book_add(message_id: int, user_id: int, text: str) -> None:
    """"добавь книгу X" / "хочу почитать X"."""
    person_name = USER_NAMES.get(user_id, str(user_id))
    fields = await extract_fields_via_llm(
        text,
        "Пользователь добавляет книгу, которую собирается читать. Извлеки "
        "из его сообщения поля JSON: title (название книги), author (автор, "
        "если назван). title обязателен.",
    )
    if not fields or not fields.get("title"):
        await send_message(user_id, NOT_UNDERSTOOD_TEXT)
        await ack_incoming_messages([message_id])
        return

    try:
        await add_book(person_name, fields["title"], fields.get("author", ""))
        await send_message(user_id, f"Добавил книгу «{fields['title']}».")
    except Exception:
        print(f"_handle_book_add failed for user={user_id}:", flush=True)
        traceback.print_exc()
        await send_message(user_id, TRILIUM_UNAVAILABLE_TEXT)
    await ack_incoming_messages([message_id])


async def _handle_book_review(message_id: int, user_id: int, text: str) -> None:
    """"не понравилась книга X, потому что..." — needs an EXACT existing
    note title match (see app/trilium_client.add_book_review); no search
    fallback here, matching what the Odysseus tool already required."""
    fields = await extract_fields_via_llm(
        text,
        "Пользователь делится отзывом на книгу. Извлеки из его сообщения "
        "поля JSON: book_title (точное название книги) и review_text "
        "(мнение своими словами, без искажения сути). Оба обязательны.",
    )
    if not fields or not fields.get("book_title") or not fields.get("review_text"):
        await send_message(user_id, NOT_UNDERSTOOD_TEXT)
        await ack_incoming_messages([message_id])
        return

    try:
        await add_book_review(fields["book_title"], fields["review_text"])
        await send_message(user_id, f"Записал отзыв на «{fields['book_title']}».")
    except TriliumNoteNotFoundError:
        await send_message(user_id, f"Не нашёл книгу «{fields['book_title']}» — проверь название.")
    except Exception:
        print(f"_handle_book_review failed for user={user_id}:", flush=True)
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


_NOTE_REQUEST_KEYWORDS = ("запиши", "зафиксируй", "запомни", "занеси")


def _looks_like_note_request(text: str) -> bool:
    """"запиши в заметку...", "зафиксируй что...", "запомни это" — an ACTIVE
    request to log something to Trilium (see prompts.py's "зафиксировать/
    записать/запомнить" instruction). Unlike passive messages (ALWAYS forced
    to trilium_notes — see ingest_incoming), active messages had NO
    require_tool coverage for this at all until now: confirmed live, asked
    to log credit-card principles to his journal, the model replied "Готово,
    заметка... добавлена" with 0 native calls / 0 tool blocks — a pure
    hallucinated confirmation, and since require_tool_type was never set for
    this message shape, there was no retry and no fallback to catch it. Kept
    narrow (explicit "добавь" needs "заметк"/"журнал" alongside it) so it
    doesn't collide with other "добавь"-shaped intents (chinese word, book)."""
    lowered = text.lower()
    if any(kw in lowered for kw in _NOTE_REQUEST_KEYWORDS):
        return True
    return "добавь" in lowered and any(kw in lowered for kw in ("заметк", "журнал"))


_MONEY_KEYWORDS = (
    "потратил", "потратила", "заработал", "заработала", "купил", "купила",
    "₽", "руб", "доллар", "евро", "€", "цена", "стоит", "стоимост",
)


def _looks_like_finance(text: str) -> bool:
    """Whether a note request (see _looks_like_note_request above) is
    money-related — decides target_note ("ФИНАНСЫ // FINANCE" vs the
    person's own Журнал), per the new "Наша жизнь" Trilium architecture
    where finances get their own branch. Only ever checked alongside an
    explicit log-request verb, same as the plain journal path — a bare
    mention of money in general chat doesn't get auto-logged, matching how
    active messages have always required an explicit "запиши"/"зафиксируй"
    to log anything at all (unlike passive replies to the daily question,
    which log unconditionally)."""
    lowered = text.lower()
    return any(kw in lowered for kw in _MONEY_KEYWORDS)


_KANBAN_KEYWORDS = ("канбан", "бэклог", "в работе", "будущие задачи")


def _looks_like_kanban_status(text: str) -> bool:
    """"что в канбане?" / "что в работе?" / "покажи бэклог" — a pure,
    judgment-free read (see read_kanban_status's own docstring on why board
    membership can't be inferred from the note tree), so this is answered
    directly via app/trilium_client.py before ever reaching an LLM."""
    lowered = text.lower()
    return any(kw in lowered for kw in _KANBAN_KEYWORDS)


_CHINESE_WORD_VERBS = ("добавь", "запиши", "выучил", "выучила", "новое слово", "занеси")


def _looks_like_chinese_word(text: str) -> bool:
    """"добавь иероглиф ..." / "запиши новое китайское слово ..." — adding to
    Ostap's Chinese-vocabulary board. Requires an explicit add-type verb
    alongside the Chinese context, not just any mention of "иероглиф" — a
    plain "покажи иероглифы" (unimplemented read path) falls through to
    general Q&A instead of misfiring an add call."""
    lowered = text.lower()
    if "иероглиф" not in lowered and "китайск" not in lowered:
        return False
    return any(v in lowered for v in _CHINESE_WORD_VERBS)


def _looks_like_book_review(text: str) -> bool:
    """"отзыв на книгу ..." / "мне не понравилась книга ..." — checked
    BEFORE _looks_like_book_add since both share the "книг" context word;
    review-specific keywords win the ambiguity."""
    lowered = text.lower()
    return "книг" in lowered and any(
        kw in lowered for kw in ("отзыв", "ревью", "понравилась", "не понравилась", "мнение")
    )


def _looks_like_book_add(text: str) -> bool:
    """"добавь книгу ..." / "хочу почитать ..." / "начал читать ..." —
    adding a new book note under КНИГИ. "добавь"/"добавить" need "книг"
    alongside them (too generic alone — collides with the plain note-request
    "добавь"), but "хочу почитать X"/"начал читать X" are distinctive enough
    to stand alone — a real title usually won't itself contain "книг"."""
    lowered = text.lower()
    if "книг" in lowered and any(kw in lowered for kw in ("добавь", "добавить")):
        return True
    return any(kw in lowered for kw in ("хочу почитать", "буду читать", "начал читать", "начала читать"))


_SALE_KEYWORDS = ("продал", "продала", "продажа", "выручил", "выручила")


def _looks_like_sale(text: str) -> bool:
    """"продал куртку за 3000" — logging a sale on the ПРОДАЖИ board."""
    lowered = text.lower()
    return any(kw in lowered for kw in _SALE_KEYWORDS)


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


async def send_kanban_status(user_id: int) -> None:
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

    await send_message(user_id, answer, parse_mode="HTML")


async def ingest_incoming() -> None:
    incoming = [m for m in await pull_unconfirmed_incoming() if m["kind"] == "passive"]
    if not incoming:
        return

    try:
        base_url, model = await get_active_endpoint()
    except Exception:
        # Must not crash the 60s scheduler job — e.g. get_active_endpoint()
        # raises RuntimeError if Odysseus has no enabled model configured.
        print("ingest_incoming: could not resolve active endpoint:", flush=True)
        traceback.print_exc()
        return

    confirmed_ids = []
    for message in incoming:
        try:
            tagged_text = _tag_message(
                message["user_id"], message["text"], message["created_at"], message["reply_to_text"],
            )
            system_prompt = _build_prompt(message["user_id"], "пассивное")
            # Every passive message must end in a trilium_notes append per
            # INGEST_PROMPT_TEMPLATE's passive branch — never optional here,
            # unlike handle_active_message() below which covers general Q&A too.
            result = await _chat_with_session(
                message["user_id"], tagged_text, base_url, model, system_prompt,
                require_tool=True, require_tool_type="trilium_notes",
            )
            # forced_fallback is only present when the model never called a
            # tool even after correction; False means the deterministic
            # fallback in Odysseus ALSO couldn't act (unexpected message
            # shape) — nothing was actually logged despite the HTTP 200, so
            # this must not be acked, or the message is lost for good.
            if result.get("forced_fallback") is False:
                print(f"ingest: nothing was logged for incoming id={message['id']} "
                      f"(require_tool fallback failed) — will retry", flush=True)
                continue
        except Exception:
            print(f"ingest failed for incoming id={message['id']}:", flush=True)
            traceback.print_exc()
            continue

        confirmed_ids.append(message["id"])

    await ack_incoming_messages(confirmed_ids)


async def handle_active_message(
    message_id: int, user_id: int, text: str, received_at: datetime,
    reply_to_text: str | None = None,
) -> None:
    """Answer a real question right away — called as a fire-and-forget task
    from app/service.py as soon as the webhook receives it, not from the 60s
    ingest_incoming() poll, since a real answer shouldn't wait up to a minute."""
    try:
        # Kanban is a pure read with zero content judgment, so it's handled
        # entirely before ever reaching Odysseus/an LLM — same reasoning as
        # the ежедневник flow, see app/trilium_client.py.
        if _looks_like_kanban_status(text):
            try:
                answer = await read_kanban_status()
            except Exception:
                print(f"handle_active_message: kanban read failed for incoming id={message_id}:", flush=True)
                traceback.print_exc()
                answer = TRILIUM_UNAVAILABLE_TEXT
            await send_message(user_id, answer, parse_mode="HTML")
            await ack_incoming_messages([message_id])
            return

        # Same reasoning as kanban above — sender, target and anonymity are
        # all decided by lightweight keyword rules with no real judgment
        # needed, so this never reaches Odysseus except for a narrow,
        # single-shot time-parsing fallback (see _handle_relay_or_reminder).
        if _looks_like_relay_or_reminder(text):
            await _handle_relay_or_reminder(message_id, user_id, text)
            return

        # Chinese word / book add / book review / sale all need a model to
        # read free text, but only ever a single narrow extraction — never
        # the full agent loop, sessions, or Odysseus's require_tool retry
        # dance (see extract_fields_via_llm). Order matters: most-specific
        # first, since e.g. "книг" appears in both review and add phrasing.
        if _looks_like_chinese_word(text):
            await _handle_chinese_word(message_id, user_id, text)
            return
        if _looks_like_book_review(text):
            await _handle_book_review(message_id, user_id, text)
            return
        if _looks_like_book_add(text):
            await _handle_book_add(message_id, user_id, text)
            return
        if _looks_like_sale(text):
            await _handle_sale(message_id, user_id, text)
            return

        base_url, model = await get_active_endpoint()
        tagged_text = _tag_message(user_id, text, received_at, reply_to_text)
        system_prompt = _build_prompt(user_id, "активное")

        note_intent = _looks_like_note_request(text)
        finance_intent = note_intent and _looks_like_finance(text)

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

        forced_fallback_types = (
            note_intent, finance_intent,
        )
        result = await _chat_with_session(
            user_id, tagged_text, base_url, model, system_prompt,
            require_tool=note_intent,
            require_tool_type=require_tool_type,
        )
        if any(forced_fallback_types) and result.get("forced_fallback") is False:
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
