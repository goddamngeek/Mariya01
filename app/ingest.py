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

import traceback
from datetime import datetime

from app.config import TIMEZONE
from app.db import (
    ack_incoming_messages,
    get_odysseus_session_id,
    pull_unconfirmed_incoming,
    set_odysseus_session_id,
    utcnow,
)
from app.odysseus_client import SessionNotFoundError, agent_chat, get_active_endpoint
from app.people import USER_NAMES
from app.prompts import INGEST_PROMPT_TEMPLATE
from app.telegram import send_message
from app.trilium_client import read_kanban_status


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

    await send_message(user_id, answer)


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
            await send_message(user_id, answer)
            await ack_incoming_messages([message_id])
            return

        base_url, model = await get_active_endpoint()
        tagged_text = _tag_message(user_id, text, received_at, reply_to_text)
        system_prompt = _build_prompt(user_id, "активное")

        relay_intent = _looks_like_relay_or_reminder(text)
        # Order matters throughout: each check below only fires if nothing
        # higher up already claimed the message, most-specific first.
        claimed = relay_intent
        chinese_word_intent = (not claimed) and _looks_like_chinese_word(text)
        claimed = claimed or chinese_word_intent
        book_review_intent = (not claimed) and _looks_like_book_review(text)
        claimed = claimed or book_review_intent
        book_add_intent = (not claimed) and _looks_like_book_add(text)
        claimed = claimed or book_add_intent
        sale_intent = (not claimed) and _looks_like_sale(text)
        claimed = claimed or sale_intent
        note_intent = (not claimed) and _looks_like_note_request(text)
        finance_intent = note_intent and _looks_like_finance(text)

        # require_tool_type is now ALSO how Odysseus scopes which tools the
        # model is even offered (see _TOOLS_BY_REQUIRE_TYPE in
        # webhook_routes.py) — not just which fallback to run. Confirmed
        # live: a message with NO matching intent ("это на какую дату ты
        # спрашиваешь") still had schedule_send available (the old
        # relevant_tools set was static, every tool offered on every single
        # call regardless of relevance) and the model called it anyway,
        # relaying a week-old test message to Masha completely unprompted.
        # So every distinct intent now gets its own specific string (even
        # ones with no deterministic fallback, previously lumped under the
        # generic "none") purely so Odysseus can scope tightly — and the
        # unclassified case sends an explicit "general" instead of omitting
        # the field, so general chat never has schedule_send or any other
        # write tool available at all.
        if relay_intent:
            require_tool_type = "schedule_send"
        elif chinese_word_intent:
            require_tool_type = "add_chinese_word"
        elif book_review_intent:
            require_tool_type = "add_book_review"
        elif book_add_intent:
            require_tool_type = "add_book"
        elif sale_intent:
            require_tool_type = "log_sale"
        elif finance_intent:
            require_tool_type = "trilium_notes_finance"
        elif note_intent:
            require_tool_type = "trilium_notes"
        else:
            require_tool_type = "general"

        forced_fallback_types = (
            relay_intent, note_intent, finance_intent,
        )
        result = await _chat_with_session(
            user_id, tagged_text, base_url, model, system_prompt,
            require_tool=(
                relay_intent or chinese_word_intent or book_review_intent
                or book_add_intent or sale_intent or note_intent
            ),
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
