"""The one mechanism for making messages go away.

Every interaction the bot has — a command and its answer, a multi-step
dialogue, a water reminder — belongs to a *thread*: a group of message ids
(the person's and the bot's alike) that live and die together.

A thread has exactly one terminal state, and three ways into it:

  * the flow succeeded — whatever was being collected is in Trilium now, so
    the conversation has no reason to stay in the chat;
  * ttl_minutes elapsed with no activity — abandoned halfway;
  * the person reacted to any of its messages — "done with this, clear it".

Reaching that state always tears the whole thread down. A reaction is not a
delete: it just ends the thread early, and the same teardown follows as for
the other two paths.

This replaced four overlapping mechanisms (a delayed-deletion table plus a
private message_ids list on three different prompt tables), each with its own
rule for when a conversation disappeared — /quote deleted itself only when
ABANDONED, /addbook only when it SUCCEEDED, and activity/ежедневник never.
Which one applied was impossible to predict from the outside.

ежедневник is the deliberate exception: it opens no thread at all, so its
messages survive until an explicit /clear and an answer can still be edited
after the fact (see app/service.py's handle_message_edit).
"""

from app.db import (
    append_thread_message,
    close_message_thread,
    close_prompts_for_thread,
    create_message_thread,
    get_thread_by_message,
)
from app.channel import delete_message, delete_messages, send_message_get_id, send_message_with_buttons

# How long a thread survives with no activity, by what kind of thing it is.
# A water reminder is a nudge you either act on or don't; an informational
# dump (kanban board, weekly summary, links) is there to be read once; a
# dialogue needs room to answer at a human pace.
TTL_WATER = 2
TTL_INFO = 3
TTL_DIALOG = 5
# Описание книги — четыре раздела свободным текстом, это дольше, чем ответ
# на вопрос чек-ина. На пяти минутах шаблон успевал исчезнуть раньше, чем
# человек дописывал.
TTL_BOOK_DETAILS = 10
# A day exactly: the morning thought is meant to be there all day and gone
# by the time the next one arrives.
TTL_DAY = 24 * 60


async def open_thread(
    user_id: int, ttl_minutes: int = TTL_DIALOG, trigger_message_id: int | None = None,
    closing_text: str | None = None,
) -> int:
    """Start a thread. trigger_message_id is the person's own message that
    set this off (a command, or the phrase that matched a trigger) — it's
    part of the conversation and goes away with it. closing_text, if given,
    is sent as the thread's final act on the timeout path only."""
    thread_id = await create_message_thread(user_id, ttl_minutes, closing_text)
    if trigger_message_id is not None:
        await append_thread_message(thread_id, trigger_message_id)
    return thread_id


async def track(thread_id: int | None, message_id: int | None) -> None:
    """Add an already-sent/received message to a thread. Tolerates None on
    both sides: a send that failed has no id to track, and a flow whose
    thread was already dismissed has nowhere to put it — neither is worth
    interrupting the flow over."""
    if thread_id is not None and message_id is not None:
        await append_thread_message(thread_id, message_id)


async def send(
    thread_id: int | None, chat_id: int, text: str, parse_mode: str | None = None,
    buttons: list[tuple[str, str]] | None = None, row_width: int = 1,
) -> int | None:
    """Send a message and track it in one step — the normal way for any
    threaded flow to talk, so nothing is left untracked by accident."""
    if buttons:
        message_id = await send_message_with_buttons(
            chat_id, text, buttons, parse_mode=parse_mode, row_width=row_width,
        )
    else:
        message_id = await send_message_get_id(chat_id, text, parse_mode=parse_mode)
    await track(thread_id, message_id)
    return message_id


async def thread_for_message(user_id: int, message_id: int):
    """Which open thread a given message belongs to, if any — resolves both
    a button press and a reaction back to its conversation."""
    return await get_thread_by_message(user_id, message_id)


async def dismiss(thread, send_closing: bool = False) -> None:
    """Terminal state: wipe every message the thread owns and close it out,
    along with any half-finished prompt attached to it (an open prompt whose
    question was just deleted would silently swallow the person's next
    unrelated message as if it were an answer).

    send_closing is for the timeout path only — a thread that ended because
    the person walked away can have a last word (see /addbook's "Добавил
    книгу"), which is itself part of the thread and goes down with it.
    Success and reaction don't use it: success sends its own confirmation,
    and a reaction means they're already done looking."""
    if send_closing and thread["closing_text"]:
        message_id = await send_message_get_id(thread["user_id"], thread["closing_text"])
        if message_id is not None:
            await delete_message(thread["user_id"], message_id)

    await delete_messages([(thread["user_id"], mid) for mid in thread["message_ids"]])
    await close_prompts_for_thread(thread["id"])
    await close_message_thread(thread["id"])
