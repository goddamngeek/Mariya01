"""Мысль дня из «Круга чтения» Толстого.

Tolstoy spent the last decades of his life compiling other people's wisdom
into day-by-day calendars, and «Круг чтения» (1904–1908) is the largest of
them: for every day of the year, a theme and several thoughts gathered from
the Gospels, Confucius, Lao Tzu, the Dhammapada, the Talmud, Marcus
Aurelius, Pascal, Emerson, Ruskin and a few hundred others — 2701 thoughts
across 366 days, 500 distinct sources. Public domain (he died in 1910).

The book ships with the bot rather than living in Trilium: it never
changes, and a day's worth of it is far too much to paste into a note by
hand. Anything the two of them collect themselves goes to Trilium as usual.

A whole day as Tolstoy laid it out runs 2300 characters on average and up
to 6400 — past what anyone reads at nine in the morning. So the day is cut
into message-sized portions and sent three times (9:00 / 14:00 / 20:00, see
app/scheduler.py) instead of once: a single 900-character sample used to be
all anyone ever saw of a day, about 30% of it.

The split is deterministic — the same date always cuts the same way — so
the three sends continue each other rather than repeating, both people get
the same text, and a redeploy between two of them doesn't reshuffle what is
still to come. The seed includes the year, so the same calendar day draws
differently next time around.

Days differ in size, so a short one runs out before the evening slot and
simply sends nothing then: 95% of days have a second portion, 61% a third.
"""

import html
import json
import random
from datetime import date as _date
from pathlib import Path

_DATA_PATH = Path(__file__).with_name("krug_chteniya.json")

# Roughly a screenful. Individual thoughts run from a line (13 characters)
# to three pages (3779) — one that cannot fit a portion on its own is left
# out entirely, since including it would either blow the size the portions
# exist to keep or have to be cut mid-sentence.
#
# The cutoff is the budget itself, not something smaller: at the old 500 it
# discarded 18% of the thoughts holding 48% of the book's text, which left
# too little for three sends — the evening one would have had nothing to say
# on four days out of five.
_MESSAGE_BUDGET = 900
_MAX_THOUGHT = _MESSAGE_BUDGET

_days: dict | None = None


def _load() -> dict:
    global _days
    if _days is None:
        _days = json.loads(_DATA_PATH.read_text(encoding="utf-8"))
    return _days


def _portions(day: _date) -> list[list[tuple[str, str]]]:
    """The day's thoughts cut into message-sized portions, in a fixed order.

    Packed greedily: a thought goes into the current portion while it still
    fits, and starts the next one when it doesn't — so no thought is ever
    split across two messages."""
    entry = _load().get(f"{day.month:02d}-{day.day:02d}")
    if entry is None:
        return []

    short = [t for t in entry["thoughts"] if len(t[0]) <= _MAX_THOUGHT]
    # A day made entirely of long pieces would otherwise compose to nothing.
    pool = list(short or entry["thoughts"])
    random.Random(day.isoformat()).shuffle(pool)

    portions: list[list[tuple[str, str]]] = []
    current: list[tuple[str, str]] = []
    total = 0
    for text, source in pool:
        if current and total + len(text) > _MESSAGE_BUDGET:
            portions.append(current)
            current, total = [], 0
        current.append((text, source))
        total += len(text)
    if current:
        portions.append(current)
    return portions


def compose_for(day: _date, part: int = 0) -> str | None:
    """One portion of a day, already HTML-formatted for Telegram — part 0 is
    the morning message, 1 and 2 continue it at 14:00 and 20:00.

    None when there is nothing more to send, which is ordinary rather than an
    error: a short day runs out before the evening slot. (It also covers a
    date the book has nothing for, which shouldn't happen — it covers all
    366.)

    The theme heads the morning portion only. Repeating it on all three read
    as a machine restating itself, and the later portions are the same day
    continuing, not new subjects being announced."""
    portions = _portions(day)
    if part >= len(portions):
        return None

    entry = _load()[f"{day.month:02d}-{day.day:02d}"]
    parts = [f"<b>{html.escape(entry['theme'])}</b>"] if part == 0 else []
    for text, source in portions[part]:
        block = html.escape(text)
        if source:
            block += f"\n<i>— {html.escape(source)}</i>"
        parts.append(block)
    return "\n\n".join(parts)
