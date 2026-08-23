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
to 6400 — past what Telegram accepts in one message, and past what anyone
reads at nine in the morning. So a day is sampled down to a comfortable
size, deterministically: the same date always composes the same message, so
both people get the same text and a redeploy doesn't reshuffle it. The seed
includes the year, so the same calendar day draws differently next time
around.
"""

import html
import json
import random
from datetime import date as _date
from pathlib import Path

_DATA_PATH = Path(__file__).with_name("krug_chteniya.json")

# Roughly a screenful. Individual thoughts run from a line to three pages;
# the long ones are essays in their own right and don't belong in a morning
# message at all, so they're left out of the sampling entirely.
_MESSAGE_BUDGET = 900
_MAX_THOUGHT = 500

_days: dict | None = None


def _load() -> dict:
    global _days
    if _days is None:
        _days = json.loads(_DATA_PATH.read_text(encoding="utf-8"))
    return _days


def compose_for(day: _date) -> str | None:
    """The message for one calendar day, already HTML-formatted for
    Telegram. None if there's nothing for that date (shouldn't happen —
    the book covers all 366)."""
    entry = _load().get(f"{day.month:02d}-{day.day:02d}")
    if entry is None:
        return None

    short = [t for t in entry["thoughts"] if len(t[0]) <= _MAX_THOUGHT]
    # A day made entirely of long pieces would otherwise compose to nothing.
    pool = short or entry["thoughts"]

    rnd = random.Random(day.isoformat())
    rnd.shuffle(pool := list(pool))

    picked, total = [], 0
    for text, source in pool:
        if picked and total + len(text) > _MESSAGE_BUDGET:
            break
        picked.append((text, source))
        total += len(text)

    parts = [f"<b>{html.escape(entry['theme'])}</b>"]
    for text, source in picked:
        block = html.escape(text)
        if source:
            block += f"\n<i>— {html.escape(source)}</i>"
        parts.append(block)
    return "\n\n".join(parts)
