"""Spaced-repetition scheduling for the flashcard review feature — a
simplified (binary-grade) variant of SM-2, sometimes called a Leitner
scheduler: two grades (know/don't know) instead of SM-2's four, since a
casual two-person review flow doesn't need the "hard vs easy" nuance and
it keeps the Telegram button UI to two taps.

Pure function, no I/O — deterministic by design (not delegated to the LLM),
same reasoning as require_tool's Trilium fallback: an action this well-
defined should never depend on a model getting it right.
"""

from datetime import datetime, timedelta, timezone

MIN_EASE_FACTOR = 1.3
DEFAULT_EASE_FACTOR = 2.5


def compute_next_schedule(
    ease_factor: float, interval_days: int, repetitions: int, know: bool
) -> tuple[float, int, int, datetime]:
    """Given a card's current schedule and a review grade, return the new
    (ease_factor, interval_days, repetitions, next_review_at)."""
    now = datetime.now(timezone.utc)

    if not know:
        new_repetitions = 0
        new_interval = 1
        new_ease = max(ease_factor - 0.2, MIN_EASE_FACTOR)
        return new_ease, new_interval, new_repetitions, now + timedelta(days=new_interval)

    new_repetitions = repetitions + 1
    new_ease = min(ease_factor + 0.1, DEFAULT_EASE_FACTOR)
    if new_repetitions == 1:
        new_interval = 1
    elif new_repetitions == 2:
        new_interval = 6
    else:
        new_interval = max(1, round(interval_days * new_ease))

    return new_ease, new_interval, new_repetitions, now + timedelta(days=new_interval)
