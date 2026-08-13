"""Regex-first parser for "when" in a relay/reminder message ("напомни в
15:00", "передай через час", "завтра напомни мне..."). Covers the common
Russian phrasings without ever touching a model — deliberately narrow, not
a general date/time NLP library. Falls back to a single narrow LLM call
(see app/odysseus_client.parse_time_via_llm) only when the text still
smells like it names a specific time we just don't have a pattern for;
otherwise (no time-like words at all) defaults straight to "now" with no
model call needed."""

import re
from datetime import datetime, timedelta, timezone

from app.config import TIMEZONE

_NOW_RE = re.compile(r"\b(прямо\s+сейчас|сейчас)\b", re.IGNORECASE)
_IN_MINUTES_RE = re.compile(r"через\s+(\d+)\s*(минут[ыу]?|мин)\b", re.IGNORECASE)
_IN_HOURS_RE = re.compile(r"через\s+(\d+)\s*(час(?:а|ов)?|ч)\b", re.IGNORECASE)
_IN_A_MINUTE_RE = re.compile(r"через\s+минут[уy]\b", re.IGNORECASE)  # bare, no number -> 1
_IN_AN_HOUR_RE = re.compile(r"через\s+час\b", re.IGNORECASE)  # bare, no number -> 1
_IN_HALF_HOUR_RE = re.compile(r"через\s+полчаса", re.IGNORECASE)
_TOMORROW_AT_RE = re.compile(r"завтра\s+в\s+(\d{1,2})[:.](\d{2})", re.IGNORECASE)
_TODAY_AT_RE = re.compile(r"сегодня\s+в\s+(\d{1,2})[:.](\d{2})", re.IGNORECASE)
_AT_HHMM_RE = re.compile(r"\bв\s+(\d{1,2})[:.](\d{2})\b")
_AT_HOUR_RE = re.compile(r"\bв\s+(\d{1,2})\s*час", re.IGNORECASE)
_TOMORROW_RE = re.compile(r"\bзавтра\b", re.IGNORECASE)

# Words that suggest a specific time WAS named but none of the patterns
# above caught it — worth the narrow LLM fallback rather than silently
# defaulting to "now".
_TIME_SIGNAL_RE = re.compile(
    r"час|минут|завтра|послезавтра|утра|утром|вечера|вечером|днём|днем|ночи|ночью|"
    r"понедельник|вторник|сред[аy]|четверг|пятниц|суббот|воскресень",
    re.IGNORECASE,
)


def _next_occurrence(now: datetime, hour: int, minute: int) -> datetime:
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    return target


def parse_reminder_time(text: str) -> tuple[datetime | None, bool]:
    """Returns (run_at_utc, needs_llm_fallback). run_at_utc=None means
    "now" — either "сейчас" was explicit, or nothing time-like was found
    at all. needs_llm_fallback=True means: nothing matched here, but the
    text still contains a time-signal word, so the caller should try the
    LLM fallback before giving up and defaulting to now."""
    now = datetime.now(TIMEZONE)

    if _NOW_RE.search(text):
        return None, False

    m = _IN_MINUTES_RE.search(text)
    if m:
        return (now + timedelta(minutes=int(m.group(1)))).astimezone(timezone.utc), False

    m = _IN_HOURS_RE.search(text)
    if m:
        return (now + timedelta(hours=int(m.group(1)))).astimezone(timezone.utc), False

    if _IN_HALF_HOUR_RE.search(text):
        return (now + timedelta(minutes=30)).astimezone(timezone.utc), False

    if _IN_AN_HOUR_RE.search(text):
        return (now + timedelta(hours=1)).astimezone(timezone.utc), False

    if _IN_A_MINUTE_RE.search(text):
        return (now + timedelta(minutes=1)).astimezone(timezone.utc), False

    m = _TOMORROW_AT_RE.search(text)
    if m:
        hour, minute = int(m.group(1)), int(m.group(2))
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            target = (now + timedelta(days=1)).replace(hour=hour, minute=minute, second=0, microsecond=0)
            return target.astimezone(timezone.utc), False

    m = _TODAY_AT_RE.search(text)
    if m:
        hour, minute = int(m.group(1)), int(m.group(2))
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return _next_occurrence(now, hour, minute).astimezone(timezone.utc), False

    m = _AT_HHMM_RE.search(text)
    if m:
        hour, minute = int(m.group(1)), int(m.group(2))
        if 0 <= hour <= 23 and 0 <= minute <= 59:
            return _next_occurrence(now, hour, minute).astimezone(timezone.utc), False

    m = _AT_HOUR_RE.search(text)
    if m:
        hour = int(m.group(1))
        if 0 <= hour <= 23:
            return _next_occurrence(now, hour, 0).astimezone(timezone.utc), False

    if _TOMORROW_RE.search(text):
        return (now + timedelta(days=1)).astimezone(timezone.utc), False

    return None, bool(_TIME_SIGNAL_RE.search(text))
