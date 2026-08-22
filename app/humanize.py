"""Dates and times as a person would say them, in Russian.

Kept apart from app/people.py (who the people are) and app/prompts.py (the
fixed question texts) because both the check-in confirmations and the
reminder confirmations need the same phrasing, and neither module is the
natural home for the other's.
"""

from datetime import date as _date, datetime

from app.config import TIMEZONE

MONTHS_GENITIVE = (
    "января", "февраля", "марта", "апреля", "мая", "июня",
    "июля", "августа", "сентября", "октября", "ноября", "декабря",
)


def format_date(value: _date) -> str:
    """"21 августа"."""
    return f"{value.day} {MONTHS_GENITIVE[value.month - 1]}"


def format_when(value: datetime) -> str:
    """A moment, named relative to now where that reads more naturally:
    "сегодня в 18:00" / "завтра в 09:30" / "23 августа в 09:30". Accepts any
    aware datetime and says it in Moscow time, since that's the only clock
    either person thinks in."""
    local = value.astimezone(TIMEZONE)
    today = datetime.now(TIMEZONE).date()
    delta = (local.date() - today).days
    if delta == 0:
        day = "сегодня"
    elif delta == 1:
        day = "завтра"
    elif delta == 2:
        day = "послезавтра"
    else:
        day = format_date(local.date())
    return f"{day} в {local.strftime('%H:%M')}"
