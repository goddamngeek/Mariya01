"""What an incoming message is asking for, decided by keywords alone.

Every one of these is the same shape of guess: does the text contain the
words that mean "the person wants X". They used to be spread across two
files — half in app/service.py, half in app/ingest.py — with the order they
were tried in encoded implicitly, as the order of `if` statements in two
different functions. That made collisions easy to introduce and impossible
to see: "читаю" (what am I reading) and "начал читать" (add a book) very
nearly ended up matching each other's phrasing, and nothing in either file
showed that they were even in competition.

So the order lives here, once, as PRECEDENCE — and classify() is the only
way anything asks. Most specific first: a phrase that satisfies two rules
belongs to the narrower one.

The names classify() returns are split across two stages. app/service.py
handles the dialogue starters (it sees every message first); anything it
doesn't recognise falls through to app/ingest.py's handle_active_message,
which handles the rest. That split is about which module owns the flow, not
about precedence — precedence is entirely this file's business.
"""

import re

# --- app/service.py's own (multi-step dialogues) ---------------------------

_ACTIVITY_VERBS = (
    "позанимал", "занимал", "делал", "сделал", "провел", "провёл",
    "отработал", "потрейдил", "затрейдил",
)
_ACTIVITY_KEYWORDS = {
    "yoga": ("йог",),
    "chinese": ("китайск",),
    "trading": ("трейдинг", "трейд"),
}


def activity_kind(text: str) -> str | None:
    """Which activity "позанималась йогой" / "потрейдил сегодня" is about,
    or None. Requires both an activity keyword AND a completion-signal verb
    — stricter than most rules here, since a false positive starts a real
    multi-step conversation rather than just costing a wasted retry."""
    lowered = text.lower()
    for activity, keywords in _ACTIVITY_KEYWORDS.items():
        if any(kw in lowered for kw in keywords) and any(v in lowered for v in _ACTIVITY_VERBS):
            return activity
    return None


def is_activity_log(text: str) -> bool:
    return activity_kind(text) is not None


def is_quote_request(text: str) -> bool:
    """Any word-form of "цитата" — цитату, цитаты, цитате — all share the
    "цитат" stem, so one substring check covers every inflection."""
    return "цитат" in text.lower()


def is_reading_status(text: str) -> bool:
    """"что я сейчас читаю". Present tense only, which is what keeps it
    clear of is_book_add's infinitives ("начал читать", "хочу почитать")."""
    return "читаю" in text.lower()


def is_finished_books(text: str) -> bool:
    """"прочитанные книги" — the "прочитанн" stem, distinct from both
    "читаю" above and the infinitives below."""
    return "прочитанн" in text.lower()


def is_book_add(text: str) -> bool:
    """"добавь книгу X" / "хочу почитать X" / "начал читать X". "добавь"
    needs "книг" beside it (alone it collides with every other add-shaped
    intent), while the infinitive phrasings are distinctive on their own."""
    lowered = text.lower()
    if "книг" in lowered and any(kw in lowered for kw in ("добавь", "добавить")):
        return True
    return any(kw in lowered for kw in ("хочу почитать", "буду читать", "начал читать", "начала читать"))


# --- app/ingest.py's own (single-shot actions) -----------------------------

_KANBAN_KEYWORDS = ("канбан", "бэклог", "в работе", "будущие задачи")
_KANBAN_ADD_VERBS = ("добавь", "добавить", "закинь", "закинуть", "создай", "создать", "занеси")


def is_kanban_add(text: str) -> bool:
    """"добавь в канбан купить молоко" / "закинь задачу...". Ranks above
    is_kanban_status, which shares the same context words — an explicit add
    verb settles the ambiguity. "задач" counts as context on its own, since
    "добавь задачу X" is natural phrasing that never names the board."""
    lowered = text.lower()
    has_kanban_context = any(kw in lowered for kw in _KANBAN_KEYWORDS) or "задач" in lowered
    if not has_kanban_context:
        return False
    return any(v in lowered for v in _KANBAN_ADD_VERBS)


_TASK_PREFIX_RE = re.compile(
    r"^\s*(?:добав(?:ь|ить)|закин(?:ь|уть)|созда(?:й|ть)|занеси|поставь)\s*"
    r"(?:(?:в|на)\s+)?(?:канбан\w*|доск\w+|бэклог\w*)?\s*"
    r"(?:(?:в|на)\s+)?(?:задач\w*|таск\w*)?\s*[:\-—,]*\s*",
    re.IGNORECASE,
)


def strip_task_prefix(text: str) -> str:
    """«добавь в канбан купить молоко» → «купить молоко».

    Раньше заголовок вытаскивала языковая модель — сетевой вызов и
    агентный цикл ради того, чтобы отрезать два слова в начале. Приставка
    тут всегда одной формы (глагол, необязательное «в канбан», необязательное
    «задачу»), так что регулярка справляется и делает это мгновенно.

    Если после среза ничего не осталось, отдаём исходный текст: пусть
    задача называется неудачно, но не пропадает."""
    stripped = _TASK_PREFIX_RE.sub("", text, count=1).strip()
    return stripped or text.strip()


def is_kanban_status(text: str) -> bool:
    """"что в канбане?" / "покажи бэклог" — a pure read, answered straight
    from Trilium without an LLM."""
    lowered = text.lower()
    return any(kw in lowered for kw in _KANBAN_KEYWORDS)


_RELAY_OR_REMINDER_KEYWORDS = ("передай", "передать", "скажи", "напомни")


def is_relay_or_reminder(text: str) -> bool:
    """"напомни мне..." / "передай Остапу...". Deliberately loose in the
    risk-accepting direction: a false positive costs an unneeded fallback,
    a false negative silently drops a real message to another person —
    which is exactly what happened live before this existed."""
    lowered = text.lower()
    return any(kw in lowered for kw in _RELAY_OR_REMINDER_KEYWORDS)

_SPEND_VERBS = ("потратил", "потратила", "купил", "купила", "оплатил",
                "оплатила", "заплатил", "заплатила")
_AMOUNT_RE = re.compile(r"(\d[\d\s]*)(?:[.,](\d{1,2}))?")


def expense_parts(text: str) -> tuple[str, str] | None:
    """«потратил 1200 на продукты» -> ("1200.00", "продукты"), или None.

    Нужен и глагол траты, и число: одного глагола мало («купил бы»), одного
    числа тем более. Пробелы внутри числа считаются разделителем тысяч —
    так его и пишут руками («1 200»), — а запятая или точка отделяют копейки.

    Описанием становится всё, что осталось после вычитания глагола и суммы,
    без ведущих предлогов. Оно же уходит в Firefly именем получателя, и
    расходный счёт с таким именем Firefly заводит себе сам."""
    lowered = text.lower()
    if not any(v in lowered for v in _SPEND_VERBS):
        return None
    match = _AMOUNT_RE.search(text)
    if match is None:
        return None

    rubles = match.group(1).replace(" ", "")
    if not rubles:
        return None
    amount = f"{int(rubles)}.{(match.group(2) or '0').ljust(2, '0')}"

    rest = (text[:match.start()] + " " + text[match.end():])
    for verb in _SPEND_VERBS:
        rest = re.sub(verb + r"\w*", " ", rest, flags=re.IGNORECASE)
    rest = re.sub(r"^[\s,.:—-]*(на|за|в|для)\b", " ", rest.strip(), flags=re.IGNORECASE)
    rest = re.sub(r"\s+", " ", rest).strip(" ,.:—-")
    return amount, rest or "Без описания"


def is_expense(text: str) -> bool:
    return expense_parts(text) is not None


# Most specific first. A message satisfying two rules belongs to whichever
# appears earlier, so anything sharing context words with a broader rule has
# to sit above it: kanban_add over kanban_status (both say "канбан"),
# book_add over note_request (both say "добавь"), and so on down to
# note_request, whose verbs are the most generic of the lot.
PRECEDENCE = (
    ("expense", is_expense),
    ("activity", is_activity_log),
    ("quote", is_quote_request),
    ("reading_status", is_reading_status),
    ("finished_books", is_finished_books),
    ("book_add", is_book_add),
    ("kanban_add", is_kanban_add),
    ("kanban_status", is_kanban_status),
    ("relay_or_reminder", is_relay_or_reminder),
)


def classify(text: str) -> str | None:
    """The name of the first rule this text matches, or None for a message
    that isn't asking for anything in particular (general Q&A)."""
    return next((name for name, matches in PRECEDENCE if matches(text)), None)

