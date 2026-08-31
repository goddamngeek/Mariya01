import random

# Replaces the old random-pool daily question (see scheduler.py). Entirely
# deterministic, no LLM or Odysseus involved at all: each slot is a fixed
# sequence of ONE QUESTION PER FIELD (see app/service.py's step-by-step
# flow) — once every question maps 1:1 to exactly one Trilium column,
# there's nothing left for a model to interpret, so the bot writes straight
# to Trilium itself (see app/trilium_client.py) once a slot's last step is
# answered. 'am' and 'pm' share the same casual
# pool (por request — same phrasing works for a midday and an afternoon
# check-in, just asked at different times), each followed by a fixed score
# follow-up; 'evening' is the full-day retrospective, one question at a
# time instead of one combined list (confirmed live: a single message with
# 6 questions read as overwhelming/easy to only partially answer).
EZHEDNEVNIK_AM_POOL = [
    "Как дела?",
    "Как жизнь?",
    "Как поживаешь?",
    "Что нового?",
    "Как оно вообще?",
    "Ну как ты там?",
    "Чем занимаешься?",
    "Что там у тебя?",
    "Как проходит день?",
    "Как ты себя чувствуешь?",
]

EZHEDNEVNIK_SCORE_FOLLOWUP_TEXT = "Оцени это в баллах, от 0 до 100."

# Each slot: ordered list of (kind, fixed_text_or_None, field_name).
# kind="pool" picks randomly from EZHEDNEVNIK_AM_POOL at send time;
# kind="fixed" always sends fixed_text as-is. field_name is exactly the
# app.trilium_client.fill_ezhednevnik field that step's reply fills — a
# name ending in "_score" is parsed as a number (see service.py), everything
# else is stored verbatim.
EZHEDNEVNIK_STEPS = {
    "am": [
        ("pool", None, "hdif_am"),
        ("fixed", EZHEDNEVNIK_SCORE_FOLLOWUP_TEXT, "hdif_am_score"),
    ],
    "pm": [
        ("pool", None, "hdif_pm"),
        ("fixed", EZHEDNEVNIK_SCORE_FOLLOWUP_TEXT, "hdif_pm_score"),
    ],
    "evening": [
        ("fixed", "Было сегодня что-то заметное?", "event"),
        ("fixed", "Что заметил в себе?", "wdis_self"),
        ("fixed", "Что заметил на рынке?", "wdis_market"),
        ("fixed", "Что заметил в новостях?", "wdis_news"),
        ("fixed", "Чему сегодня научился?", "wdil"),
        ("fixed", "Какие ошибки сделал?", "mistakes"),
    ],
}


def ezhednevnik_step_text(slot: str, step: int) -> str:
    kind, text, _field = EZHEDNEVNIK_STEPS[slot][step]
    if kind == "pool":
        return random.choice(EZHEDNEVNIK_AM_POOL)
    return text


# User-initiated activity logging (yoga / китайский / трейдинг) — same
# one-question-at-a-time shape as EZHEDNEVNIK_STEPS, but started by the
# person's own message ("позанималась йогой") rather than a scheduled
# slot. Two steps per activity: a feedback question, then a score. No
# duration question by explicit choice — see app/trilium_client.py's
# log_activity for how a mentioned duration still gets captured.
ACTIVITY_STEPS = {
    "yoga": [
        ("fixed", "Как тебе?", "feedback"),
        ("fixed", EZHEDNEVNIK_SCORE_FOLLOWUP_TEXT, "score"),
    ],
    "chinese": [
        ("fixed", "Чему научился?", "feedback"),
        ("fixed", EZHEDNEVNIK_SCORE_FOLLOWUP_TEXT, "score"),
    ],
    "trading": [
        ("fixed", "Чему научился?", "feedback"),
        ("fixed", EZHEDNEVNIK_SCORE_FOLLOWUP_TEXT, "score"),
    ],
}


def activity_step_text(activity: str, step: int) -> str:
    _kind, text, _field = ACTIVITY_STEPS[activity][step]
    return text


# Book "interesting moment" flow (/quote or the word "цитата") — book choice
# itself is a button (see app/callbacks.py), these two are the text steps
# that follow it: send the quote, then say what you liked about it.
QUOTE_STEPS = ["Какой момент понравился?", "Что понравилось в этом моменте?"]


def quote_step_text(step: int) -> str:
    return QUOTE_STEPS[step]


# /addbook flow (or the free-text triggers in app/service.py) — two plain
# text steps (title, then author), then the note gets created and this
# template is sent as a follow-up. Its 4 lines (Об Авторе/Аннотация/Жанр/
# Похожие книги) match, in order, the 4 sections already present in every
# book note (from _ШАБЛОН_КНИГА — see app/trilium_client.BOOK_DETAIL_HEADERS)
# — a reply to THIS message (Telegram's native reply, matched by its
# message_id) is taken by paragraph POSITION, not by re-matching these
# header strings out of the reply, per explicit request.
BOOK_ADD_STEPS = ["Какое название книги?", "Кто автор книги?"]


def book_add_step_text(step: int) -> str:
    return BOOK_ADD_STEPS[step]


BOOK_DETAILS_TEMPLATE = (
    "«{title}» {author}. Расскажи мне пожалуйста подробнее:\n"
    "Об Авторе\n"
    "Аннотация\n"
    "Жанр\n"
    "Похожие книги"
)


# "Я дочитал" flow — the button under a book's description in /reading (see
# app/service.py's handle_book_finished). Two steps, rating then free text;
# together they become a review note cloned into both the book itself and
# ОТЗЫВЫ НА КНИГИ.
BOOK_REVIEW_STEPS = ["Оцените от 1 до 10?", "Расскажите подробнее?"]


def book_review_step_text(step: int) -> str:
    return BOOK_REVIEW_STEPS[step]
