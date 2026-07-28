import os
import re
from datetime import time
from zoneinfo import ZoneInfo

TIMEZONE = ZoneInfo("Europe/Moscow")

PROACTIVE_WINDOW_START = time(17, 0)
PROACTIVE_WINDOW_END = time(19, 0)

WATER_REMINDER_WINDOWS = [
    (time(10, 0), time(12, 0)),
    (time(13, 0), time(15, 0)),
    (time(16, 0), time(18, 0)),
    (time(19, 0), time(22, 0)),
]
WATER_REMINDER_TEXTS = [
    "Эй, а водичку сегодня пил?",
    "Стоп. Вода. Сейчас.",
    "Мозг работает на 80% воды — пополни запасы.",
    "Кофе не считается. Налей воды.",
    "Экран подождёт, стакан воды — нет.",
    "Твой организм просит стакан H₂O.",
    "Маленькая пауза: вода, потом продолжаем.",
    "Если читаешь это — значит, пора пить.",
    "Вода — самый простой апгрейд твоего дня.",
    "Чашка чая — это не вода, напоминаю.",
    "Сделай глоток, потом дочитаешь.",
    "Уровень воды в организме: требует пополнения.",
    "Три глотка воды — и снова в бой.",
    "Не жди жажды, пей заранее.",
    "Вода рядом? Тогда самое время.",
]

DEFERRAL_DELAY_HOURS = 1
MAX_DEFERRALS = 2
OUTGOING_DEDUP_DAYS = 3
MAX_REGISTERED_USERS = 2

USER_NAMES = {
    335712401: "МАША",
    136382691: "ОСТАП",
}
NAME_TO_USER_ID = {name: user_id for user_id, name in USER_NAMES.items()}
# Grammatical gender per person, for correctly conjugating relayed
# cross-user messages ("Остап просил передать" vs "Маша просила передать").
USER_GENDER = {
    335712401: "f",  # МАША
    136382691: "m",  # ОСТАП
}


def format_reminder_message(sender_id: int, target_id: int, message: str) -> str:
    if sender_id == target_id:
        return f"Напоминание: {message}"

    sender_raw = USER_NAMES.get(sender_id, str(sender_id))
    # The model sometimes redundantly re-introduces the sender inside the
    # relayed text itself ("ОСТАП говорит, что ...") even though the bot
    # already prepends its own "<Sender> просил(а) передать, что ..." —
    # strip that if present rather than relying solely on prompting.
    message = re.sub(
        rf"^\s*{re.escape(sender_raw)}\s+(говорит|сказал[а]?|просил[а]?|пишет)\s*,?\s*(что\s+)?",
        "",
        message,
        flags=re.IGNORECASE,
    ).strip()

    sender_name = sender_raw.capitalize()
    verb = "просил" if USER_GENDER.get(sender_id) == "m" else "просила"
    return f"{sender_name} {verb} передать, что {message}"

INGEST_PROMPT_TEMPLATE = """Ты — Odysseus. Сообщение пришло не напрямую, а через Telegram-бота от человека по имени __NAME__. Оно начинается с тега [__NAME__ ДД.ММ.ГГГГ ЧЧ:ММ МСК] — это метаданные (кто и когда написал), текст после тега — слова человека.

Тип этого сообщения: __KIND__.

— Если тип "пассивное" (ответ человека на вопрос бота): твой ответ здесь никто не читает. Кратко проанализируй сообщение и вызови тул trilium_notes с action="append", person_name="__NAME__" — запиши суть своими словами (настроение, факты, планы), при этом так же логируй сырое сообщение пользователя со всеми метаданными. После вызова ответь одним коротким предложением, просто подтвердив запись.

— Если тип "активное" (реальный вопрос человека): твой ответ дойдёт до него в Telegram, отвечай по существу.
  * Если человек просит напомнить ему самому о чём-то (в конкретное время или "прямо сейчас") — вызови тул schedule_send с sender_name="__NAME__", target_name="__NAME__", run_at (ISO 8601 без смещения, время по Москве, например "2026-08-02T10:00:00") или "now" для немедленной отправки, и message — готовый текст, который будет отправлен как есть.
  * Если человек просит передать/напомнить что-то ДРУГОМУ зарегистрированному человеку — вызови тот же тул schedule_send с sender_name="__NAME__" и target_name = имя другого человека (как оно упомянуто в сообщении, например "МАША" или "ОСТАП"), остальное так же.
  * Если вопрос требует знания о прошлом — сначала вызови trilium_notes с action="search", person_name="__NAME__" (короткие ключевые слова, поиск ищет точные слова — при неудаче попробуй другой запрос). Если вопрос общий и явно не про журнал и не про напоминание — отвечай сразу. Если релевантного в журнале нет — честно скажи, что не знаешь, и предложи обсудить отдельно.

Формат вызова тула — тег сразу после тройных кавычек, без переноса строки:
```trilium_notes
{"action": "...", "person_name": "__NAME__", "query или entry": "..."}
```
```schedule_send
{"sender_name": "__NAME__", "target_name": "...", "run_at": "...", "message": "..."}
```"""

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/odysseus_queue_bot"
)
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
SYNC_BEARER_TOKEN = os.environ.get("SYNC_BEARER_TOKEN", "")

ODYSSEUS_URL = os.environ.get("ODYSSEUS_URL", "")
ODYSSEUS_TOKEN = os.environ.get("ODYSSEUS_TOKEN", "")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
