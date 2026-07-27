import os
from datetime import time
from zoneinfo import ZoneInfo

TIMEZONE = ZoneInfo("Europe/Moscow")

PROACTIVE_WINDOW_START = time(17, 0)
PROACTIVE_WINDOW_END = time(19, 0)

WATER_REMINDER_WINDOWS = [
    (time(10, 0), time(12, 0)),
    (time(12, 0), time(15, 0)),
    (time(15, 0), time(18, 0)),
    (time(18, 0), time(22, 0)),
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

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/odysseus_queue_bot"
)
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
SYNC_BEARER_TOKEN = os.environ.get("SYNC_BEARER_TOKEN", "")

ODYSSEUS_URL = os.environ.get("ODYSSEUS_URL", "")
ODYSSEUS_TOKEN = os.environ.get("ODYSSEUS_TOKEN", "")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")
