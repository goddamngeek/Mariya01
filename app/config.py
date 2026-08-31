import hashlib
import os
from datetime import time
from zoneinfo import ZoneInfo

TIMEZONE = ZoneInfo("Europe/Moscow")

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

MAX_REGISTERED_USERS = 2

# Публичный канал с «мыслью дня» из «Круга чтения» (see app/parables.py).
# Не секрет — публичный юзернейм, поэтому здесь, а не в переменных окружения.
# Пустая строка выключает публикацию.
THOUGHT_CHANNEL = "@blessandbeblessed"

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/odysseus_queue_bot"
)
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
SYNC_BEARER_TOKEN = os.environ.get("SYNC_BEARER_TOKEN", "")

ODYSSEUS_URL = os.environ.get("ODYSSEUS_URL", "")
ODYSSEUS_TOKEN = os.environ.get("ODYSSEUS_TOKEN", "")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")

# Direct Trilium ETAPI access — used for the fully-deterministic writes
# (ежедневник) that don't need Odysseus/an LLM at all, so they don't have
# to pay for Odysseus's whole session/auth stack just to reach Trilium.
# Same instance and token Odysseus itself uses, reached over its own public
# Caddy-proxied subdomain rather than the internal-only docker network.
TRILIUM_URL = os.environ.get("TRILIUM_URL", "").rstrip("/")
TRILIUM_ETAPI_TOKEN = os.environ.get("TRILIUM_ETAPI_TOKEN", "")

# Firefly III — учёт денег, поднят на том же VPS, что Trilium, за своим
# поддоменом. Токен у КАЖДОГО человека свой: в Firefly два пользователя с
# полностью изолированными данными, общего представления нет — сводку по
# двоим, если понадобится, складывает бот, держа оба токена.
FIREFLY_URL = os.environ.get("FIREFLY_URL", "").rstrip("/")
FIREFLY_TOKENS = {
    "ОСТАП": os.environ.get("FIREFLY_TOKEN_OSTAP", ""),
    "МАША": os.environ.get("FIREFLY_TOKEN_MASHA", ""),
}


# Телеграм подписывает каждый запрос к вебхуку этим значением (заголовок
# X-Telegram-Bot-Api-Secret-Token), если передать его в setWebhook. Без
# проверки вебхук принимает ЛЮБОЙ POST: адрес бота не секрет, а
# is_registered() смотрит на chat_id ИЗ САМОГО ЗАПРОСА — то есть подделать
# сообщение от любого из двоих мог кто угодно.
#
# Значение по умолчанию выводится из токена бота, а не требует отдельной
# переменной: так защита включается сразу на деплое, без окна, в котором
# переменную ещё не проставили. Знать его может только тот, кто и так знает
# токен бота, то есть владеет ботом целиком.
TELEGRAM_WEBHOOK_SECRET = os.environ.get("TELEGRAM_WEBHOOK_SECRET") or (
    hashlib.sha256(f"webhook:{TELEGRAM_BOT_TOKEN}".encode()).hexdigest()
    if TELEGRAM_BOT_TOKEN else ""
)
