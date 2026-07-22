import os
from datetime import time
from zoneinfo import ZoneInfo

TIMEZONE = ZoneInfo("Europe/Moscow")

PROACTIVE_WINDOW_START = time(13, 0)
PROACTIVE_WINDOW_END = time(22, 0)

DEFERRAL_DELAY_HOURS = 1
MAX_DEFERRALS = 2
OUTGOING_DEDUP_DAYS = 3
MAX_REGISTERED_USERS = 2

DATABASE_URL = os.environ.get(
    "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/odysseus_queue_bot"
)
TELEGRAM_BOT_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN", "")
SYNC_BEARER_TOKEN = os.environ.get("SYNC_BEARER_TOKEN", "")
