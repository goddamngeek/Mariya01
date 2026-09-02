"""Последние ошибки в памяти — чтобы их можно было посмотреть, не деплоя.

FastAPI отдаёт наружу голое «Internal Server Error», а логи Northflank
видны не всем, кто чинит. Из-за этого приходилось делать отдельный деплой
только ради того, чтобы увидеть текст исключения. Здесь оно оседает, а
/sync/errors показывает.

В памяти, а не в базе: это диагностика последнего часа, а не история.
Перезапуск её теряет — и это правильно, после перезапуска она уже про
другую сборку.
"""

import traceback
from collections import deque
from datetime import datetime

from app.config import TIMEZONE

_MAX = 50
_errors: deque = deque(maxlen=_MAX)


def record(label: str, exc: BaseException) -> None:
    _errors.append({
        "at": datetime.now(TIMEZONE).strftime("%d.%m %H:%M:%S"),
        "label": label,
        "error": f"{type(exc).__name__}: {exc}",
        "tb": "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))[-1500:],
    })


def recent(limit: int = 10) -> list[dict]:
    return list(_errors)[-limit:][::-1]
