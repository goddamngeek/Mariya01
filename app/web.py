"""Веб-чат — второй вход в бота.

Зачем: сегодня единственная дверь к боту — телеграм. Его могут заблокировать,
в него можно потерять доступ (уже случалось), и вместе с ним пропадает вся
переписка после первого же /clear. Страница на своём адресе снимает
зависимость от одной компании.

Как устроено. Показывать нечего не надо собирать: бот и так журналит каждую
свою реплику в chat_journal (см. app/telegram.py). Страница просто читает
журнал. Поэтому ответ, ушедший в телеграм, виден и здесь — а если телеграм
недоступен, отправка провалится, но строка в журнале останется, и здесь
будет ровно то же самое.

Написанное в поле ввода уходит в ту же handle_incoming, что и сообщение из
телеграма (см. app/router.py); нажатая кнопка — в ту же handle_press.
Отдельной логики у веба нет и быть не должно.

Вход по токену из переменных окружения, свой у каждого человека: он же
говорит, чей разговор показывать. Токен приходит один раз в адресе,
обменивается на куку и из адреса исчезает — иначе он навсегда оставался бы
в истории браузера и в логах прокси.
"""

import html as html_escape
import json
import re
from datetime import datetime
from pathlib import Path

from fastapi import APIRouter, Cookie, HTTPException, Request, Response
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import BaseModel

from app import background
from app.callbacks import handle_press
from app.config import WEB_TOKENS
from app.db import (
    get_journal_row,
    get_journal_updates,
    journal_message,
    utcnow,
)
from app.people import NAME_TO_USER_ID
from app.press import Press
from app.channel import WEB_PRESS
from app.router import handle_incoming

router = APIRouter(prefix="/w")

_PAGE = Path(__file__).with_name("web_chat.html")
_COOKIE = "web_token"
# Год: страницей пользуются с телефона, и переспрашивать токен раз в неделю
# означало бы держать его под рукой — то есть в заметках, то есть нигде.
_COOKIE_MAX_AGE = 365 * 24 * 3600


def _person_for(token: str | None) -> str | None:
    """Чей это токен. Пустые значения не считаются: незаполненная переменная
    окружения не должна открывать вход всем подряд с пустой кукой."""
    if not token:
        return None
    for name, known in WEB_TOKENS.items():
        if known and token == known:
            return name
    return None


def _require_person(token: str | None) -> tuple[str, int]:
    person = _person_for(token)
    if person is None:
        raise HTTPException(status_code=401, detail="нужен токен")
    return person, NAME_TO_USER_ID[person]


# Телеграмная разметка, которую бот реально использует (parse_mode="HTML").
# Всё остальное экранируется: цитаты приезжают из книг через импорт с
# читалки, и доверять их содержимому как разметке нельзя.
_ALLOWED = re.compile(
    r'&lt;(/?)(b|strong|i|em|u|s|code|pre|blockquote)&gt;'
)
_ALLOWED_LINK = re.compile(r'&lt;a href=&quot;(https?://[^&"\s]+)&quot;&gt;')


def _render(text: str, parse_mode: str | None) -> str:
    """Текст реплики в безопасный HTML."""
    out = html_escape.escape(text)
    if parse_mode == "HTML":
        out = _ALLOWED.sub(r"<\1\2>", out)
        out = _ALLOWED_LINK.sub(r'<a href="\1" target="_blank" rel="noopener">', out)
        out = out.replace("&lt;/a&gt;", "</a>")
    return out.replace("\n", "<br>")


def _row(r) -> dict:
    return {
        "id": r["id"],
        "author": r["author"],
        "html": _render(r["text"], r["parse_mode"]),
        "buttons": json.loads(r["buttons"]) if r["buttons"] else None,
        "at": r["created_at"].isoformat(),
        "edited": r["edited_at"] is not None,
        "deleted": r["deleted_at"] is not None,
    }


@router.get("")
async def page(request: Request, t: str | None = None, web_token: str | None = Cookie(default=None)):
    """Страница. С ?t=… — обмен токена на куку и редирект без него в адресе."""
    if not any(WEB_TOKENS.values()):
        raise HTTPException(status_code=503, detail="веб-вход не настроен")

    if t is not None:
        if _person_for(t) is None:
            raise HTTPException(status_code=401, detail="неизвестный токен")
        response = RedirectResponse(url=str(request.url_for("page")), status_code=303)
        response.set_cookie(
            _COOKIE, t, max_age=_COOKIE_MAX_AGE, httponly=True, secure=True, samesite="lax",
        )
        return response

    _require_person(web_token)
    return HTMLResponse(_PAGE.read_text(encoding="utf-8"))


@router.get("/api/messages")
async def messages(
    after: int = 0, since: str | None = None, web_token: str | None = Cookie(default=None),
):
    """Что нового с прошлого опроса. `since` — момент, который вернул прошлый
    ответ; по нему приезжают ещё и правки с удалениями, которых по id не
    видно."""
    person, chat_id = _require_person(web_token)
    try:
        moment = datetime.fromisoformat(since) if since else None
    except ValueError:
        # Сломанный или подставленный since — не повод ронять опрос: отдаём
        # то же, что при первом заходе, страница доберёт остальное сама.
        moment = None
    rows = await get_journal_updates(chat_id, after, moment)
    return {"person": person, "now": utcnow().isoformat(), "messages": [_row(r) for r in rows]}


class Say(BaseModel):
    text: str


@router.post("/api/say")
async def say(body: Say, web_token: str | None = Cookie(default=None)):
    """Написанное на странице проходит ровно тот же путь, что сообщение из
    телеграма. Без telegram_message_id: привязывать нечего, и всё, что
    завязано на «ответ реплаем» и на удаление конкретного сообщения, для
    этого пути просто не сработает — осознанное упрощение, а не недоделка."""
    _person, chat_id = _require_person(web_token)
    text = body.text.strip()
    if not text:
        raise HTTPException(status_code=400, detail="пустое сообщение")
    await journal_message(chat_id, "user", text)
    # В фон, а не ожиданием: ответ бота всё равно уходит в телеграм, и если
    # телеграм недоступен, отправка будет ждать своего таймаута. Страница
    # опрашивает журнал, ответ появится в ней сам.
    background.spawn(handle_incoming(chat_id, text, None), "веб: сообщение")
    return {"ok": True}


class PressBody(BaseModel):
    id: int
    data: str


@router.post("/api/press")
async def press(body: PressBody, web_token: str | None = Cookie(default=None)):
    """Нажатие на кнопке в журнале. message_id берём из самой строки, а не из
    запроса, и проверяем, что строка из этого разговора: иначе страница могла
    бы попросить нажать кнопку в чужом."""
    _person, chat_id = _require_person(web_token)
    row = await get_journal_row(body.id)
    if row is None or row["chat_id"] != str(chat_id):
        raise HTTPException(status_code=404, detail="нет такой реплики")
    background.spawn(
        handle_press(
            Press(id=WEB_PRESS, data=body.data, chat_id=chat_id,
                  message_id=row["telegram_message_id"]),
        ),
        "веб: нажатие",
    )
    return {"ok": True}


@router.post("/api/logout")
async def logout(response: Response):
    response.delete_cookie(_COOKIE)
    return {"ok": True}
