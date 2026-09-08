"""Поиск фильмов и сериалов в TMDb.

Зачем вообще. Книгу бот заводит с твоих слов: спрашивает название, автора и
четыре раздела описания — это форма на несколько сообщений. Для кино так
делать незачем, потому что всё это уже есть в открытой базе: пишешь «хочу
посмотреть Дюну», выбираешь из находок кнопкой, и карточка заполняется сама.

TMDb бесплатен для некоммерческого использования, ключ выдают сразу.
Условие — упоминание, оно лежит в /help (см. app/router.py).

Модуль намеренно ничего не знает ни про Trilium, ни про телеграм: он
отвечает на «найди» и «расскажи подробнее», а куда это положить — забота
вызывающего.
"""

import functools
from dataclasses import dataclass

import httpx

from app.config import TMDB_TOKEN
from app.media import MediaKind

_BASE = "https://api.themoviedb.org/3"


class TmdbNotConfiguredError(Exception):
    pass


@dataclass(frozen=True)
class Found:
    """Одна находка — ровно то, что нужно показать кнопкой и запомнить."""

    tmdb_id: int
    title: str
    year: str
    overview: str

    def label(self) -> str:
        """«Дюна (2021)» — на кнопке нужен год, иначе три экранизации
        одного романа неразличимы."""
        return f"{self.title} ({self.year})" if self.year else self.title


_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    """Общий keep-alive клиент — то же соображение, что в telegram.py и
    trilium_client.py: добавление одного фильма это два запроса подряд
    (поиск, потом подробности), и платить за рукопожатие дважды незачем.

    Способ авторизации определяется по виду ключа. TMDb выдаёт два разных:
    короткий v3 api_key, который идёт параметром в адресе, и длинный v4
    read access token — JWT, который идёт заголовком. Перепутать их легко, а
    ошибка выглядит одинаково (401), поэтому распознаём сами вместо того,
    чтобы требовать от человека знать разницу."""
    global _client
    if _client is None:
        headers = {"accept": "application/json"}
        params = {}
        if TMDB_TOKEN.count(".") == 2 and TMDB_TOKEN.startswith("ey"):
            headers["Authorization"] = f"Bearer {TMDB_TOKEN}"
        else:
            params["api_key"] = TMDB_TOKEN
        _client = httpx.AsyncClient(timeout=10, headers=headers, params=params)
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def _needs_tmdb(func):
    """Без ключа не работает ничего. Бросаем, а не возвращаем пустое: у
    вызывающего есть запасной путь (спросить название сообщением), и он
    должен отличать «ничего не нашлось» от «искать нечем»."""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        if not TMDB_TOKEN:
            raise TmdbNotConfiguredError("TMDB_TOKEN не задан")
        return await func(*args, **kwargs)
    return wrapper


# Пять — сколько находок показывать. Больше не влезает на экран телефона
# кнопками в один столбец, а нужный результат у TMDb почти всегда в первой
# тройке: поиск отсортирован по популярности.
MAX_RESULTS = 5


@_needs_tmdb
async def search(kind: MediaKind, query: str) -> list[Found]:
    """Находки по названию, самые популярные первыми.

    language=ru-RU — иначе вернутся оригинальные названия, и «Дюна» станет
    «Dune». Русский перевод у TMDb есть почти всегда; когда его нет,
    остаётся оригинал, и это лучше пустоты."""
    resp = await get_client().get(
        f"{_BASE}/search/{kind.tmdb_path}",
        params={"query": query.strip(), "language": "ru-RU", "include_adult": "false"},
    )
    resp.raise_for_status()
    results = resp.json().get("results") or []

    found = []
    for item in results[:MAX_RESULTS]:
        # У фильмов поля называются title/release_date, у сериалов
        # name/first_air_date — единственное место, где это различие
        # приходится знать.
        title = item.get("title") or item.get("name") or ""
        released = item.get("release_date") or item.get("first_air_date") or ""
        if not title:
            continue
        found.append(Found(
            tmdb_id=item["id"],
            title=title,
            year=released[:4],
            overview=(item.get("overview") or "").strip(),
        ))
    return found


@_needs_tmdb
async def details(kind: MediaKind, tmdb_id: int) -> dict:
    """Подробности одной находки: создатель, жанры, описание, год.

    append_to_response=credits — чтобы забрать съёмочную группу тем же
    запросом, а не вторым. Для сериалов режиссёра в привычном смысле нет,
    там создатели лежат отдельным полем created_by."""
    resp = await get_client().get(
        f"{_BASE}/{kind.tmdb_path}/{tmdb_id}",
        params={"language": "ru-RU", "append_to_response": "credits"},
    )
    resp.raise_for_status()
    data = resp.json()

    if kind.has_in_progress:  # сериал
        creators = [p.get("name", "") for p in (data.get("created_by") or [])]
    else:
        crew = (data.get("credits") or {}).get("crew") or []
        creators = [p.get("name", "") for p in crew if p.get("job") == "Director"]

    released = data.get("release_date") or data.get("first_air_date") or ""
    return {
        "title": data.get("title") or data.get("name") or "",
        "original_title": data.get("original_title") or data.get("original_name") or "",
        "year": released[:4],
        "creator": ", ".join(c for c in creators if c),
        "genres": ", ".join(g.get("name", "") for g in (data.get("genres") or [])),
        "overview": (data.get("overview") or "").strip(),
        "tmdb_id": tmdb_id,
    }
