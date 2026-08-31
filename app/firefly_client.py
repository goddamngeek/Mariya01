"""Firefly III через его REST API — учёт денег.

Устроен как app/trilium_client.py и по тем же причинам: общий keep-alive
клиент вместо одноразовых, проверка конфига декоратором, никакой модели в
разборе. Отличие одно, и оно определяет всё остальное: **токен у каждого
человека свой**. В Firefly два пользователя с полностью изолированными
данными — админ не видит чужих транзакций, — поэтому здесь нет «клиента
бота», есть вызовы от имени конкретного user_id.

Транзакция в Firefly всегда переход из счёта в счёт: расход уходит с
основного счёта на расходный, доход приходит с доходного на основной.
Расходный счёт можно не заводить заранее — Firefly создаёт его по имени,
если передать destination_name вместо id.
"""

import functools

import httpx

from app.config import FIREFLY_TOKENS, FIREFLY_URL
from app.people import USER_NAMES


class FireflyNotConfiguredError(Exception):
    pass


_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    """Общий клиент без заголовка авторизации: он у каждого запроса свой,
    потому что токен зависит от того, чьи это деньги."""
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=15)
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def _token_for(user_id: int) -> str:
    return FIREFLY_TOKENS.get(USER_NAMES.get(user_id, ""), "")


def _needs_firefly(func):
    """Проверка конфига в одном месте — как _needs_trilium. Токена нет
    отдельно у каждого человека, поэтому проверяем и его: у Маши он может
    появиться позже, чем у Остапа, и до тех пор её вызовы должны падать
    внятно, а не уходить в Firefly без авторизации."""
    @functools.wraps(func)
    async def wrapper(user_id: int, *args, **kwargs):
        if not FIREFLY_URL:
            raise FireflyNotConfiguredError("FIREFLY_URL not configured")
        if not _token_for(user_id):
            name = USER_NAMES.get(user_id, str(user_id))
            raise FireflyNotConfiguredError(f"no Firefly token for {name}")
        return await func(user_id, *args, **kwargs)
    return wrapper


def _headers(user_id: int) -> dict:
    return {
        "Authorization": f"Bearer {_token_for(user_id)}",
        "Accept": "application/json",
        "Content-Type": "application/json",
    }


@_needs_firefly
async def list_asset_accounts(user_id: int) -> list[dict]:
    """Счета, с которых человек платит, — карты, наличные, накопительные.
    Именно они станут кнопками при записи траты. Расходные и доходные сюда
    не попадают: они не «твои деньги», а адресаты операций."""
    resp = await get_client().get(
        f"{FIREFLY_URL}/api/v1/accounts",
        headers=_headers(user_id),
        params={"type": "asset", "limit": 100},
    )
    resp.raise_for_status()
    return [
        {
            "id": item["id"],
            "name": item["attributes"]["name"],
            "balance": item["attributes"].get("current_balance"),
            "currency": item["attributes"].get("currency_code"),
            "role": item["attributes"].get("account_role"),
        }
        for item in resp.json().get("data", [])
    ]


@_needs_firefly
async def recent_transactions(user_id: int, limit: int = 10) -> list[dict]:
    """Последние операции — чтобы проверять, что записалось на самом деле,
    не открывая Firefly. Тот же приём, что /sync/note для Trilium."""
    resp = await get_client().get(
        f"{FIREFLY_URL}/api/v1/transactions",
        headers=_headers(user_id),
        params={"limit": limit},
    )
    resp.raise_for_status()
    out = []
    for item in resp.json().get("data", []):
        for tx in item["attributes"].get("transactions", []):
            out.append({
                "id": item["id"],
                "type": tx.get("type"),
                "date": (tx.get("date") or "")[:10],
                "amount": tx.get("amount"),
                "currency": tx.get("currency_code"),
                "description": tx.get("description"),
                "source": tx.get("source_name"),
                "destination": tx.get("destination_name"),
                "category": tx.get("category_name"),
                "external_id": tx.get("external_id"),
            })
    return out


@_needs_firefly
async def create_expense(
    user_id: int, amount: str, description: str, source_id: str,
    destination_name: str, category_name: str | None = None,
    date: str | None = None, external_id: str | None = None,
) -> str:
    """Одна трата. Возвращает id созданной транзакции.

    external_id — защита от двойной записи: телеграм передоставляет апдейт,
    если бот не ответил за минуту, и дубль заметки безобиден, а дубль
    траты нет. Кладём туда id исходного сообщения и перед созданием
    проверяем, нет ли уже такой (see find_by_external_id)."""
    payload = {
        "transactions": [{
            "type": "withdrawal",
            "date": date,
            "amount": str(amount),
            "description": description,
            "source_id": str(source_id),
            "destination_name": destination_name,
        }]
    }
    tx = payload["transactions"][0]
    if category_name:
        tx["category_name"] = category_name
    if external_id:
        tx["external_id"] = external_id
    if not date:
        tx.pop("date")

    resp = await get_client().post(
        f"{FIREFLY_URL}/api/v1/transactions", headers=_headers(user_id), json=payload,
    )
    if resp.status_code >= 400:
        print(f"firefly create_expense failed {resp.status_code}: {resp.text[:400]}", flush=True)
    resp.raise_for_status()
    return resp.json()["data"]["id"]


@_needs_firefly
async def find_by_external_id(user_id: int, external_id: str) -> str | None:
    """Есть ли уже транзакция с таким external_id. Firefly сам дубли по
    этому полю не отбивает, так что проверяем перед записью."""
    resp = await get_client().get(
        f"{FIREFLY_URL}/api/v1/search/transactions",
        headers=_headers(user_id),
        params={"query": f"external_id:{external_id}", "limit": 1},
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    data = resp.json().get("data") or []
    return data[0]["id"] if data else None


@_needs_firefly
async def list_categories(user_id: int) -> list[dict]:
    """Категории, заведённые человеком в Firefly, — они станут кнопками на
    первом шаге записи траты.

    Читаются, а не задаются списком в коде, по прямому выбору: пусть бот
    показывает то, что реально есть у человека, а не навязывает свой набор,
    который потом разъедется с настоящим. Обратная сторона — пустой список
    означает, что показывать нечего, и поток должен сказать об этом
    по-человечески, а не молчать."""
    resp = await get_client().get(
        f"{FIREFLY_URL}/api/v1/categories",
        headers=_headers(user_id),
        params={"limit": 100},
    )
    resp.raise_for_status()
    return [
        {"id": item["id"], "name": item["attributes"]["name"]}
        for item in resp.json().get("data", [])
    ]
