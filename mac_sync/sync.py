"""Runs on the Mac (cron/launchd). Bridges the bot's sync API and the local
Odysseus service. Odysseus contract (not implemented here, see
odysseus_stub.py for a placeholder that satisfies it):

  POST {ODYSSEUS_URL}/odysseus/ingest
    body: {"id": int, "user_id": int, "text": str, "created_at": str}   (forwarded as-is)
    resp: {"confirmed": bool}

  GET  {ODYSSEUS_URL}/odysseus/generate?user_id=U&questions=N&replies=M
    resp: {"questions": [str, ...], "replies": [str, ...]}
"""

import os

import httpx

BOT_URL = os.environ.get("BOT_URL", "http://localhost:8000")
BOT_SYNC_TOKEN = os.environ.get("SYNC_BEARER_TOKEN", "")
ODYSSEUS_URL = os.environ.get("ODYSSEUS_URL", "http://localhost:9000")
ALLOWED_CHAT_IDS = [int(x) for x in os.environ.get("ALLOWED_CHAT_IDS", "").split(",") if x.strip()]

HEADERS = {"Authorization": f"Bearer {BOT_SYNC_TOKEN}"}


def pull_incoming() -> list[dict]:
    resp = httpx.get(f"{BOT_URL}/sync/pull", headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()


def forward_to_odysseus(message: dict) -> bool:
    resp = httpx.post(f"{ODYSSEUS_URL}/odysseus/ingest", json=message, timeout=10)
    resp.raise_for_status()
    return bool(resp.json().get("confirmed", False))


def ack(ids: list[int]) -> None:
    if not ids:
        return
    resp = httpx.post(f"{BOT_URL}/sync/ack", headers=HEADERS, json={"ids": ids}, timeout=10)
    resp.raise_for_status()


def fetch_new_outgoing(user_id: int) -> dict:
    resp = httpx.get(
        f"{ODYSSEUS_URL}/odysseus/generate",
        params={"user_id": user_id, "questions": 5, "replies": 5},
        timeout=10,
    )
    resp.raise_for_status()
    return resp.json()


def push_outgoing(user_id: int, questions: list[str], replies: list[str]) -> None:
    items = [{"user_id": user_id, "category": "question", "text": t} for t in questions]
    items += [{"user_id": user_id, "category": "reply", "text": t} for t in replies]
    if not items:
        return
    resp = httpx.post(
        f"{BOT_URL}/sync/push", headers=HEADERS, json={"items": items}, timeout=10
    )
    resp.raise_for_status()


def run() -> None:
    incoming = pull_incoming()
    confirmed_ids = [m["id"] for m in incoming if forward_to_odysseus(m)]
    ack(confirmed_ids)

    for user_id in ALLOWED_CHAT_IDS:
        generated = fetch_new_outgoing(user_id)
        push_outgoing(user_id, generated.get("questions", []), generated.get("replies", []))


if __name__ == "__main__":
    run()
