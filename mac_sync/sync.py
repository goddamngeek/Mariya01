"""Runs on the Mac (cron/launchd). Bridges the bot's sync API and the real
Odysseus chat service (see odysseus_client.py for the confirmed contract).

Two independent mechanisms:
  1. Ingest — forward each unconfirmed incoming message to Odysseus so it
     can process/remember it. The response text is discarded; a successful
     call just confirms the incoming message. Each user_id has a persistent
     Odysseus session (created once via /api/session, stored in the bot's
     registered_users table, reused for every later message) so their whole
     conversation stays in one chat visible in Odysseus's own UI.
  2. Pool refresh — unrelated to any specific incoming message. Periodically
     asks Odysseus (stateless, no session) to generate a batch of
     question/reply candidates and pushes them.
"""

from pathlib import Path

from dotenv import load_dotenv

# Anchored to this file's own directory, not the caller's CWD — cron/launchd
# invoke this with an arbitrary working directory (often $HOME), so a bare
# load_dotenv() would silently find nothing and every var would fall back
# to its default.
load_dotenv(Path(__file__).resolve().parent / ".env")

import os
import re

import httpx

from odysseus_client import SessionNotFoundError, chat, create_session, get_active_endpoint

BOT_URL = os.environ.get("BOT_URL", "http://localhost:8000")
BOT_SYNC_TOKEN = os.environ.get("SYNC_BEARER_TOKEN", "")
ALLOWED_CHAT_IDS = [int(x) for x in os.environ.get("ALLOWED_CHAT_IDS", "").split(",") if x.strip()]

HEADERS = {"Authorization": f"Bearer {BOT_SYNC_TOKEN}"}

POOL_REFRESH_COUNT = 5

PROMPTS = {
    "reply": "Сгенерируй {n} коротких подтверждений, без повторов, нейтральным стилем.",
    "question": "Сгенерируй {n} коротких вопросов о том, как прошёл день, без повторов, нейтральным стилем.",
}

_LIST_MARKER_RE = re.compile(r"^\s*(?:[-*•]|\d+[.\)])\s*")


def pull_incoming() -> list[dict]:
    resp = httpx.get(f"{BOT_URL}/sync/pull", headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()


def ack(ids: list[int]) -> None:
    if not ids:
        return
    resp = httpx.post(f"{BOT_URL}/sync/ack", headers=HEADERS, json={"ids": ids}, timeout=10)
    resp.raise_for_status()


def push_outgoing(user_id: int, category: str, texts: list[str]) -> None:
    items = [{"user_id": user_id, "category": category, "text": t} for t in texts]
    if not items:
        return
    resp = httpx.post(f"{BOT_URL}/sync/push", headers=HEADERS, json={"items": items}, timeout=10)
    resp.raise_for_status()


def get_stored_session(user_id: int) -> str | None:
    resp = httpx.get(
        f"{BOT_URL}/sync/session", headers=HEADERS, params={"user_id": user_id}, timeout=10
    )
    resp.raise_for_status()
    return resp.json()["session_id"]


def store_session(user_id: int, session_id: str) -> None:
    resp = httpx.post(
        f"{BOT_URL}/sync/session",
        headers=HEADERS,
        json={"user_id": user_id, "session_id": session_id},
        timeout=10,
    )
    resp.raise_for_status()


def get_or_create_session(user_id: int) -> str:
    session_id = get_stored_session(user_id)
    print(f"[DEBUG] stored session: {session_id}")

    if session_id:
        return session_id

    session_id = create_session(f"Telegram — {user_id}")
    print(f"[DEBUG] created session: {session_id}")

    store_session(user_id, session_id)
    print("[DEBUG] stored on bot")

    return session_id


def chat_with_session(user_id: int, message: str, base_url: str, model: str) -> dict:
    session_id = get_stored_session(user_id)
    try:
        result = chat(message, base_url, model, session=session_id)
    except SessionNotFoundError:
        result = chat(message, base_url, model, session=None)  # сервер сам создаст новую сессию

    new_session_id = result.get("session_id")
    if new_session_id and new_session_id != session_id:
        store_session(user_id, new_session_id)
    return result


# --- 1. ingest: forward incoming messages, just to confirm them ------------

def ingest_incoming(base_url: str, model: str) -> None:
    incoming = pull_incoming()
    confirmed_ids = []

    for message in incoming:
        user_id = message["user_id"]

        try:
            chat_with_session(user_id, message["text"], base_url, model)
        except httpx.HTTPError as exc:
            print(f"ingest failed for incoming id={message['id']}: {exc!r}")
            continue

        confirmed_ids.append(message["id"])

    ack(confirmed_ids)


# --- 2. pool refresh: independent, generates new question/reply candidates -

def _parse_lines(text: str) -> list[str]:
    lines = []
    for raw_line in text.splitlines():
        line = _LIST_MARKER_RE.sub("", raw_line).strip()
        if line:
            lines.append(line)
    return lines


def refresh_pool(user_id: int, category: str, base_url: str, model: str) -> None:
    prompt = PROMPTS[category].format(n=POOL_REFRESH_COUNT)

    try:
        result = chat(prompt, base_url, model, session=None)
    except httpx.HTTPError as exc:
        print(f"pool refresh failed for user={user_id} category={category}: {exc!r}")
        return

    candidates = _parse_lines(result["response"])
    push_outgoing(user_id, category, candidates)


def refresh_all_pools(base_url: str, model: str) -> None:
    for user_id in ALLOWED_CHAT_IDS:
        for category in ("reply", "question"):
            refresh_pool(user_id, category, base_url, model)


def run() -> None:
    base_url, model = get_active_endpoint()
    ingest_incoming(base_url, model)
    refresh_all_pools(base_url, model)


if __name__ == "__main__":
    run()
