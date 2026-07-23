"""Runs on the Mac (cron/launchd). Bridges the bot's sync API and the real
Odysseus chat service (see odysseus_client.py for the confirmed contract).

Two independent mechanisms:
  1. Ingest — forward each unconfirmed incoming message to Odysseus so it
     can process/remember it. The response text is discarded; a successful
     call just confirms the incoming message. Session per user_id persists
     the conversation across sync runs.
  2. Pool refresh — unrelated to any specific incoming message. Periodically
     asks Odysseus (stateless, no session) to generate a batch of
     question/reply candidates and pushes them.
"""

import os
import re

import httpx

from odysseus_client import chat, load_sessions, save_sessions

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


# --- 1. ingest: forward incoming messages, just to confirm them ------------

def ingest_incoming() -> None:
    sessions = load_sessions()
    incoming = pull_incoming()
    confirmed_ids = []

    for message in incoming:
        user_key = str(message["user_id"])
        try:
            result = chat(message["text"], session=sessions.get(user_key))
        except httpx.HTTPError as exc:
            print(f"ingest failed for incoming id={message['id']}: {exc!r}")
            continue

        sessions[user_key] = result["session_id"]
        confirmed_ids.append(message["id"])

    save_sessions(sessions)
    ack(confirmed_ids)


# --- 2. pool refresh: independent, generates new question/reply candidates -

def _parse_lines(text: str) -> list[str]:
    lines = []
    for raw_line in text.splitlines():
        line = _LIST_MARKER_RE.sub("", raw_line).strip()
        if line:
            lines.append(line)
    return lines


def refresh_pool(user_id: int, category: str) -> None:
    prompt = PROMPTS[category].format(n=POOL_REFRESH_COUNT)

    try:
        result = chat(prompt, session=None)
    except httpx.HTTPError as exc:
        print(f"pool refresh failed for user={user_id} category={category}: {exc!r}")
        return

    candidates = _parse_lines(result["response"])
    push_outgoing(user_id, category, candidates)


def refresh_all_pools() -> None:
    for user_id in ALLOWED_CHAT_IDS:
        for category in ("reply", "question"):
            refresh_pool(user_id, category)


def run() -> None:
    ingest_incoming()
    refresh_all_pools()


if __name__ == "__main__":
    run()
