"""Runs on the Mac (cron/launchd). Bridges the bot's sync API and the real
Odysseus chat service (see odysseus_client.py for the confirmed contract).

Ingest — forward each unconfirmed incoming message to Odysseus so it can
process/remember it. The response text is discarded; a successful call just
confirms the incoming message. Each user_id has a persistent Odysseus
session — the bot's registered_users table stores whatever session_id
Odysseus hands back, and it's reused for every later message (Odysseus
provisions the session itself on first contact, see odysseus_client.chat())
— so their whole conversation stays in one chat visible in Odysseus's own UI.
"""

from pathlib import Path

from dotenv import load_dotenv

# Anchored to this file's own directory, not the caller's CWD — cron/launchd
# invoke this with an arbitrary working directory (often $HOME), so a bare
# load_dotenv() would silently find nothing and every var would fall back
# to its default.
load_dotenv(Path(__file__).resolve().parent / ".env")

import os

import httpx

from odysseus_client import SessionNotFoundError, chat, get_active_endpoint

BOT_URL = os.environ.get("BOT_URL", "http://localhost:8000")
BOT_SYNC_TOKEN = os.environ.get("SYNC_BEARER_TOKEN", "")

HEADERS = {"Authorization": f"Bearer {BOT_SYNC_TOKEN}"}


def pull_incoming() -> list[dict]:
    resp = httpx.get(f"{BOT_URL}/sync/pull", headers=HEADERS, timeout=10)
    resp.raise_for_status()
    return resp.json()


def ack(ids: list[int]) -> None:
    if not ids:
        return
    resp = httpx.post(f"{BOT_URL}/sync/ack", headers=HEADERS, json={"ids": ids}, timeout=10)
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


def run() -> None:
    base_url, model = get_active_endpoint()
    ingest_incoming(base_url, model)


if __name__ == "__main__":
    run()
