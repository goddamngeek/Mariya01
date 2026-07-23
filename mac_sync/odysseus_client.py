"""Real Odysseus client. Chat contract confirmed manually:

  POST {ODYSSEUS_URL}/api/v1/chat
    header: Authorization: Bearer <ODYSSEUS_TOKEN>
    body: {"message": str, "model": "mistral-large-latest",
           "api_key": <MISTRAL_API_KEY>, "base_url": "https://api.mistral.ai/v1",
           "provider": "mistral", "session": <optional session_id>}
    resp: {"response": str, "session_id": str, "model": str}

  POST {ODYSSEUS_URL}/api/session
    header: Authorization: Bearer <ODYSSEUS_TOKEN>
    body: {"name": str}
    resp: assumed to contain the new session id under "session_id" or "id" —
    NOT manually confirmed like /api/v1/chat was; verify against the real
    response and adjust create_session() below if the key differs.

Session ids live in the bot's registered_users table (fetched/stored via the
bot's /sync/session endpoints), not locally — so the same conversation
continues in Odysseus's UI regardless of which machine runs this script.
"""

import os

import httpx

ODYSSEUS_URL = os.environ.get("ODYSSEUS_URL", "http://localhost:7860")
ODYSSEUS_TOKEN = os.environ.get("ODYSSEUS_TOKEN", "")
MISTRAL_API_KEY = os.environ.get("MISTRAL_API_KEY", "")

MODEL = "mistral-large-latest"
BASE_URL = "https://api.mistral.ai/v1"
PROVIDER = "mistral"

_AUTH_HEADERS = {"Authorization": f"Bearer {ODYSSEUS_TOKEN}"}


def chat(message: str, session: str | None = None) -> dict:
    payload = {
        "message": message,
        "model": MODEL,
        "api_key": MISTRAL_API_KEY,
        "base_url": BASE_URL,
        "provider": PROVIDER,
    }
    if session is not None:
        payload["session"] = session

    resp = httpx.post(
        f"{ODYSSEUS_URL}/api/v1/chat", headers=_AUTH_HEADERS, json=payload, timeout=30
    )
    resp.raise_for_status()
    return resp.json()


def create_session(name: str) -> str:
    resp = httpx.post(
        f"{ODYSSEUS_URL}/api/session", headers=_AUTH_HEADERS, json={"name": name}, timeout=30
    )
    resp.raise_for_status()
    data = resp.json()
    return data.get("session_id") or data["id"]
