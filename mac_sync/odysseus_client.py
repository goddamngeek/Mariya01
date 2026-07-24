"""Real Odysseus client. Chat contract confirmed manually:

  POST {ODYSSEUS_URL}/api/v1/chat
    header: Authorization: Bearer <ODYSSEUS_TOKEN>
    body: {"message": str, "model": "mistral-large-latest",
           "api_key": <MISTRAL_API_KEY>, "base_url": "https://api.mistral.ai/v1",
           "provider": "mistral", "session": <optional session_id>}
    resp: {"response": str, "session_id": str, "model": str}

  POST {ODYSSEUS_URL}/api/session
    header: Authorization: Bearer <ODYSSEUS_TOKEN>
    body: {"name": str, "endpoint_url": <MISTRAL_BASE_URL>,
           "model": <MISTRAL_MODEL>, "skip_validation": true}
    resp: assumed to contain the new session id under "session_id" or "id" —
    NOT manually confirmed like /api/v1/chat was; verify against the real
    response and adjust create_session() below if the key differs.
    (endpoint_url/model/skip_validation are required — without them Odysseus
    responds 400, confirmed manually.)

Session ids live in the bot's registered_users table (fetched/stored via the
bot's /sync/session endpoints), not locally — so the same conversation
continues in Odysseus's UI regardless of which machine runs this script.
"""

import os

import httpx

ODYSSEUS_URL = os.environ.get("ODYSSEUS_URL", "http://localhost:7860")
ODYSSEUS_TOKEN = os.environ.get("ODYSSEUS_TOKEN", "")
MISTRAL_API_KEY = os.environ.get("MISTRAL_API_KEY", "")
MISTRAL_BASE_URL = os.environ.get(
    "MISTRAL_BASE_URL", "https://api.mistral.ai/v1/chat/completions"
)
MISTRAL_MODEL = os.environ.get("MISTRAL_MODEL", "mistral-large-latest")

BASE_URL = "https://api.mistral.ai/v1"
PROVIDER = "mistral"

_AUTH_HEADERS = {"Authorization": f"Bearer {ODYSSEUS_TOKEN}"}


class SessionNotFoundError(Exception):
    pass


def chat(message: str, session: str | None = None) -> dict:
    payload = {
        "message": message,
        "model": MISTRAL_MODEL,
        "api_key": MISTRAL_API_KEY,
        "base_url": BASE_URL,
        "provider": PROVIDER,
    }
    if session is not None:
        payload["session"] = session

    resp = httpx.post(
        f"{ODYSSEUS_URL}/api/v1/chat", headers=_AUTH_HEADERS, json=payload, timeout=30
    )
    if resp.status_code >= 400:
        print(f"CHAT ERROR BODY: {resp.text}", flush=True)
    if resp.status_code == 404 and "Session not found" in resp.text:
        raise SessionNotFoundError(resp.text)
    resp.raise_for_status()
    return resp.json()


def create_session(name: str) -> str:
    payload = {
        "name": name,
        "endpoint_url": MISTRAL_BASE_URL,
        "model": MISTRAL_MODEL,
        "skip_validation": True,
    }
    resp = httpx.post(
        f"{ODYSSEUS_URL}/api/session", headers=_AUTH_HEADERS, data=payload, timeout=30
    )
    if resp.status_code >= 400:
        print(f"SESSION ERROR BODY: {resp.text}", flush=True)
    resp.raise_for_status()
    data = resp.json()
    print (data)
    return data.get("session_id") or data["id"]
