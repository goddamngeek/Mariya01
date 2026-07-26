"""Real Odysseus client. Contracts confirmed manually:

  GET {ODYSSEUS_URL}/api/models
    header: Authorization: Bearer <ODYSSEUS_TOKEN>
    resp: {"hosts": [...], "items": [{"url": str, "models": [str, ...],
           "endpoint_id": str, "endpoint_name": str, ...}, ...]}
    "url" is already the full chat-completions URL (build_chat_url() applied
    server-side), not a bare base — but /api/v1/chat's own base_url handling
    strips that suffix back off (normalize_base()), so passing it straight
    through as base_url works either way. Used to auto-discover which
    base_url/model to use from whatever endpoint is currently enabled in
    Odysseus's own Admin -> Model Endpoints, so nothing here has to change
    by hand when that's reconfigured in the UI.

  POST {ODYSSEUS_URL}/api/v1/chat
    header: Authorization: Bearer <ODYSSEUS_TOKEN>
    body: {"message": str, "base_url": str, "model": str,
           "api_key": <LLM_API_KEY, omitted if unset>,
           "session": <optional session_id>}
    resp: {"response": str, "session_id": str, "model": str}

    base_url/model are resolved once per sync.py run (via get_active_endpoint()
    in run()) and passed into chat() as arguments — they don't change mid-run,
    so there's no need to re-call GET /api/models per message. api_key is
    NOT discoverable that way (Odysseus doesn't expose it, by design) — set
    it in .env yourself for whichever provider is actually configured
    (Mistral, Yandex, ...). If the endpoint's own Admin-configured api_key
    is enough, leave LLM_API_KEY unset.

  POST {ODYSSEUS_URL}/api/session
    header: Authorization: Bearer <ODYSSEUS_TOKEN>
    body: {"name": str, "endpoint_url": str, "model": str, "skip_validation": true}
    resp: assumed to contain the new session id under "session_id" or "id" —
    NOT manually confirmed like /api/v1/chat was; verify against the real
    response and adjust create_session() below if the key differs.
    Unused in the current flow — Odysseus creates sessions itself via
    /api/v1/chat's own session handling. Kept for potential future use.

Session ids live in the bot's registered_users table (fetched/stored via the
bot's /sync/session endpoints), not locally — so the same conversation
continues in Odysseus's UI regardless of which machine runs this script.
"""

import os

import httpx

ODYSSEUS_URL = os.environ.get("ODYSSEUS_URL", "http://localhost:7860")
ODYSSEUS_TOKEN = os.environ.get("ODYSSEUS_TOKEN", "")
LLM_API_KEY = os.environ.get("LLM_API_KEY", "")

_AUTH_HEADERS = {"Authorization": f"Bearer {ODYSSEUS_TOKEN}"}


class SessionNotFoundError(Exception):
    pass


def get_active_endpoint() -> tuple[str, str]:
    """Auto-discover (base_url, model) from whatever's enabled right now in
    Odysseus's Admin -> Model Endpoints, via GET /api/models."""
    resp = httpx.get(f"{ODYSSEUS_URL}/api/models", headers=_AUTH_HEADERS, timeout=15)
    resp.raise_for_status()
    items = resp.json().get("items") or []
    if not items:
        raise RuntimeError("GET /api/models returned no endpoints — configure one in Odysseus Admin")

    item = items[0]
    models = item.get("models") or []
    if not models:
        raise RuntimeError(
            f"Endpoint {item.get('endpoint_name')!r} has no available models (GET /api/models)"
        )

    return item["url"], models[0]


def chat(message: str, base_url: str, model: str, session: str | None = None) -> dict:
    payload = {"message": message, "base_url": base_url, "model": model}
    if LLM_API_KEY:
        payload["api_key"] = LLM_API_KEY
    if session is not None:
        payload["session"] = session

    resp = httpx.post(
        f"{ODYSSEUS_URL}/api/v1/chat", headers=_AUTH_HEADERS, json=payload, timeout=30
    )
    if resp.status_code == 404 and "Session not found" in resp.text:
        raise SessionNotFoundError(session)
    if resp.status_code >= 400:
        print(f"CHAT ERROR BODY: {resp.text}", flush=True)
    resp.raise_for_status()
    return resp.json()


def create_session(name: str) -> str:
    base_url, model = get_active_endpoint()
    payload = {
        "name": name,
        "endpoint_url": base_url,
        "model": model,
        "skip_validation": True,
    }
    resp = httpx.post(
        f"{ODYSSEUS_URL}/api/session", headers=_AUTH_HEADERS, data=payload, timeout=30
    )
    if resp.status_code >= 400:
        print(f"SESSION ERROR BODY: {resp.text}", flush=True)
    resp.raise_for_status()
    data = resp.json()
    return data.get("session_id") or data["id"]
