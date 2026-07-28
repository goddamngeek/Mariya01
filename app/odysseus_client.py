"""Async port of mac_sync/odysseus_client.py — same confirmed contract,
used directly by the bot's own ingest job (app/ingest.py) now that Odysseus
has a real reachable URL instead of being Mac/Tailscale-only. See
mac_sync/odysseus_client.py for the full contract docstring; kept in sync
by hand since the sync (mac_sync) and async (bot) versions can't share code
without adding a shared package neither side otherwise needs.
"""

import httpx

from app.config import LLM_API_KEY, ODYSSEUS_TOKEN, ODYSSEUS_URL

_AUTH_HEADERS = {"Authorization": f"Bearer {ODYSSEUS_TOKEN}"}

_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    """Shared, keep-alive client — reused across calls instead of paying a
    fresh TCP+TLS handshake to Odysseus on every request."""
    global _client
    if _client is None:
        _client = httpx.AsyncClient(timeout=30)
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


class SessionNotFoundError(Exception):
    pass


async def get_active_endpoint() -> tuple[str, str]:
    """Auto-discover (base_url, model) from whatever's enabled right now in
    Odysseus's Admin -> Model Endpoints, via GET /api/models."""
    resp = await get_client().get(
        f"{ODYSSEUS_URL}/api/models", headers=_AUTH_HEADERS, timeout=15
    )
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


async def chat(message: str, base_url: str, model: str, session: str | None = None) -> dict:
    payload = {"message": message, "base_url": base_url, "model": model}
    if LLM_API_KEY:
        payload["api_key"] = LLM_API_KEY
    if session is not None:
        payload["session"] = session

    resp = await get_client().post(
        f"{ODYSSEUS_URL}/api/v1/chat", headers=_AUTH_HEADERS, json=payload
    )

    if resp.status_code == 404 and "Session not found" in resp.text:
        raise SessionNotFoundError(session)
    if resp.status_code >= 400:
        print(f"CHAT ERROR BODY: {resp.text}", flush=True)
    resp.raise_for_status()
    return resp.json()


async def agent_chat(
    message: str, base_url: str, model: str, session: str | None = None,
    system_prompt: str | None = None,
) -> dict:
    """Like chat(), but hits /api/v1/agent_chat — runs the real multi-round
    tool-execution loop (trilium_notes) instead of a single bare LLM call.
    Longer timeout since tool rounds add real latency on top of the model call."""
    payload = {"message": message, "base_url": base_url, "model": model}
    if LLM_API_KEY:
        payload["api_key"] = LLM_API_KEY
    if session is not None:
        payload["session"] = session
    if system_prompt is not None:
        payload["system_prompt"] = system_prompt

    resp = await get_client().post(
        f"{ODYSSEUS_URL}/api/v1/agent_chat", headers=_AUTH_HEADERS, json=payload, timeout=90
    )

    if resp.status_code == 404 and "Session not found" in resp.text:
        raise SessionNotFoundError(session)
    if resp.status_code >= 400:
        print(f"AGENT_CHAT ERROR BODY: {resp.text}", flush=True)
    resp.raise_for_status()
    return resp.json()
