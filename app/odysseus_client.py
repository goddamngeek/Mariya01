"""Async port of mac_sync/odysseus_client.py — same confirmed contract,
used directly by the bot's own ingest job (app/ingest.py) now that Odysseus
has a real reachable URL instead of being Mac/Tailscale-only. See
mac_sync/odysseus_client.py for the full contract docstring; kept in sync
by hand since the sync (mac_sync) and async (bot) versions can't share code
without adding a shared package neither side otherwise needs.
"""

import asyncio
import time

import httpx

from app.config import LLM_API_KEY, ODYSSEUS_TOKEN, ODYSSEUS_URL

_AUTH_HEADERS = {"Authorization": f"Bearer {ODYSSEUS_TOKEN}"}

_client: httpx.AsyncClient | None = None

_ENDPOINT_CACHE_TTL = 60  # seconds — matches the ingest poll cadence
_endpoint_cache: tuple[str, str] | None = None
_endpoint_cache_at: float = 0.0


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
    Odysseus's Admin -> Model Endpoints, via GET /api/models. Cached briefly —
    this was firing on every single message (each active question, plus every
    60s ingest batch) for config that rarely changes."""
    global _endpoint_cache, _endpoint_cache_at
    now = time.monotonic()
    if _endpoint_cache is not None and (now - _endpoint_cache_at) < _ENDPOINT_CACHE_TTL:
        return _endpoint_cache

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

    # Prefer yandexgpt/latest specifically over whatever happens to be first
    # in the list (currently aliceai-llm) — confirmed live 2026-07-29:
    # yandexgpt reliably makes real native tool calls (trilium_notes,
    # schedule_send, etc.) via this exact endpoint, while aliceai-llm
    # produced zero native tool calls across an entire night of testing.
    # Exact family match, not
    # substring — "yandexgpt-5-lite"/"yandexgpt-5-pro" also contain
    # "yandexgpt" but weren't the variant actually verified. Falls back to
    # models[0] if the exact yandexgpt/latest URI isn't in the list (e.g. a
    # different endpoint/provider).
    def _model_family(uri: str) -> str:
        parts = uri.split("/")
        return parts[-2] if len(parts) >= 2 else uri

    preferred = next(
        (m for m in models if _model_family(m) == "yandexgpt" and m.endswith("/latest")),
        None,
    )
    model = preferred or models[0]

    _endpoint_cache = (item["url"], model)
    _endpoint_cache_at = now
    return _endpoint_cache


async def agent_chat(
    message: str, base_url: str, model: str, session: str | None = None,
    system_prompt: str | None = None, require_tool: bool = False,
    require_tool_type: str | None = None,
) -> dict:
    """Hits /api/v1/agent_chat — runs the real multi-round tool-execution
    loop (trilium_notes, schedule_send) instead of a single bare LLM call.

    require_tool=True tells Odysseus a tool call is mandatory for this
    message (true for every passive-log message, and for active messages
    the bot's own keyword heuristic flags as relay/reminder intent) — if
    the model instead just narrates a fake "done" with nothing actually
    called (observed live, repeatedly, in different unparseable formats
    each time), Odysseus retries once with a correction, then falls back to
    doing it deterministically itself rather than silently returning the
    lie as a success. require_tool_type picks which fallback ("trilium_notes"
    default, or "schedule_send"). Longer client timeout since both the extra
    tool round-trips and that retry add real latency."""
    payload = {"message": message, "base_url": base_url, "model": model}
    if LLM_API_KEY:
        payload["api_key"] = LLM_API_KEY
    if session is not None:
        payload["session"] = session
    if system_prompt is not None:
        payload["system_prompt"] = system_prompt
    if require_tool:
        payload["require_tool"] = True
    if require_tool_type is not None:
        payload["require_tool_type"] = require_tool_type

    resp = await get_client().post(
        f"{ODYSSEUS_URL}/api/v1/agent_chat", headers=_AUTH_HEADERS, json=payload, timeout=150
    )

    if resp.status_code == 404 and "Session not found" in resp.text:
        raise SessionNotFoundError(session)
    if resp.status_code >= 400:
        print(f"AGENT_CHAT ERROR BODY: {resp.text}", flush=True)
    resp.raise_for_status()
    return resp.json()


async def parse_time_via_llm(text: str, now_iso: str) -> str | None:
    """Narrow, single-shot fallback for reminder times the regex parser
    (app/reminder_time.py) couldn't handle — deliberately hits /v1/chat
    (a bare completion, no tools/sessions/require_tool retry dance), since
    this only ever needs one plain answer, not the whole agent loop.
    Returns an ISO 8601 Moscow-local datetime string, or None if the model
    says there's no specific time (or the call fails — caller falls back
    to "now" either way, same as if this returned None)."""
    prompt = (
        f"Текущее время в Москве: {now_iso}. Пользователь написал: {text!r}\n"
        "Если в тексте названо конкретное время или дата напоминания — "
        "ответь ТОЛЬКО датой-временем в формате 2026-08-13T15:00:00 "
        "(московское время, без смещения часового пояса), без ничего "
        "больше. Если конкретное время не названо — ответь ровно словом none."
    )
    try:
        resp = await get_client().post(
            f"{ODYSSEUS_URL}/api/v1/chat", headers=_AUTH_HEADERS,
            json={"message": prompt}, timeout=30,
        )
        resp.raise_for_status()
        reply = (resp.json().get("response") or "").strip()
    except httpx.HTTPError as exc:
        print(f"parse_time_via_llm failed: {exc!r}", flush=True)
        return None

    if not reply or reply.lower().startswith("none"):
        return None

    from datetime import datetime as _dt
    try:
        _dt.fromisoformat(reply)
    except ValueError:
        print(f"parse_time_via_llm: unparseable reply {reply!r}", flush=True)
        return None
    return reply


