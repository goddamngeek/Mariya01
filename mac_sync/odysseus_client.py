"""Real Odysseus client. Contract confirmed manually:

  POST {ODYSSEUS_URL}/api/v1/chat
    header: Authorization: Bearer <ODYSSEUS_TOKEN>
    body: {"message": str, "model": "mistral-large-latest",
           "api_key": <MISTRAL_API_KEY>, "base_url": "https://api.mistral.ai/v1",
           "provider": "mistral", "session": <optional session_id>}
    resp: {"response": str, "session_id": str, "model": str}

Session ids are per-user (see load_sessions/save_sessions) so a
conversation can continue across separate sync runs.
"""

import json
import os
from pathlib import Path

import httpx

ODYSSEUS_URL = os.environ.get("ODYSSEUS_URL", "http://localhost:7860")
ODYSSEUS_TOKEN = os.environ.get("ODYSSEUS_TOKEN", "")
MISTRAL_API_KEY = os.environ.get("MISTRAL_API_KEY", "")

MODEL = "mistral-large-latest"
BASE_URL = "https://api.mistral.ai/v1"
PROVIDER = "mistral"

SESSIONS_FILE = Path(__file__).parent / "sessions.json"


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
        f"{ODYSSEUS_URL}/api/v1/chat",
        headers={"Authorization": f"Bearer {ODYSSEUS_TOKEN}"},
        json=payload,
        timeout=30,
    )
    resp.raise_for_status()
    return resp.json()


def load_sessions() -> dict[str, str]:
    if SESSIONS_FILE.exists():
        return json.loads(SESSIONS_FILE.read_text())
    return {}


def save_sessions(sessions: dict[str, str]) -> None:
    SESSIONS_FILE.write_text(json.dumps(sessions, ensure_ascii=False, indent=2))
