"""Direct Trilium ETAPI client for the fully-deterministic paths — ported
from Odysseus's src/tool_implementations.py (do_fill_ezhednevnik,
do_kanban_status and their helpers). These never involve an LLM or
Odysseus's session/auth stack at all: by the time app/service.py calls
fill_ezhednevnik(), it already has every field it needs, parsed by plain
regex/verbatim storage, and read_kanban_status() is a pure read with no
judgment involved either — routing either through Odysseus was pure
unnecessary surface area (and the source of most of the ежедневник bugs
chased in practice — a slot enum that drifted out of sync between the two
repos, entries dated by write-time instead of send-time, note-title
resolution differing between copies of the same logic). Odysseus keeps the
Trilium tools that genuinely need an LLM (trilium_notes, add_chinese_word,
etc.) — only the deterministic paths moved.
"""

import html
import json
from datetime import date as _date, datetime
from typing import Optional

import httpx

from app.config import TIMEZONE, TRILIUM_ETAPI_TOKEN, TRILIUM_URL

_EZHEDNEVNIK_WEEKDAYS = ("Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun")

# Column layout of the real ЕЖЕДНЕВНИК spreadsheet (verified live against
# the actual note — row 0 holds these exact headers).
_EZHEDNEVNIK_TEXT_COLS = {
    "event": "2", "hdif_am": "3", "hdif_pm": "5",
    "wdis_self": "8", "wdis_market": "9", "wdis_news": "10",
    "wdil": "12", "mistakes": "14",
}
_EZHEDNEVNIK_SCORE_COLS = {"hdif_am_score": "4", "hdif_pm_score": "6"}


class TriliumNotConfiguredError(Exception):
    pass


class TriliumNoteNotFoundError(Exception):
    pass


def _cell_value(row: dict, col: str):
    cell = row.get(col)
    return cell.get("v") if cell else None


async def _get_content(client: httpx.AsyncClient, note_id: str) -> str:
    resp = await client.get(f"{TRILIUM_URL}/etapi/notes/{note_id}/content")
    resp.raise_for_status()
    return resp.text


async def _put_content(client: httpx.AsyncClient, note_id: str, content: str) -> None:
    resp = await client.put(
        f"{TRILIUM_URL}/etapi/notes/{note_id}/content",
        content=content.encode("utf-8"),
        headers={"Content-Type": "text/plain"},
    )
    resp.raise_for_status()


async def _find_note_id(client: httpx.AsyncClient, title: str) -> Optional[str]:
    """Exact-title lookup — Trilium's search is fuzzy/substring, so this
    always filters down to an exact match rather than trusting the first
    hit. Compares stripped titles: at least one real note in this vault
    has a trailing space in its actual title, which a naive == would
    silently fail to match."""
    resp = await client.get(f"{TRILIUM_URL}/etapi/notes", params={"search": title})
    resp.raise_for_status()
    return next(
        (n.get("noteId") for n in (resp.json().get("results") or [])
         if (n.get("title") or "").strip() == title.strip()),
        None,
    )


async def _find_ezhednevnik_note_id(client: httpx.AsyncClient, person_name: str) -> Optional[str]:
    """Each person has their own ЕЖЕДНЕВНИК note (e.g. "ЕЖЕДНЕВНИК ОСТАП",
    "ЕЖЕДНЕВНИК  МАША" — the real titles turned out to be inconsistently
    spaced), so this matches on whitespace-collapsed comparison rather than
    a strict one."""
    resp = await client.get(f"{TRILIUM_URL}/etapi/notes", params={"search": "ЕЖЕДНЕВНИК"})
    resp.raise_for_status()
    target = " ".join(f"ЕЖЕДНЕВНИК {person_name}".split())
    return next(
        (n.get("noteId") for n in (resp.json().get("results") or [])
         if " ".join((n.get("title") or "").split()) == target),
        None,
    )


async def fill_ezhednevnik(fields: dict) -> None:
    """Write one person's answers for one time slot (am/pm/evening) into
    that day's row of their ЕЖЕДНЕВНИК spreadsheet note — a Trilium
    "spreadsheet" note (Univer JSON, not a simple table): read the whole
    JSON, mutate specific cells, write the whole thing back with PUT
    (Content-Type text/plain even though the note's own mime is
    application/json — application/json makes Trilium's ETAPI 500).

    `fields` must contain person_name, slot, and a "date" (ISO, the day the
    question was actually SENT — not necessarily today, if answered late)
    plus whichever answer fields were actually given. Raises on any
    failure; caller decides what to do (app/service.py only closes the
    ежедневник prompt on success, so a raised exception here correctly
    leaves it open for a retry)."""
    if not TRILIUM_URL or not TRILIUM_ETAPI_TOKEN:
        raise TriliumNotConfiguredError("TRILIUM_URL/TRILIUM_ETAPI_TOKEN not configured")

    person_name = fields["person_name"]

    headers = {"Authorization": TRILIUM_ETAPI_TOKEN}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        note_id = await _find_ezhednevnik_note_id(client, person_name)
        if note_id is None:
            raise TriliumNoteNotFoundError(f"Could not find the ЕЖЕДНЕВНИК note for {person_name}")

        raw = await _get_content(client, note_id)
        data = json.loads(raw)
        sheet = next(iter(data["workbook"]["sheets"].values()))
        cell_data = sheet["cellData"]

        date_str = fields.get("date")
        entry_date = _date.fromisoformat(date_str) if date_str else datetime.now(TIMEZONE).date()
        entry_serial = (entry_date - _date(1899, 12, 30)).days
        day_abbrev = _EZHEDNEVNIK_WEEKDAYS[entry_date.weekday()]

        target_row = None
        unclaimed_row = None
        latest_row = None  # any row for this date, regardless of owner
        for row_key, row in cell_data.items():
            if row_key == "0":
                continue
            if _cell_value(row, "0") != entry_serial:
                continue
            if latest_row is None or int(row_key) > int(latest_row):
                latest_row = row_key
            row_owner = _cell_value(row, "15")
            if row_owner == person_name:
                target_row = row_key
                break
            if row_owner is None and unclaimed_row is None:
                unclaimed_row = row_key

        if target_row is None:
            target_row = unclaimed_row
        if target_row is None and latest_row is not None:
            # Today's scaffold row is already claimed by someone else —
            # insert a fresh row right after it, shifting everything below
            # down by one, so both people's entries for the same day stay
            # adjacent and in date order instead of one landing appended
            # at the very end among far-future unused scaffold rows.
            insert_after = int(latest_row)
            max_row = max((int(r) for r in cell_data.keys()), default=0)
            for rn in range(max_row, insert_after, -1):
                key = str(rn)
                if key in cell_data:
                    cell_data[str(rn + 1)] = cell_data.pop(key)
            target_row = str(insert_after + 1)
        if target_row is None:
            max_row = max((int(r) for r in cell_data.keys()), default=0)
            target_row = str(max_row + 1)

        row = cell_data.setdefault(target_row, {})
        row["0"] = {"s": "tbiu37", "v": entry_serial, "t": 2}
        row["1"] = {"s": "A_Fnmp", "v": day_abbrev, "t": 1}
        row["15"] = {"v": person_name, "t": 1}

        for field, col in _EZHEDNEVNIK_TEXT_COLS.items():
            value = (fields.get(field) or "").strip() if isinstance(fields.get(field), str) else ""
            if value:
                row[col] = {"s": "A_Fnmp", "v": value, "t": 1}
        for field, col in _EZHEDNEVNIK_SCORE_COLS.items():
            raw_value = fields.get(field)
            if raw_value not in (None, ""):
                row[col] = {"s": "FdjA42", "v": int(raw_value), "t": 2}

        header = cell_data.setdefault("0", {})
        if "15" not in header:
            header["15"] = {"s": "QF6GoM", "v": "Owner", "t": 1}

        await _put_content(client, note_id, json.dumps(data))


async def read_kanban_status(board_name: str = "КАНБАН // KANBAN") -> str:
    """Read-only. Board-view Collection notes (Kanban, Sales) do NOT create
    a real parent-child link between a card and its column — every card is
    physically a child of the board note itself regardless of column, and
    column membership lives ONLY in each card's #status label (confirmed
    live against the actual vault). So the only way to answer "what's in
    progress" is to read every child's status label directly, not the tree.
    Raises on failure; caller decides what to tell the person."""
    if not TRILIUM_URL or not TRILIUM_ETAPI_TOKEN:
        raise TriliumNotConfiguredError("TRILIUM_URL/TRILIUM_ETAPI_TOKEN not configured")

    headers = {"Authorization": TRILIUM_ETAPI_TOKEN}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        board_id = await _find_note_id(client, board_name)
        if board_id is None:
            raise TriliumNoteNotFoundError(f"No board note titled '{board_name}' found.")

        board_resp = await client.get(f"{TRILIUM_URL}/etapi/notes/{board_id}")
        board_resp.raise_for_status()
        child_ids = board_resp.json().get("childNoteIds") or []
        if not child_ids:
            return f"'{html.escape(board_name)}' has no cards."

        columns: dict[str, list[str]] = {}
        for child_id in child_ids:
            child_resp = await client.get(f"{TRILIUM_URL}/etapi/notes/{child_id}")
            child_resp.raise_for_status()
            child = child_resp.json()
            status = next(
                (a["value"] for a in child.get("attributes", [])
                 if a.get("type") == "label" and a.get("name") == "status"),
                "(без статуса)",
            )
            columns.setdefault(status, []).append(child.get("title") or "(без названия)")

        # Bold column headers via Telegram's HTML parse mode — caller must
        # send this with parse_mode="HTML". Card titles/status names come
        # straight from Trilium (user-editable), so escape them to keep
        # any stray <, >, & from breaking Telegram's HTML parsing.
        sections = []
        for status, titles in columns.items():
            numbered = "\n".join(f"{i}. {html.escape(title)}" for i, title in enumerate(titles, 1))
            sections.append(f"<b>{html.escape(status)}:</b>\n\n{numbered}")
        return "\n\n".join(sections)
