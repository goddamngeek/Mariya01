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
import re
from datetime import date as _date, datetime, timedelta
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


async def _create_attribute(client: httpx.AsyncClient, note_id: str, attr_type: str, name: str, value: str) -> None:
    resp = await client.post(
        f"{TRILIUM_URL}/etapi/attributes",
        json={"noteId": note_id, "type": attr_type, "name": name, "value": value},
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
            # No scaffold row for this date at all (beyond the pre-filled
            # range) — same "first row with no real value" scan as the
            # insert-shift branch above needs, since rows beyond the real
            # scaffold are pre-formatted (style only) all the way to
            # rowCount, and "highest key present at all" would land past
            # rowCount entirely, invisible in Trilium's own UI (confirmed
            # live for the activity tracker — see log_activity).
            row_num = 1
            while _cell_value(cell_data.get(str(row_num), {}), "0") is not None:
                row_num += 1
            target_row = str(row_num)

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


KANBAN_COLUMNS = (
    "БЭКЛОГ // BACKLOG",
    "БУДУЩИЕ ЗАДАЧИ // FUTURE TASKS",
    "В РАБОТЕ // IN PROGRESS",
    "СДЕЛАНО // DONE",
)


async def add_kanban_card(
    person_name: str, title: str, column: str = "БЭКЛОГ // BACKLOG",
    board_name: str = "КАНБАН // KANBAN",
) -> None:
    """Add one card (a child text note of the board) to the Kanban board —
    same board-view mechanism as read_kanban_status: column membership is
    just a #status label on the card, not a real tree position."""
    if not TRILIUM_URL or not TRILIUM_ETAPI_TOKEN:
        raise TriliumNotConfiguredError("TRILIUM_URL/TRILIUM_ETAPI_TOKEN not configured")

    headers = {"Authorization": TRILIUM_ETAPI_TOKEN}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        board_id = await _find_note_id(client, board_name)
        if board_id is None:
            raise TriliumNoteNotFoundError(f"No board note titled '{board_name}' found.")

        create_resp = await client.post(
            f"{TRILIUM_URL}/etapi/create-note",
            json={"parentNoteId": board_id, "title": title, "type": "text", "content": ""},
        )
        create_resp.raise_for_status()
        note_id = create_resp.json()["note"]["noteId"]

        await _create_attribute(client, note_id, "label", "status", column)
        await _create_attribute(client, note_id, "label", "owner", person_name)


async def log_reminder_to_calendar(sender_name: str, target_name: str, message: str, when: datetime) -> None:
    """Best-effort log of a scheduled reminder/relay into that day's
    Trilium calendar note, purely for visibility — never raises, since
    delivery itself does not depend on this succeeding."""
    if not TRILIUM_URL or not TRILIUM_ETAPI_TOKEN:
        return
    try:
        date_str = when.strftime("%Y-%m-%d")
        time_str = when.strftime("%H:%M")
        headers = {"Authorization": TRILIUM_ETAPI_TOKEN}
        async with httpx.AsyncClient(timeout=15, headers=headers) as client:
            day_resp = await client.get(f"{TRILIUM_URL}/etapi/calendar/days/{date_str}")
            day_resp.raise_for_status()
            note_id = day_resp.json()["noteId"]

            entry_html = f"<p><b>{time_str}</b> — напоминание для {target_name} (от {sender_name}): {message}</p>"
            existing = await _get_content(client, note_id)
            await _put_content(client, note_id, existing + entry_html)
    except Exception as exc:
        print(f"log_reminder_to_calendar failed (non-fatal): {exc!r}", flush=True)


async def log_sale(person_name: str, item: str, status: str = "") -> None:
    """Log one sale as a new card on the ПРОДАЖИ board — same board-view
    mechanism as the Kanban task board, just a different domain (each
    card's #status here holds a price/date, not a workflow stage)."""
    if not TRILIUM_URL or not TRILIUM_ETAPI_TOKEN:
        raise TriliumNotConfiguredError("TRILIUM_URL/TRILIUM_ETAPI_TOKEN not configured")

    headers = {"Authorization": TRILIUM_ETAPI_TOKEN}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        board_id = await _find_note_id(client, "ПРОДАЖИ // SALES")
        if board_id is None:
            raise TriliumNoteNotFoundError("Could not find the ПРОДАЖИ // SALES board.")

        create_resp = await client.post(
            f"{TRILIUM_URL}/etapi/create-note",
            json={"parentNoteId": board_id, "title": item, "type": "text", "content": ""},
        )
        create_resp.raise_for_status()
        note_id = create_resp.json()["note"]["noteId"]

        if status:
            await _create_attribute(client, note_id, "label", "status", status)
        await _create_attribute(client, note_id, "label", "owner", person_name)


async def add_chinese_word(
    person_name: str, hieroglyph: str, pinyin: str = "", tone: str = "",
    translation: str = "", status: str = "новое",
) -> None:
    """Add one hieroglyph to Ostap's Chinese-vocabulary board (ЖИЗНЬ >
    РУТИНА > КИТАЙСКИЙ > ОСТАП > ИЕРОГЛИФЫ) — a board-view Collection
    note; each word becomes its own child note, with pinyin/tone/status as
    labels and the translation as the note's own content."""
    if not TRILIUM_URL or not TRILIUM_ETAPI_TOKEN:
        raise TriliumNotConfiguredError("TRILIUM_URL/TRILIUM_ETAPI_TOKEN not configured")

    headers = {"Authorization": TRILIUM_ETAPI_TOKEN}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        board_id = await _find_note_id(client, "ИЕРОГЛИФЫ")
        if board_id is None:
            raise TriliumNoteNotFoundError("Could not find the ИЕРОГЛИФЫ board.")

        create_resp = await client.post(
            f"{TRILIUM_URL}/etapi/create-note",
            json={"parentNoteId": board_id, "title": hieroglyph, "type": "text",
                  "content": f"<p>{translation}</p>"},
        )
        create_resp.raise_for_status()
        note_id = create_resp.json()["note"]["noteId"]

        for name, value in [("pinyin", pinyin), ("tone", tone), ("status", status), ("owner", person_name)]:
            if value:
                await _create_attribute(client, note_id, "label", name, value)


async def add_book(person_name: str, title: str, author: str = "") -> None:
    """Add a new book note under КНИГИ, templated from _ШАБЛОН_КНИГА via a
    ~template RELATION (not a #template label — a relation's value is a
    real noteId, a label's is plain text; confirmed live which one the
    actual template uses)."""
    if not TRILIUM_URL or not TRILIUM_ETAPI_TOKEN:
        raise TriliumNotConfiguredError("TRILIUM_URL/TRILIUM_ETAPI_TOKEN not configured")

    headers = {"Authorization": TRILIUM_ETAPI_TOKEN}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        knigi_id = await _find_note_id(client, "КНИГИ")
        template_id = await _find_note_id(client, "_ШАБЛОН_КНИГА")
        if knigi_id is None or template_id is None:
            raise TriliumNoteNotFoundError("Could not find КНИГИ or _ШАБЛОН_КНИГА.")

        create_resp = await client.post(
            f"{TRILIUM_URL}/etapi/create-note",
            json={"parentNoteId": knigi_id, "title": title, "type": "text", "content": ""},
        )
        create_resp.raise_for_status()
        note_id = create_resp.json()["note"]["noteId"]

        await _create_attribute(client, note_id, "relation", "template", template_id)
        if author:
            await _create_attribute(client, note_id, "label", "author", author)
        await _create_attribute(
            client, note_id, "label", "readingStart", datetime.now(TIMEZONE).strftime("%Y-%m-%d"),
        )
        await _create_attribute(client, note_id, "label", "owner", person_name)


async def add_book_review(book_title: str, review_text: str) -> None:
    """Append a review to a specific book's own note, under a new 'Ревью'
    heading — NOT also to a separate summary note, to avoid the same
    review existing in two places. Requires an exact note-title match;
    raises TriliumNoteNotFoundError if the title isn't found verbatim."""
    if not TRILIUM_URL or not TRILIUM_ETAPI_TOKEN:
        raise TriliumNotConfiguredError("TRILIUM_URL/TRILIUM_ETAPI_TOKEN not configured")

    headers = {"Authorization": TRILIUM_ETAPI_TOKEN}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        note_id = await _find_note_id(client, book_title)
        if note_id is None:
            raise TriliumNoteNotFoundError(f"No book note titled '{book_title}' found.")

        existing = await _get_content(client, note_id)
        stamp = datetime.now(TIMEZONE).strftime("%d.%m.%Y")
        review_html = f"<h2>Ревью ({stamp})</h2><p>{review_text}</p>"
        await _put_content(client, note_id, existing + review_html)


async def get_note_content_by_title(title: str) -> str:
    """Diagnostic-only: raw HTML content of any note by exact title — used
    once to inspect _ШАБЛОН_КНИГА's real structure while building the
    /addbook flow (see app/sync.py's /note_content)."""
    if not TRILIUM_URL or not TRILIUM_ETAPI_TOKEN:
        raise TriliumNotConfiguredError("TRILIUM_URL/TRILIUM_ETAPI_TOKEN not configured")
    headers = {"Authorization": TRILIUM_ETAPI_TOKEN}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        note_id = await _find_note_id(client, title)
        if note_id is None:
            raise TriliumNoteNotFoundError(f"No note titled '{title}' found.")
        return await _get_content(client, note_id)


async def get_active_reading_books() -> list[dict]:
    """Books currently being read — child notes of КНИГИ (see add_book) that
    have a readingStart label but no readingEnd one yet. Same
    fetch-every-child-then-inspect-its-labels approach as read_kanban_status,
    since Trilium's ETAPI has no "search by missing label" filter."""
    if not TRILIUM_URL or not TRILIUM_ETAPI_TOKEN:
        raise TriliumNotConfiguredError("TRILIUM_URL/TRILIUM_ETAPI_TOKEN not configured")

    headers = {"Authorization": TRILIUM_ETAPI_TOKEN}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        knigi_id = await _find_note_id(client, "КНИГИ")
        if knigi_id is None:
            raise TriliumNoteNotFoundError("Could not find the КНИГИ note.")

        knigi_resp = await client.get(f"{TRILIUM_URL}/etapi/notes/{knigi_id}")
        knigi_resp.raise_for_status()
        child_ids = knigi_resp.json().get("childNoteIds") or []

        books = []
        for child_id in child_ids:
            child_resp = await client.get(f"{TRILIUM_URL}/etapi/notes/{child_id}")
            child_resp.raise_for_status()
            child = child_resp.json()
            labels = {a["name"] for a in child.get("attributes", []) if a.get("type") == "label"}
            if "readingStart" in labels and "readingEnd" not in labels:
                books.append({"note_id": child_id, "title": child.get("title") or "(без названия)"})
        return books


async def add_book_quote(note_id: str, quote_text: str, impression: str) -> None:
    """Append one 'interesting moment' entry straight into the book's own
    note content (not a separate child note, per explicit request) — a
    divider, then the quote verbatim wrapped in quotation marks, then the
    person's own answer about what they liked about it."""
    if not TRILIUM_URL or not TRILIUM_ETAPI_TOKEN:
        raise TriliumNotConfiguredError("TRILIUM_URL/TRILIUM_ETAPI_TOKEN not configured")

    headers = {"Authorization": TRILIUM_ETAPI_TOKEN}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        existing = await _get_content(client, note_id)
        entry_html = (
            "<hr>"
            f"<p>«{html.escape(quote_text)}»</p>"
            f"<p>{html.escape(impression)}</p>"
        )
        await _put_content(client, note_id, existing + entry_html)


async def get_week_summary(person_name: str) -> str:
    """Aggregate the current CALENDAR week (Monday through today — future
    days in the same week obviously have no data yet) from a person's own
    ЕЖЕДНЕВНИК note: average AM/PM scores, and every notable event / thing
    learned / mistake logged. Pure aggregation of what's actually there —
    never invents or paraphrases, same "list it literally" ethos as
    read_kanban_status."""
    if not TRILIUM_URL or not TRILIUM_ETAPI_TOKEN:
        raise TriliumNotConfiguredError("TRILIUM_URL/TRILIUM_ETAPI_TOKEN not configured")

    headers = {"Authorization": TRILIUM_ETAPI_TOKEN}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        note_id = await _find_ezhednevnik_note_id(client, person_name)
        if note_id is None:
            raise TriliumNoteNotFoundError(f"Could not find the ЕЖЕДНЕВНИК note for {person_name}.")
        raw = await _get_content(client, note_id)

    data = json.loads(raw)
    sheet = next(iter(data["workbook"]["sheets"].values()))
    cell_data = sheet["cellData"]

    today = datetime.now(TIMEZONE).date()
    start_date = today - timedelta(days=today.weekday())  # Monday of this week (weekday(): Mon=0)
    start_serial = (start_date - _date(1899, 12, 30)).days
    end_serial = (today - _date(1899, 12, 30)).days

    am_scores: list[int] = []
    pm_scores: list[int] = []
    events: list[str] = []
    wdils: list[str] = []
    mistakes: list[str] = []

    for row_key, row in cell_data.items():
        if row_key == "0" or not row:
            continue
        date_v = _cell_value(row, "0")
        if not isinstance(date_v, (int, float)) or not (start_serial <= date_v <= end_serial):
            continue
        am_score = _cell_value(row, "4")
        if isinstance(am_score, (int, float)):
            am_scores.append(am_score)
        pm_score = _cell_value(row, "6")
        if isinstance(pm_score, (int, float)):
            pm_scores.append(pm_score)
        event = _cell_value(row, "2")
        if event:
            events.append(str(event))
        wdil = _cell_value(row, "12")
        if wdil:
            wdils.append(str(wdil))
        mistake = _cell_value(row, "14")
        if mistake:
            mistakes.append(str(mistake))

    if not (am_scores or pm_scores or events or wdils or mistakes):
        return "За последнюю неделю записей в ежедневнике нет."

    # Bold via Telegram's HTML parse mode — caller must send this with
    # parse_mode="HTML", same as read_kanban_status.
    lines = [f"<b>Сводка за неделю ({start_date.strftime('%d.%m')}–{today.strftime('%d.%m')}):</b>"]
    if am_scores:
        lines.append(f"\nСредний балл до обеда: {round(sum(am_scores) / len(am_scores))}")
    if pm_scores:
        lines.append(f"Средний балл после обеда: {round(sum(pm_scores) / len(pm_scores))}")
    if events:
        lines.append("\nЗаметные события:")
        lines.extend(f"— {html.escape(e)}" for e in events)
    if wdils:
        lines.append("\nЧему научился(лась):")
        lines.extend(f"— {html.escape(w)}" for w in wdils)
    if mistakes:
        lines.append("\nОшибки:")
        lines.extend(f"— {html.escape(m)}" for m in mistakes)
    return "\n".join(lines)


# Column layout of the real ТРЕКЕР РУТИНЫ {ИМЯ} spreadsheets (verified live
# against both real notes — row 0 holds these exact headers, identical in
# both). One note per person — no Owner-column disambiguation needed, unlike
# the old shared ЕЖЕДНЕВНИК note.
_ACTIVITY_COLS = {
    "yoga": ("2", "3", "4"),
    "chinese": ("6", "7", "8"),
    "trading": ("10", "11", "12"),
}
_ACTIVITY_DATE_STYLE_ID = "actDateFmt"

_DURATION_DIGIT_RE = re.compile(
    r"\d+\s*(?:минут\w*|мин\b|час(?:а|ов)?\b|ч\b)", re.IGNORECASE,
)
_DURATION_HALF_HOUR_RE = re.compile(r"\bполчаса\b", re.IGNORECASE)
_DURATION_BARE_HOUR_RE = re.compile(r"\bчас\b", re.IGNORECASE)


def extract_duration(text: str) -> Optional[str]:
    """Pulled opportunistically from the feedback answer if the person
    happened to mention how long it was ("позанималась 20 минут, было
    классно" / "занимался час") — never asked as its own question, per
    explicit choice (an extra forced question added friction to what's
    meant to be a quick, low-friction log). Called by app/service.py
    while processing the feedback step's reply, so `duration` can be
    passed into log_activity() alongside the score once that step
    completes."""
    m = _DURATION_DIGIT_RE.search(text)
    if m:
        return m.group(0)
    if _DURATION_HALF_HOUR_RE.search(text):
        return "полчаса"
    if _DURATION_BARE_HOUR_RE.search(text):
        return "час"
    return None


async def log_activity(person_name: str, activity: str, feedback: str, score, duration: Optional[str] = None) -> None:
    """Write one yoga/chinese/trading session into that person's own
    ТРЕКЕР РУТИНЫ {ИМЯ} note — one row per day, activity/feedback/score in
    three adjacent columns per activity (see _ACTIVITY_COLS). Reuses
    today's row if one already exists (e.g. a second activity logged the
    same day) — a second SAME-activity log the same day just overwrites
    that activity's own three cells; multiple sessions of one activity in
    one day aren't tracked separately, by explicit choice."""
    if not TRILIUM_URL or not TRILIUM_ETAPI_TOKEN:
        raise TriliumNotConfiguredError("TRILIUM_URL/TRILIUM_ETAPI_TOKEN not configured")

    activity_col, feedback_col, score_col = _ACTIVITY_COLS[activity]
    note_title = f"ТРЕКЕР РУТИНЫ {person_name}"

    headers = {"Authorization": TRILIUM_ETAPI_TOKEN}
    async with httpx.AsyncClient(timeout=15, headers=headers) as client:
        note_id = await _find_note_id(client, note_title)
        if note_id is None:
            raise TriliumNoteNotFoundError(f"Could not find the '{note_title}' note.")

        raw = await _get_content(client, note_id)
        data = json.loads(raw)
        sheet = next(iter(data["workbook"]["sheets"].values()))
        cell_data = sheet["cellData"]

        today = datetime.now(TIMEZONE).date()
        today_serial = (today - _date(1899, 12, 30)).days
        day_abbrev = _EZHEDNEVNIK_WEEKDAYS[today.weekday()]

        target_row = None
        for row_key, row in cell_data.items():
            if row_key == "0" or not row:
                continue
            if _cell_value(row, "0") == today_serial:
                target_row = row_key
                break
        if target_row is None:
            # This sheet's rows are pre-formatted (border/style) all the way
            # to rowCount with no real data in them — confirmed live: using
            # "one past the highest key present in cellData at all" treated
            # those style-only rows as "used", landing the new row AT
            # rowCount itself (row 1000 in a 1000-row sheet), invisible in
            # Trilium's own UI since it's past the rendered grid entirely.
            # Scan for the first row with no real date value instead.
            row_num = 1
            while _cell_value(cell_data.get(str(row_num), {}), "0") is not None:
                row_num += 1
            target_row = str(row_num)

        # This sheet was created fresh (no real rows yet) with no
        # date-formatted style defined at all — its header cell's own style
        # (MVgMFY) has no number-format pattern, so a serial like 46252
        # rendered as a raw integer instead of a date. Ensure a dedicated
        # date-format style exists (matching ЕЖЕДНЕВНИК's own "n": pattern
        # convention) rather than reusing the header's style.
        styles = data["workbook"].setdefault("styles", {})
        if _ACTIVITY_DATE_STYLE_ID not in styles:
            styles[_ACTIVITY_DATE_STYLE_ID] = {"n": {"pattern": "dd.mm.yyyy"}}

        row = cell_data.setdefault(target_row, {})
        row["0"] = {"s": _ACTIVITY_DATE_STYLE_ID, "v": today_serial, "t": 2}
        row["1"] = {"v": day_abbrev, "t": 1}
        row[activity_col] = {"v": duration or "✅", "t": 1}
        row[feedback_col] = {"v": feedback, "t": 1}
        row[score_col] = {"v": int(score), "t": 2}

        await _put_content(client, note_id, json.dumps(data))
