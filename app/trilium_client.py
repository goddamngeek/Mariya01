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

import asyncio
import functools
import html
import json
import re
from datetime import date as _date, datetime, timedelta
from typing import Optional

import httpx

from app.config import TIMEZONE, TRILIUM_ETAPI_TOKEN, TRILIUM_URL
from app.db import cache_note_id, forget_note_id, get_cached_note_id

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


_client: httpx.AsyncClient | None = None


def get_client() -> httpx.AsyncClient:
    """Shared, keep-alive client — same reasoning as app/telegram.py's, and
    it matters more here: Trilium is reached over its public Caddy-proxied
    subdomain, and this module makes by far the most requests of any. Every
    operation used to open its own client and pay a fresh TCP+TLS handshake
    for it — a check-in write, a book list, every single imported quote."""
    global _client
    if _client is None:
        _client = httpx.AsyncClient(
            timeout=15, headers={"Authorization": TRILIUM_ETAPI_TOKEN},
        )
    return _client


async def close_client() -> None:
    global _client
    if _client is not None:
        await _client.aclose()
        _client = None


def _needs_trilium(func):
    """Nothing here can run unconfigured, and every public function used to
    open with the same two lines saying so. Raising (not returning) is what
    the callers expect: app/service.py only closes a prompt on success, so
    an unconfigured write leaves the person's answers intact for a retry.

    The one deliberate exception is log_reminder_to_calendar, which keeps
    its own check because it returns quietly instead — delivery must not
    depend on a best-effort calendar note."""
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        if not TRILIUM_URL or not TRILIUM_ETAPI_TOKEN:
            raise TriliumNotConfiguredError("TRILIUM_URL/TRILIUM_ETAPI_TOKEN not configured")
        return await func(*args, **kwargs)
    return wrapper


# Trilium's ETAPI has no bulk-fetch endpoint and no "search by missing
# label" filter, so listing books or kanban cards means one request per
# child. Done sequentially that is a round trip each, growing with the
# library; done all at once it is a single wait. Bounded so a large vault
# can't open dozens of simultaneous connections against Trilium.
_MAX_PARALLEL_NOTE_FETCHES = 8


async def _get_notes(client: httpx.AsyncClient, note_ids: list[str]) -> list[dict]:
    """Several notes at once, in the order asked for."""
    semaphore = asyncio.Semaphore(_MAX_PARALLEL_NOTE_FETCHES)

    async def fetch(note_id: str) -> dict:
        async with semaphore:
            resp = await client.get(f"{TRILIUM_URL}/etapi/notes/{note_id}")
            resp.raise_for_status()
            return resp.json()

    return list(await asyncio.gather(*(fetch(note_id) for note_id in note_ids)))


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


async def _note_exists(client: httpx.AsyncClient, note_id: str) -> bool:
    resp = await client.get(f"{TRILIUM_URL}/etapi/notes/{note_id}")
    if resp.status_code == 404:
        return False
    resp.raise_for_status()
    return True


async def _resolve_note_id(client: httpx.AsyncClient, key: str, search) -> Optional[str]:
    """Titles are what the bot knows a note by; noteIds are what Trilium
    guarantees. Resolve once, remember the id, and a later rename in the UI
    stops being able to break anything — the id keeps pointing at the same
    note whatever it's now called.

    A remembered id is confirmed to still exist before use (one cheap GET,
    in place of the fuzzy search it replaces), so a note that was genuinely
    deleted falls back to searching by title again instead of failing
    forever on a stale id."""
    cached = await get_cached_note_id(key)
    if cached is not None:
        if await _note_exists(client, cached):
            return cached
        await forget_note_id(key)
        print(f"trilium: cached note id for {key!r} no longer exists, re-resolving", flush=True)

    found = await search()
    if found is not None:
        await cache_note_id(key, found)
    return found


async def _search_note_id_by_title(client: httpx.AsyncClient, title: str) -> Optional[str]:
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


async def _find_note_id(client: httpx.AsyncClient, title: str) -> Optional[str]:
    return await _resolve_note_id(client, title, lambda: _search_note_id_by_title(client, title))


async def _search_ezhednevnik_note_id(client: httpx.AsyncClient, person_name: str) -> Optional[str]:
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


async def _find_ezhednevnik_note_id(client: httpx.AsyncClient, person_name: str) -> Optional[str]:
    return await _resolve_note_id(
        client, f"ЕЖЕДНЕВНИК {person_name}",
        lambda: _search_ezhednevnik_note_id(client, person_name),
    )


@_needs_trilium
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
    person_name = fields["person_name"]

    client = get_client()
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


@_needs_trilium
async def read_kanban_status(board_name: str = "КАНБАН // KANBAN") -> str:
    """Read-only. Board-view Collection notes (Kanban, Sales) do NOT create
    a real parent-child link between a card and its column — every card is
    physically a child of the board note itself regardless of column, and
    column membership lives ONLY in each card's #status label (confirmed
    live against the actual vault). So the only way to answer "what's in
    progress" is to read every child's status label directly, not the tree.
    Raises on failure; caller decides what to tell the person."""
    client = get_client()
    board_id = await _find_note_id(client, board_name)
    if board_id is None:
        raise TriliumNoteNotFoundError(f"No board note titled '{board_name}' found.")

    board_resp = await client.get(f"{TRILIUM_URL}/etapi/notes/{board_id}")
    board_resp.raise_for_status()
    child_ids = board_resp.json().get("childNoteIds") or []
    if not child_ids:
        return f"'{html.escape(board_name)}' has no cards."

    columns: dict[str, list[str]] = {}
    for child in await _get_notes(client, child_ids):
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


@_needs_trilium
async def add_kanban_card(
    person_name: str, title: str, column: str = "БЭКЛОГ // BACKLOG",
    board_name: str = "КАНБАН // KANBAN",
) -> None:
    """Add one card (a child text note of the board) to the Kanban board —
    same board-view mechanism as read_kanban_status: column membership is
    just a #status label on the card, not a real tree position."""
    client = get_client()
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
        client = get_client()
        day_resp = await client.get(f"{TRILIUM_URL}/etapi/calendar/days/{date_str}")
        day_resp.raise_for_status()
        note_id = day_resp.json()["noteId"]

        entry_html = f"<p><b>{time_str}</b> — напоминание для {target_name} (от {sender_name}): {message}</p>"
        existing = await _get_content(client, note_id)
        await _put_content(client, note_id, existing + entry_html)
    except Exception as exc:
        print(f"log_reminder_to_calendar failed (non-fatal): {exc!r}", flush=True)


@_needs_trilium
async def log_sale(person_name: str, item: str, status: str = "") -> None:
    """Log one sale as a new card on the ПРОДАЖИ board — same board-view
    mechanism as the Kanban task board, just a different domain (each
    card's #status here holds a price/date, not a workflow stage)."""
    client = get_client()
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


@_needs_trilium
async def add_chinese_word(
    person_name: str, hieroglyph: str, pinyin: str = "", tone: str = "",
    translation: str = "", status: str = "новое",
) -> None:
    """Add one hieroglyph to Ostap's Chinese-vocabulary board (ЖИЗНЬ >
    РУТИНА > КИТАЙСКИЙ > ОСТАП > ИЕРОГЛИФЫ) — a board-view Collection
    note; each word becomes its own child note, with pinyin/tone/status as
    labels and the translation as the note's own content."""
    client = get_client()
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


@_needs_trilium
async def add_book(person_name: str, title: str, author: str = "") -> str:
    """Add a new book note under КНИГИ, templated from _ШАБЛОН_КНИГА via a
    ~template RELATION (not a #template label — a relation's value is a
    real noteId, a label's is plain text; confirmed live which one the
    actual template uses) — that relation gives attribute inheritance
    (promoted attributes like readingStart/readingEnd render correctly),
    but NOT the template's body content: that's a Trilium client-side
    "duplicate on create" behavior that only fires from the desktop/web UI,
    never from the ETAPI create-note call (confirmed live — a note created
    via ETAPI with just the relation set came back with empty content, no
    "Об Авторе"/"Аннотация"/"Жанр"/"Похожие книги" sections at all). So the
    template's real content is fetched and copied in explicitly here.
    Returns the new note's id, for fill_book_details to target later."""
    client = get_client()
    knigi_id = await _find_note_id(client, "КНИГИ")
    template_id = await _find_note_id(client, "_ШАБЛОН_КНИГА")
    if knigi_id is None or template_id is None:
        raise TriliumNoteNotFoundError("Could not find КНИГИ or _ШАБЛОН_КНИГА.")

    template_content = await _get_content(client, template_id)

    create_resp = await client.post(
        f"{TRILIUM_URL}/etapi/create-note",
        json={"parentNoteId": knigi_id, "title": title, "type": "text", "content": template_content},
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
    return note_id


# Order matches the /addbook template message sent to the person (see
# app/prompts.py's BOOK_DETAILS_TEMPLATE) — their reply's paragraphs are
# taken by POSITION, not by matching these header strings back out of the
# reply (per explicit request, since the reply is always paragraph-per-
# section in this same order). Only used here to locate each section
# INSIDE the book note's own content, which does still use these exact
# <h2> headers (from _ШАБЛОН_КНИГА).
BOOK_DETAIL_HEADERS = ("Об Авторе", "Аннотация", "Жанр", "Похожие книги")


@_needs_trilium
async def fill_book_details(note_id: str, values: list[Optional[str]]) -> None:
    """Fills in whichever of the 4 template sections have a value (None =
    skip, leave whatever placeholder is already there) — values is
    positional, same order as BOOK_DETAIL_HEADERS. Each section is replaced
    wholesale, from its <h2> up to the next <h2> (or end of note), with a
    single normalized <h2>+<p> block — this cleanly overwrites whatever
    placeholder was there (an empty "&nbsp;" paragraph, the "scifi / drama /
    romance" example, or the "…" bullet list), regardless of its original
    markup shape."""
    client = get_client()
    content = await _get_content(client, note_id)
    for header, value in zip(BOOK_DETAIL_HEADERS, values):
        if not value:
            continue
        # Stop at the next <h2> *or* at the first <hr>: quotes added by
        # /quote and by the clippings import live after the last section
        # with no <h2> of their own, so a plain "up to the next heading"
        # rule made filling in «Похожие книги» swallow every quote in the
        # note (it did, once, and they had to be restored from a copy).
        pattern = re.compile(
            r"(<h2>" + re.escape(header) + r"</h2>).*?(?=<h2>|<hr>|$)", re.DOTALL,
        )
        replacement = r"\1" + f"<p>{html.escape(value)}</p>"
        content, count = pattern.subn(replacement, content, count=1)
        if count == 0:
            print(f"fill_book_details: header '{header}' not found in note {note_id}, skipped", flush=True)
    await _put_content(client, note_id, content)


_TAG_RE = re.compile(r"<[^>]+>")
_WHITESPACE_RE = re.compile(r"[ \t]+")


def _strip_html(fragment: str) -> str:
    text = _TAG_RE.sub("\n", fragment)
    text = html.unescape(text)
    text = _WHITESPACE_RE.sub(" ", text)
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line).strip()


@_needs_trilium
async def get_book_details(note_id: str) -> tuple[str, dict[str, str], list[str]]:
    """Reverse of fill_book_details — reads back the note's title plus
    whatever's currently under each of the 4 template sections (see
    BOOK_DETAIL_HEADERS), for showing a book's description on demand (see
    the /reading and /finished flows in app/service.py). A section with
    nothing meaningful filled in (still the original template placeholder,
    or genuinely empty) comes back as an empty string — the caller decides
    how to display that."""
    client = get_client()
    note_resp = await client.get(f"{TRILIUM_URL}/etapi/notes/{note_id}")
    note_resp.raise_for_status()
    title = note_resp.json().get("title") or "(без названия)"
    content = await _get_content(client, note_id)

    details = {}
    for header in BOOK_DETAIL_HEADERS:
        # Stops at the first <hr> as well as the next <h2>, for the same
        # reason fill_book_details does: quotes live after the last section
        # with no heading of their own. Without it «Похожие книги» read back
        # every quote in the note as part of the section, and a book with a
        # few of them pushed the description past Telegram's 4096-character
        # limit — the send just failed and tapping the book did nothing.
        match = re.search(
            r"<h2>" + re.escape(header) + r"</h2>(.*?)(?=<h2>|<hr>|$)", content, re.DOTALL,
        )
        text = _strip_html(match.group(1)) if match else ""
        # The un-filled template placeholders aren't real content — never
        # show "scifi / drama / romance" or "…" back as if they were an
        # actual answer.
        if text in ("scifi / drama / romance", "…"):
            text = ""
        details[header] = text
    return title, details, _extract_quotes(content)


@_needs_trilium
async def get_book_quotes(note_id: str) -> list[str]:
    """Just the interesting moments, for showing them on their own."""
    client = get_client()
    return _extract_quotes(await _get_content(client, note_id))


@_needs_trilium
async def set_reading_end(note_id: str, when: Optional[_date] = None) -> None:
    """Stamp a book as finished. The readingEnd label may already exist with
    an EMPTY value rather than not exist at all — clearing a promoted date
    field in Trilium's UI leaves the attribute attached (confirmed live, see
    get_active_reading_books) — so this PATCHes an existing attribute when
    there is one and only creates a new one otherwise; blindly creating
    would leave two competing readingEnd labels on the same note."""
    value = (when or datetime.now(TIMEZONE).date()).strftime("%Y-%m-%d")
    client = get_client()
    note_resp = await client.get(f"{TRILIUM_URL}/etapi/notes/{note_id}")
    note_resp.raise_for_status()
    existing = next(
        (a for a in note_resp.json().get("attributes", [])
         if a.get("type") == "label" and a.get("name") == "readingEnd"),
        None,
    )
    if existing is not None:
        patch_resp = await client.patch(
            f"{TRILIUM_URL}/etapi/attributes/{existing['attributeId']}", json={"value": value},
        )
        patch_resp.raise_for_status()
        return
    await _create_attribute(client, note_id, "label", "readingEnd", value)


@_needs_trilium
async def create_book_review_note(
    book_note_id: str, book_title: str, rating: int, review_text: str, person_name: str,
) -> None:
    """Create one review note for a finished book, living in BOTH places at
    once: physically created under the book itself, then CLONED into
    ОТЗЫВЫ НА КНИГИ via a second branch (Trilium's own multi-parent
    mechanism — one note, two positions in the tree, so edits in either
    place are the same note, rather than two copies that drift apart).

    Titled "{book} — отзыв" rather than reusing the book's exact title,
    which would make every exact-title lookup (_find_note_id) ambiguous
    between the book and its review."""
    client = get_client()
    reviews_id = await _find_note_id(client, "ОТЗЫВЫ НА КНИГИ")
    if reviews_id is None:
        raise TriliumNoteNotFoundError("Could not find the ОТЗЫВЫ НА КНИГИ note.")

    content = f"<p>{int(rating)}/10</p><p>{html.escape(review_text)}</p>"
    create_resp = await client.post(
        f"{TRILIUM_URL}/etapi/create-note",
        json={
            "parentNoteId": book_note_id,
            "title": f"{book_title} — отзыв",
            "type": "text",
            "content": content,
        },
    )
    create_resp.raise_for_status()
    note_id = create_resp.json()["note"]["noteId"]

    clone_resp = await client.post(
        f"{TRILIUM_URL}/etapi/branches",
        json={"noteId": note_id, "parentNoteId": reviews_id},
    )
    clone_resp.raise_for_status()

    await _create_attribute(client, note_id, "label", "rating", str(int(rating)))
    await _create_attribute(client, note_id, "label", "owner", person_name)


@_needs_trilium
async def _list_books(finished: bool) -> list[dict]:
    """Child notes of КНИГИ (see add_book), split by whether readingEnd is
    filled in. Same fetch-every-child-then-inspect-its-labels approach as
    read_kanban_status, since Trilium's ETAPI has no "search by missing
    label" filter.

    Checks the label's VALUE, not just its presence — clearing a promoted
    date field in Trilium's UI leaves the attribute attached with an empty
    string rather than removing it outright (confirmed live), which a plain
    "not in labels" check silently missed."""
    client = get_client()
    knigi_id = await _find_note_id(client, "КНИГИ")
    if knigi_id is None:
        raise TriliumNoteNotFoundError("Could not find the КНИГИ note.")

    knigi_resp = await client.get(f"{TRILIUM_URL}/etapi/notes/{knigi_id}")
    knigi_resp.raise_for_status()
    child_ids = knigi_resp.json().get("childNoteIds") or []

    books = []
    for child_id, child in zip(child_ids, await _get_notes(client, child_ids)):
        labels = {
            a["name"]: a["value"] for a in child.get("attributes", []) if a.get("type") == "label"
        }
        if not labels.get("readingStart"):
            continue
        if bool(labels.get("readingEnd")) != finished:
            continue
        books.append({"note_id": child_id, "title": child.get("title") or "(без названия)"})
    return books


def normalize_book_title(title: str) -> str:
    """Название, приведённое к виду, по которому книги сопоставляются:
    схлопнутые пробелы, нижний регистр. Публичное, потому что импорт
    выделений (app/service.py) сверяется с картой get_book_note_ids() сам,
    вместо запроса на каждую книгу из файла."""
    return " ".join(title.split()).strip().lower()


@_needs_trilium
async def inspect_note(title: str) -> dict:
    """Заголовок, лейблы и содержимое заметки по названию — чтобы можно
    было проверить, что именно бот записал в Trilium, не открывая его.
    Нужно всякий раз, когда «записалось не то»."""
    client = get_client()
    note_id = await _find_note_id(client, title)
    if note_id is None:
        raise TriliumNoteNotFoundError(f"No note titled '{title}' found.")
    resp = await client.get(f"{TRILIUM_URL}/etapi/notes/{note_id}")
    resp.raise_for_status()
    note = resp.json()
    return {
        "note_id": note_id,
        "title": note.get("title"),
        "labels": {a["name"]: a["value"] for a in note.get("attributes", [])
                   if a.get("type") == "label"},
        "content": await _get_content(client, note_id),
    }


@_needs_trilium
async def get_book_note_ids() -> dict[str, str]:
    """Вся библиотека под КНИГИ как «схлопнутое название → note_id».

    Сверка по схлопнутым пробелам и регистру: устройство берёт название из
    метаданных EPUB, и оно почти совпадает с заголовком заметки, но не
    побайтово. Здесь ВСЕ книги, а не только читаемые сейчас, — выделения
    приходят и из уже дочитанных.

    Карта целиком, а не поиск по одной книге, потому что импорт «My
    Clippings.txt» ищет сразу несколько: искать каждую отдельно значило
    обходить всю библиотеку заново на каждую книгу из файла."""
    client = get_client()
    knigi_id = await _find_note_id(client, "КНИГИ")
    if knigi_id is None:
        raise TriliumNoteNotFoundError("Could not find the КНИГИ note.")
    resp = await client.get(f"{TRILIUM_URL}/etapi/notes/{knigi_id}")
    resp.raise_for_status()
    child_ids = resp.json().get("childNoteIds") or []
    books: dict[str, str] = {}
    for child_id, child in zip(child_ids, await _get_notes(client, child_ids)):
        # setdefault, not assignment: the search this replaced returned the
        # FIRST child whose title matched, so two notes sharing a title keep
        # resolving to the same one they always did.
        books.setdefault(normalize_book_title(child.get("title") or ""), child_id)
    return books


async def find_book_note_id(title: str) -> Optional[str]:
    """Одна книга по названию — для случаев, где нужна ровно одна."""
    return (await get_book_note_ids()).get(normalize_book_title(title))


@_needs_trilium
async def get_note_labels(note_id: str) -> tuple[str, dict[str, str]]:
    """One note's title and label values — for the cases that already know
    which note they mean and only need to check its state, instead of
    listing every book just to find one of them in the list."""
    client = get_client()
    resp = await client.get(f"{TRILIUM_URL}/etapi/notes/{note_id}")
    resp.raise_for_status()
    note = resp.json()
    labels = {
        a["name"]: a["value"] for a in note.get("attributes", []) if a.get("type") == "label"
    }
    return note.get("title") or "(без названия)", labels


async def get_active_reading_books() -> list[dict]:
    """Books currently being read — readingStart set, readingEnd not."""
    return await _list_books(finished=False)


async def get_finished_books() -> list[dict]:
    """Books already read through — both readingStart and readingEnd set."""
    return await _list_books(finished=True)


_KEEP_TAGS = ("i", "b", "em", "strong", "code", "s", "u")
_BLOCK_END_RE = re.compile(r"</(?:p|div|li|h[1-6])>", re.IGNORECASE)
_ANY_TAG_RE = re.compile(r"</?([a-zA-Z0-9]+)[^>]*>")


def _to_telegram_html(fragment: str) -> str:
    """Trilium's stored HTML reduced to the handful of tags Telegram's HTML
    parse mode accepts. Block ends become newlines; every other tag is
    dropped but its text kept, so an unexpected <span> or <a> can't make
    Telegram reject the whole message as malformed markup."""
    text = _BLOCK_END_RE.sub("\n", fragment)

    def keep_or_drop(match: re.Match) -> str:
        return match.group(0) if match.group(1).lower() in _KEEP_TAGS else ""

    text = _ANY_TAG_RE.sub(keep_or_drop, text)
    lines = [line.strip() for line in text.splitlines()]
    return "\n".join(line for line in lines if line).strip()


def _extract_quotes(content: str) -> list[str]:
    """The book's interesting moments, one string per entry, ready to send.

    Entries are the <hr>-separated blocks after the «Интересные моменты»
    heading (see add_book_quote). Falls back to splitting from the first
    <hr> for a note the heading migration never reached."""
    start = content.find(QUOTES_HEADING)
    tail = content[start + len(QUOTES_HEADING):] if start != -1 else content[content.find("<hr>"):] if "<hr>" in content else ""
    return [q for block in tail.split("<hr>") if (q := _to_telegram_html(block))]


QUOTES_HEADING = "<h2>Интересные моменты</h2>"


def _with_quotes_heading(content: str) -> str:
    """Content with the «Интересные моменты» heading guaranteed present,
    sitting right after «Похожие книги» and before the first quote.

    Quotes used to be appended bare, with nothing separating them from the
    last template section — which is also why reading that section back
    swallowed them. The heading makes the boundary explicit in the note
    itself, so Trilium's own outline shows it too.

    Inserted before the first <hr> when there are already quotes, appended
    otherwise. Called on every append as well as by the one-off migration,
    so a note that somehow missed the migration heals itself the next time
    a quote lands in it."""
    if QUOTES_HEADING in content:
        return content
    divider = content.find("<hr>")
    if divider == -1:
        return content + QUOTES_HEADING
    return content[:divider] + QUOTES_HEADING + content[divider:]


def _quote_entry_html(quote_text: str, impression: str, location: str) -> str:
    """One 'interesting moment' entry: a divider, the quote verbatim in
    quotation marks, then the person's own answer about what they liked
    about it.

    location («с. 31, II МОНАШЕСКИЕ ПОДВИГИ») goes inside the quote's own
    paragraph, so a passage imported from the reader can be found again in
    the book. impression is optional: a highlight imported in bulk has
    nobody to comment on it at import time."""
    quoted = f"«{html.escape(quote_text)}»"
    if location:
        quoted += f" <i>({html.escape(location)})</i>"
    entry = "<hr>" + f"<p>{quoted}</p>"
    if impression:
        entry += f"<p>{html.escape(impression)}</p>"
    return entry


@_needs_trilium
async def add_book_quotes(note_id: str, entries: list[tuple[str, str, str]]) -> None:
    """Append several entries — (quote, impression, location) triples —
    straight into the book's own note content (not separate child notes,
    per explicit request), in ONE read-modify-write.

    Batched because the clippings import brings a whole book's highlights at
    once, and appending them one at a time meant re-reading and re-writing
    the entire note per quote — against a note that got longer with every
    one of them, so the cost grew as the square of the number imported."""
    if not entries:
        return
    client = get_client()
    existing = _with_quotes_heading(await _get_content(client, note_id))
    added = "".join(_quote_entry_html(*entry) for entry in entries)
    await _put_content(client, note_id, existing + added)


async def add_book_quote(
    note_id: str, quote_text: str, impression: str = "", location: str = "",
) -> None:
    """One entry — what /quote collects, a quote at a time."""
    await add_book_quotes(note_id, [(quote_text, impression, location)])


# Every ЕЖЕДНЕВНИК field with a human name, in the order the day fills
# them in — the same columns fill_ezhednevnik writes to, read back.
_EZHEDNEVNIK_DAY_VIEW = (
    ("3", "Как дела (день)"), ("4", "Балл"),
    ("5", "Как дела (вечер)"), ("6", "Балл"),
    ("2", "Заметное событие"),
    ("8", "Заметил в себе"), ("9", "Заметил на рынке"), ("10", "Заметил в новостях"),
    ("12", "Чему научился"), ("14", "Ошибки"),
)


@_needs_trilium
async def get_day_summary(person_name: str, day: Optional[_date] = None) -> str:
    """One day's row read back out of ЕЖЕДНЕВНИК — what's filled in and
    what's still blank. /week only ever showed aggregates over the week,
    so there was no way to see whether today's own check-ins had actually
    landed."""
    day = day or datetime.now(TIMEZONE).date()
    client = get_client()
    note_id = await _find_ezhednevnik_note_id(client, person_name)
    if note_id is None:
        raise TriliumNoteNotFoundError(f"Could not find the ЕЖЕДНЕВНИК note for {person_name}.")
    raw = await _get_content(client, note_id)

    cell_data = json.loads(raw)["workbook"]["sheets"]
    cell_data = next(iter(cell_data.values()))["cellData"]
    serial = (day - _date(1899, 12, 30)).days

    row = next(
        (r for k, r in cell_data.items()
         if k != "0" and r and _cell_value(r, "0") == serial
         and _cell_value(r, "15") == person_name),
        None,
    )
    header = f"<b>Ежедневник за {day.strftime('%d.%m')}:</b>"
    if row is None:
        return f"{header}\n\nЗа этот день ещё ничего не записано."

    lines = []
    for col, label in _EZHEDNEVNIK_DAY_VIEW:
        value = _cell_value(row, col)
        if value not in (None, ""):
            lines.append(f"<b>{label}:</b> {html.escape(str(value))}")
    if not lines:
        return f"{header}\n\nЗа этот день ещё ничего не записано."
    return header + "\n\n" + "\n".join(lines)


@_needs_trilium
async def get_week_summary(person_name: str) -> str:
    """Aggregate the current CALENDAR week (Monday through today — future
    days in the same week obviously have no data yet) from a person's own
    ЕЖЕДНЕВНИК note: average AM/PM scores, and every notable event / thing
    learned / mistake logged. Pure aggregation of what's actually there —
    never invents or paraphrases, same "list it literally" ethos as
    read_kanban_status."""
    client = get_client()
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


@_needs_trilium
async def log_activity(person_name: str, activity: str, feedback: str, score, duration: Optional[str] = None) -> None:
    """Write one yoga/chinese/trading session into that person's own
    ТРЕКЕР РУТИНЫ {ИМЯ} note — one row per day, activity/feedback/score in
    three adjacent columns per activity (see _ACTIVITY_COLS). Reuses
    today's row if one already exists (e.g. a second activity logged the
    same day) — a second SAME-activity log the same day just overwrites
    that activity's own three cells; multiple sessions of one activity in
    one day aren't tracked separately, by explicit choice."""
    activity_col, feedback_col, score_col = _ACTIVITY_COLS[activity]
    note_title = f"ТРЕКЕР РУТИНЫ {person_name}"

    client = get_client()
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
