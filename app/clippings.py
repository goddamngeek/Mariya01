"""Разбор «My Clippings.txt» с читалки.

CrossInk (форк CrossPoint, стоящий на Xteink X4) сохраняет каждое выделение
в файл на карте в кindle-совместимом формате: заголовок с названием и
автором, строка с номером страницы и главой, сам текст и разделитель.

Файл копится вечно и никогда не чистится, поэтому «что из этого новое»
решается не по файлу, а по отпечатку каждого выделения — см.
app/db.py's imported_clippings. Дат в файле нет вовсе: прошивка хранит
метку времени у себя, но в этот экспорт её не выводит.

Перенос строк внутри текста — не нарезка по ширине экрана, как кажется:
прошивка собирает выделение пословно и ставит перевод строки там, где
видит разрыв абзаца, склеивая визуально слитые слова без пробела и
сращивая половинки перенесённого по дефису слова. Так что склейка строк
через пробел — ровно то, что делает сама прошивка. На реальном файле из
99 переносов пострадало одно слово.
"""

import re
from dataclasses import dataclass

SEPARATOR = "=========="

# «Название книги (Автор)» — автор в последних скобках, чтобы скобки внутри
# самого названия не сбивали разбор.
_HEADER_RE = re.compile(r"^(?P<title>.*?)\s*\((?P<author>[^()]*)\)\s*$")
_PAGE_RE = re.compile(r"\bPage\s+(\d+)", re.IGNORECASE)
_SECTION_RE = re.compile(r"\|\s*(?P<section>.+?)\s*$")
_KIND_RE = re.compile(r"-\s*Your\s+(?P<kind>\w+)", re.IGNORECASE)


@dataclass(frozen=True)
class Clipping:
    book_title: str
    book_author: str
    text: str
    page: str | None
    section: str | None

    @property
    def location(self) -> str:
        """«с. 31, II МОНАШЕСКИЕ ПОДВИГИ» — то, что дописывается к цитате."""
        parts = []
        if self.page:
            parts.append(f"с. {self.page}")
        if self.section:
            parts.append(self.section)
        return ", ".join(parts)


def parse(raw: str) -> list[Clipping]:
    """Всё, что удалось разобрать. Записи без текста (закладки без
    выделения) и всё, что не является выделением, пропускаются молча —
    файл пишет устройство, и спорить с ним не о чем."""
    out: list[Clipping] = []
    for chunk in raw.split(SEPARATOR):
        lines = [line.rstrip() for line in chunk.strip("\n").split("\n")]
        if len(lines) < 3:
            continue

        header, meta = lines[0].strip(), lines[1].strip()
        if not header or not meta.lstrip().startswith("-"):
            continue

        kind = _KIND_RE.search(meta)
        if kind and kind.group("kind").lower() != "highlight":
            continue  # закладка без текста, заметка и т.п.

        body = " ".join(line.strip() for line in lines[2:] if line.strip())
        body = re.sub(r"\s+", " ", body).strip()
        if not body:
            continue

        matched = _HEADER_RE.match(header)
        title = (matched.group("title") if matched else header).strip()
        author = (matched.group("author") if matched else "").strip()
        page = _PAGE_RE.search(meta)
        section = _SECTION_RE.search(meta)
        out.append(Clipping(
            book_title=title,
            book_author=author,
            text=body,
            page=page.group(1) if page else None,
            section=section.group("section") if section else None,
        ))
    return out


def fingerprint(clipping: Clipping) -> str:
    """Отпечаток для отсева уже импортированного. По книге и тексту, а не
    по странице: страница у одного и того же выделения может съехать после
    смены шрифта или размера экрана, а текст — нет."""
    import hashlib

    key = f"{clipping.book_title.strip().lower()}\x00{clipping.text.strip()}"
    return hashlib.sha256(key.encode("utf-8")).hexdigest()[:40]
