"""Траты — строки в своей базе, наружу отдаваемые как beancount.

Почему не Firefly, из которого это переехало. Firefly — двойная запись: он
требует, чтобы книги сходились, и сообщает, когда не сошлись. Бухгалтеру это
страховка, двоим людям, вносящим траты руками, — работа, которой без него бы
не было. А окупиться она могла бы только на автоматическом импорте из банка,
которого у Озона для физлиц просто нет: ни CSV, ни OFX, ни API, только
PDF-справка. Значит ввод ручной навсегда, и проверка, которую нельзя
выключить, — чистый убыток.

В beancount проверки ставит автор. Не написал `balance` — ничего не «не
сходится», потому что нечему: у проводки одна нога без суммы, и она
вычисляется. Захотел сверить счёт с банком — пишешь `pad` и `balance`, и
разницу beancount подставляет сам. (Дата `pad` должна быть СТРОГО раньше даты
`balance`: утверждение проверяется на начало дня, и на одной дате pad
остаётся неиспользованным — проверено.)

Почему правда здесь, а не в файле. Бот живёт на Northflank, диск там
временный; Fava живёт на VPS, ей нужен файл. Тащить файл к боту означало бы
новый сервис, токен и блокировку двух писателей. Вместо этого строки лежат в
базе, которая у бота и так есть, а `render()` собирает из них файл, который
VPS забирает по расписанию. Писатель один — блокировка не нужна вовсе, и бот
ничего не ждёт по сети.

Цена решения: строки, собранные отсюда, нельзя править в Fava — следующая
выгрузка их перезапишет. Поэтому бот пишет в ОТДЕЛЬНЫЙ файл, подключённый
через `include` в личный, который ведут руками. Правка идёт через бота, а
всё, что написано руками, лежит в файле, которого бот не касается.

Счета разложены по людям (`Активы:Остап:…`), категории общие (`Расходы:Еда`).
Это не случайность: личные файлы складываются в общий одним `include`, и там
карты двоих обязаны остаться разными, а еда — наоборот, должна сложиться в
одну сумму на семью.
"""

from datetime import date, datetime
from decimal import Decimal

from beancount.core import amount as bc_amount, data as bc_data
from beancount.parser.printer import EntryPrinter

from app.config import LEDGER_CURRENCY, TIMEZONE
from app.db import get_pool, utcnow


async def add_expense(
    user_id: int,
    amount: str | Decimal,
    account: str,
    category: str,
    payee: str = "",
    narration: str = "",
    at: date | None = None,
    external_id: str | None = None,
) -> tuple[int, bool]:
    """Записать трату. Возвращает (id строки, записали ли её только что).

    external_id — защита от повтора: телеграм переотправляет апдейт, если бот
    не ответил за минуту, и без этого одна покупка попала бы в леджер дважды.
    На повторе возвращается id уже существующей строки и False — вызывающему
    это нужно, чтобы не прислать второе подтверждение на ту же покупку."""
    pool = await get_pool()
    if external_id:
        existing = await pool.fetchval(
            "SELECT id FROM ledger_entries WHERE user_id = $1 AND external_id = $2",
            user_id, external_id,
        )
        if existing is not None:
            return existing, False
    entry_id = await pool.fetchval(
        "INSERT INTO ledger_entries "
        "(user_id, entry_date, payee, narration, amount, currency, account, category, "
        " external_id, created_at) "
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10) RETURNING id",
        user_id, at or datetime.now(TIMEZONE).date(), payee.strip(), narration.strip(),
        Decimal(str(amount)), LEDGER_CURRENCY, account, category,
        external_id, utcnow(),
    )
    return entry_id, True


async def forget(entry_id: int, user_id: int) -> bool:
    """Убрать строку из выгрузки. Не DELETE: запись о том, что трата была и
    её отменили, стоит дешевле, чем восстановление по памяти."""
    pool = await get_pool()
    return await pool.fetchval(
        "UPDATE ledger_entries SET deleted_at = $1 "
        "WHERE id = $2 AND user_id = $3 AND deleted_at IS NULL RETURNING id",
        utcnow(), entry_id, user_id,
    ) is not None


async def _last_for_note(user_id: int, note: str, column: str) -> str | None:
    """Чем кончилась прошлая такая же трата.

    Это и есть замена двум вопросам из пяти: «креатин» во второй раз
    списывается с того же счёта и в ту же категорию, что и в первый, и
    спрашивать незачем. У StdioA то же самое сделано векторной базой и
    платным эмбеддинг-API; при двух людях точного совпадения текста хватает.

    Ключ — описание, а не получатель: люди пишут «потратил 659 на креатин»,
    а не название магазина, и склонять «в пятёрочке» надёжно всё равно
    нечем. column подставляется из кода, снаружи не приходит."""
    if not note.strip():
        return None
    pool = await get_pool()
    return await pool.fetchval(
        f"SELECT {column} FROM ledger_entries "
        "WHERE user_id = $1 AND lower(narration) = lower($2) AND deleted_at IS NULL "
        "ORDER BY id DESC LIMIT 1",
        user_id, note.strip(),
    )


async def account_for_note(user_id: int, note: str) -> str | None:
    return await _last_for_note(user_id, note, "account")


async def category_for_note(user_id: int, note: str) -> str | None:
    return await _last_for_note(user_id, note, "category")


async def get(entry_id: int, user_id: int):
    pool = await get_pool()
    return await pool.fetchrow(
        "SELECT * FROM ledger_entries WHERE id = $1 AND user_id = $2", entry_id, user_id,
    )


async def reclassify(entry_id: int, user_id: int, *, account=None, category=None) -> bool:
    """Поменять счёт или категорию уже записанной траты.

    На этом держится весь подход «записать сразу, уточнить потом». Запись
    покупки возможна только сейчас, пока помнишь; разложить по полкам можно
    когда угодно — и чаще всего не нужно вовсе. Поэтому трата уходит в
    леджер молча, с угаданными или дефолтными полями, а кнопки под
    подтверждением их правят.

    Побочно это же и обучение: поправленная строка становится ответом
    _last_for_note на следующую такую же покупку, и в третий раз бот угадает
    сам. Никакой векторной базы для этого не нужно."""
    if account is None and category is None:
        return False
    pool = await get_pool()
    return await pool.fetchval(
        "UPDATE ledger_entries SET "
        "account = coalesce($3, account), category = coalesce($4, category) "
        "WHERE id = $1 AND user_id = $2 AND deleted_at IS NULL RETURNING id",
        entry_id, user_id, account, category,
    ) is not None


async def spent(
    user_id: int, since: date, until: date, category: str | None = None,
) -> list[tuple[str, Decimal]]:
    """Сколько ушло за период, по категориям, больших первыми.

    Обычный SQL по своей же таблице — то, чего Firefly не давал ни дня:
    ответ в чате, без открытия интерфейса и без похода по сети."""
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT category, sum(amount) AS total FROM ledger_entries "
        "WHERE user_id = $1 AND entry_date BETWEEN $2 AND $3 AND deleted_at IS NULL "
        "AND ($4::text IS NULL OR category = $4) "
        "GROUP BY category ORDER BY total DESC",
        user_id, since, until, category,
    )
    return [(r["category"], r["total"]) for r in rows]


async def recent(user_id: int, limit: int = 10) -> list:
    pool = await get_pool()
    return await pool.fetch(
        "SELECT * FROM ledger_entries WHERE user_id = $1 AND deleted_at IS NULL "
        "ORDER BY id DESC LIMIT $2",
        user_id, limit,
    )


def _entry(row) -> bc_data.Transaction:
    """Строка базы — проводкой beancount.

    Собирается объектом и печатается EntryPrinter'ом, а не склейкой строк:
    печать — забота библиотеки, и она же гарантирует, что выравнивание,
    экранирование кавычек в названии магазина и формат числа окажутся
    такими, какие beancount потом сможет прочитать.

    У второй ноги суммы нет намеренно — beancount выводит её сам. Ровно
    поэтому проводка не может «не сойтись»: это тождество, а не проверка.

    Тэгов здесь нет и не будет: beancount принимает в них только латиницу
    (`#свидание` — Invalid token, проверено), а транслитерировать русское
    слово или молча его терять хуже, чем не заводить функцию. Понадобится
    пометка — она ложится в метаданные, там любой текст."""
    meta = bc_data.new_metadata("<bot>", 0, {"id": str(row["id"])})
    return bc_data.Transaction(
        meta,
        row["entry_date"],
        "*",
        row["payee"] or None,
        row["narration"] or "",
        bc_data.EMPTY_SET,
        bc_data.EMPTY_SET,
        [
            bc_data.Posting(
                row["category"],
                bc_amount.Amount(row["amount"], row["currency"]),
                None, None, None, None,
            ),
            bc_data.Posting(row["account"], None, None, None, None, None),
        ],
    )


async def render(user_id: int) -> str:
    """Все траты человека как текст beancount, старые первыми.

    Без `option` и `open` — они живут в личном файле, который этот
    подключает. Это не мелочь: кириллические корни счетов работают ТОЛЬКО
    при `option "name_assets"`, и держать их в перезаписываемом файле
    означало бы, что вся раскладка зависит от успеха последней выгрузки."""
    pool = await get_pool()
    rows = await pool.fetch(
        "SELECT * FROM ledger_entries WHERE user_id = $1 AND deleted_at IS NULL "
        "ORDER BY entry_date, id",
        user_id,
    )
    printer = EntryPrinter()
    head = (
        ";; Этот файл собирает бот из своей базы. Правки здесь пропадут при\n"
        ";; следующей выгрузке — поправить трату можно через бота, а всё, что\n"
        ";; пишется руками, лежит в файле, который этот подключает.\n"
        f";; Собран {datetime.now(TIMEZONE).strftime('%d.%m.%Y %H:%M')} МСК, "
        f"строк: {len(rows)}.\n\n"
    )
    return head + "\n".join(printer(_entry(r)) for r in rows)
