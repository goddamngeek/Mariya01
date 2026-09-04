"""Куда бот отвечает.

Вся логика бота — service.py, планировщик, ветки, напоминания — зовёт эти
функции, а не app/telegram.py напрямую. Сегодня транспорт ровно один, и они
просто перенаправляют. Смысл не в том, что происходит сейчас, а в том, что
второй вход (веб-чат, Mattermost, что угодно) добавляется одной реализацией
и правкой _transport_for — а не правкой семидесяти вызовов, разбросанных по
коду.

Почему это вообще приходится писать самим: у бота один токен, а значит один
вебхук и один адрес API. Прислать боту сообщение снаружи легко, но ответ
уйдёт туда, куда настроен единственный адрес. Выбирать, куда отвечать, может
только сам бот — готовым инструментом такое не подставляется.
"""

from app import telegram

# Транспорт — это модуль с нужным набором функций; отдельного класса под
# один-единственный телеграм заводить незачем, интерфейс задан этим файлом.
_TELEGRAM = telegram


def _transport_for(chat_id: int | str):
    """Каким транспортом отвечать на этот адрес.

    Пока всегда телеграм. Когда появится второй вход, решать будет здесь — и
    по человеку (app/people.py), а не по самому chat_id: адреса у разных
    транспортов свои и совпадать не обязаны.
    """
    return _TELEGRAM


def _group_by_transport(
    entries: list[tuple[int | str, int]],
) -> list[tuple[object, list[tuple[int | str, int]]]]:
    """Разложить пары (chat_id, message_id) по транспортам, сохраняя порядок.
    Пока группа всегда одна, но чистка чата — единственное место, где в один
    вызов приходят адреса разных людей, и разъезжаться по транспортам она
    начнёт первой."""
    groups: list[tuple[object, list[tuple[int | str, int]]]] = []
    for chat_id, message_id in entries:
        transport = _transport_for(chat_id)
        for known, group in groups:
            if known is transport:
                group.append((chat_id, message_id))
                break
        else:
            groups.append((transport, [(chat_id, message_id)]))
    return groups


async def send_message(
    chat_id: int | str, text: str, parse_mode: str | None = None, log: bool = True,
) -> bool:
    return await _transport_for(chat_id).send_message(chat_id, text, parse_mode, log=log)


async def send_message_get_id(
    chat_id: int | str, text: str, parse_mode: str | None = None,
) -> int | None:
    return await _transport_for(chat_id).send_message_get_id(chat_id, text, parse_mode)


async def send_message_with_buttons(
    chat_id: int | str, text: str, buttons: list[tuple[str, str]],
    parse_mode: str | None = None, row_width: int = 1,
) -> int | None:
    return await _transport_for(chat_id).send_message_with_buttons(
        chat_id, text, buttons, parse_mode, row_width,
    )


async def edit_message(
    chat_id: int | str, message_id: int, text: str,
    buttons: list[tuple[str, str]] | None = None, parse_mode: str | None = None,
    row_width: int = 1,
) -> bool:
    return await _transport_for(chat_id).edit_message(
        chat_id, message_id, text, buttons, parse_mode, row_width,
    )


async def clear_reply_markup(chat_id: int | str, message_id: int) -> bool:
    return await _transport_for(chat_id).clear_reply_markup(chat_id, message_id)


async def delete_message(chat_id: int | str, message_id: int) -> bool:
    return await _transport_for(chat_id).delete_message(chat_id, message_id)


async def delete_messages(entries: list[tuple[int | str, int]]) -> None:
    for transport, group in _group_by_transport(entries):
        await transport.delete_messages(group)


async def answer_callback_query(callback_query_id: str, text: str | None = None) -> bool:
    """Ответ на нажатие кнопки — единственная функция без адреса: у нажатия
    есть только собственный идентификатор. Транспорт в нём подразумевается, и
    когда появится второй, сюда придётся передавать его явно. Отмечено, чтобы
    не выяснять это в тот же момент, когда всё остальное уже переехало."""
    return await _TELEGRAM.answer_callback_query(callback_query_id, text)

