import re

USER_NAMES = {
    335712401: "МАША",
    136382691: "ОСТАП",
}
NAME_TO_USER_ID = {name: user_id for user_id, name in USER_NAMES.items()}
# Grammatical gender per person, for correctly conjugating relayed
# cross-user messages ("Остап просил передать" vs "Маша просила передать").
USER_GENDER = {
    335712401: "f",  # МАША
    136382691: "m",  # ОСТАП
}


def format_reminder_message(
    sender_id: int, target_id: int, message: str, anonymous: bool = False,
) -> str:
    if sender_id == target_id:
        return f"Напоминание: {message}"

    if anonymous:
        # Delivered as if the bot itself is saying it — no "X просил
        # передать" attribution at all, per explicit request ("передай, но
        # не говори что от меня").
        return message.strip()

    sender_raw = USER_NAMES.get(sender_id, str(sender_id))
    # The model sometimes redundantly re-introduces the sender inside the
    # relayed text itself ("ОСТАП говорит, что ...") even though the bot
    # already prepends its own "<Sender> просил(а) передать, что ..." —
    # strip that if present rather than relying solely on prompting.
    message = re.sub(
        rf"^\s*{re.escape(sender_raw)}\s+(говорит|сказал[а]?|просил[а]?|пишет)\s*,?\s*(что\s+)?",
        "",
        message,
        flags=re.IGNORECASE,
    ).strip()

    sender_name = sender_raw.capitalize()
    verb = "просил" if USER_GENDER.get(sender_id) == "m" else "просила"
    return f"{sender_name} {verb} передать, что {message}"


# Кому передают задачу — «Остапу», а не «Остап»: на кнопке это действие, а
# не подпись. Словарём, потому что склонение двух имён правилами выводить
# дороже, чем перечислить.
NAME_DATIVE = {"МАША": "Маше", "ОСТАП": "Остапу"}


def dative(name: str) -> str:
    return NAME_DATIVE.get(name, name.capitalize())
