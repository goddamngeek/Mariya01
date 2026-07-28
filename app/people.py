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


def format_reminder_message(sender_id: int, target_id: int, message: str) -> str:
    if sender_id == target_id:
        return f"Напоминание: {message}"

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
