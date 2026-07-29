from app.people import format_reminder_message
from app.telegram import send_message


async def deliver_reminder(
    reminder_id: int, sender_id: int, target_id: int, message: str, anonymous: bool = False,
) -> None:
    """Format and send a reminder, logging (not raising) on failure — shared
    by the immediate ("now") delivery path in app/sync.py and the 60s poll
    in app/scheduler.py, which used to duplicate this exact sequence."""
    text = format_reminder_message(sender_id, target_id, message, anonymous)
    if not await send_message(target_id, text):
        print(f"failed to send reminder id={reminder_id}", flush=True)
