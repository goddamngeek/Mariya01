from datetime import timedelta

from app.db import claim_reminder, insert_reminder, utcnow
from app.people import format_reminder_message
from app.channel import send_message


async def deliver_reminder(
    reminder_id: int, sender_id: int, target_id: int, message: str, anonymous: bool = False,
) -> None:
    """Format and send a reminder, logging (not raising) on failure — shared
    by the immediate ("now") delivery path in app/sync.py and the 60s poll
    in app/scheduler.py, which used to duplicate this exact sequence."""
    text = format_reminder_message(sender_id, target_id, message, anonymous)
    if not await send_message(target_id, text):
        print(f"failed to send reminder id={reminder_id}", flush=True)


async def schedule_reminder(
    sender_id: int, target_id: int, message: str, run_at, anonymous: bool = False,
) -> int:
    """Insert a reminder and deliver it immediately if it's already due —
    shared by app/sync.py's /schedule_reminder endpoint (external/manual
    trigger) and app/ingest.py's relay/reminder intent (real-time, no
    Odysseus involved). `run_at` must already be a UTC-aware datetime."""
    reminder_id = await insert_reminder(target_id, sender_id, message, run_at, anonymous)

    # Due right now (or already past) — don't make the person wait for the
    # next 60s poll tick. claim_reminder() is the atomic gate: if
    # release_due_reminders() happens to tick at the same moment and wins
    # the claim first, we just skip — it's already being sent, no double
    # delivery.
    if run_at <= utcnow() + timedelta(seconds=30) and await claim_reminder(reminder_id):
        await deliver_reminder(reminder_id, sender_id, target_id, message, anonymous)

    return reminder_id
