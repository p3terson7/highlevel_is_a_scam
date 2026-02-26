from app.db.models import Client


def ensure_booking_link(reply_text: str, client: Client) -> str:
    if not client.booking_url:
        return reply_text
    if client.booking_url in reply_text:
        return reply_text
    return f"{reply_text} Book here: {client.booking_url}".strip()


def handoff_suffix(client: Client) -> str:
    if not client.fallback_handoff_number:
        return ""
    return f" For immediate help, call {client.fallback_handoff_number}."


def calendar_booking_confirmed(_: str) -> bool:
    """
    Placeholder for calendar API lookup.
    Integrate your scheduler provider (e.g., Cal.com/Calendly/Google Calendar)
    and update lead state to BOOKED when a booking is confirmed.
    """
    return False
