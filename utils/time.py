from datetime import datetime
from zoneinfo import ZoneInfo

def format_duration(seconds):
    # this function formats a duration in seconds into a human-readable string
    # e.g. 3661 seconds -> "1 h 1 min 1 sec"
    # e.g. 60 seconds -> "1 min"
    # e.g. 0 seconds -> "0 sec"

    seconds = int(seconds)
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h:
        parts.append(f"{h} h")
    if m:
        parts.append(f"{m} min")
    if s or not parts:
        parts.append(f"{s} sec")
    return ' '.join(parts)


def formatted_date():
    # Choose a time zone (e.g., New York)
    now = datetime.now(ZoneInfo("America/New_York"))
    # Format: April 21, 2025 at 03:45 PM EDT
    now = now.strftime("%B %d, %Y at %I:%M %p %Z")
    return now