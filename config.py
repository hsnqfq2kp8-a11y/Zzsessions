from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from zoneinfo import ZoneInfo


@dataclass(frozen=True)
class Settings:
    bot_token: str
    manager_ids: tuple[int, ...]
    timezone_name: str
    timezone: ZoneInfo
    secondary_timezone_name: str | None
    secondary_timezone_label: str | None
    secondary_timezone: ZoneInfo | None
    db_path: Path
    check_interval_seconds: int
    booking_title: str


def _parse_manager_ids(raw: str) -> tuple[int, ...]:
    ids: list[int] = []
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        try:
            ids.append(int(chunk))
        except ValueError as exc:
            raise RuntimeError(f"Invalid MANAGER_IDS value: {chunk}") from exc
    if not ids:
        raise RuntimeError("MANAGER_IDS must contain at least one Telegram user ID")
    return tuple(ids)


def load_settings() -> Settings:
    bot_token = os.getenv("BOT_TOKEN", "").strip()
    if not bot_token:
        raise RuntimeError("BOT_TOKEN is missing")

    manager_ids = _parse_manager_ids(os.getenv("MANAGER_IDS", ""))

    timezone_name = os.getenv("TIMEZONE", "Asia/Riyadh").strip() or "Asia/Riyadh"
    timezone = ZoneInfo(timezone_name)

    secondary_timezone_name = os.getenv("SECONDARY_TIMEZONE", "Africa/Casablanca").strip() or None
    secondary_timezone_label = os.getenv("SECONDARY_TIMEZONE_LABEL", "توقيت المغرب").strip() or None
    secondary_timezone = ZoneInfo(secondary_timezone_name) if secondary_timezone_name else None

    db_path = Path(os.getenv("DB_PATH", "data/bot.db")).expanduser().resolve()
    db_path.parent.mkdir(parents=True, exist_ok=True)

    check_interval_seconds = int(os.getenv("REMINDER_CHECK_INTERVAL_SECONDS", "60"))
    booking_title = os.getenv("BOOKING_TITLE", "جلسة علاجية").strip() or "جلسة علاجية"

    return Settings(
        bot_token=bot_token,
        manager_ids=manager_ids,
        timezone_name=timezone_name,
        timezone=timezone,
        secondary_timezone_name=secondary_timezone_name,
        secondary_timezone_label=secondary_timezone_label,
        secondary_timezone=secondary_timezone,
        db_path=db_path,
        check_interval_seconds=check_interval_seconds,
        booking_title=booking_title,
    )