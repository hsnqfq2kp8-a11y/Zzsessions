from __future__ import annotations

import sqlite3
import threading
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from pathlib import Path
from typing import Any


@dataclass
class Slot:
    id: int
    slot_date: str
    start_time: str
    end_time: str
    is_active: int
    created_by: int | None = None
    created_at: str | None = None


@dataclass
class Booking:
    id: int
    slot_id: int
    client_user_id: int
    client_chat_id: int
    client_name: str
    client_telegram: str
    session_type: str
    status: str
    created_at: str
    cancelled_at: str | None
    day_reminder_sent: int
    hour_reminder_sent: int
    start_notice_sent: int
    slot_date: str
    start_time: str
    end_time: str


class Database:
    def __init__(self, db_path: Path) -> None:
        self._lock = threading.RLock()
        self.conn = sqlite3.connect(str(db_path), check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        with self.conn:
            self.conn.execute("PRAGMA journal_mode=WAL;")
            self.conn.execute("PRAGMA foreign_keys=ON;")
        self.init_db()

    @staticmethod
    def _normalize_date_str(value: str) -> str:
        raw = (value or "").strip()
        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw, fmt).date().isoformat()
            except ValueError:
                continue
        raise ValueError(f"Unsupported date format: {value}")

    @staticmethod
    def _parse_hhmm(value: str) -> tuple[int, int]:
        hour_str, minute_str = value.strip().split(":")
        return int(hour_str), int(minute_str)

    @staticmethod
    def _map_session_hour(hour_value: int) -> int:
        if 5 <= hour_value <= 11:
            return hour_value + 12
        if hour_value == 12:
            return 0
        return hour_value

    def _build_range_datetimes(self, slot_date: str, start_time: str, end_time: str) -> tuple[datetime, datetime]:
        normalized_date = self._normalize_date_str(slot_date)
        base_date = date.fromisoformat(normalized_date)

        start_hour_raw, start_minute = self._parse_hhmm(start_time)
        end_hour_raw, end_minute = self._parse_hhmm(end_time)

        explicit_24h = start_hour_raw >= 13 or end_hour_raw >= 13

        if explicit_24h:
            start_hour_actual = start_hour_raw
            end_hour_actual = end_hour_raw
        else:
            start_hour_actual = self._map_session_hour(start_hour_raw)
            end_hour_actual = self._map_session_hour(end_hour_raw)

        start_dt = datetime.combine(base_date, time(start_hour_actual, start_minute))
        end_dt = datetime.combine(base_date, time(end_hour_actual, end_minute))

        if end_dt <= start_dt:
            end_dt += timedelta(days=1)

        return start_dt, end_dt

    def _compose_now_dt(self, now_date: str, now_time: str) -> datetime:
        normalized_date = self._normalize_date_str(now_date)
        now_clock = time.fromisoformat(now_time)
        return datetime.combine(date.fromisoformat(normalized_date), now_clock)

    def init_db(self) -> None:
        with self._lock, self.conn:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS slots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slot_date TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_by INTEGER,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(slot_date, start_time, end_time)
                );

                CREATE TABLE IF NOT EXISTS bookings (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slot_id INTEGER NOT NULL,
                    client_user_id INTEGER NOT NULL,
                    client_chat_id INTEGER NOT NULL,
                    client_name TEXT NOT NULL,
                    client_telegram TEXT NOT NULL,
                    session_type TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'confirmed',
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    cancelled_at TEXT,
                    day_reminder_sent INTEGER NOT NULL DEFAULT 0,
                    hour_reminder_sent INTEGER NOT NULL DEFAULT 0,
                    start_notice_sent INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(slot_id) REFERENCES slots(id)
                );

                CREATE INDEX IF NOT EXISTS idx_slots_date_active
                ON slots(slot_date, is_active);

                CREATE INDEX IF NOT EXISTS idx_bookings_slot_status
                ON bookings(slot_id, status);

                CREATE INDEX IF NOT EXISTS idx_bookings_user_status
                ON bookings(client_user_id, status);
                """
            )

            cur = self.conn.execute("SELECT value FROM settings WHERE key='booking_open'")
            if cur.fetchone() is None:
                self.conn.execute(
                    "INSERT INTO settings(key, value) VALUES('booking_open', '1')"
                )

            self._migrate_slot_dates()

    def _migrate_slot_dates(self) -> None:
        rows = self.conn.execute(
            """
            SELECT id, slot_date, start_time, end_time
            FROM slots
            ORDER BY id
            """
        ).fetchall()

        for row in rows:
            old_date = row["slot_date"]
            try:
                new_date = self._normalize_date_str(old_date)
            except ValueError:
                continue

            if new_date == old_date:
                continue

            try:
                self.conn.execute(
                    "UPDATE slots SET slot_date=? WHERE id=?",
                    (new_date, row["id"]),
                )
            except sqlite3.IntegrityError:
                canonical = self.conn.execute(
                    """
                    SELECT id
                    FROM slots
                    WHERE slot_date=? AND start_time=? AND end_time=? AND id<>?
                    ORDER BY id
                    LIMIT 1
                    """,
                    (new_date, row["start_time"], row["end_time"], row["id"]),
                ).fetchone()

                if canonical:
                    canonical_id = canonical["id"]
                    self.conn.execute(
                        "UPDATE bookings SET slot_id=? WHERE slot_id=?",
                        (canonical_id, row["id"]),
                    )
                    self.conn.execute(
                        "DELETE FROM slots WHERE id=?",
                        (row["id"],),
                    )

    def set_booking_open(self, is_open: bool) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE settings SET value=? WHERE key='booking_open'",
                ("1" if is_open else "0",),
            )

    def is_booking_open(self) -> bool:
        with self._lock:
            row = self.conn.execute(
                "SELECT value FROM settings WHERE key='booking_open'"
            ).fetchone()
            return (row["value"] if row else "1") == "1"

    def upsert_slot(self, slot_date: str, start_time: str, end_time: str, created_by: int) -> str:
        normalized_date = self._normalize_date_str(slot_date)

        with self._lock, self.conn:
            existing = self.conn.execute(
                """
                SELECT id, is_active
                FROM slots
                WHERE slot_date=? AND start_time=? AND end_time=?
                """,
                (normalized_date, start_time, end_time),
            ).fetchone()

            if existing is None:
                self.conn.execute(
                    """
                    INSERT INTO slots(slot_date, start_time, end_time, is_active, created_by)
                    VALUES (?, ?, ?, 1, ?)
                    """,
                    (normalized_date, start_time, end_time, created_by),
                )
                return "created"

            if existing["is_active"] == 0:
                self.conn.execute(
                    """
                    UPDATE slots
                    SET is_active=1, created_by=?, created_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (created_by, existing["id"]),
                )
                return "reactivated"

            return "exists"

    def get_first_available_month(self, now_date: str, now_time: str) -> tuple[int, int] | None:
        now_dt = self._compose_now_dt(now_date, now_time)

        with self._lock:
            rows = self.conn.execute(
                """
                SELECT s.slot_date, s.start_time, s.end_time
                FROM slots s
                WHERE s.is_active=1
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bookings b
                      WHERE b.slot_id=s.id AND b.status='confirmed'
                  )
                """
            ).fetchall()

        candidates: list[datetime] = []
        for row in rows:
            start_dt, end_dt = self._build_range_datetimes(row["slot_date"], row["start_time"], row["end_time"])
            if end_dt > now_dt:
                candidates.append(start_dt)

        if not candidates:
            return None

        first_dt = min(candidates)
        return first_dt.year, first_dt.month

    def get_available_dates_for_month(self, year: int, month: int, now_date: str, now_time: str) -> set[int]:
        now_dt = self._compose_now_dt(now_date, now_time)

        with self._lock:
            rows = self.conn.execute(
                """
                SELECT s.slot_date, s.start_time, s.end_time
                FROM slots s
                WHERE s.is_active=1
                  AND SUBSTR(s.slot_date, 1, 7)=?
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bookings b
                      WHERE b.slot_id=s.id AND b.status='confirmed'
                  )
                """,
                (f"{year:04d}-{month:02d}",),
            ).fetchall()

        available_days: set[int] = set()
        for row in rows:
            start_dt, end_dt = self._build_range_datetimes(row["slot_date"], row["start_time"], row["end_time"])
            if end_dt > now_dt:
                available_days.add(int(row["slot_date"][8:10]))

        return available_days

    def get_manager_dates_for_month(self, year: int, month: int, now_date: str) -> set[int]:
        normalized_now_date = self._normalize_date_str(now_date)

        with self._lock:
            rows = self.conn.execute(
                """
                SELECT DISTINCT CAST(SUBSTR(slot_date, 9, 2) AS INTEGER) AS day
                FROM slots
                WHERE slot_date >= ?
                  AND SUBSTR(slot_date, 1, 7)=?
                ORDER BY day
                """,
                (normalized_now_date, f"{year:04d}-{month:02d}"),
            ).fetchall()
            return {int(r["day"]) for r in rows}

    def get_available_slots(self, slot_date: str, now_date: str | None = None, now_time: str | None = None) -> list[Slot]:
        normalized_date = self._normalize_date_str(slot_date)
        now_dt = self._compose_now_dt(now_date, now_time) if now_date and now_time else None

        with self._lock:
            rows = self.conn.execute(
                """
                SELECT s.*
                FROM slots s
                WHERE s.slot_date=?
                  AND s.is_active=1
                  AND NOT EXISTS (
                      SELECT 1
                      FROM bookings b
                      WHERE b.slot_id=s.id AND b.status='confirmed'
                  )
                """,
                (normalized_date,),
            ).fetchall()

        slots: list[tuple[datetime, Slot]] = []
        for row in rows:
            slot = Slot(**dict(row))
            start_dt, end_dt = self._build_range_datetimes(slot.slot_date, slot.start_time, slot.end_time)
            if now_dt is None or end_dt > now_dt:
                slots.append((start_dt, slot))

        slots.sort(key=lambda item: item[0])
        return [slot for _, slot in slots]

    def get_all_slots_for_date(self, slot_date: str) -> list[Slot]:
        normalized_date = self._normalize_date_str(slot_date)

        with self._lock:
            rows = self.conn.execute(
                """
                SELECT *
                FROM slots
                WHERE slot_date=? AND is_active=1
                """,
                (normalized_date,),
            ).fetchall()

        slots: list[tuple[datetime, Slot]] = []
        for row in rows:
            slot = Slot(**dict(row))
            start_dt, _ = self._build_range_datetimes(slot.slot_date, slot.start_time, slot.end_time)
            slots.append((start_dt, slot))

        slots.sort(key=lambda item: item[0])
        return [slot for _, slot in slots]

    def get_slot(self, slot_id: int) -> Slot | None:
        with self._lock:
            row = self.conn.execute(
                "SELECT * FROM slots WHERE id=?",
                (slot_id,),
            ).fetchone()
            return Slot(**dict(row)) if row else None

    def remove_slot(self, slot_id: int) -> tuple[bool, str]:
        with self._lock, self.conn:
            row = self.conn.execute(
                "SELECT * FROM slots WHERE id=?",
                (slot_id,),
            ).fetchone()
            if not row:
                return False, "not_found"

            active_booking = self.conn.execute(
                """
                SELECT 1
                FROM bookings
                WHERE slot_id=? AND status='confirmed'
                """,
                (slot_id,),
            ).fetchone()

            if active_booking:
                return False, "booked"

            self.conn.execute(
                "UPDATE slots SET is_active=0 WHERE id=?",
                (slot_id,),
            )
            return True, "removed"

    def remove_day(self, slot_date: str) -> tuple[bool, str, int]:
        normalized_date = self._normalize_date_str(slot_date)

        with self._lock, self.conn:
            active_booking = self.conn.execute(
                """
                SELECT 1
                FROM bookings b
                JOIN slots s ON s.id=b.slot_id
                WHERE s.slot_date=? AND b.status='confirmed'
                LIMIT 1
                """,
                (normalized_date,),
            ).fetchone()

            if active_booking:
                return False, "booked", 0

            cur = self.conn.execute(
                """
                UPDATE slots
                SET is_active=0
                WHERE slot_date=? AND is_active=1
                """,
                (normalized_date,),
            )
            return True, "removed", cur.rowcount

    def create_booking(
        self,
        slot_id: int,
        client_user_id: int,
        client_chat_id: int,
        client_name: str,
        client_telegram: str,
        session_type: str,
    ) -> tuple[bool, int | None]:
        with self._lock, self.conn:
            slot = self.conn.execute(
                """
                SELECT *
                FROM slots
                WHERE id=? AND is_active=1
                """,
                (slot_id,),
            ).fetchone()

            if slot is None:
                return False, None

            existing = self.conn.execute(
                """
                SELECT 1
                FROM bookings
                WHERE slot_id=? AND status='confirmed'
                """,
                (slot_id,),
            ).fetchone()

            if existing:
                return False, None

            cur = self.conn.execute(
                """
                INSERT INTO bookings(
                    slot_id,
                    client_user_id,
                    client_chat_id,
                    client_name,
                    client_telegram,
                    session_type,
                    status
                ) VALUES (?, ?, ?, ?, ?, ?, 'confirmed')
                """,
                (
                    slot_id,
                    client_user_id,
                    client_chat_id,
                    client_name,
                    client_telegram,
                    session_type,
                ),
            )
            return True, int(cur.lastrowid)

    def get_booking(self, booking_id: int) -> Booking | None:
        with self._lock:
            row = self.conn.execute(
                """
                SELECT
                    b.*,
                    s.slot_date,
                    s.start_time,
                    s.end_time
                FROM bookings b
                JOIN slots s ON s.id=b.slot_id
                WHERE b.id=?
                """,
                (booking_id,),
            ).fetchone()
            return Booking(**dict(row)) if row else None

    def get_user_upcoming_bookings(self, client_user_id: int, now_date: str, now_time: str) -> list[Booking]:
        now_dt = self._compose_now_dt(now_date, now_time)

        with self._lock:
            rows = self.conn.execute(
                """
                SELECT
                    b.*,
                    s.slot_date,
                    s.start_time,
                    s.end_time
                FROM bookings b
                JOIN slots s ON s.id=b.slot_id
                WHERE b.client_user_id=?
                  AND b.status='confirmed'
                """,
                (client_user_id,),
            ).fetchall()

        bookings: list[tuple[datetime, Booking]] = []
        for row in rows:
            booking = Booking(**dict(row))
            start_dt, end_dt = self._build_range_datetimes(booking.slot_date, booking.start_time, booking.end_time)
            if end_dt > now_dt:
                bookings.append((start_dt, booking))

        bookings.sort(key=lambda item: item[0])
        return [booking for _, booking in bookings]

    def get_all_upcoming_bookings(self, now_date: str, now_time: str) -> list[Booking]:
        now_dt = self._compose_now_dt(now_date, now_time)

        with self._lock:
            rows = self.conn.execute(
                """
                SELECT
                    b.*,
                    s.slot_date,
                    s.start_time,
                    s.end_time
                FROM bookings b
                JOIN slots s ON s.id=b.slot_id
                WHERE b.status='confirmed'
                """
            ).fetchall()

        bookings: list[tuple[datetime, Booking]] = []
        for row in rows:
            booking = Booking(**dict(row))
            start_dt, end_dt = self._build_range_datetimes(booking.slot_date, booking.start_time, booking.end_time)
            if end_dt > now_dt:
                bookings.append((start_dt, booking))

        bookings.sort(key=lambda item: item[0])
        return [booking for _, booking in bookings]

    def cancel_booking(self, booking_id: int, by_user_id: int | None = None) -> tuple[bool, str]:
        with self._lock, self.conn:
            booking = self.conn.execute(
                "SELECT * FROM bookings WHERE id=?",
                (booking_id,),
            ).fetchone()

            if booking is None:
                return False, "not_found"

            if booking["status"] != "confirmed":
                return False, "already_cancelled"

            if by_user_id is not None and int(booking["client_user_id"]) != int(by_user_id):
                return False, "forbidden"

            self.conn.execute(
                """
                UPDATE bookings
                SET status='cancelled', cancelled_at=CURRENT_TIMESTAMP
                WHERE id=?
                """,
                (booking_id,),
            )
            return True, "cancelled"

    def get_due_notifications(self, now_dt: datetime) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT
                    b.*,
                    s.slot_date,
                    s.start_time,
                    s.end_time
                FROM bookings b
                JOIN slots s ON s.id=b.slot_id
                WHERE b.status='confirmed'
                """
            ).fetchall()

        due: list[dict[str, Any]] = []

        for row in rows:
            booking = Booking(**dict(row))
            start_dt, end_dt = self._build_range_datetimes(booking.slot_date, booking.start_time, booking.end_time)

            if booking.hour_reminder_sent == 0 and start_dt > now_dt and now_dt >= start_dt - timedelta(hours=1):
                due.append({"kind": "hour", "booking": booking})

            if booking.start_notice_sent == 0 and start_dt <= now_dt < end_dt:
                due.append({"kind": "start", "booking": booking})

        return due

    def mark_notification_sent(self, booking_id: int, kind: str) -> None:
        column = {
            "day": "day_reminder_sent",
            "hour": "hour_reminder_sent",
            "start": "start_notice_sent",
        }[kind]

        with self._lock, self.conn:
            self.conn.execute(
                f"UPDATE bookings SET {column}=1 WHERE id=?",
                (booking_id,),
            )