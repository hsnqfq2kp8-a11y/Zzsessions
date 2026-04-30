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
    admin_day: str | None
    daily_sequence: int | None
    cancellation_reason: str | None
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
    def _normalize_time_str(value: str) -> str:
        raw = (value or "").strip()
        if ":" not in raw:
            raise ValueError(f"Unsupported time format: {value}")
        hh, mm = raw.split(":", 1)
        hour = int(hh)
        minute = int(mm)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            raise ValueError(f"Time out of range: {value}")
        return f"{hour:02d}:{minute:02d}"

    def _slot_start_dt(self, slot_date: str, start_time: str) -> datetime:
        return datetime.combine(
            date.fromisoformat(self._normalize_date_str(slot_date)),
            time.fromisoformat(self._normalize_time_str(start_time)),
        )

    def _slot_end_dt(self, slot_date: str, start_time: str, end_time: str) -> datetime:
        start_dt = self._slot_start_dt(slot_date, start_time)
        end_dt = datetime.combine(
            date.fromisoformat(self._normalize_date_str(slot_date)),
            time.fromisoformat(self._normalize_time_str(end_time)),
        )
        if end_dt <= start_dt:
            end_dt += timedelta(days=1)
        return end_dt

    def _compose_now_dt(self, now_date: str, now_time: str) -> datetime:
        return datetime.combine(
            date.fromisoformat(self._normalize_date_str(now_date)),
            time.fromisoformat(self._normalize_time_str(now_time)),
        )

    def _min_bookable_dt(self, now_date: str, now_time: str) -> datetime:
        return self._compose_now_dt(now_date, now_time) + timedelta(days=1)

    def _admin_day_for_slot(self, slot_date: str, start_time: str) -> str:
        start_dt = self._slot_start_dt(slot_date, start_time)
        if start_dt.time() < time(4, 0):
            return (start_dt.date() - timedelta(days=1)).isoformat()
        return start_dt.date().isoformat()

    def init_db(self) -> None:
        with self._lock, self.conn:
            self.conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS user_profiles (
                    user_id INTEGER PRIMARY KEY,
                    country_name TEXT NOT NULL,
                    timezone_name TEXT NOT NULL,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS slots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    slot_date TEXT NOT NULL,
                    start_time TEXT NOT NULL,
                    end_time TEXT NOT NULL,
                    is_active INTEGER NOT NULL DEFAULT 1,
                    created_by INTEGER,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(slot_date, start_time)
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
                    admin_day TEXT,
                    daily_sequence INTEGER,
                    cancellation_reason TEXT,
                    FOREIGN KEY(slot_id) REFERENCES slots(id)
                );

                CREATE INDEX IF NOT EXISTS idx_slots_date_active ON slots(slot_date, is_active);
                CREATE INDEX IF NOT EXISTS idx_bookings_slot_status ON bookings(slot_id, status);
                CREATE INDEX IF NOT EXISTS idx_bookings_user_status ON bookings(client_user_id, status);
                CREATE INDEX IF NOT EXISTS idx_bookings_admin_day_seq ON bookings(admin_day, daily_sequence);
                """
            )

            cur = self.conn.execute("SELECT value FROM settings WHERE key='booking_open'")
            if cur.fetchone() is None:
                self.conn.execute("INSERT INTO settings(key, value) VALUES('booking_open', '1')")

            self._migrate_slots()
            self._migrate_bookings()

    def _migrate_slots(self) -> None:
        rows = self.conn.execute(
            "SELECT id, slot_date, start_time, end_time FROM slots ORDER BY id"
        ).fetchall()

        for row in rows:
            try:
                new_date = self._normalize_date_str(row["slot_date"])
                new_start = self._normalize_time_str(row["start_time"])
                new_end = self._normalize_time_str(row["end_time"])
            except ValueError:
                continue

            if new_date == row["slot_date"] and new_start == row["start_time"] and new_end == row["end_time"]:
                continue

            try:
                self.conn.execute(
                    "UPDATE slots SET slot_date=?, start_time=?, end_time=? WHERE id=?",
                    (new_date, new_start, new_end, row["id"]),
                )
            except sqlite3.IntegrityError:
                canonical = self.conn.execute(
                    """
                    SELECT id
                    FROM slots
                    WHERE slot_date=? AND start_time=? AND id<>?
                    ORDER BY id
                    LIMIT 1
                    """,
                    (new_date, new_start, row["id"]),
                ).fetchone()
                if canonical:
                    self.conn.execute(
                        "UPDATE bookings SET slot_id=? WHERE slot_id=?",
                        (canonical["id"], row["id"]),
                    )
                    self.conn.execute("DELETE FROM slots WHERE id=?", (row["id"],))

    def _migrate_bookings(self) -> None:
        columns = {
            row["name"]
            for row in self.conn.execute("PRAGMA table_info(bookings)").fetchall()
        }

        if "admin_day" not in columns:
            self.conn.execute("ALTER TABLE bookings ADD COLUMN admin_day TEXT")
        if "daily_sequence" not in columns:
            self.conn.execute("ALTER TABLE bookings ADD COLUMN daily_sequence INTEGER")
        if "cancellation_reason" not in columns:
            self.conn.execute("ALTER TABLE bookings ADD COLUMN cancellation_reason TEXT")

        rows = self.conn.execute(
            """
            SELECT b.id, b.created_at, b.admin_day, b.daily_sequence, s.slot_date, s.start_time
            FROM bookings b
            JOIN slots s ON s.id=b.slot_id
            ORDER BY b.created_at, b.id
            """
        ).fetchall()

        sequences: dict[str, int] = {}
        for row in rows:
            admin_day = row["admin_day"] or self._admin_day_for_slot(row["slot_date"], row["start_time"])
            next_seq = sequences.get(admin_day, 0) + 1
            sequences[admin_day] = max(next_seq, row["daily_sequence"] or 0, next_seq)
            daily_sequence = row["daily_sequence"] or next_seq
            self.conn.execute(
                "UPDATE bookings SET admin_day=?, daily_sequence=? WHERE id=?",
                (admin_day, daily_sequence, row["id"]),
            )

    def set_user_profile(self, user_id: int, country_name: str, timezone_name: str) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                """
                INSERT INTO user_profiles(user_id, country_name, timezone_name, updated_at)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP)
                ON CONFLICT(user_id) DO UPDATE SET
                    country_name=excluded.country_name,
                    timezone_name=excluded.timezone_name,
                    updated_at=CURRENT_TIMESTAMP
                """,
                (user_id, country_name, timezone_name),
            )

    def get_user_profile(self, user_id: int) -> dict[str, Any] | None:
        with self._lock:
            row = self.conn.execute(
                "SELECT user_id, country_name, timezone_name, updated_at FROM user_profiles WHERE user_id=?",
                (user_id,),
            ).fetchone()
            return dict(row) if row else None

    def set_booking_open(self, is_open: bool) -> None:
        with self._lock, self.conn:
            self.conn.execute(
                "UPDATE settings SET value=? WHERE key='booking_open'",
                ("1" if is_open else "0",),
            )

    def is_booking_open(self) -> bool:
        with self._lock:
            row = self.conn.execute("SELECT value FROM settings WHERE key='booking_open'").fetchone()
            return (row["value"] if row else "1") == "1"

    def upsert_slot(self, slot_date: str, start_time: str, end_time: str, created_by: int) -> str:
        normalized_date = self._normalize_date_str(slot_date)
        normalized_start = self._normalize_time_str(start_time)
        normalized_end = self._normalize_time_str(end_time)

        with self._lock, self.conn:
            existing = self.conn.execute(
                "SELECT id, is_active FROM slots WHERE slot_date=? AND start_time=?",
                (normalized_date, normalized_start),
            ).fetchone()

            if existing is None:
                self.conn.execute(
                    """
                    INSERT INTO slots(slot_date, start_time, end_time, is_active, created_by)
                    VALUES (?, ?, ?, 1, ?)
                    """,
                    (normalized_date, normalized_start, normalized_end, created_by),
                )
                return "created"

            if existing["is_active"] == 0:
                self.conn.execute(
                    """
                    UPDATE slots
                    SET end_time=?, is_active=1, created_by=?, created_at=CURRENT_TIMESTAMP
                    WHERE id=?
                    """,
                    (normalized_end, created_by, existing["id"]),
                )
                return "reactivated"

            return "exists"

    def get_first_available_month(self, now_date: str, now_time: str) -> tuple[int, int] | None:
        min_dt = self._min_bookable_dt(now_date, now_time)

        with self._lock:
            rows = self.conn.execute(
                """
                SELECT s.slot_date, s.start_time
                FROM slots s
                WHERE s.is_active=1
                  AND NOT EXISTS (
                      SELECT 1 FROM bookings b
                      WHERE b.slot_id=s.id AND b.status='confirmed'
                  )
                """
            ).fetchall()

        candidates: list[datetime] = []
        for row in rows:
            start_dt = self._slot_start_dt(row["slot_date"], row["start_time"])
            if start_dt >= min_dt:
                candidates.append(start_dt)

        if not candidates:
            return None

        first_dt = min(candidates)
        return first_dt.year, first_dt.month

    def get_available_dates_for_month(self, year: int, month: int, now_date: str, now_time: str) -> set[int]:
        min_dt = self._min_bookable_dt(now_date, now_time)

        with self._lock:
            rows = self.conn.execute(
                """
                SELECT s.slot_date, s.start_time
                FROM slots s
                WHERE s.is_active=1
                  AND SUBSTR(s.slot_date, 1, 7)=?
                  AND NOT EXISTS (
                      SELECT 1 FROM bookings b
                      WHERE b.slot_id=s.id AND b.status='confirmed'
                  )
                """,
                (f"{year:04d}-{month:02d}",),
            ).fetchall()

        available_days: set[int] = set()
        for row in rows:
            start_dt = self._slot_start_dt(row["slot_date"], row["start_time"])
            if start_dt >= min_dt:
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
        min_dt = self._min_bookable_dt(now_date, now_time) if now_date and now_time else None

        with self._lock:
            rows = self.conn.execute(
                """
                SELECT s.*
                FROM slots s
                WHERE s.slot_date=? AND s.is_active=1
                  AND NOT EXISTS (
                      SELECT 1 FROM bookings b
                      WHERE b.slot_id=s.id AND b.status='confirmed'
                  )
                ORDER BY s.start_time
                """,
                (normalized_date,),
            ).fetchall()

        slots: list[Slot] = []
        for row in rows:
            slot = Slot(**dict(row))
            if min_dt is None or self._slot_start_dt(slot.slot_date, slot.start_time) >= min_dt:
                slots.append(slot)
        return slots

    def get_all_slots_for_date(self, slot_date: str) -> list[Slot]:
        normalized_date = self._normalize_date_str(slot_date)
        with self._lock:
            rows = self.conn.execute(
                "SELECT * FROM slots WHERE slot_date=? AND is_active=1 ORDER BY start_time",
                (normalized_date,),
            ).fetchall()
            return [Slot(**dict(r)) for r in rows]

    def get_slot(self, slot_id: int) -> Slot | None:
        with self._lock:
            row = self.conn.execute("SELECT * FROM slots WHERE id=?", (slot_id,)).fetchone()
            return Slot(**dict(row)) if row else None

    def remove_slot(self, slot_id: int) -> tuple[bool, str]:
        with self._lock, self.conn:
            row = self.conn.execute("SELECT * FROM slots WHERE id=?", (slot_id,)).fetchone()
            if not row:
                return False, "not_found"

            active_booking = self.conn.execute(
                "SELECT 1 FROM bookings WHERE slot_id=? AND status='confirmed'",
                (slot_id,),
            ).fetchone()
            if active_booking:
                return False, "booked"

            self.conn.execute("UPDATE slots SET is_active=0 WHERE id=?", (slot_id,))
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
                "UPDATE slots SET is_active=0 WHERE slot_date=? AND is_active=1",
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
                "SELECT * FROM slots WHERE id=? AND is_active=1",
                (slot_id,),
            ).fetchone()
            if slot is None:
                return False, None

            existing = self.conn.execute(
                "SELECT 1 FROM bookings WHERE slot_id=? AND status='confirmed'",
                (slot_id,),
            ).fetchone()
            if existing:
                return False, None

            admin_day = self._admin_day_for_slot(slot["slot_date"], slot["start_time"])
            row = self.conn.execute(
                "SELECT COALESCE(MAX(daily_sequence), 0) AS max_seq FROM bookings WHERE admin_day=?",
                (admin_day,),
            ).fetchone()
            daily_sequence = int(row["max_seq"] or 0) + 1

            cur = self.conn.execute(
                """
                INSERT INTO bookings(
                    slot_id, client_user_id, client_chat_id, client_name,
                    client_telegram, session_type, status, admin_day, daily_sequence
                ) VALUES (?, ?, ?, ?, ?, ?, 'confirmed', ?, ?)
                """,
                (
                    slot_id,
                    client_user_id,
                    client_chat_id,
                    client_name,
                    client_telegram,
                    session_type,
                    admin_day,
                    daily_sequence,
                ),
            )
            return True, int(cur.lastrowid)

    def get_booking(self, booking_id: int) -> Booking | None:
        with self._lock:
            row = self.conn.execute(
                """
                SELECT b.*, s.slot_date, s.start_time, s.end_time
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
                SELECT b.*, s.slot_date, s.start_time, s.end_time
                FROM bookings b
                JOIN slots s ON s.id=b.slot_id
                WHERE b.client_user_id=? AND b.status='confirmed'
                ORDER BY s.slot_date, s.start_time
                """,
                (client_user_id,),
            ).fetchall()

        bookings: list[Booking] = []
        for row in rows:
            booking = Booking(**dict(row))
            if self._slot_start_dt(booking.slot_date, booking.start_time) > now_dt:
                bookings.append(booking)
        return bookings

    def get_all_upcoming_bookings(self, now_date: str, now_time: str) -> list[Booking]:
        now_dt = self._compose_now_dt(now_date, now_time)
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT b.*, s.slot_date, s.start_time, s.end_time
                FROM bookings b
                JOIN slots s ON s.id=b.slot_id
                WHERE b.status='confirmed'
                ORDER BY s.slot_date, s.start_time
                """
            ).fetchall()

        bookings: list[Booking] = []
        for row in rows:
            booking = Booking(**dict(row))
            if self._slot_start_dt(booking.slot_date, booking.start_time) > now_dt:
                bookings.append(booking)
        return bookings

    def cancel_booking(
        self,
        booking_id: int,
        by_user_id: int | None = None,
        cancellation_reason: str | None = None,
    ) -> tuple[bool, str]:
        with self._lock, self.conn:
            booking = self.conn.execute("SELECT * FROM bookings WHERE id=?", (booking_id,)).fetchone()

            if booking is None:
                return False, "not_found"
            if booking["status"] != "confirmed":
                return False, "already_cancelled"
            if by_user_id is not None and int(booking["client_user_id"]) != int(by_user_id):
                return False, "forbidden"

            self.conn.execute(
                "UPDATE bookings SET status='cancelled', cancelled_at=CURRENT_TIMESTAMP, cancellation_reason=? WHERE id=?",
                (cancellation_reason, booking_id),
            )
            return True, "cancelled"

    def get_due_notifications(self, now_dt: datetime) -> list[dict[str, Any]]:
        with self._lock:
            rows = self.conn.execute(
                """
                SELECT b.*, s.slot_date, s.start_time, s.end_time
                FROM bookings b
                JOIN slots s ON s.id=b.slot_id
                WHERE b.status='confirmed'
                ORDER BY s.slot_date, s.start_time
                """
            ).fetchall()

        due: list[dict[str, Any]] = []
        for row in rows:
            booking = Booking(**dict(row))
            start_dt = self._slot_start_dt(booking.slot_date, booking.start_time)
            end_dt = self._slot_end_dt(booking.slot_date, booking.start_time, booking.end_time)

            if booking.day_reminder_sent == 0 and start_dt > now_dt and now_dt >= start_dt - timedelta(days=1):
                due.append({"kind": "day", "booking": booking})

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
            self.conn.execute(f"UPDATE bookings SET {column}=1 WHERE id=?", (booking_id,))