from __future__ import annotations

import json
import sqlite3
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
    is_active: bool
    created_by: int | None = None


@dataclass
class Booking:
    id: int
    slot_id: int
    slot_date: str
    start_time: str
    end_time: str
    client_user_id: int
    client_chat_id: int
    client_name: str
    client_telegram: str
    session_type: str
    status: str
    created_at: str
    cancellation_reason: str | None = None


class Database:
    def __init__(self, db_path: str) -> None:
        self.db_path = db_path
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.init_db()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path)
        conn.row_factory = sqlite3.Row
        return conn

    def init_db(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                PRAGMA foreign_keys = ON;

                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT
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
                    cancellation_reason TEXT,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    cancelled_at TEXT,
                    reminder_day_sent INTEGER NOT NULL DEFAULT 0,
                    reminder_hour_sent INTEGER NOT NULL DEFAULT 0,
                    reminder_start_sent INTEGER NOT NULL DEFAULT 0,
                    FOREIGN KEY(slot_id) REFERENCES slots(id)
                );

                CREATE TABLE IF NOT EXISTS user_profiles (
                    user_id INTEGER PRIMARY KEY,
                    country_name TEXT NOT NULL,
                    timezone_name TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS availability_alerts (
                    user_id INTEGER PRIMARY KEY,
                    chat_id INTEGER NOT NULL,
                    enabled INTEGER NOT NULL DEFAULT 1,
                    updated_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
                );

                CREATE TABLE IF NOT EXISTS schedule_alert_batches (
                    marker TEXT PRIMARY KEY,
                    changed_dates TEXT NOT NULL,
                    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    processed INTEGER NOT NULL DEFAULT 0
                );
                """
            )

            self._ensure_default_setting(conn, "booking_open", "1")
            conn.commit()

    def _ensure_default_setting(self, conn: sqlite3.Connection, key: str, value: str) -> None:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
        if row is None:
            conn.execute("INSERT INTO settings (key, value) VALUES (?, ?)", (key, value))

    def is_booking_open(self) -> bool:
        with self.connect() as conn:
            row = conn.execute("SELECT value FROM settings WHERE key = 'booking_open'").fetchone()
            return (row["value"] if row else "1") == "1"

    def set_booking_open(self, is_open: bool) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO settings (key, value)
                VALUES ('booking_open', ?)
                ON CONFLICT(key) DO UPDATE SET value = excluded.value
                """,
                ("1" if is_open else "0",),
            )
            conn.commit()

    def set_user_profile(self, user_id: int, country_name: str, timezone_name: str) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO user_profiles (user_id, country_name, timezone_name)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    country_name = excluded.country_name,
                    timezone_name = excluded.timezone_name,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (user_id, country_name, timezone_name),
            )
            conn.commit()

    def get_user_profile(self, user_id: int) -> dict[str, Any] | None:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT user_id, country_name, timezone_name FROM user_profiles WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            return dict(row) if row else None

    def set_availability_alert(self, user_id: int, chat_id: int, enabled: bool) -> None:
        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO availability_alerts (user_id, chat_id, enabled)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                    chat_id = excluded.chat_id,
                    enabled = excluded.enabled,
                    updated_at = CURRENT_TIMESTAMP
                """,
                (user_id, chat_id, 1 if enabled else 0),
            )
            conn.commit()

    def get_availability_alert_enabled(self, user_id: int) -> bool:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT enabled FROM availability_alerts WHERE user_id = ?",
                (user_id,),
            ).fetchone()
            return bool(row["enabled"]) if row else False

    def get_enabled_alert_subscribers(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                "SELECT user_id, chat_id FROM availability_alerts WHERE enabled = 1"
            ).fetchall()
            return [dict(r) for r in rows]

    def mark_schedule_changed(self, changed_dates: list[str]) -> None:
        if not changed_dates:
            return

        clean_dates = sorted(set(changed_dates))
        marker = datetime.utcnow().isoformat(timespec="seconds")

        with self.connect() as conn:
            conn.execute(
                """
                INSERT INTO schedule_alert_batches (marker, changed_dates, processed)
                VALUES (?, ?, 0)
                """,
                (marker, json.dumps(clean_dates, ensure_ascii=False)),
            )
            conn.commit()

    def get_due_schedule_alert_batch(self, delay_minutes: int) -> tuple[str | None, list[str]]:
        threshold = datetime.utcnow() - timedelta(minutes=delay_minutes)

        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT marker, changed_dates
                FROM schedule_alert_batches
                WHERE processed = 0
                  AND datetime(created_at) <= datetime(?)
                ORDER BY datetime(created_at) ASC
                LIMIT 1
                """,
                (threshold.isoformat(timespec="seconds"),),
            ).fetchone()

            if not row:
                return None, []

            return row["marker"], json.loads(row["changed_dates"])

    def mark_schedule_alert_batch_processed(self, marker: str) -> None:
        with self.connect() as conn:
            conn.execute(
                "UPDATE schedule_alert_batches SET processed = 1 WHERE marker = ?",
                (marker,),
            )
            conn.commit()

    def _slot_start_dt(self, slot_date: str, start_time: str) -> datetime:
        return datetime.combine(date.fromisoformat(slot_date), time.fromisoformat(start_time))

    def _slot_end_dt(self, slot_date: str, start_time: str, end_time: str) -> datetime:
        start_dt = self._slot_start_dt(slot_date, start_time)
        end_dt = datetime.combine(date.fromisoformat(slot_date), time.fromisoformat(end_time))
        if end_dt <= start_dt:
            end_dt += timedelta(days=1)
        return end_dt

    def _booking_cutoff(self, today_date: str, current_time: str) -> datetime:
        now_dt = datetime.combine(date.fromisoformat(today_date), time.fromisoformat(current_time))
        return now_dt + timedelta(days=1)

    def _display_sort_value(self, hhmm: str) -> tuple[int, int]:
        hh, mm = hhmm.split(":")
        hour = int(hh)
        minute = int(mm)
        rank_hour = hour + 24 if hour < 4 else hour
        return rank_hour, minute

    def _admin_day_for_slot(self, slot_date: str, start_time: str) -> str:
        """
        في منطق هذا البوت:
        الأوقات بعد منتصف الليل وحتى 4 الفجر
        تُعرض وتُنظَّم تحت نفس اليوم المختار أصلًا،
        لذلك الهاشتاق يجب أن يبقى على نفس slot_date
        ويكمل العدّ بدل أن يبدأ من جديد.
        """
        return slot_date

    def _row_to_slot(self, row: sqlite3.Row) -> Slot:
        return Slot(
            id=row["id"],
            slot_date=row["slot_date"],
            start_time=row["start_time"],
            end_time=row["end_time"],
            is_active=bool(row["is_active"]),
            created_by=row["created_by"],
        )

    def _row_to_booking(self, row: sqlite3.Row) -> Booking:
        return Booking(
            id=row["id"],
            slot_id=row["slot_id"],
            slot_date=row["slot_date"],
            start_time=row["start_time"],
            end_time=row["end_time"],
            client_user_id=row["client_user_id"],
            client_chat_id=row["client_chat_id"],
            client_name=row["client_name"],
            client_telegram=row["client_telegram"],
            session_type=row["session_type"],
            status=row["status"],
            created_at=row["created_at"],
            cancellation_reason=row["cancellation_reason"],
        )

    def upsert_slot(self, slot_date: str, start_time: str, end_time: str, created_by: int) -> str:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT id, is_active
                FROM slots
                WHERE slot_date = ? AND start_time = ?
                """,
                (slot_date, start_time),
            ).fetchone()

            if row is None:
                conn.execute(
                    """
                    INSERT INTO slots (slot_date, start_time, end_time, is_active, created_by)
                    VALUES (?, ?, ?, 1, ?)
                    """,
                    (slot_date, start_time, end_time, created_by),
                )
                conn.commit()
                self.mark_schedule_changed([slot_date])
                return "created"

            if not bool(row["is_active"]):
                conn.execute(
                    """
                    UPDATE slots
                    SET end_time = ?, is_active = 1, created_by = ?
                    WHERE id = ?
                    """,
                    (end_time, created_by, row["id"]),
                )
                conn.commit()
                self.mark_schedule_changed([slot_date])
                return "reactivated"

            return "exists"

    def get_slot(self, slot_id: int) -> Slot | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT id, slot_date, start_time, end_time, is_active, created_by
                FROM slots
                WHERE id = ?
                """,
                (slot_id,),
            ).fetchone()
            return self._row_to_slot(row) if row else None

    def get_all_slots_for_date(self, slot_date: str) -> list[Slot]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT id, slot_date, start_time, end_time, is_active, created_by
                FROM slots
                WHERE slot_date = ? AND is_active = 1
                ORDER BY start_time ASC, id ASC
                """,
                (slot_date,),
            ).fetchall()
            return [self._row_to_slot(r) for r in rows]

    def get_available_slots(self, slot_date: str, today_date: str, current_time: str) -> list[Slot]:
        cutoff = self._booking_cutoff(today_date, current_time)

        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT s.id, s.slot_date, s.start_time, s.end_time, s.is_active, s.created_by
                FROM slots s
                LEFT JOIN bookings b
                    ON b.slot_id = s.id AND b.status = 'confirmed'
                WHERE s.slot_date = ?
                  AND s.is_active = 1
                  AND b.id IS NULL
                ORDER BY s.start_time ASC, s.id ASC
                """,
                (slot_date,),
            ).fetchall()

            result: list[Slot] = []
            for row in rows:
                slot = self._row_to_slot(row)
                if self._slot_start_dt(slot.slot_date, slot.start_time) >= cutoff:
                    result.append(slot)

            return result

    def get_available_dates_for_month(
        self,
        year: int,
        month: int,
        today_date: str,
        current_time: str,
    ) -> set[int]:
        cutoff = self._booking_cutoff(today_date, current_time)

        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1)
        else:
            end_date = date(year, month + 1, 1)

        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT s.slot_date, s.start_time
                FROM slots s
                LEFT JOIN bookings b
                    ON b.slot_id = s.id AND b.status = 'confirmed'
                WHERE s.is_active = 1
                  AND b.id IS NULL
                  AND date(s.slot_date) >= date(?)
                  AND date(s.slot_date) < date(?)
                """,
                (start_date.isoformat(), end_date.isoformat()),
            ).fetchall()

            days: set[int] = set()
            for row in rows:
                slot_dt = self._slot_start_dt(row["slot_date"], row["start_time"])
                if slot_dt >= cutoff:
                    days.add(date.fromisoformat(row["slot_date"]).day)

            return days

    def get_first_available_month(self, today_date: str, current_time: str) -> tuple[int, int] | None:
        cutoff = self._booking_cutoff(today_date, current_time)

        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT s.slot_date, s.start_time
                FROM slots s
                LEFT JOIN bookings b
                    ON b.slot_id = s.id AND b.status = 'confirmed'
                WHERE s.is_active = 1
                  AND b.id IS NULL
                ORDER BY s.slot_date ASC, s.start_time ASC
                """
            ).fetchall()

            for row in rows:
                slot_dt = self._slot_start_dt(row["slot_date"], row["start_time"])
                if slot_dt >= cutoff:
                    d = date.fromisoformat(row["slot_date"])
                    return d.year, d.month

        return None

    def get_manager_dates_for_month(self, year: int, month: int, from_date: str) -> set[int]:
        start_date = date(year, month, 1)
        if month == 12:
            end_date = date(year + 1, 1, 1)
        else:
            end_date = date(year, month + 1, 1)

        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT DISTINCT slot_date
                FROM slots
                WHERE date(slot_date) >= date(?)
                  AND date(slot_date) >= date(?)
                  AND date(slot_date) < date(?)
                """,
                (from_date, start_date.isoformat(), end_date.isoformat()),
            ).fetchall()

            return {date.fromisoformat(r["slot_date"]).day for r in rows}

    def remove_slot(self, slot_id: int) -> tuple[bool, str]:
        with self.connect() as conn:
            booked = conn.execute(
                """
                SELECT b.id
                FROM bookings b
                WHERE b.slot_id = ? AND b.status = 'confirmed'
                """,
                (slot_id,),
            ).fetchone()

            if booked:
                return False, "booked"

            slot_row = conn.execute(
                "SELECT slot_date FROM slots WHERE id = ?",
                (slot_id,),
            ).fetchone()

            if not slot_row:
                return False, "not_found"

            conn.execute("DELETE FROM slots WHERE id = ?", (slot_id,))
            conn.commit()
            self.mark_schedule_changed([slot_row["slot_date"]])
            return True, "removed"

    def remove_day(self, slot_date: str) -> tuple[bool, str, int, int]:
        with self.connect() as conn:
            all_rows = conn.execute(
                """
                SELECT s.id,
                       EXISTS(
                           SELECT 1
                           FROM bookings b
                           WHERE b.slot_id = s.id AND b.status = 'confirmed'
                       ) AS has_booking
                FROM slots s
                WHERE s.slot_date = ?
                """,
                (slot_date,),
            ).fetchall()

            if not all_rows:
                return False, "not_found", 0, 0

            removable_ids = [r["id"] for r in all_rows if not r["has_booking"]]
            booked_count = sum(1 for r in all_rows if r["has_booking"])

            if removable_ids:
                conn.executemany("DELETE FROM slots WHERE id = ?", [(sid,) for sid in removable_ids])
                conn.commit()
                self.mark_schedule_changed([slot_date])

                if booked_count > 0:
                    return True, "partial", len(removable_ids), booked_count
                return True, "removed_all", len(removable_ids), booked_count

            return True, "booked_only", 0, booked_count

    def get_dates_with_available_slots(
        self,
        changed_dates: list[str],
        today_date: str,
        current_time: str,
    ) -> list[str]:
        if not changed_dates:
            return []

        cutoff = self._booking_cutoff(today_date, current_time)
        results: list[str] = []

        with self.connect() as conn:
            for slot_date in sorted(set(changed_dates)):
                rows = conn.execute(
                    """
                    SELECT s.start_time
                    FROM slots s
                    LEFT JOIN bookings b
                        ON b.slot_id = s.id AND b.status = 'confirmed'
                    WHERE s.slot_date = ?
                      AND s.is_active = 1
                      AND b.id IS NULL
                    ORDER BY s.start_time ASC
                    """,
                    (slot_date,),
                ).fetchall()

                has_valid = False
                for row in rows:
                    if self._slot_start_dt(slot_date, row["start_time"]) >= cutoff:
                        has_valid = True
                        break

                if has_valid:
                    results.append(slot_date)

        return results

    def create_booking(
        self,
        slot_id: int,
        client_user_id: int,
        client_chat_id: int,
        client_name: str,
        client_telegram: str,
        session_type: str,
    ) -> tuple[bool, int | None]:
        with self.connect() as conn:
            slot_row = conn.execute(
                """
                SELECT id, is_active
                FROM slots
                WHERE id = ?
                """,
                (slot_id,),
            ).fetchone()

            if not slot_row or not bool(slot_row["is_active"]):
                return False, None

            exists = conn.execute(
                """
                SELECT id
                FROM bookings
                WHERE slot_id = ? AND status = 'confirmed'
                """,
                (slot_id,),
            ).fetchone()

            if exists:
                return False, None

            cur = conn.execute(
                """
                INSERT INTO bookings (
                    slot_id, client_user_id, client_chat_id,
                    client_name, client_telegram, session_type, status
                )
                VALUES (?, ?, ?, ?, ?, ?, 'confirmed')
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
            booking_id = cur.lastrowid
            conn.commit()
            return True, booking_id

    def get_booking(self, booking_id: int) -> Booking | None:
        with self.connect() as conn:
            row = conn.execute(
                """
                SELECT
                    b.id, b.slot_id, b.client_user_id, b.client_chat_id,
                    b.client_name, b.client_telegram, b.session_type,
                    b.status, b.created_at, b.cancellation_reason,
                    s.slot_date, s.start_time, s.end_time
                FROM bookings b
                JOIN slots s ON s.id = b.slot_id
                WHERE b.id = ?
                """,
                (booking_id,),
            ).fetchone()
            return self._row_to_booking(row) if row else None

    def cancel_booking(
        self,
        booking_id: int,
        by_user_id: int | None = None,
        cancellation_reason: str | None = None,
    ) -> tuple[bool, str]:
        with self.connect() as conn:
            row = conn.execute(
                "SELECT id, status FROM bookings WHERE id = ?",
                (booking_id,),
            ).fetchone()

            if not row:
                return False, "not_found"

            if row["status"] != "confirmed":
                return False, "not_confirmed"

            conn.execute(
                """
                UPDATE bookings
                SET status = 'cancelled',
                    cancellation_reason = ?,
                    cancelled_at = CURRENT_TIMESTAMP
                WHERE id = ?
                """,
                (cancellation_reason, booking_id),
            )
            conn.commit()
            return True, "cancelled"

    def get_user_upcoming_bookings(
        self,
        user_id: int,
        today_date: str,
        current_time: str,
    ) -> list[Booking]:
        now_dt = datetime.combine(date.fromisoformat(today_date), time.fromisoformat(current_time))

        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    b.id, b.slot_id, b.client_user_id, b.client_chat_id,
                    b.client_name, b.client_telegram, b.session_type,
                    b.status, b.created_at, b.cancellation_reason,
                    s.slot_date, s.start_time, s.end_time
                FROM bookings b
                JOIN slots s ON s.id = b.slot_id
                WHERE b.client_user_id = ?
                  AND b.status = 'confirmed'
                ORDER BY s.slot_date ASC, s.start_time ASC, b.id ASC
                """,
                (user_id,),
            ).fetchall()

            result: list[Booking] = []
            for row in rows:
                booking = self._row_to_booking(row)
                if self._slot_start_dt(booking.slot_date, booking.start_time) >= now_dt:
                    result.append(booking)
            return result

    def get_all_upcoming_bookings(
        self,
        today_date: str,
        current_time: str,
    ) -> list[Booking]:
        now_dt = datetime.combine(date.fromisoformat(today_date), time.fromisoformat(current_time))

        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    b.id, b.slot_id, b.client_user_id, b.client_chat_id,
                    b.client_name, b.client_telegram, b.session_type,
                    b.status, b.created_at, b.cancellation_reason,
                    s.slot_date, s.start_time, s.end_time
                FROM bookings b
                JOIN slots s ON s.id = b.slot_id
                WHERE b.status = 'confirmed'
                ORDER BY s.slot_date ASC, s.start_time ASC, b.id ASC
                """
            ).fetchall()

            result: list[Booking] = []
            for row in rows:
                booking = self._row_to_booking(row)
                if self._slot_start_dt(booking.slot_date, booking.start_time) >= now_dt:
                    result.append(booking)
            return result

    def get_due_notifications(self, now_dt: datetime) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    b.id, b.slot_id, b.client_user_id, b.client_chat_id,
                    b.client_name, b.client_telegram, b.session_type,
                    b.status, b.created_at, b.cancellation_reason,
                    b.reminder_day_sent, b.reminder_hour_sent, b.reminder_start_sent,
                    s.slot_date, s.start_time, s.end_time
                FROM bookings b
                JOIN slots s ON s.id = b.slot_id
                WHERE b.status = 'confirmed'
                """
            ).fetchall()

            due: list[dict[str, Any]] = []

            for row in rows:
                booking = self._row_to_booking(row)
                start_dt = self._slot_start_dt(booking.slot_date, booking.start_time)

                if not row["reminder_day_sent"]:
                    delta = start_dt - now_dt
                    if timedelta(hours=23, minutes=59) <= delta <= timedelta(hours=24, minutes=1):
                        due.append({"booking": booking, "kind": "day"})

                if not row["reminder_hour_sent"]:
                    delta = start_dt - now_dt
                    if timedelta(minutes=59) <= delta <= timedelta(hours=1, minutes=1):
                        due.append({"booking": booking, "kind": "hour"})

                if not row["reminder_start_sent"]:
                    if start_dt <= now_dt <= start_dt + timedelta(minutes=1):
                        due.append({"booking": booking, "kind": "start"})

            return due

    def mark_notification_sent(self, booking_id: int, kind: str) -> None:
        column_map = {
            "day": "reminder_day_sent",
            "hour": "reminder_hour_sent",
            "start": "reminder_start_sent",
        }
        column = column_map.get(kind)
        if not column:
            return

        with self.connect() as conn:
            conn.execute(
                f"UPDATE bookings SET {column} = 1 WHERE id = ?",
                (booking_id,),
            )
            conn.commit()

    def get_confirmed_day_time_sequence(self, booking_id: int) -> int | None:
        target = self.get_booking(booking_id)
        if not target or target.status != "confirmed":
            return None

        target_admin_day = self._admin_day_for_slot(target.slot_date, target.start_time)

        with self.connect() as conn:
            rows = conn.execute(
                """
                SELECT
                    b.id,
                    b.status,
                    s.slot_date,
                    s.start_time
                FROM bookings b
                JOIN slots s ON s.id = b.slot_id
                WHERE b.status = 'confirmed'
                """
            ).fetchall()

        same_admin_day: list[tuple[int, str, str]] = []
        for row in rows:
            row_admin_day = self._admin_day_for_slot(row["slot_date"], row["start_time"])
            if row_admin_day == target_admin_day:
                same_admin_day.append((row["id"], row["slot_date"], row["start_time"]))

        if not same_admin_day:
            return None

        same_admin_day.sort(
            key=lambda item: (
                self._display_sort_value(item[2]),
                item[0],
            )
        )

        for index, item in enumerate(same_admin_day, start=1):
            if item[0] == booking_id:
                return index

        return None