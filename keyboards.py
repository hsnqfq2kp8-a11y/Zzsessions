from __future__ import annotations

import calendar
from datetime import date, datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup


AR_WEEKDAYS = ["أحد", "اثن", "ثلا", "أرب", "خمي", "جمع", "سبت"]
AR_MONTHS = [
    "يناير",
    "فبراير",
    "مارس",
    "أبريل",
    "مايو",
    "يونيو",
    "يوليو",
    "أغسطس",
    "سبتمبر",
    "أكتوبر",
    "نوفمبر",
    "ديسمبر",
]


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton("عرض الأوقات المتاحة")],
            [KeyboardButton("مواعيدي"), KeyboardButton("إلغاء حجز")],
        ],
        resize_keyboard=True,
    )


def panel_keyboard(is_open: bool) -> InlineKeyboardMarkup:
    status_text = "إغلاق الحجز" if is_open else "فتح الحجز"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("إضافة يوم/أوقات", callback_data="panel:add")],
            [InlineKeyboardButton("حذف وقت", callback_data="panel:remove_slot")],
            [InlineKeyboardButton("حذف يوم كامل", callback_data="panel:remove_day")],
            [InlineKeyboardButton("عرض الحجوزات", callback_data="panel:bookings")],
            [InlineKeyboardButton(status_text, callback_data="panel:toggle")],
            [InlineKeyboardButton("تحديث اللوحة", callback_data="panel:refresh")],
        ]
    )


def booking_summary_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("تأكيد الحجز", callback_data="book:confirm")],
            [InlineKeyboardButton("إلغاء العملية", callback_data="book:cancel")],
        ]
    )


def cancel_booking_keyboard(booking_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("نعم، إلغاء الحجز", callback_data=f"booking_cancel_confirm:{booking_id}")],
            [InlineKeyboardButton("لا، رجوع", callback_data="booking_cancel_abort")],
        ]
    )


def bookings_list_keyboard(booking_ids: list[int]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(f"إلغاء الحجز #{booking_id}", callback_data=f"booking_cancel:{booking_id}")]
        for booking_id in booking_ids
    ]
    if not rows:
        rows = [[InlineKeyboardButton("رجوع", callback_data="noop")]]
    return InlineKeyboardMarkup(rows)


def slots_keyboard(slot_buttons: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(label, callback_data=f"slot:{slot_id}")]
        for slot_id, label in slot_buttons
    ]
    rows.append([InlineKeyboardButton("رجوع للقائمة الرئيسية", callback_data="go:home")])
    return InlineKeyboardMarkup(rows)


def manager_slots_remove_keyboard(slot_buttons: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(f"حذف {label}", callback_data=f"remove_slot:{slot_id}")]
        for slot_id, label in slot_buttons
    ]
    rows.append([InlineKeyboardButton("رجوع للوحة", callback_data="panel:refresh")])
    return InlineKeyboardMarkup(rows)


def calendar_keyboard(
    year: int,
    month: int,
    available_days: set[int],
    mode: str,
    now_date: date,
    marked_days: set[int] | None = None,
) -> InlineKeyboardMarkup:
    cal = calendar.Calendar(firstweekday=6)
    month_name = f"{AR_MONTHS[month - 1]} {year}"

    keyboard: list[list[InlineKeyboardButton]] = [
        [InlineKeyboardButton("‹ السابق", callback_data=f"calendar:{mode}:{_prev_month(year, month)[0]}:{_prev_month(year, month)[1]}"),
         InlineKeyboardButton(month_name, callback_data="noop"),
         InlineKeyboardButton("التالي ›", callback_data=f"calendar:{mode}:{_next_month(year, month)[0]}:{_next_month(year, month)[1]}")],
        [InlineKeyboardButton(day, callback_data="noop") for day in AR_WEEKDAYS],
    ]

    marked_days = marked_days or set()
    for week in cal.monthdayscalendar(year, month):
        row: list[InlineKeyboardButton] = []
        for day in week:
            if day == 0:
                row.append(InlineKeyboardButton(" ", callback_data="noop"))
                continue
            current_date = date(year, month, day)
            if current_date < now_date:
                row.append(InlineKeyboardButton("·", callback_data="noop"))
                continue

            label = str(day)
            if day in marked_days:
                label = f"• {day}"

            if mode == "client" and day in available_days:
                row.append(InlineKeyboardButton(label, callback_data=f"pickday:{mode}:{current_date.isoformat()}"))
            elif mode in {"manager_add", "manager_remove_slot", "manager_remove_day"}:
                row.append(InlineKeyboardButton(label, callback_data=f"pickday:{mode}:{current_date.isoformat()}"))
            else:
                row.append(InlineKeyboardButton("-", callback_data="noop"))
        keyboard.append(row)

    keyboard.append([InlineKeyboardButton("الرئيسية", callback_data="go:home")])
    return InlineKeyboardMarkup(keyboard)


def _prev_month(year: int, month: int) -> tuple[int, int]:
    if month == 1:
        return year - 1, 12
    return year, month - 1


def _next_month(year: int, month: int) -> tuple[int, int]:
    if month == 12:
        return year + 1, 1
    return year, month + 1