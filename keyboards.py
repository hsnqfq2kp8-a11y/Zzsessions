from __future__ import annotations

import calendar
from datetime import date

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup


AR_WEEKDAYS = ["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]
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
            [KeyboardButton("عرض المواعيد المتاحة")],
            [KeyboardButton("مواعيدي")],
            [KeyboardButton("إلغاء حجز")],
            [KeyboardButton("تواصل مع المنسقات")],
        ],
        resize_keyboard=True,
    )


def panel_keyboard(is_open: bool) -> InlineKeyboardMarkup:
    status_text = "إغلاق تلقي الحجوزات" if is_open else "فتح الحجز"
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("إضافة يوم/ساعة", callback_data="panel:add")],
            [InlineKeyboardButton("عرض الحجوزات", callback_data="panel:bookings")],
            [InlineKeyboardButton("حذف وقت", callback_data="panel:remove_slot")],
            [InlineKeyboardButton("حذف يوم كامل", callback_data="panel:remove_day")],
            [InlineKeyboardButton("حذف حجز تم تأكيده", callback_data="panel:remove_booking")],
            [InlineKeyboardButton(status_text, callback_data="panel:toggle")],
        ]
    )


def booking_summary_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("تأكيد الحجز", callback_data="book:confirm")],
            [InlineKeyboardButton("إلغاء العملية", callback_data="book:cancel")],
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


def manager_bookings_remove_keyboard(booking_ids: list[int]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(f"حذف الحجز #{booking_id}", callback_data=f"manager_cancel_booking:{booking_id}")]
        for booking_id in booking_ids
    ]
    rows.append([InlineKeyboardButton("الرئيسية", callback_data="go:home")])
    return InlineKeyboardMarkup(rows)


def slots_keyboard(slot_buttons: list[tuple[int, str]], year: int, month: int, iso_date: str) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(label, callback_data=f"slot:{slot_id}")]
        for slot_id, label in slot_buttons
    ]
    rows.append([InlineKeyboardButton("تغيير البلد", callback_data=f"country_open:{iso_date}")])
    rows.append([InlineKeyboardButton("الرجوع لإختيار اليوم", callback_data=f"calendar:client:{year}:{month}")])
    return InlineKeyboardMarkup(rows)


def manager_slots_remove_keyboard(slot_buttons: list[tuple[int, str]]) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(f"حذف {label}", callback_data=f"remove_slot:{slot_id}")]
        for slot_id, label in slot_buttons
    ]
    rows.append([InlineKeyboardButton("الرئيسية", callback_data="go:home")])
    return InlineKeyboardMarkup(rows)


def country_keyboard(items: list[tuple[str, str]], pending_date: str | None = None) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    row: list[InlineKeyboardButton] = []

    for code, label in items:
        row.append(InlineKeyboardButton(label, callback_data=f"set_country:{code}"))
        if len(row) == 2:
            rows.append(row)
            row = []

    if row:
        rows.append(row)

    rows.append([InlineKeyboardButton("بلد آخر", callback_data="set_country:OTHER")])

    if pending_date:
        dt = date.fromisoformat(pending_date)
        rows.append([InlineKeyboardButton("الرجوع لإختيار اليوم", callback_data=f"calendar:client:{dt.year}:{dt.month}")])
    else:
        rows.append([InlineKeyboardButton("الرئيسية", callback_data="go:home")])

    return InlineKeyboardMarkup(rows)


def offers_sections_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("الجلسات النفسية", callback_data="offers:psychological")],
            [InlineKeyboardButton("الإختبارات السلوكية", callback_data="offers:behavioral_tests")],
            [InlineKeyboardButton("الدورات التعليمية", callback_data="offers:courses")],
            [InlineKeyboardButton("حصص الكوتشينق", callback_data="offers:coaching")],
            [InlineKeyboardButton("الرئيسية", callback_data="go:home")],
        ]
    )


def offers_back_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("العودة إلى أقسام العروض", callback_data="offers:menu")],
            [InlineKeyboardButton("الرئيسية", callback_data="go:home")],
        ]
    )


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

    prev_year, prev_month = _prev_month(year, month)
    next_year, next_month = _next_month(year, month)

    keyboard: list[list[InlineKeyboardButton]] = [
        [
            InlineKeyboardButton("← السابق", callback_data=f"calendar:{mode}:{prev_year}:{prev_month}"),
            InlineKeyboardButton(month_name, callback_data="noop"),
            InlineKeyboardButton("التالي →", callback_data=f"calendar:{mode}:{next_year}:{next_month}"),
        ],
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
            if day in marked_days and mode != "client":
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