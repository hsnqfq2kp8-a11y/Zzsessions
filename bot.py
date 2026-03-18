from __future__ import annotations

import asyncio
import logging
import re
from datetime import date, datetime, time

from telegram import BotCommand, BotCommandScopeAllPrivateChats, BotCommandScopeChat, CallbackQuery, Update
from telegram.constants import ChatType, ParseMode
from telegram.ext import (
    Application,
    ApplicationBuilder,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

from config import Settings, load_settings
from database import Booking, Database
from keyboards import (
    booking_summary_keyboard,
    bookings_list_keyboard,
    calendar_keyboard,
    cancel_booking_keyboard,
    main_menu_keyboard,
    manager_bookings_remove_keyboard,
    manager_slots_remove_keyboard,
    panel_keyboard,
    slots_keyboard,
)
import texts

logging.basicConfig(
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

SETTINGS: Settings = load_settings()
DB = Database(SETTINGS.db_path)
TIME_RANGE_RE = re.compile(r"^\s*(\d{1,2}:\d{2})\s*-\s*(\d{1,2}:\d{2})\s*$")


def now_local() -> datetime:
    return datetime.now(SETTINGS.timezone)


def is_private(update: Update) -> bool:
    chat = update.effective_chat
    return bool(chat and chat.type == ChatType.PRIVATE)


def is_manager(user_id: int | None) -> bool:
    return bool(user_id and user_id in SETTINGS.manager_ids)


def clear_booking_flow(user_data: dict) -> None:
    user_data.pop("state", None)
    user_data.pop("booking_draft", None)
    user_data.pop("manager_selected_date", None)


def format_date_slash(slot_date: str) -> str:
    dt = date.fromisoformat(slot_date)
    return f"{dt.year}/{dt.month}/{dt.day}"


def format_hour_12(dt: datetime) -> str:
    hour = dt.hour % 12 or 12
    return f"{hour}:{dt.minute:02d}"


def format_period(dt: datetime) -> str:
    return "صباحًا" if dt.hour < 12 else "مساءً"


def clean_secondary_label(label: str | None) -> str:
    cleaned = (label or "المغرب").replace("بتوقيت", "").replace("توقيت", "").strip()
    return cleaned or "المغرب"


def format_session_block(slot_date: str, start_time: str, end_time: str) -> str:
    start_dt = datetime.combine(date.fromisoformat(slot_date), time.fromisoformat(start_time), tzinfo=SETTINGS.timezone)
    end_dt = datetime.combine(date.fromisoformat(slot_date), time.fromisoformat(end_time), tzinfo=SETTINGS.timezone)

    mecca_range = f"{format_hour_12(start_dt)} {format_period(start_dt)}-{format_hour_12(end_dt)} {format_period(end_dt)}"

    lines = [
        f"اليوم : {format_date_slash(slot_date)}",
        f"الساعة : {mecca_range} بتوقيت مكة المكرمة",
    ]

    if SETTINGS.secondary_timezone:
        alt_start = start_dt.astimezone(SETTINGS.secondary_timezone)
        alt_end = end_dt.astimezone(SETTINGS.secondary_timezone)
        alt_range = f"{format_hour_12(alt_start)} {format_period(alt_start)}-{format_hour_12(alt_end)} {format_period(alt_end)}"
        lines.append(f"و {alt_range} بتوقيت {clean_secondary_label(SETTINGS.secondary_timezone_label)}")

    return "\n".join(lines)


def format_booking_details(
    slot_date: str,
    start_time: str,
    end_time: str,
    client_name: str,
    client_telegram: str,
    session_type: str,
    booking_id: int | None = None,
) -> str:
    lines = [
        f"الاسم : {client_name}",
        f"يوزر التيليغرام : {client_telegram}",
        f"نوع الجلسة : {session_type}",
        format_session_block(slot_date, start_time, end_time),
    ]
    if booking_id is not None:
        lines.append(f"#{booking_id}")
    return "\n".join(lines)


def booking_summary_text(draft: dict) -> str:
    details = format_booking_details(
        draft["slot_date"],
        draft["start_time"],
        draft["end_time"],
        draft["client_name"],
        draft["client_telegram"],
        draft["session_type"],
    )
    return f"{texts.BOOKING_SUMMARY_TITLE}\n\n{details}\n\nهل تريد تأكيد الحجز؟"


def booking_confirmation_text(booking: Booking) -> str:
    details = format_booking_details(
        booking.slot_date,
        booking.start_time,
        booking.end_time,
        booking.client_name,
        booking.client_telegram,
        booking.session_type,
        booking.id,
    )
    return f"تم حجز جلسة ✅\n\n{details}"


def booking_cancellation_text(booking: Booking) -> str:
    details = format_booking_details(
        booking.slot_date,
        booking.start_time,
        booking.end_time,
        booking.client_name,
        booking.client_telegram,
        booking.session_type,
        booking.id,
    )
    return f"تم إلغاء حجز\n\n{details}"


def reminder_text(booking: Booking, title: str) -> str:
    details = format_booking_details(
        booking.slot_date,
        booking.start_time,
        booking.end_time,
        booking.client_name,
        booking.client_telegram,
        booking.session_type,
        booking.id,
    )
    return f"{title}\n\n{details}"


async def send_main_menu(message, text: str | None = None) -> None:
    await message.reply_text(text or texts.WELCOME_TEXT, reply_markup=main_menu_keyboard())


async def show_client_calendar_message(target, month_year: tuple[int, int] | None = None) -> None:
    now = now_local()
    year, month = month_year or (now.year, now.month)
    available_days = DB.get_available_dates_for_month(year, month, now.date().isoformat(), now.strftime("%H:%M"))
    text_value = texts.CHOOSE_DAY if available_days else texts.NO_DATES
    markup = calendar_keyboard(year, month, available_days, "client", now.date())

    if isinstance(target, CallbackQuery):
        await target.edit_message_text(text_value, reply_markup=markup)
    else:
        await target.reply_text(text_value, reply_markup=markup)


async def show_manager_calendar_message(query: CallbackQuery, mode: str, year: int | None = None, month: int | None = None) -> None:
    now = now_local()
    chosen_year = year or now.year
    chosen_month = month or now.month
    marked = DB.get_manager_dates_for_month(chosen_year, chosen_month, now.date().isoformat())

    title = {
        "manager_add": "اختر اليوم الذي تريد إضافة الأوقات له:",
        "manager_remove_slot": "اختر اليوم الذي تريد حذف وقت منه:",
        "manager_remove_day": "اختر اليوم الذي تريد حذفه بالكامل:",
    }[mode]

    await query.edit_message_text(
        title,
        reply_markup=calendar_keyboard(
            chosen_year,
            chosen_month,
            marked,
            mode,
            now.date(),
            marked_days=marked,
        ),
    )


async def start_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private(update):
        await update.effective_message.reply_text(texts.ONLY_PRIVATE)
        return
    clear_booking_flow(context.user_data)
    await send_main_menu(update.effective_message)


async def help_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private(update):
        await update.effective_message.reply_text(texts.ONLY_PRIVATE)
        return
    await update.effective_message.reply_text(texts.HELP_TEXT, reply_markup=main_menu_keyboard())


async def panel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private(update):
        await update.effective_message.reply_text(texts.ONLY_PRIVATE)
        return

    user = update.effective_user
    if not is_manager(user.id if user else None):
        await update.effective_message.reply_text(texts.FORBIDDEN_PANEL)
        return

    await update.effective_message.reply_text(
        f"{texts.PANEL_HEADER}\n\nمعرّفك الحالي: <code>{user.id}</code>",
        parse_mode=ParseMode.HTML,
        reply_markup=panel_keyboard(DB.is_booking_open()),
    )


async def handle_text_message(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private(update):
        await update.effective_message.reply_text(texts.ONLY_PRIVATE)
        return

    text = (update.effective_message.text or "").strip()
    user_data = context.user_data
    state = user_data.get("state")

    if state == "await_name":
        if not text:
            await update.effective_message.reply_text("الاسم لا يمكن أن يكون فارغًا. أعد المحاولة:")
            return
        user_data["booking_draft"]["client_name"] = text
        user_data["state"] = "await_telegram"
        await update.effective_message.reply_text(texts.ASK_TELEGRAM)
        return

    if state == "await_telegram":
        if not text:
            await update.effective_message.reply_text("يوزر التيليغرام لا يمكن أن يكون فارغًا. أعد المحاولة:")
            return
        user_data["booking_draft"]["client_telegram"] = text
        user_data["state"] = "await_session_type"
        await update.effective_message.reply_text(texts.ASK_SESSION_TYPE)
        return

    if state == "await_session_type":
        if not text:
            await update.effective_message.reply_text("نوع الجلسة لا يمكن أن يكون فارغًا. أعد المحاولة:")
            return
        user_data["booking_draft"]["session_type"] = text
        user_data["state"] = "await_booking_confirm"
        await update.effective_message.reply_text(
            booking_summary_text(user_data["booking_draft"]),
            reply_markup=booking_summary_keyboard(),
        )
        return

    if state == "manager_await_slots_input":
        user = update.effective_user
        if not is_manager(user.id if user else None):
            await update.effective_message.reply_text(texts.FORBIDDEN_PANEL)
            return

        slot_date = user_data.get("manager_selected_date")
        if not slot_date:
            user_data.pop("state", None)
            await update.effective_message.reply_text(texts.GENERIC_ERROR)
            return

        results = add_slots_from_text(slot_date, text, user.id)
        user_data.pop("state", None)
        user_data.pop("manager_selected_date", None)

        if not results["valid"]:
            await update.effective_message.reply_text(
                "لم أتمكن من قراءة الأوقات. أرسلها بهذه الصيغة:\n11:00-12:00\n12:00-13:00"
            )
            return

        await update.effective_message.reply_text(
            f"{texts.MANAGER_ADD_DONE}\n\n"
            f"تمت الإضافة: {results['created']}\n"
            f"معاد تفعيلها: {results['reactivated']}\n"
            f"موجودة مسبقًا: {results['exists']}",
            reply_markup=main_menu_keyboard(),
        )
        return

    if text == "عرض المواعيد المتاحة":
        if not DB.is_booking_open():
            await update.effective_message.reply_text(texts.BOOKING_CLOSED)
            return
        await show_client_calendar_message(update.effective_message)
        return

    if text == "مواعيدي":
        await show_user_bookings(update, cancel_mode=False)
        return

    if text == "إلغاء حجز":
        await show_user_bookings(update, cancel_mode=True)
        return

    if text == "تواصل مع المنسقات":
        await update.effective_message.reply_text(texts.COORDINATORS_TEXT, reply_markup=main_menu_keyboard())
        return

    await update.effective_message.reply_text(texts.UNKNOWN_TEXT, reply_markup=main_menu_keyboard())


async def callback_router(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return

    await query.answer()
    data = query.data or ""

    if data == "noop":
        return

    if data == "go:home":
        clear_booking_flow(context.user_data)
        await query.message.reply_text(texts.WELCOME_TEXT, reply_markup=main_menu_keyboard())
        return

    if data.startswith("calendar:"):
        _, mode, year, month = data.split(":")
        if mode == "client":
            await show_client_calendar_message(query, (int(year), int(month)))
        else:
            await show_manager_calendar_message(query, mode, int(year), int(month))
        return

    if data.startswith("pickday:"):
        _, mode, iso_date = data.split(":", 2)

        if mode == "client":
            await show_slots_for_day(query, iso_date)
            return

        if mode == "manager_add":
            context.user_data["state"] = "manager_await_slots_input"
            context.user_data["manager_selected_date"] = iso_date
            await query.edit_message_text(f"اليوم المختار: {iso_date}\n\n{texts.ASK_MANAGER_SLOTS}")
            return

        if mode == "manager_remove_slot":
            await show_remove_slots_for_day(query, iso_date)
            return

        if mode == "manager_remove_day":
            success, reason, count = DB.remove_day(iso_date)
            if success:
                await query.edit_message_text(
                    f"{texts.MANAGER_REMOVE_DAY_DONE}\n\nالتاريخ: {iso_date}\nعدد الأوقات المحذوفة: {count}"
                )
            elif reason == "booked":
                await query.edit_message_text(texts.MANAGER_REMOVE_DAY_BLOCKED)
            else:
                await query.edit_message_text(texts.GENERIC_ERROR)
            return

    if data.startswith("slot:"):
        slot_id = int(data.split(":", 1)[1])
        await begin_booking_from_slot(query, context, slot_id)
        return

    if data == "book:cancel":
        clear_booking_flow(context.user_data)
        await query.edit_message_text("تم إلغاء العملية.")
        await query.message.reply_text(texts.WELCOME_TEXT, reply_markup=main_menu_keyboard())
        return

    if data == "book:confirm":
        await finalize_booking(query, context)
        return

    if data.startswith("panel:"):
        action = data.split(":", 1)[1]
        await handle_panel_action(query, context, action)
        return

    if data.startswith("remove_slot:"):
        slot_id = int(data.split(":", 1)[1])
        success, reason = DB.remove_slot(slot_id)
        if success:
            await query.edit_message_text(texts.MANAGER_REMOVE_SLOT_DONE)
        elif reason == "booked":
            await query.edit_message_text(texts.MANAGER_REMOVE_SLOT_BLOCKED)
        else:
            await query.edit_message_text(texts.GENERIC_ERROR)
        return

    if data.startswith("booking_cancel:") and not data.startswith("booking_cancel_confirm:"):
        booking_id = int(data.split(":", 1)[1])
        await prompt_cancel_booking(query, booking_id)
        return

    if data.startswith("booking_cancel_confirm:"):
        booking_id = int(data.split(":", 1)[1])
        await confirm_cancel_booking(query, context, booking_id)
        return

    if data == "booking_cancel_abort":
        await query.edit_message_text(texts.BOOKING_CANCEL_ABORTED)
        return

    if data.startswith("manager_cancel_booking:"):
        booking_id = int(data.split(":", 1)[1])
        await manager_cancel_booking(query, context, booking_id)
        return


async def show_slots_for_day(query: CallbackQuery, iso_date: str) -> None:
    now = now_local()
    slots = DB.get_available_slots(iso_date, now.date().isoformat(), now.strftime("%H:%M"))
    if not slots:
        await query.edit_message_text(texts.NO_SLOTS)
        return

    button_data = [(slot.id, f"{slot.start_time}-{slot.end_time}") for slot in slots]
    await query.edit_message_text(
        f"{texts.CHOOSE_SLOT}\n\nالتاريخ المختار: {iso_date}",
        reply_markup=slots_keyboard(button_data),
    )


async def begin_booking_from_slot(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, slot_id: int) -> None:
    slot = DB.get_slot(slot_id)
    if not slot or not slot.is_active:
        await query.edit_message_text(texts.BOOKING_EXPIRED)
        return

    now = now_local()
    available_now = {
        s.id: s for s in DB.get_available_slots(slot.slot_date, now.date().isoformat(), now.strftime("%H:%M"))
    }
    if slot_id not in available_now:
        await query.edit_message_text(texts.BOOKING_EXPIRED)
        return

    context.user_data["booking_draft"] = {
        "slot_id": slot.id,
        "slot_date": slot.slot_date,
        "start_time": slot.start_time,
        "end_time": slot.end_time,
    }
    context.user_data["state"] = "await_name"

    await query.edit_message_text(
        f"تم اختيار الموعد التالي:\n\n{format_session_block(slot.slot_date, slot.start_time, slot.end_time)}\n\n{texts.ASK_NAME}"
    )


async def finalize_booking(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE) -> None:
    user = query.from_user
    draft = context.user_data.get("booking_draft")
    if not user or not draft:
        await query.edit_message_text(texts.GENERIC_ERROR)
        return

    success, booking_id = DB.create_booking(
        slot_id=draft["slot_id"],
        client_user_id=user.id,
        client_chat_id=query.message.chat_id,
        client_name=draft["client_name"],
        client_telegram=draft["client_telegram"],
        session_type=draft["session_type"],
    )

    if not success or booking_id is None:
        clear_booking_flow(context.user_data)
        await query.edit_message_text(texts.BOOKING_EXPIRED)
        await query.message.reply_text(texts.WELCOME_TEXT, reply_markup=main_menu_keyboard())
        return

    booking = DB.get_booking(booking_id)
    clear_booking_flow(context.user_data)

    if booking is None:
        await query.edit_message_text(texts.GENERIC_ERROR)
        return

    confirmation = booking_confirmation_text(booking)

    await query.edit_message_text(confirmation)
    await query.message.reply_text("تم حفظ الموعد في سجلك.", reply_markup=main_menu_keyboard())

    for manager_id in SETTINGS.manager_ids:
        try:
            await context.bot.send_message(chat_id=manager_id, text=confirmation)
        except Exception:
            logger.exception("Failed to notify manager %s about new booking", manager_id)


async def show_user_bookings(update: Update, cancel_mode: bool = False) -> None:
    user = update.effective_user
    now = now_local()
    bookings = DB.get_user_upcoming_bookings(user.id, now.date().isoformat(), now.strftime("%H:%M"))

    if not bookings:
        await update.effective_message.reply_text(texts.NO_USER_BOOKINGS, reply_markup=main_menu_keyboard())
        return

    header = "هذه مواعيدك القادمة:" if not cancel_mode else "اختر الحجز الذي تريد إلغاءه:"
    lines = [header, ""]
    booking_ids: list[int] = []

    for booking in bookings:
        lines.append(
            format_booking_details(
                booking.slot_date,
                booking.start_time,
                booking.end_time,
                booking.client_name,
                booking.client_telegram,
                booking.session_type,
                booking.id,
            )
        )
        lines.append("")
        booking_ids.append(booking.id)

    reply_markup = bookings_list_keyboard(booking_ids) if cancel_mode else None
    await update.effective_message.reply_text("\n".join(lines).strip(), reply_markup=reply_markup)


async def prompt_cancel_booking(query: CallbackQuery, booking_id: int) -> None:
    booking = DB.get_booking(booking_id)
    user = query.from_user

    if not booking or booking.client_user_id != user.id or booking.status != "confirmed":
        await query.edit_message_text(texts.BOOKING_CANCEL_NOT_FOUND)
        return

    details = format_booking_details(
        booking.slot_date,
        booking.start_time,
        booking.end_time,
        booking.client_name,
        booking.client_telegram,
        booking.session_type,
        booking.id,
    )
    await query.edit_message_text(
        f"هل تريد إلغاء هذا الحجز؟\n\n{details}",
        reply_markup=cancel_booking_keyboard(booking_id),
    )


async def confirm_cancel_booking(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, booking_id: int) -> None:
    booking = DB.get_booking(booking_id)
    user = query.from_user

    if not booking or booking.client_user_id != user.id or booking.status != "confirmed":
        await query.edit_message_text(texts.BOOKING_CANCEL_NOT_FOUND)
        return

    success, _ = DB.cancel_booking(booking_id, by_user_id=user.id)
    if not success:
        await query.edit_message_text(texts.BOOKING_CANCEL_NOT_FOUND)
        return

    cancellation = booking_cancellation_text(booking)
    await query.edit_message_text(cancellation)
    await query.message.reply_text(texts.WELCOME_TEXT, reply_markup=main_menu_keyboard())

    for manager_id in SETTINGS.manager_ids:
        try:
            await context.bot.send_message(chat_id=manager_id, text=cancellation)
        except Exception:
            logger.exception("Failed to notify manager %s about cancellation", manager_id)


async def manager_cancel_booking(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, booking_id: int) -> None:
    if not is_manager(query.from_user.id if query.from_user else None):
        await query.edit_message_text(texts.FORBIDDEN_PANEL)
        return

    booking = DB.get_booking(booking_id)
    if not booking or booking.status != "confirmed":
        await query.edit_message_text(texts.BOOKING_CANCEL_NOT_FOUND)
        return

    success, _ = DB.cancel_booking(booking_id)
    if not success:
        await query.edit_message_text(texts.BOOKING_CANCEL_NOT_FOUND)
        return

    cancellation = booking_cancellation_text(booking)
    await query.edit_message_text(cancellation)

    try:
        await context.bot.send_message(chat_id=booking.client_chat_id, text=cancellation)
    except Exception:
        logger.exception("Failed to notify client about manager cancellation")

    for manager_id in SETTINGS.manager_ids:
        if manager_id == query.from_user.id:
            continue
        try:
            await context.bot.send_message(chat_id=manager_id, text=cancellation)
        except Exception:
            logger.exception("Failed to notify manager %s about manager cancellation", manager_id)


async def handle_panel_action(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, action: str) -> None:
    if not is_manager(query.from_user.id if query.from_user else None):
        await query.edit_message_text(texts.FORBIDDEN_PANEL)
        return

    if action == "add":
        await show_manager_calendar_message(query, "manager_add")
        return

    if action == "remove_slot":
        await show_manager_calendar_message(query, "manager_remove_slot")
        return

    if action == "remove_day":
        await show_manager_calendar_message(query, "manager_remove_day")
        return

    if action == "remove_booking":
        now = now_local()
        bookings = DB.get_all_upcoming_bookings(now.date().isoformat(), now.strftime("%H:%M"))
        if not bookings:
            await query.edit_message_text(texts.PANEL_NO_BOOKINGS_TO_CANCEL)
            return

        await query.edit_message_text(
            texts.MANAGER_REMOVE_BOOKING_TITLE,
            reply_markup=manager_bookings_remove_keyboard([b.id for b in bookings]),
        )
        return

    if action == "bookings":
        now = now_local()
        bookings = DB.get_all_upcoming_bookings(now.date().isoformat(), now.strftime("%H:%M"))
        if not bookings:
            await query.edit_message_text(texts.PANEL_NO_BOOKINGS)
            return

        chunks: list[str] = []
        current: list[str] = []

        for booking in bookings:
            block = format_booking_details(
                booking.slot_date,
                booking.start_time,
                booking.end_time,
                booking.client_name,
                booking.client_telegram,
                booking.session_type,
                booking.id,
            )
            if len("\n\n".join(current + [block])) > 3500 and current:
                chunks.append("\n\n".join(current))
                current = [block]
            else:
                current.append(block)

        if current:
            chunks.append("\n\n".join(current))

        await query.edit_message_text(chunks[0])
        for chunk in chunks[1:]:
            await query.message.reply_text(chunk)
        return

    if action == "toggle":
        current = DB.is_booking_open()
        DB.set_booking_open(not current)
        msg = texts.BOOKING_CLOSED_DONE if current else texts.BOOKING_OPENED
        await query.edit_message_text(msg, reply_markup=panel_keyboard(DB.is_booking_open()))
        return


async def show_remove_slots_for_day(query: CallbackQuery, iso_date: str) -> None:
    slots = DB.get_all_slots_for_date(iso_date)
    if not slots:
        await query.edit_message_text(texts.MANAGER_NO_SLOTS_THIS_DAY)
        return

    await query.edit_message_text(
        f"اختر الوقت الذي تريد حذفه من يوم {iso_date}:",
        reply_markup=manager_slots_remove_keyboard([(slot.id, f"{slot.start_time}-{slot.end_time}") for slot in slots]),
    )


def add_slots_from_text(slot_date: str, text: str, created_by: int) -> dict:
    parts = [p.strip() for p in re.split(r"[\n,]+", text) if p.strip()]
    valid = True
    created = 0
    reactivated = 0
    exists = 0

    for part in parts:
        match = TIME_RANGE_RE.match(part)
        if not match:
            valid = False
            continue

        start_raw, end_raw = match.groups()

        try:
            start_t = time.fromisoformat(start_raw)
            end_t = time.fromisoformat(end_raw)
        except ValueError:
            valid = False
            continue

        if end_t <= start_t:
            valid = False
            continue

        result = DB.upsert_slot(slot_date, start_t.strftime("%H:%M"), end_t.strftime("%H:%M"), created_by)
        if result == "created":
            created += 1
        elif result == "reactivated":
            reactivated += 1
        else:
            exists += 1

    return {
        "valid": valid and bool(parts),
        "created": created,
        "reactivated": reactivated,
        "exists": exists,
    }


async def reminder_loop(app: Application) -> None:
    while True:
        try:
            now = now_local()
            due = DB.get_due_notifications(now)
            for item in due:
                booking: Booking = item["booking"]
                kind = item["kind"]
                await send_reminder(app, booking, kind)
                DB.mark_notification_sent(booking.id, kind)
        except Exception:
            logger.exception("Reminder loop error")
        await asyncio.sleep(SETTINGS.check_interval_seconds)


async def send_reminder(app: Application, booking: Booking, kind: str) -> None:
    if kind == "hour":
        text_value = reminder_text(booking, "جلستك بعد ساعة")
    elif kind == "start":
        text_value = reminder_text(booking, "حان موعد جلستك")
    else:
        return

    try:
        await app.bot.send_message(chat_id=booking.client_chat_id, text=text_value)
    except Exception:
        logger.exception("Failed to send reminder to client %s", booking.client_chat_id)

    for manager_id in SETTINGS.manager_ids:
        try:
            await app.bot.send_message(chat_id=manager_id, text=text_value)
        except Exception:
            logger.exception("Failed to send reminder to manager %s", manager_id)


async def post_init(application: Application) -> None:
    public_commands = [
        BotCommand("start", "بدء البوت"),
        BotCommand("help", "المساعدة"),
    ]
    manager_commands = public_commands + [BotCommand("panel", "لوحة الإدارة")]

    await application.bot.set_my_commands(public_commands, scope=BotCommandScopeAllPrivateChats())

    for manager_id in SETTINGS.manager_ids:
        try:
            await application.bot.set_my_commands(manager_commands, scope=BotCommandScopeChat(chat_id=manager_id))
        except Exception:
            logger.exception("Failed to set commands for manager %s", manager_id)

    application.create_task(reminder_loop(application))


async def error_handler(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    logger.exception("Unhandled exception", exc_info=context.error)


def build_application() -> Application:
    app = ApplicationBuilder().token(SETTINGS.bot_token).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start_command))
    app.add_handler(CommandHandler("help", help_command))
    app.add_handler(CommandHandler("panel", panel_command))
    app.add_handler(CallbackQueryHandler(callback_router))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_text_message))
    app.add_error_handler(error_handler)
    return app


def main() -> None:
    app = build_application()
    logger.info("Starting bot")
    app.run_polling(drop_pending_updates=False, allowed_updates=Update.ALL_TYPES)


if __name__ == "__main__":
    main()