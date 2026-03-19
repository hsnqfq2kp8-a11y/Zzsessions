from __future__ import annotations

import asyncio
import difflib
import logging
import re
import unicodedata
from datetime import date, datetime, time, timedelta
from zoneinfo import ZoneInfo

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
    country_keyboard,
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
HOUR_ONLY_RE = re.compile(r"^\s*(\d{1,2})(?::(\d{1,2}))?\s*$")
ARABIC_TO_ENGLISH_DIGITS = str.maketrans("٠١٢٣٤٥٦٧٨٩", "0123456789")
ARABIC_NORMALIZE_MAP = str.maketrans({
    "أ": "ا",
    "إ": "ا",
    "آ": "ا",
    "ٱ": "ا",
    "ة": "ه",
    "ى": "ي",
    "ؤ": "و",
    "ئ": "ي",
})

POPULAR_COUNTRIES = [
    ("SA", "السعودية"),
    ("MA", "المغرب"),
    ("DZ", "الجزائر"),
    ("JO", "الأردن"),
    ("EG", "مصر"),
    ("KW", "الكويت"),
    ("OM", "عمان"),
    ("LB", "لبنان"),
    ("PS", "فلسطين"),
    ("AE", "الإمارات"),
    ("BH", "البحرين"),
    ("TN", "تونس"),
    ("LY", "ليبيا"),
    ("QA", "قطر"),
]

COUNTRY_DATA = {
    "SA": ("السعودية", "Asia/Riyadh", ["السعودية", "سعودية", "ksa", "saudi", "saudiarabia", "السعوديه"]),
    "MA": ("المغرب", "Africa/Casablanca", ["المغرب", "مغرب", "morocco", "maroc"]),
    "DZ": ("الجزائر", "Africa/Algiers", ["الجزائر", "جزائر", "algeria", "algerie"]),
    "JO": ("الأردن", "Asia/Amman", ["الأردن", "الاردن", "اردن", "jordan"]),
    "EG": ("مصر", "Africa/Cairo", ["مصر", "جمهوريةمصر", "egypt", "misr"]),
    "KW": ("الكويت", "Asia/Kuwait", ["الكويت", "كويت", "kuwait"]),
    "OM": ("عمان", "Asia/Muscat", ["عمان", "سلطنةعمان", "oman"]),
    "LB": ("لبنان", "Asia/Beirut", ["لبنان", "lebanon"]),
    "PS": ("فلسطين", "Asia/Hebron", ["فلسطين", "palestine"]),
    "AE": ("الإمارات", "Asia/Dubai", ["الإمارات", "الامارات", "امارات", "uae", "emirates"]),
    "BH": ("البحرين", "Asia/Bahrain", ["البحرين", "بحرين", "bahrain"]),
    "TN": ("تونس", "Africa/Tunis", ["تونس", "tunisia"]),
    "LY": ("ليبيا", "Africa/Tripoli", ["ليبيا", "ليبيه", "libya"]),
    "QA": ("قطر", "Asia/Qatar", ["قطر", "qatar"]),
    "IQ": ("العراق", "Asia/Baghdad", ["العراق", "العراقي", "iraq"]),
    "SY": ("سوريا", "Asia/Damascus", ["سوريا", "سورية", "syria"]),
    "YE": ("اليمن", "Asia/Aden", ["اليمن", "yemen"]),
    "SD": ("السودان", "Africa/Khartoum", ["السودان", "sudan"]),
    "MR": ("موريتانيا", "Africa/Nouakchott", ["موريتانيا", "mauritania"]),
    "SO": ("الصومال", "Africa/Mogadishu", ["الصومال", "somalia"]),
    "DJ": ("جيبوتي", "Africa/Djibouti", ["جيبوتي", "djibouti"]),
    "KM": ("جزر القمر", "Indian/Comoro", ["جزرالقمر", "جزر القمر", "comoros"]),
    "TR": ("تركيا", "Europe/Istanbul", ["تركيا", "turkey"]),
    "IR": ("إيران", "Asia/Tehran", ["ايران", "إيران", "iran"]),
    "FR": ("فرنسا", "Europe/Paris", ["فرنسا", "france"]),
    "DE": ("ألمانيا", "Europe/Berlin", ["المانيا", "ألمانيا", "germany"]),
    "GB": ("بريطانيا", "Europe/London", ["بريطانيا", "المملكةالمتحده", "المملكة المتحدة", "uk", "britain", "england"]),
    "ES": ("إسبانيا", "Europe/Madrid", ["اسبانيا", "إسبانيا", "spain"]),
    "IT": ("إيطاليا", "Europe/Rome", ["ايطاليا", "إيطاليا", "italy"]),
    "BE": ("بلجيكا", "Europe/Brussels", ["بلجيكا", "belgium"]),
    "NL": ("هولندا", "Europe/Amsterdam", ["هولندا", "netherlands", "holland"]),
    "CH": ("سويسرا", "Europe/Zurich", ["سويسرا", "switzerland"]),
    "AT": ("النمسا", "Europe/Vienna", ["النمسا", "austria"]),
    "GR": ("اليونان", "Europe/Athens", ["اليونان", "greece"]),
    "CY": ("قبرص", "Asia/Nicosia", ["قبرص", "cyprus"]),
    "US": ("الولايات المتحدة", "America/New_York", ["الولاياتالمتحده", "الولايات المتحدة", "امريكا", "أمريكا", "usa", "unitedstates", "us"]),
    "CA": ("كندا", "America/Toronto", ["كندا", "canada"]),
    "MX": ("المكسيك", "America/Mexico_City", ["المكسيك", "mexico"]),
    "BR": ("البرازيل", "America/Sao_Paulo", ["البرازيل", "brazil"]),
    "AR": ("الأرجنتين", "America/Argentina/Buenos_Aires", ["الارجنتين", "الأرجنتين", "argentina"]),
    "CL": ("تشيلي", "America/Santiago", ["تشيلي", "chile"]),
    "IN": ("الهند", "Asia/Kolkata", ["الهند", "india"]),
    "PK": ("باكستان", "Asia/Karachi", ["باكستان", "pakistan"]),
    "BD": ("بنغلاديش", "Asia/Dhaka", ["بنغلاديش", "bangladesh"]),
    "MY": ("ماليزيا", "Asia/Kuala_Lumpur", ["ماليزيا", "malaysia"]),
    "SG": ("سنغافورة", "Asia/Singapore", ["سنغافوره", "سنغافورة", "singapore"]),
    "ID": ("إندونيسيا", "Asia/Jakarta", ["اندونيسيا", "إندونيسيا", "indonesia"]),
    "JP": ("اليابان", "Asia/Tokyo", ["اليابان", "japan"]),
    "CN": ("الصين", "Asia/Shanghai", ["الصين", "china"]),
    "KR": ("كوريا الجنوبية", "Asia/Seoul", ["كورياالجنوبية", "كوريا الجنوبية", "southkorea", "korea"]),
    "AU": ("أستراليا", "Australia/Sydney", ["استراليا", "أستراليا", "australia"]),
    "NZ": ("نيوزيلندا", "Pacific/Auckland", ["نيوزيلندا", "newzealand"]),
    "ZA": ("جنوب أفريقيا", "Africa/Johannesburg", ["جنوبافريقيا", "جنوب أفريقيا", "southafrica"]),
    "NG": ("نيجيريا", "Africa/Lagos", ["نيجيريا", "nigeria"]),
    "KE": ("كينيا", "Africa/Nairobi", ["كينيا", "kenya"]),
}

COUNTRY_INDEX: dict[str, tuple[str, str]] = {}


def normalize_digits(value: str) -> str:
    return value.translate(ARABIC_TO_ENGLISH_DIGITS)


def normalize_country_text(value: str) -> str:
    value = normalize_digits(value).strip().lower()
    value = "".join(ch for ch in unicodedata.normalize("NFKD", value) if not unicodedata.combining(ch))
    value = value.translate(ARABIC_NORMALIZE_MAP)
    value = re.sub(r"[\s\-_]+", "", value)
    return value


def build_country_index() -> None:
    for code, (country_name, timezone_name, aliases) in COUNTRY_DATA.items():
        for alias in aliases:
            norm = normalize_country_text(alias)
            if norm:
                COUNTRY_INDEX[norm] = (country_name, timezone_name)
                if norm.startswith("ال") and len(norm) > 2:
                    COUNTRY_INDEX[norm[2:]] = (country_name, timezone_name)
                else:
                    COUNTRY_INDEX["ال" + norm] = (country_name, timezone_name)


build_country_index()


def resolve_country_text(user_text: str) -> tuple[str, str] | None:
    norm = normalize_country_text(user_text)
    if not norm:
        return None

    if norm in COUNTRY_INDEX:
        return COUNTRY_INDEX[norm]

    candidates = difflib.get_close_matches(norm, list(COUNTRY_INDEX.keys()), n=1, cutoff=0.72)
    if candidates:
        return COUNTRY_INDEX[candidates[0]]

    return None


def normalize_hour_input(value: str) -> str:
    value = normalize_digits(value).strip()
    match = HOUR_ONLY_RE.match(value)
    if not match:
        raise ValueError("Invalid hour format")
    hour = int(match.group(1))
    minute = int(match.group(2) or "0")
    if not (0 <= hour <= 23 and 0 <= minute <= 59):
        raise ValueError("Hour out of range")
    return f"{hour:02d}:{minute:02d}"


def add_one_hour(start_time: str) -> str:
    start_dt = datetime.combine(date(2000, 1, 1), time.fromisoformat(start_time))
    end_dt = start_dt + timedelta(hours=1)
    return end_dt.strftime("%H:%M")


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
    user_data.pop("country_pending_date", None)


def format_date_slash(slot_date: str) -> str:
    dt = date.fromisoformat(slot_date)
    return f"{dt.year}/{dt.month}/{dt.day}"


def format_hhmm(dt: datetime) -> str:
    return f"{dt.hour}:{dt.minute:02d}"


def get_user_timezone_and_label(user_id: int | None) -> tuple[ZoneInfo, str]:
    if user_id is not None:
        profile = DB.get_user_profile(user_id)
        if profile:
            try:
                return ZoneInfo(profile["timezone_name"]), profile["country_name"]
            except Exception:
                pass
    return SETTINGS.timezone, "مكة المكرمة"


def get_slot_datetimes(slot_date: str, start_time: str, end_time: str) -> tuple[datetime, datetime]:
    start_dt = datetime.combine(
        date.fromisoformat(slot_date),
        time.fromisoformat(start_time),
        tzinfo=SETTINGS.timezone,
    )
    end_dt = datetime.combine(
        date.fromisoformat(slot_date),
        time.fromisoformat(end_time),
        tzinfo=SETTINGS.timezone,
    )
    if end_dt <= start_dt:
        end_dt += timedelta(days=1)
    return start_dt, end_dt


def format_session_block(slot_date: str, start_time: str, end_time: str, viewer_tz: ZoneInfo, viewer_label: str) -> str:
    start_dt, _ = get_slot_datetimes(slot_date, start_time, end_time)
    local_start = start_dt.astimezone(viewer_tz)
    lines = [
        f"اليوم : {local_start.year}/{local_start.month}/{local_start.day}",
        f"الساعة : {format_hhmm(local_start)} بتوقيت {viewer_label}",
    ]
    return "\n".join(lines)


def format_booking_details(
    slot_date: str,
    start_time: str,
    end_time: str,
    client_name: str,
    client_telegram: str,
    session_type: str,
    viewer_tz: ZoneInfo,
    viewer_label: str,
    booking_id: int | None = None,
) -> str:
    lines = [
        f"الاسم : {client_name}",
        f"يوزر التيليغرام : {client_telegram}",
        f"نوع الجلسة : {session_type}",
        format_session_block(slot_date, start_time, end_time, viewer_tz, viewer_label),
    ]
    if booking_id is not None:
        lines.append(f"#{booking_id}")
    return "\n".join(lines)


def booking_summary_text(draft: dict, viewer_tz: ZoneInfo, viewer_label: str) -> str:
    details = format_booking_details(
        draft["slot_date"],
        draft["start_time"],
        draft["end_time"],
        draft["client_name"],
        draft["client_telegram"],
        draft["session_type"],
        viewer_tz,
        viewer_label,
    )
    return f"{texts.BOOKING_SUMMARY_TITLE}\n\n{details}\n\nهل تريد تأكيد الحجز؟"


def booking_confirmation_text(booking: Booking, viewer_tz: ZoneInfo, viewer_label: str) -> str:
    details = format_booking_details(
        booking.slot_date,
        booking.start_time,
        booking.end_time,
        booking.client_name,
        booking.client_telegram,
        booking.session_type,
        viewer_tz,
        viewer_label,
        booking.id,
    )
    return f"تم حجز جلسة ✅\n\n{details}"


def booking_cancellation_text(booking: Booking, viewer_tz: ZoneInfo, viewer_label: str) -> str:
    details = format_booking_details(
        booking.slot_date,
        booking.start_time,
        booking.end_time,
        booking.client_name,
        booking.client_telegram,
        booking.session_type,
        viewer_tz,
        viewer_label,
        booking.id,
    )
    return f"تم إلغاء حجز\n\n{details}"


def reminder_text(booking: Booking, title: str, viewer_tz: ZoneInfo, viewer_label: str) -> str:
    details = format_booking_details(
        booking.slot_date,
        booking.start_time,
        booking.end_time,
        booking.client_name,
        booking.client_telegram,
        booking.session_type,
        viewer_tz,
        viewer_label,
        booking.id,
    )
    return f"{title}\n\n{details}"


async def send_main_menu(message, text: str | None = None) -> None:
    await message.reply_text(text or texts.WELCOME_TEXT, reply_markup=main_menu_keyboard())


async def show_country_picker(target, pending_date: str | None = None) -> None:
    items = [(code, label) for code, label in POPULAR_COUNTRIES]
    markup = country_keyboard(items, pending_date=pending_date)

    if isinstance(target, CallbackQuery):
        await target.edit_message_text(texts.ASK_COUNTRY_PICKER, reply_markup=markup)
    else:
        await target.reply_text(texts.ASK_COUNTRY_PICKER, reply_markup=markup)


async def show_client_calendar_message(target, month_year: tuple[int, int] | None = None) -> None:
    now = now_local()

    if month_year is None:
        first_available = DB.get_first_available_month(now.date().isoformat(), now.strftime("%H:%M"))
        year, month = first_available or (now.year, now.month)
    else:
        year, month = month_year

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
        "manager_add": "اختر اليوم الذي تريد إضافة الساعات له:",
        "manager_remove_slot": "اختر اليوم الذي تريد حذف ساعة منه:",
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


async def show_slots_for_day(query: CallbackQuery, iso_date: str) -> None:
    now = now_local()
    slots = DB.get_available_slots(iso_date, now.date().isoformat(), now.strftime("%H:%M"))
    if not slots:
        await query.edit_message_text(texts.NO_SLOTS)
        return

    viewer_tz, _ = get_user_timezone_and_label(query.from_user.id if query.from_user else None)
    selected_date = date.fromisoformat(iso_date)
    button_data: list[tuple[int, str]] = []

    for slot in slots:
        start_dt, _ = get_slot_datetimes(slot.slot_date, slot.start_time, slot.end_time)
        local_start = start_dt.astimezone(viewer_tz)
        button_data.append((slot.id, format_hhmm(local_start)))

    await query.edit_message_text(
        f"{texts.CHOOSE_SLOT}\n\nالتاريخ المختار: {format_date_slash(iso_date)}",
        reply_markup=slots_keyboard(button_data, selected_date.year, selected_date.month, iso_date),
    )


async def maybe_show_country_then_slots(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, iso_date: str) -> None:
    profile = DB.get_user_profile(query.from_user.id if query.from_user else 0)
    if profile:
        await show_slots_for_day(query, iso_date)
        return

    context.user_data["country_pending_date"] = iso_date
    await show_country_picker(query, pending_date=iso_date)


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


async def country_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private(update):
        await update.effective_message.reply_text(texts.ONLY_PRIVATE)
        return
    context.user_data.pop("country_pending_date", None)
    context.user_data["state"] = None
    await show_country_picker(update.effective_message)


async def panel_command(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not is_private(update):
        await update.effective_message.reply_text(texts.ONLY_PRIVATE)
        return

    user = update.effective_user
    if not is_manager(user.id if user else None):
        await update.effective_message.reply_text(texts.FORBIDDEN_PANEL)
        return

    tz_label = get_user_timezone_and_label(user.id)[1]
    await update.effective_message.reply_text(
        f"{texts.PANEL_HEADER}\n\nمعرّفك الحالي: <code>{user.id}</code>\nبلد العرض الحالي: {tz_label}\nلتغييره استخدم /country",
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

    if state == "await_country_text":
        resolved = resolve_country_text(text)
        if not resolved:
            await update.effective_message.reply_text(texts.COUNTRY_NOT_FOUND)
            return

        country_name, timezone_name = resolved
        DB.set_user_profile(update.effective_user.id, country_name, timezone_name)
        user_data.pop("state", None)

        pending_date = user_data.pop("country_pending_date", None)
        await update.effective_message.reply_text(f"{texts.COUNTRY_SAVED}\n{country_name}")

        if pending_date:
            now = now_local()
            slots = DB.get_available_slots(pending_date, now.date().isoformat(), now.strftime("%H:%M"))
            if not slots:
                await update.effective_message.reply_text(texts.NO_SLOTS)
                await send_main_menu(update.effective_message)
                return

            viewer_tz, _ = get_user_timezone_and_label(update.effective_user.id)
            selected_date = date.fromisoformat(pending_date)
            button_data: list[tuple[int, str]] = []
            for slot in slots:
                start_dt, _ = get_slot_datetimes(slot.slot_date, slot.start_time, slot.end_time)
                local_start = start_dt.astimezone(viewer_tz)
                button_data.append((slot.id, format_hhmm(local_start)))

            await update.effective_message.reply_text(
                f"{texts.CHOOSE_SLOT}\n\nالتاريخ المختار: {format_date_slash(pending_date)}",
                reply_markup=slots_keyboard(button_data, selected_date.year, selected_date.month, pending_date),
            )
            return

        await send_main_menu(update.effective_message)
        return

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
        viewer_tz, viewer_label = get_user_timezone_and_label(update.effective_user.id if update.effective_user else None)
        await update.effective_message.reply_text(
            booking_summary_text(user_data["booking_draft"], viewer_tz, viewer_label),
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

        if not results["valid"]:
            await update.effective_message.reply_text(
                "لم أتمكن من قراءة الساعات. أرسلها بهذه الصيغة:\n"
                "9\n9:00\n13:00\n00:00\n٢١:٠٠"
            )
            return

        user_data.pop("state", None)
        user_data.pop("manager_selected_date", None)

        await update.effective_message.reply_text(
            f"{texts.MANAGER_ADD_DONE}\n\n"
            f"تم تحديث جدول المواعيد مباشرة.\n"
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

    if data.startswith("country_open:"):
        iso_date = data.split(":", 1)[1]
        context.user_data["country_pending_date"] = iso_date
        await show_country_picker(query, pending_date=iso_date)
        return

    if data.startswith("set_country:"):
        code = data.split(":", 1)[1]

        if code == "OTHER":
            context.user_data["state"] = "await_country_text"
            await query.edit_message_text(texts.ASK_COUNTRY_TEXT)
            return

        selected = COUNTRY_DATA.get(code)
        if not selected:
            await query.edit_message_text(texts.GENERIC_ERROR)
            return

        country_name, timezone_name, _ = selected
        DB.set_user_profile(query.from_user.id, country_name, timezone_name)

        pending_date = context.user_data.pop("country_pending_date", None)
        context.user_data.pop("state", None)

        if pending_date:
            await query.edit_message_text(f"{texts.COUNTRY_SAVED}\n{country_name}")
            await query.message.reply_text("جاري عرض الساعات حسب توقيتك...")
            await maybe_show_country_then_slots(query, context, pending_date)
            return

        await query.edit_message_text(f"{texts.COUNTRY_SAVED}\n{country_name}")
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
            await maybe_show_country_then_slots(query, context, iso_date)
            return

        if mode == "manager_add":
            context.user_data["state"] = "manager_await_slots_input"
            context.user_data["manager_selected_date"] = iso_date
            await query.edit_message_text(
                f"اليوم المختار: {format_date_slash(iso_date)}\n\n{texts.ASK_MANAGER_SLOTS}"
            )
            return

        if mode == "manager_remove_slot":
            await show_remove_slots_for_day(query, iso_date)
            return

        if mode == "manager_remove_day":
            success, reason, count = DB.remove_day(iso_date)
            if success:
                await query.edit_message_text(
                    f"{texts.MANAGER_REMOVE_DAY_DONE}\n\nالتاريخ: {format_date_slash(iso_date)}\nعدد الساعات المحذوفة: {count}"
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

    viewer_tz, viewer_label = get_user_timezone_and_label(query.from_user.id if query.from_user else None)
    await query.edit_message_text(
        f"تم اختيار الموعد التالي:\n\n{format_session_block(slot.slot_date, slot.start_time, slot.end_time, viewer_tz, viewer_label)}\n\n{texts.ASK_NAME}"
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

    client_tz, client_label = get_user_timezone_and_label(user.id)
    confirmation = booking_confirmation_text(booking, client_tz, client_label)

    await query.edit_message_text(confirmation)
    await query.message.reply_text("تم حفظ الموعد في سجلك.", reply_markup=main_menu_keyboard())

    for manager_id in SETTINGS.manager_ids:
        try:
            manager_tz, manager_label = get_user_timezone_and_label(manager_id)
            await context.bot.send_message(
                chat_id=manager_id,
                text=booking_confirmation_text(booking, manager_tz, manager_label),
            )
        except Exception:
            logger.exception("Failed to notify manager %s about new booking", manager_id)


async def show_user_bookings(update: Update, cancel_mode: bool = False) -> None:
    user = update.effective_user
    now = now_local()
    bookings = DB.get_user_upcoming_bookings(user.id, now.date().isoformat(), now.strftime("%H:%M"))

    if not bookings:
        await update.effective_message.reply_text(texts.NO_USER_BOOKINGS, reply_markup=main_menu_keyboard())
        return

    viewer_tz, viewer_label = get_user_timezone_and_label(user.id)
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
                viewer_tz,
                viewer_label,
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

    viewer_tz, viewer_label = get_user_timezone_and_label(user.id)
    details = format_booking_details(
        booking.slot_date,
        booking.start_time,
        booking.end_time,
        booking.client_name,
        booking.client_telegram,
        booking.session_type,
        viewer_tz,
        viewer_label,
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

    client_tz, client_label = get_user_timezone_and_label(user.id)
    cancellation = booking_cancellation_text(booking, client_tz, client_label)
    await query.edit_message_text(cancellation)
    await query.message.reply_text(texts.WELCOME_TEXT, reply_markup=main_menu_keyboard())

    for manager_id in SETTINGS.manager_ids:
        try:
            manager_tz, manager_label = get_user_timezone_and_label(manager_id)
            await context.bot.send_message(
                chat_id=manager_id,
                text=booking_cancellation_text(booking, manager_tz, manager_label),
            )
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

    manager_tz, manager_label = get_user_timezone_and_label(query.from_user.id)
    cancellation = booking_cancellation_text(booking, manager_tz, manager_label)
    await query.edit_message_text(cancellation)

    try:
        client_tz, client_label = get_user_timezone_and_label(booking.client_user_id)
        await context.bot.send_message(
            chat_id=booking.client_chat_id,
            text=booking_cancellation_text(booking, client_tz, client_label),
        )
    except Exception:
        logger.exception("Failed to notify client about manager cancellation")

    for manager_id in SETTINGS.manager_ids:
        if manager_id == query.from_user.id:
            continue
        try:
            other_tz, other_label = get_user_timezone_and_label(manager_id)
            await context.bot.send_message(
                chat_id=manager_id,
                text=booking_cancellation_text(booking, other_tz, other_label),
            )
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

        viewer_tz, viewer_label = get_user_timezone_and_label(query.from_user.id if query.from_user else None)
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
                viewer_tz,
                viewer_label,
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
        f"اختر الساعة التي تريد حذفها من يوم {format_date_slash(iso_date)}:",
        reply_markup=manager_slots_remove_keyboard(
            [
                (
                    slot.id,
                    format_hhmm(datetime.combine(date.today(), time.fromisoformat(slot.start_time)))
                )
                for slot in slots
            ]
        ),
    )


def add_slots_from_text(slot_date: str, text: str, created_by: int) -> dict:
    normalized_text = normalize_digits(text)
    parts = [p.strip() for p in re.split(r"[\n,]+", normalized_text) if p.strip()]
    valid = True
    created = 0
    reactivated = 0
    exists = 0

    for part in parts:
        try:
            start_norm = normalize_hour_input(part)
            end_norm = add_one_hour(start_norm)
        except ValueError:
            valid = False
            continue

        result = DB.upsert_slot(slot_date, start_norm, end_norm, created_by)
        if result == "created":
            created += 1
        elif result == "reactivated":
            reactivated += 1
        else:
            exists += 1

    return {
        "valid": bool(parts) and valid,
        "created": created,
        "reactivated": reactivated,
        "exists": exists,
    }


async def reminder_loop(app: Application) -> None:
    while True:
        try:
            now = now_local()
            due = DB.get_due_notifications(now.replace(tzinfo=None))
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
        client_title = "جلستك بعد ساعة"
        manager_title = "الجلسة بعد ساعة"
    elif kind == "start":
        client_title = "حان موعد جلستك"
        manager_title = "حان موعد الجلسة"
    else:
        return

    try:
        client_tz, client_label = get_user_timezone_and_label(booking.client_user_id)
        await app.bot.send_message(
            chat_id=booking.client_chat_id,
            text=reminder_text(booking, client_title, client_tz, client_label),
        )
    except Exception:
        logger.exception("Failed to send reminder to client %s", booking.client_chat_id)

    for manager_id in SETTINGS.manager_ids:
        try:
            manager_tz, manager_label = get_user_timezone_and_label(manager_id)
            await app.bot.send_message(
                chat_id=manager_id,
                text=reminder_text(booking, manager_title, manager_tz, manager_label),
            )
        except Exception:
            logger.exception("Failed to send reminder to manager %s", manager_id)


async def post_init(application: Application) -> None:
    public_non_manager = [
        BotCommand("start", "بدء البوت"),
        BotCommand("help", "المساعدة"),
    ]
    manager_commands = public_non_manager + [BotCommand("panel", "لوحة الإدارة")]

    await application.bot.set_my_commands(public_non_manager, scope=BotCommandScopeAllPrivateChats())
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
    app.add_handler(CommandHandler("country", country_command))
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