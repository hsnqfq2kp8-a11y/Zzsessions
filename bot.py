from __future__ import annotations

import asyncio
import difflib
import logging
import re
import unicodedata
from collections import defaultdict
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
from database import Booking, Database, Slot
from keyboards import (
    booking_summary_keyboard,
    bookings_list_keyboard,
    calendar_keyboard,
    country_keyboard,
    main_menu_keyboard,
    manager_bookings_remove_keyboard,
    manager_slots_remove_keyboard,
    notification_settings_keyboard,
    panel_keyboard,
    schedule_notification_keyboard,
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
SCHEDULE_ALERT_DELAY_MINUTES = 10

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
ARABIC_DAY_NAMES = [
    "الاثنين",
    "الثلاثاء",
    "الأربعاء",
    "الخميس",
    "الجمعة",
    "السبت",
    "الأحد",
]

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


def strip_diacritics(value: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFKD", value) if not unicodedata.combining(ch))


def normalize_country_text(value: str) -> str:
    value = normalize_digits(value).strip().lower()
    value = strip_diacritics(value)
    value = value.translate(ARABIC_NORMALIZE_MAP)
    value = re.sub(r"[\s\-_]+", "", value)
    return value


def build_country_index() -> None:
    for _, (country_name, timezone_name, aliases) in COUNTRY_DATA.items():
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


def _detect_meridiem(raw: str) -> tuple[str, str | None]:
    value = normalize_digits(raw).strip().lower()
    value = strip_diacritics(value).replace(".", "")
    value = re.sub(r"\s+", " ", value).strip()

    pm_tokens = ["مساءا", "مساء", " pm", "p m", "pm", "م"]
    am_tokens = ["صباحا", "صباح", " am", "a m", "am", "ص"]

    for token in pm_tokens:
        if value.endswith(token):
            return value[: -len(token)].strip(), "pm"

    for token in am_tokens:
        if value.endswith(token):
            return value[: -len(token)].strip(), "am"

    return value, None


def normalize_hour_input(value: str) -> str:
    base, meridiem = _detect_meridiem(value)
    match = HOUR_ONLY_RE.match(base)
    if not match:
        raise ValueError("Invalid hour format")

    hour = int(match.group(1))
    minute = int(match.group(2) or "0")
    if not (0 <= minute <= 59):
        raise ValueError("Minute out of range")

    if meridiem is None:
        if not (0 <= hour <= 23):
            raise ValueError("Hour out of range")
        return f"{hour:02d}:{minute:02d}"

    if not (1 <= hour <= 12):
        raise ValueError("Hour out of range")

    if meridiem == "am":
        hour = 0 if hour == 12 else hour
    else:
        hour = hour if hour == 12 else hour + 12

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
    user_data.pop("cancel_booking_id", None)


def format_date_slash(slot_date: str) -> str:
    dt = date.fromisoformat(slot_date)
    return f"{dt.year}/{dt.month}/{dt.day}"


def arabic_day_name(d: date) -> str:
    return ARABIC_DAY_NAMES[d.weekday()]


def display_sort_value(hhmm: str) -> tuple[int, int]:
    hh, mm = hhmm.split(":")
    hour = int(hh)
    minute = int(mm)
    rank_hour = hour + 24 if hour < 4 else hour
    return rank_hour, minute


def sort_slots_for_display(slots: list[Slot]) -> list[Slot]:
    return sorted(slots, key=lambda slot: (display_sort_value(slot.start_time), slot.id))


def sort_bookings_for_display(bookings: list[Booking]) -> list[Booking]:
    return sorted(bookings, key=lambda booking: (booking.slot_date, display_sort_value(booking.start_time), booking.id))


def format_time_arabic(dt: datetime) -> str:
    hour24 = dt.hour
    minute = dt.minute
    period = "صباحا" if hour24 < 12 else "مساءا"
    hour12 = hour24 % 12 or 12
    suffix = " بعد منتصف الليل" if hour24 < 4 else ""
    if minute == 0:
        return f"{hour12} {period}{suffix}"
    return f"{hour12}:{minute:02d} {period}{suffix}"


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


def manager_booking_tag(booking: Booking) -> str | None:
    seq = DB.get_confirmed_day_time_sequence(booking.id)
    return f"#{seq}" if seq else None


def format_session_block(slot_date: str, start_time: str, end_time: str, viewer_tz: ZoneInfo, viewer_label: str) -> str:
    start_dt, _ = get_slot_datetimes(slot_date, start_time, end_time)
    local_start = start_dt.astimezone(viewer_tz)
    lines = [
        f"اليوم : {arabic_day_name(local_start.date())} {local_start.year}/{local_start.month}/{local_start.day}",
        f"الساعة : {format_time_arabic(local_start)} بتوقيت {viewer_label}",
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
    booking_tag: str | None = None,
    cancellation_reason: str | None = None,
) -> str:
    lines = [
        f"الاسم : {client_name}",
        f"يوزر التيليغرام : {client_telegram}",
        f"نوع الجلسة : {session_type}",
        format_session_block(slot_date, start_time, end_time, viewer_tz, viewer_label),
    ]
    if booking_tag:
        lines.append(booking_tag)
    if cancellation_reason:
        lines.append(f"سبب الالغاء : {cancellation_reason}")
    return "\n".join(lines)


def booking_selector_label(booking: Booking, viewer_tz: ZoneInfo, action_word: str) -> str:
    start_dt, _ = get_slot_datetimes(booking.slot_date, booking.start_time, booking.end_time)
    local_start = start_dt.astimezone(viewer_tz)
    return f"{action_word} {arabic_day_name(local_start.date())} {local_start.month}/{local_start.day} - {format_time_arabic(local_start)}"


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


def booking_confirmation_text(booking: Booking, viewer_tz: ZoneInfo, viewer_label: str, booking_tag: str | None = None) -> str:
    details = format_booking_details(
        booking.slot_date,
        booking.start_time,
        booking.end_time,
        booking.client_name,
        booking.client_telegram,
        booking.session_type,
        viewer_tz,
        viewer_label,
        booking_tag=booking_tag,
    )
    return f"تم حجز جلسة ✅\n\n{details}"


def booking_cancellation_text(
    booking: Booking,
    viewer_tz: ZoneInfo,
    viewer_label: str,
    cancellation_reason: str | None = None,
    booking_tag: str | None = None,
) -> str:
    details = format_booking_details(
        booking.slot_date,
        booking.start_time,
        booking.end_time,
        booking.client_name,
        booking.client_telegram,
        booking.session_type,
        viewer_tz,
        viewer_label,
        booking_tag=booking_tag,
        cancellation_reason=cancellation_reason or booking.cancellation_reason,
    )
    return f"تم إلغاء حجز\n\n{details}"


def reminder_text(booking: Booking, title: str, viewer_tz: ZoneInfo, viewer_label: str, booking_tag: str | None = None) -> str:
    details = format_booking_details(
        booking.slot_date,
        booking.start_time,
        booking.end_time,
        booking.client_name,
        booking.client_telegram,
        booking.session_type,
        viewer_tz,
        viewer_label,
        booking_tag=booking_tag,
    )
    return f"{title}\n\n{details}"


def confirmation_text_for_recipient(booking: Booking, recipient_user_id: int | None) -> str:
    viewer_tz, viewer_label = get_user_timezone_and_label(recipient_user_id)
    tag = manager_booking_tag(booking) if is_manager(recipient_user_id) else None
    return booking_confirmation_text(booking, viewer_tz, viewer_label, booking_tag=tag)


def cancellation_text_for_recipient(
    booking: Booking,
    recipient_user_id: int | None,
    cancellation_reason: str | None = None,
    manager_tag: str | None = None,
) -> str:
    viewer_tz, viewer_label = get_user_timezone_and_label(recipient_user_id)
    tag = manager_tag if is_manager(recipient_user_id) else None
    return booking_cancellation_text(
        booking,
        viewer_tz,
        viewer_label,
        cancellation_reason=cancellation_reason,
        booking_tag=tag,
    )


def reminder_text_for_recipient(
    booking: Booking,
    recipient_user_id: int | None,
    title: str,
) -> str:
    viewer_tz, viewer_label = get_user_timezone_and_label(recipient_user_id)
    tag = manager_booking_tag(booking) if is_manager(recipient_user_id) else None
    return reminder_text(booking, title, viewer_tz, viewer_label, booking_tag=tag)


def notification_status_text(is_enabled: bool) -> str:
    status = texts.NOTIFY_STATUS_ENABLED if is_enabled else texts.NOTIFY_STATUS_DISABLED
    return f"{texts.NOTIFY_MENU_HEADER}\n\nحالة الاشعارات: {status}\n\n{texts.NOTIFY_MENU_BODY}"


def notification_dates_text(dates: list[str]) -> str:
    lines = [texts.NOTIFY_ALERT_TITLE, ""]
    for slot_date in dates:
        day = date.fromisoformat(slot_date)
        lines.append(f"- {arabic_day_name(day)} {day.year}/{day.month}/{day.day}")
    return "\n".join(lines)


def notification_back(source_token: str) -> str:
    if source_token.startswith("cal-"):
        _, year, month = source_token.split("-")
        return f"calendar:client:{year}:{month}"
    return "go:home"


async def show_notification_menu(target, user_id: int, source_token: str = "home") -> None:
    is_enabled = DB.get_availability_alert_enabled(user_id)
    text_value = notification_status_text(is_enabled)
    markup = notification_settings_keyboard(is_enabled, source_token, notification_back(source_token))
    if isinstance(target, CallbackQuery):
        await target.edit_message_text(text_value, reply_markup=markup)
    else:
        await target.reply_text(text_value, reply_markup=markup)


async def notify_managers_booking(
    context: ContextTypes.DEFAULT_TYPE,
    booking: Booking,
    exclude_user_id: int | None = None,
) -> None:
    for manager_id in SETTINGS.manager_ids:
        if exclude_user_id is not None and manager_id == exclude_user_id:
            continue
        try:
            manager_text = confirmation_text_for_recipient(booking, manager_id)
            await context.bot.send_message(chat_id=manager_id, text=manager_text)
        except Exception:
            logger.exception("Failed to notify manager %s about new booking", manager_id)


async def notify_managers_cancellation(
    context: ContextTypes.DEFAULT_TYPE,
    booking: Booking,
    exclude_user_id: int | None = None,
    cancellation_reason: str | None = None,
    manager_tag: str | None = None,
) -> None:
    for manager_id in SETTINGS.manager_ids:
        if exclude_user_id is not None and manager_id == exclude_user_id:
            continue
        try:
            manager_text = cancellation_text_for_recipient(
                booking,
                manager_id,
                cancellation_reason=cancellation_reason,
                manager_tag=manager_tag,
            )
            await context.bot.send_message(chat_id=manager_id, text=manager_text)
        except Exception:
            logger.exception("Failed to notify manager %s about cancellation", manager_id)


async def notify_managers_reminder(
    app: Application,
    booking: Booking,
    manager_title: str,
    exclude_user_id: int | None = None,
) -> None:
    for manager_id in SETTINGS.manager_ids:
        if exclude_user_id is not None and manager_id == exclude_user_id:
            continue
        try:
            manager_text = reminder_text_for_recipient(booking, manager_id, manager_title)
            await app.bot.send_message(chat_id=manager_id, text=manager_text)
        except Exception:
            logger.exception("Failed to send reminder to manager %s", manager_id)


async def send_schedule_availability_alerts(app: Application) -> None:
    marker, changed_dates = DB.get_due_schedule_alert_batch(SCHEDULE_ALERT_DELAY_MINUTES)
    if not marker:
        return

    now = now_local()
    available_dates = DB.get_dates_with_available_slots(changed_dates, now.date().isoformat(), now.strftime("%H:%M"))
    if available_dates:
        text_value = notification_dates_text(available_dates)
        markup = schedule_notification_keyboard()
        for subscriber in DB.get_enabled_alert_subscribers():
            try:
                await app.bot.send_message(
                    chat_id=subscriber["chat_id"],
                    text=text_value,
                    reply_markup=markup,
                )
            except Exception:
                logger.exception("Failed to send schedule availability alert to %s", subscriber["chat_id"])

    DB.mark_schedule_alert_batch_processed(marker)


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
    slots = sort_slots_for_display(DB.get_available_slots(iso_date, now.date().isoformat(), now.strftime("%H:%M")))
    if not slots:
        await query.edit_message_text(texts.NO_SLOTS)
        return

    viewer_tz, _ = get_user_timezone_and_label(query.from_user.id if query.from_user else None)
    selected_date = date.fromisoformat(iso_date)
    button_data: list[tuple[int, str]] = []

    for slot in slots:
        start_dt, _ = get_slot_datetimes(slot.slot_date, slot.start_time, slot.end_time)
        local_start = start_dt.astimezone(viewer_tz)
        button_data.append((slot.id, format_time_arabic(local_start)))

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
            slots = sort_slots_for_display(DB.get_available_slots(pending_date, now.date().isoformat(), now.strftime("%H:%M")))
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
                button_data.append((slot.id, format_time_arabic(local_start)))

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
                "18\n18:00\n6 م\n6 مساءا\n6 ص"
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

    if state == "await_cancel_reason":
        if not text:
            await update.effective_message.reply_text(texts.ASK_CANCEL_REASON)
            return

        booking_id = user_data.get("cancel_booking_id")
        user = update.effective_user
        booking = DB.get_booking(booking_id) if booking_id else None

        if not booking or not user or booking.client_user_id != user.id or booking.status != "confirmed":
            clear_booking_flow(user_data)
            await update.effective_message.reply_text(texts.BOOKING_CANCEL_NOT_FOUND, reply_markup=main_menu_keyboard())
            return

        manager_tag = manager_booking_tag(booking)

        success, _ = DB.cancel_booking(booking_id, by_user_id=user.id, cancellation_reason=text)
        if not success:
            clear_booking_flow(user_data)
            await update.effective_message.reply_text(texts.BOOKING_CANCEL_NOT_FOUND, reply_markup=main_menu_keyboard())
            return

        booking.cancellation_reason = text
        cancellation = cancellation_text_for_recipient(booking, user.id, cancellation_reason=text, manager_tag=manager_tag)
        clear_booking_flow(user_data)
        await update.effective_message.reply_text(cancellation, reply_markup=main_menu_keyboard())

        await notify_managers_cancellation(
            context,
            booking,
            exclude_user_id=user.id if user.id in SETTINGS.manager_ids else None,
            cancellation_reason=text,
            manager_tag=manager_tag,
        )
        return

    if text == "عرض المواعيد المتاحة":
        if not DB.is_booking_open():
            await update.effective_message.reply_text(texts.BOOKING_CLOSED)
            return
        await show_client_calendar_message(update.effective_message)
        return

    if text == "اشعارات المواعيد الجديدة":
        await show_notification_menu(update.effective_message, update.effective_user.id if update.effective_user else 0, "home")
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

    if data == "notify:view":
        await show_client_calendar_message(query)
        return

    if data.startswith("notify:open:"):
        _, _, year, month = data.split(":")
        await show_notification_menu(query, query.from_user.id, f"cal-{year}-{month}")
        return

    if data.startswith("notify:set:"):
        _, _, state_value, source_token = data.split(":", 3)
        enabled = state_value == "on"
        DB.set_availability_alert(query.from_user.id, query.message.chat_id, enabled)
        text_value = texts.NOTIFY_ENABLED if enabled else texts.NOTIFY_DISABLED
        await query.edit_message_text(
            f"{text_value}\n\n{notification_status_text(enabled)}",
            reply_markup=notification_settings_keyboard(enabled, source_token, notification_back(source_token)),
        )
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
            success, reason, count, booked_count = DB.remove_day(iso_date)
            if success and reason == "removed_all":
                await query.edit_message_text(
                    f"{texts.MANAGER_REMOVE_DAY_DONE}\n\nالتاريخ: {format_date_slash(iso_date)}\nعدد الساعات المحذوفة: {count}"
                )
            elif success and reason == "partial":
                await query.edit_message_text(
                    f"{texts.MANAGER_REMOVE_DAY_PARTIAL_DONE}\n\nالتاريخ: {format_date_slash(iso_date)}\n"
                    f"عدد الساعات المحذوفة: {count}\nالحجوزات المؤكدة المتبقية: {booked_count}"
                )
            elif success and reason == "booked_only":
                await query.edit_message_text(
                    f"{texts.MANAGER_REMOVE_DAY_BOOKED_ONLY}\n\nالتاريخ: {format_date_slash(iso_date)}\n"
                    f"الحجوزات المؤكدة المتبقية: {booked_count}"
                )
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

    if data.startswith("booking_cancel:"):
        booking_id = int(data.split(":", 1)[1])
        await prompt_cancel_booking(query, context, booking_id)
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

    confirmation = confirmation_text_for_recipient(booking, user.id)
    await query.edit_message_text(confirmation)
    await query.message.reply_text("تم حفظ الموعد في سجلك.", reply_markup=main_menu_keyboard())

    await notify_managers_booking(
        context,
        booking,
        exclude_user_id=user.id if user.id in SETTINGS.manager_ids else None,
    )


async def show_user_bookings(update: Update, cancel_mode: bool = False) -> None:
    user = update.effective_user
    now = now_local()
    bookings = sort_bookings_for_display(DB.get_user_upcoming_bookings(user.id, now.date().isoformat(), now.strftime("%H:%M")))

    if not bookings:
        await update.effective_message.reply_text(texts.NO_USER_BOOKINGS, reply_markup=main_menu_keyboard())
        return

    viewer_tz, viewer_label = get_user_timezone_and_label(user.id)
    header = "هذه مواعيدك القادمة:" if not cancel_mode else "اختر الحجز الذي تريد إلغاءه:"
    lines = [header, ""]
    button_items: list[tuple[int, str]] = []

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
                booking_tag=None,
            )
        )
        lines.append("")
        if cancel_mode:
            button_items.append((booking.id, booking_selector_label(booking, viewer_tz, "إلغاء")))

    reply_markup = bookings_list_keyboard(button_items) if cancel_mode else None
    await update.effective_message.reply_text("\n".join(lines).strip(), reply_markup=reply_markup)


async def prompt_cancel_booking(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, booking_id: int) -> None:
    booking = DB.get_booking(booking_id)
    user = query.from_user

    if not booking or booking.client_user_id != user.id or booking.status != "confirmed":
        await query.edit_message_text(texts.BOOKING_CANCEL_NOT_FOUND)
        return

    context.user_data["state"] = "await_cancel_reason"
    context.user_data["cancel_booking_id"] = booking_id

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
        booking_tag=None,
    )
    await query.edit_message_text(f"{details}\n\n{texts.ASK_CANCEL_REASON}")


async def manager_cancel_booking(query: CallbackQuery, context: ContextTypes.DEFAULT_TYPE, booking_id: int) -> None:
    if not is_manager(query.from_user.id if query.from_user else None):
        await query.edit_message_text(texts.FORBIDDEN_PANEL)
        return

    booking = DB.get_booking(booking_id)
    if not booking or booking.status != "confirmed":
        await query.edit_message_text(texts.BOOKING_CANCEL_NOT_FOUND)
        return

    tag = manager_booking_tag(booking)

    success, _ = DB.cancel_booking(booking_id)
    if not success:
        await query.edit_message_text(texts.BOOKING_CANCEL_NOT_FOUND)
        return

    manager_text = cancellation_text_for_recipient(booking, query.from_user.id, manager_tag=tag)
    await query.edit_message_text(manager_text)

    try:
        client_text = cancellation_text_for_recipient(booking, booking.client_user_id, manager_tag=tag)
        await context.bot.send_message(chat_id=booking.client_chat_id, text=client_text)
    except Exception:
        logger.exception("Failed to notify client about manager cancellation")

    await notify_managers_cancellation(
        context,
        booking,
        exclude_user_id=query.from_user.id,
        manager_tag=tag,
    )


def section_title_for_bookings(group_date: date, today_date: date) -> str:
    if group_date == today_date:
        return "حجوزات اليوم :"
    return f"حجوزات {arabic_day_name(group_date)} :"


def split_booking_sections(blocks: list[str], heading: str, limit: int = 3500) -> list[str]:
    sections: list[str] = []
    current = heading
    for block in blocks:
        candidate = current + "\n\n" + block if current else block
        if len(candidate) > limit and current != heading:
            sections.append(current)
            current = heading + "\n\n" + block
        else:
            current = candidate
    if current:
        sections.append(current)
    return sections


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
        bookings = sort_bookings_for_display(DB.get_all_upcoming_bookings(now.date().isoformat(), now.strftime("%H:%M")))
        if not bookings:
            await query.edit_message_text(texts.PANEL_NO_BOOKINGS_TO_CANCEL)
            return

        viewer_tz, viewer_label = get_user_timezone_and_label(query.from_user.id if query.from_user else None)
        lines = [texts.MANAGER_REMOVE_BOOKING_TITLE, ""]
        button_items: list[tuple[int, str]] = []

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
                    booking_tag=manager_booking_tag(booking),
                )
            )
            lines.append("")
            button_items.append((booking.id, booking_selector_label(booking, viewer_tz, "حذف")))

        await query.edit_message_text(
            "\n".join(lines).strip(),
            reply_markup=manager_bookings_remove_keyboard(button_items),
        )
        return

    if action == "bookings":
        now = now_local()
        bookings = sort_bookings_for_display(DB.get_all_upcoming_bookings(now.date().isoformat(), now.strftime("%H:%M")))
        if not bookings:
            await query.edit_message_text(texts.PANEL_NO_BOOKINGS)
            return

        viewer_tz, viewer_label = get_user_timezone_and_label(query.from_user.id if query.from_user else None)
        grouped: dict[date, list[Booking]] = defaultdict(list)
        for booking in bookings:
            grouped[date.fromisoformat(booking.slot_date)].append(booking)

        sections_text: list[str] = []
        for group_date in sorted(grouped.keys()):
            day_bookings = sorted(grouped[group_date], key=lambda b: (display_sort_value(b.start_time), b.id))
            heading = section_title_for_bookings(group_date, now.date())
            blocks = [
                format_booking_details(
                    booking.slot_date,
                    booking.start_time,
                    booking.end_time,
                    booking.client_name,
                    booking.client_telegram,
                    booking.session_type,
                    viewer_tz,
                    viewer_label,
                    booking_tag=manager_booking_tag(booking),
                )
                for booking in day_bookings
            ]
            sections_text.extend(split_booking_sections(blocks, heading))

        await query.edit_message_text(sections_text[0])
        for section in sections_text[1:]:
            await query.message.reply_text(section)
        return

    if action == "toggle":
        current = DB.is_booking_open()
        DB.set_booking_open(not current)
        msg = texts.BOOKING_CLOSED_DONE if current else texts.BOOKING_OPENED
        await query.edit_message_text(msg, reply_markup=panel_keyboard(DB.is_booking_open()))
        return


async def show_remove_slots_for_day(query: CallbackQuery, iso_date: str) -> None:
    slots = sort_slots_for_display(DB.get_all_slots_for_date(iso_date))
    if not slots:
        await query.edit_message_text(texts.MANAGER_NO_SLOTS_THIS_DAY)
        return

    await query.edit_message_text(
        f"اختر الساعة التي تريد حذفها من يوم {format_date_slash(iso_date)}:",
        reply_markup=manager_slots_remove_keyboard(
            [
                (
                    slot.id,
                    format_time_arabic(datetime.combine(date.today(), time.fromisoformat(slot.start_time))),
                )
                for slot in slots
            ]
        ),
    )


def add_slots_from_text(slot_date: str, text: str, created_by: int) -> dict:
    normalized_text = normalize_digits(text)
    parts = [p.strip() for p in re.split(r"[\n,]+", normalized_text) if p.strip()]
    valid_count = 0
    created = 0
    reactivated = 0
    exists = 0

    for part in parts:
        try:
            start_norm = normalize_hour_input(part)
            end_norm = add_one_hour(start_norm)
            valid_count += 1
        except ValueError:
            continue

        result = DB.upsert_slot(slot_date, start_norm, end_norm, created_by)
        if result == "created":
            created += 1
        elif result == "reactivated":
            reactivated += 1
        else:
            exists += 1

    return {
        "valid": valid_count > 0,
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

            await send_schedule_availability_alerts(app)
        except Exception:
            logger.exception("Reminder loop error")
        await asyncio.sleep(SETTINGS.check_interval_seconds)


async def send_reminder(app: Application, booking: Booking, kind: str) -> None:
    if kind == "day":
        client_title = "بكرا موعدك"
        manager_title = "بكرا موعد الجلسة"
    elif kind == "hour":
        client_title = "جلستك بعد ساعة"
        manager_title = "الجلسة بعد ساعة"
    elif kind == "start":
        client_title = "حان موعد جلستك"
        manager_title = "حان موعد الجلسة"
    else:
        return

    try:
        client_text = reminder_text_for_recipient(booking, booking.client_user_id, client_title)
        await app.bot.send_message(chat_id=booking.client_chat_id, text=client_text)
    except Exception:
        logger.exception("Failed to send reminder to client %s", booking.client_chat_id)

    await notify_managers_reminder(
        app,
        booking,
        manager_title,
        exclude_user_id=booking.client_user_id if booking.client_user_id in SETTINGS.manager_ids else None,
    )


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