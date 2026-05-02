import collections
import datetime
import json
import logging
import os
import sqlite3
import struct
import threading
import time
import zlib

import requests
import telebot
from flask import Flask, jsonify
from telebot import types

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
logger = logging.getLogger("demo_single")

# Transient network failures that should be retried (Connection reset by peer,
# socket timeouts, etc). When the VPS network blips, Telegram drops the TLS
# connection mid-call; we retry a couple of times before giving up so the
# polling loop doesn't die from a single dropped packet.
_TRANSIENT_NETWORK_EXCEPTIONS = (
    requests.exceptions.ConnectionError,
    requests.exceptions.Timeout,
    requests.exceptions.ChunkedEncodingError,
    ConnectionResetError,
)


class _ResilientExceptionHandler(telebot.ExceptionHandler):
    """Swallow handler-level exceptions so infinity_polling never dies.

    Without this, an unhandled ConnectionError inside e.g. send_media_group
    propagates all the way up to the polling loop and kills it. infinity_polling
    then takes a few seconds to come back, during which the bot looks frozen.
    """

    def handle(self, exception):
        logger.warning(
            "Bot handler raised %s; swallowed to keep polling alive",
            type(exception).__name__,
            exc_info=exception,
        )
        return True

def load_env_file(path=".env"):
    if not os.path.exists(path):
        return
    with open(path, "r", encoding="utf-8") as file:
        for line in file:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def _load_env():
    try:
        from dotenv import load_dotenv as _load_dotenv
    except ImportError:
        load_env_file()
        return
    _load_dotenv()


_load_env()


TOKEN = os.getenv("TELEGRAM_TOKEN", "YOUR_TOKEN").strip()


def _parse_int_env(name, default):
    raw = os.getenv(name)
    if not raw:
        return default
    try:
        return int(raw.strip())
    except ValueError:
        return default


OWNER_ID = _parse_int_env("OWNER_ID", _parse_int_env("ADMIN_ID", 0))
DB_PATH = os.getenv("BOT_DB_PATH", "bot.db")
MASTER_CONTACT = os.getenv("MASTER_CONTACT", "Контакты мастера пока не указаны")
PORTFOLIO_DIR = os.getenv("PORTFOLIO_DIR", "portfolio_examples")

# Demo-mode flags. The whole point of this repo is to be a public, click-around
# showcase of how the booking bot looks for an end client. Real bookings, admin
# notifications and DB writes for bookings are short-circuited; the user sees
# polished confirmations explaining that this is a demo.
DEMO_MODE = os.getenv("DEMO_MODE", "1").strip() not in ("", "0", "false", "False")
DEMO_USERNAME = os.getenv("DEMO_USERNAME", "@kilolown").strip()
DEMO_TARIFFS_URL = os.getenv("DEMO_TARIFFS_URL", "https://t.me/your_channel").strip()

if TOKEN in ("", "YOUR_TOKEN") or ":" not in TOKEN:
    raise RuntimeError(
        "Не задан TELEGRAM_TOKEN. Создайте .env рядом с bot.py или задайте "
        "переменную окружения TELEGRAM_TOKEN токеном от @BotFather."
    )

bot = telebot.TeleBot(TOKEN, exception_handler=_ResilientExceptionHandler())


def _send_media_group_with_retry(chat_id, image_paths, caption, attempts=3, backoff=0.5):
    """Send a media group, re-opening file handles on each retry."""
    last_exc = None
    for attempt in range(attempts):
        media = []
        handles = []
        try:
            for index, path in enumerate(image_paths):
                photo = open(path, "rb")
                handles.append(photo)
                media.append(
                    types.InputMediaPhoto(photo, caption=caption if index == 0 else "")
                )
            return bot.send_media_group(chat_id, media)
        except _TRANSIENT_NETWORK_EXCEPTIONS as exc:
            last_exc = exc
            logger.warning(
                "send_media_group attempt %d/%d failed: %s",
                attempt + 1,
                attempts,
                exc,
            )
            if attempt + 1 < attempts:
                time.sleep(backoff * (attempt + 1))
        finally:
            for handle in handles:
                try:
                    handle.close()
                except Exception:
                    pass
    raise last_exc


app = Flask(__name__)

db_lock = threading.Lock()
user_data = {}
admin_states = {}

# Track recent bot-sent message ids per chat for the "Очистить чат" feature.
_chat_messages_lock = threading.Lock()
_chat_messages = collections.defaultdict(lambda: collections.deque(maxlen=200))


def _record_message(chat_id, message_id):
    if chat_id is None or message_id is None:
        return
    with _chat_messages_lock:
        _chat_messages[chat_id].append(message_id)


_orig_send_message = bot.send_message
_orig_send_photo = bot.send_photo


def _tracked_send_message(chat_id, *args, **kwargs):
    msg = _orig_send_message(chat_id, *args, **kwargs)
    try:
        _record_message(msg.chat.id, msg.message_id)
    except Exception:
        pass
    return msg


def _tracked_send_photo(chat_id, *args, **kwargs):
    msg = _orig_send_photo(chat_id, *args, **kwargs)
    try:
        _record_message(msg.chat.id, msg.message_id)
    except Exception:
        pass
    return msg


bot.send_message = _tracked_send_message
bot.send_photo = _tracked_send_photo


_orig_process_new_messages = bot.process_new_messages


def _patched_process_new_messages(messages):
    # Record every incoming user message so "Очистить чат" can delete it too.
    for m in messages:
        try:
            _record_message(m.chat.id, m.message_id)
        except Exception:
            pass
    return _orig_process_new_messages(messages)


bot.process_new_messages = _patched_process_new_messages


def clear_chat(chat_id):
    """Delete recorded messages for chat_id (best-effort).

    Tracks both bot-sent and user-sent messages; old/undeletable ids are skipped.
    """
    with _chat_messages_lock:
        ids = list(_chat_messages[chat_id])
        _chat_messages[chat_id].clear()
    for msg_id in ids:
        try:
            bot.delete_message(chat_id, msg_id)
        except Exception:
            pass


client_states = {}
nav_stacks = {}

SERVICE_CATALOG = {
    "cosmo": {
        "title": "Косметология",
        "duration": 90,
        "price": "X",
        "description": "Уходовые процедуры для очищения, увлажнения и улучшения состояния кожи.",
        "subtypes": {
            "face_cleaning": {
                "title": "Чистка лица",
                "description": "Глубокое очищение кожи, удаление загрязнений и подготовка к уходу.",
            },
            "care": {
                "title": "Уходовая процедура",
                "description": "Комплексный уход с акцентом на увлажнение, питание и восстановление кожи.",
            },
            "peeling": {
                "title": "Пилинг",
                "description": "Обновление кожи, выравнивание тона и улучшение текстуры.",
            },
        },
    },
    "nails": {
        "title": "Маникюр",
        "duration": 120,
        "price": "X",
        "description": "Обработка ногтей и кутикулы, аккуратное покрытие и дизайн по желанию.",
        "subtypes": {
            "classic": {
                "title": "Классический маникюр",
                "description": "Базовый уход за ногтями и кутикулой с чистым аккуратным результатом.",
            },
            "nude": {
                "title": "Нюдовый дизайн",
                "description": "Спокойные натуральные оттенки для универсального ежедневного образа.",
            },
            "french": {
                "title": "Френч и геометрия",
                "description": "Лаконичный дизайн с френчем, линиями, акцентами и геометрией.",
            },
        },
    },
    "pedicure": {
        "title": "Педикюр",
        "duration": 105,
        "price": "X",
        "description": "Уход за стопами и ногтями, аккуратная обработка и покрытие по желанию.",
        "subtypes": {
            "classic": {
                "title": "Классический педикюр",
                "description": "Аккуратная обработка стоп и ногтей для ухоженного результата.",
            },
            "gel": {
                "title": "Покрытие гель-лаком",
                "description": "Стойкое покрытие с глянцевым финишем и аккуратной обработкой.",
            },
            "spa": {
                "title": "Spa-педикюр",
                "description": "Расслабляющий уход для стоп с мягким spa-эффектом.",
            },
        },
    },
    "brows": {
        "title": "Брови",
        "duration": 60,
        "price": "X",
        "description": "Коррекция формы, окрашивание и укладка для выразительного взгляда.",
        "subtypes": {
            "correction": {
                "title": "Коррекция формы",
                "description": "Подбор и аккуратная коррекция формы под черты лица.",
            },
            "coloring": {
                "title": "Окрашивание",
                "description": "Подбор оттенка и окрашивание для более выразительных бровей.",
            },
            "lamination": {
                "title": "Долговременная укладка",
                "description": "Фиксация направления волосков и ухоженный эффект на несколько недель.",
            },
        },
    },
    "makeup": {
        "title": "Макияж",
        "duration": 105,
        "price": "X",
        "description": "Макияж под событие, стиль и пожелания клиента с учетом особенностей внешности.",
        "subtypes": {
            "day": {
                "title": "Дневной макияж",
                "description": "Легкий естественный образ для повседневных задач и встреч.",
            },
            "evening": {
                "title": "Вечерний макияж",
                "description": "Более яркий образ с акцентом на глаза, губы или сияние кожи.",
            },
            "wedding": {
                "title": "Свадебный образ",
                "description": "Стойкий и фотогеничный макияж для важного события.",
            },
        },
    },
}

SERVICES = {key: value["title"] for key, value in SERVICE_CATALOG.items()}

WORK_DURATIONS = [60, 90, 105, 120]
BREAK_DURATIONS = [15, 30, 60]


def get_connection():
    return sqlite3.connect(DB_PATH, check_same_thread=False)


conn = get_connection()
cursor = conn.cursor()


def init_db():
    with db_lock:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS bookings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER,
                service TEXT,
                date TEXT,
                time TEXT,
                end_time TEXT,
                contact TEXT,
                reminder_sent INTEGER DEFAULT 0,
                status TEXT DEFAULT 'confirmed',
                created_at TEXT,
                client_name TEXT,
                client_username TEXT,
                master_reminder_sent INTEGER DEFAULT 0
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS reschedule_requests (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                booking_id INTEGER,
                user_id INTEGER,
                old_date TEXT,
                old_time TEXT,
                old_end_time TEXT,
                new_slot_id INTEGER,
                new_date TEXT,
                new_time TEXT,
                new_end_time TEXT,
                status TEXT DEFAULT 'pending',
                created_at TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS day_slots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date TEXT,
                start_time TEXT,
                end_time TEXT,
                is_break INTEGER DEFAULT 0,
                is_booked INTEGER DEFAULT 0,
                is_closed INTEGER DEFAULT 0,
                source_mode TEXT DEFAULT 'template'
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS client_notes (
                user_id INTEGER PRIMARY KEY,
                client_name TEXT,
                note TEXT,
                is_blocked INTEGER DEFAULT 0,
                updated_at TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                added_by INTEGER,
                added_at TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS client_profiles (
                user_id INTEGER PRIMARY KEY,
                client_name TEXT,
                client_username TEXT,
                allergies TEXT,
                preferences TEXT,
                extra_notes TEXT,
                updated_at TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS tg_users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                full_name TEXT,
                last_seen_at TEXT
            )
            """
        )
        conn.commit()

    ensure_column("bookings", "end_time", "TEXT")
    ensure_column("bookings", "reminder_sent", "INTEGER DEFAULT 0")
    ensure_column("bookings", "status", "TEXT DEFAULT 'confirmed'")
    ensure_column("bookings", "created_at", "TEXT")
    ensure_column("bookings", "client_name", "TEXT")
    ensure_column("bookings", "client_username", "TEXT")
    ensure_column("bookings", "master_reminder_sent", "INTEGER DEFAULT 0")
    ensure_column("day_slots", "is_closed", "INTEGER DEFAULT 0")
    ensure_column("day_slots", "source_mode", "TEXT DEFAULT 'template'")


def ensure_column(table_name, column_name, definition):
    with db_lock:
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [row[1] for row in cursor.fetchall()]
        if column_name not in columns:
            cursor.execute(
                f"ALTER TABLE {table_name} ADD COLUMN {column_name} {definition}"
            )
            conn.commit()


def set_setting(key, value):
    with db_lock:
        cursor.execute(
            """
            INSERT INTO settings(key, value)
            VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
            """,
            (key, str(value)),
        )
        conn.commit()


def get_setting(key, default=None):
    with db_lock:
        cursor.execute("SELECT value FROM settings WHERE key=?", (key,))
        row = cursor.fetchone()
    return row[0] if row else default


def ensure_default_settings():
    if get_setting("master_reminders_enabled") is None:
        set_setting("master_reminders_enabled", "1")
    # One-time cleanup: drop legacy template_session_minutes so stale rows
    # from the old duration-based template flow don't masquerade as a saved
    # template after upgrade. Block-based templates write blocks only.
    with db_lock:
        cursor.execute("DELETE FROM settings WHERE key=?", ("template_session_minutes",))
        conn.commit()
    # In demo mode the calendar/time-slot flow is rendered from in-memory
    # constants, so no template / day_slots seeding is required.


def safe_edit_message_text(text, chat_id, message_id, **kwargs):
    """Try to edit a message; if it's gone, send a fresh one. Never raise."""
    try:
        return bot.edit_message_text(text, chat_id, message_id, **kwargs)
    except Exception:
        try:
            return bot.send_message(chat_id, text, reply_markup=kwargs.get("reply_markup"))
        except Exception:
            return None


def safe_delete_message(chat_id, message_id):
    try:
        bot.delete_message(chat_id, message_id)
    except Exception:
        pass


def get_admin_user_ids():
    ids = set()
    if OWNER_ID:
        ids.add(OWNER_ID)
    with db_lock:
        cursor.execute("SELECT user_id FROM admins")
        for row in cursor.fetchall():
            ids.add(row[0])
    return ids


def is_admin(user_id):
    if not user_id:
        return False
    if OWNER_ID and user_id == OWNER_ID:
        return True
    with db_lock:
        cursor.execute("SELECT 1 FROM admins WHERE user_id=?", (user_id,))
        return cursor.fetchone() is not None


def is_owner(user_id):
    return bool(OWNER_ID) and user_id == OWNER_ID


def add_admin_record(user_id, username, added_by):
    with db_lock:
        cursor.execute(
            """
            INSERT INTO admins(user_id, username, added_by, added_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                added_by=excluded.added_by,
                added_at=excluded.added_at
            """,
            (
                user_id,
                username,
                added_by,
                datetime.datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()


def remove_admin_record(user_id):
    with db_lock:
        cursor.execute("DELETE FROM admins WHERE user_id=?", (user_id,))
        conn.commit()


def list_admin_records():
    with db_lock:
        cursor.execute(
            "SELECT user_id, username FROM admins ORDER BY added_at"
        )
        return cursor.fetchall()


def remember_tg_user(user):
    if not user or not user.id:
        return
    full_name = getattr(user, "full_name", None) or " ".join(
        filter(None, [getattr(user, "first_name", None), getattr(user, "last_name", None)])
    ).strip()
    with db_lock:
        cursor.execute(
            """
            INSERT INTO tg_users(user_id, username, full_name, last_seen_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                full_name=excluded.full_name,
                last_seen_at=excluded.last_seen_at
            """,
            (
                user.id,
                getattr(user, "username", None),
                full_name or None,
                datetime.datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()


def find_tg_user_by_username(username):
    if not username:
        return None
    handle = username.lstrip("@").strip().lower()
    if not handle:
        return None
    with db_lock:
        cursor.execute(
            "SELECT user_id, username, full_name FROM tg_users WHERE LOWER(username)=?",
            (handle,),
        )
        row = cursor.fetchone()
    if row:
        return row
    with db_lock:
        cursor.execute(
            """
            SELECT user_id, client_username, client_name
            FROM bookings
            WHERE LOWER(client_username)=?
            ORDER BY id DESC
            LIMIT 1
            """,
            (handle,),
        )
        return cursor.fetchone()


def get_client_profile(user_id):
    with db_lock:
        cursor.execute(
            """
            SELECT client_name, client_username, allergies, preferences, extra_notes, updated_at
            FROM client_profiles
            WHERE user_id=?
            """,
            (user_id,),
        )
        return cursor.fetchone()


def upsert_client_profile(user_id, client_name, client_username, allergies, preferences, extra_notes):
    with db_lock:
        cursor.execute(
            """
            INSERT INTO client_profiles(
                user_id, client_name, client_username, allergies, preferences, extra_notes, updated_at
            )
            VALUES(?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                client_name=excluded.client_name,
                client_username=excluded.client_username,
                allergies=COALESCE(excluded.allergies, client_profiles.allergies),
                preferences=COALESCE(excluded.preferences, client_profiles.preferences),
                extra_notes=COALESCE(excluded.extra_notes, client_profiles.extra_notes),
                updated_at=excluded.updated_at
            """,
            (
                user_id,
                client_name,
                client_username,
                allergies,
                preferences,
                extra_notes,
                datetime.datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()


def get_profile_questionnaire_markup():
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("Заполнить", callback_data="profile_fill_start"),
        types.InlineKeyboardButton("Пропустить", callback_data="profile_fill_skip"),
    )
    return markup


def get_profile_skip_markup(step):
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("Пропустить", callback_data=f"profile_skip_{step}"))
    return markup


def send_profile_questionnaire_prompt(user_id):
    profile = get_client_profile(user_id)
    if profile and any(profile[2:5]):
        text = (
            "Мастер уже знает ваши предпочтения. Хотите обновить ответы перед визитом?"
        )
    else:
        text = (
            "Чтобы мастер заранее подготовился — пара коротких вопросов:\n"
            "аллергии, предпочтения по процедуре и любые комментарии."
        )
    try:
        bot.send_message(user_id, text, reply_markup=get_profile_questionnaire_markup())
    except Exception:
        pass


def _advance_profile_flow(user_id, chat_id, step, value, source_user=None):
    state = client_states.setdefault(user_id, {})
    if step == "allergies":
        state["allergies"] = value
        state["flow"] = "profile_preferences"
        bot.send_message(
            chat_id,
            "2/3. Особые предпочтения по процедуре или нюансы, которые важно учесть?",
            reply_markup=get_profile_skip_markup("preferences"),
        )
        return
    if step == "preferences":
        state["preferences"] = value
        state["flow"] = "profile_extra"
        bot.send_message(
            chat_id,
            "3/3. Что-то ещё, что мастеру стоит знать?",
            reply_markup=get_profile_skip_markup("extra"),
        )
        return
    if step == "extra":
        state["extra"] = value
        full_name = getattr(source_user, "full_name", None)
        username = getattr(source_user, "username", None)
        upsert_client_profile(
            user_id,
            full_name,
            username,
            state.get("allergies"),
            state.get("preferences"),
            state.get("extra"),
        )
        client_states.pop(user_id, None)
        bot.send_message(chat_id, "Спасибо, записал! Мастер увидит ответы перед визитом.")
        return


def format_client_profile(profile):
    if not profile:
        return "Анкета не заполнена."
    _, _, allergies, preferences, extra, updated_at = profile
    lines = []
    lines.append(f"Аллергии / противопоказания: {allergies or '—'}")
    lines.append(f"Предпочтения: {preferences or '—'}")
    lines.append(f"Прочее: {extra or '—'}")
    if updated_at:
        lines.append(f"Обновлено: {updated_at}")
    return "\n".join(lines)


def flush_step_msgs(user_id, chat_id):
    if user_id not in user_data:
        return
    msgs = user_data[user_id].pop("_step_msgs", [])
    for mid in msgs:
        try:
            bot.delete_message(chat_id, mid)
        except Exception:
            pass


def add_step_msg(user_id, msg_id):
    if user_id not in user_data:
        user_data[user_id] = {}
    user_data[user_id].setdefault("_step_msgs", []).append(msg_id)


admin_action_messages = {}
admin_action_lock = threading.Lock()


def notify_all_admins(text, reply_markup=None, exclude=None, action_key=None):
    exclude_ids = set(exclude or [])
    delivered = []
    for admin_user_id in get_admin_user_ids():
        if admin_user_id in exclude_ids:
            continue
        try:
            sent = bot.send_message(admin_user_id, text, reply_markup=reply_markup)
            delivered.append((admin_user_id, sent.message_id))
        except Exception:
            pass
    if action_key and reply_markup is not None:
        with admin_action_lock:
            admin_action_messages[action_key] = delivered
    return delivered


def lock_admin_action(action_key, acted_by_user_id, resolution_text):
    with admin_action_lock:
        entries = admin_action_messages.pop(action_key, [])
    for chat_id, msg_id in entries:
        if chat_id == acted_by_user_id:
            continue
        try:
            bot.edit_message_reply_markup(chat_id, msg_id, reply_markup=None)
        except Exception:
            pass
        if resolution_text:
            try:
                bot.send_message(chat_id, resolution_text)
            except Exception:
                pass


def parse_time_to_minutes(value):
    try:
        text = value.strip()
        if ":" in text:
            hour, minute = text.split(":")
        else:
            hour, minute = text, "0"
        hour = int(hour)
        minute = int(minute)
        if not (0 <= hour <= 23 and 0 <= minute <= 59):
            return None
        return hour * 60 + minute
    except (TypeError, ValueError, AttributeError):
        return None


def minutes_to_time(value):
    hours = value // 60
    minutes = value % 60
    return f"{hours:02d}:{minutes:02d}"


def service_by_title(title):
    for service_key, service in SERVICE_CATALOG.items():
        if service["title"] == title:
            return service_key, service
    return None, None


def make_png(path, width=640, height=420, color=(255, 255, 255)):
    raw_rows = []
    row = b"\x00" + bytes(color) * width
    for _ in range(height):
        raw_rows.append(row)
    raw_data = b"".join(raw_rows)

    def chunk(chunk_type, data):
        return (
            struct.pack(">I", len(data))
            + chunk_type
            + data
            + struct.pack(">I", zlib.crc32(chunk_type + data) & 0xFFFFFFFF)
        )

    png_data = (
        b"\x89PNG\r\n\x1a\n"
        + chunk(b"IHDR", struct.pack(">IIBBBBB", width, height, 8, 2, 0, 0, 0))
        + chunk(b"IDAT", zlib.compress(raw_data, 9))
        + chunk(b"IEND", b"")
    )
    with open(path, "wb") as file:
        file.write(png_data)


def ensure_portfolio_placeholders():
    for service_key, service in SERVICE_CATALOG.items():
        for subtype_key in service["subtypes"]:
            subtype_dir = os.path.join(PORTFOLIO_DIR, service_key, subtype_key)
            os.makedirs(subtype_dir, exist_ok=True)
            for index in range(1, 6):
                path = os.path.join(subtype_dir, f"{index}.png")
                if not os.path.exists(path):
                    make_png(path)


def get_portfolio_images(service_key, subtype_key):
    subtype_dir = os.path.join(PORTFOLIO_DIR, service_key, subtype_key)
    images = []
    for index in range(1, 6):
        path = os.path.join(subtype_dir, f"{index}.png")
        if os.path.exists(path):
            images.append(path)
    return images


def is_less_than_24_hours(date_value, start_time):
    appointment_at = datetime.datetime.strptime(
        f"{date_value} {start_time}", "%Y-%m-%d %H:%M"
    )
    return appointment_at - datetime.datetime.now() < datetime.timedelta(hours=24)


def normalize_date_text(text):
    try:
        value = datetime.datetime.strptime(text.strip(), "%Y-%m-%d").date()
        return value.isoformat()
    except ValueError:
        return None


def next_days_markup(prefix, days=90, include_back=False, back_cb="admin_menu",
                      mark_booked=False):
    markup = types.InlineKeyboardMarkup(row_width=4)
    today = datetime.date.today()
    buttons = []
    for offset in range(days):
        current = today + datetime.timedelta(days=offset)
        date_value = current.isoformat()
        label = current.strftime("%d.%m")
        if mark_booked and has_bookings_on_date(date_value):
            buttons.append(
                types.InlineKeyboardButton(
                    f"{label}❌",
                    callback_data="noop_busy_date",
                )
            )
            continue
        buttons.append(
            types.InlineKeyboardButton(
                label,
                callback_data=f"{prefix}_{date_value}",
            )
        )
    markup.add(*buttons)
    if include_back:
        markup.add(types.InlineKeyboardButton("Назад", callback_data=back_cb))
    return markup


def get_main_menu(user_id):
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    if DEMO_MODE:
        # Hide the Admin role: the demo is for prospective clients only.
        markup.row("Клиент 👤")
        markup.row("О тарифах 💼", "Очистить чат 🧹")
    else:
        markup.row("Клиент 👤", "Админ 🛠")
    return markup


def get_client_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True)
    markup.row("Выбрать услугу 💅", "Мои записи 📅")
    if DEMO_MODE:
        markup.row("О тарифах 💼")
    markup.row("Очистить чат 🧹", "Назад ⬅️")
    return markup


def list_client_bookings(user_id, limit=20):
    with db_lock:
        cursor.execute(
            """
            SELECT id, date, time, end_time, service, status
            FROM bookings
            WHERE user_id=? AND status != 'cancelled'
            ORDER BY date DESC, time DESC
            LIMIT ?
            """,
            (user_id, limit),
        )
        return cursor.fetchall()


def format_client_booking_line(row):
    _booking_id, date_value, time_value, end_value, service_key, status = row
    end_part = f"–{end_value}" if end_value else ""
    return f"{date_value} {time_value}{end_part} · {service_key} · {status_label(status)}"


def get_admin_menu(user_id=None):
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton("График 📅", callback_data="admin_schedule"),
        types.InlineKeyboardButton("Календарь 📒", callback_data="admin_bookings_calendar"),
    )
    markup.row(
        types.InlineKeyboardButton("Клиенты 👥", callback_data="admin_clients"),
        types.InlineKeyboardButton("Статистика 📊", callback_data="admin_stats"),
    )
    markup.row(
        types.InlineKeyboardButton("Настройки ⚙️", callback_data="admin_settings"),
        types.InlineKeyboardButton("Закрыть дату 🚫", callback_data="admin_close_date"),
    )
    markup.add(types.InlineKeyboardButton("Очистить чат 🧹", callback_data="admin_clear_chat"))
    if user_id is None or is_owner(user_id):
        markup.add(types.InlineKeyboardButton("Админы 🛡", callback_data="admin_admins"))
    return markup


def list_known_clients(query=None, limit=50):
    sql = (
        """
        SELECT user_id,
               COALESCE(MAX(client_username), NULL) AS username,
               COALESCE(MAX(client_name), NULL) AS full_name,
               COUNT(*) AS total
        FROM bookings
        WHERE user_id IS NOT NULL
        """
    )
    params = []
    if query:
        sql += " AND (LOWER(COALESCE(client_username, '')) LIKE ? OR LOWER(COALESCE(client_name, '')) LIKE ?)"
        like = f"%{query.lower().lstrip('@')}%"
        params.extend([like, like])
    sql += " GROUP BY user_id ORDER BY MAX(date) DESC, MAX(time) DESC LIMIT ?"
    params.append(limit)
    with db_lock:
        cursor.execute(sql, params)
        return cursor.fetchall()


def get_clients_list_markup(query=None):
    markup = types.InlineKeyboardMarkup()
    rows = list_known_clients(query=query)
    if not rows:
        markup.add(types.InlineKeyboardButton("Ничего не найдено", callback_data="noop"))
    else:
        for client_user_id, username, full_name, total in rows:
            parts = []
            if full_name:
                parts.append(full_name)
            if username:
                parts.append(f"@{username}")
            if not parts:
                parts.append(f"ID {client_user_id}")
            label = " · ".join(parts)
            markup.add(
                types.InlineKeyboardButton(
                    f"{label} · {total}",
                    callback_data=f"client_card_{client_user_id}",
                )
            )
    markup.row(
        types.InlineKeyboardButton("Поиск", callback_data="client_search"),
        types.InlineKeyboardButton("Назад", callback_data="admin_menu"),
    )
    return markup


def get_client_card_markup(target_user_id):
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton("Заметка 📝", callback_data=f"client_note_{target_user_id}"),
        types.InlineKeyboardButton("Все записи 📜", callback_data=f"client_history_{target_user_id}"),
    )
    markup.add(types.InlineKeyboardButton("Удалить клиента 🗑", callback_data=f"client_delete_{target_user_id}"))
    markup.add(types.InlineKeyboardButton("Назад к списку ⬅️", callback_data="admin_clients"))
    return markup


def render_client_card(chat_id, target_user_id):
    profile = get_client_profile(target_user_id)
    note, _ = get_client_note(target_user_id)
    with db_lock:
        cursor.execute(
            """
            SELECT client_username, client_name, COUNT(*),
                   MAX(date || ' ' || COALESCE(time, ''))
            FROM bookings
            WHERE user_id=?
            """,
            (target_user_id,),
        )
        meta = cursor.fetchone()
    username = (meta[0] if meta else None) or (profile[1] if profile else None)
    full_name = (meta[1] if meta else None) or (profile[0] if profile else None) or "не указано"
    total = meta[2] if meta else 0
    last_visit = meta[3] if meta and meta[3] else "—"
    parts = [
        f"Клиент: {full_name}",
        f"Username: @{username}" if username else "Username: не указан",
        f"Telegram ID: {target_user_id}",
        f"Всего записей: {total}",
        f"Последняя запись: {last_visit}",
        "",
        "Анкета:",
        format_client_profile(profile),
        "",
        f"Заметка: {note or '—'}",
    ]
    bot.send_message(chat_id, "\n".join(parts), reply_markup=get_client_card_markup(target_user_id))


def render_client_history(chat_id, target_user_id, limit=10):
    with db_lock:
        cursor.execute(
            """
            SELECT id, service, date, time, end_time, status
            FROM bookings
            WHERE user_id=?
            ORDER BY date DESC, time DESC
            LIMIT ?
            """,
            (target_user_id, limit),
        )
        rows = cursor.fetchall()
    if not rows:
        bot.send_message(chat_id, "Записей нет")
        return
    lines = [f"Последние {len(rows)} записей:"]
    for booking_id, service, date_value, start, end, status in rows:
        lines.append(f"#{booking_id} {date_value} {start}–{end or start} | {service} | {status_label(status)}")
    bot.send_message(chat_id, "\n".join(lines))


def get_admins_management_markup():
    markup = types.InlineKeyboardMarkup()
    for admin_user_id, username in list_admin_records():
        label = f"@{username}" if username else f"ID {admin_user_id}"
        markup.add(
            types.InlineKeyboardButton(
                f"{label} ✕",
                callback_data=f"admin_remove_{admin_user_id}",
            )
        )
    markup.add(types.InlineKeyboardButton("Добавить админа", callback_data="admin_add"))
    markup.add(types.InlineKeyboardButton("Назад", callback_data="admin_menu"))
    return markup


def get_admin_settings_markup():
    enabled = get_setting("master_reminders_enabled", "1") == "1"
    markup = types.InlineKeyboardMarkup()
    label = "Выключить напоминания мастеру" if enabled else "Включить напоминания мастеру"
    markup.add(types.InlineKeyboardButton(label, callback_data="toggle_master_reminders"))
    markup.add(types.InlineKeyboardButton("Назад", callback_data="admin_menu"))
    return markup


def get_schedule_menu():
    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.row(
        types.InlineKeyboardButton("Настроить шаблон 🧩", callback_data="schedule_template"),
        types.InlineKeyboardButton("Показать шаблон 👁", callback_data="schedule_show_template"),
    )
    markup.row(
        types.InlineKeyboardButton("Создать день ➕", callback_data="schedule_create_day"),
        types.InlineKeyboardButton("Редактировать ✏️", callback_data="schedule_edit_day"),
    )
    markup.add(types.InlineKeyboardButton("Назад ⬅️", callback_data="admin_menu"))
    return markup


def get_template_config():
    start = get_setting("template_start")
    end = get_setting("template_end")
    blocks_raw = get_setting("template_blocks")
    blocks = None
    if blocks_raw:
        try:
            parsed = json.loads(blocks_raw)
            if isinstance(parsed, list) and parsed:
                blocks = parsed
        except (TypeError, ValueError):
            blocks = None
    if not (start and end and blocks):
        return None
    return {
        "start": start,
        "end": end,
        "session_minutes": None,
        "blocks": blocks,
    }


def build_template_slots(start_time, end_time, session_minutes=None, blocks=None):
    if blocks is not None:
        return build_manual_timeline(start_time, end_time, blocks)
    start_minutes = parse_time_to_minutes(start_time)
    end_minutes = parse_time_to_minutes(end_time)
    if start_minutes is None or end_minutes is None:
        return None
    if not session_minutes or end_minutes <= start_minutes or session_minutes <= 0:
        return None

    slots = []
    current = start_minutes
    while current + session_minutes <= end_minutes:
        slots.append(
            {
                "type": "work",
                "start": minutes_to_time(current),
                "end": minutes_to_time(current + session_minutes),
                "duration": session_minutes,
            }
        )
        current += session_minutes

    return {
        "slots": slots,
        "shift_start": start_time,
        "shift_end": end_time,
        "actual_end": minutes_to_time(current),
        "unused_minutes": end_minutes - current,
        "overflow_minutes": 0,
    }


def schedule_from_template(template):
    if not template:
        return None
    return build_template_slots(
        template["start"],
        template["end"],
        session_minutes=template.get("session_minutes"),
        blocks=template.get("blocks"),
    )


def build_manual_timeline(shift_start, shift_end, blocks):
    start_minutes = parse_time_to_minutes(shift_start)
    end_minutes = parse_time_to_minutes(shift_end)
    if start_minutes is None or end_minutes is None or end_minutes <= start_minutes:
        return None

    current = start_minutes
    timeline = []
    for block in blocks:
        block_end = current + block["duration"]
        timeline.append(
            {
                "type": block["type"],
                "start": minutes_to_time(current),
                "end": minutes_to_time(block_end),
                "duration": block["duration"],
            }
        )
        current = block_end

    return {
        "slots": timeline,
        "shift_start": shift_start,
        "shift_end": shift_end,
        "actual_end": minutes_to_time(current),
        "unused_minutes": max(0, end_minutes - current),
        "overflow_minutes": max(0, current - end_minutes),
    }


def minutes_label(value):
    hours = value // 60
    minutes = value % 60
    parts = []
    if hours:
        parts.append(f"{hours} ч")
    if minutes:
        parts.append(f"{minutes} мин")
    return " ".join(parts) if parts else "0 мин"


def format_schedule_preview(schedule, title):
    lines = [title]
    lines.append(
        f"Смена: {schedule['shift_start']}–{schedule['shift_end']}, факт: до {schedule['actual_end']}"
    )
    if schedule["slots"]:
        for index, slot in enumerate(schedule["slots"], start=1):
            label = "Сеанс" if slot["type"] == "work" else "Перерыв"
            lines.append(
                f"{index}. {label}: {slot['start']}–{slot['end']} ({minutes_label(slot['duration'])})"
            )
    else:
        lines.append("Блоков пока нет")

    if schedule["unused_minutes"]:
        lines.append(
            f"Остаток рабочего времени: {minutes_label(schedule['unused_minutes'])}"
        )
    if schedule["overflow_minutes"]:
        lines.append(
            f"Выход за рамки смены: {minutes_label(schedule['overflow_minutes'])}"
        )
    if not schedule["unused_minutes"] and not schedule["overflow_minutes"]:
        lines.append("График полностью совпадает со сменой")
    return "\n".join(lines)


def find_overlapping_slots(date_value, start_time, end_time):
    """Return (has_booking_conflict, [(slot_id, start, end, is_break, is_closed)...]).

    `has_booking_conflict` is True if the proposed window touches any booked
    slot or pending/confirmed booking on the day. The list contains all
    non-booked existing slots that would be replaced.
    """
    start_min = parse_time_to_minutes(start_time)
    end_min = parse_time_to_minutes(end_time)
    if start_min is None or end_min is None:
        return True, []
    has_booking = False
    overlaps = []
    with db_lock:
        cursor.execute(
            "SELECT id, start_time, end_time, is_break, is_booked, is_closed FROM day_slots WHERE date=?",
            (date_value,),
        )
        for sid, s_start, s_end, s_break, s_booked, s_closed in cursor.fetchall():
            sm = parse_time_to_minutes(s_start)
            em = parse_time_to_minutes(s_end) if s_end else None
            if sm is None or em is None:
                continue
            if start_min < em and end_min > sm:
                if s_booked:
                    has_booking = True
                else:
                    overlaps.append((sid, s_start, s_end, s_break, s_closed))
        cursor.execute(
            "SELECT time, end_time FROM bookings WHERE date=? AND status IN ('pending','confirmed')",
            (date_value,),
        )
        for b_time, b_end in cursor.fetchall():
            bm = parse_time_to_minutes(b_time)
            em = parse_time_to_minutes(b_end) if b_end else None
            if bm is None or em is None:
                continue
            if start_min < em and end_min > bm:
                has_booking = True
    return has_booking, overlaps


def describe_slot(start, end, is_break, is_closed):
    if is_break:
        kind = "перерыв"
    elif is_closed:
        kind = "закрытое окно"
    else:
        kind = "окно"
    return f"{kind} {start}–{end}"


def insert_slot(date_value, start_time, end_time, kind):
    """Insert a single slot of kind 'window' or 'break' into day_slots."""
    if kind == "break":
        is_break, is_closed, source = 1, 1, "service"
    else:
        is_break, is_closed, source = 0, 0, "manual"
    with db_lock:
        cursor.execute(
            """
            INSERT INTO day_slots(date, start_time, end_time, is_break, is_booked, is_closed, source_mode)
            VALUES (?, ?, ?, ?, 0, ?, ?)
            """,
            (date_value, start_time, end_time, is_break, is_closed, source),
        )
        conn.commit()


def delete_slots_by_ids(ids):
    if not ids:
        return
    placeholders = ",".join("?" for _ in ids)
    with db_lock:
        cursor.execute(f"DELETE FROM day_slots WHERE id IN ({placeholders})", tuple(ids))
        conn.commit()


def slot_conflicts(date_value, start_time, end_time):
    """Return reason string if a new [start, end) window would overlap an existing
    slot or booking on the given date; otherwise return None."""
    start_min = parse_time_to_minutes(start_time)
    end_min = parse_time_to_minutes(end_time)
    if start_min is None or end_min is None:
        return "Некорректное время"
    with db_lock:
        cursor.execute(
            """
            SELECT start_time, end_time, is_break, is_booked
            FROM day_slots
            WHERE date=?
            """,
            (date_value,),
        )
        slots = cursor.fetchall()
        cursor.execute(
            """
            SELECT time, end_time
            FROM bookings
            WHERE date=? AND status IN ('pending','confirmed')
            """,
            (date_value,),
        )
        bookings = cursor.fetchall()
    for s_start, s_end, s_break, s_booked in slots:
        s_start_min = parse_time_to_minutes(s_start)
        s_end_min = parse_time_to_minutes(s_end) if s_end else None
        if s_end_min is None:
            continue
        if start_min < s_end_min and end_min > s_start_min:
            if s_booked:
                return f"Пересекается с записью {s_start}–{s_end}"
            if s_break:
                return f"Пересекается с перерывом {s_start}–{s_end}"
            return f"Пересекается с окном {s_start}–{s_end}"
    for b_time, b_end in bookings:
        b_start_min = parse_time_to_minutes(b_time)
        b_end_min = parse_time_to_minutes(b_end) if b_end else None
        if b_start_min is None or b_end_min is None:
            continue
        if start_min < b_end_min and end_min > b_start_min:
            return f"Пересекается с записью {b_time}–{b_end}"
    return None


def has_bookings_on_date(date_value):
    with db_lock:
        cursor.execute("SELECT COUNT(*) FROM bookings WHERE date=?", (date_value,))
        count = cursor.fetchone()[0]
    return count > 0


def delete_day_slots(date_value):
    with db_lock:
        cursor.execute("DELETE FROM day_slots WHERE date=?", (date_value,))
        conn.commit()


def save_schedule_to_day(date_value, schedule, source_mode):
    if has_bookings_on_date(date_value):
        return False, "На этой дате уже есть записи. Используй режим редактирования дня."

    delete_day_slots(date_value)
    with db_lock:
        for slot in schedule["slots"]:
            cursor.execute(
                """
                INSERT INTO day_slots(date, start_time, end_time, is_break, is_booked, is_closed, source_mode)
                VALUES (?, ?, ?, ?, 0, 0, ?)
                """,
                (
                    date_value,
                    slot["start"],
                    slot["end"],
                    1 if slot["type"] == "break" else 0,
                    source_mode,
                ),
            )
        conn.commit()
    return True, "График сохранен"


def ensure_template_schedule_for_date(date_value):
    with db_lock:
        cursor.execute("SELECT COUNT(*) FROM day_slots WHERE date=?", (date_value,))
        slots_count = cursor.fetchone()[0]
    if slots_count:
        return

    template = get_template_config()
    if not template:
        return

    schedule = schedule_from_template(template)
    if not schedule:
        return

    with db_lock:
        for slot in schedule["slots"]:
            cursor.execute(
                """
                INSERT INTO day_slots(date, start_time, end_time, is_break, is_booked, is_closed, source_mode)
                VALUES (?, ?, ?, 0, 0, 0, 'template')
                """,
                (date_value, slot["start"], slot["end"]),
            )
        conn.commit()


def get_day_slots(date_value):
    ensure_template_schedule_for_date(date_value)
    with db_lock:
        cursor.execute(
            """
            SELECT id, start_time, end_time, is_break, is_booked, is_closed, source_mode
            FROM day_slots
            WHERE date=?
            ORDER BY start_time
            """,
            (date_value,),
        )
        rows = cursor.fetchall()
    return rows


def get_available_slot_count(date_value):
    ensure_template_schedule_for_date(date_value)
    with db_lock:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM day_slots
            WHERE date=? AND is_break=0 AND is_booked=0 AND is_closed=0
            """,
            (date_value,),
        )
        count = cursor.fetchone()[0]
    return count


CALENDAR_WINDOW_MONTHS = 12
RU_MONTHS = [
    "январь",
    "февраль",
    "март",
    "апрель",
    "май",
    "июнь",
    "июль",
    "август",
    "сентябрь",
    "октябрь",
    "ноябрь",
    "декабрь",
]
RU_WEEKDAYS_SHORT = ["Пн", "Вт", "Ср", "Чт", "Пт", "Сб", "Вс"]


def calendar_window_bounds(today=None):
    today = today or datetime.date.today()
    start = today.replace(day=1)
    end_year = start.year + (start.month - 1 + CALENDAR_WINDOW_MONTHS - 1) // 12
    end_month = (start.month - 1 + CALENDAR_WINDOW_MONTHS - 1) % 12 + 1
    end = datetime.date(end_year, end_month, 1)
    return start, end


def is_month_in_window(year, month, today=None):
    start, end = calendar_window_bounds(today)
    target = datetime.date(year, month, 1)
    return start <= target <= end


def shift_month(year, month, delta):
    total = (year * 12 + (month - 1)) + delta
    return total // 12, total % 12 + 1


def get_day_free_slots(date_value):
    with db_lock:
        cursor.execute(
            """
            SELECT start_time, end_time
            FROM day_slots
            WHERE date=? AND is_break=0 AND is_booked=0 AND is_closed=0
            ORDER BY start_time
            """,
            (date_value,),
        )
        return cursor.fetchall()


def date_has_enough_time(date_value, service_duration, today=None):
    if not service_duration:
        return True
    today = today or datetime.datetime.now()
    for start, end in get_day_free_slots(date_value):
        if parse_time_to_minutes(end) - parse_time_to_minutes(start) < service_duration:
            continue
        appointment_at = datetime.datetime.strptime(f"{date_value} {start}", "%Y-%m-%d %H:%M")
        if appointment_at - today < datetime.timedelta(hours=24):
            continue
        return True
    return False


# Fixed time grid used by the demo flow: every day looks the same, every
# slot is "available". Some slots are randomly hidden per (date, slot)
# pair to make the demo feel realistic, but the choice is deterministic
# so the same date always shows the same picture between renders.
DEMO_TIME_SLOTS = (
    "10:00", "11:00", "12:00", "13:00",
    "15:00", "16:00", "17:00", "18:00", "19:00",
)


def _demo_slot_is_busy(date_value, slot):
    # Hash-based pseudo-randomness: ~25% of slots show as busy, but the
    # decision is stable so reopening the same date doesn't shuffle them.
    return (hash((date_value, slot)) & 0xFF) < 64


def _demo_available_count(date_value):
    return sum(1 for s in DEMO_TIME_SLOTS if not _demo_slot_is_busy(date_value, s))


def get_available_dates_map(year, month, service_duration=None):
    today = datetime.date.today()
    if DEMO_MODE:
        # Every future day in the rendered month is "open" with the demo grid.
        result = {}
        for day in range(1, 32):
            try:
                current = datetime.date(year, month, day)
            except ValueError:
                continue
            if current <= today:
                continue
            count = _demo_available_count(current.isoformat())
            if count:
                result[day] = count
        return result
    result = {}
    now = datetime.datetime.now()
    for day in range(1, 32):
        try:
            current = datetime.date(year, month, day)
        except ValueError:
            continue
        if current < today:
            continue
        free_slots = get_day_free_slots(current.isoformat())
        if not free_slots:
            continue
        usable = 0
        for start, end in free_slots:
            if service_duration and parse_time_to_minutes(end) - parse_time_to_minutes(start) < service_duration:
                continue
            appointment_at = datetime.datetime.strptime(
                f"{current.isoformat()} {start}", "%Y-%m-%d %H:%M"
            )
            if appointment_at - now < datetime.timedelta(hours=24):
                continue
            usable += 1
        if usable:
            result[day] = usable
    return result


def get_demo_time_slots_markup(date_value):
    markup = types.InlineKeyboardMarkup(row_width=3)
    buttons = []
    for slot in DEMO_TIME_SLOTS:
        if _demo_slot_is_busy(date_value, slot):
            buttons.append(types.InlineKeyboardButton(f"{slot} — занято", callback_data="busy"))
        else:
            buttons.append(
                types.InlineKeyboardButton(
                    slot, callback_data=f"demotime_{date_value}_{slot}"
                )
            )
    markup.add(*buttons)
    markup.add(types.InlineKeyboardButton("Назад", callback_data="back_calendar"))
    return markup


def get_calendar(year, month, service_duration=None):
    markup = types.InlineKeyboardMarkup(row_width=7)
    today = datetime.date.today()
    start_window, end_window = calendar_window_bounds(today)

    header_label = f"{RU_MONTHS[month - 1].capitalize()} {year}"
    can_prev = datetime.date(year, month, 1) > start_window
    can_next = datetime.date(year, month, 1) < end_window
    prev_year, prev_month = shift_month(year, month, -1)
    next_year, next_month = shift_month(year, month, 1)
    markup.row(
        types.InlineKeyboardButton(
            "‹" if can_prev else " ",
            callback_data=f"prev_{prev_year}_{prev_month}" if can_prev else "noop",
        ),
        types.InlineKeyboardButton(header_label, callback_data="noop"),
        types.InlineKeyboardButton(
            "›" if can_next else " ",
            callback_data=f"next_{next_year}_{next_month}" if can_next else "noop",
        ),
    )
    markup.row(*[types.InlineKeyboardButton(d, callback_data="noop") for d in RU_WEEKDAYS_SHORT])

    available_map = get_available_dates_map(year, month, service_duration=service_duration)
    first_day = datetime.date(year, month, 1)
    leading_blanks = first_day.weekday()
    last_day = (
        (datetime.date(year + (1 if month == 12 else 0), 1 if month == 12 else month + 1, 1)
         - datetime.timedelta(days=1)).day
    )

    cells = [types.InlineKeyboardButton(" ", callback_data="noop") for _ in range(leading_blanks)]
    for day in range(1, last_day + 1):
        current = datetime.date(year, month, day)
        if current < today:
            cells.append(types.InlineKeyboardButton(f"{day}❌", callback_data="noop"))
            continue
        count = available_map.get(day, 0)
        if count:
            cells.append(
                types.InlineKeyboardButton(
                    f"{day} ({count})",
                    callback_data=f"date_{current.isoformat()}",
                )
            )
        else:
            cells.append(types.InlineKeyboardButton(f"{day}❌", callback_data="no_slots_day"))

    while len(cells) % 7:
        cells.append(types.InlineKeyboardButton(" ", callback_data="noop"))

    for i in range(0, len(cells), 7):
        markup.row(*cells[i:i + 7])

    markup.add(types.InlineKeyboardButton("Назад", callback_data="back_services"))
    return markup


def cleanup_old_day_slots(today=None):
    today = today or datetime.date.today().isoformat()
    with db_lock:
        cursor.execute(
            """
            DELETE FROM day_slots
            WHERE date < ?
              AND is_booked = 0
            """,
            (today,),
        )
        conn.commit()


def get_time_slots_markup(date_value, user_id=None):
    markup = types.InlineKeyboardMarkup()
    rows = get_day_slots(date_value)
    service_title = user_data.get(user_id, {}).get("service")
    _service_key, service = service_by_title(service_title)
    if not rows:
        markup.add(types.InlineKeyboardButton("Назад", callback_data="back_calendar"))
        return markup

    available_count = 0
    for slot_id, start, end, is_break, is_booked, is_closed, _ in rows:
        if is_break or is_booked or is_closed:
            continue
        if service and parse_time_to_minutes(end) - parse_time_to_minutes(start) < service["duration"]:
            continue
        if is_less_than_24_hours(date_value, start):
            continue
        markup.add(
            types.InlineKeyboardButton(
                f"{start}–{end}",
                callback_data=f"slot_{slot_id}",
            )
        )
        available_count += 1

    if not available_count:
        markup.add(types.InlineKeyboardButton("На эту дату нет подходящих окон", callback_data="noop"))

    markup.add(types.InlineKeyboardButton("Назад", callback_data="back_calendar"))
    return markup


def format_day_view(date_value):
    rows = get_day_slots(date_value)
    lines = [f"График на {date_value}"]
    if not rows:
        lines.append("День еще не создан")
        return "\n".join(lines)

    for index, row in enumerate(rows, start=1):
        _slot_id, start, end, is_break, is_booked, is_closed, _source_mode = row
        if is_break:
            state = "перерыв"
        elif is_booked:
            state = "занято"
        elif is_closed:
            state = "закрыто"
        else:
            state = "свободно"
        lines.append(f"{index}. {start}–{end} | {state}")
    return "\n".join(lines)


def get_day_edit_markup(date_value):
    rows = get_day_slots(date_value)
    markup = types.InlineKeyboardMarkup()
    for slot_id, start, end, is_break, is_booked, is_closed, _ in rows:
        if is_break:
            continue
        state = "занято" if is_booked else "закрыто" if is_closed else "открыто"
        toggle_label = "Открыть" if is_closed else "Закрыть"
        if is_booked:
            markup.add(
                types.InlineKeyboardButton(
                    f"{start}–{end} ({state})", callback_data="locked_slot"
                )
            )
        else:
            markup.row(
                types.InlineKeyboardButton(
                    f"{start}–{end} ({state})", callback_data="noop"
                ),
                types.InlineKeyboardButton(
                    toggle_label, callback_data=f"edit_toggle_{slot_id}"
                ),
                types.InlineKeyboardButton(
                    "Удалить", callback_data=f"edit_delete_{slot_id}"
                ),
            )

    markup.add(types.InlineKeyboardButton("Добавить окно", callback_data="edit_add_slot"))
    markup.add(types.InlineKeyboardButton("Добавить служебное окно", callback_data="edit_add_break"))
    markup.add(types.InlineKeyboardButton("Назад", callback_data="admin_schedule"))
    return markup


def get_admin_booking_decision_markup(booking_id):
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton(
            "Подтвердить", callback_data=f"admin_confirm_{booking_id}"
        ),
        types.InlineKeyboardButton(
            "Отклонить", callback_data=f"admin_decline_menu_{booking_id}"
        ),
    )
    return markup


def get_decline_reason_markup(booking_id):
    markup = types.InlineKeyboardMarkup()
    reasons = {
        "busy": "Это время уже занято",
        "cant": "Мастер не сможет принять",
        "other": "Предложу другое время",
    }
    for reason_key, reason_text in reasons.items():
        markup.add(
            types.InlineKeyboardButton(
                reason_text, callback_data=f"admin_decline_{booking_id}_{reason_key}"
            )
        )
    markup.add(types.InlineKeyboardButton("Назад", callback_data="admin_menu"))
    return markup


def get_admin_reschedule_decision_markup(request_id):
    markup = types.InlineKeyboardMarkup()
    markup.row(
        types.InlineKeyboardButton(
            "Подтвердить перенос", callback_data=f"reschedule_confirm_{request_id}"
        ),
        types.InlineKeyboardButton(
            "Отклонить перенос", callback_data=f"reschedule_decline_{request_id}"
        ),
    )
    return markup


def get_user_booking_markup(booking_id, status):
    markup = types.InlineKeyboardMarkup()
    if status == "confirmed":
        markup.add(
            types.InlineKeyboardButton(
                "Перенести запись", callback_data=f"reschedule_start_{booking_id}"
            )
        )
        markup.add(types.InlineKeyboardButton("Отменить", callback_data=f"cancel_{booking_id}"))
    elif status == "pending":
        markup.add(types.InlineKeyboardButton("Отменить заявку", callback_data=f"cancel_{booking_id}"))
    return markup


def get_admin_booking_actions_markup(booking_id, user_id):
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton(
            "Предложить перенос", callback_data=f"master_reschedule_start_{booking_id}"
        )
    )
    markup.add(
        types.InlineKeyboardButton("Заметка 📝", callback_data=f"client_note_{user_id}"),
    )
    return markup


def get_reschedule_dates_markup(booking_id):
    return next_days_markup(
        f"reschedule_date_{booking_id}", include_back=True, back_cb="client_close"
    )


def get_reschedule_slots_markup(booking_id, date_value):
    markup = types.InlineKeyboardMarkup()
    rows = get_day_slots(date_value)
    for slot_id, start, end, is_break, is_booked, is_closed, _ in rows:
        if is_break or is_booked or is_closed:
            continue
        markup.add(
            types.InlineKeyboardButton(
                f"{start}–{end}", callback_data=f"reschedule_slot_{booking_id}_{slot_id}"
            )
        )
    markup.add(
        types.InlineKeyboardButton(
            "Назад к датам", callback_data=f"reschedule_start_{booking_id}"
        )
    )
    return markup


def get_master_reschedule_dates_markup(booking_id):
    return next_days_markup(
        f"master_reschedule_date_{booking_id}",
        include_back=True,
        back_cb="admin_bookings_calendar",
    )


def get_master_reschedule_slots_markup(booking_id, date_value):
    markup = types.InlineKeyboardMarkup()
    rows = get_day_slots(date_value)
    for slot_id, start, end, is_break, is_booked, is_closed, _ in rows:
        if is_break or is_booked or is_closed or is_less_than_24_hours(date_value, start):
            continue
        markup.add(
            types.InlineKeyboardButton(
                f"{start}–{end}",
                callback_data=f"master_reschedule_slot_{booking_id}_{slot_id}",
            )
        )
    markup.add(
        types.InlineKeyboardButton(
            "Назад к датам", callback_data=f"master_reschedule_start_{booking_id}"
        )
    )
    return markup


def get_service_info_markup(service_key):
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton(
            "Портфолио", callback_data=f"portfolio_service_{service_key}"
        )
    )
    markup.add(
        types.InlineKeyboardButton(
            "Выбрать эту услугу", callback_data=f"service_{service_key}"
        )
    )
    markup.add(types.InlineKeyboardButton("Назад к услугам", callback_data="back_services"))
    return markup


def get_portfolio_subtypes_markup(service_key):
    markup = types.InlineKeyboardMarkup()
    for subtype_key, subtype in SERVICE_CATALOG[service_key]["subtypes"].items():
        markup.add(
            types.InlineKeyboardButton(
                subtype["title"], callback_data=f"portfolio_subtype_{service_key}_{subtype_key}"
            )
        )
    markup.add(
        types.InlineKeyboardButton(
            "Выбрать услугу", callback_data=f"service_{service_key}"
        )
    )
    markup.add(types.InlineKeyboardButton("Назад", callback_data=f"service_info_{service_key}"))
    return markup


def get_admin_bookings_calendar(year, month):
    markup = types.InlineKeyboardMarkup(row_width=7)

    header_label = f"{RU_MONTHS[month - 1].capitalize()} {year}"
    prev_year, prev_month = shift_month(year, month, -1)
    next_year, next_month = shift_month(year, month, 1)
    markup.row(
        types.InlineKeyboardButton("‹", callback_data=f"admin_bookings_prev_{prev_year}_{prev_month}"),
        types.InlineKeyboardButton(header_label, callback_data="noop"),
        types.InlineKeyboardButton("›", callback_data=f"admin_bookings_next_{next_year}_{next_month}"),
    )
    markup.row(*[types.InlineKeyboardButton(d, callback_data="noop") for d in RU_WEEKDAYS_SHORT])

    first_day = datetime.date(year, month, 1)
    leading_blanks = first_day.weekday()
    last_day = (
        (datetime.date(year + (1 if month == 12 else 0), 1 if month == 12 else month + 1, 1)
         - datetime.timedelta(days=1)).day
    )
    cells = [types.InlineKeyboardButton(" ", callback_data="noop") for _ in range(leading_blanks)]
    for day in range(1, last_day + 1):
        current = datetime.date(year, month, day)
        current_iso = current.isoformat()
        with db_lock:
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM bookings
                WHERE date=? AND status != 'cancelled' AND status != 'declined'
                """,
                (current_iso,),
            )
            count = cursor.fetchone()[0]
        label = f"{day} ({count})" if count else str(day)
        callback = f"admin_bookings_date_{current_iso}" if count else "admin_no_bookings_day"
        cells.append(types.InlineKeyboardButton(label, callback_data=callback))

    while len(cells) % 7:
        cells.append(types.InlineKeyboardButton(" ", callback_data="noop"))
    for i in range(0, len(cells), 7):
        markup.row(*cells[i:i + 7])

    markup.add(types.InlineKeyboardButton("Назад", callback_data="admin_menu"))
    return markup


def get_admin_day_bookings_markup(date_value):
    markup = types.InlineKeyboardMarkup()
    with db_lock:
        cursor.execute(
            """
            SELECT id, time, end_time, service, status
            FROM bookings
            WHERE date=? AND status != 'cancelled' AND status != 'declined'
            ORDER BY time
            """,
            (date_value,),
        )
        rows = cursor.fetchall()

    for booking_id, start, end, service, status in rows:
        markup.add(
            types.InlineKeyboardButton(
                f"{start}–{end} | {service} | {status_label(status)}",
                callback_data=f"admin_booking_view_{booking_id}",
            )
        )
    markup.add(types.InlineKeyboardButton("Назад к календарю", callback_data="admin_bookings_calendar"))
    return markup


def format_booking_details(booking_id):
    with db_lock:
        cursor.execute(
            """
            SELECT id, user_id, service, date, time, end_time, contact, status,
                   client_name, client_username, created_at
            FROM bookings
            WHERE id=?
            """,
            (booking_id,),
        )
        booking = cursor.fetchone()
    if not booking:
        return "Запись не найдена"

    username = f"@{booking[9]}" if booking[9] else "не указан"
    client_name = booking[8] or "не указано"
    return (
        f"Запись #{booking[0]}\n"
        f"Статус: {status_label(booking[7])}\n"
        f"Услуга: {booking[2]}\n"
        f"Дата и время: {booking[3]} {booking[4]}–{booking[5]}\n"
        f"Имя клиента: {client_name}\n"
        f"Username: {username}\n"
        f"Telegram ID: {booking[1]}\n"
        f"Контакт: {booking[6]}\n"
        f"Создана: {booking[10] or 'не указано'}"
    )


def get_client_note(user_id):
    with db_lock:
        cursor.execute(
            "SELECT note, is_blocked FROM client_notes WHERE user_id=?",
            (user_id,),
        )
        row = cursor.fetchone()
    if not row:
        return "", 0
    return row[0] or "", row[1]


def set_client_note(user_id, client_name, note):
    with db_lock:
        cursor.execute(
            """
            INSERT INTO client_notes(user_id, client_name, note, updated_at)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                client_name=excluded.client_name,
                note=excluded.note,
                updated_at=excluded.updated_at
            """,
            (
                user_id,
                client_name,
                note,
                datetime.datetime.now().isoformat(timespec="seconds"),
            ),
        )
        conn.commit()


def status_label(status):
    labels = {
        "pending": "на рассмотрении",
        "confirmed": "подтверждена",
        "declined": "отклонена",
        "cancelled": "отменена",
    }
    return labels.get(status, status)


def notify_admin_new_booking(
    booking_id, user_id, client_name, client_username, service, date_value, start, end, contact
):
    username = f"@{client_username}" if client_username else "не указан"
    notify_all_admins(
        (
            "Новая заявка на запись\n"
            f"#{booking_id} | клиент: {client_name} | {username} | ID {user_id}\n"
            f"{service} | {date_value} | {start}–{end}\n"
            f"Контакт: {contact}"
        ),
        reply_markup=get_admin_booking_decision_markup(booking_id),
        action_key=f"booking:{booking_id}",
    )


def notify_admin_reschedule_request(request_id, booking_id, user_id, service, old_info, new_info):
    notify_all_admins(
        (
            "Заявка на перенос записи\n"
            f"Запись #{booking_id}, заявка #{request_id}, клиент: {user_id}\n"
            f"{service}\n"
            f"Было: {old_info}\n"
            f"Стало: {new_info}"
        ),
        reply_markup=get_admin_reschedule_decision_markup(request_id),
        action_key=f"reschedule:{request_id}",
    )


def open_admin_menu(chat_id, user_id=None):
    bot.send_message(chat_id, "Админка", reply_markup=get_admin_menu(user_id or chat_id))


def open_schedule_menu(chat_id):
    bot.send_message(chat_id, "Управление графиком", reply_markup=get_schedule_menu())


def show_template(chat_id):
    template = get_template_config()
    print(f"[show_template] template={template}", flush=True)
    if not template:
        bot.send_message(chat_id, "Шаблон еще не настроен", reply_markup=get_schedule_menu())
        return

    schedule = schedule_from_template(template)
    bot.send_message(
        chat_id,
        format_schedule_preview(schedule, "Текущий шаблон"),
        reply_markup=get_schedule_menu(),
    )


def clear_admin_state(user_id):
    if user_id in admin_states:
        del admin_states[user_id]


def get_template_builder_markup():
    markup = types.InlineKeyboardMarkup(row_width=2)
    for duration in WORK_DURATIONS:
        markup.add(
            types.InlineKeyboardButton(
                f"Сеанс {minutes_label(duration)}",
                callback_data=f"manual_work_{duration}",
            )
        )
    for duration in BREAK_DURATIONS:
        markup.add(
            types.InlineKeyboardButton(
                f"Перерыв {minutes_label(duration)}",
                callback_data=f"manual_break_{duration}",
            )
        )
    markup.row(
        types.InlineKeyboardButton("Отменить последний", callback_data="manual_undo"),
        types.InlineKeyboardButton("Удалить блок", callback_data="manual_remove_menu"),
    )
    markup.add(types.InlineKeyboardButton("Сохранить шаблон", callback_data="template_save_blocks"))
    markup.add(types.InlineKeyboardButton("Сбросить", callback_data="manual_reset"))
    return markup


def get_template_apply_markup():
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("Применить на неделю", callback_data="template_apply_week"))
    markup.add(types.InlineKeyboardButton("Применить на месяц", callback_data="template_apply_month"))
    markup.add(types.InlineKeyboardButton("Готово", callback_data="admin_schedule"))
    return markup


def apply_template_to_range(template, days, today=None):
    if not template:
        return {"applied": [], "skipped": []}
    today = today or datetime.date.today()
    schedule = schedule_from_template(template)
    if not schedule:
        return {"applied": [], "skipped": []}
    applied = []
    skipped = []
    for offset in range(days):
        target = today + datetime.timedelta(days=offset)
        date_value = target.isoformat()
        if has_bookings_on_date(date_value):
            skipped.append((date_value, "есть записи"))
            continue
        with db_lock:
            cursor.execute(
                "SELECT COUNT(*) FROM day_slots WHERE date=? AND source_mode='manual'",
                (date_value,),
            )
            manual_count = cursor.fetchone()[0]
        if manual_count:
            skipped.append((date_value, "ручной график"))
            continue
        delete_day_slots(date_value)
        with db_lock:
            for slot in schedule["slots"]:
                cursor.execute(
                    """
                    INSERT INTO day_slots(date, start_time, end_time, is_break, is_booked, is_closed, source_mode)
                    VALUES (?, ?, ?, ?, 0, 0, 'template')
                    """,
                    (date_value, slot["start"], slot["end"], 1 if slot["type"] == "break" else 0),
                )
            conn.commit()
        applied.append(date_value)
    return {"applied": applied, "skipped": skipped}


def get_create_day_mode_markup(date_value):
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton(
            "Создать по шаблону", callback_data=f"create_template_{date_value}"
        )
    )
    markup.add(
        types.InlineKeyboardButton(
            "Собрать вручную", callback_data=f"create_manual_{date_value}"
        )
    )
    markup.add(types.InlineKeyboardButton("Назад", callback_data="admin_schedule"))
    return markup


def get_template_save_markup(date_value):
    markup = types.InlineKeyboardMarkup()
    markup.add(
        types.InlineKeyboardButton(
            "Сохранить день", callback_data=f"save_template_day_{date_value}"
        )
    )
    markup.add(types.InlineKeyboardButton("Назад", callback_data="admin_schedule"))
    return markup


def get_manual_builder_markup():
    markup = types.InlineKeyboardMarkup(row_width=2)
    for duration in WORK_DURATIONS:
        markup.add(
            types.InlineKeyboardButton(
                f"Сеанс {minutes_label(duration)}",
                callback_data=f"manual_work_{duration}",
            )
        )
    for duration in BREAK_DURATIONS:
        markup.add(
            types.InlineKeyboardButton(
                f"Перерыв {minutes_label(duration)}",
                callback_data=f"manual_break_{duration}",
            )
        )
    markup.row(
        types.InlineKeyboardButton("Отменить последний", callback_data="manual_undo"),
        types.InlineKeyboardButton("Удалить блок", callback_data="manual_remove_menu"),
    )
    markup.add(types.InlineKeyboardButton("Создать график", callback_data="manual_save"))
    markup.add(types.InlineKeyboardButton("Сбросить", callback_data="manual_reset"))
    return markup


def get_manual_force_markup():
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("Подтвердить как есть", callback_data="manual_force"))
    markup.add(
        types.InlineKeyboardButton(
            "Продолжить редактирование", callback_data="manual_continue"
        )
    )
    markup.add(types.InlineKeyboardButton("Сбросить", callback_data="manual_reset"))
    return markup


def get_manual_remove_markup(blocks):
    markup = types.InlineKeyboardMarkup()
    for index, block in enumerate(blocks):
        label = "Сеанс" if block["type"] == "work" else "Перерыв"
        markup.add(
            types.InlineKeyboardButton(
                f"Удалить {index + 1}: {label} {minutes_label(block['duration'])}",
                callback_data=f"manual_remove_{index}",
            )
        )
    markup.add(types.InlineKeyboardButton("Назад", callback_data="manual_continue"))
    return markup


def _delete_msg_safe(chat_id, msg_id):
    if not msg_id:
        return
    try:
        bot.delete_message(chat_id, msg_id)
    except Exception:
        pass


def _render_day_view(chat_id, user_id, date_value, source_message=None):
    state = admin_states.setdefault(user_id, {})
    _delete_msg_safe(chat_id, state.pop("day_prompt_msg_id", None))
    if source_message is not None:
        _delete_msg_safe(chat_id, source_message.message_id)
    text = format_day_view(date_value)
    markup = get_day_edit_markup(date_value)
    day_view_msg_id = state.get("day_view_msg_id")
    if day_view_msg_id:
        try:
            safe_edit_message_text(text, chat_id, day_view_msg_id, reply_markup=markup)
            return
        except Exception:
            _delete_msg_safe(chat_id, day_view_msg_id)
    sent = bot.send_message(chat_id, text, reply_markup=markup)
    state["day_view_msg_id"] = sent.message_id


def render_manual_builder(chat_id, user_id, extra_text=None, markup=None):
    state = admin_states[user_id]
    schedule = build_manual_timeline(
        state["shift_start"], state["shift_end"], state["blocks"]
    )
    text = format_schedule_preview(schedule, f"Конструктор дня {state['date']}")
    if extra_text:
        text = f"{extra_text}\n\n{text}"
    if markup is None:
        if state.get("flow") == "template_builder":
            markup = get_template_builder_markup()
        else:
            markup = get_manual_builder_markup()
    builder_msg_id = state.get("builder_msg_id")
    if builder_msg_id:
        try:
            bot.edit_message_text(text, chat_id, builder_msg_id, reply_markup=markup)
            return
        except Exception:
            try:
                bot.delete_message(chat_id, builder_msg_id)
            except Exception:
                pass
            state["builder_msg_id"] = None
    sent = bot.send_message(chat_id, text, reply_markup=markup)
    state["builder_msg_id"] = sent.message_id


def save_manual_schedule(user_id):
    state = admin_states[user_id]
    schedule = build_manual_timeline(
        state["shift_start"], state["shift_end"], state["blocks"]
    )
    ok, text = save_schedule_to_day(state["date"], schedule, "manual")
    return ok, text, schedule


def get_contact_value(message):
    if message.contact:
        return message.contact.phone_number
    return message.text.strip()


@bot.message_handler(commands=["start"])
def start(message):
    remember_tg_user(message.from_user)
    if DEMO_MODE:
        bot.send_message(
            message.chat.id,
            (
                "Привет! 👋\n\n"
                "Это демонстрационный бот — здесь можно потыкать кнопки и почувствовать, "
                "как такой бот будет выглядеть и работать у вас — будь то салон или один мастер: меню услуг, "
                "портфолио по подвидам, календарь записи и аккуратные подтверждения.\n\n"
                "Это безопасная песочница: записи никуда не сохраняются, "
                "никто вам не позвонит и не окажет услуг.\n\n"
                f"Подробнее о тарифах и примерах: {DEMO_TARIFFS_URL}\n"
                f"Заказать своего бота: {DEMO_USERNAME}"
            ),
            reply_markup=get_main_menu(message.chat.id),
        )
        return
    bot.send_message(
        message.chat.id,
        "Кто ты? Выбери режим.",
        reply_markup=get_main_menu(message.chat.id),
    )


@bot.message_handler(func=lambda m: _menu_text_matches(m.text, "О тарифах"))
def about_tariffs(message):
    if not DEMO_MODE:
        return
    bot.send_message(
        message.chat.id,
        (
            "📌 Подробнее о тарифах и примерах работы — на канале:\n"
            f"{DEMO_TARIFFS_URL}\n\n"
            "Тарифы вкратце:\n"
            "• Старт — визитка с заявкой, от 15 000 ₽\n"
            "• Стандарт — запись по календарю, от 25 000 ₽\n"
            "• Премиум — этот бот без ограничений демо, от 50 000 ₽\n\n"
            f"Заказать или задать вопрос: {DEMO_USERNAME}"
        ),
    )


def _menu_text_matches(value, target):
    if not value:
        return False
    stripped = value.strip()
    if stripped == target:
        return True
    parts = stripped.rsplit(" ", 1)
    if len(parts) == 2 and parts[0].strip() == target:
        return True
    return False


@bot.message_handler(func=lambda m: _menu_text_matches(m.text, "Клиент"))
def client_mode_entry(message):
    remember_tg_user(message.from_user)
    bot.send_message(
        message.chat.id,
        "Режим клиента",
        reply_markup=get_client_menu(),
    )


@bot.message_handler(func=lambda m: _menu_text_matches(m.text, "Админ"))
def admin_mode_entry(message):
    remember_tg_user(message.from_user)
    if not is_admin(message.chat.id):
        bot.send_message(
            message.chat.id,
            "Этот режим доступен только администраторам.",
            reply_markup=get_main_menu(message.chat.id),
        )
        return
    open_admin_menu(message.chat.id, message.from_user.id)


@bot.message_handler(func=lambda m: _menu_text_matches(m.text, "Назад"))
def back_to_main_menu(message):
    bot.send_message(
        message.chat.id,
        "Главное меню",
        reply_markup=get_main_menu(message.chat.id),
    )


@bot.message_handler(func=lambda m: _menu_text_matches(m.text, "Очистить чат"))
def clear_chat_client(message):
    user_id = message.from_user.id
    try:
        bot.delete_message(message.chat.id, message.message_id)
    except Exception:
        pass
    clear_chat(message.chat.id)
    if is_admin(user_id):
        open_admin_menu(message.chat.id, user_id)
    else:
        bot.send_message(
            message.chat.id,
            "Меню клиента",
            reply_markup=get_client_menu(),
        )


@bot.message_handler(func=lambda m: _menu_text_matches(m.text, "Мои записи"))
def my_bookings(message):
    user_id = message.from_user.id
    if DEMO_MODE:
        bot.send_message(
            message.chat.id,
            (
                "В демо-боте записи не сохраняются.\n\n"
                "У вашего бота здесь будет полноценный список будущих визитов: "
                "дата, время, услуга, статус — с кнопками «Отменить» и «Перенести». "
                "Перенос предлагается клиенту в новых свободных окнах и подтверждается мастером.\n\n"
                f"Хотите такого же бота? Напишите {DEMO_USERNAME}"
            ),
        )
        return
    rows = list_client_bookings(user_id)
    if not rows:
        bot.send_message(message.chat.id, "У тебя пока нет записей.")
        return
    bot.send_message(message.chat.id, "Твои записи:")
    for row in rows:
        booking_id = row[0]
        status = row[5]
        line = format_client_booking_line(row)
        markup = get_user_booking_markup(booking_id, status)
        bot.send_message(message.chat.id, line, reply_markup=markup)


@bot.message_handler(func=lambda m: _menu_text_matches(m.text, "Выбрать услугу"))
def choose_service(message):
    markup = types.InlineKeyboardMarkup()
    for key, service in SERVICE_CATALOG.items():
        markup.add(
            types.InlineKeyboardButton(
                service["title"], callback_data=f"service_info_{key}"
            )
        )
    bot.send_message(message.chat.id, "Выберите услугу", reply_markup=markup)


@bot.message_handler(func=lambda m: m.text == "Админка")
def admin_menu_entry(message):
    remember_tg_user(message.from_user)
    if not is_admin(message.chat.id):
        return
    open_admin_menu(message.chat.id, message.from_user.id)


@bot.message_handler(content_types=["text", "contact"])
def text_router(message):
    user_id = message.from_user.id
    remember_tg_user(message.from_user)

    if user_id in client_states and message.content_type == "text":
        state = client_states[user_id]
        flow = state.get("flow")
        text_value = (message.text or "").strip() or None
        if flow == "profile_allergies":
            _advance_profile_flow(user_id, message.chat.id, "allergies", text_value, source_user=message.from_user)
            return
        if flow == "profile_preferences":
            _advance_profile_flow(user_id, message.chat.id, "preferences", text_value, source_user=message.from_user)
            return
        if flow == "profile_extra":
            _advance_profile_flow(user_id, message.chat.id, "extra", text_value, source_user=message.from_user)
            return

    if is_admin(user_id) and user_id in admin_states:
        state = admin_states[user_id]
        flow = state.get("flow")

        if flow == "template_start":
            time_value = parse_time_to_minutes(message.text)
            if time_value is None:
                bot.send_message(message.chat.id, "Введи время в формате HH:MM")
                return
            state["start"] = minutes_to_time(time_value)
            state["flow"] = "template_end"
            bot.send_message(message.chat.id, "Теперь введи конец смены в формате HH:MM")
            return

        if flow == "template_end":
            time_value = parse_time_to_minutes(message.text)
            if time_value is None:
                bot.send_message(message.chat.id, "Введи время в формате HH:MM")
                return
            if parse_time_to_minutes(state["start"]) is not None and time_value <= parse_time_to_minutes(state["start"]):
                bot.send_message(message.chat.id, "Конец смены должен быть позже начала")
                return
            state["end"] = minutes_to_time(time_value)
            state["shift_start"] = state["start"]
            state["shift_end"] = state["end"]
            state["blocks"] = []
            state["flow"] = "template_builder"
            state["date"] = "шаблон"
            render_manual_builder(
                message.chat.id,
                user_id,
                "Собери шаблон по блокам и нажми «Сохранить шаблон»",
                markup=get_template_builder_markup(),
            )
            return

        if flow == "create_day_input":
            date_value = normalize_date_text(message.text)
            if not date_value:
                bot.send_message(message.chat.id, "Введи дату в формате YYYY-MM-DD")
                return
            state["date"] = date_value
            state["flow"] = "create_day_mode"
            bot.send_message(
                message.chat.id,
                f"Как создать график на {date_value}?",
                reply_markup=get_create_day_mode_markup(date_value),
            )
            return

        if flow == "manual_shift_start":
            time_value = parse_time_to_minutes(message.text)
            if time_value is None:
                bot.send_message(message.chat.id, "Введи время в формате HH:MM")
                return
            state["shift_start"] = minutes_to_time(time_value)
            state["flow"] = "manual_shift_end"
            bot.send_message(message.chat.id, "Теперь введи конец смены в формате HH:MM")
            return

        if flow == "manual_shift_end":
            time_value = parse_time_to_minutes(message.text)
            if time_value is None:
                bot.send_message(message.chat.id, "Введи время в формате HH:MM")
                return
            state["shift_end"] = minutes_to_time(time_value)
            state["blocks"] = []
            state["flow"] = "manual_builder"
            render_manual_builder(message.chat.id, user_id)
            return

        if flow == "edit_day_input":
            date_value = normalize_date_text(message.text)
            if not date_value:
                bot.send_message(message.chat.id, "Введи дату в формате YYYY-MM-DD")
                return
            state["date"] = date_value
            ensure_template_schedule_for_date(date_value)
            _render_day_view(message.chat.id, user_id, date_value, message)
            return

        if flow in ("edit_add_slot", "edit_add_break"):
            kind = "break" if flow == "edit_add_break" else "window"
            parts = [part.strip() for part in message.text.split("-")]
            if len(parts) != 2:
                example = "13:00-14:00" if kind == "break" else "18:00-19:30"
                bot.send_message(message.chat.id, f"Введи окно как HH:MM-HH:MM, например {example}")
                return
            start_minutes = parse_time_to_minutes(parts[0])
            end_minutes = parse_time_to_minutes(parts[1])
            start_time = minutes_to_time(start_minutes) if start_minutes is not None else None
            end_time = minutes_to_time(end_minutes) if end_minutes is not None else None
            if not start_time or not end_time or end_minutes <= start_minutes:
                bot.send_message(message.chat.id, "Некорректное окно. Пример: 18:00-19:30")
                return

            date_value = state["date"]
            has_booking, overlaps = find_overlapping_slots(date_value, start_time, end_time)
            label = "перерыв" if kind == "break" else "окно"
            if has_booking:
                bot.send_message(
                    message.chat.id,
                    f"Не получилось добавить {label}: пересекается с записью клиента. Сначала отмени запись.",
                )
                return
            open_overlaps = [s for s in overlaps if not s[4]]
            closed_overlaps = [s for s in overlaps if s[4]]
            if open_overlaps:
                names = ", ".join(describe_slot(s[1], s[2], s[3], s[4]) for s in open_overlaps)
                bot.send_message(
                    message.chat.id,
                    (
                        f"Не получилось добавить {label}: пересекается с {names}. "
                        "Сначала закрой существующее окно (кнопка 🚫 в редакторе дня), "
                        "после этого добавь новое."
                    ),
                )
                return
            if closed_overlaps:
                ids = [slot[0] for slot in closed_overlaps]
                old_descr = ", ".join(describe_slot(s[1], s[2], s[3], s[4]) for s in closed_overlaps)
                new_descr = f"{'служебное окно' if kind == 'break' else 'окно'} {start_time}–{end_time}"
                state["pending_replace"] = {
                    "kind": kind,
                    "date": date_value,
                    "start": start_time,
                    "end": end_time,
                    "ids": ids,
                }
                state["flow"] = "edit_day_input"
                markup = types.InlineKeyboardMarkup()
                markup.row(
                    types.InlineKeyboardButton("Подтвердить", callback_data="replace_confirm"),
                    types.InlineKeyboardButton("Отмена", callback_data="replace_cancel"),
                )
                bot.send_message(
                    message.chat.id,
                    f"Заменить {old_descr} на {new_descr}?",
                    reply_markup=markup,
                )
                return

            insert_slot(date_value, start_time, end_time, kind)
            state["flow"] = "edit_day_input"
            _render_day_view(message.chat.id, user_id, date_value, message)
            return

        if flow == "close_date_input":
            date_value = normalize_date_text(message.text)
            if not date_value:
                bot.send_message(message.chat.id, "Введи дату в формате YYYY-MM-DD")
                return
            ensure_template_schedule_for_date(date_value)
            with db_lock:
                cursor.execute(
                    """
                    UPDATE day_slots
                    SET is_closed=1
                    WHERE date=? AND is_booked=0 AND is_break=0
                    """,
                    (date_value,),
                )
                conn.commit()
            clear_admin_state(user_id)
            bot.send_message(message.chat.id, f"Дата {date_value} закрыта")
            open_admin_menu(message.chat.id, user_id)
            return

        if flow == "client_note_input":
            target_user_id = state["target_user_id"]
            client_name = state.get("client_name") or "Клиент"
            set_client_note(target_user_id, client_name, message.text.strip())
            clear_admin_state(user_id)
            bot.send_message(message.chat.id, "Заметка сохранена", reply_markup=get_admin_menu(user_id))
            return

        if flow == "admin_add_input":
            if not is_owner(user_id):
                clear_admin_state(user_id)
                return
            raw = (message.text or "").strip()
            target_id = None
            target_username = None
            if raw.startswith("@") or not raw.lstrip("-").isdigit():
                resolved = find_tg_user_by_username(raw)
                if resolved:
                    target_id = resolved[0]
                    target_username = resolved[1]
                else:
                    bot.send_message(
                        message.chat.id,
                        "Не нашёл такого пользователя в базе бота. Попроси его отправить /start и пришли username снова, либо пришли числовой ID.",
                    )
                    return
            else:
                try:
                    target_id = int(raw)
                except ValueError:
                    bot.send_message(message.chat.id, "Не понял. Пришли @username или числовой ID.")
                    return
                resolved = None
                with db_lock:
                    cursor.execute("SELECT username FROM tg_users WHERE user_id=?", (target_id,))
                    row = cursor.fetchone()
                target_username = row[0] if row else None
            if target_id == OWNER_ID:
                bot.send_message(message.chat.id, "Это владелец, у него уже есть все права.")
                clear_admin_state(user_id)
                return
            add_admin_record(target_id, target_username, user_id)
            clear_admin_state(user_id)
            bot.send_message(
                message.chat.id,
                f"Готово. Админ добавлен: {target_id} (@{target_username})" if target_username else f"Готово. Админ добавлен: ID {target_id}",
                reply_markup=get_admins_management_markup(),
            )
            return

        if flow == "client_search_input":
            query = (message.text or "").strip()
            clear_admin_state(user_id)
            if not query:
                bot.send_message(
                    message.chat.id,
                    "Поиск отменён.",
                    reply_markup=get_clients_list_markup(),
                )
                return
            bot.send_message(
                message.chat.id,
                f"Результаты поиска по запросу «{query}»:",
                reply_markup=get_clients_list_markup(query=query),
            )
            return

    if user_id in user_data and user_data[user_id].get("awaiting_contact"):
        if message.content_type == "text" and (message.text or "").strip() == "Отмена":
            user_data.pop(user_id, None)
            bot.send_message(
                message.chat.id,
                "Запись отменена.",
                reply_markup=get_client_menu(),
            )
            return
        contact = get_contact_value(message)
        slot_id = user_data[user_id]["slot_id"]
        with db_lock:
            cursor.execute(
                """
                SELECT date, start_time, end_time, is_booked, is_closed
                FROM day_slots
                WHERE id=? AND is_break=0
                """,
                (slot_id,),
            )
            slot = cursor.fetchone()

            if not slot or slot[3] or slot[4]:
                bot.send_message(message.chat.id, "Это окно уже недоступно")
                user_data.pop(user_id, None)
                return
            if is_less_than_24_hours(slot[0], slot[1]):
                bot.send_message(
                    message.chat.id,
                    "Запись доступна только минимум за 24 часа до сеанса.",
                    reply_markup=get_main_menu(user_id),
                )
                user_data.pop(user_id, None)
                return

            cursor.execute(
                """
                INSERT INTO bookings(
                    user_id, service, date, time, end_time, contact, reminder_sent,
                    status, created_at, client_name, client_username, master_reminder_sent
                )
                VALUES (?, ?, ?, ?, ?, ?, 0, 'pending', ?, ?, ?, 0)
                """,
                (
                    user_id,
                    user_data[user_id]["service"],
                    slot[0],
                    slot[1],
                    slot[2],
                    contact,
                    datetime.datetime.now().isoformat(timespec="seconds"),
                    message.from_user.full_name,
                    message.from_user.username,
                ),
            )
            booking_id = cursor.lastrowid
            cursor.execute(
                "UPDATE day_slots SET is_booked=1 WHERE id=?",
                (slot_id,),
            )
            conn.commit()

        bot.send_message(
            message.chat.id,
            (
                "Заявка на запись отправлена мастеру.\n"
                "Бронь пока на рассмотрении — как только мастер подтвердит, я пришлю сообщение."
            ),
            reply_markup=get_main_menu(user_id),
        )
        bot.send_message(
            message.chat.id,
            (
                f"Заявка #{booking_id}\n"
                f"{user_data[user_id]['service']} | {slot[0]} | {slot[1]}–{slot[2]}\n"
                f"Статус: {status_label('pending')}"
            ),
            reply_markup=get_user_booking_markup(booking_id, "pending"),
        )
        notify_admin_new_booking(
            booking_id,
            user_id,
            message.from_user.full_name,
            message.from_user.username,
            user_data[user_id]["service"],
            slot[0],
            slot[1],
            slot[2],
            contact,
        )
        user_data.pop(user_id, None)
        return


@bot.callback_query_handler(func=lambda call: True)
def callback_router(call):
    bot.answer_callback_query(call.id)
    remember_tg_user(call.from_user)
    user_id = call.from_user.id
    data = call.data

    if data == "noop":
        return

    if data == "noop_busy_date":
        bot.answer_callback_query(call.id, "На этот день уже есть запись")
        return

    if data == "locked_slot":
        bot.answer_callback_query(call.id, "Это окно уже занято записью")
        return

    if data == "client_close":
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        return

    if data == "profile_fill_start":
        client_states[user_id] = {"flow": "profile_allergies"}
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass
        bot.send_message(
            call.message.chat.id,
            "1/3. Есть ли аллергии или противопоказания? Если нет — нажмите «Пропустить».",
            reply_markup=get_profile_skip_markup("allergies"),
        )
        return

    if data == "profile_fill_skip":
        client_states.pop(user_id, None)
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass
        bot.send_message(call.message.chat.id, "Хорошо, пропускаем. Вы всегда можете рассказать мастеру лично.")
        return

    if data.startswith("profile_skip_"):
        step = data.split("_", 2)[2]
        state = client_states.get(user_id)
        if not state:
            return
        try:
            bot.edit_message_reply_markup(call.message.chat.id, call.message.message_id, reply_markup=None)
        except Exception:
            pass
        _advance_profile_flow(user_id, call.message.chat.id, step, value=None, source_user=call.from_user)
        return

    if data.startswith("service_info_"):
        flush_step_msgs(user_id, call.message.chat.id)
        service_key = data.split("_", 2)[2]
        service = SERVICE_CATALOG[service_key]
        safe_edit_message_text(
            (
                f"{service['title']}\n"
                f"Длительность: {minutes_label(service['duration'])}\n"
                f"Цена: {service['price']}\n\n"
                f"{service['description']}"
            ),
            call.message.chat.id,
            call.message.message_id,
            reply_markup=get_service_info_markup(service_key),
        )
        return

    if data.startswith("portfolio_service_"):
        flush_step_msgs(user_id, call.message.chat.id)
        service_key = data.split("_", 2)[2]
        service = SERVICE_CATALOG[service_key]
        try:
            safe_edit_message_text(
                f"Портфолио: {service['title']}. Выберите подвид.",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=get_portfolio_subtypes_markup(service_key),
            )
        except Exception:
            sent = bot.send_message(
                call.message.chat.id,
                f"Портфолио: {service['title']}. Выберите подвид.",
                reply_markup=get_portfolio_subtypes_markup(service_key),
            )
            add_step_msg(user_id, sent.message_id)
        return

    if data.startswith("portfolio_subtype_"):
        _, _, service_key, subtype_key = data.split("_", 3)
        service = SERVICE_CATALOG[service_key]
        subtype = service["subtypes"][subtype_key]
        images = get_portfolio_images(service_key, subtype_key)
        caption = (
            f"{service['title']} → {subtype['title']}\n\n"
            f"{subtype['description']}\n\n"
            f"{service['description']}\n"
            f"Длительность услуги: {minutes_label(service['duration'])}\n"
            f"Цена: {service['price']}"
        )
        flush_step_msgs(user_id, call.message.chat.id)
        # Drop the prompt the user clicked from above the photos so it doesn't
        # linger as a duplicate "Назад" anchor after returning to service info.
        safe_delete_message(call.message.chat.id, call.message.message_id)
        if images:
            try:
                sent = _send_media_group_with_retry(
                    call.message.chat.id, images, caption
                )
                for m in sent:
                    add_step_msg(user_id, m.message_id)
            except _TRANSIENT_NETWORK_EXCEPTIONS as exc:
                logger.warning(
                    "Portfolio media group failed after retries: %s", exc
                )
                fallback = bot.send_message(
                    call.message.chat.id,
                    (
                        f"{caption}\n\n"
                        "(Сейчас сеть моргает — фото не загрузились. "
                        "Попробуй ещё раз через секунду 🙏)"
                    ),
                )
                add_step_msg(user_id, fallback.message_id)
        else:
            sent = bot.send_message(call.message.chat.id, caption)
            add_step_msg(user_id, sent.message_id)
        sent = bot.send_message(
            call.message.chat.id,
            "Можно посмотреть другой подвид или выбрать услугу.",
            reply_markup=get_portfolio_subtypes_markup(service_key),
        )
        add_step_msg(user_id, sent.message_id)
        return

    if data.startswith("cancel_"):
        booking_id = int(data.split("_")[1])
        with db_lock:
            cursor.execute(
                "SELECT user_id, date, time, end_time, status FROM bookings WHERE id=?",
                (booking_id,),
            )
            booking = cursor.fetchone()
            if not booking:
                safe_edit_message_text("Запись уже удалена", call.message.chat.id, call.message.message_id)
                return
            if booking[0] != user_id and not is_admin(user_id):
                bot.answer_callback_query(call.id, "Можно отменять только свои записи")
                return
            cursor.execute(
                "UPDATE bookings SET status='cancelled' WHERE id=?",
                (booking_id,),
            )
            cursor.execute(
                """
                UPDATE day_slots
                SET is_booked=0
                WHERE date=? AND start_time=? AND end_time=?
                """,
                (booking[1], booking[2], booking[3]),
            )
            conn.commit()
        safe_edit_message_text("Отменено", call.message.chat.id, call.message.message_id)
        notify_all_admins(
            f"Запись #{booking_id} отменена клиентом. Было: {booking[1]} {booking[2]}–{booking[3]}",
            exclude=[booking[0]],
        )
        return

    if data.startswith("reschedule_start_"):
        booking_id = int(data.split("_")[2])
        with db_lock:
            cursor.execute(
                "SELECT user_id, status FROM bookings WHERE id=?",
                (booking_id,),
            )
            booking = cursor.fetchone()
        if not booking or booking[0] != user_id:
            bot.answer_callback_query(call.id, "Запись не найдена")
            return
        if booking[1] != "confirmed":
            bot.answer_callback_query(call.id, "Перенос доступен только для подтвержденных записей")
            return
        bot.send_message(
            call.message.chat.id,
            "Выберите новую дату для переноса",
            reply_markup=get_reschedule_dates_markup(booking_id),
        )
        return

    if data.startswith("reschedule_date_"):
        _, _, booking_id, date_value = data.split("_", 3)
        booking_id = int(booking_id)
        bot.send_message(
            call.message.chat.id,
            f"Выберите новое время на {date_value}",
            reply_markup=get_reschedule_slots_markup(booking_id, date_value),
        )
        return

    if data.startswith("reschedule_slot_"):
        _, _, booking_id, slot_id = data.split("_")
        booking_id = int(booking_id)
        slot_id = int(slot_id)
        with db_lock:
            cursor.execute(
                """
                SELECT id, user_id, service, date, time, end_time, status
                FROM bookings
                WHERE id=?
                """,
                (booking_id,),
            )
            booking = cursor.fetchone()
            cursor.execute(
                """
                SELECT date, start_time, end_time, is_booked, is_closed
                FROM day_slots
                WHERE id=? AND is_break=0
                """,
                (slot_id,),
            )
            slot = cursor.fetchone()
            if not booking or booking[1] != user_id:
                bot.answer_callback_query(call.id, "Запись не найдена")
                return
            if booking[6] != "confirmed":
                bot.answer_callback_query(call.id, "Перенос доступен только для подтвержденных записей")
                return
            cursor.execute(
                """
                SELECT COUNT(*)
                FROM reschedule_requests
                WHERE booking_id=? AND status='pending'
                """,
                (booking_id,),
            )
            pending_count = cursor.fetchone()[0]
            if pending_count:
                bot.answer_callback_query(call.id, "По этой записи уже есть заявка на перенос")
                return
            if not slot or slot[3] or slot[4]:
                bot.answer_callback_query(call.id, "Новое окно уже недоступно")
                return
            cursor.execute(
                """
                INSERT INTO reschedule_requests(
                    booking_id, user_id, old_date, old_time, old_end_time,
                    new_slot_id, new_date, new_time, new_end_time, status, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
                """,
                (
                    booking_id,
                    user_id,
                    booking[3],
                    booking[4],
                    booking[5],
                    slot_id,
                    slot[0],
                    slot[1],
                    slot[2],
                    datetime.datetime.now().isoformat(timespec="seconds"),
                ),
            )
            request_id = cursor.lastrowid
            cursor.execute("UPDATE day_slots SET is_booked=1 WHERE id=?", (slot_id,))
            conn.commit()
        bot.send_message(
            call.message.chat.id,
            (
                "Заявка на перенос отправлена мастеру.\n"
                "Текущая запись сохраняется, пока мастер не подтвердит перенос."
            ),
        )
        notify_admin_reschedule_request(
            request_id,
            booking_id,
            user_id,
            booking[2],
            f"{booking[3]} {booking[4]}–{booking[5]}",
            f"{slot[0]} {slot[1]}–{slot[2]}",
        )
        return

    if data.startswith("client_reschedule_accept_"):
        request_id = int(data.split("_")[3])
        with db_lock:
            cursor.execute(
                """
                SELECT id, booking_id, user_id, old_date, old_time, old_end_time,
                       new_slot_id, new_date, new_time, new_end_time, status
                FROM reschedule_requests
                WHERE id=?
                """,
                (request_id,),
            )
            request = cursor.fetchone()
            if not request or request[2] != user_id or request[10] != "master_pending":
                bot.answer_callback_query(call.id, "Предложение уже недоступно")
                return
            cursor.execute(
                """
                UPDATE bookings
                SET date=?, time=?, end_time=?, reminder_sent=0, master_reminder_sent=0
                WHERE id=?
                """,
                (request[7], request[8], request[9], request[1]),
            )
            cursor.execute(
                """
                UPDATE day_slots
                SET is_booked=0
                WHERE date=? AND start_time=? AND end_time=?
                """,
                (request[3], request[4], request[5]),
            )
            cursor.execute(
                "UPDATE reschedule_requests SET status='confirmed' WHERE id=?",
                (request_id,),
            )
            conn.commit()
        safe_edit_message_text(
            "Перенос подтвержден",
            call.message.chat.id,
            call.message.message_id,
        )
        notify_all_admins(
            f"Клиент подтвердил перенос записи #{request[1]} на {request[7]} {request[8]}–{request[9]}",
        )
        return

    if data.startswith("client_reschedule_decline_"):
        request_id = int(data.split("_")[3])
        with db_lock:
            cursor.execute(
                """
                SELECT booking_id, user_id, new_slot_id
                FROM reschedule_requests
                WHERE id=? AND status='master_pending'
                """,
                (request_id,),
            )
            request = cursor.fetchone()
            if not request or request[1] != user_id:
                bot.answer_callback_query(call.id, "Предложение уже недоступно")
                return
            cursor.execute(
                "UPDATE reschedule_requests SET status='declined' WHERE id=?",
                (request_id,),
            )
            cursor.execute(
                "UPDATE day_slots SET is_booked=0 WHERE id=?",
                (request[2],),
            )
            conn.commit()
        safe_edit_message_text(
            "Вы оставили старое время записи",
            call.message.chat.id,
            call.message.message_id,
        )
        notify_all_admins(f"Клиент отклонил перенос записи #{request[0]}")
        return

    if data.startswith("service_"):
        flush_step_msgs(user_id, call.message.chat.id)
        service_key = data.split("_", 1)[1]
        service = SERVICE_CATALOG[service_key]
        user_data[user_id] = {
            "service": service["title"],
            "service_key": service_key,
            "service_duration": service["duration"],
            "service_price": service["price"],
        }
        now = datetime.datetime.now()
        safe_edit_message_text(
            "Выберите дату",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=get_calendar(
                now.year, now.month, service_duration=user_data[user_id].get("service_duration")
            ),
        )
        return

    if data.startswith("date_"):
        date_value = data.split("_", 1)[1]
        user_data.setdefault(user_id, {})
        user_data[user_id]["date"] = date_value
        if DEMO_MODE:
            safe_edit_message_text(
                f"Выберите время на {date_value}",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=get_demo_time_slots_markup(date_value),
            )
            return
        safe_edit_message_text(
            f"Выберите время на {date_value}",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=get_time_slots_markup(date_value, user_id),
        )
        return

    if DEMO_MODE and data.startswith("demotime_"):
        # Format: demotime_YYYY-MM-DD_HH:MM
        _, date_value, slot_time = data.split("_", 2)
        try:
            human_date = datetime.datetime.strptime(date_value, "%Y-%m-%d").strftime("%d.%m.%Y")
        except ValueError:
            human_date = date_value
        safe_edit_message_text(
            (
                f"Готово! Вы записаны на {human_date} в {slot_time}. ✨\n\n"
                "Это демонстрационный бот — реальной записи не сохраняется. "
                "У настоящего бота на этом месте мастер получил бы уведомление, "
                "а клиенту пришло бы то же подтверждение и напоминание за день.\n\n"
                f"Спасибо, что попробовали! Хотите такого же бота себе? Напишите {DEMO_USERNAME}"
            ),
            call.message.chat.id,
            call.message.message_id,
        )
        return

    if data.startswith("slot_"):
        slot_id = int(data.split("_")[1])
        with db_lock:
            cursor.execute(
                "SELECT date, start_time, is_booked, is_closed FROM day_slots WHERE id=?",
                (slot_id,),
            )
            slot = cursor.fetchone()
        if not slot or slot[2] or slot[3] or is_less_than_24_hours(slot[0], slot[1]):
            bot.answer_callback_query(call.id, "Это окно недоступно")
            return
        user_data.setdefault(user_id, {})
        user_data[user_id]["slot_id"] = slot_id
        user_data[user_id]["awaiting_contact"] = True
        safe_edit_message_text(
            "Отправьте телефон или введите контакт для связи",
            call.message.chat.id,
            call.message.message_id,
        )
        contact_kb = types.ReplyKeyboardMarkup(resize_keyboard=True, one_time_keyboard=True)
        contact_kb.add(types.KeyboardButton("Отправить мой номер", request_contact=True))
        contact_kb.add("Отмена")
        bot.send_message(
            call.message.chat.id,
            "Можно нажать кнопку ниже, чтобы отправить номер из Telegram, или ввести контакт текстом.",
            reply_markup=contact_kb,
        )
        return

    if data == "busy":
        bot.answer_callback_query(call.id, "Это окно недоступно")
        return

    if data == "no_slots_day":
        bot.answer_callback_query(call.id, "На эту дату сейчас нет свободных окон")
        return

    if data == "back_services":
        flush_step_msgs(user_id, call.message.chat.id)
        markup = types.InlineKeyboardMarkup()
        for key, service in SERVICE_CATALOG.items():
            markup.add(
                types.InlineKeyboardButton(
                    service["title"], callback_data=f"service_info_{key}"
                )
            )
        try:
            safe_edit_message_text(
                "Выберите услугу",
                call.message.chat.id,
                call.message.message_id,
                reply_markup=markup,
            )
        except Exception:
            bot.send_message(call.message.chat.id, "Выберите услугу", reply_markup=markup)
        return

    if data == "back_calendar":
        current = datetime.datetime.now()
        safe_edit_message_text(
            "Выберите дату",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=get_calendar(
                current.year,
                current.month,
                service_duration=user_data.get(user_id, {}).get("service_duration"),
            ),
        )
        return

    if data.startswith("prev_") or data.startswith("next_"):
        _direction, year, month = data.split("_")
        year = int(year)
        month = int(month)
        if not is_month_in_window(year, month):
            bot.answer_callback_query(call.id, "Этого месяца ещё нет в окне записи")
            return
        safe_edit_message_text(
            "Выберите дату",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=get_calendar(
                year,
                month,
                service_duration=user_data.get(user_id, {}).get("service_duration"),
            ),
        )
        return

    if not is_admin(user_id):
        return

    if data == "admin_settings":
        enabled = get_setting("master_reminders_enabled", "1") == "1"
        text = "Напоминания мастеру за 15 минут: включены" if enabled else "Напоминания мастеру за 15 минут: выключены"
        bot.send_message(call.message.chat.id, text, reply_markup=get_admin_settings_markup())
        return

    if data == "toggle_master_reminders":
        enabled = get_setting("master_reminders_enabled", "1") == "1"
        set_setting("master_reminders_enabled", "0" if enabled else "1")
        new_text = "Напоминания мастеру выключены" if enabled else "Напоминания мастеру включены"
        bot.send_message(call.message.chat.id, new_text, reply_markup=get_admin_settings_markup())
        return

    if data.startswith("admin_confirm_"):
        booking_id = int(data.split("_")[2])
        with db_lock:
            cursor.execute(
                """
                SELECT user_id, service, date, time, end_time, status
                FROM bookings
                WHERE id=?
                """,
                (booking_id,),
            )
            booking = cursor.fetchone()
            if not booking:
                bot.answer_callback_query(call.id, "Заявка не найдена")
                return
            if booking[5] != "pending":
                bot.answer_callback_query(call.id, "Заявка уже обработана")
                return
            cursor.execute(
                "UPDATE bookings SET status='confirmed' WHERE id=?",
                (booking_id,),
            )
            conn.commit()
        safe_edit_message_text(
            f"Заявка #{booking_id} подтверждена",
            call.message.chat.id,
            call.message.message_id,
        )
        lock_admin_action(
            f"booking:{booking_id}",
            user_id,
            f"Заявка #{booking_id} уже подтверждена другим админом",
        )
        bot.send_message(
            booking[0],
            (
                "Запись подтверждена.\n"
                f"{booking[1]} | {booking[2]} | {booking[3]}–{booking[4]}\n"
                f"Контакты мастера: {MASTER_CONTACT}"
            ),
            reply_markup=get_user_booking_markup(booking_id, "confirmed"),
        )
        send_profile_questionnaire_prompt(booking[0])
        return

    if data.startswith("admin_decline_menu_"):
        booking_id = int(data.split("_")[3])
        bot.send_message(
            call.message.chat.id,
            "Выберите причину отклонения",
            reply_markup=get_decline_reason_markup(booking_id),
        )
        return

    if data.startswith("admin_decline_"):
        booking_id = int(data.split("_")[2])
        reason_key = data.split("_")[3] if len(data.split("_")) > 3 else "other"
        decline_reasons = {
            "busy": "Это время уже занято.",
            "cant": "Мастер не сможет принять в выбранное время.",
            "other": "Мастер предложит выбрать другое время.",
        }
        reason_text = decline_reasons.get(reason_key, decline_reasons["other"])
        with db_lock:
            cursor.execute(
                """
                SELECT user_id, service, date, time, end_time, status
                FROM bookings
                WHERE id=?
                """,
                (booking_id,),
            )
            booking = cursor.fetchone()
            if not booking:
                bot.answer_callback_query(call.id, "Заявка не найдена")
                return
            if booking[5] != "pending":
                bot.answer_callback_query(call.id, "Заявка уже обработана")
                return
            cursor.execute(
                "UPDATE bookings SET status='declined' WHERE id=?",
                (booking_id,),
            )
            cursor.execute(
                """
                UPDATE day_slots
                SET is_booked=0
                WHERE date=? AND start_time=? AND end_time=?
                """,
                (booking[2], booking[3], booking[4]),
            )
            conn.commit()
        safe_edit_message_text(
            f"Заявка #{booking_id} отклонена",
            call.message.chat.id,
            call.message.message_id,
        )
        lock_admin_action(
            f"booking:{booking_id}",
            user_id,
            f"Заявка #{booking_id} уже отклонена другим админом",
        )
        bot.send_message(
            booking[0],
            (
                f"К сожалению, мастер не смог подтвердить эту запись.\nПричина: {reason_text}\n"
                "Вы можете выбрать другое свободное время в боте."
            ),
        )
        return

    if data.startswith("reschedule_confirm_"):
        request_id = int(data.split("_")[2])
        with db_lock:
            cursor.execute(
                """
                SELECT id, booking_id, user_id, old_date, old_time, old_end_time,
                       new_slot_id, new_date, new_time, new_end_time, status
                FROM reschedule_requests
                WHERE id=?
                """,
                (request_id,),
            )
            request = cursor.fetchone()
            if not request:
                bot.answer_callback_query(call.id, "Заявка не найдена")
                return
            if request[10] != "pending":
                bot.answer_callback_query(call.id, "Заявка уже обработана")
                return
            cursor.execute(
                """
                UPDATE bookings
                SET date=?, time=?, end_time=?, reminder_sent=0
                WHERE id=?
                """,
                (request[7], request[8], request[9], request[1]),
            )
            cursor.execute(
                """
                UPDATE day_slots
                SET is_booked=0
                WHERE date=? AND start_time=? AND end_time=?
                """,
                (request[3], request[4], request[5]),
            )
            cursor.execute(
                "UPDATE reschedule_requests SET status='confirmed' WHERE id=?",
                (request_id,),
            )
            conn.commit()
        safe_edit_message_text(
            f"Перенос #{request_id} подтвержден",
            call.message.chat.id,
            call.message.message_id,
        )
        lock_admin_action(
            f"reschedule:{request_id}",
            user_id,
            f"Перенос #{request_id} уже подтвержден другим админом",
        )
        bot.send_message(
            request[2],
            (
                "Перенос записи подтвержден.\n"
                f"Новое время: {request[7]} {request[8]}–{request[9]}\n"
                f"Контакты мастера: {MASTER_CONTACT}"
            ),
        )
        return

    if data.startswith("reschedule_decline_"):
        request_id = int(data.split("_")[2])
        with db_lock:
            cursor.execute(
                """
                SELECT user_id, new_slot_id, new_date, new_time, new_end_time, status
                FROM reschedule_requests
                WHERE id=?
                """,
                (request_id,),
            )
            request = cursor.fetchone()
            if not request:
                bot.answer_callback_query(call.id, "Заявка не найдена")
                return
            if request[5] != "pending":
                bot.answer_callback_query(call.id, "Заявка уже обработана")
                return
            cursor.execute(
                "UPDATE reschedule_requests SET status='declined' WHERE id=?",
                (request_id,),
            )
            cursor.execute(
                "UPDATE day_slots SET is_booked=0 WHERE id=?",
                (request[1],),
            )
            conn.commit()
        safe_edit_message_text(
            f"Перенос #{request_id} отклонен",
            call.message.chat.id,
            call.message.message_id,
        )
        lock_admin_action(
            f"reschedule:{request_id}",
            user_id,
            f"Перенос #{request_id} уже отклонен другим админом",
        )
        bot.send_message(
            request[0],
            (
                "Мастер не подтвердил перенос на выбранное время.\n"
                "Ваша текущая запись остается без изменений."
            ),
        )
        return

    if data == "admin_menu":
        safe_edit_message_text(
            "Админка",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=get_admin_menu(user_id),
        )
        return

    if data == "admin_admins":
        if not is_owner(user_id):
            bot.answer_callback_query(call.id, "Только владелец может управлять админами")
            return
        bot.send_message(
            call.message.chat.id,
            "Администраторы (без владельца):",
            reply_markup=get_admins_management_markup(),
        )
        return

    if data == "admin_add":
        if not is_owner(user_id):
            bot.answer_callback_query(call.id, "Только владелец может управлять админами")
            return
        admin_states[user_id] = {"flow": "admin_add_input"}
        bot.send_message(
            call.message.chat.id,
            "Пришлите @username (если пользователь уже писал боту) или числовой Telegram ID нового админа.",
        )
        return

    if data == "admin_clients":
        bot.send_message(
            call.message.chat.id,
            "Записи о клиентах",
            reply_markup=get_clients_list_markup(),
        )
        return

    if data == "client_search":
        admin_states[user_id] = {"flow": "client_search_input"}
        bot.send_message(
            call.message.chat.id,
            "Введите часть имени или @username для поиска (пустая строка — отмена).",
        )
        return

    if data.startswith("client_card_"):
        target_user_id = int(data.split("_")[2])
        render_client_card(call.message.chat.id, target_user_id)
        return

    if data.startswith("client_history_"):
        target_user_id = int(data.split("_")[2])
        render_client_history(call.message.chat.id, target_user_id)
        return

    if data.startswith("admin_remove_"):
        if not is_owner(user_id):
            bot.answer_callback_query(call.id, "Только владелец может управлять админами")
            return
        target_id = int(data.split("_")[2])
        if target_id == OWNER_ID:
            bot.answer_callback_query(call.id, "Владельца нельзя удалить")
            return
        remove_admin_record(target_id)
        bot.send_message(
            call.message.chat.id,
            f"Админ {target_id} удалён",
            reply_markup=get_admins_management_markup(),
        )
        return

    if data == "admin_bookings_calendar":
        current = datetime.date.today()
        bot.send_message(
            call.message.chat.id,
            "Календарь записей",
            reply_markup=get_admin_bookings_calendar(current.year, current.month),
        )
        return

    if data == "admin_no_bookings_day":
        bot.answer_callback_query(call.id, "На эту дату записей нет")
        return

    if data.startswith("admin_bookings_prev_") or data.startswith("admin_bookings_next_"):
        parts = data.split("_")
        year = int(parts[3])
        month = int(parts[4])
        safe_edit_message_text(
            "Календарь записей",
            call.message.chat.id,
            call.message.message_id,
            reply_markup=get_admin_bookings_calendar(year, month),
        )
        return

    if data.startswith("admin_bookings_date_"):
        date_value = data.split("_", 3)[3]
        bot.send_message(
            call.message.chat.id,
            f"Записи на {date_value}",
            reply_markup=get_admin_day_bookings_markup(date_value),
        )
        return

    if data.startswith("admin_booking_view_"):
        booking_id = int(data.split("_")[3])
        with db_lock:
            cursor.execute(
                "SELECT user_id FROM bookings WHERE id=?",
                (booking_id,),
            )
            row = cursor.fetchone()
        markup = get_admin_booking_actions_markup(booking_id, row[0]) if row else None
        bot.send_message(call.message.chat.id, format_booking_details(booking_id), reply_markup=markup)
        return

    if data.startswith("client_note_"):
        target_user_id = int(data.split("_")[2])
        with db_lock:
            cursor.execute(
                "SELECT client_name FROM bookings WHERE user_id=? ORDER BY id DESC LIMIT 1",
                (target_user_id,),
            )
            row = cursor.fetchone()
        admin_states[user_id] = {
            "flow": "client_note_input",
            "target_user_id": target_user_id,
            "client_name": row[0] if row else "Клиент",
        }
        note, _ = get_client_note(target_user_id)
        bot.send_message(
            call.message.chat.id,
            f"Текущая заметка: {note or 'нет'}\n\nВведите новую заметку:",
        )
        return

    if data.startswith("client_delete_"):
        target_user_id = int(data.rsplit("_", 1)[1])
        with db_lock:
            cursor.execute(
                "SELECT COUNT(*) FROM bookings WHERE user_id=?",
                (target_user_id,),
            )
            total_b = cursor.fetchone()[0] or 0
        if total_b > 0:
            bot.answer_callback_query(
                call.id,
                "Удалить нельзя: у клиента есть записи (включая историю).",
                show_alert=True,
            )
            return
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton("Подтвердить", callback_data=f"client_del_yes_{target_user_id}"),
            types.InlineKeyboardButton("Отмена", callback_data=f"client_del_no_{target_user_id}"),
        )
        bot.send_message(
            call.message.chat.id,
            "Удалить клиента и всю информацию о нём (заметку, анкету)?",
            reply_markup=markup,
        )
        return

    if data.startswith("client_del_yes_"):
        target_user_id = int(data.rsplit("_", 1)[1])
        with db_lock:
            cursor.execute(
                "SELECT COUNT(*) FROM bookings WHERE user_id=?",
                (target_user_id,),
            )
            total_b = cursor.fetchone()[0] or 0
        if total_b > 0:
            try:
                bot.delete_message(call.message.chat.id, call.message.message_id)
            except Exception:
                pass
            bot.answer_callback_query(
                call.id,
                "Удалить нельзя: у клиента есть записи.",
                show_alert=True,
            )
            return
        with db_lock:
            cursor.execute("DELETE FROM client_notes WHERE user_id=?", (target_user_id,))
            cursor.execute("DELETE FROM client_profiles WHERE user_id=?", (target_user_id,))
            conn.commit()
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        bot.send_message(
            call.message.chat.id,
            "Клиент удалён.",
            reply_markup=get_clients_list_markup(),
        )
        return

    if data.startswith("client_del_no_"):
        target_user_id = int(data.rsplit("_", 1)[1])
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        render_client_card(call.message.chat.id, target_user_id)
        return

    if data.startswith("master_reschedule_start_"):
        booking_id = int(data.split("_")[3])
        bot.send_message(
            call.message.chat.id,
            "Выберите новую дату, которую хотите предложить клиенту",
            reply_markup=get_master_reschedule_dates_markup(booking_id),
        )
        return

    if data.startswith("master_reschedule_date_"):
        _, _, _, booking_id, date_value = data.split("_", 4)
        booking_id = int(booking_id)
        bot.send_message(
            call.message.chat.id,
            f"Выберите новое время на {date_value}",
            reply_markup=get_master_reschedule_slots_markup(booking_id, date_value),
        )
        return

    if data.startswith("master_reschedule_slot_"):
        _, _, _, booking_id, slot_id = data.split("_")
        booking_id = int(booking_id)
        slot_id = int(slot_id)
        with db_lock:
            cursor.execute(
                """
                SELECT id, user_id, service, date, time, end_time, status
                FROM bookings
                WHERE id=?
                """,
                (booking_id,),
            )
            booking = cursor.fetchone()
            cursor.execute(
                """
                SELECT date, start_time, end_time, is_booked, is_closed
                FROM day_slots
                WHERE id=? AND is_break=0
                """,
                (slot_id,),
            )
            slot = cursor.fetchone()
            if not booking or booking[6] != "confirmed":
                bot.answer_callback_query(call.id, "Можно переносить только подтвержденную запись")
                return
            if not slot or slot[3] or slot[4] or is_less_than_24_hours(slot[0], slot[1]):
                bot.answer_callback_query(call.id, "Окно недоступно")
                return
            cursor.execute(
                """
                INSERT INTO reschedule_requests(
                    booking_id, user_id, old_date, old_time, old_end_time,
                    new_slot_id, new_date, new_time, new_end_time, status, created_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'master_pending', ?)
                """,
                (
                    booking_id,
                    booking[1],
                    booking[3],
                    booking[4],
                    booking[5],
                    slot_id,
                    slot[0],
                    slot[1],
                    slot[2],
                    datetime.datetime.now().isoformat(timespec="seconds"),
                ),
            )
            request_id = cursor.lastrowid
            cursor.execute("UPDATE day_slots SET is_booked=1 WHERE id=?", (slot_id,))
            conn.commit()
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton(
                "Принять перенос", callback_data=f"client_reschedule_accept_{request_id}"
            ),
            types.InlineKeyboardButton(
                "Оставить старое время", callback_data=f"client_reschedule_decline_{request_id}"
            ),
        )
        bot.send_message(call.message.chat.id, "Предложение переноса отправлено клиенту")
        bot.send_message(
            booking[1],
            (
                "Мастер предлагает перенести запись.\n"
                f"Было: {booking[3]} {booking[4]}–{booking[5]}\n"
                f"Новое время: {slot[0]} {slot[1]}–{slot[2]}"
            ),
            reply_markup=markup,
        )
        return

    if data == "admin_schedule":
        open_schedule_menu(call.message.chat.id)
        return

    if data == "admin_stats":
        with db_lock:
            cursor.execute("SELECT COUNT(*) FROM bookings")
            total = cursor.fetchone()[0]
            cursor.execute(
                "SELECT service, COUNT(*) FROM bookings GROUP BY service ORDER BY COUNT(*) DESC"
            )
            grouped = cursor.fetchall()
        lines = [f"Всего записей: {total}"]
        for service, count in grouped:
            lines.append(f"{service}: {count}")
        bot.send_message(call.message.chat.id, "\n".join(lines), reply_markup=get_admin_menu(user_id))
        return

    if data == "admin_close_date":
        admin_states[user_id] = {"flow": "close_date_input"}
        bot.send_message(
            call.message.chat.id,
            "Введи дату для закрытия в формате YYYY-MM-DD",
            reply_markup=next_days_markup("closequick", include_back=True, back_cb="admin_menu"),
        )
        return

    if data == "admin_clear_chat":
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        clear_chat(call.message.chat.id)
        open_admin_menu(call.message.chat.id, user_id)
        return

    if data.startswith("closequick_"):
        date_value = data.split("_", 1)[1]
        ensure_template_schedule_for_date(date_value)
        with db_lock:
            cursor.execute(
                """
                UPDATE day_slots
                SET is_closed=1
                WHERE date=? AND is_booked=0 AND is_break=0
                """,
                (date_value,),
            )
            conn.commit()
        clear_admin_state(user_id)
        bot.send_message(call.message.chat.id, f"Дата {date_value} закрыта", reply_markup=get_admin_menu(user_id))
        return

    if data == "schedule_template":
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("Отмена", callback_data="template_cancel"))
        sent = bot.send_message(
            call.message.chat.id,
            "Введи начало смены в формате HH:MM",
            reply_markup=markup,
        )
        admin_states[user_id] = {"flow": "template_start", "prompt_msg_id": sent.message_id}
        return

    if data == "template_cancel":
        state = admin_states.get(user_id, {})
        prompt_msg_id = state.get("prompt_msg_id")
        clear_admin_state(user_id)
        if prompt_msg_id:
            safe_delete_message(call.message.chat.id, prompt_msg_id)
        else:
            safe_delete_message(call.message.chat.id, call.message.message_id)
        open_schedule_menu(call.message.chat.id)
        return

    if data == "schedule_show_template":
        show_template(call.message.chat.id)
        return

    if data == "schedule_create_day":
        admin_states[user_id] = {"flow": "create_day_input"}
        bot.send_message(
            call.message.chat.id,
            "Введи дату для графика в формате YYYY-MM-DD",
            reply_markup=next_days_markup(
                "createquick",
                include_back=True,
                back_cb="admin_schedule",
                mark_booked=True,
            ),
        )
        return

    if data.startswith("createquick_"):
        date_value = data.split("_", 1)[1]
        admin_states[user_id] = {"flow": "create_day_mode", "date": date_value}
        bot.send_message(
            call.message.chat.id,
            f"Как создать график на {date_value}?",
            reply_markup=get_create_day_mode_markup(date_value),
        )
        return

    if data == "schedule_edit_day":
        admin_states[user_id] = {"flow": "edit_day_input"}
        bot.send_message(
            call.message.chat.id,
            "Введи дату для редактирования в формате YYYY-MM-DD",
            reply_markup=next_days_markup("editquick", include_back=True, back_cb="admin_schedule"),
        )
        return

    if data.startswith("editquick_"):
        date_value = data.split("_", 1)[1]
        admin_states[user_id] = {"flow": "edit_day_input", "date": date_value}
        ensure_template_schedule_for_date(date_value)
        _render_day_view(call.message.chat.id, user_id, date_value)
        return

    if data == "template_save_blocks":
        state = admin_states.get(user_id, {})
        blocks = state.get("blocks") or []
        if not blocks:
            bot.answer_callback_query(call.id, "Добавь хотя бы один блок")
            return
        shift_start = state["shift_start"]
        shift_end = state["shift_end"]
        blocks_json = json.dumps(blocks)
        set_setting("template_start", shift_start)
        set_setting("template_end", shift_end)
        set_setting("template_blocks", blocks_json)
        verify = get_template_config()
        if not verify or verify.get("blocks") != blocks:
            print(
                f"[template_save_blocks] WARN: save mismatch saved_blocks={blocks_json} verify={verify}",
                flush=True,
            )
        schedule = build_manual_timeline(shift_start, shift_end, blocks)
        builder_msg_id = state.get("builder_msg_id")
        if builder_msg_id:
            try:
                bot.delete_message(call.message.chat.id, builder_msg_id)
            except Exception:
                pass
        clear_admin_state(user_id)
        bot.send_message(
            call.message.chat.id,
            format_schedule_preview(schedule, "Шаблон сохранен"),
            reply_markup=get_template_apply_markup(),
        )
        return

    if data == "template_apply_week" or data == "template_apply_month":
        template = get_template_config()
        if not template:
            bot.answer_callback_query(call.id, "Шаблон не настроен")
            return
        days = 7 if data == "template_apply_week" else 30
        result = apply_template_to_range(template, days)
        applied_count = len(result["applied"])
        skipped = result["skipped"]
        lines = [f"Применено к {applied_count} дн."]
        if skipped:
            lines.append("Пропущены:")
            for date_value, reason in skipped[:30]:
                lines.append(f"• {date_value} — {reason}")
            if len(skipped) > 30:
                lines.append(f"…и ещё {len(skipped) - 30}")
        bot.send_message(call.message.chat.id, "\n".join(lines), reply_markup=get_schedule_menu())
        return

    if data.startswith("create_template_"):
        date_value = data.split("_", 2)[2]
        template = get_template_config()
        if not template:
            bot.send_message(call.message.chat.id, "Сначала настрой шаблон")
            return
        schedule = schedule_from_template(template)
        bot.send_message(
            call.message.chat.id,
            format_schedule_preview(schedule, f"Шаблон на {date_value}"),
            reply_markup=get_template_save_markup(date_value),
        )
        return

    if data.startswith("save_template_day_"):
        date_value = data.split("_", 3)[3]
        template = get_template_config()
        if not template:
            bot.send_message(call.message.chat.id, "Сначала настрой шаблон")
            return
        schedule = schedule_from_template(template)
        ok, text = save_schedule_to_day(date_value, schedule, "template")
        bot.send_message(call.message.chat.id, text, reply_markup=get_schedule_menu())
        return

    if data.startswith("create_manual_"):
        date_value = data.split("_", 2)[2]
        admin_states[user_id] = {"flow": "manual_shift_start", "date": date_value}
        bot.send_message(
            call.message.chat.id,
            f"Собираем день {date_value}. Введи начало смены в формате HH:MM",
        )
        return

    if data.startswith("manual_work_"):
        duration = int(data.split("_")[2])
        state = admin_states[user_id]
        state["blocks"].append({"type": "work", "duration": duration})
        render_manual_builder(call.message.chat.id, user_id)
        return

    if data.startswith("manual_break_"):
        duration = int(data.split("_")[2])
        state = admin_states[user_id]
        state["blocks"].append({"type": "break", "duration": duration})
        render_manual_builder(call.message.chat.id, user_id)
        return

    if data == "manual_undo":
        state = admin_states[user_id]
        if state["blocks"]:
            state["blocks"].pop()
        render_manual_builder(call.message.chat.id, user_id)
        return

    if data == "manual_remove_menu":
        blocks = admin_states[user_id]["blocks"]
        if not blocks:
            render_manual_builder(call.message.chat.id, user_id, "Удалять пока нечего")
            return
        render_manual_builder(
            call.message.chat.id,
            user_id,
            "Выбери блок для удаления",
            markup=get_manual_remove_markup(blocks),
        )
        return

    if data.startswith("manual_remove_"):
        index = int(data.split("_")[2])
        state = admin_states[user_id]
        if 0 <= index < len(state["blocks"]):
            state["blocks"].pop(index)
        render_manual_builder(call.message.chat.id, user_id)
        return

    if data == "manual_continue":
        render_manual_builder(call.message.chat.id, user_id)
        return

    if data == "manual_reset":
        state = admin_states[user_id]
        state["blocks"] = []
        render_manual_builder(call.message.chat.id, user_id, "График сброшен")
        return

    if data == "manual_save":
        state = admin_states[user_id]
        schedule = build_manual_timeline(
            state["shift_start"], state["shift_end"], state["blocks"]
        )
        if not schedule["slots"]:
            render_manual_builder(call.message.chat.id, user_id, "Добавь хотя бы один блок")
            return
        if schedule["unused_minutes"] or schedule["overflow_minutes"]:
            render_manual_builder(
                call.message.chat.id,
                user_id,
                "Есть несостыковка со сменой",
                markup=get_manual_force_markup(),
            )
            return
        ok, text, _ = save_manual_schedule(user_id)
        builder_msg_id = state.get("builder_msg_id")
        if builder_msg_id:
            try:
                bot.delete_message(call.message.chat.id, builder_msg_id)
            except Exception:
                pass
        clear_admin_state(user_id)
        bot.send_message(call.message.chat.id, text, reply_markup=get_schedule_menu())
        return

    if data == "manual_force":
        state = admin_states.get(user_id, {})
        ok, text, schedule = save_manual_schedule(user_id)
        builder_msg_id = state.get("builder_msg_id")
        if builder_msg_id:
            try:
                bot.delete_message(call.message.chat.id, builder_msg_id)
            except Exception:
                pass
        if ok:
            text = f"{text}\n{format_schedule_preview(schedule, 'Подтверждено как есть')}"
        clear_admin_state(user_id)
        bot.send_message(call.message.chat.id, text, reply_markup=get_schedule_menu())
        return

    if data == "edit_add_slot":
        state = admin_states.setdefault(user_id, {})
        if "date" not in state:
            bot.send_message(call.message.chat.id, "Сначала выбери дату")
            return
        state["flow"] = "edit_add_slot"
        _delete_msg_safe(call.message.chat.id, state.pop("day_prompt_msg_id", None))
        sent = bot.send_message(call.message.chat.id, "Введи новое окно как HH:MM-HH:MM")
        state["day_prompt_msg_id"] = sent.message_id
        return

    if data == "replace_confirm":
        state = admin_states.get(user_id, {})
        pending = state.pop("pending_replace", None)
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        if not isinstance(pending, dict):
            bot.answer_callback_query(call.id, "Нет ожидающей замены")
            return
        delete_slots_by_ids(pending["ids"])
        insert_slot(pending["date"], pending["start"], pending["end"], pending["kind"])
        _render_day_view(call.message.chat.id, user_id, pending["date"])
        return

    if data == "replace_cancel":
        state = admin_states.get(user_id, {})
        pending = state.pop("pending_replace", None)
        try:
            bot.delete_message(call.message.chat.id, call.message.message_id)
        except Exception:
            pass
        if isinstance(pending, dict):
            _render_day_view(call.message.chat.id, user_id, pending["date"])
        return

    if data == "edit_add_break":
        state = admin_states.setdefault(user_id, {})
        if "date" not in state:
            bot.send_message(call.message.chat.id, "Сначала выбери дату")
            return
        state["flow"] = "edit_add_break"
        _delete_msg_safe(call.message.chat.id, state.pop("day_prompt_msg_id", None))
        sent = bot.send_message(call.message.chat.id, "Введи служебное окно как HH:MM-HH:MM")
        state["day_prompt_msg_id"] = sent.message_id
        return

    if data.startswith("edit_toggle_"):
        slot_id = int(data.split("_")[2])
        with db_lock:
            cursor.execute(
                "SELECT date, is_closed, is_booked FROM day_slots WHERE id=?",
                (slot_id,),
            )
            row = cursor.fetchone()
            if not row or row[2]:
                bot.answer_callback_query(call.id, "Это окно нельзя изменить")
                return
            cursor.execute(
                "UPDATE day_slots SET is_closed=? WHERE id=?",
                (0 if row[1] else 1, slot_id),
            )
            conn.commit()
        _render_day_view(call.message.chat.id, user_id, row[0])
        return

    if data.startswith("edit_delete_"):
        slot_id = int(data.split("_")[2])
        with db_lock:
            cursor.execute(
                "SELECT date, is_booked FROM day_slots WHERE id=?",
                (slot_id,),
            )
            row = cursor.fetchone()
            if not row or row[1]:
                bot.answer_callback_query(call.id, "Занятое окно удалить нельзя")
                return
            cursor.execute("DELETE FROM day_slots WHERE id=?", (slot_id,))
            conn.commit()
        _render_day_view(call.message.chat.id, user_id, row[0])
        return


def reminder_loop():
    last_cleanup_month = None
    while True:
        today = datetime.date.today()
        current_month = (today.year, today.month)
        if last_cleanup_month != current_month:
            if last_cleanup_month is not None:
                try:
                    cleanup_old_day_slots(today.isoformat())
                except Exception:
                    pass
            last_cleanup_month = current_month

        with db_lock:
            cursor.execute(
                """
                SELECT id, user_id, service, date, time
                FROM bookings
                WHERE reminder_sent=0 AND status='confirmed'
                """
            )
            rows = cursor.fetchall()
            cursor.execute(
                """
                SELECT id, user_id, service, date, time, end_time, client_name, contact
                FROM bookings
                WHERE master_reminder_sent=0 AND status='confirmed'
                """
            )
            master_rows = cursor.fetchall()

        now = datetime.datetime.now()
        for booking_id, user_id, service, date_value, time_value in rows:
            booking_time = datetime.datetime.strptime(
                f"{date_value} {time_value}", "%Y-%m-%d %H:%M"
            )
            delta = booking_time - now
            if 0 < delta.total_seconds() <= 3600:
                bot.send_message(
                    user_id,
                    f"Напоминание: {service} сегодня в {time_value}",
                )
                with db_lock:
                    cursor.execute(
                        "UPDATE bookings SET reminder_sent=1 WHERE id=?",
                        (booking_id,),
                    )
                    conn.commit()
        if get_setting("master_reminders_enabled", "1") == "1":
            for booking_id, user_id, service, date_value, time_value, end_time, client_name, contact in master_rows:
                booking_time = datetime.datetime.strptime(
                    f"{date_value} {time_value}", "%Y-%m-%d %H:%M"
                )
                delta = booking_time - now
                if 0 < delta.total_seconds() <= 900:
                    notify_all_admins(
                        (
                            "Через 15 минут клиент.\n"
                            f"{client_name or user_id} | {service}\n"
                            f"{date_value} {time_value}–{end_time}\n"
                            f"Контакт: {contact}"
                        ),
                    )
                    with db_lock:
                        cursor.execute(
                            "UPDATE bookings SET master_reminder_sent=1 WHERE id=?",
                            (booking_id,),
                        )
                        conn.commit()
        time.sleep(30)


@app.route("/bookings")
def api_bookings():
    with db_lock:
        cursor.execute(
            """
            SELECT id, service, date, time, end_time, contact, status, client_name, client_username
            FROM bookings
            ORDER BY date, time
            """
        )
        rows = cursor.fetchall()
    return jsonify(rows)


@app.route("/schedule/<date_value>")
def api_schedule(date_value):
    return jsonify(get_day_slots(date_value))


def run_flask():
    app.run(port=5000, use_reloader=False)


init_db()
ensure_default_settings()
ensure_portfolio_placeholders()
threading.Thread(target=reminder_loop, daemon=True).start()
threading.Thread(target=run_flask, daemon=True).start()
bot.infinity_polling(skip_pending=True, timeout=20, long_polling_timeout=30)
