import telebot
from telebot import types

import json
import os
import re
import secrets
import sqlite3
import string
import sys
import time
import logging
from datetime import datetime, timedelta
from threading import Lock
from typing import Optional, List, Tuple, Dict

# ========================================
# КОНФИГУРАЦИЯ
# ========================================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN environment variable is required")

PAYMENT_REQUISITES = """🟢 Сбербанк 🟢
+79085545373
Александр Валерьевич Ш."""

DATA_FILE = os.path.join(BASE_DIR, "users_data.json")
DATABASE_FILE = os.path.join(BASE_DIR, "bot_database.db")
KEYS_FOLDER = os.path.join(BASE_DIR, "keys")
LOG_FILE = os.path.join(BASE_DIR, "bot.log")

TICKETS_PER_PAGE = 5
ADMIN_SESSION_DAYS = 4

bot = telebot.TeleBot(BOT_TOKEN, skip_pending=True)

# ========================================
# ЛОГИРОВАНИЕ
# ========================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ========================================
# ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ
# ========================================
user_languages: Dict[int, str] = {}
user_states: Dict = {}
db_connection: Optional[sqlite3.Connection] = None
deposit_context: Dict[int, int] = {}

db_lock = Lock()
storage_lock = Lock()
key_lock = Lock()
purchase_lock = Lock()

# ========================================
# ПРОДУКТЫ И ПЕРИОДЫ
# ========================================
PRODUCTS = {
    "primehack": {
        "name": "🔷 PRIMEHACK",
        "prices": {"1d": 10, "3d": 20, "7d": 40, "14d": 65, "30d": 90}
    },
    "zolo": {
        "name": "🔷 ZOLO",
        "prices": {"1d": 85, "3d": 180, "7d": 280, "14d": 400, "30d": 800}
    },
    "dexo": {
        "name": "🔷 DEXO",
        "prices": {"1d": 75, "3d": 100, "7d": 160, "14d": 320, "30d": 450}
    }
}

CATEGORIES = {
    "short": {"name": "♦️ ⏱ Краткосрочные подписки", "periods": ["1d", "3d"]},
    "mid": {"name": "♦️ 📅 Среднесрочные подписки", "periods": ["7d", "14d"]},
    "long": {"name": "♦️ 📆 Долгосрочные подписки", "periods": ["30d"]}
}

PERIOD_DISPLAY = {
    "1d": "1 DAY",
    "3d": "3 DAYS",
    "7d": "7 DAYS",
    "14d": "14 DAYS",
    "30d": "30 DAYS"
}

# KEY_FILES: "product_period" -> absolute path
KEY_FILES: Dict[str, str] = {}

WARNING_LEVELS = {
    1: ("Предупреждение", "Warning"),
    2: ("Выговор", "Reprimand"),
    3: ("Строгий выговор", "Strict Reprimand")
}

TICKET_STATUSES = {
    "open": {"ru": "🔓 Открыт", "en": "🔓 Open"},
    "in_progress": {"ru": "⏳ В работе", "en": "⏳ In progress"},
    "answered": {"ru": "✅ Ответ дан", "en": "✅ Answered"},
    "closed": {"ru": "🔒 Закрыт", "en": "🔒 Closed"}
}

# ========================================
# ТЕКСТЫ ИНТЕРФЕЙСА
# ========================================
LANGUAGES = {
    "ru": {
        "welcome": "👋 Привет! Выберите раздел:",
        "settings": "⚙️ Настройки",
        "support": "🆘 Поддержка",
        "products": "♦️ КАТАЛОГ ТОВАРОВ",
        "profile": "👤 Профиль",
        "back_menu": "🔙 Назад в меню",
        "language": "🏳️ Язык",
        "top_up_balance": "💳 Пополнить баланс",
        "admin_panel": "🛠️ Админ-панель",
        "admin_menu_title": "<b>🛠️ АДМИН-ПАНЕЛЬ</b>",
        "purchase_success": "✅ <b>ПОКУПКА УСПЕШНА!</b>\n\n🔑 Товар: {}\n🔢 Кол-во: {}\n💵 Списано: {} ₽\n💰 Остаток: {} ₽\n\n🔐 Ваши ключи:\n{}",
        "insufficient_funds": "❌ <b>НЕДОСТАТОЧНО СРЕДСТВ!</b>\n\n💰 Ваш баланс: {} ₽\n💵 Требуется: {} ₽\n📉 Не хватает: {} ₽\n\n<b>Пополните баланс:</b>",
        "no_keys_available": "❌ Недостаточно ключей на складе!",
        "unknown_command": "❓ Используйте кнопки.",
        "balance_topup_notification": "✅ <b>Баланс пополнен!</b>\n\n👤 Пользователь: {}\n💰 Добавлено: {} ₽\n💳 Новый баланс: {} ₽",
        "limited_access": "❌ Только Супер-Админы могут это делать.",
        "enter_admin_password": "🔐 <b>Требуется пароль администратора!</b>\n\nВведите пароль для доступа к панели:",
        "password_correct": "✅ Пароль верный! Доступ разрешён.",
        "password_incorrect": "❌ Неверный пароль! Попробуйте снова.",
        "password_set": "✅ Пароль администратора установлен.",
        "no_password_set": "⚠️ Пароль ещё не установлен. Установите через /set_admin_password",
        "admin_panel_blocked": "🚫 <b>Доступ закрыт!</b>\n\nВведённый пароль неверен.",
        "select_qty_title": "<b>Выберите количество ключей:</b>",
        "confirm_title": "<b>ПОДТВЕРЖДЕНИЕ ПОКУПКИ</b>",
        "confirm_text": "🛍 Товар: {}\n🔢 Количество: {} шт.\n💵 Общая сумма: {} ₽\n\nНажмите кнопку ниже для подтверждения:",
        "rus": "🇷🇺 Русский",
        "eng": "🇬🇧 Английский",
        "current_lang": "🌐 Текущий язык: {}",
        "settings_content": "⚙️ Настройки:",
        "give_warning": "⚖️ Выдать предупреждение",
        "deposit_wait_amount": "💰 <b>Отправьте сумму пополнения:</b>\n\nПример: <code>1000</code>\nДля отмены: /cancel",
        "deposit_received_screenshot": "📸 <b>Скриншот получен!</b>\nЗаявка №{} ожидает проверки.",
        "deposit_new_request_notify": "🔔 <b>НОВАЯ ЗАЯВКА #{}</b>\n\n👤 ID: {}\n💵 Сумма: {} ₽\n⏰ Время: {}",
        "confirm_deposit_action": "✅ Подтвердить заявку",
        "reject_deposit_action": "❌ Отклонить заявку",
        "deposit_confirmed_msg": "✅ <b>Пополнение подтверждено!</b>\n\n💰 Сумма: {} ₽\n💰 Новый баланс: {} ₽",
        "deposit_rejected_msg": "❌ <b>Пополнение отклонено.</b>\n\nПричина: {}",
        "check_deposits_btn": "📋 Проверить заявки",
        "ticket_created": "✅ <b>Тикет #{} успешно создан!</b>\n\nОжидайте ответа администратора, вы получите уведомление.",
        "new_ticket_admin_notify": "🔔 <b>НОВЫЙ ТИКЕТ #{}</b>\n\n👤 Пользователь ID: {}\n✍️ Текст проблемы:\n{}\n⏰ Время создания: {}",
        "ticket_empty_message": "❌ Ошибка! Используйте формат:\n<code>/ticket Описание вашей проблемы</code>",
        "ticket_not_found": "❌ Тикет с указанным ID не найден.",
        "ticket_info_header": "📋 <b>ИНФОРМАЦИЯ ПО ТИКЕТУ #{}</b>\n\n👤 Пользователь ID: {}\n📅 Создан: {}\n🚦 Статус: {}\n\n✍️ Проблема пользователя:\n{}\n\n💬 Последний ответ администратора:\n{}",
        "ticket_awaiting_reply": "💬 Отправьте текст ответа для этого тикета, или введите /cancel для отмены.",
        "ticket_reply_sent_admin": "✅ Ответ на тикет #{} успешно отправлен пользователю.",
        "ticket_new_reply_user": "📩 <b>ВАМ ПРИШЁЛ ОТВЕТ НА ТИКЕТ #{}</b>\n\n💬 Ответ администратора:\n{}",
        "ticket_list_title": "📋 <b>СПИСОК ОТКРЫТЫХ ТИКЕТОВ</b>\n\nСтраница {} из {}\n",
        "ticket_no_open": "ℹ️ Нет открытых тикетов.",
        "ticket_view_list": "📋 Просмотреть доступные тикеты",
        "support_page_text": "🆘 <b>ПОДДЕРЖКА</b>\n\n✅ Для связи с администрацией отправьте команду:\n<code>/ticket Описание вашей проблемы</code>",
        "broadcast": "📢 Рассылка",
        "broadcast_prompt": "📢 Отправьте текст сообщения для рассылки всем пользователям.\n\nДля отмены: /cancel",
        "broadcast_confirm": "📢 Вы уверены что хотите отправить это сообщение всем пользователям?",
        "broadcast_started": "✅ Рассылка началась! Сообщение будет отправлено всем пользователям.",
        "broadcast_completed": "✅ Рассылка завершена! Отправлено сообщений: {}",
        "access_denied": "⛔️ Доступ к боту ограничен.\n\nПожалуйста, свяжитесь с администратором или используйте команду /login для доступа.",
        "password_used_success": "✅ Доступ открыт! Используйте /start",
        "password_invalid": "❌ Неверный или уже использованный пароль.",
        "enter_access_password": "🔐 Введите пароль для доступа:",
        "my_tickets": "📋 Мои тикеты",
        "give_access": "🔓 Выдать доступ",
        "add_user": "👤 Добавить пользователя",
        "generate_password": "🔑 Пароль",
        "single_use": "📱 Единоразовый",
        "multi_use": "👥 Многоразовый",
        "enter_user_id": "👤 <b>Введите ID или @username пользователя:</b>\n\nДля отмены: /cancel",
        "access_granted": "✅ Доступ открыт! Используйте /start",
        "access_granted_admin": "✅ Доступ выдан пользователю {}",
        "user_not_found": "❌ Пользователь не найден. Проверьте ID или username.",
        "password_generated": "🔒 <b>Пароль сгенерирован:</b>\n\n<code>{}</code>\n\n⚡ Тип: {}\n👥 Максимум использований: {}",
        "select_max_uses": "👥 <b>Выберите максимальное количество использований:</b>",
        "choose_product": "Выберите товар:",
        "choose_category": "Выберите категорию:",
        "choose_subscription": "Выберите подписку:"
    },
    "en": {
        "welcome": "👋 Hello! Choose section:",
        "settings": "⚙️ Settings",
        "support": "🆘 Support",
        "products": "♦️ PRODUCT CATALOG",
        "profile": "👤 Profile",
        "back_menu": "🔙 Back to menu",
        "language": "🏳️ Language",
        "top_up_balance": "💳 Top up balance",
        "admin_panel": "🛠️ Admin Panel",
        "admin_menu_title": "<b>🛠️ ADMIN PANEL</b>",
        "purchase_success": "✅ <b>PURCHASE SUCCESSFUL!</b>\n\n🔑 Product: {}\n🔢 Qty: {}\n💵 Charged: {} ₽\n💰 Remaining: {} ₽\n\n🔐 Your keys:\n{}",
        "insufficient_funds": "❌ <b>INSUFFICIENT FUNDS!</b>\n\n💰 Your balance: {} ₽\n💵 Required: {} ₽\n📉 Missing: {} ₽\n\n<b>Top up balance:</b>",
        "no_keys_available": "❌ Not enough keys in stock!",
        "unknown_command": "❓ Please use buttons.",
        "balance_topup_notification": "✅ <b>Balance topped up!</b>\n\n👤 User: {}\n💰 Added: {} ₽\n💳 New balance: {} ₽",
        "limited_access": "❌ Only Super-Admins allowed.",
        "enter_admin_password": "🔐 <b>Admin password required!</b>\n\nEnter password for access:",
        "password_correct": "✅ Password correct! Access granted.",
        "password_incorrect": "❌ Wrong password! Try again.",
        "password_set": "✅ Admin password set.",
        "no_password_set": "⚠️ No password set. Use /set_admin_password",
        "admin_panel_blocked": "🚫 <b>Access Denied!</b>\n\nWrong password.",
        "select_qty_title": "<b>Select number of keys:</b>",
        "confirm_title": "<b>CONFIRM PURCHASE</b>",
        "confirm_text": "🛍 Product: {}\n🔢 Quantity: {} pcs.\n💵 Total price: {} ₽\n\nPress button to confirm:",
        "rus": "🇷🇺 Russian",
        "eng": "🇬🇧 English",
        "current_lang": "🌐 Current language: {}",
        "settings_content": "⚙️ Settings:",
        "give_warning": "⚖️ Issue Warning",
        "deposit_wait_amount": "💰 <b>Enter deposit amount:</b>\n\nExample: <code>1000</code>\nTo cancel: /cancel",
        "deposit_received_screenshot": "📸 <b>Screenshot received!</b>\nRequest #{} awaiting review.",
        "deposit_new_request_notify": "🔔 <b>NEW REQUEST #{}</b>\n\n👤 User ID: {}\n💵 Amount: {} ₽\n⏰ Time: {}",
        "confirm_deposit_action": "✅ Confirm request",
        "reject_deposit_action": "❌ Reject request",
        "deposit_confirmed_msg": "✅ <b>Deposit confirmed!</b>\n\n💰 Amount: {} ₽\n💰 New balance: {} ₽",
        "deposit_rejected_msg": "❌ <b>Deposit rejected.</b>\n\nReason: {}",
        "check_deposits_btn": "📋 Check requests",
        "ticket_created": "✅ <b>Ticket #{} created!</b>\n\nWait for admin reply, you will get notification.",
        "new_ticket_admin_notify": "🔔 <b>NEW TICKET #{}</b>\n\n👤 User ID: {}\n✍️ Problem text:\n{}\n⏰ Created at: {}",
        "ticket_empty_message": "❌ Error! Use format:\n<code>/ticket Describe your problem</code>",
        "ticket_not_found": "❌ Ticket with selected ID not found.",
        "ticket_info_header": "📋 <b>TICKET INFO #{}</b>\n\n👤 User ID: {}\n📅 Created: {}\n🚦 Status: {}\n\n✍️ User problem:\n{}\n\n💬 Last admin reply:\n{}",
        "ticket_awaiting_reply": "💬 Send reply text for this ticket, or type /cancel to abort.",
        "ticket_reply_sent_admin": "✅ Reply to ticket #{} sent to user.",
        "ticket_new_reply_user": "📩 <b>YOU GOT REPLY FOR TICKET #{}</b>\n\n💬 Admin reply:\n{}",
        "ticket_list_title": "📋 <b>OPEN TICKETS LIST</b>\n\nPage {} of {}\n",
        "ticket_no_open": "ℹ️ No open tickets.",
        "ticket_view_list": "📋 View open tickets",
        "support_page_text": "🆘 <b>SUPPORT</b>\n\n✅ To contact admin send command:\n<code>/ticket Describe your problem</code>",
        "broadcast": "📢 Broadcast",
        "broadcast_prompt": "📢 Send the message you want to broadcast to all users.\n\nTo cancel: /cancel",
        "broadcast_confirm": "📢 Are you sure you want to send this message to all users?",
        "broadcast_started": "✅ Broadcast started! Message will be sent to all users.",
        "broadcast_completed": "✅ Broadcast completed! Messages sent: {}",
        "access_denied": "⛔️ Access to the bot is limited.\n\nPlease contact the administrator or use /login command for access.",
        "password_used_success": "✅ Access granted! Use /start",
        "password_invalid": "❌ Invalid or already used password.",
        "enter_access_password": "🔐 Enter password to access the bot:",
        "my_tickets": "📋 My Tickets",
        "give_access": "🔓 Grant Access",
        "add_user": "👤 Add User",
        "generate_password": "🔑 Password",
        "single_use": "📱 Single-use",
        "multi_use": "👥 Multi-use",
        "enter_user_id": "👤 <b>Enter user ID or @username:</b>\n\nTo cancel: /cancel",
        "access_granted": "✅ Access granted! Use /start",
        "access_granted_admin": "✅ Access granted to user {}",
        "user_not_found": "❌ User not found. Check ID or username.",
        "password_generated": "🔒 <b>Password generated:</b>\n\n<code>{}</code>\n\n⚡ Type: {}\n👥 Max uses: {}",
        "select_max_uses": "👥 <b>Select maximum number of uses:</b>",
        "choose_product": "Choose product:",
        "choose_category": "Choose category:",
        "choose_subscription": "Choose subscription:"
    }
}

# ========================================
# УТИЛИТЫ
# ========================================
def safe_text(value) -> str:
    """Защита текста от .format() и вставки в HTML-строки."""
    return str(value).replace("{", "{{").replace("}", "}}")


def format_timestamp(timestamp=None) -> str:
    if timestamp is None:
        return datetime.now().strftime("%d.%m.%Y %H:%M")
    if isinstance(timestamp, datetime):
        return timestamp.strftime("%d.%m.%Y %H:%M")
    if isinstance(timestamp, str):
        try:
            dt = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
            return dt.strftime("%d.%m.%Y %H:%M")
        except Exception:
            for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S"):
                try:
                    return datetime.strptime(timestamp, fmt).strftime("%d.%m.%Y %H:%M")
                except Exception:
                    pass
    return "N/A"


def get_lang(user_id: int) -> str:
    return user_languages.get(user_id, "ru")


def format_balance(amount) -> str:
    try:
        return "{:,.0f}".format(float(amount)).replace(",", " ")
    except (ValueError, TypeError):
        return "0"


def generate_password(length=16) -> str:
    alphabet = string.ascii_letters + string.digits + "!@#%^&()-=+"
    while True:
        pwd = "".join(secrets.choice(alphabet) for _ in range(length))
        if (
            any(c.islower() for c in pwd)
            and any(c.isupper() for c in pwd)
            and any(c.isdigit() for c in pwd)
            and any(c in "!@#%^&()-=+" for c in pwd)
        ):
            return pwd


def normalize_btn_text(s: str) -> str:
    if not s:
        return ""
    s = s.strip()
    s = re.sub(r"^[^\wА-Яа-я]+", "", s, flags=re.UNICODE)
    s = re.sub(r"\s+", " ", s)
    return s.lower()


def btn_equals(user_text: str, template_text: str) -> bool:
    if not user_text or not template_text:
        return False
    return (user_text.strip() == template_text.strip()) or (
        normalize_btn_text(user_text) == normalize_btn_text(template_text)
    )


def clear_user_state(user_id: int):
    user_states.pop(user_id, None)
    user_states.pop(f"broadcast_{user_id}", None)


# ========================================
# БАЗА ДАННЫХ
# ========================================
def get_db_connection() -> sqlite3.Connection:
    global db_connection
    if db_connection is None:
        db_connection = sqlite3.connect(
            DATABASE_FILE,
            check_same_thread=False,
            isolation_level=None
        )
        db_connection.execute("PRAGMA journal_mode = WAL")
        db_connection.execute("PRAGMA synchronous = NORMAL")
        db_connection.execute("PRAGMA foreign_keys = ON")
        logger.info(f"✅ Подключение к БД установлено: {DATABASE_FILE}")
    return db_connection


def init_database():
    global db_connection
    try:
        db_connection = get_db_connection()
        cursor = db_connection.cursor()

        tables = [
            """CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                balance REAL DEFAULT 0.0,
                strict_warning_active BOOLEAN DEFAULT FALSE,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )""",
            """CREATE TABLE IF NOT EXISTS warnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                stage INTEGER NOT NULL CHECK (stage IN (1, 2, 3)),
                reason TEXT NOT NULL,
                issued_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                admin_id INTEGER NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )""",
            """CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                type TEXT NOT NULL,
                related_warning_id INTEGER,
                executed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE,
                FOREIGN KEY (related_warning_id) REFERENCES warnings (id) ON DELETE SET NULL
            )""",
            """CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                message TEXT NOT NULL,
                status TEXT DEFAULT 'open',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                admin_response TEXT DEFAULT NULL,
                updated_at DATETIME DEFAULT NULL,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )""",
            """CREATE TABLE IF NOT EXISTS ticket_attachments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ticket_id INTEGER NOT NULL,
                file_id TEXT NOT NULL,
                file_type TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (ticket_id) REFERENCES tickets(id) ON DELETE CASCADE
            )""",
            """CREATE TABLE IF NOT EXISTS deposits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                screenshot_file_id TEXT,
                status TEXT DEFAULT 'pending',
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                admin_response TEXT DEFAULT NULL,
                processed_by INTEGER DEFAULT NULL,
                processed_at DATETIME DEFAULT NULL,
                FOREIGN KEY(user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )""",
            """CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY,
                role TEXT NOT NULL CHECK (role IN ('super', 'regular')),
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            )""",
            """CREATE TABLE IF NOT EXISTS admin_sessions (
                user_id INTEGER PRIMARY KEY,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                expires_at DATETIME NOT NULL,
                FOREIGN KEY (user_id) REFERENCES admins (user_id) ON DELETE CASCADE
            )""",
            """CREATE TABLE IF NOT EXISTS system_settings (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )""",
            """CREATE TABLE IF NOT EXISTS access_grants (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                granted_by INTEGER NOT NULL,
                granted_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE,
                FOREIGN KEY (granted_by) REFERENCES users (user_id) ON DELETE CASCADE,
                UNIQUE(user_id)
            )""",
            """CREATE TABLE IF NOT EXISTS access_passwords (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                password TEXT UNIQUE NOT NULL,
                type TEXT NOT NULL CHECK (type IN ('single', 'multi')),
                max_uses INTEGER NOT NULL,
                current_uses INTEGER DEFAULT 0,
                created_by INTEGER,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                is_active BOOLEAN DEFAULT 1,
                FOREIGN KEY (created_by) REFERENCES admins (user_id) ON DELETE SET NULL
            )""",
            """CREATE TABLE IF NOT EXISTS password_uses (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                password_id INTEGER,
                user_id INTEGER,
                used_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (password_id) REFERENCES access_passwords(id) ON DELETE CASCADE,
                FOREIGN KEY (user_id) REFERENCES users (user_id) ON DELETE CASCADE
            )"""
        ]

        for sql in tables:
            cursor.execute(sql)

        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_users_username ON users(username)",
            "CREATE INDEX IF NOT EXISTS idx_warnings_user_id ON warnings(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_tickets_user_id ON tickets(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_tickets_status ON tickets(status)",
            "CREATE INDEX IF NOT EXISTS idx_deposits_status ON deposits(status)",
            "CREATE INDEX IF NOT EXISTS idx_access_grants_user_id ON access_grants(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_password_uses_user_id ON password_uses(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_password_uses_password_id ON password_uses(password_id)",
        ]
        for sql in indexes:
            cursor.execute(sql)

        db_connection.commit()
        logger.info(f"✅ База данных инициализирована: {DATABASE_FILE}")

    except sqlite3.Error as e:
        logger.error(f"❌ Критическая ошибка инициализации БД: {e}")
        raise


def raw_get_user_row(user_id: int) -> Optional[Tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    return cursor.fetchone()


def sync_user_to_json_from_db(user_id: int):
    with storage_lock:
        row = raw_get_user_row(user_id)
        if not row:
            return

        data = load_users_data()
        uid = str(user_id)
        existing = data.get(uid, {})
        data[uid] = {
            "balance": float(row[4] or 0.0),
            "username": row[1],
            "first_name": row[2],
            "last_name": row[3],
            "purchases": existing.get("purchases", []),
            "created_at": existing.get("created_at", row[6] if len(row) > 6 else datetime.now().isoformat())
        }
        save_users_data(data)


def sync_all_users_json_from_db():
    with storage_lock:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT user_id, username, first_name, last_name, balance, created_at FROM users")
        rows = cursor.fetchall()

        data = load_users_data()
        for row in rows:
            uid = str(row[0])
            existing = data.get(uid, {})
            data[uid] = {
                "balance": float(row[4] or 0.0),
                "username": row[1],
                "first_name": row[2],
                "last_name": row[3],
                "purchases": existing.get("purchases", []),
                "created_at": existing.get("created_at", row[5] if len(row) > 5 else datetime.now().isoformat())
            }
        save_users_data(data)


def restore_state_from_storage():
    """
    Если БД пустая/частично пустая — подтягиваем пользователей из JSON.
    После этого пересобираем JSON как резервную копию из БД.
    """
    with storage_lock:
        json_data = load_users_data()
        if json_data:
            conn = get_db_connection()
            cursor = conn.cursor()
            for uid_str, info in json_data.items():
                if not str(uid_str).isdigit():
                    continue
                user_id = int(uid_str)

                cursor.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
                exists = cursor.fetchone() is not None
                if exists:
                    continue

                balance = float(info.get("balance", 0.0) or 0.0)
                username = info.get("username")
                first_name = info.get("first_name")
                last_name = info.get("last_name")
                created_at = info.get("created_at") or datetime.now().isoformat()

                cursor.execute("""
                    INSERT INTO users (user_id, username, first_name, last_name, balance, created_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (user_id, username, first_name, last_name, balance, created_at))
            conn.commit()

        sync_all_users_json_from_db()


# ========================================
# ДЕКОРАТОР БЕЗОПАСНЫХ ОПЕРАЦИЙ
# ========================================
def safe_db_operation(func):
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except sqlite3.Error as e:
            logger.error(f"Ошибка БД в {func.__name__}: {e}")
            return None
        except Exception as e:
            logger.error(f"Неожиданная ошибка в {func.__name__}: {e}")
            return None
    return wrapper


# ========================================
# ПОЛЬЗОВАТЕЛИ / ДОСТУП
# ========================================
@safe_db_operation
def get_user_from_db(user_id: int) -> Optional[Tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user = cursor.fetchone()
    if not user:
        cursor.execute("""
            INSERT OR IGNORE INTO users (user_id, username, first_name, last_name, balance)
            VALUES (?, ?, ?, ?, 0.0)
        """, (user_id, None, None, None))
        conn.commit()
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        user = cursor.fetchone()
    return user


@safe_db_operation
def update_user_info(user_id: int, first_name: str, last_name: str, username: str) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()

    existing = raw_get_user_row(user_id)
    existed_before = existing is not None

    balance = float(existing[4] if existing else 0.0)
    strict_warning_active = int(existing[5] if existing else 0)
    created_at = existing[6] if existing and len(existing) > 6 else datetime.now().isoformat()

    cursor.execute("""
        INSERT OR IGNORE INTO users (user_id, username, first_name, last_name, balance, strict_warning_active, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (user_id, username, first_name, last_name, balance, strict_warning_active, created_at))

    cursor.execute("""
        UPDATE users
        SET username = ?, first_name = ?, last_name = ?
        WHERE user_id = ?
    """, (username, first_name, last_name, user_id))
    conn.commit()

    with storage_lock:
        data = load_users_data()
        uid = str(user_id)
        if uid not in data:
            data[uid] = {
                "balance": balance,
                "username": username,
                "first_name": first_name,
                "last_name": last_name,
                "purchases": [],
                "created_at": created_at
            }
        else:
            data[uid]["username"] = username
            data[uid]["first_name"] = first_name
            data[uid]["last_name"] = last_name
            data[uid].setdefault("balance", balance)
            data[uid].setdefault("purchases", [])
            data[uid].setdefault("created_at", created_at)
        save_users_data(data)

    if not existed_before:
        log_new_user(user_id, first_name, last_name, username)
        notify_admins_about_new_user(user_id, first_name, last_name, username)

    return True


@safe_db_operation
def update_user_balance_db(user_id: int, new_balance: float) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR IGNORE INTO users (user_id, balance)
        VALUES (?, 0.0)
    """, (user_id,))
    cursor.execute("""
        UPDATE users
        SET balance = ?
        WHERE user_id = ?
    """, (new_balance, user_id))
    conn.commit()
    return True


@safe_db_operation
def is_user_access_granted(user_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM access_grants WHERE user_id = ?", (user_id,))
    return cursor.fetchone() is not None


@safe_db_operation
def grant_user_access(user_id: int, granted_by: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()

    # ensure foreign-key users exist
    get_user_from_db(user_id)
    get_user_from_db(granted_by)

    cursor.execute("""
        INSERT OR REPLACE INTO access_grants (user_id, granted_by)
        VALUES (?, ?)
    """, (user_id, granted_by))
    conn.commit()
    return True


@safe_db_operation
def get_authorized_users() -> List[int]:
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT DISTINCT user_id FROM access_grants")
    granted_users = [row[0] for row in cursor.fetchall()]

    cursor.execute("SELECT user_id FROM admins")
    admins = [row[0] for row in cursor.fetchall()]

    return list(set(granted_users + admins))


@safe_db_operation
def get_all_users() -> List[int]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM users")
    return [row[0] for row in cursor.fetchall()]


@safe_db_operation
def get_user_by_username_or_id(identifier: str) -> Optional[Tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()

    if identifier.lstrip("@").isdigit():
        user_id = int(identifier.lstrip("@"))
        cursor.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
        return cursor.fetchone()

    username = identifier.lstrip("@")
    cursor.execute("SELECT * FROM users WHERE username = ?", (username,))
    return cursor.fetchone()


@safe_db_operation
def get_super_admins() -> List[int]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM admins WHERE role = 'super'")
    return [row[0] for row in cursor.fetchall()]


@safe_db_operation
def get_all_admins() -> List[int]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT user_id FROM admins")
    return [row[0] for row in cursor.fetchall()]


@safe_db_operation
def is_super_admin(user_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM admins WHERE user_id = ? AND role = 'super'", (user_id,))
    return cursor.fetchone() is not None


@safe_db_operation
def is_admin(user_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT 1 FROM admins WHERE user_id = ?", (user_id,))
    return cursor.fetchone() is not None


@safe_db_operation
def add_to_admin(role: str, user_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("INSERT OR REPLACE INTO admins (user_id, role) VALUES (?, ?)", (user_id, role))
    conn.commit()
    return True


@safe_db_operation
def remove_from_admin(user_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("DELETE FROM admins WHERE user_id = ?", (user_id,))
    cursor.execute("DELETE FROM admin_sessions WHERE user_id = ?", (user_id,))
    conn.commit()
    return True


@safe_db_operation
def is_admin_session_valid(user_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT expires_at FROM admin_sessions WHERE user_id = ?", (user_id,))
    result = cursor.fetchone()
    if not result:
        return False
    try:
        expires_at = datetime.fromisoformat(result[0])
        return datetime.now() < expires_at
    except Exception:
        return False


@safe_db_operation
def create_admin_session(user_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()
    expires_at = datetime.now() + timedelta(days=ADMIN_SESSION_DAYS)
    cursor.execute("""
        INSERT OR REPLACE INTO admin_sessions (user_id, expires_at)
        VALUES (?, ?)
    """, (user_id, expires_at.isoformat()))
    conn.commit()


@safe_db_operation
def load_admin_password() -> str:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT value FROM system_settings WHERE key = 'admin_password'")
    result = cursor.fetchone()
    return result[0] if result else ""


@safe_db_operation
def save_admin_password(password: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO system_settings (key, value)
        VALUES ('admin_password', ?)
    """, (password,))
    conn.commit()


def check_admin_password(password: str) -> bool:
    stored = load_admin_password()
    return bool(stored) and password == stored


def init_admins_database():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM admins WHERE role = 'super'")
    count = cursor.fetchone()[0]

    if count == 0:
        env_owner = os.getenv("SUPERADMIN_ID") or os.getenv("SUPER_ADMIN_ID")
        if env_owner and str(env_owner).isdigit():
            owner_id = int(env_owner)
            add_to_admin("super", owner_id)
            logger.info(f"✅ Супер-Админ установлен из переменной окружения: {owner_id}")
            return

        if sys.stdin.isatty():
            print("\n" + "=" * 60)
            print("⚠️ Первый запуск! Нужно установить Супер-Админа")
            try:
                owner_id_str = input("👑 Введите ваш Telegram ID: ").strip()
                if owner_id_str.isdigit():
                    owner_id = int(owner_id_str)
                    add_to_admin("super", owner_id)
                    print(f"✅ Супер-Админ установлен: {owner_id}")
                else:
                    print("❌ ID должен быть числом. Супер-админ не установлен.")
            except Exception as e:
                print(f"❌ Ошибка ввода ID: {e}")
        else:
            logger.warning(
                "⚠️ Супер-админ не установлен. "
                "Задайте SUPERADMIN_ID в переменных окружения или добавьте запись вручную в БД."
            )


# ========================================
# СВЯЗЬ С ACCESS PASSWORDS
# ========================================
@safe_db_operation
def check_access_password(password_text: str) -> Optional[Tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, type, max_uses, current_uses
        FROM access_passwords
        WHERE password = ? AND is_active = 1 AND current_uses < max_uses
    """, (password_text,))
    return cursor.fetchone()


@safe_db_operation
def use_access_password(password_id: int, user_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE access_passwords
        SET current_uses = current_uses + 1
        WHERE id = ?
    """, (password_id,))
    cursor.execute("""
        INSERT INTO password_uses (password_id, user_id)
        VALUES (?, ?)
    """, (password_id, user_id))
    conn.commit()
    return True


@safe_db_operation
def create_access_password(password_text: str, pass_type: str, max_uses: int, admin_id: int) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO access_passwords (password, type, max_uses, created_by)
        VALUES (?, ?, ?, ?)
    """, (password_text, pass_type, max_uses, admin_id))
    conn.commit()
    return True


def generate_access_password(pass_type: str, max_uses: int, admin_id: int) -> Optional[str]:
    if pass_type not in ("single", "multi"):
        return None
    if max_uses < 1:
        max_uses = 1
    if pass_type == "single":
        max_uses = 1

    for _ in range(20):
        password = generate_password(16)
        if create_access_password(password, pass_type, max_uses, admin_id):
            return password
    return None


def check_and_use_password(password_text: str, user_id: int) -> bool:
    password_text = (password_text or "").strip()
    if not password_text:
        return False

    with db_lock:
        conn = get_db_connection()
        cursor = conn.cursor()
        try:
            cursor.execute("BEGIN IMMEDIATE")

            cursor.execute("""
                SELECT id, type, max_uses, current_uses, is_active
                FROM access_passwords
                WHERE password = ?
            """, (password_text,))
            row = cursor.fetchone()

            if not row:
                conn.rollback()
                return False

            password_id, pass_type, max_uses, current_uses, is_active = row
            if not is_active or current_uses >= max_uses:
                conn.rollback()
                return False

            # ensure user exists in users
            cursor.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,))
            if cursor.fetchone() is None:
                cursor.execute("""
                    INSERT INTO users (user_id, username, first_name, last_name, balance)
                    VALUES (?, NULL, NULL, NULL, 0.0)
                """, (user_id,))

            cursor.execute("""
                INSERT OR IGNORE INTO access_grants (user_id, granted_by)
                VALUES (?, ?)
            """, (user_id, user_id))

            cursor.execute("""
                INSERT INTO password_uses (password_id, user_id)
                VALUES (?, ?)
            """, (password_id, user_id))

            new_uses = current_uses + 1
            new_active = 0 if new_uses >= max_uses else 1
            cursor.execute("""
                UPDATE access_passwords
                SET current_uses = ?, is_active = ?
                WHERE id = ?
            """, (new_uses, new_active, password_id))

            conn.commit()
            sync_user_to_json_from_db(user_id)
            return True

        except sqlite3.Error as e:
            conn.rollback()
            logger.error(f"Ошибка при использовании access-password: {e}")
            return False


def handle_login(message):
    user_id = message.from_user.id
    password_text = (message.text or "").strip()
    result = check_and_use_password(password_text, user_id)

    if result:
        clear_user_state(user_id)
        bot.send_message(message.chat.id, LANGUAGES[get_lang(user_id)]["password_used_success"])
    else:
        bot.send_message(message.chat.id, LANGUAGES[get_lang(user_id)]["password_invalid"])


# ========================================
# JSON-РЕЗЕРВНАЯ КОПИЯ
# ========================================
def load_users_data() -> dict:
    if not os.path.exists(DATA_FILE):
        return {}
    try:
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Ошибка загрузки users_data.json: {e}")
        return {}


def save_users_data(data: dict):
    try:
        with open(DATA_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except IOError as e:
        logger.error(f"Ошибка сохранения users_data.json: {e}")


def get_user_data(user_id: int) -> dict:
    with storage_lock:
        db_user = raw_get_user_row(user_id)
        if not db_user:
            get_user_from_db(user_id)
            db_user = raw_get_user_row(user_id)

        data = load_users_data()
        uid = str(user_id)
        existing = data.get(uid, {})

        data[uid] = {
            "balance": float(db_user[4] if db_user else existing.get("balance", 0.0) or 0.0),
            "username": db_user[1] if db_user else existing.get("username"),
            "first_name": db_user[2] if db_user else existing.get("first_name"),
            "last_name": db_user[3] if db_user else existing.get("last_name"),
            "purchases": existing.get("purchases", []),
            "created_at": existing.get("created_at", db_user[6] if db_user and len(db_user) > 6 else datetime.now().isoformat())
        }
        save_users_data(data)
        return data[uid]


def update_user_balance(user_id: int, amount: float) -> float:
    with storage_lock:
        data = load_users_data()
        uid = str(user_id)

        if uid not in data:
            get_user_data(user_id)
            data = load_users_data()

        if "balance" not in data[uid] or not isinstance(data[uid]["balance"], (int, float)):
            data[uid]["balance"] = 0.0

        data[uid]["balance"] = round(float(data[uid]["balance"]) + float(amount), 2)
        save_users_data(data)

    update_user_balance_db(user_id, data[str(user_id)]["balance"])
    sync_user_to_json_from_db(user_id)
    return data[str(user_id)]["balance"]


def add_purchase_record(user_id: int, product: str, price: float, key: str):
    with storage_lock:
        data = load_users_data()
        uid = str(user_id)
        if uid not in data:
            data[uid] = {
                "balance": 0.0,
                "username": None,
                "first_name": None,
                "last_name": None,
                "purchases": [],
                "created_at": datetime.now().isoformat()
            }
        data[uid].setdefault("purchases", []).append({
            "product": product,
            "price": price,
            "key": key,
            "date": datetime.now().isoformat()
        })
        save_users_data(data)


# ========================================
# КЛЮЧИ / СКЛАД
# ========================================
def resolve_key_file(product_key: str, period: str) -> str:
    key = f"{product_key}_{period}"
    path = KEY_FILES.get(key)
    if path and os.path.exists(path):
        return path

    filename = f"keys_{product_key}_{period}.txt"
    path = os.path.join(KEYS_FOLDER, filename)
    KEY_FILES[key] = path

    if not os.path.exists(path):
        os.makedirs(KEYS_FOLDER, exist_ok=True)
        open(path, "a", encoding="utf-8").close()

    return path


def migrate_legacy_primehack_keys(period: str):
    legacy_path = os.path.join(KEYS_FOLDER, f"keys_{period}.txt")
    new_path = os.path.join(KEYS_FOLDER, f"keys_primehack_{period}.txt")

    if not os.path.exists(legacy_path):
        return

    legacy_keys = []
    try:
        with open(legacy_path, "r", encoding="utf-8") as f:
            legacy_keys = [line.strip() for line in f if line.strip()]
    except IOError as e:
        logger.error(f"Ошибка чтения legacy-файла {legacy_path}: {e}")
        return

    if not legacy_keys:
        return

    os.makedirs(KEYS_FOLDER, exist_ok=True)
    if not os.path.exists(new_path):
        open(new_path, "a", encoding="utf-8").close()

    try:
        with open(new_path, "r", encoding="utf-8") as f:
            current_keys = {line.strip() for line in f if line.strip()}
    except IOError:
        current_keys = set()

    merged = list(current_keys)
    changed = False
    for key in legacy_keys:
        if key not in current_keys:
            merged.append(key)
            changed = True

    if changed:
        with open(new_path, "w", encoding="utf-8") as f:
            f.write("\n".join(merged) + "\n")
        logger.info(f"✅ Миграция legacy ключей PRIMEHACK {period} выполнена")


def refresh_key_files():
    """
    Сканирует папку keys/, создаёт недостающие файлы для всех товаров и периодов,
    обновляет KEY_FILES.
    """
    global KEY_FILES
    os.makedirs(KEYS_FOLDER, exist_ok=True)

    updated = {}
    for product_key, info in PRODUCTS.items():
        for period in info["prices"].keys():
            filename = f"keys_{product_key}_{period}.txt"
            path = os.path.join(KEYS_FOLDER, filename)
            if not os.path.exists(path):
                open(path, "a", encoding="utf-8").close()
            updated[f"{product_key}_{period}"] = path

            # legacy migration support for primehack/period.txt
            if product_key == "primehack":
                migrate_legacy_primehack_keys(period)

    KEY_FILES = updated
    logger.info(f"✅ KEY_FILES обновлён. Файлов: {len(KEY_FILES)}")
    return KEY_FILES


def get_keys_count(product_key: str, period: str) -> int:
    """
    Возвращает количество ключей в файле для товара и периода.
    """
    with key_lock:
        path = resolve_key_file(product_key, period)
        if not os.path.exists(path):
            return 0
        try:
            with open(path, "r", encoding="utf-8") as f:
                return len([line.strip() for line in f if line.strip()])
        except IOError as e:
            logger.error(f"Ошибка чтения файла ключей {path}: {e}")
            return 0


def get_available_keys(product_key: str, period: str, quantity: int) -> List[str]:
    """
    Читает quantity ключей из файла, удаляет их из файла, возвращает список.
    """
    if quantity <= 0:
        return []

    with key_lock:
        path = resolve_key_file(product_key, period)
        try:
            with open(path, "r", encoding="utf-8") as f:
                lines = [line.strip() for line in f if line.strip()]

            if len(lines) < quantity:
                return []

            taken = lines[:quantity]
            remaining = lines[quantity:]

            with open(path, "w", encoding="utf-8") as f:
                if remaining:
                    f.write("\n".join(remaining) + "\n")

            return taken

        except IOError as e:
            logger.error(f"Ошибка работы с файлом ключей {path}: {e}")
            return []


def get_multiple_keys_from_file(product_key: str, period: str, quantity: int) -> List[str]:
    """
    Backward-compatible alias.
    """
    return get_available_keys(product_key, period, quantity)


def add_key_to_file(product_key: str, period: str, key: str) -> bool:
    path = resolve_key_file(product_key, period)
    try:
        with key_lock:
            with open(path, "a", encoding="utf-8") as f:
                f.write(key.strip() + "\n")
        return True
    except IOError as e:
        logger.error(f"Ошибка добавления ключа в файл {path}: {e}")
        return False


# ========================================
# НОТИФИКАЦИИ
# ========================================
def log_new_user(user_id: int, first_name: str, last_name: str, username: str):
    full_name = first_name + (f" {last_name}" if last_name else "")
    display_username = f"@{username}" if username else "не указан"
    logger.info(f"Новый пользователь: ID={user_id}, Имя={full_name}, Username={display_username}")


def notify_admins_about_new_user(user_id: int, first_name: str, last_name: str, username: str):
    full_name = first_name + (f" {last_name}" if last_name else "")
    display_username = f"@{username}" if username else "не указан"
    text = (
        f"🔔 <b>Новый пользователь!</b>\n\n"
        f"👤 Имя: {full_name}\n"
        f"🆔 username: {display_username}\n"
        f"🔢 ID: {user_id}\n"
        f"⏰ Время: {format_timestamp()}"
    )
    for admin_id in get_all_admins():
        try:
            bot.send_message(admin_id, text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления новом пользователе админу {admin_id}: {e}")


def notify_admins_about_new_ticket(ticket_id: int, user_id: int, message: str, created_at: datetime):
    for admin_id in get_all_admins():
        try:
            bot.send_message(
                admin_id,
                LANGUAGES[get_lang(admin_id)]["new_ticket_admin_notify"].format(
                    ticket_id, user_id, safe_text(message), format_timestamp(created_at)
                ),
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления админу {admin_id}: {e}")


def notify_admins_about_purchase(user_id: int, product_name: str, quantity: int, total_price: float, keys: List[str]):
    user_db = get_user_from_db(user_id)
    if user_db and user_db[1]:
        user_display = f"@{user_db[1]}"
    elif user_db and user_db[2]:
        user_display = user_db[2]
    else:
        user_display = f"ID {user_id}"

    keys_text = "\n".join(f"• {key}" for key in keys)
    notification_text = (
        f"🔔 Уведомление об заказе пользователя {user_display}\n"
        f"➖➖➖➖➖➖➖➖➖➖➖➖\n"
        f"📃 Товар: {product_name}\n"
        f"💰 Цена: {total_price} ₽\n"
        f"📦 Кол-во: {quantity}\n"
        f"🔐 Ключи:\n{keys_text}"
    )

    for admin_id in get_all_admins():
        try:
            bot.send_message(admin_id, notification_text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления о покупке админу {admin_id}: {e}")


def notify_admins_about_warning(user_id: int, stage: int, reason: str, admin_id: int):
    user_db = get_user_from_db(user_id)
    target_username = f"@{user_db[1]}" if user_db and user_db[1] else f"ID {user_id}"
    admin_db = get_user_from_db(admin_id)
    admin_username = f"@{admin_db[1]}" if admin_db and admin_db[1] else f"ID {admin_id}"
    stage_name = WARNING_LEVELS[stage][0] if stage in WARNING_LEVELS else str(stage)

    notification_text = (
        f"🚨 ВЫДАНО ПРЕДУПРЕЖДЕНИЕ\n\n"
        f"Пользователь: {target_username}\n"
        f"Уровень: {stage} ({stage_name})\n"
        f"Причина: {reason}\n"
        f"Выдал: {admin_username}\n"
        f"Дата: {datetime.now().strftime('%d.%m.%Y %H:%M')}"
    )

    for admin_id_notify in get_all_admins():
        try:
            bot.send_message(admin_id_notify, notification_text, parse_mode="HTML")
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления о предупреждении админу {admin_id_notify}: {e}")


def send_balance_notification(target_user_id: int, amount: float, new_balance: float):
    target_lang = get_lang(target_user_id)
    template = LANGUAGES[target_lang]["balance_topup_notification"]
    text = template.format(target_user_id, int(amount), int(new_balance))
    try:
        bot.send_message(target_user_id, text, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Ошибка отправки уведомления о балансе пользователю {target_user_id}: {e}")


# ========================================
# ТИКЕТЫ
# ========================================
@safe_db_operation
def create_ticket(user_id: int, message: str) -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO tickets (user_id, message, status)
        VALUES (?, ?, ?)
    """, (user_id, message, "open"))
    conn.commit()
    return cursor.lastrowid


@safe_db_operation
def get_ticket_by_id(ticket_id: int) -> Optional[Tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM tickets WHERE id = ?", (ticket_id,))
    return cursor.fetchone()


@safe_db_operation
def count_open_tickets() -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM tickets WHERE status IN ('open', 'in_progress')")
    result = cursor.fetchone()
    return result[0] if result else 0


@safe_db_operation
def get_open_tickets_paginated(page: int = 0) -> List[Tuple]:
    offset = page * TICKETS_PER_PAGE
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, user_id, message, status, created_at FROM tickets
        WHERE status IN ('open', 'in_progress')
        ORDER BY created_at DESC LIMIT ? OFFSET ?
    """, (TICKETS_PER_PAGE, offset))
    return cursor.fetchall()


@safe_db_operation
def update_ticket_status(ticket_id: int, new_status: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE tickets
        SET status = ?, updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (new_status, ticket_id))
    conn.commit()


@safe_db_operation
def add_admin_response_to_ticket(ticket_id: int, admin_response_text: str):
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE tickets
        SET admin_response = ?, status = 'answered', updated_at = CURRENT_TIMESTAMP
        WHERE id = ?
    """, (admin_response_text, ticket_id))
    conn.commit()


@safe_db_operation
def add_ticket_attachment(ticket_id: int, file_id: str, file_type: str) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO ticket_attachments (ticket_id, file_id, file_type)
        VALUES (?, ?, ?)
    """, (ticket_id, file_id, file_type))
    conn.commit()
    return True


@safe_db_operation
def get_ticket_attachments(ticket_id: int) -> List[Tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT file_id, file_type FROM ticket_attachments WHERE ticket_id = ?", (ticket_id,))
    return cursor.fetchall()


@safe_db_operation
def get_user_tickets(user_id: int) -> List[Tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, message, status, created_at FROM tickets
        WHERE user_id = ? ORDER BY created_at DESC
    """, (user_id,))
    return cursor.fetchall()


# ========================================
# ПРЕДУПРЕЖДЕНИЯ
# ========================================
def issue_warning(user_id: int, stage: int, reason: str, admin_id: int) -> Optional[dict]:
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO warnings (user_id, stage, reason, admin_id, is_active)
            VALUES (?, ?, ?, ?, 1)
        """, (user_id, stage, reason, admin_id))
        warning_id = cursor.lastrowid

        user_before = get_user_from_db(user_id)
        old_balance = user_before[4] if user_before else 0.0
        new_balance = old_balance

        if stage == 2:
            deduction = old_balance * 0.3
            new_balance = update_user_balance(user_id, -deduction)
            add_transaction(user_id, deduction, "warn_deduction", warning_id)
        elif stage == 3:
            cursor.execute("UPDATE users SET strict_warning_active = 1 WHERE user_id = ?", (user_id,))

        conn.commit()
        return {
            "warning_id": warning_id,
            "old_balance": old_balance,
            "new_balance": new_balance,
            "deduction": old_balance - new_balance if stage == 2 else 0
        }
    except Exception as e:
        logger.error(f"Ошибка выдачи предупреждения: {e}")
        return None


@safe_db_operation
def get_user_warnings(user_id: int) -> List[Tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT w.id, w.stage, w.reason, w.issued_at, w.admin_id, u.username as admin_username
        FROM warnings w
        LEFT JOIN users u ON w.admin_id = u.user_id
        WHERE w.user_id = ? AND w.is_active = 1
        ORDER BY w.issued_at DESC
    """, (user_id,))
    return cursor.fetchall()


# ========================================
# ДЕПОЗИТЫ
# ========================================
@safe_db_operation
def create_deposit_request(user_id: int, amount: float) -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        INSERT INTO deposits (user_id, amount, status)
        VALUES (?, ?, ?)
    """, (user_id, amount, "pending"))
    conn.commit()
    return cursor.lastrowid


@safe_db_operation
def update_deposit_screenshot(deposit_id: int, screenshot_file_id: str) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE deposits
        SET screenshot_file_id = ?
        WHERE id = ?
    """, (screenshot_file_id, deposit_id))
    conn.commit()
    return cursor.rowcount > 0


@safe_db_operation
def get_deposit_by_id(deposit_id: int) -> Optional[Tuple]:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM deposits WHERE id = ?", (deposit_id,))
    return cursor.fetchone()


@safe_db_operation
def count_pending_deposits() -> int:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM deposits WHERE status = 'pending'")
    result = cursor.fetchone()
    return result[0] if result else 0


@safe_db_operation
def get_pending_deposits_paginated(page: int = 0) -> List[Tuple]:
    offset = page * 10
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT id, user_id, amount, screenshot_file_id, created_at FROM deposits
        WHERE status = 'pending'
        ORDER BY created_at DESC LIMIT 10 OFFSET ?
    """, (offset,))
    return cursor.fetchall()


@safe_db_operation
def update_deposit_status(deposit_id: int, new_status: str, processed_by: int = None) -> bool:
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        UPDATE deposits
        SET status = ?, processed_by = ?, processed_at = CURRENT_TIMESTAMP
        WHERE id = ? AND status = 'pending'
    """, (new_status, processed_by, deposit_id))
    conn.commit()
    return cursor.rowcount > 0


def notify_admins_about_new_deposit(deposit_id: int, user_id: int, amount: float):
    for admin_id in get_all_admins():
        try:
            bot.send_message(
                admin_id,
                LANGUAGES["ru"]["deposit_new_request_notify"].format(
                    deposit_id, user_id, int(amount), format_timestamp()
                ),
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"Ошибка отправки уведомления о депозите админу {admin_id}: {e}")


def show_deposit_requests(chat_id: int):
    total_deposits = count_pending_deposits()
    if total_deposits == 0:
        bot.send_message(chat_id, "✅ Нет активных заявок на пополнение.")
        return

    deps = get_pending_deposits_paginated(page=0)
    markup = types.InlineKeyboardMarkup()
    for dep_id, user_id, amount, _, created_at in deps:
        user_obj = get_user_from_db(user_id)
        uname = f"@{user_obj[1]}" if user_obj and user_obj[1] else f"ID {user_id}"
        time_str = format_timestamp(created_at)
        markup.add(types.InlineKeyboardButton(
            f"#{dep_id} | {int(amount)}₽ от {uname} ({time_str})",
            callback_data=f"view_deposit_{dep_id}"
        ))
    markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_back_btn"))
    bot.send_message(
        chat_id,
        f"📋 <b>Активные заявки на пополнение ({total_deposits}):</b>",
        parse_mode="HTML",
        reply_markup=markup
    )


# ========================================
# КЛАВИАТУРЫ
# ========================================
def get_main_keyboard(user_id: int) -> types.ReplyKeyboardMarkup:
    lang = get_lang(user_id)
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(LANGUAGES[lang]["products"], LANGUAGES[lang]["profile"])
    kb.row(LANGUAGES[lang]["settings"], LANGUAGES[lang]["support"])
    if is_admin(user_id):
        kb.row(LANGUAGES[lang]["admin_panel"])
    return kb


def get_platforms_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    kb.add(types.InlineKeyboardButton("📱 Android", callback_data="platform_android"))
    kb.add(types.InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu"))
    return kb


def get_catalog_keyboard() -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    for product_key, info in PRODUCTS.items():
        kb.add(types.InlineKeyboardButton(info["name"], callback_data=f"product_{product_key}"))
    kb.add(types.InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu"))
    return kb


def get_categories_keyboard(product_key: str) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    for category_key, info in CATEGORIES.items():
        kb.add(types.InlineKeyboardButton(info["name"], callback_data=f"category_{product_key}_{category_key}"))
    kb.add(types.InlineKeyboardButton("🔙 Назад к продуктам", callback_data="back_to_catalog"))
    return kb


def get_subscriptions_keyboard(product_key: str, category_key: str) -> types.InlineKeyboardMarkup:
    kb = types.InlineKeyboardMarkup(row_width=1)
    periods = CATEGORIES[category_key]["periods"]

    buttons = []
    for period in periods:
        price = PRODUCTS[product_key]["prices"][period]
        display = PERIOD_DISPLAY[period]
        buttons.append(
            types.InlineKeyboardButton(
                f"{display} — {price}₽",
                callback_data=f"select_qty_{product_key}_{period}"
            )
        )

    for i in range(0, len(buttons), 2):
        kb.row(*buttons[i:i + 2])

    kb.add(types.InlineKeyboardButton("🔙 Назад к категориям", callback_data=f"product_{product_key}"))
    return kb


def get_profile_keyboard(user_id: int) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("💳 Создать заявку на баланс", callback_data=f"deposit_start_{user_id}"))
    markup.add(types.InlineKeyboardButton(LANGUAGES[get_lang(user_id)]["my_tickets"], callback_data="my_tickets"))
    markup.add(types.InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu"))
    return markup


def get_settings_keyboard(user_id: int) -> types.ReplyKeyboardMarkup:
    lang = get_lang(user_id)
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(LANGUAGES[lang]["language"])
    kb.row(LANGUAGES[lang]["back_menu"])
    return kb


def get_language_keyboard(user_id: int) -> types.ReplyKeyboardMarkup:
    lang = get_lang(user_id)
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(LANGUAGES[lang]["rus"], LANGUAGES[lang]["eng"])
    kb.row(LANGUAGES[lang]["back_menu"])
    return kb


def get_rules_keyboard() -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu"))
    return markup


def get_insufficient_funds_keyboard(user_id: int) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(LANGUAGES[get_lang(user_id)]["top_up_balance"], callback_data=f"deposit_start_{user_id}"))
    markup.add(types.InlineKeyboardButton("🔙 Назад в меню", callback_data="back_to_menu"))
    return markup


def get_admin_keyboard(user_id: int) -> types.ReplyKeyboardMarkup:
    lang = get_lang(user_id)
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    kb.add("📊 Наличие", "💸 Пополнить", "🧯 Обнулить", "🔓 Выдать доступ", "⚖️ Выдать варн",
           LANGUAGES[lang]["ticket_view_list"])
    kb.add(LANGUAGES[lang]["broadcast"], LANGUAGES[lang]["check_deposits_btn"])
    if is_super_admin(user_id):
        kb.add("🔑 Добавить ключ", "➕ Добавить админа", "👑 Супер-админ", "❌ Снять с админки",
               "🔐 Установить пароль админа")
    kb.add("🔙 Назад")
    return kb


def get_access_menu_keyboard() -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("👤 Добавить пользователя", callback_data="access_add_user"))
    markup.add(types.InlineKeyboardButton("🔑 Пароль", callback_data="access_password_menu"))
    markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data="access_back"))
    return markup


def get_password_type_keyboard() -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup(row_width=1)
    markup.add(types.InlineKeyboardButton("📱 Единоразовый (1 пользователь)", callback_data="password_single"))
    markup.add(types.InlineKeyboardButton("👥 Многоразовый", callback_data="password_multi_menu"))
    markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data="access_back"))
    return markup


def get_multi_use_keyboard() -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup(row_width=5)
    buttons = [types.InlineKeyboardButton(str(i), callback_data=f"password_multi_{i}") for i in range(1, 11)]
    for i in range(0, len(buttons), 5):
        markup.row(*buttons[i:i + 5])
    markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data="access_password_menu"))
    return markup


def get_deposit_buttons(deposit_id: int) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton(LANGUAGES["ru"]["confirm_deposit_action"], callback_data=f"confirm_deposit_{deposit_id}"))
    markup.add(types.InlineKeyboardButton(LANGUAGES["ru"]["reject_deposit_action"], callback_data=f"reject_deposit_{deposit_id}"))
    markup.add(types.InlineKeyboardButton("← Назад к списку", callback_data="list_deposits"))
    return markup


def get_broadcast_confirm_keyboard() -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("✅ Отправить", callback_data="broadcast_confirm"))
    markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="broadcast_cancel"))
    return markup


def get_tickets_user_keyboard(tickets: List[Tuple]) -> types.InlineKeyboardMarkup:
    markup = types.InlineKeyboardMarkup()
    for ticket in tickets:
        ticket_id, _, status, _ = ticket
        attachments = get_ticket_attachments(ticket_id)
        has_photo = any(att[1] == "photo" for att in attachments) if attachments else False
        status_text = TICKET_STATUSES.get(status, {}).get("ru", status)
        btn_text = f"#{ticket_id} - {status_text}"
        if has_photo:
            btn_text = f"📷 {btn_text}"
        markup.add(types.InlineKeyboardButton(btn_text, callback_data=f"view_my_ticket_{ticket_id}"))
    markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data="back_to_menu"))
    return markup


# ========================================
# ПРОВЕРКА ДОСТУПА
# ========================================
def check_user_access(user_id: int) -> bool:
    return is_admin(user_id) or is_user_access_granted(user_id)


def send_access_denied(message):
    user_id = message.from_user.id
    lang = get_lang(user_id)
    bot.send_message(message.chat.id, LANGUAGES[lang]["access_denied"], parse_mode="HTML")


# ========================================
# КОМАНДЫ
# ========================================
@bot.message_handler(commands=["start"])
def cmd_start(message):
    user_id = message.from_user.id

    if not check_user_access(user_id):
        clear_user_state(user_id)
        bot.send_message(message.chat.id, LANGUAGES[get_lang(user_id)]["access_denied"])
        return

    clear_user_state(user_id)
    first_name = message.from_user.first_name or ""
    last_name = message.from_user.last_name or ""
    username = message.from_user.username or ""
    user_languages.setdefault(user_id, "ru")
    update_user_info(user_id, first_name, last_name, username)
    bot.send_message(
        message.chat.id,
        LANGUAGES[get_lang(user_id)]["welcome"],
        reply_markup=get_main_keyboard(user_id)
    )


@bot.message_handler(commands=["login"])
def cmd_login(message):
    user_id = message.from_user.id
    if check_user_access(user_id):
        bot.send_message(message.chat.id, "✅ У вас уже есть доступ к боту!")
        return
    user_states[user_id] = "waiting_for_password"
    bot.send_message(message.chat.id, LANGUAGES[get_lang(user_id)]["enter_access_password"])


@bot.message_handler(commands=["refresh_keys"])
def cmd_refresh_keys(message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        return
    refresh_key_files()
    text = "✅ Список файлов ключей обновлён.\n\n"
    for product_key in PRODUCTS:
        text += f"<b>{PRODUCTS[product_key]['name']}</b>\n"
        for period in PRODUCTS[product_key]["prices"]:
            text += f"• {PERIOD_DISPLAY[period]}: {get_keys_count(product_key, period)} шт.\n"
        text += "\n"
    bot.reply_to(message, text, parse_mode="HTML")


@bot.message_handler(commands=["ticket"])
def cmd_create_ticket(message):
    user_id = message.from_user.id
    if not check_user_access(user_id):
        send_access_denied(message)
        return

    lang = get_lang(user_id)
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        bot.reply_to(message, LANGUAGES[lang]["ticket_empty_message"], parse_mode="HTML")
        return

    message_text = parts[1].strip()
    if len(message_text) < 10:
        bot.reply_to(message, "❌ Опишите проблему подробнее, минимум 10 символов.")
        return

    ticket_id = create_ticket(user_id, message_text)
    if not ticket_id:
        bot.reply_to(message, "❌ Произошла ошибка при создании тикета. Попробуйте позже.")
        return

    bot.reply_to(message, LANGUAGES[lang]["ticket_created"].format(ticket_id), parse_mode="HTML")
    notify_admins_about_new_ticket(ticket_id, user_id, message_text, datetime.now())


@bot.message_handler(commands=["my_tickets"])
def cmd_my_tickets(message):
    user_id = message.from_user.id
    if not check_user_access(user_id):
        send_access_denied(message)
        return

    tickets = get_user_tickets(user_id)
    if not tickets:
        bot.send_message(message.chat.id, "ℹ️ У вас нет созданных тикетов.")
        return

    bot.send_message(message.chat.id, "📋 <b>Ваши тикеты:</b>", parse_mode="HTML", reply_markup=get_tickets_user_keyboard(tickets))


@bot.message_handler(commands=["checkticket"])
def cmd_check_ticket(message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        return

    lang = get_lang(user_id)
    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        bot.reply_to(message, "❌ Используйте формат: /checkticket <ID тикета>")
        return

    try:
        ticket_id = int(parts[1])
    except ValueError:
        bot.reply_to(message, "❌ ID тикета должен быть числом")
        return

    ticket = get_ticket_by_id(ticket_id)
    if not ticket:
        bot.reply_to(message, LANGUAGES[lang]["ticket_not_found"])
        return

    if ticket[3] == "open":
        update_ticket_status(ticket_id, "in_progress")
        ticket = get_ticket_by_id(ticket_id)

    status_text = TICKET_STATUSES.get(ticket[3], {}).get("ru", ticket[3])
    admin_reply = ticket[5] if ticket[5] is not None else "<i>Ответа пока нет</i>"

    bot.reply_to(
        message,
        LANGUAGES[lang]["ticket_info_header"].format(
            ticket_id,
            ticket[1],
            format_timestamp(ticket[4]),
            status_text,
            safe_text(ticket[2]),
            safe_text(admin_reply)
        ),
        parse_mode="HTML"
    )
    bot.send_message(message.chat.id, LANGUAGES[lang]["ticket_awaiting_reply"])
    user_states[user_id] = f"awaiting_ticket_reply_{ticket_id}"


@bot.message_handler(commands=["my_warn"])
def cmd_my_warn(message):
    user_id = message.from_user.id
    if not check_user_access(user_id):
        send_access_denied(message)
        return

    warnings = get_user_warnings(user_id)
    if not warnings:
        bot.reply_to(message, "✅ <b>У вас нет действующих предупреждений</b>", parse_mode="HTML")
        return

    text = "<b>📋 ВАШИ АКТИВНЫЕ ПРЕДУПРЕЖДЕНИЯ:</b>\n\n"
    for idx, warning in enumerate(warnings, 1):
        _, stage, reason, issued_at, _, _ = warning
        stage_name = WARNING_LEVELS.get(stage, (f"Уровень {stage}", ""))[0]
        time_formatted = format_timestamp(issued_at)
        text += f"<b>{idx}. {stage_name}</b> (от {time_formatted})\n"
        text += f" <i>Причина:</i> {reason}\n\n"

    bot.reply_to(message, text, parse_mode="HTML")


@bot.message_handler(commands=["warn"])
def cmd_warn(message):
    if not is_super_admin(message.from_user.id):
        bot.reply_to(message, LANGUAGES[get_lang(message.from_user.id)]["limited_access"])
        return
    try:
        parts = message.text.split(maxsplit=3)
        if len(parts) < 4:
            bot.reply_to(message, "❌ Неверный формат. Используйте: <code>/warn ID_пользователя 1|2|3 причина</code>", parse_mode="HTML")
            return
        target_id = int(parts[1])
        stage = int(parts[2])
        reason = parts[3].strip()

        if stage not in [1, 2, 3]:
            bot.reply_to(message, "❌ Ошибка: уровень должен быть 1, 2 или 3")
            return
        if len(reason) < 10:
            bot.reply_to(message, "❌ Ошибка: причина должна быть не менее 10 символов")
            return

        user_db = get_user_from_db(target_id)
        if not user_db:
            bot.reply_to(message, "❌ Ошибка: пользователь с таким ID не найден")
            return

        result = issue_warning(target_id, stage, reason, message.from_user.id)
        if not result:
            bot.reply_to(message, "❌ Ошибка: не удалось выдать предупреждение. См. логи.")
            return

        username_target = f"@{user_db[1]}" if user_db[1] else f"ID {target_id}"

        if stage == 1:
            notif_text = f"🔔 Вам выдано <b>предупреждение</b>.\nПричина: {reason}"
        elif stage == 2:
            notif_text = (
                f"⚠️ Вам выдан <b>ВЫГОВОР</b>.\nПричина: {reason}\n"
                f"С вашего баланса списано 30%: {format_balance(result['old_balance'])} ₽ → {format_balance(result['new_balance'])} ₽"
            )
        else:
            notif_text = f"🚨 Вам выдан <b>СТРОГИЙ ВЫГОВОР</b>.\nПричина: {reason}"

        try:
            bot.send_message(target_id, notif_text, parse_mode="HTML")
        except Exception:
            pass

        notify_admins_about_warning(target_id, stage, reason, message.from_user.id)
        bot.reply_to(message, f"✅ Предупреждение (ур. {stage}) успешно выдано пользователю {username_target}")

    except ValueError:
        bot.reply_to(message, "❌ Ошибка: ID и уровень должны быть числами")
    except Exception as e:
        bot.reply_to(message, f"❌ Непредвиденная ошибка: {str(e)}")


@bot.message_handler(commands=["admin"])
def cmd_admin(message):
    user_id = message.from_user.id
    if not is_admin(user_id):
        return

    lang = get_lang(user_id)
    password = load_admin_password()
    if not password:
        if is_super_admin(user_id):
            bot.send_message(message.chat.id, LANGUAGES[lang]["no_password_set"], reply_markup=get_admin_keyboard(user_id))
        else:
            bot.send_message(message.chat.id, "Админ-панель недоступна, пароль не установлен.")
    elif is_admin_session_valid(user_id):
        bot.send_message(message.chat.id, LANGUAGES[lang]["admin_menu_title"], parse_mode="HTML", reply_markup=get_admin_keyboard(user_id))
    else:
        user_states[user_id] = "awaiting_admin_password"
        bot.send_message(message.chat.id, LANGUAGES[lang]["enter_admin_password"], parse_mode="HTML")


@bot.message_handler(commands=["set_admin_password"])
def cmd_set_admin_password(message):
    if not is_super_admin(message.from_user.id):
        return bot.reply_to(message, LANGUAGES[get_lang(message.from_user.id)]["limited_access"])
    try:
        args = message.text.split(maxsplit=1)
        if len(args) != 2:
            bot.reply_to(message, "❌ Неверный формат. Используйте: <code>/set_admin_password ВАШ_ПАРОЛЬ</code>", parse_mode="HTML")
            return
        new_password = args[1].strip()
        if len(new_password) < 6:
            bot.reply_to(message, "❌ Пароль слишком короткий. Минимум 6 символов!")
            return
        save_admin_password(new_password)
        bot.reply_to(message, LANGUAGES[get_lang(message.from_user.id)]["password_set"])
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")


@bot.message_handler(commands=["add_balance"])
def cmd_add_balance(message):
    if not is_admin(message.from_user.id):
        return
    try:
        parts = message.text.split()
        if len(parts) != 3:
            raise ValueError
        target_id = int(parts[1].lstrip("@"))
        amount = float(parts[2])
        if amount <= 0:
            bot.reply_to(message, "❌ Сумма пополнения должна быть больше нуля.")
            return

        get_user_from_db(target_id)
        new_bal = update_user_balance(target_id, amount)
        bot.reply_to(message, f"✅ Баланс пользователя {target_id} пополнен на {int(amount)} ₽. Новый баланс: {int(new_bal)} ₽.")
        send_balance_notification(target_id, amount, new_bal)

    except (ValueError, IndexError):
        bot.reply_to(message, "❌ Неверный формат. Используйте: <code>/add_balance ID сумма</code>", parse_mode="HTML")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")


@bot.message_handler(commands=["remove_admin"])
def cmd_remove_admin(message):
    if not is_super_admin(message.from_user.id):
        return bot.reply_to(message, LANGUAGES[get_lang(message.from_user.id)]["limited_access"])
    try:
        _, target = message.text.split()
        target_id = int(target.lstrip("@"))
        if target_id == message.from_user.id:
            return bot.reply_to(message, "❌ Нельзя удалить самого себя!")
        if not is_admin(target_id):
            return bot.reply_to(message, f"❌ Пользователь {target_id} не является админом.")
        remove_from_admin(target_id)
        bot.reply_to(message, f"✅ Права администратора у пользователя {target_id} отозваны.")
    except (ValueError, IndexError):
        bot.reply_to(message, "❌ Неверный формат. Используйте: <code>/remove_admin ID</code>", parse_mode="HTML")


@bot.message_handler(commands=["add_super_admin"])
def cmd_add_super_admin(message):
    if not is_super_admin(message.from_user.id):
        return bot.reply_to(message, LANGUAGES[get_lang(message.from_user.id)]["limited_access"])
    try:
        _, target = message.text.split()
        new_id = int(target.lstrip("@"))
        if is_super_admin(new_id):
            return bot.reply_to(message, f"⚠️ Пользователь {new_id} уже является супер-админом.")
        if add_to_admin("super", new_id):
            bot.reply_to(message, f"✅ Пользователь {new_id} успешно назначен Супер-Админом!")
            try:
                bot.send_message(new_id, "👑 Вы были назначены СУПЕР-АДМИНОМ!", parse_mode="HTML")
            except Exception:
                pass
    except (ValueError, IndexError):
        bot.reply_to(message, "❌ Неверный формат. Используйте: <code>/add_super_admin ID</code>", parse_mode="HTML")


@bot.message_handler(commands=["add_admin"])
def cmd_add_admin(message):
    if not is_super_admin(message.from_user.id):
        return bot.reply_to(message, LANGUAGES[get_lang(message.from_user.id)]["limited_access"])
    try:
        _, target = message.text.split()
        new_id = int(target.lstrip("@"))
        if is_admin(new_id):
            return bot.reply_to(message, f"⚠️ Пользователь {new_id} уже является админом.")
        if add_to_admin("regular", new_id):
            bot.reply_to(message, f"✅ Пользователь {new_id} успешно назначен администратором.")
            try:
                bot.send_message(new_id, "🛠️ Вы были назначены администратором!", parse_mode="HTML")
            except Exception:
                pass
    except (ValueError, IndexError):
        bot.reply_to(message, "❌ Неверный формат. Используйте: <code>/add_admin ID</code>", parse_mode="HTML")


@bot.message_handler(commands=["list_admins"])
def cmd_list_admins(message):
    if not is_super_admin(message.from_user.id):
        return bot.reply_to(message, LANGUAGES[get_lang(message.from_user.id)]["limited_access"])

    super_admins = get_super_admins()
    admins = get_all_admins()
    text = "<b>📋 Список администраторов:</b>\n"

    if super_admins:
        text += "\n👑 <b>Супер-Админы:</b>\n"
        for aid in super_admins:
            text += f" • <code>{aid}</code>\n"

    regular_admins = [a for a in admins if a not in super_admins]
    if regular_admins:
        text += "\n👤 <b>Админы:</b>\n"
        for aid in regular_admins:
            text += f" • <code>{aid}</code>\n"

    if not admins:
        text = "ℹ️ Список администраторов пуст."

    bot.reply_to(message, text, parse_mode="HTML")


@bot.message_handler(commands=["addkey"])
def cmd_addkey(message):
    if not is_super_admin(message.from_user.id):
        return bot.reply_to(message, LANGUAGES[get_lang(message.from_user.id)]["limited_access"])
    try:
        args = message.text.split(maxsplit=3)
        if len(args) != 4:
            return bot.reply_to(message, "Используйте: <code>/addkey ПРОДУКТ ПЕРИОД КЛЮЧ</code>", parse_mode="HTML")
        _, product_key, period, key = args
        product_key = product_key.lower()
        period = period.lower()
        if product_key not in PRODUCTS:
            return bot.reply_to(message, f"❌ Неверный продукт. Доступные: {', '.join(PRODUCTS.keys())}")
        if period not in PRODUCTS[product_key]["prices"]:
            return bot.reply_to(message, f"❌ Неверный период. Доступные: {', '.join(PRODUCTS[product_key]['prices'].keys())}")
        if add_key_to_file(product_key, period, key):
            bot.reply_to(message, f"✅ Ключ <code>{key}</code> добавлен для {product_key} ({period}).", parse_mode="HTML")
        else:
            bot.reply_to(message, "❌ Ошибка при добавлении ключа в файл.")
    except Exception as e:
        bot.reply_to(message, f"❌ Ошибка: {str(e)}")


@bot.message_handler(commands=["cancel"])
def cmd_cancel(message):
    uid = message.from_user.id
    if user_states.get(uid) or user_states.get(f"broadcast_{uid}"):
        clear_user_state(uid)
        bot.send_message(message.chat.id, "✅ Действие отменено.", reply_markup=get_main_keyboard(uid))
    else:
        bot.send_message(message.chat.id, "ℹ️ Нет активных действий для отмены.")


@bot.message_handler(commands=["myid"])
def cmd_myid(message):
    bot.send_message(message.chat.id, f"🆔 Ваш ID: <code>{message.from_user.id}</code>", parse_mode="HTML")


@bot.message_handler(commands=["help"])
def cmd_help(message):
    user_id = message.from_user.id
    if not check_user_access(user_id):
        send_access_denied(message)
        return

    lang = get_lang(user_id)
    help_text = "<b>ℹ️ Доступные команды:</b>\n"
    help_text += "/start - <i>Начало работы</i>\n"
    help_text += "/login - <i>Ввод пароля доступа</i>\n"
    help_text += "/help - <i>Это сообщение</i>\n"
    help_text += "/my_warn - <i>Мои предупреждения</i>\n"
    help_text += "/ticket <code>[текст]</code> - <i>Создать обращение в поддержку</i>\n"
    help_text += "/my_tickets - <i>Мои обращения</i>\n"

    if is_admin(user_id):
        help_text += "\n<b>Админ-команды:</b>\n"
        help_text += "/add_balance <code>[id] [сумма]</code> - <i>Пополнить баланс</i>\n"
        help_text += "/checkticket <code>[id]</code> - <i>Проверить тикет</i>\n"
        help_text += "/refresh_keys - <i>Обновить склад ключей</i>\n"

    if is_super_admin(user_id):
        help_text += "\n<b>👑 Супер-админ:</b>\n"
        help_text += "/add_admin <code>[id]</code>\n"
        help_text += "/add_super_admin <code>[id]</code>\n"
        help_text += "/remove_admin <code>[id]</code>\n"
        help_text += "/list_admins\n"
        help_text += "/addkey <code>[prod] [period] [key]</code>\n"
        help_text += "/set_admin_password <code>[пароль]</code>\n"
        help_text += "/warn <code>[id] [1-3] [причина]</code>\n"

    bot.send_message(message.chat.id, help_text, parse_mode="HTML")


# ========================================
# ОБРАБОТКА ТЕКСТОВЫХ СООБЩЕНИЙ
# ========================================
@bot.message_handler(content_types=["text"])
def handle_text(message):
    user_id = message.from_user.id
    chat_id = message.chat.id
    text = message.text or ""
    lang = get_lang(user_id)
    current_state = user_states.get(user_id)

    # Не перехватываем команды, пусть их обработают command handlers
    if text.lstrip().startswith("/"):
        return

    # Если не админ и нет доступа — показываем ограничение, но только вне состояний
    if not is_admin(user_id) and not current_state and not check_user_access(user_id):
        send_access_denied(message)
        return

    # ========================================
    # СОСТОЯНИЯ
    # ========================================
    if current_state:
        if current_state in ("waiting_for_password", "awaiting_access_password"):
            handle_login(message)
            return

        if current_state == "awaiting_user_id_for_access":
            identifier = text.strip()
            target_user = get_user_by_username_or_id(identifier)

            if not target_user:
                bot.send_message(chat_id, LANGUAGES[lang]["user_not_found"])
                return

            target_id = target_user[0]
            if is_user_access_granted(target_id):
                bot.send_message(chat_id, "❌ Этот пользователь уже имеет доступ!")
                clear_user_state(user_id)
                return

            if grant_user_access(target_id, user_id):
                try:
                    bot.send_message(target_id, LANGUAGES[get_lang(target_id)]["access_granted"], parse_mode="HTML")
                except Exception:
                    pass
                username = f"@{target_user[1]}" if target_user[1] else f"ID {target_id}"
                bot.send_message(chat_id, LANGUAGES[lang]["access_granted_admin"].format(username), parse_mode="HTML")
            else:
                bot.send_message(chat_id, "❌ Ошибка при выдаче доступа!")

            clear_user_state(user_id)
            return

        if current_state == "awaiting_admin_password":
            if check_admin_password(text):
                clear_user_state(user_id)
                create_admin_session(user_id)
                bot.send_message(chat_id, LANGUAGES[lang]["password_correct"], parse_mode="HTML", reply_markup=get_admin_keyboard(user_id))
            else:
                clear_user_state(user_id)
                bot.send_message(chat_id, LANGUAGES[lang]["admin_panel_blocked"], parse_mode="HTML", reply_markup=get_main_keyboard(user_id))
            return

        if current_state == "deposit_wait_amount":
            try:
                amount = float(text.replace(",", "."))
                if amount <= 0:
                    raise ValueError
            except ValueError:
                bot.send_message(chat_id, "❌ Отправьте корректное число (например: 1000)")
                return

            deposit_id = create_deposit_request(user_id, amount)
            user_states[user_id] = "deposit_wait_screenshot"
            deposit_context[user_id] = deposit_id
            req_text = (
                f"✅ <b>Заявка №{deposit_id} создана!</b>\n\n"
                f"💰 Сумма к оплате: <b>{int(amount)} ₽</b>\n\n"
                f"🏦 Пожалуйста, переведите сумму по реквизитам:\n\n"
                f"<code>{PAYMENT_REQUISITES}</code>\n\n"
                f"📸 <b>После перевода отправьте скриншот/чек об оплате прямо в этот чат.</b>"
            )
            bot.send_message(chat_id, req_text, parse_mode="HTML")
            notify_admins_about_new_deposit(deposit_id, user_id, amount)
            return

        if current_state == "deposit_wait_screenshot":
            bot.send_message(chat_id, "📸 Отправьте скриншот/чек об оплате в этот чат.")
            return

        if isinstance(current_state, str) and current_state.startswith("awaiting_ticket_reply_"):
            ticket_id = int(current_state.split("_")[-1])
            ticket = get_ticket_by_id(ticket_id)
            if not ticket:
                bot.send_message(chat_id, LANGUAGES[lang]["ticket_not_found"])
                clear_user_state(user_id)
                return
            reply_text = text.strip()
            if len(reply_text) < 2:
                bot.send_message(chat_id, "❌ Ответ слишком короткий!")
                return
            add_admin_response_to_ticket(ticket_id, reply_text)
            clear_user_state(user_id)
            try:
                target_user_id = ticket[1]
                target_lang = get_lang(target_user_id)
                bot.send_message(
                    target_user_id,
                    LANGUAGES[target_lang]["ticket_new_reply_user"].format(ticket_id, safe_text(reply_text)),
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.error(f"Ошибка отправки ответа на тикет {ticket_id}: {e}")
            bot.send_message(chat_id, LANGUAGES[lang]["ticket_reply_sent_admin"].format(ticket_id), reply_markup=get_admin_keyboard(user_id))
            return

        if isinstance(current_state, str) and current_state.startswith("awaiting_warn_reason_"):
            stage = int(current_state.split("_")[-1])
            parts = text.split(maxsplit=1)
            if len(parts) < 2:
                bot.send_message(chat_id, "❌ Неверный формат. Отправьте: <ID> <причина (мин. 10 символов)>")
                return
            try:
                target_id = int(parts[0])
            except ValueError:
                bot.send_message(chat_id, "❌ ID должен быть числом!")
                return
            reason = parts[1].strip()
            if len(reason) < 10:
                bot.send_message(chat_id, "❌ Причина должна содержать минимум 10 символов!")
                return
            user_db = get_user_from_db(target_id)
            if not user_db:
                bot.send_message(chat_id, "❌ Пользователь не найден в БД!")
                clear_user_state(user_id)
                return

            result = issue_warning(target_id, stage, reason, user_id)
            clear_user_state(user_id)
            if not result:
                bot.send_message(chat_id, "❌ Не удалось выдать предупреждение. Проверьте логи.")
                return

            username_target = f"@{user_db[1]}" if user_db and user_db[1] else f"ID {target_id}"
            if stage == 1:
                notif_text = f"🔔 Вам выдано <b>предупреждение</b>.\nПричина: {reason}"
            elif stage == 2:
                notif_text = (
                    f"⚠️ Вам выдан <b>ВЫГОВОР</b>.\nПричина: {reason}\n"
                    f"С вашего баланса списано 30%: {format_balance(result['old_balance'])} ₽ → {format_balance(result['new_balance'])} ₽"
                )
            else:
                notif_text = f"🚨 Вам выдан <b>СТРОГИЙ ВЫГОВОР</b>.\nПричина: {reason}"
            try:
                bot.send_message(target_id, notif_text, parse_mode="HTML")
            except Exception:
                pass
            notify_admins_about_warning(target_id, stage, reason, user_id)
            bot.send_message(chat_id, f"✅ Предупреждение (ур. {stage}) успешно выдано пользователю {username_target}", reply_markup=get_admin_keyboard(user_id))
            return

        if current_state == "awaiting_reset_balance":
            try:
                target_id = int(text.strip())
            except ValueError:
                bot.send_message(chat_id, "❌ ID должен быть числом!")
                return

            user_db = get_user_from_db(target_id)
            if not user_db:
                bot.send_message(chat_id, "❌ Пользователь не найден в БД!")
                clear_user_state(user_id)
                return

            user_data = get_user_data(target_id)
            old_balance = user_data.get("balance", 0.0)
            if old_balance > 0:
                update_user_balance(target_id, -old_balance)

            clear_user_state(user_id)
            username_target = f"@{user_db[1]}" if user_db and user_db[1] else f"ID {target_id}"
            bot.send_message(chat_id, f"✅ Баланс пользователя {username_target} обнулён.", reply_markup=get_admin_keyboard(user_id))
            return

        if current_state == "awaiting_broadcast":
            clear_user_state(user_id)
            user_states[f"broadcast_{user_id}"] = text
            markup = get_broadcast_confirm_keyboard()
            bot.send_message(
                chat_id,
                f"{LANGUAGES[lang]['broadcast_confirm']}\n\n<i>Ваше сообщение:</i>\n{text}",
                parse_mode="HTML",
                reply_markup=markup
            )
            return

    # ========================================
    # КНОПКИ
    # ========================================
    if btn_equals(text, LANGUAGES[lang]["top_up_balance"]):
        user_states[user_id] = "deposit_wait_amount"
        bot.send_message(chat_id, LANGUAGES[lang]["deposit_wait_amount"], parse_mode="HTML")
        return

    if btn_equals(text, LANGUAGES[lang]["products"]):
        bot.send_message(chat_id, "<b>♦️ КАТАЛОГ ТОВАРОВ</b>\n\nВыберите платформу:", parse_mode="HTML", reply_markup=get_platforms_keyboard())
        return

    if btn_equals(text, LANGUAGES[lang]["profile"]):
        user_data = get_user_data(user_id)
        username = message.from_user.username or message.from_user.first_name or "unknown"
        profile_text = (
            f"👤 <b>Ваш профиль</b>\n\n"
            f"🆔 ID: <code>{user_id}</code>\n"
            f"👤 Логин: @{username}\n"
            f"💰 Баланс: <b>{format_balance(user_data.get('balance', 0.0))} ₽</b>"
        )
        bot.send_message(chat_id, profile_text, parse_mode="HTML", reply_markup=get_profile_keyboard(user_id))
        return

    if btn_equals(text, LANGUAGES[lang]["settings"]):
        bot.send_message(chat_id, LANGUAGES[lang]["settings_content"], reply_markup=get_settings_keyboard(user_id))
        return

    if btn_equals(text, LANGUAGES[lang]["support"]):
        bot.send_message(chat_id, LANGUAGES[lang]["support_page_text"], parse_mode="HTML", reply_markup=get_rules_keyboard())
        return

    if btn_equals(text, LANGUAGES[lang]["language"]):
        current = LANGUAGES[lang]["rus"] if lang == "ru" else LANGUAGES[lang]["eng"]
        bot.send_message(chat_id, LANGUAGES[lang]["current_lang"].format(current), reply_markup=get_language_keyboard(user_id))
        return

    if btn_equals(text, LANGUAGES[lang]["rus"]):
        user_languages[user_id] = "ru"
        bot.send_message(chat_id, "✅ Язык изменен на Русский", reply_markup=get_main_keyboard(user_id))
        return

    if btn_equals(text, LANGUAGES[lang]["eng"]):
        user_languages[user_id] = "en"
        bot.send_message(chat_id, "✅ Language changed to English", reply_markup=get_main_keyboard(user_id))
        return

    if btn_equals(text, LANGUAGES[lang]["back_menu"]):
        bot.send_message(chat_id, LANGUAGES[lang]["welcome"], reply_markup=get_main_keyboard(user_id))
        return

    if is_admin(user_id) and btn_equals(text, LANGUAGES[lang]["ticket_view_list"]):
        total_tickets = count_open_tickets()
        if total_tickets == 0:
            bot.send_message(chat_id, LANGUAGES[lang]["ticket_no_open"])
            return
        total_pages = (total_tickets + TICKETS_PER_PAGE - 1) // TICKETS_PER_PAGE
        tickets = get_open_tickets_paginated(page=0)
        markup = types.InlineKeyboardMarkup()
        for ticket in tickets:
            status_text = TICKET_STATUSES.get(ticket[3], {}).get("ru", ticket[3])
            markup.add(types.InlineKeyboardButton(f"#{ticket[0]} от {ticket[1]} | {status_text}", callback_data=f"open_ticket_{ticket[0]}"))
        if total_pages > 1:
            markup.row(
                types.InlineKeyboardButton(" ", callback_data="ignore"),
                types.InlineKeyboardButton("Вперёд ▶️", callback_data="tickets_page_1")
            )
        markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_back_btn"))
        bot.send_message(chat_id, LANGUAGES[lang]["ticket_list_title"].format(1, total_pages), parse_mode="HTML", reply_markup=markup)
        return

    if is_admin(user_id) and btn_equals(text, LANGUAGES[lang]["check_deposits_btn"]):
        show_deposit_requests(chat_id)
        return

    if is_admin(user_id) and btn_equals(text, LANGUAGES[lang]["admin_panel"]):
        password = load_admin_password()
        if not password:
            if is_super_admin(user_id):
                bot.send_message(chat_id, LANGUAGES[lang]["no_password_set"], reply_markup=get_admin_keyboard(user_id))
            else:
                bot.send_message(chat_id, "Админ-панель недоступна, пароль не установлен.")
            return
        elif is_admin_session_valid(user_id):
            bot.send_message(chat_id, LANGUAGES[lang]["admin_menu_title"], parse_mode="HTML", reply_markup=get_admin_keyboard(user_id))
        else:
            user_states[user_id] = "awaiting_admin_password"
            bot.send_message(chat_id, LANGUAGES[lang]["enter_admin_password"], parse_mode="HTML")
        return

    if btn_equals(text, "🔓 Выдать доступ") and is_admin(user_id):
        bot.send_message(chat_id, "<b>🔓 Выдача доступа</b>\n\nВыберите способ:", parse_mode="HTML", reply_markup=get_access_menu_keyboard())
        return

    if btn_equals(text, "⚖️ Выдать варн") and is_super_admin(user_id):
        markup = types.InlineKeyboardMarkup()
        markup.row(types.InlineKeyboardButton("Уровень 1 - Предупреждение", callback_data="warn_level_1"))
        markup.row(types.InlineKeyboardButton("Уровень 2 - Выговор (-30% баланса)", callback_data="warn_level_2"))
        markup.row(types.InlineKeyboardButton("Уровень 3 - Строгий выговор", callback_data="warn_level_3"))
        markup.row(types.InlineKeyboardButton("🔙 Отмена", callback_data="admin_back_btn"))
        bot.send_message(chat_id, "🛡️ <b>Выберите уровень предупреждения:</b>", parse_mode="HTML", reply_markup=markup)
        return

    if btn_equals(text, "🧯 Обнулить") and is_admin(user_id):
        user_states[user_id] = "awaiting_reset_balance"
        bot.send_message(chat_id, "📌 Отправьте ID пользователя, баланс которого нужно обнулить.\n\nДля отмены: /cancel", parse_mode="HTML")
        return

    if btn_equals(text, LANGUAGES[lang]["broadcast"]) and is_admin(user_id):
        user_states[user_id] = "awaiting_broadcast"
        bot.send_message(chat_id, LANGUAGES[lang]["broadcast_prompt"], parse_mode="HTML")
        return

    if btn_equals(text, "🔙 Назад"):
        bot.send_message(chat_id, LANGUAGES[lang]["welcome"], reply_markup=get_main_keyboard(user_id))
        return

    if btn_equals(text, "🔑 Добавить ключ") and is_super_admin(user_id):
        bot.send_message(
            chat_id,
            "Используйте: <code>/addkey ПРОДУКТ ПЕРИОД КЛЮЧ</code>\nПример: <code>/addkey zolo 1d ABC123</code>",
            parse_mode="HTML"
        )
        return

    if btn_equals(text, "➕ Добавить админа") and is_super_admin(user_id):
        bot.send_message(chat_id, "Используйте: <code>/add_admin ID_пользователя</code>", parse_mode="HTML")
        return

    if btn_equals(text, "👑 Супер-админ") and is_super_admin(user_id):
        bot.send_message(chat_id, "Используйте: <code>/add_super_admin ID_пользователя</code>", parse_mode="HTML")
        return

    if btn_equals(text, "❌ Снять с админки") and is_super_admin(user_id):
        bot.send_message(chat_id, "Используйте: <code>/remove_admin ID_пользователя</code>", parse_mode="HTML")
        return

    if btn_equals(text, "🔐 Установить пароль админа") and is_super_admin(user_id):
        bot.send_message(chat_id, "Используйте: <code>/set_admin_password новый_пароль</code>", parse_mode="HTML")
        return

    if btn_equals(text, "📊 Наличие") and is_admin(user_id):
        lines = ["<b>📊 Наличие ключей по товарам:</b>"]
        for product_key in PRODUCTS:
            lines.append(f"\n<b>{PRODUCTS[product_key]['name']}</b>:")
            for period in PRODUCTS[product_key]["prices"]:
                count = get_keys_count(product_key, period)
                lines.append(f" • {PERIOD_DISPLAY[period]}: <b>{count} шт.</b>")
        bot.send_message(chat_id, "\n".join(lines), parse_mode="HTML")
        return

    if btn_equals(text, "💸 Пополнить") and is_admin(user_id):
        bot.send_message(chat_id, "Используйте: <code>/add_balance ID_пользователя сумма</code>", parse_mode="HTML")
        return

    bot.send_message(chat_id, LANGUAGES[lang]["unknown_command"], reply_markup=get_main_keyboard(user_id))


# ========================================
# ФОТОГРАФИИ
# ========================================
@bot.message_handler(content_types=["photo"])
def handle_user_photo(message):
    user_id = message.from_user.id
    if not check_user_access(user_id):
        send_access_denied(message)
        return

    if user_states.get(user_id) == "deposit_wait_screenshot":
        deposit_id = deposit_context.get(user_id)
        if not deposit_id:
            bot.send_message(message.chat.id, "❌ Ошибка: не найдена активная заявка. Создайте заявку заново.")
            clear_user_state(user_id)
            return

        file_id = message.photo[-1].file_id
        if update_deposit_screenshot(deposit_id, file_id):
            bot.send_message(
                message.chat.id,
                LANGUAGES[get_lang(user_id)]["deposit_received_screenshot"].format(deposit_id),
                parse_mode="HTML"
            )
        else:
            bot.send_message(message.chat.id, "❌ Не удалось сохранить скриншот.")
        clear_user_state(user_id)
        deposit_context.pop(user_id, None)
        return

    bot.send_message(
        message.chat.id,
        "ℹ️ Если хотите создать тикет с фото, используйте команду <code>/ticket [текст]</code> и прикрепите фото.",
        parse_mode="HTML"
    )


# ========================================
# CALLBACK ОБРАБОТЧИК
# ========================================
@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    user_id = call.from_user.id
    msg_id = call.message.message_id
    chat_id = call.message.chat.id
    lang = get_lang(user_id)

    if call.data == "ignore":
        bot.answer_callback_query(call.id)
        return

    # === МЕНЮ ДОСТУПА ===
    if call.data == "access_back":
        if not is_admin(user_id):
            bot.answer_callback_query(call.id, "⛔ Доступ запрещён")
            return
        bot.edit_message_text("<b>🔓 Выдача доступа</b>\n\nВыберите способ:", chat_id, msg_id, parse_mode="HTML", reply_markup=get_access_menu_keyboard())
        bot.answer_callback_query(call.id)
        return

    if call.data == "access_add_user":
        if not is_admin(user_id):
            bot.answer_callback_query(call.id, "⛔ Доступ запрещён")
            return
        user_states[user_id] = "awaiting_user_id_for_access"
        bot.edit_message_text(LANGUAGES[lang]["enter_user_id"], chat_id, msg_id, parse_mode="HTML")
        bot.answer_callback_query(call.id)
        return

    if call.data == "access_password_menu":
        if not is_admin(user_id):
            bot.answer_callback_query(call.id, "⛔ Доступ запрещён")
            return
        bot.edit_message_text("<b>🔑 Тип пароля</b>\n\nВыберите тип пароля:", chat_id, msg_id, parse_mode="HTML", reply_markup=get_password_type_keyboard())
        bot.answer_callback_query(call.id)
        return

    if call.data == "password_single":
        if not is_admin(user_id):
            bot.answer_callback_query(call.id, "⛔ Доступ запрещён")
            return
        password = generate_access_password("single", 1, user_id)
        if password:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔄 Сгенерировать ещё", callback_data="password_single"))
            markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data="access_password_menu"))
            bot.edit_message_text(
                LANGUAGES[lang]["password_generated"].format(password, "Единоразовый", 1),
                chat_id, msg_id, parse_mode="HTML", reply_markup=markup
            )
        else:
            bot.answer_callback_query(call.id, "❌ Ошибка при создании пароля", show_alert=True)
        return

    if call.data == "password_multi_menu":
        if not is_admin(user_id):
            bot.answer_callback_query(call.id, "⛔ Доступ запрещён")
            return
        bot.edit_message_text(LANGUAGES[lang]["select_max_uses"], chat_id, msg_id, parse_mode="HTML", reply_markup=get_multi_use_keyboard())
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("password_multi_"):
        if not is_admin(user_id):
            bot.answer_callback_query(call.id, "⛔ Доступ запрещён")
            return
        max_uses = int(call.data.split("_")[-1])
        password = generate_access_password("multi", max_uses, user_id)
        if password:
            markup = types.InlineKeyboardMarkup()
            markup.add(types.InlineKeyboardButton("🔄 Сгенерировать ещё", callback_data="password_single"))
            markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data="access_password_menu"))
            bot.edit_message_text(
                LANGUAGES[lang]["password_generated"].format(password, f"Многоразовый ({max_uses} использований)", max_uses),
                chat_id, msg_id, parse_mode="HTML", reply_markup=markup
            )
        else:
            bot.answer_callback_query(call.id, "❌ Ошибка при создании пароля", show_alert=True)
        return

    # === РАССЫЛКА ===
    if call.data == "broadcast_confirm":
        if not is_admin(user_id):
            return
        broadcast_text = user_states.pop(f"broadcast_{user_id}", None)
        if not broadcast_text:
            bot.answer_callback_query(call.id, "❌ Текст для рассылки не найден.")
            return
        bot.answer_callback_query(call.id, LANGUAGES[lang]["broadcast_started"])
        bot.edit_message_text(LANGUAGES[lang]["broadcast_started"], chat_id, msg_id, reply_markup=None)

        authorized_users = get_authorized_users()
        sent_count = 0
        for uid in authorized_users:
            try:
                bot.send_message(uid, broadcast_text, parse_mode="HTML", disable_web_page_preview=True)
                sent_count += 1
                time.sleep(0.05)
            except Exception:
                pass

        bot.send_message(chat_id, LANGUAGES[lang]["broadcast_completed"].format(sent_count))
        return

    if call.data == "broadcast_cancel":
        if not is_admin(user_id):
            return
        user_states.pop(f"broadcast_{user_id}", None)
        bot.answer_callback_query(call.id, "❌ Отменено")
        bot.edit_message_text("❌ Рассылка отменена.", chat_id, msg_id, reply_markup=None)
        return

    # === НАВИГАЦИЯ ===
    if call.data == "admin_back_btn":
        if not is_admin(user_id):
            return
        bot.answer_callback_query(call.id)
        try:
            bot.delete_message(chat_id, msg_id)
        except Exception:
            pass
        bot.send_message(chat_id, "Возвращаю в админ-меню...", reply_markup=get_admin_keyboard(user_id))
        return

    if call.data == "back_to_menu":
        try:
            bot.delete_message(chat_id, msg_id)
        except Exception:
            pass
        bot.send_message(chat_id, LANGUAGES[lang]["welcome"], reply_markup=get_main_keyboard(user_id))
        bot.answer_callback_query(call.id)
        return

    if call.data == "platform_android":
        text = f"<b>📱 Android</b>\n\n{LANGUAGES[lang]['choose_product']}"
        bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=get_catalog_keyboard())
        bot.answer_callback_query(call.id)
        return

    if call.data == "back_to_catalog":
        text = "<b>♦️ КАТАЛОГ ТОВАРОВ</b>\n\nВыберите платформу:"
        bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=get_platforms_keyboard())
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("product_"):
        product_key = call.data.replace("product_", "")
        product_name = PRODUCTS[product_key]["name"]
        text = f"<b>{product_name}</b>\n\n{LANGUAGES[lang]['choose_category']}"
        bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=get_categories_keyboard(product_key))
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("category_"):
        parts = call.data.split("_")
        product_key = parts[1]
        category_key = parts[2]
        product_name = PRODUCTS[product_key]["name"]
        text = f"<b>{product_name}</b>\n\n{LANGUAGES[lang]['choose_subscription']}"
        bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=get_subscriptions_keyboard(product_key, category_key))
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("select_qty_"):
        parts = call.data.split("_")
        product_key = parts[2]
        period = parts[3]
        price = PRODUCTS[product_key]["prices"][period]
        display = PERIOD_DISPLAY[period]
        product_name = PRODUCTS[product_key]["name"]
        stock = get_keys_count(product_key, period)
        text = (
            f"<b>🛍 {product_name} • {display}</b>\n"
            f"💵 Цена за 1 шт: {price} ₽\n"
            f"📦 В наличии: {stock} шт.\n\n"
            f"{LANGUAGES[lang]['select_qty_title']}"
        )
        markup = types.InlineKeyboardMarkup(row_width=3)
        buttons = [
            types.InlineKeyboardButton(f"{i} шт.", callback_data=f"confirm_qty_{product_key}_{period}_{i}")
            for i in range(1, 6)
        ]
        markup.add(*buttons)
        category_key = next((k for k, v in CATEGORIES.items() if period in v["periods"]), None)
        if category_key:
            markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data=f"category_{product_key}_{category_key}"))
        else:
            markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data=f"product_{product_key}"))
        bot.edit_message_text(text, chat_id, msg_id, parse_mode="HTML", reply_markup=markup)
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("confirm_qty_"):
        parts = call.data.split("_")
        product_key = parts[2]
        period = parts[3]
        quantity = int(parts[4])
        price = PRODUCTS[product_key]["prices"][period]
        total_price = price * quantity
        display = PERIOD_DISPLAY[period]
        product_name = PRODUCTS[product_key]["name"]
        full_product_name = f"{product_name} • {display}"
        stock = get_keys_count(product_key, period)

        if stock < quantity:
            bot.answer_callback_query(call.id, "❌ Недостаточно ключей на складе!", show_alert=True)
            return

        confirm_text = LANGUAGES[lang]["confirm_text"].format(full_product_name, quantity, total_price)
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton(f"✅ Подтвердить и купить за {total_price} ₽", callback_data=f"final_buy_{product_key}_{period}_{quantity}"))
        markup.add(types.InlineKeyboardButton("🔙 Изменить кол-во", callback_data=f"select_qty_{product_key}_{period}"))
        markup.add(types.InlineKeyboardButton("❌ Отмена", callback_data="back_to_menu"))
        bot.edit_message_text(f"<b>{LANGUAGES[lang]['confirm_title']}</b>\n\n{confirm_text}", chat_id, msg_id, parse_mode="HTML", reply_markup=markup)
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("final_buy_"):
        parts = call.data.split("_")
        product_key = parts[2]
        period = parts[3]
        quantity = int(parts[4])
        price = PRODUCTS[product_key]["prices"][period]
        total_price = price * quantity
        display = PERIOD_DISPLAY[period]
        product_name = PRODUCTS[product_key]["name"]
        full_product_name = f"{product_name} • {display}"

        with purchase_lock:
            user_data = get_user_data(user_id)
            balance = float(user_data.get("balance", 0.0))

            if balance < total_price:
                missing = total_price - balance
                text = LANGUAGES[lang]["insufficient_funds"].format(
                    format_balance(balance),
                    format_balance(total_price),
                    format_balance(missing)
                )
                bot.edit_message_text(
                    text,
                    chat_id,
                    msg_id,
                    parse_mode="HTML",
                    reply_markup=get_insufficient_funds_keyboard(user_id)
                )
                bot.answer_callback_query(call.id)
                return

            stock = get_keys_count(product_key, period)
            if stock < quantity:
                bot.answer_callback_query(call.id, "❌ Недостаточно ключей на складе!", show_alert=True)
                bot.edit_message_text("❌ Недостаточно ключей на складе!", chat_id, msg_id, reply_markup=None)
                return

            keys = get_available_keys(product_key, period, quantity)
            if not keys or len(keys) < quantity:
                bot.answer_callback_query(call.id, "❌ Недостаточно ключей на складе!", show_alert=True)
                bot.edit_message_text("❌ Недостаточно ключей на складе!", chat_id, msg_id, reply_markup=None)
                return

            new_balance = update_user_balance(user_id, -total_price)
            for key in keys:
                add_purchase_record(user_id, full_product_name, price, key)

            notify_admins_about_purchase(user_id, full_product_name, quantity, total_price, keys)

            keys_text = "\n".join([f"<code>{k}</code>" for k in keys])
            success_text = LANGUAGES[lang]["purchase_success"].format(
                full_product_name,
                quantity,
                format_balance(total_price),
                format_balance(new_balance),
                keys_text
            )
            bot.edit_message_text(success_text, chat_id, msg_id, parse_mode="HTML", reply_markup=get_rules_keyboard())
            bot.answer_callback_query(call.id, "✅ Покупка успешна!")
            return

    # === ДЕПОЗИТЫ ===
    if call.data.startswith("deposit_start_"):
        if int(call.data.split("_")[2]) != user_id:
            bot.answer_callback_query(call.id, "Вы можете создать заявку только для себя.")
            return
        user_states[user_id] = "deposit_wait_amount"
        deposit_context.pop(user_id, None)
        bot.answer_callback_query(call.id)
        bot.send_message(user_id, LANGUAGES[lang]["deposit_wait_amount"], parse_mode="HTML")
        try:
            bot.delete_message(chat_id, msg_id)
        except Exception:
            pass
        return

    if call.data.startswith("view_deposit_"):
        if not is_admin(user_id):
            return
        deposit_id = int(call.data.split("_")[-1])
        deposit = get_deposit_by_id(deposit_id)
        if not deposit:
            bot.answer_callback_query(call.id, "Заявка не найдена.", show_alert=True)
            return
        user_obj = get_user_from_db(deposit[1])
        uname = f"@{user_obj[1]}" if user_obj and user_obj[1] else f"ID {deposit[1]}"
        info_text = (
            f"📋 <b>ЗАЯВКА НА ПОПОЛНЕНИЕ №{deposit[0]}</b>\n\n"
            f"👤 Пользователь: {uname}\n"
            f"💵 Сумма: <b>{deposit[2]} ₽</b>\n"
            f"📊 Статус: {deposit[4]}\n"
            f"⏰ Создана: {format_timestamp(deposit[5])}"
        )
        markup = get_deposit_buttons(deposit_id)
        screenshot_file_id = deposit[3]
        if screenshot_file_id:
            try:
                bot.send_photo(chat_id, screenshot_file_id, caption=info_text, parse_mode="HTML", reply_markup=markup)
                bot.delete_message(chat_id, msg_id)
            except Exception:
                bot.edit_message_text(
                    info_text + "\n\n⚠️ <b>Не удалось загрузить скриншот.</b>",
                    chat_id,
                    msg_id,
                    parse_mode="HTML",
                    reply_markup=markup
                )
        else:
            bot.edit_message_text(info_text + "\n\n⚠️ <b>Скриншот не прикреплён!</b>", chat_id, msg_id, parse_mode="HTML", reply_markup=markup)
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("confirm_deposit_"):
        if not is_admin(user_id):
            return
        deposit_id = int(call.data.split("_")[-1])
        deposit = get_deposit_by_id(deposit_id)
        if not deposit or deposit[4] != "pending":
            bot.answer_callback_query(call.id, "Заявка уже обработана.", show_alert=True)
            return
        if update_deposit_status(deposit_id, "confirmed", user_id):
            target_user_id, amount, *_ = deposit
            new_bal = update_user_balance(target_user_id, amount)
            try:
                bot.send_message(
                    target_user_id,
                    LANGUAGES[get_lang(target_user_id)]["deposit_confirmed_msg"].format(int(amount), int(new_bal)),
                    parse_mode="HTML"
                )
            except Exception:
                pass
            bot.delete_message(chat_id, msg_id)
            bot.answer_callback_query(call.id, "✅ Подтверждено!")
            show_deposit_requests(chat_id)
        else:
            bot.answer_callback_query(call.id, "❌ Ошибка.", show_alert=True)
        return

    if call.data.startswith("reject_deposit_"):
        if not is_admin(user_id):
            return
        deposit_id = int(call.data.split("_")[-1])
        deposit = get_deposit_by_id(deposit_id)
        if not deposit or deposit[4] != "pending":
            bot.answer_callback_query(call.id, "Заявка уже обработана.", show_alert=True)
            return
        if update_deposit_status(deposit_id, "rejected", user_id):
            target_user_id, *_ = deposit
            try:
                bot.send_message(
                    target_user_id,
                    LANGUAGES[get_lang(target_user_id)]["deposit_rejected_msg"].format("Проверьте реквизиты и попробуйте снова"),
                    parse_mode="HTML"
                )
            except Exception:
                pass
            bot.delete_message(chat_id, msg_id)
            bot.answer_callback_query(call.id, "❌ Отклонено!")
            show_deposit_requests(chat_id)
        else:
            bot.answer_callback_query(call.id, "❌ Ошибка.", show_alert=True)
        return

    if call.data == "list_deposits":
        if not is_admin(user_id):
            return
        bot.delete_message(chat_id, msg_id)
        show_deposit_requests(chat_id)
        bot.answer_callback_query(call.id)
        return

    # === ТИКЕТЫ ===
    if call.data.startswith("open_ticket_"):
        if not is_admin(user_id):
            return
        ticket_id = int(call.data.replace("open_ticket_", ""))
        bot.answer_callback_query(call.id)
        ticket = get_ticket_by_id(ticket_id)
        if not ticket:
            bot.send_message(chat_id, LANGUAGES[lang]["ticket_not_found"])
            return
        if ticket[3] == "open":
            update_ticket_status(ticket_id, "in_progress")
            ticket = get_ticket_by_id(ticket_id)

        status_text = TICKET_STATUSES.get(ticket[3], {}).get("ru", ticket[3])
        admin_reply = ticket[5] if ticket[5] is not None else "<i>Ответа пока нет</i>"
        bot.edit_message_text(
            LANGUAGES[lang]["ticket_info_header"].format(
                ticket_id,
                ticket[1],
                format_timestamp(ticket[4]),
                status_text,
                safe_text(ticket[2]),
                safe_text(admin_reply)
            ),
            chat_id,
            msg_id,
            parse_mode="HTML"
        )
        bot.send_message(chat_id, LANGUAGES[lang]["ticket_awaiting_reply"])
        user_states[user_id] = f"awaiting_ticket_reply_{ticket_id}"
        return

    if call.data.startswith("tickets_page_"):
        if not is_admin(user_id):
            return
        page = int(call.data.replace("tickets_page_", ""))
        total_tickets = count_open_tickets()
        total_pages = (total_tickets + TICKETS_PER_PAGE - 1) // TICKETS_PER_PAGE
        page = max(0, min(page, max(total_pages - 1, 0)))
        tickets = get_open_tickets_paginated(page=page)
        markup = types.InlineKeyboardMarkup()
        for ticket in tickets:
            status_text = TICKET_STATUSES.get(ticket[3], {}).get("ru", ticket[3])
            markup.add(types.InlineKeyboardButton(f"#{ticket[0]} от {ticket[1]} | {status_text}", callback_data=f"open_ticket_{ticket[0]}"))
        nav_buttons = []
        if page > 0:
            nav_buttons.append(types.InlineKeyboardButton("◀️ Назад", callback_data=f"tickets_page_{page - 1}"))
        else:
            nav_buttons.append(types.InlineKeyboardButton(" ", callback_data="ignore"))
        if page < total_pages - 1:
            nav_buttons.append(types.InlineKeyboardButton("Вперёд ▶️", callback_data=f"tickets_page_{page + 1}"))
        else:
            nav_buttons.append(types.InlineKeyboardButton(" ", callback_data="ignore"))
        if total_pages > 1:
            markup.row(*nav_buttons)
        markup.add(types.InlineKeyboardButton("🔙 Назад", callback_data="admin_back_btn"))
        bot.edit_message_text(LANGUAGES[lang]["ticket_list_title"].format(page + 1, max(total_pages, 1)), chat_id, msg_id, parse_mode="HTML", reply_markup=markup)
        bot.answer_callback_query(call.id)
        return

    if call.data.startswith("view_my_ticket_"):
        ticket_id = int(call.data.replace("view_my_ticket_", ""))
        ticket = get_ticket_by_id(ticket_id)
        if not ticket or ticket[1] != user_id:
            bot.answer_callback_query(call.id, "Тикет не найден.", show_alert=True)
            return
        status_text = TICKET_STATUSES.get(ticket[3], {}).get("ru", ticket[3])
        text = (
            f"<b>Тикет #{ticket_id}</b>\n\n"
            f"<b>Статус:</b> {status_text}\n"
            f"<b>Создан:</b> {format_timestamp(ticket[4])}\n\n"
            f"<b>Ваше обращение:</b>\n{safe_text(ticket[2])}"
        )
        if ticket[5]:
            text += f"\n\n<b>Ответ администратора:</b>\n{safe_text(ticket[5])}"
        bot.send_message(chat_id, text, parse_mode="HTML")
        attachments = get_ticket_attachments(ticket_id)
        if attachments:
            media = [types.InputMediaPhoto(file_id) for file_id, file_type in attachments if file_type == "photo"]
            if media:
                bot.send_media_group(chat_id, media)
        bot.answer_callback_query(call.id)
        return

    # === ПРЕДУПРЕЖДЕНИЯ ===
    if call.data.startswith("warn_level_"):
        if not is_super_admin(user_id):
            return
        stage = int(call.data.split("_")[-1])
        user_states[user_id] = f"awaiting_warn_reason_{stage}"
        bot.edit_message_text(
            f"⚠️ <b>Выдача предупреждения (Уровень {stage})</b>\n\n"
            f"Отправьте сообщение в формате:\n<code>ID_пользователя причина</code>\n\n"
            f"<i>Пример: <code>123456789 Нарушение правил чата</code></i>\n\n"
            f"Для отмены используйте /cancel",
            chat_id,
            msg_id,
            parse_mode="HTML"
        )
        bot.answer_callback_query(call.id)
        return

    # === МОИ ТИКЕТЫ ===
    if call.data == "my_tickets":
        if not check_user_access(user_id):
            bot.answer_callback_query(call.id, "⛔ Доступ запрещён")
            return
        tickets = get_user_tickets(user_id)
        if not tickets:
            bot.answer_callback_query(call.id, "ℹ️ У вас нет тикетов.")
            return
        bot.answer_callback_query(call.id)
        bot.edit_message_text("📋 <b>Ваши тикеты:</b>", chat_id, msg_id, parse_mode="HTML", reply_markup=get_tickets_user_keyboard(tickets))
        return

    bot.answer_callback_query(call.id)


# ========================================
# ЗАПУСК
# ========================================
def main():
    try:
        init_database()
        refresh_key_files()
        restore_state_from_storage()
        init_admins_database()

        logger.info("========================================")
        logger.info("✅ БОТ ГОТОВ К РАБОТЕ")
        logger.info(f"🗄️ База данных: {DATABASE_FILE}")
        logger.info(f"📁 Директория ключей: {KEYS_FOLDER}")
        logger.info(f"👑 Супер-админы: {get_super_admins()}")
        logger.info("========================================")

        bot.remove_webhook()
        while True:
            try:
                bot.infinity_polling(timeout=20, long_polling_timeout=10, skip_pending=True)
            except Exception as e:
                logger.error(f"Критическая ошибка в цикле polling: {e}")
                logger.info("Перезапуск через 15 секунд...")
                time.sleep(15)

    except KeyboardInterrupt:
        logger.info("Бот остановлен пользователем (Ctrl+C)")
    except Exception as e:
        logger.critical(f"Критическая ошибка при запуске бота: {e}")
    finally:
        if db_connection:
            db_connection.close()
        logger.info("Соединение с БД закрыто")


if __name__ == "__main__":
    main()