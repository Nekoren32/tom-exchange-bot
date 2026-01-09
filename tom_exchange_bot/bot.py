# -*- coding: utf-8 -*-
"""
TOM EXCHANGE — простой Telegram-бот обменника (один файл).

Функции:
- Покупка / Продажа (FSM)
- SQLite (orders/users/settings)
- Авто-расчёт ₽:
  * Покупка: курс buy_rate, округление ВВЕРХ до ₽
  * Продажа: курс sell_rate, округление ВНИЗ до ₽
  * Ввод всегда в $
  * Мин сумма min_usd
- Продажа: клиент присылает ОДНИМ сообщением TX/скрин + "Выплата: ..."
- Бонус: скидка 20₽ после 5 выполненных покупок (только покупка)
- Личный кабинет + Мои заявки + /status
- Поддержка: кнопка-ссылка на @username оператора (задаётся в админке)
- Бан/разбан из заявки (и "Доступ запрещён" для забаненных)
- Админка: курсы/мин сумма/крипты/кошельки/юзер поддержки
- Рассылка оператором
"""

import os
import threading
import time
import sqlite3
import logging
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from datetime import datetime

import telebot
from telebot import custom_filters
from telebot.apihelper import ApiTelegramException
from telebot.handler_backends import State, StatesGroup
from telebot.storage import StateMemoryStorage
from telebot.types import (
    ReplyKeyboardMarkup, KeyboardButton,
    InlineKeyboardMarkup, InlineKeyboardButton,
    CallbackQuery, Message
)

# ---------------- Конфигурация (ТОЛЬКО из окружения) ----------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в окружении")

try:
    OPERATOR_ID = int(os.environ.get("OPERATOR_ID", "0"))
except ValueError:
    raise RuntimeError("OPERATOR_ID должен быть целым числом в окружении")

DB_PATH = os.environ.get("DB_PATH", "orders.db")

# Дефолтные кошельки (можно менять из админки)
LTC_WALLET_DEFAULT = os.environ.get("LTC_WALLET", "LWzfxJHnRswAhu5uYP1trdzVh68HrxYrDT")
USDT_WALLET_DEFAULT = os.environ.get("USDT_WALLET", "TBVKYMdP63hGm4wszvpRmsbUazCyriyYUT")

# ---------------- Дефолтные настройки (settings) ----------------
DEFAULT_BUY_RATE = "18.6"          # ₽ за 1$
DEFAULT_SELL_RATE = "16.5"         # ₽ за 1$
DEFAULT_MIN_USD = "10.00"          # минималка в $
DEFAULT_CRYPTOS = "USDT_TRON,LTC"  # доступные крипты
DEFAULT_SUPPORT_USERNAME = "@TOM_EXCH_PMR"

BONUS_BUY_AFTER = 5
BONUS_DISCOUNT_RUB = 20

ALLOWED_CRYPTOS = {"USDT_TRON", "LTC"}

# ---------------- Логирование ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

# ---------------- Инициализация бота ----------------
state_storage = StateMemoryStorage()
bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML", state_storage=state_storage)

# ---------------- Состояния ----------------
class OrderStates(StatesGroup):
    action = State()       # "buy" | "sell"
    amount = State()       # Decimal (строкой)
    crypto = State()       # "USDT_TRON" | "LTC"
    buy_method = State()   # "transfer" | "requisites" (для покупки)
    wait_tx = State()      # ожидание TX/скрина + реквизитов (для продажи)

class BroadcastStates(StatesGroup):
    wait_content = State()
    confirm = State()

class AdminStates(StatesGroup):
    choose = State()
    wait_value = State()
    wait_wallet_crypto = State()

# ---------------- SQLite helper ----------------
def get_conn():
    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        pass
    return conn

def db_init():
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
        CREATE TABLE IF NOT EXISTS orders (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            full_name TEXT,
            action TEXT,
            amount TEXT,
            crypto TEXT,
            tx_info TEXT,
            status TEXT,
            created_at TEXT
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            full_name TEXT,
            first_seen TEXT,
            last_seen TEXT,
            blocked INTEGER DEFAULT 0
        )
        """)
        cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """)
        conn.commit()

# --- settings ---
def db_get_setting(key: str, default: str = None) -> str:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cur.fetchone()
        return row[0] if row and row[0] is not None else default

def db_set_setting(key: str, value: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO settings(key, value) VALUES(?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value
        """, (key, value))
        conn.commit()

# --- users ---
def db_upsert_user(user):
    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO users (user_id, username, full_name, first_seen, last_seen, blocked)
            VALUES (?, ?, ?, ?, ?, 0)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                full_name = excluded.full_name,
                last_seen = excluded.last_seen
        """, (user.id, user.username, full_name, now, now))
        conn.commit()

def db_set_user_blocked(user_id: int, blocked: bool):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute(
            "UPDATE users SET blocked = ?, last_seen = ? WHERE user_id = ?",
            (1 if blocked else 0, datetime.utcnow().isoformat(), user_id)
        )
        conn.commit()

def db_is_user_blocked(user_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT blocked FROM users WHERE user_id = ?", (user_id,))
        row = cur.fetchone()
        return bool(row and row[0] == 1)

def db_all_user_ids(only_active=True):
    with get_conn() as conn:
        cur = conn.cursor()
        if only_active:
            cur.execute("SELECT user_id FROM users WHERE blocked = 0")
        else:
            cur.execute("SELECT user_id FROM users")
        return [r[0] for r in cur.fetchall()]

# --- orders ---
def db_create_order(user, action, amount, crypto, tx_info) -> int:
    with get_conn() as conn:
        cur = conn.cursor()
        full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
        now = datetime.utcnow().isoformat()
        cur.execute("""
            INSERT INTO orders (user_id, username, full_name, action, amount, crypto, tx_info, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (user.id, user.username, full_name, action, str(amount), crypto, tx_info, "pending", now))
        conn.commit()
        return cur.lastrowid

def db_update_status(order_id: int, status: str):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE orders SET status = ? WHERE id = ?", (status, order_id))
        conn.commit()

def db_get_order(order_id: int):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, user_id, username, full_name, action, amount, crypto, tx_info, status, created_at
            FROM orders WHERE id = ?
        """, (order_id,))
        return cur.fetchone()

def db_count_approved_buys(user_id: int) -> int:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT COUNT(*) FROM orders
            WHERE user_id = ? AND action = 'buy' AND status = 'approved'
        """, (user_id,))
        return int(cur.fetchone()[0] or 0)

def db_count_orders(user_id: int) -> int:
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM orders WHERE user_id = ?", (user_id,))
        return int(cur.fetchone()[0] or 0)

def db_last_orders(user_id: int, limit: int = 5):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("""
            SELECT id, action, amount, crypto, status, created_at
            FROM orders
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT ?
        """, (user_id, limit))
        return cur.fetchall()

# ---------------- Утилиты ----------------
def escape_html(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def user_link(u) -> str:
    name = (f"{u.first_name or ''} {u.last_name or ''}").strip() or f"id:{u.id}"
    return f'<a href="tg://user?id={u.id}">{escape_html(name)}</a>'

def status_human(status: str) -> str:
    if status == "pending":
        return "⏳ Ожидает"
    if status == "approved":
        return "✅ Одобрено"
    if status == "rejected":
        return "❌ Отклонено"
    return status or "—"

def get_user_status(buys: int) -> str:
    if buys < 5:
        return "Новичок"
    elif buys < 9:
        return "Уверенный пользователь"
    elif buys < 15:
        return "Опытный клиент"
    elif buys < 20:
        return "Постоянный клиент"
    elif buys < 25:
        return "Важный клиент"
    else:
        return "VIP-клиент"

def parse_amount(text: str):
    try:
        t = text.replace(",", ".").strip()
        amt = Decimal(t)
        if amt <= 0:
            return None
        return amt.quantize(Decimal("0.01"))
    except (InvalidOperation, AttributeError):
        return None

def get_buy_rate() -> Decimal:
    return Decimal(db_get_setting("buy_rate", DEFAULT_BUY_RATE))

def get_sell_rate() -> Decimal:
    return Decimal(db_get_setting("sell_rate", DEFAULT_SELL_RATE))

def get_min_usd() -> Decimal:
    return Decimal(db_get_setting("min_usd", DEFAULT_MIN_USD))

def get_enabled_cryptos():
    raw = db_get_setting("cryptos", DEFAULT_CRYPTOS) or DEFAULT_CRYPTOS
    items = [x.strip() for x in raw.split(",") if x.strip()]
    items = [x for x in items if x in ALLOWED_CRYPTOS]
    return items or ["USDT_TRON", "LTC"]

def get_wallet(code: str) -> str:
    default_wallet = USDT_WALLET_DEFAULT if code == "USDT_TRON" else LTC_WALLET_DEFAULT
    return db_get_setting(f"wallet_{code}", default_wallet)

def get_support_username() -> str:
    v = (db_get_setting("support_username", DEFAULT_SUPPORT_USERNAME) or DEFAULT_SUPPORT_USERNAME).strip()
    if not v.startswith("@"):
        v = "@" + v
    return v

def calc_rub(action: str, usd_amount: Decimal) -> int:
    """Покупка: округление вверх, Продажа: округление вниз (пользователю про округление не пишем)."""
    if action == "buy":
        rub = (usd_amount * get_buy_rate()).quantize(Decimal("1"), rounding=ROUND_CEILING)
    else:
        rub = (usd_amount * get_sell_rate()).quantize(Decimal("1"), rounding=ROUND_FLOOR)
    return int(rub)

def crypto_human(code: str) -> str:
    return "USDT (TRC20)" if code == "USDT_TRON" else "LTC"

def split_tx_and_payout(text: str):
    """Достаём TX и реквизиты 'Выплата:' из текста (если есть)."""
    t = (text or "").strip()
    if not t:
        return "", ""

    lines = [x.strip() for x in t.splitlines() if x.strip()]
    payout = ""
    tx = ""

    for line in lines:
        low = line.lower()
        if low.startswith(("выплата:", "карта:", "переводилка:", "payout:")):
            payout = line.split(":", 1)[1].strip() if ":" in line else line
        elif low.startswith(("tx:", "hash:", "хеш:", "хэш:")):
            tx = line.split(":", 1)[1].strip() if ":" in line else line

    if not tx and len(lines) == 1:
        tx = lines[0]

    return tx, payout

# ---------------- Клавиатуры ----------------
def main_menu(is_operator: bool = False) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("💰 Купить крипту"), KeyboardButton("💸 Продать крипту"))
    kb.add(KeyboardButton("👤 Личный кабинет"), KeyboardButton("📄 Мои заявки"))
    kb.add(KeyboardButton("🎁 Бонусы"), KeyboardButton("👨‍💻 Поддержка"))
    if is_operator:
        kb.add(KeyboardButton("⚙️ Админка"), KeyboardButton("📢 Рассылка"))
    return kb

def crypto_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    enabled = get_enabled_cryptos()
    buttons = []
    for code in enabled:
        if code == "USDT_TRON":
            buttons.append(InlineKeyboardButton("USDT (TRC20)", callback_data="crypto:USDT_TRON"))
        elif code == "LTC":
            buttons.append(InlineKeyboardButton("LTC", callback_data="crypto:LTC"))
    if buttons:
        kb.add(*buttons)
    return kb

def buymethod_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("Переводилка", callback_data="buymethod:transfer"),
        InlineKeyboardButton("Реквизиты", callback_data="buymethod:requisites")
    )
    return kb

def confirm_kb_for_sell() -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("📤 Я отправил"))
    kb.add(KeyboardButton("Отмена"))
    return kb

def operator_kb(order_id: int, user_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("✔ Подтвердить", callback_data=f"approve:{order_id}"),
        InlineKeyboardButton("✖ Отклонить", callback_data=f"reject:{order_id}")
    )
    kb.add(
        InlineKeyboardButton("🚫 Заблокировать", callback_data=f"ban:{user_id}"),
        InlineKeyboardButton("✅ Разблокировать", callback_data=f"unban:{user_id}")
    )
    return kb

def myorders_kb():
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("🔄 Обновить", callback_data="myorders:refresh"))
    return kb

def admin_menu_kb():
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("Курс покупки"))
    kb.add(KeyboardButton("Курс продажи"))
    kb.add(KeyboardButton("Мин сумма"))
    kb.add(KeyboardButton("Добавление криптовалюты"))
    kb.add(KeyboardButton("Кошельки"))
    kb.add(KeyboardButton("Юзер оператора"))
    kb.add(KeyboardButton("⬅ Назад"))
    return kb

# ---------------- Safe send helpers ----------------
def _extract_retry_after(exc):
    try:
        res = getattr(exc, "result", None)
        if isinstance(res, dict):
            params = res.get("parameters", {})
            ra = params.get("retry_after")
            if ra:
                return int(ra)
    except Exception:
        pass
    return None

def safe_send_message(chat_id, text, **kwargs):
    try:
        return bot.send_message(chat_id, text, **kwargs)
    except ApiTelegramException as e:
        logger.warning("ApiTelegramException send_message to %s: %s", chat_id, e)
        if e.error_code == 403:
            try:
                db_set_user_blocked(chat_id, True)
            except Exception:
                logger.exception("Не удалось пометить пользователя как заблокированного")
            return None
        if e.error_code == 429:
            retry = _extract_retry_after(e) or 5
            time.sleep(retry + 1)
            try:
                return bot.send_message(chat_id, text, **kwargs)
            except Exception:
                logger.exception("Ошибка после retry send_message")
                return None
        logger.exception("Необработанная ошибка send_message")
        return None
    except Exception:
        logger.exception("Ошибка send_message")
        return None

def safe_copy_message(chat_id, from_chat_id, message_id):
    try:
        return bot.copy_message(chat_id, from_chat_id, message_id)
    except ApiTelegramException as e:
        logger.warning("ApiTelegramException copy_message to %s: %s", chat_id, e)
        if e.error_code == 403:
            try:
                db_set_user_blocked(chat_id, True)
            except Exception:
                logger.exception("Не удалось пометить пользователя как заблокированного")
            return None
        if e.error_code == 429:
            retry = _extract_retry_after(e) or 5
            time.sleep(retry + 1)
            try:
                return bot.copy_message(chat_id, from_chat_id, message_id)
            except Exception:
                logger.exception("Ошибка после retry copy_message")
                return None
        return None
    except Exception:
        logger.exception("Ошибка copy_message")
        return None

def safe_send_photo(chat_id, photo, caption=None, **kwargs):
    try:
        return bot.send_photo(chat_id, photo, caption=caption, **kwargs)
    except ApiTelegramException as e:
        logger.warning("ApiTelegramException send_photo to %s: %s", chat_id, e)
        if e.error_code == 403:
            try:
                db_set_user_blocked(chat_id, True)
            except Exception:
                logger.exception("Не удалось пометить пользователя как заблокированного")
            return None
        if e.error_code == 429:
            retry = _extract_retry_after(e) or 5
            time.sleep(retry + 1)
            try:
                return bot.send_photo(chat_id, photo, caption=caption, **kwargs)
            except Exception:
                logger.exception("Ошибка после retry send_photo")
                return None
        return None
    except Exception:
        logger.exception("Ошибка send_photo")
        return None

# ---------------- Трекинг пользователей ----------------
def listener(messages):
    for msg in messages:
        try:
            if getattr(msg, "from_user", None):
                db_upsert_user(msg.from_user)
        except Exception:
            logger.exception("Ошибка при апдейте пользователя из listener")

bot.set_update_listener(listener)

# ---------------- Проверка блокировки ----------------
def deny_if_blocked(user_id: int, chat_id: int) -> bool:
    if user_id == OPERATOR_ID:
        return False
    if db_is_user_blocked(user_id):
        bot.send_message(chat_id, "🚫 Доступ запрещён.")
        return True
    return False

# ---------------- Хэндлеры ----------------
@bot.message_handler(commands=["start"])
def cmd_start(m: Message):
    is_op = (m.from_user.id == OPERATOR_ID)
    if deny_if_blocked(m.from_user.id, m.chat.id):
        return
    try:
        bot.delete_state(m.from_user.id, m.chat.id)
    except Exception:
        pass
    bot.send_message(
        m.chat.id,
        "Вас приветствует <b>TOM EXCHANGE</b> 👋\n"
        "Выберите действие ниже.",
        reply_markup=main_menu(is_op)
    )

@bot.message_handler(commands=["cancel"])
def cmd_cancel(m: Message):
    is_op = (m.from_user.id == OPERATOR_ID)
    if deny_if_blocked(m.from_user.id, m.chat.id):
        return
    try:
        bot.delete_state(m.from_user.id, m.chat.id)
    except Exception:
        pass
    bot.send_message(m.chat.id, "Действие отменено.", reply_markup=main_menu(is_op))

@bot.message_handler(func=lambda msg: getattr(msg, "text", "") == "Отмена")
def cancel_btn(m: Message):
    is_op = (m.from_user.id == OPERATOR_ID)
    if deny_if_blocked(m.from_user.id, m.chat.id):
        return
    try:
        bot.delete_state(m.from_user.id, m.chat.id)
    except Exception:
        pass
    bot.send_message(m.chat.id, "Отменено.", reply_markup=main_menu(is_op))

# --- Поддержка ---
@bot.message_handler(func=lambda msg: getattr(msg, "text", "") == "👨‍💻 Поддержка")
def support(m: Message):
    if deny_if_blocked(m.from_user.id, m.chat.id):
        return
    is_op = (m.from_user.id == OPERATOR_ID)
    op = get_support_username()
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("💬 Написать оператору", url=f"https://t.me/{op.lstrip('@')}"))
    bot.send_message(m.chat.id, f"Оператор: <b>{escape_html(op)}</b>", reply_markup=kb)
    bot.send_message(m.chat.id, "Вернуться в меню: /start", reply_markup=main_menu(is_op))

# --- Бонусы ---
@bot.message_handler(func=lambda m: getattr(m, "text", "") == "🎁 Бонусы")
def bonuses(m: Message):
    if deny_if_blocked(m.from_user.id, m.chat.id):
        return
    is_op = (m.from_user.id == OPERATOR_ID)
    done = db_count_approved_buys(m.from_user.id)
    left = max(0, BONUS_BUY_AFTER - done)
    if done >= BONUS_BUY_AFTER:
        bot.send_message(
            m.chat.id,
            f"🎁 Скидка <b>{BONUS_DISCOUNT_RUB} ₽</b> активна и применяется при покупке.",
            reply_markup=main_menu(is_op)
        )
    else:
        bot.send_message(
            m.chat.id,
            f"🎁 До скидки осталось покупок: <b>{left}</b>\nВыполнено покупок: <b>{done}</b>",
            reply_markup=main_menu(is_op)
        )

# --- Личный кабинет ---
@bot.message_handler(func=lambda m: getattr(m, "text", "") == "👤 Личный кабинет")
def profile(m: Message):
    if deny_if_blocked(m.from_user.id, m.chat.id):
        return
    is_op = (m.from_user.id == OPERATOR_ID)
    total_orders = db_count_orders(m.from_user.id)
    approved_buys = db_count_approved_buys(m.from_user.id)
    status = get_user_status(approved_buys)

    if approved_buys >= BONUS_BUY_AFTER:
        bonus_text = f"Скидка: {BONUS_DISCOUNT_RUB} ₽ (активна)"
    else:
        left = BONUS_BUY_AFTER - approved_buys
        bonus_text = f"До скидки: {left} покупок"

    bot.send_message(
        m.chat.id,
        f"👤 <b>Личный кабинет</b>\n\n"
        f"ID: <code>{m.from_user.id}</code>\n"
        f"Статус: <b>{escape_html(status)}</b>\n"
        f"Всего заявок: <b>{total_orders}</b>\n"
        f"Покупок выполнено: <b>{approved_buys}</b>\n"
        f"🎁 {escape_html(bonus_text)}",
        reply_markup=main_menu(is_op)
    )

# --- Мои заявки ---
@bot.message_handler(func=lambda m: getattr(m, "text", "") == "📄 Мои заявки")
def my_orders(m: Message):
    if deny_if_blocked(m.from_user.id, m.chat.id):
        return
    rows = db_last_orders(m.from_user.id, 5)
    if not rows:
        bot.send_message(m.chat.id, "У вас пока нет заявок.", reply_markup=main_menu(m.from_user.id == OPERATOR_ID))
        return

    lines = ["📄 <b>Ваши последние заявки</b>\n"]
    for oid, action, amount, crypto, status, created_at in rows:
        a = "Покупка" if action == "buy" else "Продажа"
        lines.append(f"#{oid} — {a} — {amount}$ — {crypto_human(crypto)} — <b>{status_human(status)}</b>")

    bot.send_message(m.chat.id, "\n".join(lines), reply_markup=myorders_kb())

@bot.callback_query_handler(func=lambda c: c.data == "myorders:refresh")
def myorders_refresh(c: CallbackQuery):
    if deny_if_blocked(c.from_user.id, c.message.chat.id):
        bot.answer_callback_query(c.id)
        return

    rows = db_last_orders(c.from_user.id, 5)
    if not rows:
        text = "У вас пока нет заявок."
    else:
        lines = ["📄 <b>Ваши последние заявки</b>\n"]
        for oid, action, amount, crypto, status, created_at in rows:
            a = "Покупка" if action == "buy" else "Продажа"
            lines.append(f"#{oid} — {a} — {amount}$ — {crypto_human(crypto)} — <b>{status_human(status)}</b>")
        text = "\n".join(lines)

    bot.answer_callback_query(c.id, "Обновлено")
    try:
        bot.edit_message_text(
            text,
            chat_id=c.message.chat.id,
            message_id=c.message.message_id,
            reply_markup=myorders_kb()
        )
    except Exception:
        bot.send_message(c.message.chat.id, text, reply_markup=myorders_kb())

# --- /status 123 ---
@bot.message_handler(commands=["status"])
def cmd_status(m: Message):
    if deny_if_blocked(m.from_user.id, m.chat.id):
        return

    parts = (m.text or "").split(maxsplit=1)
    if len(parts) < 2:
        bot.send_message(m.chat.id, "Напишите так: /status 123")
        return

    s = parts[1].strip().lstrip("#")
    if not s.isdigit():
        bot.send_message(m.chat.id, "ID должен быть числом. Пример: /status 123")
        return

    order_id = int(s)
    row = db_get_order(order_id)
    if not row:
        bot.send_message(m.chat.id, "Заявка не найдена.")
        return

    user_id = row[1]
    if m.from_user.id != OPERATOR_ID and user_id != m.from_user.id:
        bot.send_message(m.chat.id, "Это не ваша заявка.")
        return

    status = row[8]
    action = row[4]
    amount = row[5]
    crypto = row[6]
    a = "Покупка" if action == "buy" else "Продажа"

    bot.send_message(
        m.chat.id,
        f"📌 Заявка <b>#{order_id}</b>\n"
        f"{a} — {amount}$ — {crypto_human(crypto)}\n"
        f"Статус: <b>{status_human(status)}</b>"
    )

# --- Старт покупки/продажи ---
@bot.message_handler(func=lambda msg: getattr(msg, "text", "") == "💰 Купить крипту")
def buy_crypto(m: Message):
    if deny_if_blocked(m.from_user.id, m.chat.id):
        return
    bot.set_state(m.from_user.id, OrderStates.action, m.chat.id)
    with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
        data["action"] = "buy"
    bot.set_state(m.from_user.id, OrderStates.amount, m.chat.id)
    kb = ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("Отмена"))
    bot.send_message(m.chat.id, "Введите сумму в $ (например, 150 или 150.50)", reply_markup=kb)

@bot.message_handler(func=lambda msg: getattr(msg, "text", "") == "💸 Продать крипту")
def sell_crypto(m: Message):
    if deny_if_blocked(m.from_user.id, m.chat.id):
        return
    bot.set_state(m.from_user.id, OrderStates.action, m.chat.id)
    with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
        data["action"] = "sell"
    bot.set_state(m.from_user.id, OrderStates.amount, m.chat.id)
    kb = ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("Отмена"))
    bot.send_message(m.chat.id, "Введите сумму в $ (например, 200 или 200.00)", reply_markup=kb)

# --- Ввод суммы ---
@bot.message_handler(state=OrderStates.amount, content_types=["text"])
def handle_amount(m: Message):
    if deny_if_blocked(m.from_user.id, m.chat.id):
        return

    amt = parse_amount(m.text)
    if not amt:
        bot.send_message(m.chat.id, "Введите корректную сумму > 0. Пример: 100 или 100.50")
        return

    min_usd = get_min_usd()
    if amt < min_usd:
        bot.send_message(m.chat.id, f"Минимальная сумма: <b>{min_usd}$</b>. Введите сумму заново.")
        return

    with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
        action = data.get("action")
        if action not in ["buy", "sell"]:
            bot.send_message(m.chat.id, "Заявка потеряна. Нажмите /start")
            try:
                bot.delete_state(m.from_user.id, m.chat.id)
            except Exception:
                pass
            return
        data["amount"] = str(amt)

    rub = calc_rub(action, amt)

    # скидка только при покупке
    if action == "buy":
        discount = BONUS_DISCOUNT_RUB if db_count_approved_buys(m.from_user.id) >= BONUS_BUY_AFTER else 0
        pay_rub = max(0, rub - discount)
        if discount > 0:
            bot.send_message(
                m.chat.id,
                f"💱 <b>Расчёт заявки</b>\n\n"
                f"Сумма: <b>{amt}$</b>\n"
                f"К оплате: <b>{pay_rub} ₽</b>\n"
                f"Скидка: <b>{discount} ₽</b>"
            )
        else:
            bot.send_message(
                m.chat.id,
                f"💱 <b>Расчёт заявки</b>\n\n"
                f"Сумма: <b>{amt}$</b>\n"
                f"К оплате: <b>{rub} ₽</b>"
            )
    else:
        bot.send_message(
            m.chat.id,
            f"💱 <b>Расчёт заявки</b>\n\n"
            f"Сумма: <b>{amt}$</b>\n"
            f"Вы получите: <b>{rub} ₽</b>"
        )

    bot.set_state(m.from_user.id, OrderStates.crypto, m.chat.id)
    bot.send_message(m.chat.id, "Выберите криптовалюту:", reply_markup=ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("Отмена")))
    bot.send_message(m.chat.id, "Доступные варианты:", reply_markup=crypto_kb())

# --- Выбор криптовалюты (inline) ---
@bot.callback_query_handler(func=lambda c: c.data.startswith("crypto:"), state=OrderStates.crypto)
def select_crypto(c: CallbackQuery):
    if deny_if_blocked(c.from_user.id, c.message.chat.id):
        bot.answer_callback_query(c.id)
        return

    code = c.data.split(":", 1)[1]
    if code not in get_enabled_cryptos():
        bot.answer_callback_query(c.id, "Неверный выбор")
        return

    with bot.retrieve_data(c.from_user.id, c.message.chat.id) as data:
        data["crypto"] = code
        action = data.get("action")
        amt = data.get("amount")

    bot.answer_callback_query(c.id)
    human = crypto_human(code)

    if action == "buy":
        bot.set_state(c.from_user.id, OrderStates.buy_method, c.message.chat.id)
        bot.send_message(
            c.message.chat.id,
            f"Заявка: Покупка\nСумма: <b>{escape_html(str(amt))}$</b>\nКриптовалюта: <b>{escape_html(human)}</b>\n\n"
            "Выберите способ оплаты:",
            reply_markup=buymethod_kb()
        )
    else:
        wallet = get_wallet(code)
        bot.send_message(
            c.message.chat.id,
            f"Заявка: Продажа\nСумма: <b>{escape_html(str(amt))}$</b>\nКриптовалюта: <b>{escape_html(human)}</b>"
        )
        bot.send_message(
            c.message.chat.id,
            f"Отправьте <b>{escape_html(human)}</b> на адрес:\n<code>{escape_html(wallet)}</code>\n\n"
            "После отправки нажмите «Я отправил».",
            reply_markup=confirm_kb_for_sell()
        )

# --- Покупка: выбор способа оплаты (inline) ---
@bot.callback_query_handler(func=lambda c: c.data.startswith("buymethod:"), state=OrderStates.buy_method)
def select_buy_method(c: CallbackQuery):
    if deny_if_blocked(c.from_user.id, c.message.chat.id):
        bot.answer_callback_query(c.id)
        return

    method_code = c.data.split(":", 1)[1]
    if method_code not in ["transfer", "requisites"]:
        bot.answer_callback_query(c.id, "Неверный выбор")
        return

    with bot.retrieve_data(c.from_user.id, c.message.chat.id) as data:
        action = data.get("action")
        amount = data.get("amount")
        crypto = data.get("crypto")

    if action != "buy" or not all([amount, crypto]):
        bot.answer_callback_query(c.id, "Заявка потеряна. Нажмите /start")
        bot.delete_state(c.from_user.id, c.message.chat.id)
        return

    bot.answer_callback_query(c.id)
    human = crypto_human(crypto)
    method_human = "Переводилка" if method_code == "transfer" else "Реквизиты"

    discount = BONUS_DISCOUNT_RUB if db_count_approved_buys(c.from_user.id) >= BONUS_BUY_AFTER else 0

    order_id = db_create_order(c.from_user, "buy", amount, crypto, f"buy_method:{method_code}")

    extra = f"\n🎁 Скидка: <b>{discount} ₽</b>" if discount > 0 else ""
    text = (
        f"📩 <b>Новая заявка — ПОКУПКА</b>\n\n"
        f"ID заявки: <b>#{order_id}</b>\n"
        f"Пользователь: {user_link(c.from_user)} @{escape_html(c.from_user.username or '—')}\n"
        f"Сумма: <b>{escape_html(str(amount))}$</b>\n"
        f"Криптовалюта: <b>{escape_html(human)}</b>\n"
        f"Способ оплаты: <b>{method_human}</b>\n"
        f"Статус: <b>pending</b>{extra}"
    )
    safe_send_message(OPERATOR_ID, text, reply_markup=operator_kb(order_id, c.from_user.id))

    bot.send_message(
        c.message.chat.id,
        f"✅ Заявка отправлена! Номер: <b>#{order_id}</b>\nОжидайте ответа оператора.",
        reply_markup=main_menu(False)
    )
    bot.delete_state(c.from_user.id, c.message.chat.id)

# --- Продажа: клиент нажал "Я отправил" ---
@bot.message_handler(func=lambda m: getattr(m, "text", "") == "📤 Я отправил")
def confirm_sent(m: Message):
    if deny_if_blocked(m.from_user.id, m.chat.id):
        return

    state = bot.get_state(m.from_user.id, m.chat.id)
    if state is None:
        bot.send_message(m.chat.id, "Нет активной заявки. Нажмите /start", reply_markup=main_menu(m.from_user.id == OPERATOR_ID))
        return

    with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
        action = data.get("action")
        if action != "sell":
            bot.send_message(m.chat.id, "Эта кнопка доступна только в процессе продажи. Нажмите /start", reply_markup=main_menu(m.from_user.id == OPERATOR_ID))
            return

    bot.set_state(m.from_user.id, OrderStates.wait_tx, m.chat.id)
    bot.send_message(
        m.chat.id,
        "Отправьте <b>ОДНИМ сообщением</b>:\n\n"
        "— хеш транзакции <b>или</b> скриншот\n"
        "— реквизиты для выплаты (Переводилка или номер карты)\n\n"
        "Пример:\n"
        "TX: abcd1234...\n"
        "Выплата: Карта 2200 0000 0000 0000"
    )

# --- Продажа: приём TX/фото + реквизитов ---
@bot.message_handler(state=OrderStates.wait_tx, content_types=["text", "photo"])
def receive_tx(m: Message):
    if deny_if_blocked(m.from_user.id, m.chat.id):
        return

    with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
        action = data.get("action")
        amount = data.get("amount")
        crypto = data.get("crypto")

    if action != "sell" or not all([amount, crypto]):
        bot.send_message(m.chat.id, "Данные заявки потеряны. Начните заново: /start", reply_markup=main_menu(m.from_user.id == OPERATOR_ID))
        bot.delete_state(m.from_user.id, m.chat.id)
        return

    tx_info = ""
    photo_id = None
    tx_text = ""
    payout = ""

    if m.content_type == "text":
        tx_text, payout = split_tx_and_payout(m.text)
        tx_info = (m.text or "").strip()

    elif m.content_type == "photo":
        photo_id = m.photo[-1].file_id
        cap = (m.caption or "").strip()
        tx_text, payout = split_tx_and_payout(cap)
        tx_info = f"photo:{photo_id}" + (f"\n{cap}" if cap else "")

    # простая защита: реквизиты выплаты обязательны
    if not payout:
        bot.send_message(m.chat.id, "❗️Не вижу реквизитов.\nНапишите строкой: <b>Выплата: ...</b>")
        return

    order_id = db_create_order(m.from_user, "sell", amount, crypto, tx_info)
    human = crypto_human(crypto)

    payout_line = f"Выплата: <b>{escape_html(payout)}</b>\n"
    tx_line = escape_html(tx_text) if tx_text else ("—" if not photo_id else "скриншот во вложении")

    text = (
        f"📩 <b>Новая заявка — ПРОДАЖА</b>\n\n"
        f"ID заявки: <b>#{order_id}</b>\n"
        f"Пользователь: {user_link(m.from_user)} @{escape_html(m.from_user.username or '—')}\n"
        f"Сумма: <b>{escape_html(str(amount))}$</b>\n"
        f"Криптовалюта: <b>{escape_html(human)}</b>\n"
        f"{payout_line}"
        f"TX: {(tx_line if not photo_id else 'скриншот во вложении')}\n"
        f"Статус: <b>pending</b>"
    )

    if photo_id:
        safe_send_photo(OPERATOR_ID, photo_id, caption=text, reply_markup=operator_kb(order_id, m.from_user.id))
    else:
        safe_send_message(OPERATOR_ID, text, reply_markup=operator_kb(order_id, m.from_user.id))

    bot.send_message(m.chat.id, f"✅ Заявка отправлена! Номер: <b>#{order_id}</b>\nОжидайте подтверждения.", reply_markup=main_menu(m.from_user.id == OPERATOR_ID))
    bot.delete_state(m.from_user.id, m.chat.id)

# --- Решение оператора по заявке ---
@bot.callback_query_handler(func=lambda c: c.data.startswith(("approve:", "reject:")))
def operator_decision(c: CallbackQuery):
    if c.from_user.id != OPERATOR_ID:
        bot.answer_callback_query(c.id, "Недостаточно прав")
        return

    action, id_str = c.data.split(":", 1)
    try:
        order_id = int(id_str)
    except ValueError:
        bot.answer_callback_query(c.id, "Некорректный ID")
        return

    row = db_get_order(order_id)
    if not row:
        bot.answer_callback_query(c.id, "Заявка не найдена")
        return

    user_id = row[1]
    status = "approved" if action == "approve" else "rejected"
    db_update_status(order_id, status)

    if status == "approved":
        safe_send_message(user_id, f"✅ Ваша заявка <b>#{order_id}</b> — <b>Одобрено</b>.")
        bot.answer_callback_query(c.id, "Одобрено")
    else:
        safe_send_message(user_id, f"❌ Ваша заявка <b>#{order_id}</b> — <b>Отклонено</b>.\nЕсли нужно — нажмите «Поддержка».")
        bot.answer_callback_query(c.id, "Отклонено")

    try:
        bot.edit_message_reply_markup(chat_id=c.message.chat.id, message_id=c.message.message_id, reply_markup=None)
    except Exception:
        pass

# --- Бан / разбан пользователя ---
@bot.callback_query_handler(func=lambda c: c.data.startswith("ban:"))
def operator_ban(c: CallbackQuery):
    if c.from_user.id != OPERATOR_ID:
        bot.answer_callback_query(c.id, "Недостаточно прав")
        return
    try:
        user_id = int(c.data.split(":", 1)[1])
    except ValueError:
        bot.answer_callback_query(c.id, "Ошибка")
        return

    db_set_user_blocked(user_id, True)
    safe_send_message(user_id, "🚫 Доступ запрещён.")
    bot.answer_callback_query(c.id, "Пользователь заблокирован")

@bot.callback_query_handler(func=lambda c: c.data.startswith("unban:"))
def operator_unban(c: CallbackQuery):
    if c.from_user.id != OPERATOR_ID:
        bot.answer_callback_query(c.id, "Недостаточно прав")
        return
    try:
        user_id = int(c.data.split(":", 1)[1])
    except ValueError:
        bot.answer_callback_query(c.id, "Ошибка")
        return

    db_set_user_blocked(user_id, False)
    safe_send_message(user_id, "✅ Вы разблокированы. Можете пользоваться ботом.")
    bot.answer_callback_query(c.id, "Пользователь разблокирован")

# ---------------- Админка ----------------
@bot.message_handler(func=lambda m: getattr(m, "text", "") == "⚙️ Админка")
def admin_panel(m: Message):
    if m.from_user.id != OPERATOR_ID:
        bot.send_message(m.chat.id, "Недостаточно прав.", reply_markup=main_menu(False))
        return

    bot.set_state(m.from_user.id, AdminStates.choose, m.chat.id)

    text = (
        "⚙️ <b>Админка</b>\n\n"
        f"Курс покупки: <b>{get_buy_rate()} ₽</b> за 1$\n"
        f"Курс продажи: <b>{get_sell_rate()} ₽</b> за 1$\n"
        f"Мин сумма: <b>{get_min_usd()}$</b>\n"
        f"Крипты: <b>{', '.join(get_enabled_cryptos())}</b>\n"
        f"USDT кошелёк: <code>{escape_html(get_wallet('USDT_TRON'))}</code>\n"
        f"LTC кошелёк: <code>{escape_html(get_wallet('LTC'))}</code>\n"
        f"Поддержка: <b>{escape_html(get_support_username())}</b>\n"
    )
    bot.send_message(m.chat.id, text, reply_markup=admin_menu_kb())

@bot.message_handler(func=lambda m: getattr(m, "text", "") == "⬅ Назад", state=AdminStates.choose)
def admin_back(m: Message):
    bot.delete_state(m.from_user.id, m.chat.id)
    bot.send_message(m.chat.id, "Ок.", reply_markup=main_menu(True))

@bot.message_handler(state=AdminStates.choose, content_types=["text"])
def admin_choose(m: Message):
    if m.from_user.id != OPERATOR_ID:
        bot.delete_state(m.from_user.id, m.chat.id)
        return

    t = (m.text or "").strip()

    if t == "Курс покупки":
        with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
            data["edit"] = "buy_rate"
        bot.set_state(m.from_user.id, AdminStates.wait_value, m.chat.id)
        bot.send_message(m.chat.id, "Введи новый курс покупки (пример: 18.6):")

    elif t == "Курс продажи":
        with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
            data["edit"] = "sell_rate"
        bot.set_state(m.from_user.id, AdminStates.wait_value, m.chat.id)
        bot.send_message(m.chat.id, "Введи новый курс продажи (пример: 16.5):")

    elif t == "Мин сумма":
        with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
            data["edit"] = "min_usd"
        bot.set_state(m.from_user.id, AdminStates.wait_value, m.chat.id)
        bot.send_message(m.chat.id, "Введи минимальную сумму в $ (пример: 10):")

    elif t == "Добавление криптовалюты":
        with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
            data["edit"] = "cryptos"
        bot.set_state(m.from_user.id, AdminStates.wait_value, m.chat.id)
        bot.send_message(m.chat.id, "Введи список через запятую (USDT_TRON,LTC):")

    elif t == "Кошельки":
        bot.set_state(m.from_user.id, AdminStates.wait_wallet_crypto, m.chat.id)
        kb = InlineKeyboardMarkup()
        kb.add(
            InlineKeyboardButton("USDT_TRON", callback_data="admin_wallet:USDT_TRON"),
            InlineKeyboardButton("LTC", callback_data="admin_wallet:LTC"),
        )
        bot.send_message(m.chat.id, "Выбери крипту для смены кошелька:", reply_markup=kb)

    elif t == "Юзер оператора":
        with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
            data["edit"] = "support_username"
        bot.set_state(m.from_user.id, AdminStates.wait_value, m.chat.id)
        bot.send_message(m.chat.id, "Введи @username оператора (пример: @TOM_EXCH_PMR):")

    else:
        bot.send_message(m.chat.id, "Выбери пункт из меню.", reply_markup=admin_menu_kb())

@bot.callback_query_handler(func=lambda c: c.data.startswith("admin_wallet:"), state=AdminStates.wait_wallet_crypto)
def admin_wallet_pick(c: CallbackQuery):
    if c.from_user.id != OPERATOR_ID:
        bot.answer_callback_query(c.id, "Недостаточно прав")
        return

    code = c.data.split(":", 1)[1]
    if code not in ALLOWED_CRYPTOS:
        bot.answer_callback_query(c.id, "Неверно")
        return

    bot.answer_callback_query(c.id)
    with bot.retrieve_data(c.from_user.id, c.message.chat.id) as data:
        data["wallet_code"] = code
        data["edit"] = "wallet"

    bot.set_state(c.from_user.id, AdminStates.wait_value, c.message.chat.id)
    bot.send_message(c.message.chat.id, f"Введи новый кошелёк для {code}:")

@bot.message_handler(state=AdminStates.wait_value, content_types=["text"])
def admin_set_value(m: Message):
    if m.from_user.id != OPERATOR_ID:
        bot.delete_state(m.from_user.id, m.chat.id)
        return

    value = (m.text or "").strip()

    with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
        edit = data.get("edit")
        wallet_code = data.get("wallet_code")

    # кошельки
    if wallet_code:
        if len(value) < 10:
            bot.send_message(m.chat.id, "Слишком короткий адрес. Введи кошелёк ещё раз.")
            return
        db_set_setting(f"wallet_{wallet_code}", value)
        with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
            data.pop("wallet_code", None)
            data.pop("edit", None)
        bot.set_state(m.from_user.id, AdminStates.choose, m.chat.id)
        bot.send_message(m.chat.id, "✅ Кошелёк сохранён.", reply_markup=admin_menu_kb())
        return

    # числа
    if edit in ["buy_rate", "sell_rate", "min_usd"]:
        try:
            d = Decimal(value.replace(",", "."))
            if d <= 0:
                raise InvalidOperation()
        except Exception:
            bot.send_message(m.chat.id, "Некорректное число. Пример: 18.6")
            return
        db_set_setting(edit, str(d))

    # список крипт
    elif edit == "cryptos":
        items = [x.strip() for x in value.split(",") if x.strip()]
        items = [x for x in items if x in ALLOWED_CRYPTOS]
        if not items:
            bot.send_message(m.chat.id, "Список пуст или неверный. Пример: USDT_TRON,LTC")
            return
        db_set_setting("cryptos", ",".join(items))

    # юзер поддержки
    elif edit == "support_username":
        v = value.strip()
        if not v:
            bot.send_message(m.chat.id, "Пусто. Пример: @TOM_EXCH_PMR")
            return
        if not v.startswith("@"):
            v = "@" + v
        db_set_setting("support_username", v)

    else:
        bot.send_message(m.chat.id, "Не понял что менять. Вернись в админку.")
        bot.set_state(m.from_user.id, AdminStates.choose, m.chat.id)
        bot.send_message(m.chat.id, "⚙️ Админка:", reply_markup=admin_menu_kb())
        return

    bot.set_state(m.from_user.id, AdminStates.choose, m.chat.id)
    bot.send_message(m.chat.id, "✅ Сохранено.", reply_markup=admin_menu_kb())

# ---------------- Рассылка (только оператор) ----------------
@bot.message_handler(func=lambda m: getattr(m, "text", "") == "📢 Рассылка")
def start_broadcast(m: Message):
    if m.from_user.id != OPERATOR_ID:
        bot.send_message(m.chat.id, "Недостаточно прав.", reply_markup=main_menu(False))
        return
    bot.set_state(m.from_user.id, BroadcastStates.wait_content, m.chat.id)
    kb = ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("Отмена"))
    bot.send_message(m.chat.id, "Отправьте текст/медиа для рассылки (любое сообщение).", reply_markup=kb)

@bot.message_handler(state=BroadcastStates.wait_content, content_types=[
    "text","photo","video","document","audio","voice","video_note","animation","sticker"
])
def broadcast_got_content(m: Message):
    if m.from_user.id != OPERATOR_ID:
        bot.delete_state(m.from_user.id, m.chat.id)
        return
    with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
        data["src_chat_id"] = m.chat.id
        data["src_message_id"] = m.message_id
    total = len(db_all_user_ids(only_active=False))
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton(f"▶ Отправить ({total})", callback_data="broadcast:send"),
        InlineKeyboardButton("Отмена", callback_data="broadcast:cancel")
    )
    bot.set_state(m.from_user.id, BroadcastStates.confirm, m.chat.id)
    bot.send_message(m.chat.id, f"Готовы отправить рассылку {total} пользователям?", reply_markup=kb)

@bot.callback_query_handler(func=lambda c: c.data in ["broadcast:send","broadcast:cancel"], state=BroadcastStates.confirm)
def broadcast_confirm(c: CallbackQuery):
    if c.from_user.id != OPERATOR_ID:
        bot.answer_callback_query(c.id, "Недостаточно прав")
        return

    if c.data == "broadcast:cancel":
        bot.delete_state(c.from_user.id, c.message.chat.id)
        try:
            bot.edit_message_reply_markup(c.message.chat.id, c.message.message_id, reply_markup=None)
        except Exception:
            pass
        bot.answer_callback_query(c.id, "Рассылка отменена")
        bot.send_message(c.message.chat.id, "Отменено.", reply_markup=main_menu(True))
        return

    with bot.retrieve_data(c.from_user.id, c.message.chat.id) as data:
        src_chat_id = data.get("src_chat_id")
        src_message_id = data.get("src_message_id")

    try:
        bot.edit_message_reply_markup(c.message.chat.id, c.message.message_id, reply_markup=None)
    except Exception:
        pass
    bot.answer_callback_query(c.id, "Рассылка запущена")

    def run_broadcast():
        users = db_all_user_ids(only_active=False)
        sent = 0
        failed = 0
        for uid in users:
            if uid == OPERATOR_ID:
                continue
            res = safe_copy_message(uid, src_chat_id, src_message_id)
            if res is not None:
                sent += 1
            else:
                failed += 1
            time.sleep(0.06)
        safe_send_message(OPERATOR_ID, f"Рассылка завершена.\nУспешно: {sent}\nОшибок: {failed}", reply_markup=main_menu(True))

    threading.Thread(target=run_broadcast, daemon=True).start()
    bot.delete_state(c.from_user.id, c.message.chat.id)

# --- Фолбэк ---
@bot.message_handler(func=lambda m: True)
def fallback(m: Message):
    if deny_if_blocked(m.from_user.id, m.chat.id):
        return
    bot.send_message(m.chat.id, "Выберите действие из меню или нажмите /start", reply_markup=main_menu(m.from_user.id == OPERATOR_ID))

# ---------------- Запуск ----------------
if __name__ == "__main__":
    db_init()

    # один раз проставим дефолты в settings (если ещё пусто)
    if db_get_setting("buy_rate") is None:
        db_set_setting("buy_rate", DEFAULT_BUY_RATE)
    if db_get_setting("sell_rate") is None:
        db_set_setting("sell_rate", DEFAULT_SELL_RATE)
    if db_get_setting("min_usd") is None:
        db_set_setting("min_usd", DEFAULT_MIN_USD)
    if db_get_setting("cryptos") is None:
        db_set_setting("cryptos", DEFAULT_CRYPTOS)
    if db_get_setting("support_username") is None:
        db_set_setting("support_username", DEFAULT_SUPPORT_USERNAME)
    if db_get_setting("wallet_USDT_TRON") is None:
        db_set_setting("wallet_USDT_TRON", USDT_WALLET_DEFAULT)
    if db_get_setting("wallet_LTC") is None:
        db_set_setting("wallet_LTC", LTC_WALLET_DEFAULT)

    bot.add_custom_filter(custom_filters.StateFilter(bot))
    bot.add_custom_filter(custom_filters.TextMatchFilter())

    bot.remove_webhook()  # важно: выключаем webhook, иначе polling не получит сообщения
    logger.info("Bot started")
    bot.infinity_polling(timeout=30, long_polling_timeout=30, skip_pending=True)
