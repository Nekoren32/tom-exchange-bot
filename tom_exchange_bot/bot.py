
import os
import threading
import time
import sqlite3
import logging
from decimal import Decimal, InvalidOperation
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

# ---------------- Конфигурация (из окружения) ----------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN не задан в окружении")

try:
    OPERATOR_ID = int(os.environ.get("OPERATOR_ID", "0"))
except ValueError:
    raise RuntimeError("OPERATOR_ID должен быть целым числом в окружении")

DB_PATH = os.environ.get("DB_PATH", "orders.db")
LTC_WALLET = os.environ.get("LTC_WALLET", "LWzfxJHnRswAhu5uYP1trdzVh68HrxYrDT")
USDT_WALLET = os.environ.get("USDT_WALLET", "TBVKYMdP63hGm4wszvpRmsbUazCyriyYUT")

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
    wait_tx = State()      # ожидание хеша/скриншота (для продажи)

class BroadcastStates(StatesGroup):
    wait_content = State()
    confirm = State()

# ---------------- SQLite helper ----------------
def get_conn():
    # Применяем timeout, включаем WAL для повышения конкурентной записи
    conn = sqlite3.connect(DB_PATH, timeout=30, detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES)
    try:
        # Повысим производительность и конкурентность
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
    except Exception:
        # В старых версиях SQLite pragma может быть не поддержан — игнорируем
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
        conn.commit()

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
        cur.execute("SELECT id, user_id, username, full_name, action, amount, crypto, tx_info, status, created_at FROM orders WHERE id = ?", (order_id,))
        return cur.fetchone()

def db_upsert_user(user):
    full_name = f"{user.first_name or ''} {user.last_name or ''}".strip()
    now = datetime.utcnow().isoformat()
    with get_conn() as conn:
        cur = conn.cursor()
        # Используем UPSERT; если старый sqlite не поддерживает — на уровне app это крайний случай
        cur.execute("""
            INSERT INTO users (user_id, username, full_name, first_seen, last_seen, blocked)
            VALUES (?, ?, ?, ?, ?, 0)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                full_name = excluded.full_name,
                last_seen = excluded.last_seen
        """, (user.id, user.username, full_name, now, now))
        conn.commit()

def db_all_user_ids(only_active=True):
    with get_conn() as conn:
        cur = conn.cursor()
        if only_active:
            cur.execute("SELECT user_id FROM users WHERE blocked = 0")
        else:
            cur.execute("SELECT user_id FROM users")
        return [r[0] for r in cur.fetchall()]

def db_set_user_blocked(user_id: int, blocked: bool):
    with get_conn() as conn:
        cur = conn.cursor()
        cur.execute("UPDATE users SET blocked = ?, last_seen = ? WHERE user_id = ?",
                    (1 if blocked else 0, datetime.utcnow().isoformat(), user_id))
        conn.commit()

# ---------------- Утилиты ----------------
def escape_html(s: str) -> str:
    return (s or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")

def user_link(u) -> str:
    name = (f"{u.first_name or ''} {u.last_name or ''}").strip() or f"id:{u.id}"
    return f'<a href="tg://user?id={u.id}">{escape_html(name)}</a>'

def main_menu(is_operator: bool = False) -> ReplyKeyboardMarkup:
    kb = ReplyKeyboardMarkup(resize_keyboard=True)
    kb.add(KeyboardButton("💰 Купить крипту"), KeyboardButton("💸 Продать крипту"))
    kb.add(KeyboardButton("📞 Контакты"), KeyboardButton("❓ Помощь"))
    if is_operator:
        kb.add(KeyboardButton("📢 Рассылка"))
    return kb

def crypto_kb() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("USDT (TRC20)", callback_data="crypto:USDT_TRON"),
        InlineKeyboardButton("LTC", callback_data="crypto:LTC")
    )
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

def operator_kb(order_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    kb.add(
        InlineKeyboardButton("✔ Подтвердить", callback_data=f"approve:{order_id}"),
        InlineKeyboardButton("✖ Отклонить", callback_data=f"reject:{order_id}")
    )
    return kb

def parse_amount(text: str):
    try:
        t = text.replace(",", ".").strip()
        amt = Decimal(t)
        if amt <= 0:
            return None
        return amt.quantize(Decimal("0.01"))
    except (InvalidOperation, AttributeError):
        return None

def wallet_by_crypto(code: str) -> str:
    return USDT_WALLET if code == "USDT_TRON" else LTC_WALLET

def crypto_human(code: str) -> str:
    return "USDT (TRC20)" if code == "USDT_TRON" else "LTC"

# ---------------- Safe send helpers ----------------
def _extract_retry_after(exc):
    # Пытаемся прочитать retry_after из тела ошибки
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
        logger.warning("ApiTelegramException при отправке сообщения %s -> %s: %s", chat_id, text[:80], e)
        if e.error_code == 403:
            # пользователь заблокировал бота
            try:
                db_set_user_blocked(chat_id, True)
            except Exception:
                logger.exception("Не удалось пометить пользователя как заблокированного")
            return None
        if e.error_code == 429:
            retry = _extract_retry_after(e) or 5
            logger.info("Flood: ждем %s сек", retry)
            time.sleep(retry + 1)
            try:
                return bot.send_message(chat_id, text, **kwargs)
            except Exception as e2:
                logger.exception("Ошибка после retry при отправке сообщения")
                return None
        logger.exception("Необработанная ошибка при отправке сообщения")
        return None
    except Exception:
        logger.exception("Ошибка при отправке сообщения")
        return None

def safe_copy_message(chat_id, from_chat_id, message_id):
    try:
        return bot.copy_message(chat_id, from_chat_id, message_id)
    except ApiTelegramException as e:
        logger.warning("ApiTelegramException при copy_message to %s: %s", chat_id, e)
        if e.error_code == 403:
            try:
                db_set_user_blocked(chat_id, True)
            except Exception:
                logger.exception("Не удалось пометить пользователя как заблокированного")
            return None
        if e.error_code == 429:
            retry = _extract_retry_after(e) or 5
            logger.info("Flood during copy: ждем %s сек", retry)
            time.sleep(retry + 1)
            try:
                return bot.copy_message(chat_id, from_chat_id, message_id)
            except Exception:
                logger.exception("Ошибка после retry при copy_message")
                return None
        return None
    except Exception:
        logger.exception("Ошибка при копировании сообщения")
        return None

def safe_send_photo(chat_id, photo, caption=None, **kwargs):
    try:
        return bot.send_photo(chat_id, photo, caption=caption, **kwargs)
    except ApiTelegramException as e:
        logger.warning("ApiTelegramException при send_photo to %s: %s", chat_id, e)
        if e.error_code == 403:
            try:
                db_set_user_blocked(chat_id, True)
            except Exception:
                logger.exception("Не удалось пометить пользователя как заблокированного")
            return None
        if e.error_code == 429:
            retry = _extract_retry_after(e) or 5
            logger.info("Flood during photo send: ждем %s сек", retry)
            time.sleep(retry + 1)
            try:
                return bot.send_photo(chat_id, photo, caption=caption, **kwargs)
            except Exception:
                logger.exception("Ошибка после retry при send_photo")
                return None
        return None
    except Exception:
        logger.exception("Ошибка при отправке фото")
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

# ---------------- Хэндлеры ----------------
@bot.message_handler(commands=["start"])
def cmd_start(m: Message):
    is_op = (m.from_user.id == OPERATOR_ID)
    try:
        bot.delete_state(m.from_user.id, m.chat.id)
    except Exception:
        pass
    bot.send_message(
        m.chat.id,
        "Вас приветствует <b>TOM EXCHANGE</b> 👋\n"
        "У нас вы можете безопасно купить или продать криптовалюту.\n\n"
        "Выберите действие ниже.",
        reply_markup=main_menu(is_op)
    )

@bot.message_handler(commands=["cancel"])
def cmd_cancel(m: Message):
    is_op = (m.from_user.id == OPERATOR_ID)
    try:
        bot.delete_state(m.from_user.id, m.chat.id)
    except Exception:
        pass
    bot.send_message(m.chat.id, "Действие отменено.", reply_markup=main_menu(is_op))

@bot.message_handler(func=lambda msg: getattr(msg, "text", "") == "Отмена")
def cancel_btn(m: Message):
    is_op = (m.from_user.id == OPERATOR_ID)
    try:
        bot.delete_state(m.from_user.id, m.chat.id)
    except Exception:
        pass
    bot.send_message(m.chat.id, "Отменено.", reply_markup=main_menu(is_op))

@bot.message_handler(func=lambda msg: getattr(msg, "text", "") == "📞 Контакты")
def contacts(m: Message):
    is_op = (m.from_user.id == OPERATOR_ID)
    bot.send_message(
        m.chat.id,
        "🏠 Группа: https://t.me/+xHNTmcHniZQ1YzM0\n"
        "👥 Отзывы: https://t.me/+2rUIkxQxaN81MzJk\n"
        "📢 Канал: https://t.me/tom_exch\n\n"
        "📞 Оператор: @TOM_EXCH_PMR\n"
        "⏰ Мы на связи с 04:00 до 23:00",
        reply_markup=main_menu(is_op)
    )

@bot.message_handler(func=lambda msg: getattr(msg, "text", "") == "❓ Помощь")
def help_(m: Message):
    is_op = (m.from_user.id == OPERATOR_ID)
    bot.send_message(
        m.chat.id,
        "Как это работает:\n"
        "— Покупка: сумма → выбор крипты → выбор способа (Переводилка/Реквизиты) → заявка уходит оператору.\n"
        "— Продажа: сумма → выбор крипты → адрес кошелька → «Я отправил» → хеш/скрин → заявка оператору.",
        reply_markup=main_menu(is_op)
    )

# --- Старт покупки/продажи ---
@bot.message_handler(func=lambda msg: getattr(msg, "text", "") == "💰 Купить крипту")
def buy_crypto(m: Message):
    bot.set_state(m.from_user.id, OrderStates.action, m.chat.id)
    with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
        data["action"] = "buy"
    bot.set_state(m.from_user.id, OrderStates.amount, m.chat.id)
    kb = ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("Отмена"))
    bot.send_message(m.chat.id, "Введите сумму в $ (например, 150 или 150.50)", reply_markup=kb)

@bot.message_handler(func=lambda msg: getattr(msg, "text", "") == "💸 Продать крипту")
def sell_crypto(m: Message):
    bot.set_state(m.from_user.id, OrderStates.action, m.chat.id)
    with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
        data["action"] = "sell"
    bot.set_state(m.from_user.id, OrderStates.amount, m.chat.id)
    kb = ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("Отмена"))
    bot.send_message(m.chat.id, "Введите сумму в $ (например, 200 или 200.00)", reply_markup=kb)

# --- Ввод суммы ---
@bot.message_handler(state=OrderStates.amount, content_types=["text"])
def handle_amount(m: Message):
    amt = parse_amount(m.text)
    if not amt:
        bot.send_message(m.chat.id, "Введите корректную сумму > 0. Пример: 100 или 100.50")
        return
    with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
        data["amount"] = str(amt)
    bot.set_state(m.from_user.id, OrderStates.crypto, m.chat.id)
    bot.send_message(m.chat.id, "Выберите криптовалюту:", reply_markup=ReplyKeyboardMarkup(resize_keyboard=True).add(KeyboardButton("Отмена")))
    bot.send_message(m.chat.id, "Доступные варианты:", reply_markup=crypto_kb())

# --- Выбор криптовалюты (inline) ---
@bot.callback_query_handler(func=lambda c: c.data.startswith("crypto:"), state=OrderStates.crypto)
def select_crypto(c: CallbackQuery):
    code = c.data.split(":", 1)[1]
    if code not in ["USDT_TRON", "LTC"]:
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
            f"Заявка: Покупка\nСумма: <b>{amt}$</b>\nКриптовалюта: <b>{human}</b>\n\n"
            "Выберите способ оплаты:",
            reply_markup=buymethod_kb()
        )
    else:
        wallet = wallet_by_crypto(code)
        bot.send_message(
            c.message.chat.id,
            f"Заявка: Продажа\nСумма: <b>{amt}$</b>\nКриптовалюта: <b>{human}</b>"
        )
        bot.send_message(
            c.message.chat.id,
            f"Для продажи отправьте <b>{human}</b> на адрес:\n<code>{wallet}</code>\n\n"
            "После отправки нажмите «Я отправил», затем пришлите хеш транзакции или скриншот.",
            reply_markup=confirm_kb_for_sell()
        )

# --- Покупка: выбор способа оплаты (inline) ---
@bot.callback_query_handler(func=lambda c: c.data.startswith("buymethod:"), state=OrderStates.buy_method)
def select_buy_method(c: CallbackQuery):
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

    # Создаём заявку (tx_info — выбранный способ оплаты)
    order_id = db_create_order(c.from_user, "buy", amount, crypto, f"buy_method:{method_code}")

    # Отправляем оператору через safe_send
    text = (
        f"📩 <b>Новая заявка — ПОКУПКА</b>\n\n"
        f"ID заявки: <b>#{order_id}</b>\n"
        f"Пользователь: {user_link(c.from_user)} @{escape_html(c.from_user.username or '—')}\n"
        f"Сумма: <b>{escape_html(str(amount))}$</b>\n"
        f"Криптовалюта: <b>{escape_html(human)}</b>\n"
        f"Способ оплаты: <b>{method_human}</b>\n"
        f"Статус: <b>pending</b>"
    )
    try:
        safe_send_message(OPERATOR_ID, text, reply_markup=operator_kb(order_id))
    except Exception:
        logger.exception("Не удалось уведомить оператора при создании заявки на покупку")

    bot.send_message(
        c.message.chat.id,
        f"Заявка отправлена оператору! Номер: #{order_id}\nОжидайте ответа. Оператор свяжется с вами и предоставит {'реквизиты' if method_code=='requisites' else 'детали перевода'}.",
        reply_markup=main_menu(False)
    )
    bot.delete_state(c.from_user.id, c.message.chat.id)

# --- Продажа: подтверждение отправки и приём хеша/скриншота ---
@bot.message_handler(func=lambda m: getattr(m, "text", "") in ["📤 Я отправил"])
def confirm_sent(m: Message):
    state = bot.get_state(m.from_user.id, m.chat.id)
    if state is None:
        is_op = (m.from_user.id == OPERATOR_ID)
        bot.send_message(m.chat.id, "Нет активной заявки. Нажмите /start", reply_markup=main_menu(is_op))
        return

    with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
        action = data.get("action")
        if action != "sell":
            is_op = (m.from_user.id == OPERATOR_ID)
            bot.send_message(m.chat.id, "Эта кнопка доступна только в процессе продажи. Нажмите /start", reply_markup=main_menu(is_op))
            return

    bot.set_state(m.from_user.id, OrderStates.wait_tx, m.chat.id)
    bot.send_message(m.chat.id, "Отправьте хеш транзакции текстом или приложите скриншот (фото).")

@bot.message_handler(state=OrderStates.wait_tx, content_types=["text", "photo"])
def receive_tx(m: Message):
    with bot.retrieve_data(m.from_user.id, m.chat.id) as data:
        action = data.get("action")
        amount = data.get("amount")
        crypto = data.get("crypto")

    if action != "sell" or not all([amount, crypto]):
        is_op = (m.from_user.id == OPERATOR_ID)
        bot.send_message(m.chat.id, "Данные заявки потеряны. Начните заново: /start", reply_markup=main_menu(is_op))
        bot.delete_state(m.from_user.id, m.chat.id)
        return

    tx_info = ""
    photo_id = None
    if m.content_type == "text":
        tx_info = m.text.strip()
    elif m.content_type == "photo":
        photo_id = m.photo[-1].file_id
        tx_info = f"photo:{photo_id}"

    order_id = db_create_order(m.from_user, "sell", amount, crypto, tx_info)
    human = crypto_human(crypto)

    text = (
        f"📩 <b>Новая заявка — ПРОДАЖА</b>\n\n"
        f"ID заявки: <b>#{order_id}</b>\n"
        f"Пользователь: {user_link(m.from_user)} @{escape_html(m.from_user.username or '—')}\n"
        f"Сумма: <b>{escape_html(amount)}$</b>\n"
        f"Криптовалюта: <b>{escape_html(human)}</b>\n"
        f"TX: {(escape_html(tx_info) if not tx_info.startswith('photo:') else 'скриншот во вложении')}\n"
        f"Статус: <b>pending</b>"
    )

    try:
        if photo_id:
            safe_send_photo(OPERATOR_ID, photo_id, caption=text, reply_markup=operator_kb(order_id))
        else:
            safe_send_message(OPERATOR_ID, text, reply_markup=operator_kb(order_id))
    except Exception:
        logger.exception("Не удалось уведомить оператора при продаже")
        is_op = (m.from_user.id == OPERATOR_ID)
        bot.send_message(m.chat.id, "Не удалось уведомить оператора. Попробуйте позже или свяжитесь вручную: @TOM_EXCH_PMR", reply_markup=main_menu(is_op))
        bot.delete_state(m.from_user.id, m.chat.id)
        return

    is_op = (m.from_user.id == OPERATOR_ID)
    bot.send_message(m.chat.id, f"Заявка отправлена оператору! Номер: #{order_id}\nОжидайте подтверждения.", reply_markup=main_menu(is_op))
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

    try:
        if status == "approved":
            safe_send_message(user_id, f"✅ Ваша заявка #{order_id} подтверждена! Спасибо.")
            bot.answer_callback_query(c.id, f"Заявка #{order_id} подтверждена")
        else:
            safe_send_message(user_id, f"❌ Ваша заявка #{order_id} отклонена. Свяжитесь с оператором для уточнения.")
            bot.answer_callback_query(c.id, f"Заявка #{order_id} отклонена")
    except Exception:
        logger.exception("Ошибка при уведомлении пользователя после решения оператора")
        bot.answer_callback_query(c.id, "Статус обновлен, но отправка пользователю не удалась")

    try:
        bot.edit_message_reply_markup(chat_id=c.message.chat.id, message_id=c.message.message_id, reply_markup=None)
        bot.reply_to(c.message, f"Статус заявки #{order_id}: {status}")
    except Exception:
        logger.exception("Не удалось отредактировать сообщение оператора")

# --- Рассылка (только оператор) ---
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
    total = len(db_all_user_ids())
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
            try:
                # Копируем исходное сообщение (сохраняет тип: фото/текст/документ)
                res = safe_copy_message(uid, src_chat_id, src_message_id)
                if res is not None:
                    sent += 1
                else:
                    failed += 1
                # Небольшая пауза — регулировать по потребности
                time.sleep(0.06)
            except Exception:
                failed += 1
                logger.exception("Неожиданная ошибка при рассылке пользователю %s", uid)
        safe_send_message(OPERATOR_ID, f"Рассылка завершена.\nУспешно: {sent}\nОшибок: {failed}", reply_markup=main_menu(True))

    threading.Thread(target=run_broadcast, daemon=True).start()
    bot.delete_state(c.from_user.id, c.message.chat.id)

# --- Фолбэк ---
@bot.message_handler(func=lambda m: True)
def fallback(m: Message):
    is_op = (m.from_user.id == OPERATOR_ID)
    bot.send_message(m.chat.id, "Выберите действие из меню или нажмите /start", reply_markup=main_menu(is_op))

# ---------------- Запуск ----------------
if __name__ == "__main__":
    db_init()
    bot.add_custom_filter(custom_filters.StateFilter(bot))
    bot.add_custom_filter(custom_filters.TextMatchFilter())
    logger.info("Bot started")
    # infinity_polling запускает цикл. Можно дополнительно обернуть в retry
    bot.infinity_polling(timeout=30, long_polling_timeout=30)

