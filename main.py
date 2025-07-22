import logging
import aiosqlite
import asyncio
import os
from datetime import datetime, timedelta
from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.filters import CommandStart, Command
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardRemove
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.context import FSMContext
from aiohttp import web

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('bot.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Переменные окружения
BOT_TOKEN = os.getenv("BOT_TOKEN", "YOUR_BOT_TOKEN_HERE")
ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_IDS", "123456789,987654321").split(",") if id]
PRIVACY_URL = "https://telegra.ph/Politika-konfidencialnosti-07-19-25"
RULES_URL = "https://telegra.ph/Pravila-07-19-160"
DB_PATH = os.getenv("DB_PATH", "fanpay_bot.db")

# Проверка переменных
if not BOT_TOKEN:
    logger.error("Отсутствует BOT_TOKEN")
    raise ValueError("Не указан BOT_TOKEN")

# Константы сообщений
MESSAGES = {
    "welcome": "Добро пожаловать в бота сопровождения PUBG Mobile - Metro Royale! 🎮",
    "no_access": "❌ У вас нет доступа к этой команде.",
    "no_squads": "🏠 Нет доступных сквадов.",
    "no_escorts": "👤 Нет зарегистрированных сопровождающих.",
    "no_orders": "📋 Сейчас нет доступных заказов.",
    "no_active_orders": "📋 У вас нет активных заказов.",
    "error": "⚠️ Произошла ошибка. Попробуйте снова позже.",
    "invalid_format": "❌ Неверный формат ввода. Попробуйте снова.",
    "order_completed": "✅ Заказ #{order_id} завершен пользователем @{username} (Telegram ID: {telegram_id}, PUBG ID: {pubg_id})!",
    "order_already_completed": "⚠️ Заказ #{order_id} уже завершен.",
    "balance_added": "💸 Баланс {amount} руб. начислен пользователю {user_id}",
    "squad_full": "⚠️ Сквад '{squad_name}' уже имеет максимум 6 участников!",
    "squad_too_small": "⚠️ В скваде '{squad_name}' должно быть минимум 3 участника для принятия заказа!",
    "order_added": "📝 Заказ #{order_id} добавлен! Сумма: {amount} руб., Описание: {description}, Клиент: {customer}",
    "rules_not_accepted": "📜 Пожалуйста, примите правила и политику конфиденциальности.",
    "user_banned": "🚫 Вы заблокированы.",
    "user_restricted": "⛔ Ваш доступ к сопровождениям ограничен до {date}.",
    "balance_zeroed": "💰 Баланс пользователя {user_id} обнулен.",
    "pubg_id_updated": "🔢 PUBG ID успешно обновлен!",
    "ping": "🏓 Бот активен!",
    "order_taken": "📝 Заказ #{order_id} принят сквадом {squad_name}!\nУчастники:\n{participants}",
    "order_not_enough_members": "⚠️ В скваде '{squad_name}' недостаточно участников (минимум 3)!",
    "order_already_in_progress": "⚠️ Заказ #{order_id} уже в наборе или принят!",
    "order_joined": "✅ Вы присоединились к набору для заказа #{order_id}!\nТекущий состав:\n{participants}",
    "order_confirmed": "✅ Заказ #{order_id} подтвержден и принят!\nУчастники:\n{participants}",
    "not_in_squad": "⚠️ Вы не состоите в скваде!",
    "max_participants": "⚠️ Максимум 4 участника для заказа!",
    "rating_submitted": "🌟 Оценка {rating} для заказа #{order_id} сохранена! Репутация обновлена.",
    "rate_order": "🌟 Поставьте оценку за заказ #{order_id} (1-5):"
}

# Инициализация бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(storage=MemoryStorage())

# Состояния FSM
class Form(StatesGroup):
    squad_name = State()
    escort_info = State()
    pubg_id = State()
    balance_amount = State()
    complete_order = State()
    add_order = State()
    ban_duration = State()
    rate_order = State()

# --- Веб-обработчик для пинга ---
async def ping(request):
    return web.Response(text="OK")

# --- Функции базы данных ---
async def init_db():
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.executescript('''
            CREATE TABLE IF NOT EXISTS squads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                rating REAL DEFAULT 0,
                rating_count INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS escorts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                telegram_id INTEGER UNIQUE NOT NULL,
                username TEXT,
                pubg_id TEXT,
                squad_id INTEGER,
                balance REAL DEFAULT 0,
                reputation INTEGER DEFAULT 0,
                completed_orders INTEGER DEFAULT 0,
                rating REAL DEFAULT 0,
                rating_count INTEGER DEFAULT 0,
                is_banned INTEGER DEFAULT 0,
                ban_until TIMESTAMP,
                restrict_until TIMESTAMP,
                rules_accepted INTEGER DEFAULT 0,
                FOREIGN KEY (squad_id) REFERENCES squads (id)
            );
            CREATE TABLE IF NOT EXISTS orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                fanpay_order_id TEXT UNIQUE NOT NULL,
                customer_info TEXT,
                amount REAL,
                status TEXT DEFAULT 'pending',
                squad_id INTEGER,
                completed_at TIMESTAMP,
                rating INTEGER DEFAULT 0,
                FOREIGN KEY (squad_id) REFERENCES squads (id)
            );
            CREATE TABLE IF NOT EXISTS order_escorts (
                order_id INTEGER,
                escort_id INTEGER,
                pubg_id TEXT,
                PRIMARY KEY (order_id, escort_id),
                FOREIGN KEY (order_id) REFERENCES orders (id),
                FOREIGN KEY (escort_id) REFERENCES escorts (id)
            );
            CREATE TABLE IF NOT EXISTS order_applications (
                order_id INTEGER,
                escort_id INTEGER,
                squad_id INTEGER,
                pubg_id TEXT,
                PRIMARY KEY (order_id, escort_id),
                FOREIGN KEY (order_id) REFERENCES orders (id),
                FOREIGN KEY (escort_id) REFERENCES escorts (id),
                FOREIGN KEY (squad_id) REFERENCES squads (id)
            );
        ''')
        await conn.commit()
    logger.info("База данных успешно инициализирована")

async def get_escort(telegram_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            "SELECT id, squad_id, pubg_id, balance, reputation, completed_orders, username, "
            "rating, rating_count, is_banned, ban_until, restrict_until, rules_accepted "
            "FROM escorts WHERE telegram_id = ?", (telegram_id,)
        )
        return await cursor.fetchone()

async def add_escort(telegram_id: int, username: str):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO escorts (telegram_id, username, rules_accepted) VALUES (?, ?, 0)",
            (telegram_id, username)
        )
        await conn.commit()
    logger.info(f"Добавлен пользователь {telegram_id}")

async def get_squad_escorts(squad_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            "SELECT telegram_id, username, pubg_id, rating FROM escorts WHERE squad_id = ?", (squad_id,)
        )
        return await cursor.fetchall()

async def get_squad_info(squad_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            '''
            SELECT s.name, COUNT(e.id) as member_count,
                   SUM(e.completed_orders) as total_orders,
                   SUM(e.balance) as total_balance,
                   s.rating, s.rating_count
            FROM squads s
            LEFT JOIN escorts e ON e.squad_id = s.id
            WHERE s.id = ?
            GROUP BY s.id
            ''', (squad_id,)
        )
        return await cursor.fetchone()

async def notify_squad(squad_id: int, message: str):
    escorts = await get_squad_escorts(squad_id)
    for telegram_id, _, _, _ in escorts:
        try:
            await bot.send_message(telegram_id, message)
        except Exception as e:
            logger.warning(f"Не удалось уведомить {telegram_id}: {e}")

async def notify_admins(message: str, reply_markup=None):
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, message, reply_markup=reply_markup)
        except Exception as e:
            logger.warning(f"Не удалось уведомить админа {admin_id}: {e}")

async def get_order_applications(order_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            '''
            SELECT e.telegram_id, e.username, e.pubg_id, e.squad_id, s.name
            FROM order_applications oa
            JOIN escorts e ON oa.escort_id = e.id
            LEFT JOIN squads s ON e.squad_id = s.id
            WHERE oa.order_id = ?
            ''', (order_id,)
        )
        return await cursor.fetchall()

async def get_order_info(fanpay_order_id: str):
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            "SELECT id, customer_info, amount, status, squad_id FROM orders WHERE fanpay_order_id = ?",
            (fanpay_order_id,)
        )
        return await cursor.fetchone()

async def update_escort_reputation(escort_id: int, rating: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            '''
            UPDATE escorts SET reputation = reputation + ?, rating = rating + ?, rating_count = rating_count + 1
            WHERE id = ?
            ''', (rating, rating, escort_id)
        )
        await conn.commit()

async def update_squad_reputation(squad_id: int, rating: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            '''
            UPDATE squads SET rating = rating + ?, rating_count = rating_count + 1
            WHERE id = ?
            ''', (rating, squad_id)
        )
        await conn.commit()

async def get_order_escorts(order_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            '''
            SELECT e.telegram_id, e.username, oe.pubg_id, e.squad_id, s.name
            FROM order_escorts oe
            JOIN escorts e ON oe.escort_id = e.id
            LEFT JOIN squads s ON e.squad_id = s.id
            WHERE oe.order_id = ?
            ''', (order_id,)
        )
        return await cursor.fetchall()

# --- Проверка админских прав ---
def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# --- Клавиатуры ---
def get_menu_keyboard(user_id: int):
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🏆 Рейтинг сквадов"),
                KeyboardButton(text="🌟 Рейтинг пользователей")
            ],
            [
                KeyboardButton(text="✅ Завершить заказ"),
                KeyboardButton(text="📋 Мои заказы")
            ],
            [
                KeyboardButton(text="🔢 Ввести PUBG ID"),
                KeyboardButton(text="ℹ️ Информация")
            ],
            [
                KeyboardButton(text="👤 Мой профиль"),
                KeyboardButton(text="📋 Доступные заказы")
            ],
            [
                KeyboardButton(text="🔐 Админ-панель")
            ] if is_admin(user_id) else [],
            [
                KeyboardButton(text="🔙 На главную")
            ]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_admin_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [
                KeyboardButton(text="🏠 Добавить сквад"),
                KeyboardButton(text="📋 Список сквадов")
            ],
            [
                KeyboardButton(text="👤 Добавить сопровождающего"),
                KeyboardButton(text="🗑️ Удалить сопровождающего")
            ],
            [
                KeyboardButton(text="💰 Балансы сопровождающих"),
                KeyboardButton(text="💸 Начислить")
            ],
            [
                KeyboardButton(text="📊 Статистика"),
                KeyboardButton(text="📝 Добавить заказ")
            ],
            [
                KeyboardButton(text="🚫 Бан навсегда"),
                KeyboardButton(text="⏰ Бан на время")
            ],
            [
                KeyboardButton(text="⛔ Ограничить"),
                KeyboardButton(text="👥 Пользователи")
            ],
            [
                KeyboardButton(text="💰 Обнулить баланс"),
                KeyboardButton(text="📊 Все балансы")
            ],
            [
                KeyboardButton(text="📖 Справочник админ-команд"),
                KeyboardButton(text="🔙 На главную")
            ]
        ],
        resize_keyboard=True,
        one_time_keyboard=False
    )
    return keyboard

def get_rules_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Принять правила")],
            [KeyboardButton(text="📜 Политика конфиденциальности")],
            [KeyboardButton(text="📖 Правила")]
        ],
        resize_keyboard=True,
        one_time_keyboard=True
    )
    return keyboard

def get_order_keyboard(order_id: str):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Готово", callback_data=f"join_order_{order_id}")]
    ])
    return keyboard

def get_confirmed_order_keyboard(order_id: str):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Завершить заказ", callback_data=f"complete_order_{order_id}")]
    ])
    return keyboard

def get_rating_keyboard(order_id: str):
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="1 ⭐", callback_data=f"rate_{order_id}_1"),
            InlineKeyboardButton(text="2 ⭐", callback_data=f"rate_{order_id}_2"),
            InlineKeyboardButton(text="3 ⭐", callback_data=f"rate_{order_id}_3"),
            InlineKeyboardButton(text="4 ⭐", callback_data=f"rate_{order_id}_4"),
            InlineKeyboardButton(text="5 ⭐", callback_data=f"rate_{order_id}_5")
        ]
    ])
    return keyboard

# --- Проверка доступа ---
async def check_access(message: types.Message, initial_start: bool = False):
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await add_escort(user_id, message.from_user.username or "Unknown")
            escort = await get_escort(user_id)

        if escort[9]:  # is_banned
            await message.answer(MESSAGES["user_banned"], reply_markup=ReplyKeyboardRemove())
            return False
        if escort[10] and datetime.fromisoformat(escort[10]) > datetime.now():  # ban_until
            await message.answer(MESSAGES["user_banned"], reply_markup=ReplyKeyboardRemove())
            return False
        if escort[11] and datetime.fromisoformat(escort[11]) > datetime.now():  # restrict_until
            await message.answer(MESSAGES["user_restricted"].format(date=escort[11]), reply_markup=ReplyKeyboardRemove())
            return False
        if not escort[12] and initial_start:  # rules_accepted
            await message.answer(MESSAGES["rules_not_accepted"], reply_markup=get_rules_keyboard())
            return False
        return True
    except Exception as e:
        logger.error(f"Ошибка в check_access для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=ReplyKeyboardRemove())
        return False

# --- Обработчики ---
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Unknown"
    try:
        if not await check_access(message, initial_start=True):
            return
        await message.answer(f"{MESSAGES['welcome']}\n📌 Выберите действие:", reply_markup=get_menu_keyboard(user_id))
        logger.info(f"Пользователь {user_id} (@{username}) запустил бота")
    except Exception as e:
        logger.error(f"Ошибка в cmd_start для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=ReplyKeyboardRemove())

@dp.message(Command("ping"))
async def cmd_ping(message: types.Message):
    await message.answer(MESSAGES["ping"], reply_markup=get_menu_keyboard(message.from_user.id))

@dp.message(F.text == "✅ Принять правила")
async def accept_rules(message: types.Message):
    user_id = message.from_user.id
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute("UPDATE escorts SET rules_accepted = 1 WHERE telegram_id = ?", (user_id,))
            await conn.commit()
        await message.answer(f"✅ Правила приняты! Добро пожаловать!\n📌 Выберите действие:", reply_markup=get_menu_keyboard(user_id))
        logger.info(f"Пользователь {user_id} принял правила")
    except Exception as e:
        logger.error(f"Ошибка в accept_rules для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=ReplyKeyboardRemove())

@dp.message(F.text == "🔢 Ввести PUBG ID")
async def enter_pubg_id(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    await message.answer("🔢 Введите ваш PUBG ID:", reply_markup=ReplyKeyboardRemove())
    await state.set_state(Form.pubg_id)

@dp.message(Form.pubg_id)
async def process_pubg_id(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    pubg_id = message.text.strip()
    if not pubg_id:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "UPDATE escorts SET pubg_id = ? WHERE telegram_id = ?",
                (pubg_id, user_id)
            )
            await conn.commit()
        await message.answer(MESSAGES["pubg_id_updated"], reply_markup=get_menu_keyboard(user_id))
        logger.info(f"Пользователь {user_id} обновил PUBG ID: {pubg_id}")
    except Exception as e:
        logger.error(f"Ошибка в process_pubg_id для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
    finally:
        await state.clear()

@dp.message(F.text == "ℹ️ Информация")
async def info_handler(message: types.Message):
    if not await check_access(message):
        return
    try:
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="📜 Политика конфиденциальности", url=PRIVACY_URL)],
            [InlineKeyboardButton(text="📖 Правила", url=RULES_URL)]
        ])
        response = "ℹ️ Информация о боте:"
        await message.answer(response, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Ошибка в info_handler: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(message.from_user.id))

@dp.message(F.text.in_(["📜 Политика конфиденциальности", "📖 Правила"]))
async def rules_links(message: types.Message):
    if not await check_access(message):
        return
    try:
        if message.text == "📜 Политика конфиденциальности":
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📜 Политика конфиденциальности", url=PRIVACY_URL)]
            ])
            await message.answer("📜 Политика конфиденциальности:", reply_markup=keyboard)
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="📖 Правила", url=RULES_URL)]
            ])
            await message.answer("📖 Правила:", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Ошибка в rules_links: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(message.from_user.id))

@dp.message(F.text == "👤 Мой профиль")
async def my_profile(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await add_escort(user_id, message.from_user.username or "Unknown")
            escort = await get_escort(user_id)

        escort_id, squad_id, pubg_id, balance, reputation, completed_orders, username, rating, rating_count, _, ban_until, restrict_until, _ = escort
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (squad_id,))
            squad = await cursor.fetchone()
        avg_rating = rating / rating_count if rating_count > 0 else 0
        response = (
            f"👤 Ваш профиль:\n"
            f"🔹 Username: @{username or 'Unknown'}\n"
            f"🔹 PUBG ID: {pubg_id or 'не указан'}\n"
            f"🏠 Сквад: {squad[0] if squad else 'не назначен'}\n"
            f"💰 Баланс: {balance:.2f} руб.\n"
            f"⭐ Репутация: {reputation}\n"
            f"📊 Выполнено заказов: {completed_orders}\n"
            f"🌟 Рейтинг: {avg_rating:.2f} ⭐ ({rating_count} оценок)"
        )
        await message.answer(response, reply_markup=get_menu_keyboard(user_id))
    except Exception as e:
        logger.error(f"Ошибка в my_profile: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(F.text == "📋 Доступные заказы")
async def available_orders(message: types.Message):
    if not await check_access(message):
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, fanpay_order_id, customer_info, amount FROM orders WHERE status = 'pending'"
            )
            orders = await cursor.fetchall()

        if not orders:
            await message.answer(MESSAGES["no_orders"], reply_markup=get_menu_keyboard(message.from_user.id))
            return

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text=f"#{order_id} - {customer}, {amount:.2f} руб.", callback_data=f"select_order_{db_id}")]
            for db_id, order_id, customer, amount in orders
        ])
        await message.answer("📋 Доступные заказы:", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Ошибка в available_orders: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(message.from_user.id))

@dp.callback_query(F.data.startswith("select_order_"))
async def select_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        order_db_id = int(callback.data.split("_")[-1])
        await callback.answer()
        # Перенаправление на join_order для обработки
        await join_order(callback)
    except Exception as e:
        logger.error(f"Ошибка в select_order для {user_id}: {e}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("join_order_"))
async def join_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await callback.message.answer("⚠️ Ваш профиль не найден. Обратитесь к администратору.", reply_markup=get_menu_keyboard(user_id))
            await callback.answer()
            return
        if not escort[2]:  # pubg_id
            await callback.message.answer("⚠️ Укажите ваш PUBG ID!", reply_markup=get_menu_keyboard(user_id))
            await callback.answer()
            return

        escort_id = escort[0]
        pubg_id = escort[2]
        order_db_id = int(callback.data.split("_")[-1])

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT status, squad_id FROM orders WHERE id = ?", (order_db_id,))
            order = await cursor.fetchone()
            if not order or order[0] != "pending":
                await callback.message.answer(MESSAGES["order_already_in_progress"], reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return

            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_applications WHERE order_id = ? AND escort_id = ?",
                (order_db_id, escort_id)
            )
            if (await cursor.fetchone())[0] > 0:
                await callback.message.answer("⚠️ Вы уже присоединились к этому заказу!", reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return

            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_applications WHERE order_id = ?", (order_db_id,)
            )
            participant_count = (await cursor.fetchone())[0]
            if participant_count >= 4:  # Максимум 4 участника
                await callback.message.answer(MESSAGES["max_participants"], reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return

            await conn.execute(
                "INSERT INTO order_applications (order_id, escort_id, squad_id, pubg_id) VALUES (?, ?, ?, ?) "
                "ON CONFLICT(order_id, escort_id) DO NOTHING",
                (order_db_id, escort_id, escort[1], pubg_id)
            )
            await conn.commit()

        # Обновление сообщения с участниками
        applications = await get_order_applications(order_db_id)
        participants = "\n".join(f"👤 @{u or 'Unknown'} (PUBG ID: {p}, Сквад: {s or 'Не назначен'})" for _, u, p, _, s in applications)
        response = f"📋 Заказ #{order_db_id} в ожидании:\nУчастники:\n{participants if participants else 'Пока никто не присоединился'}\nУчастников: {len(applications)}/4"
        
        # Проверка, достаточно ли участников для начала
        if len(applications) >= 2:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Начать выполнение", callback_data=f"start_order_{order_db_id}")],
                [InlineKeyboardButton(text="Отмена", callback_data=f"cancel_order_{order_db_id}")]
            ])
            await callback.message.edit_text(response, reply_markup=keyboard)
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Отмена", callback_data=f"cancel_order_{order_db_id}")]
            ])
            await callback.message.edit_text(response, reply_markup=keyboard)

        await callback.answer()

    except aiosqlite.IntegrityError as e:
        logger.error(f"Ошибка целостности данных в join_order для {user_id}: {e}")
        await callback.message.answer("⚠️ Ошибка данных. Обратитесь к администратору.", reply_markup=get_menu_keyboard(user_id))
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в join_order для {user_id}: {e}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("start_order_"))
async def start_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        order_db_id = int(callback.data.split("_")[-1])
        escort = await get_escort(user_id)
        if not escort or not escort[1]:  # Проверка профиля и сквада
            await callback.message.answer("⚠️ Ваш профиль не найден или вы не в скваде.", reply_markup=get_menu_keyboard(user_id))
            await callback.answer()
            return

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT fanpay_order_id, status FROM orders WHERE id = ?", (order_db_id,)
            )
            order = await cursor.fetchone()
            if not order or order[1] != "pending":
                await callback.message.answer(MESSAGES["order_already_in_progress"], reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return

            cursor = await conn.execute(
                "SELECT escort_id, squad_id, pubg_id FROM order_applications WHERE order_id = ?", (order_db_id,)
            )
            applications = await cursor.fetchall()
            if len(applications) < 2 or len(applications) > 4:
                await callback.message.answer("⚠️ Для начала выполнения нужно 2-4 участника!", reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return

            # Определяем сквад первого участника как победивший
            winning_squad_id = applications[0][1]  # Сквад первого, кто присоединился
            valid_applications = [app for app in applications if app[1] == winning_squad_id]

            if not valid_applications:
                await callback.message.answer("⚠️ Не удалось определить сквад для заказа.", reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return

            # Перенос только участников из победившего сквада в order_escorts
            for escort_id, _, pubg_id in valid_applications:
                await conn.execute(
                    "INSERT INTO order_escorts (order_id, escort_id, pubg_id) VALUES (?, ?, ?) "
                    "ON CONFLICT(order_id, escort_id) DO NOTHING",
                    (order_db_id, escort_id, pubg_id)
                )
                await conn.execute(
                    "UPDATE escorts SET completed_orders = completed_orders + 1 WHERE id = ?",
                    (escort_id,)
                )
            await conn.execute(
                "UPDATE orders SET status = 'in_progress', squad_id = ? WHERE id = ?",
                (winning_squad_id, order_db_id)
            )
            await conn.execute("DELETE FROM order_applications WHERE order_id = ?", (order_db_id,))
            await conn.commit()

        # Обновление сообщения с новыми данными
        order_id = order[0]
        participants = "\n".join(f"👤 @{u or 'Unknown'} (PUBG ID: {p}, Сквад: {s or 'Не назначен'})" for _, u, p, _, s in await get_order_escorts(order_db_id))
        response = MESSAGES["order_confirmed"].format(order_id=order_id, participants=participants)
        keyboard = get_confirmed_order_keyboard(order_id)
        await callback.message.edit_text(response, reply_markup=keyboard)

        # Уведомление участников
        for telegram_id, _, _, _, _ in await get_order_escorts(order_db_id):
            await bot.send_message(telegram_id, f"📝 Заказ #{order_id} начат! Готовьтесь к сопровождению.", reply_markup=get_menu_keyboard(telegram_id))

        # Уведомление админов
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (winning_squad_id,))
            squad_name = (await cursor.fetchone())[0] or "Не назначен"
        await notify_admins(MESSAGES["order_taken"].format(order_id=order_id, squad_name=squad_name, participants=participants))

        await callback.answer()

    except Exception as e:
        logger.error(f"Ошибка в start_order для {user_id}: {e}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("complete_order_"))
async def complete_order_callback(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    order_id = callback.data.split("_")[-1]
    try:
        escort = await get_escort(user_id)
        if not escort:
            await callback.message.answer("⚠️ Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
            await callback.answer()
            return
        escort_id = escort[0]
        username = escort[6] or "Unknown"
        pubg_id = escort[2] or "Не указан"

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, squad_id, amount FROM orders WHERE fanpay_order_id = ? AND status = 'in_progress'",
                (order_id,)
            )
            order = await cursor.fetchone()
            if not order:
                await callback.message.answer(MESSAGES["order_already_completed"], reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return
            order_db_id, squad_id, amount = order

            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_escorts WHERE order_id = ? AND escort_id = ?",
                (order_db_id, escort_id)
            )
            if (await cursor.fetchone())[0] == 0:
                await callback.message.answer("⚠️ Вы не участвуете в этом заказе.", reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return

            await conn.execute(
                "UPDATE orders SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE id = ?",
                (order_db_id,)
            )
            await conn.commit()

        await callback.message.edit_text(MESSAGES["order_completed"].format(order_id=order_id, username=username, telegram_id=user_id, pubg_id=pubg_id), reply_markup=None)
        admin_message = MESSAGES["order_completed"].format(order_id=order_id, username=username, telegram_id=user_id, pubg_id=pubg_id)
        await notify_admins(admin_message)

        # Уведомление всех участников о завершении заказа
        participants = await get_order_escorts(order_db_id)
        for telegram_id, _, _, _, _ in participants:
            try:
                await bot.send_message(telegram_id, f"✅ Заказ #{order_id} завершен! Ожидайте оценки.", reply_markup=get_menu_keyboard(telegram_id))
            except Exception as e:
                logger.warning(f"Не удалось уведомить {telegram_id} о завершении заказа: {e}")

        # Уведомление админов с запросом оценки
        rating_keyboard = get_rating_keyboard(order_id)
        await notify_admins(MESSAGES["rate_order"].format(order_id=order_id), reply_markup=rating_keyboard)

        await callback.answer()

    except Exception as e:
        logger.error(f"Ошибка в complete_order_callback для {user_id}: {e}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

@dp.callback_query(F.data.startswith("cancel_order_"))
async def cancel_order(callback: types.CallbackQuery):
    user_id = callback.from_user.id
    try:
        order_db_id = int(callback.data.split("_")[-1])
        await callback.answer()

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT escort_id FROM order_applications WHERE order_id = ? AND escort_id = (SELECT id FROM escorts WHERE telegram_id = ?)",
                (order_db_id, user_id)
            )
            escort = await cursor.fetchone()
            if not escort:
                await callback.message.answer("⚠️ Вы не участвуете в этом заказе.", reply_markup=get_menu_keyboard(user_id))
                return

            await conn.execute(
                "DELETE FROM order_applications WHERE order_id = ? AND escort_id = (SELECT id FROM escorts WHERE telegram_id = ?)",
                (order_db_id, user_id)
            )
            await conn.commit()

        applications = await get_order_applications(order_db_id)
        participants = "\n".join(f"👤 @{u or 'Unknown'} (PUBG ID: {p}, Сквад: {s or 'Не назначен'})" for _, u, p, _, s in applications)
        response = f"📋 Заказ #{order_db_id} в ожидании:\nУчастники:\n{participants if participants else 'Пока никто не присоединился'}\nУчастников: {len(applications)}/4"
        
        if len(applications) >= 2:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Начать выполнение", callback_data=f"start_order_{order_db_id}")],
                [InlineKeyboardButton(text="Отмена", callback_data=f"cancel_order_{order_db_id}")]
            ])
            await callback.message.edit_text(response, reply_markup=keyboard)
        else:
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Отмена", callback_data=f"cancel_order_{order_db_id}")]
            ])
            await callback.message.edit_text(response, reply_markup=keyboard)

    except Exception as e:
        logger.error(f"Ошибка в cancel_order для {user_id}: {e}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(F.text == "📋 Мои заказы")
async def my_orders(message: types.Message):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("⚠️ Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
            return
        escort_id = escort[0]
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT o.fanpay_order_id, o.customer_info, o.amount, o.status
                FROM orders o
                JOIN order_escorts oe ON o.id = oe.order_id
                JOIN escorts e ON oe.escort_id = e.id
                WHERE e.telegram_id = ?
                ''', (user_id,)
            )
            orders = await cursor.fetchall()

        if not orders:
            await message.answer(MESSAGES["no_active_orders"], reply_markup=get_menu_keyboard(user_id))
            return

        response = "📋 Ваши заказы:\n"
        for order_id, customer, amount, status in orders:
            status_text = "Ожидает" if status == "pending" else "В процессе" if status == "in_progress" else "Завершен"
            response += f"#{order_id} - {customer}, {amount:.2f} руб., Статус: {status_text}\n"
        await message.answer(response, reply_markup=get_menu_keyboard(user_id))
    except Exception as e:
        logger.error(f"Ошибка в my_orders: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(F.text == "✅ Завершить заказ")
async def complete_order(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("⚠️ Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
            return
        escort_id = escort[0]
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT o.fanpay_order_id, o.id, o.squad_id, o.amount
                FROM orders o
                JOIN order_escorts oe ON o.id = oe.order_id
                JOIN escorts e ON oe.escort_id = e.id
                WHERE e.telegram_id = ? AND o.status = 'in_progress'
                ''', (user_id,)
            )
            orders = await cursor.fetchall()

        if not orders:
            await message.answer(MESSAGES["no_active_orders"], reply_markup=get_menu_keyboard(user_id))
            return

        response = "✅ Введите ID заказа для завершения:\n"
        for order_id, _, _, amount in orders:
            response += f"#{order_id} - {amount:.2f} руб.\n"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.complete_order)
    except Exception as e:
        logger.error(f"Ошибка в complete_order: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))

@dp.message(Form.complete_order)
async def process_complete_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    order_id = message.text.strip()
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("⚠️ Ваш профиль не найден.", reply_markup=get_menu_keyboard(user_id))
            await state.clear()
            return
        escort_id = escort[0]
        username = escort[6] or "Unknown"
        pubg_id = escort[2] or "Не указан"
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, squad_id, amount FROM orders WHERE fanpay_order_id = ? AND status = 'in_progress'",
                (order_id,)
            )
            order = await cursor.fetchone()
            if not order:
                await message.answer(MESSAGES["order_already_completed"], reply_markup=get_menu_keyboard(user_id))
                await state.clear()
                return
            order_db_id, squad_id, amount = order

            # Проверка, начислен ли баланс (простая проверка для админов)
            cursor = await conn.execute("SELECT SUM(balance) FROM escorts WHERE squad_id = ?", (squad_id,))
            total_balance = (await cursor.fetchone())[0] or 0
            if total_balance < amount:
                await message.answer(f"⚠️ Общий баланс сквада ({total_balance:.2f} руб.) меньше суммы заказа ({amount:.2f} руб.). Обратитесь к администратору.", reply_markup=get_menu_keyboard(user_id))
                await state.clear()
                return

            await conn.execute(
                "UPDATE orders SET status = 'completed', completed_at = CURRENT_TIMESTAMP WHERE id = ?",
                (order_db_id,)
            )
            await conn.commit()

        await message.answer(MESSAGES["order_completed"].format(order_id=order_id, username=username, telegram_id=user_id, pubg_id=pubg_id), reply_markup=get_menu_keyboard(user_id))
        admin_message = MESSAGES["order_completed"].format(order_id=order_id, username=username, telegram_id=user_id, pubg_id=pubg_id)
        await notify_admins(admin_message)

        # Уведомление всех участников о завершении заказа
        participants = await get_order_escorts(order_db_id)
        for telegram_id, _, _, _, _ in participants:
            try:
                await bot.send_message(telegram_id, f"✅ Заказ #{order_id} завершен! Ожидайте оценки.")
            except Exception as e:
                logger.warning(f"Не удалось уведомить {telegram_id} о завершении заказа: {e}")

        # Уведомление админов с запросом оценки
        rating_keyboard = get_rating_keyboard(order_id)
        await notify_admins(MESSAGES["rate_order"].format(order_id=order_id), reply_markup=rating_keyboard)

        logger.info(f"Заказ #{order_id} завершен пользователем {user_id}")
        await state.clear()
    except Exception as e:
        logger.error(f"Ошибка в process_complete_order для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await state.clear()

@dp.callback_query(F.data.startswith("rate_"))
async def rate_order(callback: types.CallbackQuery, state: FSMContext):
    user_id = callback.from_user.id
    if not is_admin(user_id):
        await callback.message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()
        return
    try:
        _, order_id, rating = callback.data.split("_")
        order_id = order_id
        rating = int(rating)
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, squad_id FROM orders WHERE fanpay_order_id = ? AND status = 'completed'",
                (order_id,)
            )
            order = await cursor.fetchone()
            if not order:
                await callback.message.answer("⚠️ Заказ не найден или не завершен.", reply_markup=get_menu_keyboard(user_id))
                await callback.answer()
                return
            order_db_id, squad_id = order

            cursor = await conn.execute(
                "SELECT escort_id FROM order_escorts WHERE order_id = ?", (order_db_id,)
            )
            escorts = await cursor.fetchall()

            for (escort_id,) in escorts:
                await update_escort_reputation(escort_id, rating)
            await update_squad_reputation(squad_id, rating)
            await conn.execute(
                "UPDATE orders SET rating = ? WHERE id = ?", (rating, order_db_id)
            )
            await conn.commit()

        await callback.message.edit_text(MESSAGES["rating_submitted"].format(rating=rating, order_id=order_id), reply_markup=None)
        await notify_squad(squad_id, f"🌟 Заказ #{order_id} получил оценку {rating}!")
        await callback.answer()
    except Exception as e:
        logger.error(f"Ошибка в rate_order для {user_id}: {e}")
        await callback.message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(user_id))
        await callback.answer()

# --- Остальные обработчики ---
@dp.message(F.text == "🔐 Админ-панель")
async def admin_panel(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    await message.answer("🔐 Админ-панель:", reply_markup=get_admin_keyboard())

@dp.message(F.text == "🏆 Рейтинг сквадов")
async def squad_rating(message: types.Message):
    if not await check_access(message):
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT name, rating, rating_count FROM squads ORDER BY rating DESC"
            )
            squads = await cursor.fetchall()

        if not squads:
            await message.answer(MESSAGES["no_squads"], reply_markup=get_menu_keyboard(message.from_user.id))
            return

        response = "🏆 Рейтинг сквадов:\n"
        for name, rating, rating_count in squads:
            avg_rating = rating / rating_count if rating_count > 0 else 0
            response += f"🏠 {name}: {avg_rating:.2f} ⭐ ({rating_count} оценок)\n"
        await message.answer(response, reply_markup=get_menu_keyboard(message.from_user.id))
    except Exception as e:
        logger.error(f"Ошибка в squad_rating: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(message.from_user.id))

@dp.message(F.text == "🌟 Рейтинг пользователей")
async def user_rating(message: types.Message):
    if not await check_access(message):
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT username, rating, rating_count FROM escorts ORDER BY rating DESC"
            )
            escorts = await cursor.fetchall()

        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_menu_keyboard(message.from_user.id))
            return

        response = "🌟 Рейтинг пользователей:\n"
        for username, rating, rating_count in escorts:
            avg_rating = rating / rating_count if rating_count > 0 else 0
            response += f"👤 @{username or 'Unknown'}: {avg_rating:.2f} ⭐ ({rating_count} оценок)\n"
        await message.answer(response, reply_markup=get_menu_keyboard(message.from_user.id))
    except Exception as e:
        logger.error(f"Ошибка в user_rating: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_menu_keyboard(message.from_user.id))

@dp.message(F.text == "🏠 Добавить сквад")
async def add_squad(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    await message.answer("🏠 Введите название нового сквада:", reply_markup=ReplyKeyboardRemove())
    await state.set_state(Form.squad_name)

@dp.message(Form.squad_name)
async def process_squad_name(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    squad_name = message.text.strip()
    if not squad_name:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
        await state.clear()
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute("INSERT INTO squads (name) VALUES (?)", (squad_name,))
            await conn.commit()
        await message.answer(f"🏠 Сквад '{squad_name}' успешно добавлен!", reply_markup=get_admin_keyboard())
        logger.info(f"Добавлен сквад: {squad_name}")
        await notify_admins(f"🏠 Новый сквад '{squad_name}' создан")
    except aiosqlite.IntegrityError:
        await message.answer(f"⚠️ Сквад '{squad_name}' уже существует!", reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в process_squad_name для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
    finally:
        await state.clear()

@dp.message(F.text == "📋 Список сквадов")
async def list_squads(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, name, (SELECT COUNT(*) FROM escorts WHERE squad_id = squads.id) as count "
                "FROM squads"
            )
            squads = await cursor.fetchall()

        if not squads:
            await message.answer(MESSAGES["no_squads"], reply_markup=get_admin_keyboard())
            return

        response = "📋 Список сквадов:\n"
        for squad_id, name, count in squads:
            response += f"{squad_id}. {name} ({count} участников)\n"
        await message.answer(response, reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в list_squads: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "👤 Добавить сопровождающего")
async def add_escort_handler(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    await message.answer(
        "👤 Введите Telegram ID и название сквада через пробел:\nПример: 123456789 НазваниеСквада",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Form.escort_info)

@dp.message(Form.escort_info)
async def process_escort_info(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        parts = message.text.split(maxsplit=1)
        if len(parts) != 2:
            await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
            await state.clear()
            return
        escort_id = int(parts[0])
        squad_name = parts[1].strip()

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id FROM squads WHERE name = ?", (squad_name,))
            squad = await cursor.fetchone()
            if not squad:
                await message.answer(f"⚠️ Сквад '{squad_name}' не найден!", reply_markup=get_admin_keyboard())
                await state.clear()
                return

            cursor = await conn.execute("SELECT COUNT(*) FROM escorts WHERE squad_id = ?", (squad[0],))
            member_count = (await cursor.fetchone())[0]
            if member_count >= 6:
                await message.answer(MESSAGES["squad_full"].format(squad_name=squad_name), reply_markup=get_admin_keyboard())
                await state.clear()
                return

            await conn.execute(
                "INSERT OR REPLACE INTO escorts (telegram_id, squad_id, username, rules_accepted) "
                "VALUES (?, ?, (SELECT username FROM escorts WHERE telegram_id = ?), 0)",
                (escort_id, squad[0], escort_id)
            )
            await conn.commit()

        await message.answer(f"👤 Пользователь {escort_id} добавлен в сквад '{squad_name}'!", reply_markup=get_admin_keyboard())
        logger.info(f"Добавлен сопровождающий {escort_id} в сквад {squad_name}")
        await notify_admins(f"👤 Пользователь {escort_id} добавлен в сквад '{squad_name}'")
    except ValueError:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в process_escort_info для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
    finally:
        await state.clear()

@dp.message(F.text == "🗑️ Удалить сопровождающего")
async def remove_escort(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()

        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return

        response = "👤 Список сопровождающих (ID - username):\n"
        for telegram_id, username in escorts:
            response += f"{telegram_id} - @{username or 'Unknown'}\n"
        response += "\nВведите ID сопровождающего для удаления:"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.escort_info)
    except Exception as e:
        logger.error(f"Ошибка в remove_escort: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "💰 Балансы сопровождающих")
async def escort_balances(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT e.telegram_id, e.username, e.balance, s.name FROM escorts e LEFT JOIN squads s ON e.squad_id = s.id"
            )
            escorts = await cursor.fetchall()

        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return

        response = "💰 Балансы сопровождающих:\n"
        for telegram_id, username, balance, squad_name in escorts:
            response += f"ID: {telegram_id}, @{username or 'Unknown'}, Сквад: {squad_name or 'не назначен'}, Баланс: {balance:.2f} руб.\n"
        await message.answer(response, reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в escort_balances: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "💸 Начислить")
async def add_balance(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()

        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return

        response = "👤 Список сопровождающих (ID - username):\n"
        for telegram_id, username in escorts:
            response += f"{telegram_id} - @{username or 'Unknown'}\n"
        response += "\nВведите ID сопровождающего и сумму через пробел:\nПример: 123456789 500"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.balance_amount)
    except Exception as e:
        logger.error(f"Ошибка в add_balance: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(Form.balance_amount)
async def process_balance_amount(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        parts = message.text.split(maxsplit=1)
        if len(parts) != 2:
            await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
            await state.clear()
            return
        target_id = int(parts[0])
        amount = float(parts[1])
        if amount <= 0:
            await message.answer("❌ Сумма должна быть положительной!", reply_markup=get_admin_keyboard())
            await state.clear()
            return

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "UPDATE escorts SET balance = balance + ? WHERE telegram_id = ?",
                (amount, target_id)
            )
            await conn.commit()
            if cursor.rowcount > 0:
                await message.answer(MESSAGES["balance_added"].format(user_id=target_id, amount=amount), reply_markup=get_admin_keyboard())
                logger.info(f"Начислено {amount} руб. пользователю {target_id} администратором {user_id}")
                try:
                    await bot.send_message(target_id, f"💸 Вам начислено {amount} руб. на баланс!")
                except Exception as e:
                    logger.warning(f"Не удалось уведомить {target_id}: {e}")
            else:
                await message.answer(f"⚠️ Пользователь {target_id} не найден.", reply_markup=get_admin_keyboard())
    except ValueError:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в process_balance_amount для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
    finally:
        await state.clear()

@dp.message(F.text == "📊 Статистика")
async def squad_statistics(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT s.name, COUNT(e.id) as member_count,
                       SUM(e.completed_orders) as total_orders,
                       SUM(e.balance) as total_balance,
                       s.rating, s.rating_count
                FROM squads s
                LEFT JOIN escorts e ON e.squad_id = s.id
                GROUP BY s.id
                '''
            )
            squads = await cursor.fetchall()

        if not squads:
            await message.answer(MESSAGES["no_squads"], reply_markup=get_admin_keyboard())
            return

        response = "📊 Статистика сквадов:\n"
        for name, member_count, total_orders, total_balance, rating, rating_count in squads:
            avg_rating = rating / rating_count if rating_count > 0 else 0
            response += (
                f"🏠 {name}\n"
                f"👥 Участников: {member_count}\n"
                f"📋 Заказов: {total_orders or 0}\n"
                f"💰 Заработок: {total_balance or 0:.2f} руб.\n"
                f"🌟 Рейтинг: {avg_rating:.2f} ⭐ ({rating_count} оценок)\n\n"
            )
        await message.answer(response, reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в squad_statistics: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "📝 Добавить заказ")
async def add_order(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    await message.answer(
        "📝 Введите ID заказа, сумму, описание и имя клиента через пробел:\n"
        "Пример: 789 2000 Продажа_предмета Client1",
        reply_markup=ReplyKeyboardRemove()
    )
    await state.set_state(Form.add_order)

@dp.message(Form.add_order)
async def process_add_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        parts = message.text.split(maxsplit=3)
        if len(parts) != 4:
            await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
            await state.clear()
            return
        order_id, amount, description, customer = parts[0], float(parts[1]), parts[2], parts[3]
        if amount <= 0:
            await message.answer("❌ Сумма должна быть положительной!", reply_markup=get_admin_keyboard())
            await state.clear()
            return

        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "INSERT INTO orders (fanpay_order_id, customer_info, amount, status) VALUES (?, ?, ?, 'pending')",
                (order_id, customer, amount)
            )
            await conn.commit()

        await message.answer(
            MESSAGES["order_added"].format(order_id=order_id, amount=amount, description=description, customer=customer),
            reply_markup=get_admin_keyboard()
        )
        logger.info(f"Добавлен заказ #{order_id} администратором {user_id}")
        await notify_admins(f"📝 Новый заказ #{order_id} добавлен: {amount} руб., {description}, клиент: {customer}")
    except ValueError:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в process_add_order для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
    finally:
        await state.clear()

@dp.message(F.text == "🚫 Бан навсегда")
async def ban_user_permanent(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()

        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return

        response = "👤 Список пользователей (ID - username):\n"
        for telegram_id, username in escorts:
            response += f"{telegram_id} - @{username or 'Unknown'}\n"
        response += "\nВведите ID пользователя для блокировки:"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.escort_info)
    except Exception as e:
        logger.error(f"Ошибка в ban_user_permanent: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "⏰ Бан на время")
async def ban_user_temporary(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()

        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return

        response = "👤 Список пользователей (ID - username):\n"
        for telegram_id, username in escorts:
            response += f"{telegram_id} - @{username or 'Unknown'}\n"
        response += "\nВведите ID пользователя и длительность в днях через пробел:\nПример: 123456789 7"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.ban_duration)
    except Exception as e:
        logger.error(f"Ошибка в ban_user_temporary: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(Form.ban_duration)
async def process_ban_duration(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        parts = message.text.split()
        if len(parts) != 2:
            await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
            await state.clear()
            return
        target_id = int(parts[0])
        days = int(parts[1])
        ban_until = datetime.now() + timedelta(days=days)
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "UPDATE escorts SET ban_until = ? WHERE telegram_id = ?",
                (ban_until.isoformat(), target_id)
            )
            await conn.commit()
            if conn.rowcount > 0:
                await message.answer(f"🚫 Пользователь {target_id} заблокирован до {ban_until}", reply_markup=get_admin_keyboard())
                logger.info(f"Пользователь {target_id} заблокирован до {ban_until}")
                try:
                    await bot.send_message(target_id, f"🚫 Вы заблокированы до {ban_until}")
                except Exception as e:
                    logger.warning(f"Не удалось уведомить {target_id}: {e}")
            else:
                await message.answer(f"⚠️ Пользователь {target_id} не найден.", reply_markup=get_admin_keyboard())
    except ValueError:
        await message.answer(MESSAGES["invalid_format"], reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в process_ban_duration для {user_id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
    finally:
        await state.clear()

@dp.message(F.text == "⛔ Ограничить")
async def restrict_user(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()

        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return

        response = "👤 Список пользователей (ID - username):\n"
        for telegram_id, username in escorts:
            response += f"{telegram_id} - @{username or 'Unknown'}\n"
        response += "\nВведите ID пользователя и длительность в днях через пробел:\nПример: 123456789 7"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.ban_duration)
    except Exception as e:
        logger.error(f"Ошибка в restrict_user: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "👥 Пользователи")
async def list_users(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id, e.username, e.pubg_id, s.name, e.is_banned, e.ban_until, e.restrict_until
                FROM escorts e
                LEFT JOIN squads s ON e.squad_id = s.id
                '''
            )
            escorts = await cursor.fetchall()

        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return

        response = "👥 Список всех пользователей:\n"
        for telegram_id, username, pubg_id, squad_name, is_banned, ban_until, restrict_until in escorts:
            status = "✅ Активен"
            if is_banned:
                status = "🚫 Заблокирован навсегда"
            elif ban_until and datetime.fromisoformat(ban_until) > datetime.now():
                status = f"🚫 Заблокирован до {ban_until}"
            elif restrict_until and datetime.fromisoformat(restrict_until) > datetime.now():
                status = f"⛔ Ограничен до {restrict_until}"
            response += (
                f"ID: {telegram_id}, @{username or 'Unknown'}, PUBG ID: {pubg_id or 'Не указан'}, "
                f"Сквад: {squad_name or 'Не назначен'}, Статус: {status}\n"
            )
        await message.answer(response, reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в list_users: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "💰 Обнулить баланс")
async def zero_balance(message: types.Message, state: FSMContext):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()

        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return

        response = "👤 Список пользователей (ID - username):\n"
        for telegram_id, username in escorts:
            response += f"{telegram_id} - @{username or 'Unknown'}\n"
        response += "\nВведите ID пользователя для обнуления баланса:"
        await message.answer(response, reply_markup=ReplyKeyboardRemove())
        await state.set_state(Form.escort_info)
    except Exception as e:
        logger.error(f"Ошибка в zero_balance: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "📊 Все балансы")
async def view_all_balances(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT telegram_id, username, balance FROM escorts"
            )
            escorts = await cursor.fetchall()

        if not escorts:
            await message.answer(MESSAGES["no_escorts"], reply_markup=get_admin_keyboard())
            return

        response = "📊 Балансы всех пользователей:\n"
        for telegram_id, username, balance in escorts:
            response += f"ID: {telegram_id}, @{username or 'Unknown'}, Баланс: {balance:.2f} руб.\n"
        await message.answer(response, reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в view_all_balances: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())
@dp.message(F.text == "📖 Справочник админ-команд")
async def admin_commands_help(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        response = (
            "📖 Справочник админ-команд:\n"
            "🏠 Добавить сквад - Создаёт новый сквад (максимум 6 участников).\n"
            "📋 Список сквадов - Показывает все сквады и количество участников.\n"
            "👤 Добавить сопровождающего - Добавляет пользователя в указанный сквад (через Telegram ID и название сквада).\n"
            "🗑️ Удалить сопровождающего - Удаляет пользователя по Telegram ID.\n"
            "💰 Балансы сопровождающих - Показывает балансы всех пользователей.\n"
            "💸 Начислить - Начисляет сумму на баланс пользователя (ID и сумма).\n"
            "📊 Статистика - Показывает статистику по сквадам.\n"
            "📝 Добавить заказ - Добавляет новый заказ (ID, сумма, описание, клиент).\n"
            "🚫 Бан навсегда - Блокирует пользователя навсегда по ID.\n"
            "⏰ Бан на время - Блокирует пользователя на указанное количество дней.\n"
            "⛔ Ограничить - Ограничивает доступ пользователя на указанное количество дней.\n"
            "👥 Пользователи - Показывает список всех пользователей с их статусом.\n"
            "💰 Обнулить баланс - Обнуляет баланс пользователя по ID.\n"
            "📊 Все балансы - Показывает балансы всех пользователей.\n"
        )
        await message.answer(response, reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в admin_commands_help: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

@dp.message(F.text == "🔙 На главную")
async def back_to_menu(message: types.Message):
    if not await check_access(message):
        return
    await message.answer("🔙 Вы вернулись в главное меню:", reply_markup=get_menu_keyboard(message.from_user.id))

# --- Команда /stats для общей статистики ---
@dp.message(Command("stats"))
async def cmd_stats(message: types.Message):
    if not await check_access(message):
        return
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"], reply_markup=get_menu_keyboard(message.from_user.id))
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT COUNT(*) FROM escorts")
            total_escorts = (await cursor.fetchone())[0]
            cursor = await conn.execute("SELECT COUNT(*) FROM squads")
            total_squads = (await cursor.fetchone())[0]
            cursor = await conn.execute("SELECT COUNT(*) FROM orders WHERE status = 'completed'")
            completed_orders = (await cursor.fetchone())[0]
            cursor = await conn.execute("SELECT SUM(amount) FROM orders WHERE status = 'completed'")
            total_earnings = (await cursor.fetchone())[0] or 0
            cursor = await conn.execute("SELECT AVG(rating) FROM orders WHERE status = 'completed'")
            avg_rating = (await cursor.fetchone())[0] or 0

        response = (
            "📊 Общая статистика бота:\n"
            f"👥 Сопровождающих: {total_escorts}\n"
            f"🏠 Сквадов: {total_squads}\n"
            f"✅ Завершённых заказов: {completed_orders}\n"
            f"💰 Общий заработок: {total_earnings:.2f} руб.\n"
            f"🌟 Средний рейтинг заказов: {avg_rating:.2f} ⭐"
        )
        await message.answer(response, reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в cmd_stats для {message.from_user.id}: {e}")
        await message.answer(MESSAGES["error"], reply_markup=get_admin_keyboard())

# --- Запуск бота ---
async def main():
    try:
        # Настройка веб-сервера для пинга
        app = web.Application()
        app.router.add_get('/ping', ping)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, '0.0.0.0', 8080)
        await site.start()
        logger.info("Веб-сервер запущен на порту 8080")

        # Инициализация базы данных
        await init_db()

        # Запуск бота
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")
    finally:
        await bot.session.close()

if __name__ == "__main__":
    asyncio.run(main())
