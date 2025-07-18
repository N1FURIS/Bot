import logging
import aiosqlite
import asyncio
import os
from datetime import datetime
from aiogram import Bot, Dispatcher, types, F
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.context import FSMContext
from aiogram.filters import CommandStart, StateFilter
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton, ReplyKeyboardMarkup, KeyboardButton

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('bot.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Загрузка переменных окружения из Replit Secrets
BOT_TOKEN = os.getenv("BOT_TOKEN", "7807764002:AAGwGZmzbz-kroPyWf9kp2C0JmRdjob-Fpc")
ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_IDS", "8123068186").split(",") if id]

# Проверка обязательных переменных
if not BOT_TOKEN:
    logger.error("Отсутствует BOT_TOKEN")
    raise ValueError("Не указан BOT_TOKEN")

# Константы
DB_PATH = 'fanpay_bot.db'
MESSAGES = {
    "welcome": "Добро пожаловать в бота сопровождения PUBG Mobile - Metro Royale! 🎮",
    "no_access": "❌ У вас нет доступа к этой команде.",
    "no_squads": "🏠 Нет доступных сквадов.",
    "no_escorts": "👤 Нет зарегистрированных сопровождающих.",
    "no_orders": "📋 Сейчас нет доступных заказов.",
    "no_active_orders": "📋 У вас нет активных заказов.",
    "error": "⚠️ Произошла ошибка. Попробуйте снова позже.",
    "invalid_format": "❌ Неверный формат ввода. Попробуйте снова.",
    "order_completed": "✅ Заказ #{order_id} успешно завершен!",
    "order_already_completed": "⚠️ Заказ #{order_id} уже завершен.",
    "balance_added": "💸 Баланс {amount} руб. начислен пользователю {user_id}",
    "squad_full": "⚠️ Сквад '{squad_name}' уже имеет максимум 6 участников!",
    "squad_too_small": "⚠️ В скваде должно быть минимум 3 участника для принятия заказа!",
    "order_added": "📝 Заказ #{order_id} добавлен! Сумма: {amount} руб., Описание: {description}, Клиент: {customer}"
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
    complete_order_rating = State()
    add_order = State()

# Вручную заданные данные заказов
ORDERS = [
    {"id": "123", "amount": 1000.0, "description": "Продажа золота", "customer": "Client1", "category": "accompaniment"},
    {"id": "456", "amount": 5000.0, "description": "Продажа аккаунта", "customer": "Client2", "category": "accompaniment"}
]

# Пустой список сообщений
MESSAGES_DATA = []

# --- Функции для работы с базой данных ---
async def init_db():
    """Инициализация базы данных."""
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.executescript('''
            CREATE TABLE IF NOT EXISTS squads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
        ''')
        await conn.commit()
    logger.info("База данных инициализирована")

async def get_escort(telegram_id: int):
    """Получение информации о сопровождающем."""
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            "SELECT id, squad_id, pubg_id, balance, reputation, completed_orders, username "
            "FROM escorts WHERE telegram_id = ?", (telegram_id,)
        )
        return await cursor.fetchone()

async def add_escort(telegram_id: int, username: str):
    """Добавление нового сопровождающего."""
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO escorts (telegram_id, username) VALUES (?, ?)",
            (telegram_id, username)
        )
        await conn.commit()

async def get_squad_escorts(squad_id: int):
    """Получение списка Telegram ID сопровождающих в скваде."""
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            "SELECT telegram_id, username, pubg_id FROM escorts WHERE squad_id = ?", (squad_id,)
        )
        return await cursor.fetchall()

async def get_squad_info(squad_id: int):
    """Получение информации о скваде."""
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute(
            '''
            SELECT s.name, COUNT(e.id) as member_count,
                   SUM(e.completed_orders) as total_orders,
                   SUM(e.balance) as total_balance
            FROM squads s
            LEFT JOIN escorts e ON e.squad_id = s.id
            WHERE s.id = ?
            GROUP BY s.id
            ''', (squad_id,)
        )
        return await cursor.fetchone()

async def notify_squad(squad_id: int, message: str):
    """Отправка уведомления всем сопровождающим в скваде."""
    try:
        escorts = await get_squad_escorts(squad_id)
        for telegram_id, _, _ in escorts:
            try:
                await bot.send_message(telegram_id, message)
            except Exception as e:
                logger.warning(f"Не удалось уведомить {telegram_id}: {e}")
    except Exception as e:
        logger.error(f"Ошибка уведомления сквада {squad_id}: {e}")

async def notify_admins(message: str):
    """Отправка уведомления админам."""
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_message(admin_id, message)
        except Exception as e:
            logger.warning(f"Не удалось уведомить админа {admin_id}: {e}")

# --- Функции для работы с вручную заданными данными ---
async def fetch_manual_orders():
    """Получение вручную заданных заказов и синхронизация с базой."""
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            for order in ORDERS:
                if order.get('category') == 'accompaniment':
                    await conn.execute(
                        '''
                        INSERT OR IGNORE INTO orders (fanpay_order_id, customer_info, amount, status)
                        VALUES (?, ?, ?, 'pending')
                        ''',
                        (order['id'], order.get('customer', 'Unknown'), order['amount'])
                    )
            await conn.commit()
        logger.info("Заказы синхронизированы из локального списка")
        await notify_admins("🔔 Новые заказы синхронизированы из локального списка")
        return ORDERS
    except Exception as e:
        logger.error(f"Ошибка при получении заказов: {e}")
        return []

# --- Проверка админских прав ---
def is_admin(user_id: int) -> bool:
    if not ADMIN_IDS:
        logger.warning("Список ADMIN_IDS пуст")
        return False
    return user_id in ADMIN_IDS

# --- Клавиатуры ---
def get_main_keyboard(user_id: int):
    """Главное меню."""
    keyboard = ReplyKeyboardMarkup(keyboard=[], resize_keyboard=True)
    buttons = [
        [KeyboardButton(text="👤 Мой профиль")],
        [KeyboardButton(text="📋 Доступные заказы")],
        [KeyboardButton(text="📊 Мои заказы")],
        [KeyboardButton(text="✅ Завершить заказ")]
    ]
    if is_admin(user_id):
        buttons.append([KeyboardButton(text="🔐 Админ-панель")])
    keyboard.keyboard = buttons
    return keyboard

def get_admin_keyboard():
    """Админ-панель."""
    keyboard = ReplyKeyboardMarkup(keyboard=[], resize_keyboard=True)
    buttons = [
        [KeyboardButton(text="🏠 Добавить сквад")],
        [KeyboardButton(text="📋 Список сквадов")],
        [KeyboardButton(text="👤 Добавить сопровождающего")],
        [KeyboardButton(text="🗑️ Удалить сопровождающего")],
        [KeyboardButton(text="💰 Балансы сопровождающих")],
        [KeyboardButton(text="💸 Начислить баланс")],
        [KeyboardButton(text="📊 Статистика сквадов")],
        [KeyboardButton(text="📝 Добавить заказ")],
        [KeyboardButton(text="📖 Справочник")],
        [KeyboardButton(text="🔙 На главную")]
    ]
    keyboard.keyboard = buttons
    return keyboard

def get_squad_menu(squad_id: int):
    """Всплывающее меню для сквада."""
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Статистика сквада", callback_data=f"squad_stats_{squad_id}")],
        [InlineKeyboardButton(text="👥 Состав сквада", callback_data=f"squad_members_{squad_id}")]
    ])
    return keyboard

# --- Обработчики ---
@dp.message(CommandStart())
async def cmd_start(message: types.Message):
    user_id = message.from_user.id
    username = message.from_user.username or "Unknown"
    try:
        await add_escort(user_id, username)
        await message.answer(MESSAGES["welcome"], reply_markup=get_main_keyboard(user_id))
        logger.info(f"Пользователь {user_id} (@{username}) запустил бота")
    except Exception as e:
        logger.error(f"Ошибка в cmd_start: {e}")
        await message.answer(MESSAGES["error"])

@dp.message(F.text == "🔙 На главную")
async def main_menu(message: types.Message):
    try:
        await message.answer("Главное меню:", reply_markup=get_main_keyboard(message.from_user.id))
    except Exception as e:
        logger.error(f"Ошибка в main_menu: {e}")
        await message.answer(MESSAGES["error"])

@dp.message(F.text == "🔐 Админ-панель")
async def admin_panel(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"])
        return
    try:
        await message.answer("Админ-панель:", reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в admin_panel: {e}")
        await message.answer(MESSAGES["error"])

@dp.message(F.text == "📖 Справочник")
async def admin_help(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"])
        return
    try:
        response = (
            "📖 Справочник админ-панели:\n"
            "🏠 Добавить сквад - создает новый сквад (3-6 участников).\n"
            "📋 Список сквадов - показывает все сквады и их статистику.\n"
            "👤 Добавить сопровождающего - добавляет пользователя в сквад.\n"
            "🗑️ Удалить сопровождающего - удаляет пользователя из базы.\n"
            "💰 Балансы сопровождающих - показывает балансы всех сопровождающих.\n"
            "💸 Начислить баланс - добавляет средства на баланс сопровождающего.\n"
            "📊 Статистика сквадов - показывает общую статистику сквадов.\n"
            "📝 Добавить заказ - добавляет новый заказ в локальный список.\n"
            "🔙 На главную - возврат в главное меню."
        )
        await message.answer(response, reply_markup=get_admin_keyboard())
    except Exception as e:
        logger.error(f"Ошибка в admin_help: {e}")
        await message.answer(MESSAGES["error"])

@dp.message(F.text == "🏠 Добавить сквад")
async def add_squad(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"])
        return
    try:
        await state.set_state(Form.squad_name)
        await message.answer("Введите название нового сквада:")
    except Exception as e:
        logger.error(f"Ошибка в add_squad: {e}")
        await message.answer(MESSAGES["error"])

@dp.message(StateFilter(Form.squad_name))
async def process_squad_name(message: types.Message, state: FSMContext):
    squad_name = message.text.strip()
    if not squad_name:
        await message.answer(MESSAGES["invalid_format"])
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute("INSERT INTO squads (name) VALUES (?)", (squad_name,))
            await conn.commit()
        await message.answer(f"🏠 Сквад '{squad_name}' успешно добавлен!")
        logger.info(f"Добавлен сквад: {squad_name}")
        await notify_admins(f"🏠 Новый сквад '{squad_name}' создан")
    except aiosqlite.IntegrityError:
        await message.answer(f"⚠️ Сквад '{squad_name}' уже существует!")
    except Exception as e:
        logger.error(f"Ошибка в process_squad_name: {e}")
        await message.answer(MESSAGES["error"])
    finally:
        await state.clear()

@dp.message(F.text == "📋 Список сквадов")
async def list_squads(message: types.Message):
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, name, (SELECT COUNT(*) FROM escorts WHERE squad_id = squads.id) as count "
                "FROM squads"
            )
            squads = await cursor.fetchall()

        if not squads:
            await message.answer(MESSAGES["no_squads"])
            return

        keyboard = InlineKeyboardMarkup(inline_keyboard=[])
        response = "📋 Список сквадов:\n"
        for squad_id, name, count in squads:
            response += f"{squad_id}. {name} ({count} участников)\n"
            keyboard.inline_keyboard.append([InlineKeyboardButton(text=f"{name}", callback_data=f"squad_menu_{squad_id}")])
        await message.answer(response, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Ошибка в list_squads: {e}")
        await message.answer(MESSAGES["error"])

@dp.callback_query(F.data.startswith('squad_menu_'))
async def squad_menu(callback_query: types.CallbackQuery):
    squad_id = int(callback_query.data.split('_')[2])
    try:
        squad_info = await get_squad_info(squad_id)
        if not squad_info:
            await callback_query.answer("⚠️ Сквад не найден.", show_alert=True)
            return
        name, member_count, total_orders, total_balance = squad_info
        response = (
            f"🏠 Сквад: {name}\n"
            f"👥 Участников: {member_count}\n"
            f"📊 Выполнено заказов: {total_orders}\n"
            f"💰 Общий заработок: {total_balance:.2f} руб."
        )
        await callback_query.message.answer(response, reply_markup=get_squad_menu(squad_id))
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Ошибка в squad_menu: {e}")
        await callback_query.answer(MESSAGES["error"], show_alert=True)

@dp.callback_query(F.data.startswith('squad_stats_'))
async def squad_stats(callback_query: types.CallbackQuery):
    squad_id = int(callback_query.data.split('_')[2])
    try:
        squad_info = await get_squad_info(squad_id)
        if not squad_info:
            await callback_query.answer("⚠️ Сквад не найден.", show_alert=True)
            return
        name, member_count, total_orders, total_balance = squad_info
        response = (
            f"📊 Статистика сквада '{name}':\n"
            f"👥 Участников: {member_count}\n"
            f"📋 Заказов: {total_orders}\n"
            f"💰 Заработок: {total_balance:.2f} руб."
        )
        await callback_query.message.answer(response)
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Ошибка в squad_stats: {e}")
        await callback_query.answer(MESSAGES["error"], show_alert=True)

@dp.callback_query(F.data.startswith('squad_members_'))
async def squad_members(callback_query: types.CallbackQuery):
    squad_id = int(callback_query.data.split('_')[2])
    try:
        escorts = await get_squad_escorts(squad_id)
        if not escorts:
            await callback_query.answer("⚠️ В скваде нет участников.", show_alert=True)
            return
        response = "👥 Состав сквада:\n"
        for _, username, pubg_id in escorts:
            response += f"@{username or 'Unknown'} (PUBG ID: {pubg_id or 'не указан'})\n"
        await callback_query.message.answer(response)
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Ошибка в squad_members: {e}")
        await callback_query.answer(MESSAGES["error"], show_alert=True)

@dp.message(F.text == "👤 Добавить сопровождающего")
async def add_escort_handler(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"])
        return
    try:
        await state.set_state(Form.escort_info)
        await message.answer(
            "Введите Telegram ID и название сквада через пробел:\nПример: 123456789 НазваниеСквада"
        )
    except Exception as e:
        logger.error(f"Ошибка в add_escort_handler: {e}")
        await message.answer(MESSAGES["error"])

@dp.message(StateFilter(Form.escort_info))
async def process_escort_info(message: types.Message, state: FSMContext):
    try:
        parts = message.text.split(maxsplit=1)
        if len(parts) != 2:
            await message.answer(MESSAGES["invalid_format"])
            return
        user_id = int(parts[0])
        squad_name = parts[1].strip()

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT id FROM squads WHERE name = ?", (squad_name,))
            squad = await cursor.fetchone()
            if not squad:
                await message.answer(f"⚠️ Сквад '{squad_name}' не найден!")
                return

            cursor = await conn.execute(
                "SELECT COUNT(*) FROM escorts WHERE squad_id = ?", (squad[0],)
            )
            member_count = (await cursor.fetchone())[0]
            if member_count >= 6:
                await message.answer(MESSAGES["squad_full"].format(squad_name=squad_name))
                return

            await conn.execute(
                "INSERT OR REPLACE INTO escorts (telegram_id, squad_id, username) "
                "VALUES (?, ?, (SELECT username FROM escorts WHERE telegram_id = ?))",
                (user_id, squad[0], user_id)
            )
            await conn.commit()

        await message.answer(f"👤 Пользователь {user_id} добавлен в сквад '{squad_name}'!")
        logger.info(f"Добавлен сопровождающий {user_id} в сквад {squad_name}")
        await notify_admins(f"👤 Пользователь {user_id} добавлен в сквад '{squad_name}'")
    except ValueError:
        await message.answer(MESSAGES["invalid_format"])
    except Exception as e:
        logger.error(f"Ошибка в process_escort_info: {e}")
        await message.answer(MESSAGES["error"])
    finally:
        await state.clear()

@dp.message(F.text == "👤 Мой профиль")
async def my_profile(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await add_escort(user_id, message.from_user.username or "Unknown")
            escort = await get_escort(user_id)

        escort_id, squad_id, pubg_id, balance, reputation, orders, username = escort
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT name FROM squads WHERE id = ?", (squad_id,))
            squad = await cursor.fetchone()

        response = (
            f"👤 Ваш профиль:\n"
            f"🔹 Username: @{username or 'Unknown'}\n"
            f"🔹 PUBG ID: {pubg_id or 'не указан'}\n"
            f"🏠 Сквад: {squad[0] if squad else 'не назначен'}\n"
            f"💰 Баланс: {balance:.2f} руб.\n"
            f"⭐ Репутация: {reputation}\n"
            f"📊 Выполнено заказов: {orders}\n"
            f"\nВведите PUBG ID для обновления:"
        )
        await message.answer(response)
        await state.set_state(Form.pubg_id)
    except Exception as e:
        logger.error(f"Ошибка в my_profile: {e}")
        await message.answer(MESSAGES["error"])

@dp.message(StateFilter(Form.pubg_id))
async def process_pubg_id(message: types.Message, state: FSMContext):
    pubg_id = message.text.strip()
    user_id = message.from_user.id
    if not pubg_id:
        await message.answer(MESSAGES["invalid_format"])
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "UPDATE escorts SET pubg_id = ? WHERE telegram_id = ?",
                (pubg_id, user_id)
            )
            await conn.commit()
        await message.answer(f"🔹 PUBG ID {pubg_id} сохранен!")
        logger.info(f"Пользователь {user_id} обновил PUBG ID: {pubg_id}")
    except Exception as e:
        logger.error(f"Ошибка в process_pubg_id: {e}")
        await message.answer(MESSAGES["error"])
    finally:
        await state.clear()

@dp.message(F.text == "📋 Доступные заказы")
async def available_orders(message: types.Message):
    try:
        await fetch_manual_orders()
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT fanpay_order_id, customer_info, amount FROM orders WHERE status = 'pending'"
            )
            orders = await cursor.fetchall()

        if not orders:
            await message.answer(MESSAGES["no_orders"])
            return

        keyboard = InlineKeyboardMarkup(inline_keyboard=[])
        for order_id, customer, amount in orders:
            keyboard.inline_keyboard.append([
                InlineKeyboardButton(
                    text=f"Заказ #{order_id} - {amount:.2f} руб.",
                    callback_data=f"order_{order_id}"
                )
            ])
        await message.answer("📋 Доступные заказы:", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Ошибка в available_orders: {e}")
        await message.answer(MESSAGES["error"])

@dp.callback_query(F.data.startswith('order_'))
async def process_order_selection(callback_query: types.CallbackQuery):
    order_id = callback_query.data.split('_')[1]
    user_id = callback_query.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await callback_query.answer("⚠️ Ваш профиль не найден.", show_alert=True)
            return
        if not escort[2]:
            await callback_query.answer("⚠️ Укажите PUBG ID в профиле!", show_alert=True)
            return
        if not escort[1]:
            await callback_query.answer("⚠️ Вы не состоите в скваде.", show_alert=True)
            return

        squad_id = escort[1]
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM escorts WHERE squad_id = ?", (squad_id,)
            )
            member_count = (await cursor.fetchone())[0]
            if member_count < 3:
                await callback_query.answer(MESSAGES["squad_too_small"], show_alert=True)
                return

            cursor = await conn.execute(
                "SELECT fanpay_order_id, customer_info, amount FROM orders WHERE fanpay_order_id = ?",
                (order_id,)
            )
            order = await cursor.fetchone()
            if not order:
                await callback_query.answer("⚠️ Заказ не найден.", show_alert=True)
                return

        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Принять", callback_data=f"accept_{order_id}"),
                InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")
            ]
        ])
        await callback_query.message.answer(
            f"📋 Заказ #{order[0]}\n"
            f"👤 Клиент: {order[1]}\n"
            f"💰 Сумма: {order[2]:.2f} руб.\n"
            f"📝 Детали: Сопровождение\n\n"
            f"Требуется подтверждение минимум 2 сопровождающих.",
            reply_markup=keyboard
        )
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Ошибка в process_order_selection: {e}")
        await callback_query.answer(MESSAGES["error"], show_alert=True)

@dp.callback_query(F.data.startswith('accept_'))
async def process_accept_order(callback_query: types.CallbackQuery):
    order_id = callback_query.data.split('_')[1]
    user_id = callback_query.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort or not escort[1]:
            await callback_query.answer("⚠️ Вы не состоите в скваде.", show_alert=True)
            return

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "SELECT id, squad_id, amount FROM orders WHERE fanpay_order_id = ? AND status = 'pending'",
                (order_id,)
            )
            order = await cursor.fetchone()
            if not order:
                await callback_query.answer("⚠️ Заказ не найден или уже принят.", show_alert=True)
                return

            order_db_id, squad_id, amount = order
            if escort[1] != squad_id:
                await callback_query.answer("⚠️ Вы не в этом скваде.", show_alert=True)
                return

            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_escorts WHERE order_id = ?", (order_db_id,)
            )
            accepted_count = (await cursor.fetchone())[0]

            await conn.execute(
                "INSERT INTO order_escorts (order_id, escort_id, pubg_id) VALUES (?, ?, ?)",
                (order_db_id, escort[0], escort[2])
            )

            if accepted_count + 1 >= 2:
                await conn.execute(
                    "UPDATE orders SET status = 'in_progress' WHERE id = ?", (order_db_id,)
                )
                escorts = await get_squad_escorts(squad_id)
                pubg_ids = [escort[2] for escort in escorts if escort[2]]
                await notify_squad(
                    squad_id,
                    f"📋 Заказ #{order_id} принят!\n"
                    f"💰 Сумма: {amount:.2f} руб.\n"
                    f"PUBG ID сопровождающих: {', '.join(pubg_ids)}"
                )
                await notify_admins(f"📋 Заказ #{order_id} принят сквадом {squad_id}")
            await conn.commit()

        await callback_query.message.answer(f"✅ Вы приняли заказ #{order_id}!")
        logger.info(f"Пользователь {user_id} принял заказ #{order_id}")
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Ошибка в process_accept_order: {e}")
        await callback_query.answer(MESSAGES["error"], show_alert=True)

@dp.message(F.text == "📊 Мои заказы")
async def my_orders(message: types.Message):
    user_id = message.from_user.id
    try:
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
            await message.answer(MESSAGES["no_active_orders"])
            return

        response = "📊 Ваши заказы:\n"
        for order_id, customer, amount, status in orders:
            response += (
                f"📋 Заказ #{order_id}\n"
                f"👤 Клиент: {customer}\n"
                f"💰 Сумма: {amount:.2f} руб.\n"
                f"📌 Статус: {status}\n\n"
            )
        await message.answer(response)
    except Exception as e:
        logger.error(f"Ошибка в my_orders: {e}")
        await message.answer(MESSAGES["error"])

@dp.message(F.text == "✅ Завершить заказ")
async def complete_order(message: types.Message, state: FSMContext):
    user_id = message.from_user.id
    try:
        escort = await get_escort(user_id)
        if not escort:
            await message.answer("⚠️ Ваш профиль не найден.")
            return
        escort_id = escort[0]
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT o.fanpay_order_id, o.id, o.squad_id, o.amount
                FROM orders o
                JOIN order_escorts oe ON o.id = oe.order_id
                WHERE oe.escort_id = ? AND o.status = 'in_progress'
                ''', (escort_id,)
            )
            orders = await cursor.fetchall()

        if not orders:
            await message.answer("⚠️ Нет активных заказов для завершения.")
            return

        keyboard = InlineKeyboardMarkup(inline_keyboard=[])
        for order_id, _, _, amount in orders:
            keyboard.inline_keyboard.append([
                InlineKeyboardButton(
                    text=f"Заказ #{order_id} - {amount:.2f} руб.",
                    callback_data=f"complete_{order_id}"
                )
            ])
        await message.answer("✅ Выберите заказ для завершения:", reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Ошибка в complete_order: {e}")
        await message.answer(MESSAGES["error"])

@dp.callback_query(F.data.startswith('complete_'))
async def process_complete_order(callback_query: types.CallbackQuery, state: FSMContext):
    order_id = callback_query.data.split('_')[1]
    user_id = callback_query.from_user.id
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT o.id, o.squad_id, o.amount, o.status
                FROM orders o
                JOIN order_escorts oe ON o.id = oe.order_id
                WHERE o.fanpay_order_id = ? AND oe.escort_id = (SELECT id FROM escorts WHERE telegram_id = ?)
                ''', (order_id, user_id)
            )
            order = await cursor.fetchone()
            if not order:
                await callback_query.answer("⚠️ Заказ не найден или вы не участвуете.", show_alert=True)
                return

            order_db_id, squad_id, amount, status = order
            if status == 'completed':
                await callback_query.answer(MESSAGES["order_already_completed"].format(order_id=order_id), show_alert=True)
                return

            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(text="1⭐", callback_data=f"rate_{order_id}_1"),
                    InlineKeyboardButton(text="2⭐", callback_data=f"rate_{order_id}_2"),
                    InlineKeyboardButton(text="3⭐", callback_data=f"rate_{order_id}_3"),
                    InlineKeyboardButton(text="4⭐", callback_data=f"rate_{order_id}_4"),
                    InlineKeyboardButton(text="5⭐", callback_data=f"rate_{order_id}_5")
                ]
            ])
            await callback_query.message.answer("🌟 Оцените заказ (1-5 звезд):", reply_markup=keyboard)
            await state.update_data(order_id=order_db_id, squad_id=squad_id, amount=amount)
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Ошибка в process_complete_order: {e}")
        await callback_query.answer(MESSAGES["error"], show_alert=True)

@dp.callback_query(F.data.startswith('rate_'))
async def process_order_rating(callback_query: types.CallbackQuery, state: FSMContext):
    parts = callback_query.data.split('_')
    order_id, rating = parts[1], int(parts[2])
    user_id = callback_query.from_user.id
    data = await state.get_data()
    order_db_id, squad_id, amount = data['order_id'], data['squad_id'], data['amount']

    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                '''
                UPDATE orders
                SET status = 'completed', completed_at = ?, rating = ?
                WHERE id = ?
                ''', (datetime.now(), rating, order_db_id)
            )
            await conn.execute(
                '''
                UPDATE escorts
                SET completed_orders = completed_orders + 1, reputation = reputation + ?
                WHERE id IN (SELECT escort_id FROM order_escorts WHERE order_id = ?)
                ''', (rating, order_db_id)
            )
            cursor = await conn.execute(
                "SELECT COUNT(*) FROM order_escorts WHERE order_id = ?", (order_db_id,)
            )
            escort_count = (await cursor.fetchone())[0]
            share = 0
            if escort_count > 0:
                share = (amount * 0.95) / escort_count
                await conn.execute(
                    '''
                    UPDATE escorts
                    SET balance = balance + ?
                    WHERE id IN (SELECT escort_id FROM order_escorts WHERE order_id = ?)
                    ''', (share, order_db_id)
                )
            await conn.commit()

        await notify_squad(
            squad_id,
            f"✅ Заказ #{order_id} завершен!\n"
            f"💰 Сумма: {amount:.2f} руб.\n"
            f"🌟 Оценка: {rating} звезд\n"
            f"💸 Начислено: {share:.2f} руб. каждому"
        )
        await notify_admins(f"✅ Заказ #{order_id} завершен сквадом {squad_id}, оценка: {rating} звезд")
        await callback_query.message.answer(MESSAGES["order_completed"].format(order_id=order_id))
        logger.info(f"Заказ #{order_id} завершен пользователем {user_id}, оценка: {rating}")
        await callback_query.answer()
    except Exception as e:
        logger.error(f"Ошибка в process_order_rating: {e}")
        await callback_query.answer(MESSAGES["error"], show_alert=True)
    finally:
        await state.clear()

@dp.message(F.text == "📝 Добавить заказ")
async def add_order(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"])
        return
    try:
        await state.set_state(Form.add_order)
        await message.answer(
            "Введите ID заказа, сумму, описание и имя клиента через пробел:\n"
            "Пример: 789 2000 Продажа_предмета Client1"
        )
    except Exception as e:
        logger.error(f"Ошибка в add_order: {e}")
        await message.answer(MESSAGES["error"])

@dp.message(StateFilter(Form.add_order))
async def process_add_order(message: types.Message, state: FSMContext):
    try:
        parts = message.text.split(maxsplit=3)
        if len(parts) != 4:
            await message.answer(MESSAGES["invalid_format"])
            return
        order_id, amount, description, customer = parts[0], float(parts[1]), parts[2], parts[3]
        if amount <= 0:
            await message.answer("❌ Сумма должна быть положительной!")
            return

        # Добавление заказа в локальный список
        new_order = {
            "id": order_id,
            "amount": amount,
            "description": description,
            "customer": customer,
            "category": "accompaniment"
        }
        ORDERS.append(new_order)

        # Синхронизация с базой
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                '''
                INSERT OR IGNORE INTO orders (fanpay_order_id, customer_info, amount, status)
                VALUES (?, ?, ?, 'pending')
                ''',
                (order_id, customer, amount)
            )
            await conn.commit()

        await message.answer(
            MESSAGES["order_added"].format(
                order_id=order_id, amount=amount, description=description, customer=customer
            )
        )
        logger.info(f"Добавлен заказ #{order_id} администратором {message.from_user.id}")
        await notify_admins(
            f"📝 Новый заказ #{order_id} добавлен: {amount} руб., {description}, клиент: {customer}"
        )
    except ValueError:
        await message.answer(MESSAGES["invalid_format"])
    except Exception as e:
        logger.error(f"Ошибка в process_add_order: {e}")
        await message.answer(MESSAGES["error"])
    finally:
        await state.clear()

@dp.message(F.text == "🗑️ Удалить сопровождающего")
async def remove_escort(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"])
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()

        if not escorts:
            await message.answer(MESSAGES["no_escorts"])
            return

        response = "👤 Список сопровождающих (ID - username):\n"
        for telegram_id, username in escorts:
            response += f"{telegram_id} - @{username or 'Unknown'}\n"
        response += "\nВведите ID сопровождающего для удаления:"
        await message.answer(response)
        await state.set_state(Form.escort_info)
    except Exception as e:
        logger.error(f"Ошибка в remove_escort: {e}")
        await message.answer(MESSAGES["error"])

@dp.message(StateFilter(Form.escort_info))
async def process_remove_escort(message: types.Message, state: FSMContext):
    try:
        user_id = int(message.text)
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("DELETE FROM escorts WHERE telegram_id = ?", (user_id,))
            await conn.commit()
            if cursor.rowcount > 0:
                await message.answer(f"🗑️ Сопровождающий {user_id} удален!")
                logger.info(f"Удален сопровождающий {user_id}")
                await notify_admins(f"🗑️ Сопровождающий {user_id} удален")
            else:
                await message.answer(f"⚠️ Сопровождающий с ID {user_id} не найден.")
    except ValueError:
        await message.answer("❌ Неверный формат ID. Введите числовой ID.")
    except Exception as e:
        logger.error(f"Ошибка в process_remove_escort: {e}")
        await message.answer(MESSAGES["error"])
    finally:
        await state.clear()

@dp.message(F.text == "💰 Балансы сопровождающих")
async def escort_balances(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"])
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT e.telegram_id, e.username, e.balance, s.name
                FROM escorts e
                LEFT JOIN squads s ON e.squad_id = s.id
                '''
            )
            escorts = await cursor.fetchall()

        if not escorts:
            await message.answer(MESSAGES["no_escorts"])
            return

        response = "💰 Балансы сопровождающих:\n"
        for telegram_id, username, balance, squad_name in escorts:
            response += f"ID: {telegram_id}, @{username or 'Unknown'}, Сквад: {squad_name or 'не назначен'}, Баланс: {balance:.2f} руб.\n"
        await message.answer(response)
    except Exception as e:
        logger.error(f"Ошибка в escort_balances: {e}")
        await message.answer(MESSAGES["error"])

@dp.message(F.text == "💸 Начислить баланс")
async def add_balance(message: types.Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"])
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute("SELECT telegram_id, username FROM escorts")
            escorts = await cursor.fetchall()

        if not escorts:
            await message.answer(MESSAGES["no_escorts"])
            return

        response = "👤 Список сопровождающих (ID - username):\n"
        for telegram_id, username in escorts:
            response += f"{telegram_id} - @{username or 'Unknown'}\n"
        response += "\nВведите ID сопровождающего и сумму через пробел:\nПример: 123456789 500"
        await message.answer(response)
        await state.set_state(Form.balance_amount)
    except Exception as e:
        logger.error(f"Ошибка в add_balance: {e}")
        await message.answer(MESSAGES["error"])

@dp.message(StateFilter(Form.balance_amount))
async def process_balance_amount(message: types.Message, state: FSMContext):
    try:
        parts = message.text.split(maxsplit=1)
        if len(parts) != 2:
            await message.answer(MESSAGES["invalid_format"])
            return
        user_id = int(parts[0])
        amount = float(parts[1])
        if amount <= 0:
            await message.answer("❌ Сумма должна быть положительной!")
            return

        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                "UPDATE escorts SET balance = balance + ? WHERE telegram_id = ?",
                (amount, user_id)
            )
            await conn.commit()
            if cursor.rowcount > 0:
                await message.answer(MESSAGES["balance_added"].format(user_id=user_id, amount=amount))
                logger.info(f"Начислено {amount} руб. пользователю {user_id}")
                await notify_admins(f"💸 Начислено {amount} руб. пользователю {user_id}")
                try:
                    await bot.send_message(user_id, f"💸 Вам начислено {amount} руб. на баланс!")
                except Exception as e:
                    logger.warning(f"Не удалось уведомить {user_id}: {e}")
            else:
                await message.answer(f"⚠️ Пользователь {user_id} не найден.")
    except ValueError:
        await message.answer(MESSAGES["invalid_format"])
    except Exception as e:
        logger.error(f"Ошибка в process_balance_amount: {e}")
        await message.answer(MESSAGES["error"])
    finally:
        await state.clear()

@dp.message(F.text == "📊 Статистика сквадов")
async def squad_statistics(message: types.Message):
    if not is_admin(message.from_user.id):
        await message.answer(MESSAGES["no_access"])
        return
    try:
        async with aiosqlite.connect(DB_PATH) as conn:
            cursor = await conn.execute(
                '''
                SELECT s.name, COUNT(e.id) as member_count,
                       SUM(e.completed_orders) as total_orders,
                       SUM(e.balance) as total_balance
                FROM squads s
                LEFT JOIN escorts e ON e.squad_id = s.id
                GROUP BY s.id
                '''
            )
            squads = await cursor.fetchall()

        if not squads:
            await message.answer(MESSAGES["no_squads"])
            return

        response = "📊 Статистика сквадов:\n"
        for name, member_count, total_orders, total_balance in squads:
            response += (
                f"🏠 {name}\n"
                f"👥 Участников: {member_count}\n"
                f"📋 Заказов: {total_orders}\n"
                f"💰 Заработок: {total_balance:.2f} руб.\n\n"
            )
        await message.answer(response)
    except Exception as e:
        logger.error(f"Ошибка в squad_statistics: {e}")
        await message.answer(MESSAGES["error"])

# --- Запуск периодической проверки ---
async def check_manual_orders():
    """Периодическая проверка заказов."""
    while True:
        await fetch_manual_orders()
        await asyncio.sleep(300)  # Проверка каждые 5 минут

# --- Запуск бота ---
async def main():
    try:
        await init_db()
        asyncio.create_task(check_manual_orders())
        logger.info("Бот запущен")
        await dp.start_polling(bot, skip_updates=True)
    except Exception as e:
        logger.error(f"Ошибка при запуске бота: {e}")

if __name__ == '__main__':
    asyncio.run(main())
