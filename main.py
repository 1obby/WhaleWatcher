# =============================================================================
# Mantle Network — мониторинг + агрегация + тегирование кошельков
# v8: +авторизация, +dynamic token0/token1, +semaphore, +price_cache lock,
#     +catchup cap, +flood control send_telegram
# v9: +Agni Finance (Uniswap V3 fork), +decode_agni_swap_log, +DEX label in alerts
# Зависимости: pip install web3 requests openai python-dotenv "aiogram>=3.4"
# =============================================================================

import asyncio
import functools
import json
import math
import os
import re
import sqlite3
from contextlib import contextmanager
import threading
import time
import requests
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from web3 import Web3
from openai import OpenAI

from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import (
    Message,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    WebAppInfo,
    CallbackQuery,
)
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from dotenv import load_dotenv
load_dotenv()

# =============================================================================
# RETRY-ДЕКОРАТОР
# =============================================================================

def async_retry(max_attempts: int = 3, base_delay: float = 1.0, exceptions: tuple = (Exception,)):
    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_attempts - 1:
                        print(f"[ERROR] {func.__name__} failed after {max_attempts} attempts: {e}")
                        raise
                    delay = base_delay * (2 ** attempt)
                    print(f"[RETRY] {func.__name__} attempt {attempt + 1} failed: {e}. Retrying in {delay:.1f}s...")
                    await asyncio.sleep(delay)
        return wrapper
    return decorator

# =============================================================================
# КОНФИГУРАЦИЯ — config.json
# =============================================================================

CONFIG_FILE = "config.json"

def load_config() -> dict:
    """Читает config.json; при отсутствии или ошибке возвращает пустой словарь."""
    if not os.path.exists(CONFIG_FILE):
        return {}
    try:
        with open(CONFIG_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        print(f"[WARN] Не удалось прочитать {CONFIG_FILE}: {e}")
        return {}

def save_config(data: dict) -> None:
    """Перезаписывает config.json переданными данными."""
    try:
        with open(CONFIG_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        print(f"[CFG] {CONFIG_FILE} обновлён: {data}")
    except OSError as e:
        print(f"[ERROR] Не удалось сохранить {CONFIG_FILE}: {e}")

# =============================================================================
# НАСТРОЙКИ
# =============================================================================

RPC_URL            = os.getenv("MANTLE_RPC_URL")
POLL_INTERVAL      = 15
AGGREGATION_WINDOW = 1 * 60    # 1 минута

TELEGRAM_TOKEN     = os.getenv("TELEGRAM_TOKEN")
TELEGRAM_CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")
MODELSCOPE_API_KEY = os.getenv("MODELSCOPE_API_KEY")

MANTLESCAN_URL = "https://mantlescan.xyz/tx/"
DB_FILE        = "alerts.db"   # SQLite вместо alerts.json

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/simple/price"
    "?ids=mantle&vs_currencies=usd&include_24hr_change=true"
)
PRICE_CACHE_TTL = 5 * 60   # 5 минут

_cfg = load_config()
THRESHOLD_MNT: float = float(_cfg.get("threshold_mnt", 50))
print(f"[CFG] Порог алертов: {THRESHOLD_MNT} MNT")

# FIX-B: ранняя проверка обязательных переменных окружения
_required_env = ["MANTLE_RPC_URL", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"]
_missing = [v for v in _required_env if not os.getenv(v)]
if _missing:
    raise EnvironmentError(f"Не заданы переменные окружения: {', '.join(_missing)}")

WEBAPP_URL = os.getenv("WEBAPP_URL", "")  # URL мини-апп на Railway (опционально)

# =============================================================================
# FREEMIUM — PRO-пользователи
# =============================================================================

# PRO-пользователи — список user_id через запятую в .env
# Пример: PRO_USER_IDS=123456789,987654321
PRO_USER_IDS: set[int] = {
    int(uid.strip())
    for uid in os.getenv("PRO_USER_IDS", "").split(",")
    if uid.strip().isdigit()
}

def is_pro(message) -> bool:
    """Проверяет является ли пользователь PRO-подписчиком."""
    return message.from_user.id in PRO_USER_IDS

PRO_UPSELL = (
    "\n\n🔒 <b>Полная статистика доступна в PRO-версии</b>\n"
    "Подробнее: @whalewatcherhtBot → /subscribe"
)

# =============================================================================
# DEX: Merchant Moe (Uniswap V2 fork)
# =============================================================================

MERCHANT_MOE_POOL = Web3.to_checksum_address("0x013e138EF6008ae5FDFDE29700e3f2Bc61d21E3a")
SWAP_TOPIC = "0xd78ad95fa46c994b6551d0da85fc275fe613ce37657fb8d5e3d130840159d822"

# Адрес WMNT (Wrapped MNT) — используется для определения позиции токена в пуле
MNT_TOKEN_ADDRESS = "0x78c1b0C915c4FAA5fFFa6cAbF0219DA63d7f4CB8"

# Минимальный ABI пула для чтения token0/token1
POOL_ABI = [
    {"inputs": [], "name": "token0",
     "outputs": [{"internalType": "address", "name": "", "type": "address"}],
     "stateMutability": "view", "type": "function"},
    {"inputs": [], "name": "token1",
     "outputs": [{"internalType": "address", "name": "", "type": "address"}],
     "stateMutability": "view", "type": "function"},
]

# =============================================================================
# DEX: Agni Finance (Uniswap V3 fork)
# =============================================================================

# Swap event topic V3 — отличается от V2
AGNI_SWAP_TOPIC = "0xc42079f94a6350d7e6235f29174924f928cc2ac818eb64fed8004e115fbcca67"

# WMNT и USDT на Mantle (используются для поиска пула через factory)
# FIX-C: AGNI_WMNT_ADDRESS удалён — дублировал MNT_TOKEN_ADDRESS; используем его напрямую
AGNI_USDT_ADDRESS = "0x201EBa5CC46D216Ce6DC03F6a759e8E766e956aE"

# ABI фабрики Agni Finance для вызова getPool()
AGNI_FACTORY_ABI = [
    {
        "inputs": [
            {"internalType": "address", "name": "tokenA", "type": "address"},
            {"internalType": "address", "name": "tokenB", "type": "address"},
            {"internalType": "uint24",  "name": "fee",    "type": "uint24"}
        ],
        "name": "getPool",
        "outputs": [{"internalType": "address", "name": "", "type": "address"}],
        "stateMutability": "view",
        "type": "function"
    }
]

# Глобальная переменная — заполняется при старте через get_agni_pool_address()
AGNI_POOL_ADDRESS: str = ""

# =============================================================================
# TAGGED WALLETS
# Структура: первый тег — роль/имя, второй — категория для Alpha Score
# =============================================================================

TAGGED_WALLETS: dict[str, list[str]] = {
    # Bybit (CEX + relay)
    "0x0d4Dc3b8BEcc98782309e443A6DA4b9455B5CA48": ["Bybit",          "CEX"],
    "0x88a1493366d48225FC3cefBdAe9Ebb23e323Ade3": ["Bybit",          "CEX"],
    "0x588846213a30fd36244e0Ae0eBb2374516DA836C": ["Bybit Hot Relay", "High-frequency"],
    "0xC868D0EA71243f1580F934cDc59620603Bf9f1f1": ["Bybit Hot Relay", "High-frequency"],
    "0x4a67e97E770DE93952b8596F04c13ADa0AB9A69C": ["Bybit Relay",     "High-frequency"],
    # Binance (CEX)
    "0xB38e8c17e38363aF6EbdCb3dAE12e0243582891D": ["Binance", "CEX"],
    "0x28C6c06298d514Db089934071355E5743bf21d60": ["Binance", "CEX"],
    # KuCoin (CEX)
    "0x2933782B5a8d72f2754103d1489614F29bfA4625": ["KuCoin", "CEX"],
    # Mantle официальные контракты / казна
    "0xb9d507990c009Ed1ee853A07B6a20C0925DD8A08": ["Mantle: Budget L2",   "Protocol"],
    "0x78c1b0C915c4FAA5fFFa6cAbF0219DA63d7f4CB8": ["Mantle: WMNT Token",  "Protocol"],
    "0xeD884F0460A634c69DbB7DEf54858465808AACEf": ["Mantle: Rewards Stn", "Protocol"],
    "0xcD9Dab9Fa5b55eE4569EDc402D3206123B1285F4": ["Mantle: Treasury FF", "Protocol"],
    "0x94FEC56BbEcEAcc71C9e61623ACE9f8E1B1cf473": ["Mantle: Treasury L2", "Protocol"],
    # Merchant Moe DEX
    "0x013e138EF6008ae5FDFDE29700e3f2Bc61d21E3a": ["Merchant Moe Pool", "DEX"],
    # MNT киты (Top Accounts MantleScan)
    "0xF22943D05AB93f63b0A229b12f4425E72A4c1f1C": ["MNT Whale Top-1",    "Smart Money"],
    "0x59800fc68C7039566eD7a04b0f735255093cAC1d": ["MNT Whale Top-2",    "Smart Money"],
    "0x0f0C716b007C289C0011E470cC7f14De4fe9FC80": ["Strategic Holder",   "Smart Money"],
    "0x15Bb5D31048381C84a157526cef9513531B8BE1e": ["Institutional Fund",  "Smart Money"],
    "0xA19AB9905dC9e4bCb8F982B063710A508B612434": ["Strategic Holder #2", "Smart Money"],
    "0xA713fc94dB054aA435af4D9c66c3433dCA98559F": ["Strategic Holder #3", "Smart Money"],
    "0x6117A8AF9d748780051415433a5702ee5F669D2D": ["MNT Whale #3",        "Smart Money"],
    "0xeaF4311EE279734FAcf77D167EeC277D8343603e": ["Smart Holder #1",     "Smart Money"],
    "0x4EdB32CFc71E6C404bEa8BBBdc8D9b8E03B08235": ["Smart Holder #2",     "Smart Money"],
    "0xD4D2E6eBCA6c94dD28a0935ae468012FDda5D35A": ["Smart Holder #3",     "Smart Money"],
    # Активные DeFi-трейдеры
    "0x682a1aB616f3Ff8378392FBE6C8d17826081456f": ["Active DeFi Trader", "Smart Money"],
    "0xd8169F099CE16C87A99d2A8494023574b5eEA9c5": ["High-freq Trader",   "Smart Money"],
    # Mega Whale / OTC-кластер (поведенческий анализ Jun 2026)
    "0x0000004ebA872864a71b957180Eb17DFf71BB8f1": ["Mega Whale", "OTC Distributor", "Bybit-funded", "Smart Money"],
    "0x88A8984F2B8507BBc1c699594E3a4ECdefED4784": ["Whale Cold Storage", "Accumulator", "Smart Money"],
    "0x7647b72B4c89446f7d86BB7A30fd51b6D91577Aa": ["Personal Relay", "Routing Wallet", "High-frequency"],
    "0x6906d4ac9236849A755d16b38945Cdc44Dc01d07": ["Routing Wallet", "Relay"],
    "0xE6aEc6f5b4A21722d2663e0E2bF8cBE4D16c0747": ["Large Sender",      "Potential Whale"],
    "0x193f3520FbC1948d46a4Cf37F2D1B13AD6c5ea17": ["Large Accumulator", "Potential Whale"],
    "0x4589ac7bC932B8C8E4ea001d44D40d5e4858B808": ["Unknown Sender"],
    "0x6d9982a5902227E7d6838f3E5dA421de587e94b3": ["DeFi Contract?", "Protocol"],
}

TAGGED_WALLETS_LOWER: dict[str, list[str]] = {
    k.lower(): v for k, v in TAGGED_WALLETS.items()
}

# =============================================================================
# ИСПРАВЛЕНИЕ 3 — Единый источник тегов кошельков
# CEX_ADDRESSES и CEX_ADDRESSES_LOWER (как отдельные константы) удалены.
# CEX_ADDRESSES_LOWER вычисляется динамически из TAGGED_WALLETS.
# Все проверки вида `addr in CEX_ADDRESSES_LOWER` остаются без изменений.
# =============================================================================

CEX_ADDRESSES_LOWER: set[str] = {
    addr.lower()
    for addr, tags in TAGGED_WALLETS.items()
    if any(t in ("CEX", "Binance", "Bybit", "KuCoin", "OKX", "Coinbase") for t in tags)
}

# =============================================================================
# ALPHA SCORE — веса категорий кошельков
# =============================================================================

TAG_CATEGORY_WEIGHTS: dict[str, int] = {
    "Smart Money":        15,
    "Active DeFi Trader": 12,
    "High-freq Trader":   10,
    "CEX":                 8,
    "High-frequency":      6,
    "DEX":                 5,
    "Protocol":            2,
    "Mega Whale":         15,
    "Whale Cold Storage": 12,
    "Accumulator":        10,
    "OTC Distributor":     8,
    "Bybit-funded":        4,
    "Personal Relay":      6,
    "Routing Wallet":      4,
    "Relay":               3,
    "Potential Whale":     8,
    "Large Sender":        6,
    "Large Accumulator":   8,
    "Unknown Sender":      1,
}

# ИСПРАВЛЕНИЕ 3 — asyncio.Lock для _last_alpha
# Защищает от гонки между aggregate_and_send (запись) и cmd_alpha (чтение).
_last_alpha: dict = {
    "score":       None,
    "signal":      None,
    "computed_at": None,
    "total_mnt":   None,
    "price":       None,
    "change_24h":  None,
}
_last_alpha_lock: asyncio.Lock | None = None  # FIX-LOCK: инициализируем внутри event loop

# =============================================================================
# ПОДКЛЮЧЕНИЕ К СЕТИ
# =============================================================================

w3 = Web3(Web3.HTTPProvider(RPC_URL))
if not w3.is_connected():
    raise ConnectionError(f"Не удалось подключиться к RPC: {RPC_URL}")
print(f"[INFO] Подключено к Mantle. Последний блок: {w3.eth.block_number}")

# =============================================================================
# ИСПРАВЛЕНИЕ 1 — Retry-обёртки для RPC-вызовов Mantle
# =============================================================================

@async_retry(max_attempts=4, base_delay=1.0, exceptions=(Exception,))
async def rpc_get_block_number() -> int:
    return await asyncio.to_thread(lambda: w3.eth.block_number)

@async_retry(max_attempts=4, base_delay=1.0, exceptions=(Exception,))
async def rpc_get_block(block_num: int) -> dict:
    return await asyncio.to_thread(lambda: w3.eth.get_block(block_num, full_transactions=True))

@async_retry(max_attempts=4, base_delay=1.0, exceptions=(Exception,))
async def rpc_get_logs(filter_params: dict) -> list:
    return await asyncio.to_thread(lambda: w3.eth.get_logs(filter_params))

# =============================================================================
# QWEN КЛИЕНТ
# =============================================================================

qwen_client = None
if MODELSCOPE_API_KEY:
    try:
        qwen_client = OpenAI(
            base_url="https://api-inference.modelscope.ai/v1",
            api_key=MODELSCOPE_API_KEY,
        )
        print("[QWEN] Клиент инициализирован. Модель: Qwen/Qwen3-30B-A3B-Instruct-2507")
    except Exception as e:
        print(f"[WARN] Не удалось инициализировать Qwen клиент: {e}")
else:
    print("[QWEN] ОШИБКА: MODELSCOPE_API_KEY не задан — AI-анализ отключён")

# =============================================================================
# TELEGRAM BOT (aiogram 3.x)
# =============================================================================

bot = Bot(
    token=TELEGRAM_TOKEN,
    default=DefaultBotProperties(parse_mode=ParseMode.HTML),
)
dp = Dispatcher()

# =============================================================================
# =============================================================================
# ИСПРАВЛЕНИЕ 1 (авторизация) — вспомогательные функции сразу после инициализации бота
# =============================================================================

def is_authorized(message) -> bool:
    """Проверяет что команду вызывает авторизованный пользователь.
    /set_threshold доступен только владельцу. Остальные команды — всем."""
    return True  # чтение доступно всем

def is_admin(message) -> bool:
    """Только владелец может менять настройки бота.
    ADMIN_ID задаётся в .env: ADMIN_ID=123456789"""
    admin_id = int(os.getenv("ADMIN_ID", "0"))
    return message.from_user.id == admin_id

# =============================================================================
# БУФЕРЫ И БЛОКИРОВКИ
#
# _buffer_lock  — защита pending_alerts от concurrent append/clear.
# _hashes_lock и _file_lock УДАЛЕНЫ: дедупликация и атомарность теперь
# обеспечиваются на уровне SQLite через UNIQUE constraint + INSERT OR IGNORE.
#
# _tx_semaphore — ИСПРАВЛЕНИЕ 3: ограничение параллельных process_transaction.
# =============================================================================

_buffer_lock  = asyncio.Lock()
_tx_semaphore = asyncio.Semaphore(20)  # объявлена как глобальная константа

pending_alerts: list[dict] = []

# =============================================================================
# КЭШИРОВАННАЯ ЦЕНА MNT (CoinGecko)
# ИСПРАВЛЕНИЕ 4 — double-checked locking: устраняет double-fetch
# когда два потока одновременно проходят TTL-проверку.
# =============================================================================

_price_cache: dict = {
    "price":      0.0,
    "change_24h": 0.0,
    "fetched_at": datetime.min.replace(tzinfo=timezone.utc),
}
_price_cache_lock = threading.Lock()  # threading.Lock потому что get_mnt_price — sync функция

def get_mnt_price() -> tuple[float, float]:
    """Возвращает (price_usd, change_24h_pct). Кэш живёт PRICE_CACHE_TTL сек.
    ИСПРАВЛЕНИЕ 4 — паттерн double-checked locking: fetched_at обновляется
    внутри лока ДО HTTP-запроса, чтобы второй поток не пошёл за данными
    одновременно с первым."""
    now = datetime.now(timezone.utc)

    with _price_cache_lock:
        age = (now - _price_cache["fetched_at"]).total_seconds()
        if age < PRICE_CACHE_TTL:
            return _price_cache["price"], _price_cache["change_24h"]
        # Помечаем что идём за данными — сбрасываем fetched_at в «сейчас»,
        # чтобы второй поток увидел age=0 и вернул старые данные без повторного запроса
        _price_cache["fetched_at"] = now  # временная блокировка

    # HTTP-запрос вне лока — не блокируем других на время HTTP
    try:
        resp = requests.get(COINGECKO_URL, timeout=8)
        data = resp.json()
        price  = data["mantle"]["usd"]
        change = data["mantle"]["usd_24h_change"]
    except Exception as e:
        print(f"[WARN] CoinGecko ошибка: {e}")
        with _price_cache_lock:
            # FIX-E: сбрасываем таймер чтобы следующий вызов не ждал 5 минут
            _price_cache["fetched_at"] = datetime.min.replace(tzinfo=timezone.utc)
            return _price_cache["price"], _price_cache["change_24h"]

    # Обновляем кэш с реальным временем после успешного запроса
    with _price_cache_lock:
        _price_cache.update({
            "price":      price,
            "change_24h": change,
            "fetched_at": datetime.now(timezone.utc),
        })

    print(f"[PRICE] MNT: ${price:.4f}  |  24h: {change:+.2f}%")
    return price, change

def format_price_line(price: float | None, change_24h: float | None) -> str:
    if price is None:
        return "💰 <b>MNT:</b> N/A"
    if change_24h is None:
        return f"💰 <b>MNT:</b> ${price:.4f}"
    arrow = "📈" if change_24h >= 0 else "📉"
    sign  = "+" if change_24h >= 0 else ""
    return f"💰 <b>MNT:</b> ${price:.4f}  ({arrow} {sign}{change_24h:.2f}% 24h)"

# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ТЕГИРОВАНИЯ
# =============================================================================

def get_wallet_tags(address: str) -> list[str]:
    return TAGGED_WALLETS_LOWER.get(address.lower(), [])

def format_tags(tags: list[str]) -> str:
    if not tags:
        return ""
    return " ".join(f"[{t}]" for t in tags)

# =============================================================================
# БАЗА ДАННЫХ — инициализация
# =============================================================================

def init_db() -> None:
    """
    Создаёт таблицы alerts и meta (idempotent — безопасно вызывать повторно).

    alerts — лог всех алертов; tx_hash UNIQUE обеспечивает дедупликацию
             на уровне БД без хранения множества в памяти.
    meta   — пары ключ/значение для чекпоинтов (last_block и др.).
    """
    conn = sqlite3.connect(DB_FILE)
    # ИСПРАВЛЕНИЕ 1 — WAL-режим устраняет OperationalError: database is locked
    # при параллельных записях из нескольких asyncio-задач
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    with conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS alerts (
                id        INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT    NOT NULL,
                tx_hash   TEXT    UNIQUE NOT NULL,
                value_mnt REAL    NOT NULL,
                from_addr TEXT    NOT NULL,
                to_addr   TEXT    NOT NULL,
                type      TEXT    NOT NULL,
                ai_signal TEXT    DEFAULT '',
                tags      TEXT    DEFAULT '[]',
                extra     TEXT    DEFAULT '{}'
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key   TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        # ↓ НОВАЯ ТАБЛИЦА
        conn.execute("""
            CREATE TABLE IF NOT EXISTS predictions (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at   TEXT    NOT NULL,
                signal       TEXT    NOT NULL,
                price_at     REAL,
                alpha_score  INTEGER,
                total_mnt    REAL,
                resolved_1h  INTEGER DEFAULT NULL,
                resolved_4h  INTEGER DEFAULT NULL,
                resolved_24h INTEGER DEFAULT NULL,
                price_1h     REAL    DEFAULT NULL,
                price_4h     REAL    DEFAULT NULL,
                price_24h    REAL    DEFAULT NULL
            )
        """)
    conn.close()
    print(f"[DB] База данных инициализирована: {DB_FILE}")

# =============================================================================
# ВСПОМОГАТЕЛЬНЫЙ КОНТЕКСТНЫЙ МЕНЕДЖЕР ДЛЯ SQLite
# =============================================================================

@contextmanager
def get_db():
    """Открывает соединение с БД, включает WAL и возвращает conn как контекст."""
    conn = sqlite3.connect(DB_FILE, timeout=10)  # FIX-3
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=NORMAL")
    conn.execute("PRAGMA busy_timeout=10000")   # 10 сек ожидания при SQLITE_BUSY  # FIX-3
    try:
        yield conn
    finally:
        conn.close()

def save_prediction(signal: str, price: float | None, alpha_score: int, total_mnt: float) -> None:
    """Сохраняет предсказание AI в таблицу predictions для последующей верификации."""
    with get_db() as conn:
        conn.execute(
            """INSERT INTO predictions
               (created_at, signal, price_at, alpha_score, total_mnt)
               VALUES (?, ?, ?, ?, ?)""",
            (datetime.now(timezone.utc).isoformat(), signal, price, alpha_score, total_mnt)
        )
        conn.commit()
    print(f"[PRED] Сохранено предсказание: {signal} @ ${price}")

# =============================================================================
# МЕТА-ТАБЛИЦА — чекпоинты (last_block и другие настройки)
# =============================================================================

async def save_meta(key: str, value: str) -> None:
    """Сохраняет или обновляет пару ключ/значение в таблице meta."""
    def _sync() -> None:  # FIX-A: заменяем сырой connect на get_db()
        with get_db() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
                (key, value),
            )
            conn.commit()
    await asyncio.to_thread(_sync)

async def load_meta(key: str, default: str | None = None) -> str | None:
    """Читает значение из таблицы meta; возвращает default если ключ не найден."""
    def _sync() -> str | None:  # FIX-A: заменяем сырой connect на get_db()
        with get_db() as conn:
            cur = conn.execute("SELECT value FROM meta WHERE key = ?", (key,))
            row = cur.fetchone()
            return row[0] if row else default
    return await asyncio.to_thread(_sync)

# =============================================================================
# ЧТЕНИЕ АЛЕРТОВ ИЗ SQLITE
# ИСПРАВЛЕНИЕ 2 — фильтрация, сортировка и лимит выполняются на уровне SQL,
# а не через read_all + Python-итерацию по всем записям.
# =============================================================================

async def read_alerts(
    limit: int | None = None,
    since: datetime | None = None,
    order_by_value: bool = False,
) -> list[dict]:
    """
    Читает алерты из таблицы alerts и возвращает список словарей.
    Поля tags (JSON-массив) и extra (JSON-объект) десериализуются автоматически.

    Параметры:
        limit          — максимальное количество возвращаемых записей.
        since          — если задан, возвращаются только записи timestamp >= since.
        order_by_value — если True, сортировка по value_mnt DESC; иначе по id DESC.
    """
    def _sync() -> list[dict]:  # FIX-A: заменяем сырой connect на get_db()
        with get_db() as conn:
            conn.row_factory = sqlite3.Row

            query = "SELECT * FROM alerts"
            params: list = []
            conditions: list[str] = []

            if since:
                conditions.append("timestamp >= ?")
                params.append(since.isoformat())

            if conditions:
                query += " WHERE " + " AND ".join(conditions)

            if order_by_value:
                query += " ORDER BY value_mnt DESC"
            else:
                query += " ORDER BY id DESC"

            if limit:
                query += " LIMIT ?"
                params.append(limit)

            cur = conn.execute(query, params)
            rows = cur.fetchall()

            result = []
            for row in rows:
                entry = dict(row)
                try:
                    entry["tags"] = json.loads(entry.get("tags") or "[]")
                except (json.JSONDecodeError, TypeError):
                    entry["tags"] = []
                try:
                    extra = json.loads(entry.pop("extra") or "{}")
                    if extra:
                        entry.update(extra)
                except (json.JSONDecodeError, TypeError):
                    entry.pop("extra", None)
                result.append(entry)
            return result

    return await asyncio.to_thread(_sync)

# =============================================================================
# ЗАПИСЬ АЛЕРТА В SQLITE
#
# Дедупликация реализована через UNIQUE(tx_hash) + INSERT OR IGNORE.
# Если rowcount == 0 — транзакция уже была обработана ранее; возвращаем False.
# Если rowcount == 1 — новая запись; возвращаем True.
# Множество processed_hashes в памяти больше не нужно.
# Все операции с БД выполняются в потоке через asyncio.to_thread.
# =============================================================================

async def save_alert(
    tx_hash: str,
    value_mnt: float,
    from_addr: str,
    to_addr: str,
    event_type: str,
    ai_signal: str = "",
    extra: dict = None,
    tags: list[str] = None,
) -> bool:
    """
    Сохраняет алерт в SQLite.
    Возвращает True если запись была новой, False если дубликат (уже существовал).
    """
    ts         = datetime.now(timezone.utc).isoformat()
    tags_json  = json.dumps(tags or [], ensure_ascii=False)
    extra_json = json.dumps(extra or {}, ensure_ascii=False)

    def _sync() -> bool:  # FIX-A: заменяем сырой connect на get_db()
        with get_db() as conn:
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO alerts
                    (timestamp, tx_hash, value_mnt, from_addr, to_addr,
                     type, ai_signal, tags, extra)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    ts,
                    tx_hash,
                    round(float(value_mnt), 6),
                    from_addr,
                    to_addr,
                    event_type,
                    ai_signal,
                    tags_json,
                    extra_json,
                ),
            )
            was_inserted = cur.rowcount == 1
            conn.commit()
        return was_inserted

    was_inserted = await asyncio.to_thread(_sync)
    if was_inserted:
        tag_str = f" tags={tags}" if tags else ""
        print(f"[DB] Алерт '{event_type}' сохранён | {tx_hash[:14]}...{tag_str}")
    else:
        print(f"[DB] Дубликат пропущен: {tx_hash[:14]}...")
    return was_inserted

# =============================================================================
# ОТПРАВКА В TELEGRAM
# ИСПРАВЛЕНИЕ 6 — Flood control: обработка HTTP 429 + корректный retry
# =============================================================================

@async_retry(max_attempts=5, base_delay=1.0, exceptions=(Exception,))
async def send_telegram(message: str) -> None:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    resp = await asyncio.to_thread(
        lambda: requests.post(url, json=payload, timeout=10)
    )
    if resp.status_code == 429:
        retry_after = resp.json().get("parameters", {}).get("retry_after", 5)
        print(f"[TG] Rate limit. Жду {retry_after}s...")
        await asyncio.sleep(retry_after)
        raise RuntimeError("Rate limited — retry")
    if not resp.ok:
        raise RuntimeError(f"Telegram error {resp.status_code}: {resp.text}")
    print("[TG] Уведомление отправлено.")

# =============================================================================
# AI-АНАЛИЗ (агрегированный)
# =============================================================================

def analyze_batch_sync(
    summary_lines: list[str],
    batch: list[dict] | None = None,
    window_minutes: int = 1,
    mnt_price: float = 0.0,
    price_change_24h: float = 0.0,
    alpha_score: int = 0,
    wallet_tags_summary: str = "",
) -> str:
    if qwen_client is None:
        return ""

    # Вычисляем статистику батча прямо внутри функции из переданного списка транзакций
    total_mnt_volume = 0.0
    unique_senders   = 0
    avg_transfer     = 0.0
    min_transfer     = 0.0
    max_transfer     = 0.0

    if batch:
        values           = [e["value_mnt"] for e in batch]
        total_mnt_volume = sum(values)
        unique_senders   = len({e["from_addr"] for e in batch})
        avg_transfer     = total_mnt_volume / len(values) if values else 0.0
        min_transfer     = min(values) if values else 0.0
        max_transfer     = max(values) if values else 0.0

    total_mnt_usd = total_mnt_volume * mnt_price
    tx_count      = len(batch) if batch else len(summary_lines)
    tx_summary    = "\n".join(summary_lines)

    try:
        # stream=True — как в эталонном скрипте ModelScope.
        # Без стриминга ModelScope может таймаутить или вернуть пустой ответ
        # на длинных генерациях; стриминг получает токены по мере готовности.
        response = qwen_client.chat.completions.create(
            model="Qwen/Qwen3-30B-A3B-Instruct-2507",
            messages=[
                {
                    "role": "system",
                    "content": (
                        "You are a professional on-chain analyst specializing in Mantle Network whale behavior.\n"
                        "Analyze the provided transaction batch and output a concise, actionable signal.\n\n"
                        "PATTERN RECOGNITION RULES (apply in order, first match wins):\n"
                        # FIX-PROMPT: уточнено — CEX должен быть ПОЛУЧАТЕЛЕМ (депозит на биржу).
                        # Старая формулировка не различала депозит и вывод, что давало
                        # SELL-сигнал на CEX-withdrawal, который скорее бычий (накопление).
                        "1. If >50% of transfers go TO CEX-tagged wallets (CEX is RECEIVER, not sender) → "
                        'pattern: "CEX Deposit Flow", signal: SELL\n'
                        "   IMPORTANT: If CEX-tagged wallet is the SENDER (withdrawal from exchange) → "
                        "   this rule does NOT apply. Classify by destination wallet instead.\n"
                        "2. If transfer amounts are suspiciously uniform (all within 2% of each other) → "
                        'add flag: "Chunk Splitting Detected — automated distribution"\n'
                        "3. If OTC Distributor / Mega Whale is SENDER → "
                        'pattern: "Whale Distribution", signal: SELL\n'
                        "4. If Smart Money / OTC Distributor is RECEIVER → "
                        'pattern: "Smart Money Accumulation", signal: BUY\n'
                        # FIX-PROMPT: новое правило 4b — CEX как отправитель означает вывод
                        # с биржи, что чаще говорит о накоплении, а не о продаже.
                        "4b. If CEX-tagged wallet is SENDER and destination is untagged or Smart Money → "
                        'pattern: "CEX Withdrawal — potential accumulation", signal: WATCH\n'
                        "5. If large DEX swap BUY (Merchant Moe / Agni Finance) → "
                        'pattern: "DEX Demand Spike", signal: BUY\n'
                        "6. If large DEX swap SELL → "
                        'pattern: "DEX Supply Dump", signal: SELL\n'
                        "7. If High-frequency wallets dominate with no CEX endpoint → "
                        'pattern: "Arb/Bot Activity", signal: WATCH\n'
                        "8. Default → "
                        'pattern: "Unknown Flow", signal: WATCH\n\n'
                        "OUTPUT FORMAT (strict, max 5 lines, no markdown except **bold**):\n"
                        "**Signal: SELL / BUY / WATCH** — one sentence reason (max 12 words)\n"
                        "**Pattern:** pattern name from rules above\n"
                        "**Volume:** total MNT moved + approx USD value\n"
                        "**Key actor:** most significant tagged wallet in this batch\n"
                        "**Flag:** any anomaly (chunk splitting, OTC activation, unusual size) OR omit this line\n\n"
                        'Never write "monitor", "watch closely", or "stay alert". Always commit to a direction.'
                    ),
                },
                {
                    "role": "user",
                    "content": (
                        f"Mantle Network whale batch — {window_minutes}min window | {tx_count} transactions\n\n"
                        f"MNT price: ${mnt_price:.4f} | 24h: {price_change_24h:+.2f}%\n"
                        f"Alpha Score: {alpha_score}/100\n"
                        f"Total MNT moved in batch: {total_mnt_volume:.2f} MNT (~${total_mnt_usd:.0f})\n"
                        f"Unique source wallets: {unique_senders}\n"
                        f"Avg transfer size: {avg_transfer:.2f} MNT | Min: {min_transfer:.2f} | Max: {max_transfer:.2f}\n\n"
                        f"Transactions summary:\n{tx_summary}\n\n"
                        f"Active wallet tags in this batch:\n{wallet_tags_summary}\n"
                    ),
                },
            ],
            stream=True,  # совпадает с эталонным скриптом ModelScope
        )
        # При stream=True ответ — итератор чанков; накапливаем delta.content
        # (при stream=False был бы response.choices[0].message.content — несовместимо)
        result_parts: list[str] = []
        for chunk in response:
            if chunk.choices:
                delta_content = chunk.choices[0].delta.content
                if delta_content:
                    result_parts.append(delta_content)
        raw = "".join(result_parts).strip()
        # Конвертируем markdown **bold** → HTML <b>bold</b> для Telegram
        clean = re.sub(r'\*\*(.+?)\*\*', r'<b>\1</b>', raw)
        # Убираем одиночные звёздочки если остались после конвертации
        clean = clean.replace('*', '')
        return clean
    except Exception as e:
        print(f"[WARN] Ошибка Qwen API (batch): {e}")
        return ""

async def analyze_batch(
    summary_lines: list[str],
    batch: list[dict] | None = None,
    window_minutes: int = 1,
    mnt_price: float = 0.0,
    price_change_24h: float = 0.0,
    alpha_score: int = 0,
    wallet_tags_summary: str = "",
) -> str:
    return await asyncio.to_thread(
        analyze_batch_sync,
        summary_lines,
        batch,
        window_minutes,
        mnt_price,
        price_change_24h,
        alpha_score,
        wallet_tags_summary,
    )

# =============================================================================
# ALPHA SCORE — вычисление
# =============================================================================

def _parse_signal_direction(ai_signal: str) -> str:  # FIX-PARSE
    """Извлекает направление только из строки **Signal:** — не из всего текста.

    Проблема старой версии: поиск "sell"/"buy" по всему ответу Qwen давал
    ложные срабатывания — например, слово "sell" в названии паттерна
    "DEX Supply Dump" или в описании правила приводило к неверному signal_dir.
    Теперь парсим только строку вида **Signal: SELL** (первая строка ответа).
    """
    import re
    # Ищем только в строке вида: **Signal: SELL** или **Signal: BUY**
    match = re.search(r"\*\*Signal:\s*(SELL|BUY|WATCH)", ai_signal, re.IGNORECASE)
    if match:
        return match.group(1).lower()
    # Fallback: первое вхождение в первой строке (если модель опустила **)
    first_line = ai_signal.split("\n")[0].lower()
    if "sell" in first_line:
        return "sell"
    if "buy" in first_line:
        return "buy"
    return "watch"

def _wallet_reputation_score(batch: list[dict]) -> int:
    # Исправление 3 — учитываем оба адреса (from и to)
    seen:  set[str] = set()
    total: int      = 0
    for entry in batch:
        candidates = (
            (entry["from_addr"].lower(), entry.get("from_tags", [])),
            (entry["to_addr"].lower(),   entry.get("to_tags",   [])),
        )
        for addr, tags in candidates:
            if addr in seen:
                continue
            seen.add(addr)
            max_w = max((TAG_CATEGORY_WEIGHTS.get(t, 0) for t in tags), default=0)
            total += max_w
    return min(total, 40)

def _volume_score(total_mnt: float) -> int:
    if total_mnt <= 0:
        return 0
    return min(30, int(math.log10(max(total_mnt, 1)) * 6))

def _market_alignment_score(signal_dir: str, change_24h: float | None) -> int:
    if change_24h is None:
        return 10
    if signal_dir == "sell":
        if change_24h <= -5:  return 30
        if change_24h <= -2:  return 25
        if change_24h <   0:  return 20
        if change_24h <   2:  return 10
        return 5
    if signal_dir == "buy":
        if change_24h >= 5:   return 28
        if change_24h >= 2:   return 22
        if change_24h >  0:   return 15
        if change_24h > -2:   return 10
        return 5
    return 8  # watch

def compute_alpha_score(
    batch: list[dict],
    ai_signal: str,
    price: float | None,
    change_24h: float | None,
) -> tuple[int, str]:
    signal_dir = _parse_signal_direction(ai_signal) if ai_signal else "watch"
    total_mnt  = sum(e["value_mnt"] for e in batch)
    w_rep    = _wallet_reputation_score(batch)
    w_vol    = _volume_score(total_mnt)
    w_market = _market_alignment_score(signal_dir, change_24h)
    score = min(100, w_rep + w_vol + w_market)
    print(
        f"[ALPHA] rep={w_rep} vol={w_vol} market={w_market} "
        f"score={score} signal={signal_dir} "
        f"total_mnt={total_mnt:.0f} change_24h={change_24h}"
    )
    return score, signal_dir

def format_alpha_line(score: int, signal_dir: str) -> str:
    filled = round(score / 10)
    bar    = "█" * filled + "░" * (10 - filled)
    if score >= 75:
        level_emoji = "🔴"
    elif score >= 50:
        level_emoji = "🟡"
    else:
        level_emoji = "🟢"
    signal_labels = {"buy": "📈 buy", "sell": "📉 sell", "watch": "👀 watch"}
    signal_str    = signal_labels.get(signal_dir, signal_dir)
    return (
        f"{level_emoji} <b>Alpha Score: {score}/100</b>  ({signal_str})\n"
        f"<code>[{bar}]</code>"
    )

# =============================================================================
# ДОБАВЛЕНИЕ В БУФЕР
#
# Дедупликация вынесена в save_alert(): INSERT OR IGNORE + rowcount.
# Если save_alert вернул False — транзакция уже есть в БД, выходим.
# processed_hashes и _hashes_lock полностью удалены.
# =============================================================================

async def fire_alert(
    tx_hash: str,
    value_mnt: float,
    from_addr: str,
    to_addr: str,
    event_type: str,
    extra_lines: list = None,
    extra_log: dict = None,
) -> None:
    from_tags = get_wallet_tags(from_addr)
    to_tags   = get_wallet_tags(to_addr)
    combined_tags: list[str] = (
        [f"FROM:{t}" for t in from_tags] + [f"TO:{t}" for t in to_tags]
    )

    print(f"[BUFFER][{event_type}] {value_mnt:.2f} MNT | {tx_hash} | tags={combined_tags}")

    # Дедупликация на уровне БД: False = дубликат, пропускаем
    was_new = await save_alert(
        tx_hash    = tx_hash,
        value_mnt  = float(value_mnt),
        from_addr  = from_addr,
        to_addr    = to_addr,
        event_type = event_type,
        ai_signal  = "pending",
        extra      = extra_log,
        tags       = combined_tags,
    )
    if not was_new:
        return

    tag_extra: list[str] = []
    if from_tags:
        tag_extra.append(f"🏷 <b>From-метки:</b> {', '.join(from_tags)}")
    if to_tags:
        tag_extra.append(f"🏷 <b>To-метки:</b> {', '.join(to_tags)}")

    entry = {
        "timestamp":   datetime.now(timezone.utc).isoformat(),
        "tx_hash":     tx_hash,
        "value_mnt":   float(value_mnt),
        "from_addr":   from_addr,
        "to_addr":     to_addr,
        "type":        event_type,
        "extra_lines": (extra_lines or []) + tag_extra,
        "tags":        combined_tags,
        "from_tags":   from_tags,
        "to_tags":     to_tags,
    }

    async with _buffer_lock:
        pending_alerts.append(entry)

# =============================================================================
# АГРЕГАЦИЯ И ОТПРАВКА ОТЧЁТА
# =============================================================================

async def aggregate_and_send() -> None:
    global _last_alpha

    async with _buffer_lock:
        if not pending_alerts:
            print("[AGG] Буфер пуст - ничего не отправляем.")
            return
        batch = pending_alerts.copy()
        pending_alerts.clear()

    now_str    = datetime.now(timezone.utc).strftime("%H:%M UTC")
    window_min = AGGREGATION_WINDOW // 60
    print(f"[AGG] Формируем отчёт: {len(batch)} транзакций за {window_min} мин.")

    # Исправление 4 — await asyncio.to_thread для синхронной get_mnt_price
    price, change_24h = await asyncio.to_thread(get_mnt_price)
    price_line = format_price_line(price, change_24h)

    grouped: dict[str, dict] = defaultdict(lambda: {"total_mnt": 0.0, "txs": []})
    for entry in batch:
        grouped[entry["from_addr"]]["total_mnt"] += entry["value_mnt"]
        grouped[entry["from_addr"]]["txs"].append(entry)

    ai_summary: list[str] = []
    for addr, data in grouped.items():
        from_tags = get_wallet_tags(addr)
        tag_str   = f" [{', '.join(from_tags)}]" if from_tags else ""
        types     = ", ".join({tx["type"] for tx in data["txs"]})
        ai_summary.append(
            f"Wallet {addr[:10]}...{tag_str}: "
            f"{data['total_mnt']:.2f} MNT, "
            f"{len(data['txs'])} tx(s), types: {types}"
        )

    # Формируем строку активных тегов кошельков в этом батче для AI-промпта
    seen_tagged: dict[str, list[str]] = {}
    for entry in batch:
        for addr in (entry["from_addr"], entry["to_addr"]):
            if addr not in seen_tagged:
                tags = get_wallet_tags(addr)
                if tags:
                    seen_tagged[addr] = tags
    wallet_tags_summary = "\n".join(
        f"{addr[:10]}...: {', '.join(tags)}"
        for addr, tags in seen_tagged.items()
    ) if seen_tagged else "No tagged wallets"

    # Предварительный alpha_score (до AI-ответа, signal_dir="watch" по умолчанию)
    # Передаём в AI-промпт как контекст; финальный score пересчитывается ниже
    prelim_score, _ = compute_alpha_score(
        batch=batch, ai_signal="", price=price, change_24h=change_24h,
    )

    ai_signal = await analyze_batch(
        summary_lines       = ai_summary,
        batch               = batch,
        window_minutes      = window_min,
        mnt_price           = price or 0.0,
        price_change_24h    = change_24h or 0.0,
        alpha_score         = prelim_score,
        wallet_tags_summary = wallet_tags_summary,
    )

    # Финальный alpha_score — пересчитываем с учётом реального AI-сигнала
    alpha_score, signal_dir = compute_alpha_score(
        batch=batch, ai_signal=ai_signal, price=price, change_24h=change_24h,
    )
    alpha_line = format_alpha_line(alpha_score, signal_dir)

    # ИСПРАВЛЕНИЕ 3 — запись _last_alpha под локом для защиты от гонки с cmd_alpha
    async with _last_alpha_lock:
        _last_alpha.update({
            "score":       alpha_score,
            "signal":      signal_dir,
            "computed_at": datetime.now(timezone.utc).isoformat(),
            "total_mnt":   sum(e["value_mnt"] for e in batch),
            "price":       price,
            "change_24h":  change_24h,
        })

    # Синхронизируем alpha_score с мета-таблицей для веб-дашборда
    await save_meta("alpha_score", str(alpha_score))   # FIX-META-1: save_meta — async, to_thread недопустим
    await save_meta("alpha_signal", signal_dir)

    type_icons  = {"transfer": "🔔", "swap": "🔄", "cex_inflow": "🏦📥", "cex_outflow": "🏦📤"}
    type_labels = {"transfer": "Перевод", "swap": "Своп", "cex_inflow": "На биржу", "cex_outflow": "С биржи"}

    lines: list[str] = [
        f"📊 <b>Агрегированный отчёт Mantle</b> | {now_str}",
        price_line,
        alpha_line,
        f"⏱ Окно: <b>{window_min} мин</b>  |  Транзакций: <b>{len(batch)}</b>",
        "",
    ]

    # AI-блок: ПОСЛЕ Alpha Score, ДО списка транзакций
    # Показываем только если AI вернул непустой ответ
    if ai_signal:
        lines += [
            "─────────────────",
            ai_signal,
            "─────────────────",
            "",
        ]

    for addr, data in sorted(grouped.items(), key=lambda x: x[1]["total_mnt"], reverse=True):
        short     = addr[:10] + "..."
        from_tags = get_wallet_tags(addr)
        tag_badge = f" <b>{format_tags(from_tags)}</b>" if from_tags else ""
        lines.append(f"👤 <code>{short}</code>{tag_badge} — <b>{data['total_mnt']:.2f} MNT</b>")
        for tx in data["txs"]:
            icon     = type_icons.get(tx["type"], "🔔")
            label    = type_labels.get(tx["type"], tx["type"])
            scan     = f"{MANTLESCAN_URL}{tx['tx_hash']}"
            to_short = tx["to_addr"][:10] + "..."
            to_tags  = tx.get("to_tags", [])
            to_badge = f" {format_tags(to_tags)}" if to_tags else ""
            lines.append(
                f"  {icon} {label} {tx['value_mnt']:.2f} MNT "
                f"→ <code>{to_short}</code>{to_badge} "
                f'<a href="{scan}">[tx]</a>'
            )
            for extra in tx.get("extra_lines", []):
                lines.append(f"     {extra}")
        lines.append("")

    message = "\n".join(lines)

    # FIX-H: безопасная обрезка — закрываем незакрытые HTML-теги
    if len(message) > 4000:
        cut = message[:3900]
        safe_cut = cut.rsplit("\n", 1)[0]
        # Закрываем незакрытые теги чтобы Telegram не выдал ошибку парсинга
        for tag in ("</b>", "</code>", "</i>", "</a>"):
            open_tag = tag.replace("/", "")
            if safe_cut.count(open_tag) > safe_cut.count(tag):
                safe_cut += tag
        message = safe_cut + "\n\n<i>...сообщение обрезано</i>"

    # Исправление 5 — await send_telegram
    await send_telegram(message)
    print(f"[AGG] Отчёт отправлен. Score={alpha_score} Кошельков: {len(grouped)}")
    # Сохраняем предсказание ПОСЛЕ отправки — не блокируем критический путь  # FIX-1
    if signal_dir in ("buy", "sell"):
        try:
            await asyncio.to_thread(
                save_prediction,
                signal_dir, price, alpha_score,
                sum(e["value_mnt"] for e in batch),
            )
        except Exception as e:
            print(f"[WARN] save_prediction не сохранено: {e}")

# =============================================================================
# ТАЙМЕР АГРЕГАЦИИ
# =============================================================================

async def aggregation_timer() -> None:
    print(f"[AGG] Таймер запущен. Интервал: {AGGREGATION_WINDOW // 60} мин.")
    while True:
        await asyncio.sleep(AGGREGATION_WINDOW)
        try:
            await aggregate_and_send()
        except Exception as e:
            print(f"[ERROR] aggregate_and_send: {e}")

# =============================================================================
# ВЕРИФИКАЦИЯ ПРЕДСКАЗАНИЙ — проверяем точность через 1/4/24 часа
# =============================================================================

async def resolve_predictions() -> None:
    """Проверяем предсказания через 1/4/24 часа после создания."""
    while True:
        await asyncio.sleep(1800)  # каждые 30 минут
        try:
            price_now, _ = await asyncio.to_thread(get_mnt_price)
            if not price_now or price_now <= 0:  # FIX-2
                continue
            now = datetime.now(timezone.utc)
            await asyncio.to_thread(_resolve_predictions_sync, price_now, now)
        except Exception as e:
            print(f"[PRED] Ошибка resolve: {e}")

def _resolve_predictions_sync(price_now: float, now: datetime) -> None:
    """Синхронная часть — обновляет resolved_*/price_* в БД."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT id, created_at, signal, price_at, "
            "resolved_1h, resolved_4h, resolved_24h "
            "FROM predictions WHERE resolved_24h IS NULL"  # FIX-4
        ).fetchall()
        for row in rows:
            pred_id, created_at_str, signal, price_at, *_ = row  # FIX-4: распаковка 7-колоночного row
            if price_at is None:
                continue
            created   = datetime.fromisoformat(created_at_str)
            elapsed_h = (now - created).total_seconds() / 3600
            change_pct = (price_now - price_at) / price_at * 100
            correct = (signal == "buy" and change_pct > 0) or (signal == "sell" and change_pct < 0)
            updates = {}
            if elapsed_h >= 1  and row[4] is None:  # FIX-4: resolved_1h из row, без N+1 SELECT
                updates["resolved_1h"] = 1 if correct else 0
                updates["price_1h"]    = price_now
            if elapsed_h >= 4  and row[5] is None:  # FIX-4: resolved_4h из row, без N+1 SELECT
                updates["resolved_4h"] = 1 if correct else 0
                updates["price_4h"]    = price_now
            if elapsed_h >= 24 and row[6] is None:  # FIX-4: resolved_24h из row, без N+1 SELECT
                updates["resolved_24h"] = 1 if correct else 0
                updates["price_24h"]    = price_now
            if updates:
                set_clause = ", ".join(f"{k}=?" for k in updates)
                conn.execute(
                    f"UPDATE predictions SET {set_clause} WHERE id=?",
                    (*updates.values(), pred_id)
                )
        conn.commit()

# =============================================================================
# ОБРАБОТКА НАТИВНЫХ ПЕРЕВОДОВ MNT
# =============================================================================

async def process_transaction(tx) -> None:
    value_mnt = w3.from_wei(tx["value"], "ether")
    if value_mnt < THRESHOLD_MNT:  # FIX-D: транзакции ровно на пороге теперь проходят
        return
    tx_hash   = tx["hash"].hex()
    from_addr = tx["from"]
    to_addr   = tx.get("to") or "Contract Creation"
    event_type  = "transfer"
    extra_lines = []
    to_lower    = to_addr.lower()
    from_lower  = from_addr.lower()
    if to_lower in CEX_ADDRESSES_LOWER:
        event_type = "cex_inflow"
        # ИСПРАВЛЕНИЕ 3 — имя биржи берём из TAGGED_WALLETS_LOWER (первый тег)
        cex_name = (TAGGED_WALLETS_LOWER.get(to_lower) or ["?"])[0]
        extra_lines.append(f"🏦 <b>Биржа:</b> {cex_name}")
    elif from_lower in CEX_ADDRESSES_LOWER:
        event_type = "cex_outflow"
        # ИСПРАВЛЕНИЕ 3 — имя биржи берём из TAGGED_WALLETS_LOWER (первый тег)
        cex_name = (TAGGED_WALLETS_LOWER.get(from_lower) or ["?"])[0]
        extra_lines.append(f"🏦 <b>Биржа:</b> {cex_name}")
    await fire_alert(
        tx_hash=tx_hash, value_mnt=float(value_mnt),
        from_addr=from_addr, to_addr=to_addr,
        event_type=event_type, extra_lines=extra_lines,
    )
# =============================================================================
# ИСПРАВЛЕНИЕ 3 — Semaphore-обёртка для process_transaction
# =============================================================================

async def process_transaction_limited(tx) -> None:
    async with _tx_semaphore:
        await process_transaction(tx)

# =============================================================================
# ИСПРАВЛЕНИЕ 2 — Динамическое определение token0/token1 в decode_swap_log
# Кэш позиций токена в пулах, чтобы не делать RPC-запрос на каждый лог.
# =============================================================================

_pool_token_order: dict[str, bool] = {}  # pool_address -> mnt_is_token0

async def get_mnt_is_token0(pool_address: str) -> bool:
    """Возвращает True если MNT (WMNT) является token0 в данном пуле.
    Результат кэшируется в _pool_token_order."""
    if pool_address in _pool_token_order:
        return _pool_token_order[pool_address]

    pool = w3.eth.contract(address=Web3.to_checksum_address(pool_address), abi=POOL_ABI)
    token0 = await asyncio.to_thread(pool.functions.token0().call)
    mnt_is_token0 = token0.lower() == MNT_TOKEN_ADDRESS.lower()
    _pool_token_order[pool_address] = mnt_is_token0
    return mnt_is_token0

# =============================================================================
# AGNI FINANCE — получение адреса пула MNT/USDT через factory.getPool()
# Пробуем тиры 500, 3000, 10000 — берём первый непустой.
# =============================================================================

async def get_agni_pool_address() -> str:
    """Определяет адрес пула MNT/USDT на Agni Finance через вызов factory.getPool().
    Перебирает fee-тиры 500, 3000, 10000 и возвращает первый найденный пул."""
    factory = w3.eth.contract(
        address=Web3.to_checksum_address("0x25780dc8Fc3cfBD75F33bFDAB65e969b603b2035"),
        abi=AGNI_FACTORY_ABI,
    )
    wmnt = Web3.to_checksum_address(MNT_TOKEN_ADDRESS)  # FIX-C: единая константа вместо AGNI_WMNT_ADDRESS
    usdt = Web3.to_checksum_address(AGNI_USDT_ADDRESS)
    zero = "0x0000000000000000000000000000000000000000"

    for fee in [500, 3000, 10000]:
        pool = await asyncio.to_thread(
            factory.functions.getPool(wmnt, usdt, fee).call
        )
        if pool != zero:
            print(f"[AGNI] Найден пул MNT/USDT fee={fee}: {pool}")
            return pool

    raise RuntimeError("[AGNI] MNT/USDT пул не найден ни на одном fee-тире")

# =============================================================================
# ОБРАБОТКА СОБЫТИЙ SWAP (Merchant Moe — Uniswap V2)
# ИСПРАВЛЕНИЕ 2 — decode_swap_log теперь async, определяет позицию MNT динамически.
# ИСПРАВЛЕНИЕ 1 — добавлены поля mnt_is_out и direction для корректного
#                 определения Buy/Sell независимо от позиции MNT в пуле.
# =============================================================================

async def decode_swap_log(log) -> dict | None:
    try:
        # Исправление 2 — корректный парсинг hex-данных
        if isinstance(log["data"], bytes):
            data = log["data"]
        else:
            hex_str = log["data"]
            if hex_str.startswith(("0x", "0X")):
                hex_str = hex_str[2:]
            data = bytes.fromhex(hex_str)

        if len(data) < 128:
            return None
        amount0_in  = int.from_bytes(data[0:32],   "big")
        amount1_in  = int.from_bytes(data[32:64],  "big")
        amount0_out = int.from_bytes(data[64:96],  "big")
        amount1_out = int.from_bytes(data[96:128], "big")
        sender = "0x" + log["topics"][1].hex()[-40:]
        to     = "0x" + log["topics"][2].hex()[-40:]

        # Исправление 2 — динамическое определение позиции MNT в пуле
        mnt_is_token0 = await get_mnt_is_token0(log["address"])

        if mnt_is_token0:
            mnt_raw   = amount0_in if amount0_in > 0 else amount0_out
            other_raw = amount1_in if amount1_in > 0 else amount1_out
        else:
            mnt_raw   = amount1_in if amount1_in > 0 else amount1_out
            other_raw = amount0_in if amount0_in > 0 else amount0_out

        # ИСПРАВЛЕНИЕ 1 — Buy MNT = MNT вышел из пула (amount_out > 0 для MNT)
        #                  Sell MNT = MNT вошёл в пул (amount_in > 0 для MNT)
        if mnt_is_token0:
            mnt_is_out = amount0_out > 0
        else:
            mnt_is_out = amount1_out > 0

        mnt_val = w3.from_wei(mnt_raw, "ether")
        return {
            "sender":        Web3.to_checksum_address(sender),
            "to":            Web3.to_checksum_address(to),
            "amount0_in":    amount0_in,
            "amount1_in":    amount1_in,
            "amount0_out":   amount0_out,
            "amount1_out":   amount1_out,
            "mnt_val":       float(mnt_val),
            "other_raw":     other_raw,
            "tx_hash":       log["transactionHash"].hex(),
            "mnt_is_token0": mnt_is_token0,
            "mnt_is_out":    mnt_is_out,
            "direction":     "Buy MNT" if mnt_is_out else "Sell MNT",
            "dex":           "Merchant Moe",
        }
    except Exception as e:
        print(f"[WARN] Ошибка декодирования Swap лога (V2): {e}")
        return None

# =============================================================================
# ОБРАБОТКА СОБЫТИЙ SWAP (Agni Finance — Uniswap V3)
#
# V3 Swap event data layout (5 параметров по 32 байта каждый):
#   int256  amount0      [0:32]   — знаковый: < 0 = вышло из пула, > 0 = вошло
#   int256  amount1      [32:64]
#   uint160 sqrtPriceX96 [64:96]
#   uint128 liquidity    [96:128]
#   int24   tick         [128:160]
#
# Логика направления:
#   mnt_delta < 0  →  MNT вышел из пула  →  пользователь купил MNT  (Buy)
#   mnt_delta > 0  →  MNT вошёл в пул    →  пользователь продал MNT (Sell)
# =============================================================================

async def decode_agni_swap_log(log: dict) -> dict | None:
    """Декодирует Swap-лог Agni Finance (Uniswap V3).
    Возвращает словарь, совместимый с полями decode_swap_log, или None при ошибке."""
    try:
        raw = log["data"]
        if isinstance(raw, bytes):
            data = raw
        else:
            hex_str = raw[2:] if raw.startswith(("0x", "0X")) else raw
            data = bytes.fromhex(hex_str)

        if len(data) < 64:
            return None

        # int256: знаковые 32-байтовые целые (two's complement big-endian)
        def to_int256(b: bytes) -> int:
            val = int.from_bytes(b, "big")
            if val >= 2 ** 255:
                val -= 2 ** 256
            return val

        amount0 = to_int256(data[0:32])
        amount1 = to_int256(data[32:64])

        # amount < 0 = вышло из пула (пользователь получил)
        # amount > 0 = вошло в пул (пользователь отдал)
        mnt_is_token0 = await get_mnt_is_token0(log["address"])

        if mnt_is_token0:
            mnt_delta = amount0
        else:
            mnt_delta = amount1

        mnt_val    = float(w3.from_wei(abs(mnt_delta), "ether"))
        mnt_is_out = mnt_delta < 0  # MNT вышел из пула = пользователь купил MNT

        # sender и recipient — indexed параметры V3, лежат в topics[1] и topics[2]
        sender = "0x" + log["topics"][1].hex()[-40:]
        to     = "0x" + log["topics"][2].hex()[-40:]

        return {
            "sender":        Web3.to_checksum_address(sender),
            "to":            Web3.to_checksum_address(to),
            "amount0":       amount0,
            "amount1":       amount1,
            "mnt_val":       mnt_val,
            "other_raw":     abs(amount1 if mnt_is_token0 else amount0),
            "tx_hash":       log["transactionHash"].hex(),
            "mnt_is_token0": mnt_is_token0,
            "mnt_is_out":    mnt_is_out,
            "direction":     "Buy MNT" if mnt_is_out else "Sell MNT",
            "dex":           "Agni Finance",
        }
    except Exception as e:
        print(f"[AGNI] Ошибка decode_agni_swap_log: {e}")
        return None

# =============================================================================
# ОБРАБОТКА СОБЫТИЙ SWAP — оба DEX в одном вызове get_logs
#
# Используется OR-фильтр по topics[0]: [[SWAP_TOPIC_V2, AGNI_SWAP_TOPIC_V3]]
# Маршрутизация на нужный декодер выполняется по адресу пула в log["address"].
# ИСПРАВЛЕНИЕ 1 — используем rpc_get_logs вместо прямого w3.eth.get_logs
# ИСПРАВЛЕНИЕ 2 — decode_swap_log/decode_agni_swap_log async, используем await
# =============================================================================

async def process_swap_logs(from_block: int, to_block: int) -> None:
    global AGNI_POOL_ADDRESS

    # Автоматический retry адреса пула Agni Finance если при старте RPC был недоступен
    if not AGNI_POOL_ADDRESS:
        try:
            AGNI_POOL_ADDRESS = await get_agni_pool_address()
            TAGGED_WALLETS_LOWER[AGNI_POOL_ADDRESS.lower()] = ["Agni Finance Pool", "DEX"]
            print(f"[AGNI] Пул восстановлен: {AGNI_POOL_ADDRESS}")
        except Exception as e:
            print(f"[AGNI] Retry не удался: {e}. Следующая попытка через {POLL_INTERVAL}с.")

    try:
        # Формируем список активных пулов (Agni добавляется только если адрес получен)
        dex_pools = [MERCHANT_MOE_POOL]
        if AGNI_POOL_ADDRESS:
            dex_pools.append(AGNI_POOL_ADDRESS)

        # OR-условие по topic[0]: вернёт логи любого из двух DEX за один RPC-вызов
        filter_params = {
            "fromBlock": from_block,
            "toBlock":   to_block,
            "address":   dex_pools,
            "topics":    [[SWAP_TOPIC, AGNI_SWAP_TOPIC]],
        }
        logs = await rpc_get_logs(filter_params)
        if logs:
            print(f"[DEX] {len(logs)} Swap событий в блоках {from_block}-{to_block}")

        tasks = []
        for log in logs:
            pool_addr = log["address"].lower()

            # Определяем DEX по адресу пула и вызываем соответствующий декодер
            if pool_addr == MERCHANT_MOE_POOL.lower():
                decoded = await decode_swap_log(log)
            elif AGNI_POOL_ADDRESS and pool_addr == AGNI_POOL_ADDRESS.lower():
                decoded = await decode_agni_swap_log(log)
            else:
                # Неизвестный пул — пропускаем
                continue

            if decoded is None or decoded["mnt_val"] <= THRESHOLD_MNT:
                continue

            dex_name = decoded["dex"]

            tasks.append(fire_alert(
                tx_hash     = decoded["tx_hash"],
                value_mnt   = decoded["mnt_val"],
                from_addr   = decoded["sender"],
                to_addr     = decoded["to"],
                event_type  = "swap",
                extra_lines = [
                    # ИСПРАВЛЕНИЕ 1 — используем decoded["direction"] вместо хардкоженной проверки
                    # v9 — добавляем название DEX: [SWAP · Agni Finance] Buy MNT
                    f"🔄 <b>Тип:</b> [SWAP · {dex_name}] {decoded['direction']}",
                    f"📊 <b>Другой токен:</b> {decoded['other_raw']} wei",
                ],
                extra_log = {
                    "dex":     dex_name,
                    "amount0": decoded.get("amount0", decoded.get("amount0_in")),
                    "amount1": decoded.get("amount1", decoded.get("amount1_in")),
                },
            ))

        if tasks:
            await asyncio.gather(*tasks)
    except Exception as e:
        print(f"[ERROR] process_swap_logs: {e}")

# =============================================================================
# ОСНОВНОЙ ЦИКЛ МОНИТОРИНГА
#
# last_block загружается из meta-таблицы при старте — блоки не теряются
# при перезапуске бота. После обработки каждой группы блоков чекпоинт
# сохраняется через save_meta("last_block", ...).
# ИСПРАВЛЕНИЕ 5 — Лимит catchup-блоков при старте (MAX_CATCHUP_BLOCKS = 500).
# ИСПРАВЛЕНИЕ 3 — asyncio.gather использует process_transaction_limited.
# ИСПРАВЛЕНИЕ 2 — last_dex_block персистируется в SQLite meta-таблице.
# v9 — при старте вызывается get_agni_pool_address() для получения адреса
#      пула Agni Finance и регистрации его в TAGGED_WALLETS_LOWER.
# =============================================================================

async def monitor_blocks() -> None:
    global AGNI_POOL_ADDRESS

    # v9 — получаем адрес пула Agni Finance до начала мониторинга
    try:
        AGNI_POOL_ADDRESS = await get_agni_pool_address()
        # Регистрируем пул в словаре тегов, чтобы он отображался в алертах
        TAGGED_WALLETS_LOWER[AGNI_POOL_ADDRESS.lower()] = ["Agni Finance Pool", "DEX"]
        print(f"[AGNI] Пул зарегистрирован: {AGNI_POOL_ADDRESS}")
    except Exception as e:
        print(f"[WARN] Не удалось получить адрес пула Agni Finance: {e}. Мониторинг V3 отключён.")

    # ИСПРАВЛЕНИЕ 5 — загрузка last_block с ограничением на количество catchup-блоков
    # ИСПРАВЛЕНИЕ 2 — загрузка last_dex_block из meta; стартует от last_block если нет чекпойнта
    saved_block     = await load_meta("last_block")
    saved_dex_block = await load_meta("last_dex_block")

    if saved_block:
        last_block = int(saved_block)
        current_now = await rpc_get_block_number()
        MAX_CATCHUP_BLOCKS = 500
        if current_now - last_block > MAX_CATCHUP_BLOCKS:
            print(f"[WARN] Пропущено {current_now - last_block} блоков. Начинаем с -{MAX_CATCHUP_BLOCKS}.")
            last_block = current_now - MAX_CATCHUP_BLOCKS
    else:
        # ИСПРАВЛЕНИЕ 1 — используем rpc_get_block_number вместо прямого w3.eth.block_number
        last_block = await rpc_get_block_number()

    # last_dex_block стартует от last_block если нет чекпойнта
    last_dex_block = int(saved_dex_block) if saved_dex_block else last_block

    print(f"[INFO] Мониторинг с блока #{last_block}. DEX с #{last_dex_block}. Порог: {THRESHOLD_MNT} MNT")
    while True:
        await asyncio.sleep(POLL_INTERVAL)
        try:
            # ИСПРАВЛЕНИЕ 1 — используем rpc_get_block_number вместо прямого w3.eth.block_number
            current_block = await rpc_get_block_number()
            if current_block > last_block:
                for block_num in range(last_block + 1, current_block + 1):
                    print(f"[INFO] Блок #{block_num}...")
                    # ИСПРАВЛЕНИЕ 1 — используем rpc_get_block вместо прямого w3.eth.get_block
                    block = await rpc_get_block(block_num)
                    txs   = block.get("transactions", [])
                    print(f"       {len(txs)} транзакций")
                    # ИСПРАВЛЕНИЕ 3 — Semaphore ограничивает параллелизм до 20 tx
                    await asyncio.gather(*[process_transaction_limited(tx) for tx in txs])
                last_block = current_block
                await save_meta("last_block", str(last_block))
            else:
                print(f"[INFO] Новых блоков нет. Текущий: #{current_block}")
            # Исправление 1 — DEX-сканирование без откатывания назад
            dex_from = last_dex_block + 1
            if dex_from <= current_block:
                await process_swap_logs(dex_from, current_block)
            # ИСПРАВЛЕНИЕ 2 — персистируем last_dex_block в SQLite после каждой итерации
            last_dex_block = current_block
            await save_meta("last_dex_block", str(last_dex_block))
        except Exception as e:
            print(f"[ERROR] monitor_blocks: {e}")

# =============================================================================
# TELEGRAM BOT: КОМАНДЫ
# Все обработчики защищены проверкой is_authorized (ИСПРАВЛЕНИЕ 1).
# =============================================================================

# =============================================================================
# INLINE-КЛАВИАТУРА ГЛАВНОГО МЕНЮ
# =============================================================================

def main_menu_keyboard() -> InlineKeyboardMarkup:
    """Главное меню бота с inline-кнопками."""
    rows = [
        [
            InlineKeyboardButton(text="📊 Статистика",   callback_data="cmd_stats"),
            InlineKeyboardButton(text="🐳 Топ китов",    callback_data="cmd_top_whales"),
        ],
        [
            InlineKeyboardButton(text="🎯 Alpha Score",  callback_data="cmd_alpha"),
            InlineKeyboardButton(text="🔒 Accuracy PRO", callback_data="cmd_accuracy"),
        ],
        [
            # Кнопка /pro — информация о PRO-подписке
            InlineKeyboardButton(text="⭐ PRO-версия",   callback_data="cmd_pro"),
        ],
    ]
    # Кнопка Web App — добавляется только если WEBAPP_URL задан в окружении
    if WEBAPP_URL:
        rows.append([
            InlineKeyboardButton(
                text="🌐 Открыть Dashboard",
                web_app=WebAppInfo(url=WEBAPP_URL),
            )
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


@dp.callback_query(lambda c: c.data in ("cmd_stats", "cmd_top_whales", "cmd_alpha", "cmd_accuracy", "cmd_pro"))
async def handle_menu_callback(callback: CallbackQuery) -> None:
    """Обрабатывает нажатия кнопок главного меню."""
    print(f"[BTN] Нажата кнопка: {callback.data} от user_id={callback.from_user.id}")
    await callback.answer()

    # aiogram-объекты — frozen pydantic-модели, поля напрямую не изменить.
    # Создаём лёгкую обёртку поверх callback.message: все атрибуты делегируются
    # оригинальному message через __getattr__, но from_user возвращает реального
    # пользователя (callback.from_user), а не бота.
    class _MessageProxy:
        """Прокси вокруг Message с подменённым from_user."""
        def __init__(self, msg, real_user):
            object.__setattr__(self, "_msg",  msg)
            object.__setattr__(self, "_user", real_user)

        @property
        def from_user(self):
            return object.__getattribute__(self, "_user")

        def __getattr__(self, name):
            return getattr(object.__getattribute__(self, "_msg"), name)

    proxy = _MessageProxy(callback.message, callback.from_user)

    try:
        if callback.data == "cmd_stats":
            await cmd_stats(proxy)
        elif callback.data == "cmd_top_whales":
            await cmd_top_whales(proxy)
        elif callback.data == "cmd_alpha":
            await cmd_alpha(proxy)
        elif callback.data == "cmd_accuracy":
            await cmd_accuracy(proxy)
        elif callback.data == "cmd_pro":
            await cmd_pro(proxy)
    except Exception as e:
        print(f"[BTN] Ошибка при обработке {callback.data}: {e}")
        await callback.message.answer("⚠️ Ошибка при выполнении команды.")


@dp.message(Command("start"))
async def cmd_start(message: Message) -> None:
    """Приветственное сообщение — без рекламы и без списка команд."""
    if not is_authorized(message):
        return
    text = (
        "👋 <b>WhaleWatcher — Mantle Network Monitor</b>\n\n"
        "Отслеживаю китов в реальном времени:\n"
        "• Крупные переводы MNT\n"
        "• Свопы на Merchant Moe и Agni Finance\n"
        "• Движения CEX / Smart Money кошельков\n\n"
        "/help — список команд"
    )
    # Отправляем приветствие с inline-клавиатурой главного меню
    await message.answer(text, reply_markup=main_menu_keyboard())


@dp.message(Command("help"))
async def cmd_help(message: Message) -> None:
    """Выводит список всех доступных команд с кратким описанием."""
    if not is_authorized(message):
        return
    threshold = THRESHOLD_MNT
    text = (
        "📋 <b>Команды WhaleWatcher</b>\n\n"
        "/stats — статистика за 24 часа\n"
        "/top_whales — крупнейшие переводы\n"
        "/alpha — последний Alpha Score\n"
        "/accuracy — точность AI-сигналов 🔒 PRO\n"
        f"/set_threshold N — порог алертов (сейчас: <b>{threshold} MNT</b>)\n\n"
        "/pro — подробнее о PRO-версии"
    )
    await message.answer(text, parse_mode="HTML")


@dp.message(Command("pro"))
async def cmd_pro(message: Message) -> None:
    """Информация о PRO-подписке и способ подключения."""
    if not is_authorized(message):
        return
    text = (
        "⭐ <b>WhaleWatcher PRO — $29/мес</b>\n\n"
        "Включает:\n"
        "• Полная статистика за 24ч\n"
        "• Топ-10 крупнейших кошельков\n"
        "• Верификация AI-сигналов (/accuracy)\n\n"
        "Подключение: @notuzo"
    )
    await message.answer(text, parse_mode="HTML")


async def cmd_stats(message: Message) -> None:
    """Статистика за последние 24 часа из SQLite."""
    if not is_authorized(message):
        return
    # ИСПРАВЛЕНИЕ 2 — фильтрация по времени делегирована SQL-запросу (since=)
    recent_all = await read_alerts(since=datetime.now(timezone.utc) - timedelta(hours=24))
    if not recent_all:
        await message.answer(
            "📭 За последние 24 часа алертов не найдено.\n"
            "Убедитесь, что мониторинг запущен."
        )
        return

    # Freemium: Free-пользователи видят только 3 последних алерта
    limit = 50 if is_pro(message) else 3  # Free: только 3 последних алерта
    recent = recent_all[:limit]

    by_type: dict[str, list[float]] = defaultdict(list)
    for a in recent_all:  # статистика считается по всем, даже для Free (общие цифры)
        by_type[a.get("type", "unknown")].append(float(a.get("value_mnt", 0)))
    all_values = [float(a.get("value_mnt", 0)) for a in recent_all]
    avg_mnt   = sum(all_values) / len(all_values)
    max_mnt   = max(all_values)
    total_mnt = sum(all_values)
    type_emoji = {
        "transfer": "🔔", "swap": "🔄", "cex_inflow": "🏦📥", "cex_outflow": "🏦📤",
    }
    lines: list[str] = [
        "📈 <b>Статистика за 24 часа</b>", "",
        f"Всего алертов: <b>{len(recent_all)}</b>",
        f"Суммарно: <b>{total_mnt:,.0f} MNT</b>",
        f"Средняя сумма: <b>{avg_mnt:,.2f} MNT</b>",
        f"Максимальная сумма: <b>{max_mnt:,.2f} MNT</b>",
        "", "<b>По типам:</b>",
    ]
    for event_type, values in sorted(by_type.items(), key=lambda x: -len(x[1])):
        emoji = type_emoji.get(event_type, "•")
        t_sum = sum(values)
        lines.append(
            f"  {emoji} {event_type}: <b>{len(values)}</b> алерт(ов) | {t_sum:,.0f} MNT"
        )
    price, change_24h = await asyncio.to_thread(get_mnt_price)
    lines.append("")
    lines.append(format_price_line(price, change_24h))

    # Free-пользователям показываем пометку об ограничении
    if not is_pro(message):
        text = "\n".join(lines) + PRO_UPSELL
    else:
        text = "\n".join(lines)
    await message.answer(text)


@dp.message(Command("top_whales"))
async def cmd_top_whales(message: Message) -> None:
    """Топ-10 крупнейших одиночных переводов за всё время."""
    if not is_authorized(message):
        return
    # Freemium: Free-пользователи видят топ-3 вместо топ-10
    limit = 10 if is_pro(message) else 3  # Free: топ-3 вместо топ-10
    # ИСПРАВЛЕНИЕ 2 — сортировка и лимит делегированы SQL-запросу
    top10 = await read_alerts(limit=limit, order_by_value=True)
    if not top10:
        await message.answer("📭 Алертов пока нет.")
        return
    type_emoji = {
        "transfer": "🔔", "swap": "🔄", "cex_inflow": "🏦📥", "cex_outflow": "🏦📤",
    }
    # Заголовок отражает реальный лимит для данного пользователя
    top_label = "10" if is_pro(message) else "3"
    lines: list[str] = [f"🐳 <b>Топ-{top_label} крупнейших переводов</b>", ""]
    for i, alert in enumerate(top10, start=1):
        value      = float(alert.get("value_mnt", 0))
        event_type = alert.get("type", "?")
        emoji      = type_emoji.get(event_type, "•")
        from_addr  = alert.get("from_addr", "?")
        to_addr    = alert.get("to_addr", "?")
        tx_hash    = alert.get("tx_hash", "")
        ts_raw     = alert.get("timestamp", "")
        tags       = alert.get("tags", [])
        try:
            ts     = datetime.fromisoformat(ts_raw)
            ts_str = ts.strftime("%d.%m %H:%M")
        except ValueError:
            ts_str = "?"
        tag_str = ""
        if tags:
            clean   = [t.split(":", 1)[-1] for t in tags[:2]]
            tag_str = f" <i>[{', '.join(clean)}]</i>"
        scan_link = f'<a href="{MANTLESCAN_URL}{tx_hash}">[tx]</a>' if tx_hash else ""
        lines.append(
            f"{i}. {emoji} <b>{value:,.2f} MNT</b>{tag_str}\n"
            f"   From: <code>{from_addr[:12]}...</code>\n"
            f"   To:   <code>{to_addr[:12]}...</code>\n"
            f"   {ts_str}  {scan_link}"
        )
        lines.append("")
    # Free-пользователям добавляем апсейл
    if not is_pro(message):
        lines.append(PRO_UPSELL)
    await message.answer("\n".join(lines), disable_web_page_preview=True)


@dp.message(Command("alpha"))
async def cmd_alpha(message: Message) -> None:
    """Показывает последний вычисленный Alpha Score."""
    if not is_authorized(message):
        return

    # ИСПРАВЛЕНИЕ 3 — читаем _last_alpha под локом, дальше работаем с копией
    async with _last_alpha_lock:
        data = dict(_last_alpha)

    score      = data.get("score")
    signal     = data.get("signal")
    ts_raw     = data.get("computed_at")
    total_mnt  = data.get("total_mnt")
    price      = data.get("price")
    change_24h = data.get("change_24h")

    if score is None:
        await message.answer(
            "⏳ Alpha Score ещё не вычислялся.\n"
            "Он появится после первого агрегированного отчёта."
        )
        return
    try:
        ts     = datetime.fromisoformat(ts_raw)
        ts_str = ts.strftime("%d.%m.%Y %H:%M UTC")
    except (TypeError, ValueError):
        ts_str = "?"
    signal_labels = {"buy": "📈 buy", "sell": "📉 sell", "watch": "👀 watch"}
    signal_str    = signal_labels.get(signal or "watch", signal or "?")
    if score >= 75:
        level = "🔴 Высокий"
    elif score >= 50:
        level = "🟡 Средний"
    else:
        level = "🟢 Низкий"
    filled = round(score / 10)
    bar    = "█" * filled + "░" * (10 - filled)
    lines: list[str] = [
        "⚡ <b>Последний Alpha Score</b>", "",
        f"<b>Score:</b> {score}/100  — {level}",
        f"<b>Сигнал:</b> {signal_str}",
        f"<code>[{bar}]</code>", "",
        "<b>Детали окна:</b>",
    ]
    if total_mnt is not None:
        lines.append(f"  Объём: <b>{total_mnt:,.2f} MNT</b>")
    if price is not None:
        price_str = f"${price:.4f}"
        if change_24h is not None:
            arrow     = "📈" if change_24h >= 0 else "📉"
            sign      = "+" if change_24h >= 0 else ""
            price_str += f"  ({arrow} {sign}{change_24h:.2f}%)"
        lines.append(f"  Цена MNT: <b>{price_str}</b>")
    else:
        lines.append("  Цена MNT: <b>N/A</b>")
    lines.append(f"  Вычислен: <i>{ts_str}</i>")
    lines += ["", "<i>Обновляется с каждым агрегированным отчётом</i>"]
    await message.answer("\n".join(lines))


@dp.message(Command("set_threshold"))
async def cmd_set_threshold(message: Message) -> None:
    """
    Изменяет THRESHOLD_MNT и сохраняет новое значение в config.json.
    Использование: /set_threshold 100
    Доступно только администратору (ADMIN_ID из .env).
    """
    if not is_admin(message):  # FIX-ADMIN: только владелец меняет порог
        return
    global THRESHOLD_MNT
    parts = (message.text or "").strip().split(maxsplit=1)
    if len(parts) < 2 or not parts[1].strip():
        await message.answer(
            "⚠️ Укажите число после команды.\n"
            f"Пример: <code>/set_threshold 100</code>\n\n"
            f"Текущий порог: <b>{THRESHOLD_MNT} MNT</b>"
        )
        return
    raw = parts[1].strip().replace(",", ".")
    try:
        new_threshold = float(raw)
    except ValueError:
        await message.answer(
            f"❌ Не удалось распознать число: <code>{raw}</code>\n"
            "Используйте целое или дробное (например, <code>75</code> или <code>0.5</code>)."
        )
        return
    if new_threshold <= 0:
        await message.answer("❌ Порог должен быть больше нуля.")
        return
    old_threshold = THRESHOLD_MNT
    THRESHOLD_MNT = new_threshold
    current_cfg = await asyncio.to_thread(load_config)
    current_cfg["threshold_mnt"] = new_threshold
    await asyncio.to_thread(save_config, current_cfg)
    await message.answer(
        f"✅ Порог алертов обновлён:\n"
        f"  {old_threshold} MNT  →  <b>{new_threshold} MNT</b>\n\n"
        "Новое значение применено немедленно и сохранено в <code>config.json</code>."
    )
    print(f"[CFG] Порог изменён: {old_threshold} -> {new_threshold} MNT")

@dp.message(Command("accuracy"))
async def cmd_accuracy(message: Message) -> None:
    """Показывает статистику точности AI-сигналов BUY/SELL."""
    if not is_authorized(message):
        return
    # Команда доступна только PRO-пользователям
    if not is_pro(message):
        await message.answer(
            "🔒 <b>Команда /accuracy доступна только в PRO-версии</b>\n\n"
            "Верификация AI-сигналов, история точности по 1ч/4ч/24ч — "
            "это инструмент для серьёзных трейдеров.\n\n"
            "Стоимость PRO: <b>$29/мес</b>\n"
            "Для подключения: @notuzo",
            parse_mode="HTML"
        )
        return
    stats = await asyncio.to_thread(_get_accuracy_stats)
    await message.answer(stats, parse_mode="HTML")

def _get_accuracy_stats() -> str:
    """Синхронно считывает статистику точности из таблицы predictions."""
    with get_db() as conn:
        total = conn.execute(
            "SELECT COUNT(*) FROM predictions WHERE signal != 'watch'"
        ).fetchone()[0]
        if total == 0:
            return (
                "📊 <b>Точность AI-сигналов</b>\n\n"
                "Предсказаний пока нет. Данные появятся после первых BUY/SELL сигналов."
            )

        _ALLOWED_PRED_COLS = frozenset({"resolved_1h", "resolved_4h", "resolved_24h"})  # FIX-5

        def pct(col: str) -> str:
            if col not in _ALLOWED_PRED_COLS:  # FIX-5: блокируем SQL-injection через имя колонки
                return "—"
            row = conn.execute(
                f"SELECT COUNT(*) as n, SUM({col}) as correct "
                f"FROM predictions WHERE {col} IS NOT NULL"
            ).fetchone()
            n, correct = row
            if n == 0:
                return "—"
            correct = correct or 0  # FIX-F: SUM() возвращает None если нет подходящих строк
            return f"{int(correct / n * 100)}% ({int(correct)}/{n})"

        lines = [
            "📊 <b>Точность AI-сигналов WhaleWatcher</b>",
            "",
            f"Всего предсказаний (BUY/SELL): <b>{total}</b>",
            "",
            f"✅ Через  1ч: <b>{pct('resolved_1h')}</b>",
            f"✅ Через  4ч: <b>{pct('resolved_4h')}</b>",
            f"✅ Через 24ч: <b>{pct('resolved_24h')}</b>",
            "",
            "<i>Верным считается сигнал если цена двинулась в предсказанном направлении.</i>",
        ]
        return "\n".join(lines)

# =============================================================================
# ТОЧКА ВХОДА
# =============================================================================

if __name__ == "__main__":
    async def main() -> None:
        global _last_alpha_lock
        _last_alpha_lock = asyncio.Lock()  # FIX-LOCK: создаём внутри event loop, а не на уровне модуля
        init_db()   # создаём таблицы при первом запуске (idempotent)
        print("[BOT] Запуск polling и мониторинга блоков...")
        # FIX-G: включаем resolve_predictions в gather — исключения не будут молча проглочены
        await asyncio.gather(
            resolve_predictions(),
            monitor_blocks(),
            aggregation_timer(),
            dp.start_polling(bot, allowed_updates=["message", "callback_query"]),
        )

    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n[INFO] Остановлено.")
