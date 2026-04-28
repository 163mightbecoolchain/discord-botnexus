"""
Witness v5.0 — Discord Bot for Gaming Communities
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NEW IN v5:

  🛡️ SECURITY MODULE (Premium tier 1):
    - Full logging: 20+ event types, per-server toggle (хранится в DB)
    - Invite tracker: кто кого пригласил, история в SQLite (per guild)
    - Anti-raid: авто-кик при 8+ входах за 10 секунд
    - Anti-spam: авто-таймаут при 6+ сообщениях за 5 секунд
    - Suspicious detection: аккаунт младше 7 дней — alert
    - /security status/toggle/setlog
    - /invcheck /invuser /invdel
    - /warn /warnings /clearwarn
    - /purge

  💰 BLACKMARKET v2 (Pro):
    - Предметы T6.0–T8.4 (все уровни зачаровки)
    - % профит от городов И от Бреккилена отдельно
    - Сортировка по % профиту

  🤖 FREE AI (Groq → Gemini → Claude):
    - GROQ_API_KEY  — бесплатно (console.groq.com)
    - GEMINI_API_KEY — бесплатно (aistudio.google.com)
    - Claude как платный fallback

  📦 БЕЗОПАСНОСТЬ ХРАНЕНИЯ ДАННЫХ:
    - Все таблицы имеют guild_id как первичный/составной ключ
    - Данные серверов физически изолированы по guild_id
    - Сервер A НИКОГДА не видит данные сервера B
    - Варны, инвайты, настройки — всё per-guild
    - invite_cache ключи: "guild_id:invite_code" (нет пересечений)

  💳 КУДА ИДУТ ДЕНЬГИ ОТ ПОДПИСОК:
    - Stripe → твой банковский счёт (IBAN немецкий)
    - Настройка: Stripe Dashboard → Settings → Payouts
    - Вывод: автоматически каждые 2-7 рабочих дней
    - Минимум: €1

Requirements:
  pip install discord.py aiohttp python-dotenv aiosqlite
"""

import discord
from discord.ext import commands
from discord import app_commands
import aiohttp
import aiosqlite
import os, asyncio, random, time, json, datetime
from datetime import timedelta
from dotenv import load_dotenv
from functools import wraps

load_dotenv()

TOKEN         = os.getenv("DISCORD_TOKEN")
GROQ_KEY      = os.getenv("GROQ_API_KEY")
GEMINI_KEY    = os.getenv("GEMINI_API_KEY")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")
WEATHER_KEY   = os.getenv("WEATHER_API_KEY")
HENRIK_KEY    = os.getenv("HENRIK_API_KEY")
RIOT_KEY      = os.getenv("RIOT_API_KEY")
STEAM_KEY     = os.getenv("STEAM_API_KEY")
LOSTARK_KEY   = os.getenv("LOSTARK_API_KEY")
GOOGLE_CREDS  = os.getenv("GOOGLE_CREDENTIALS")
SHEET_ID      = os.getenv("SHEET_ID")
DB_PATH       = os.getenv("DB_PATH", "witnessbot.db")
# Автосоздание папки для БД (нужно если Volume ещё не примонтирован)
_db_dir = os.path.dirname(DB_PATH)
if _db_dir:
    os.makedirs(_db_dir, exist_ok=True)
# Security module
HMAC_SECRET     = os.getenv("HMAC_SECRET", "")           # любая случайная строка, фиксированная!
ALBION_BASE   = "https://gameinfo.albiononline.com/api/gameinfo"
ALBION_DATA   = "https://west.albion-online-data.com/api/v2"

TIER_FREE, TIER_PREMIUM, TIER_SECURITY = 0, 1, 2
TIER_PRO = TIER_PREMIUM  # backward compat alias
TIER_NAMES  = {0: "Free", 1: "⭐ Premium €2.99", 2: "🛡️ Security €4.99"}
TIER_COLORS = {0: 0x6b7fa3, 1: 0x00E5FF, 2: 0xFF6B35}

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ДИЗАЙН-СИСТЕМА
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class C:
    """Цветовая палитра Witness"""
    PRIMARY   = 0x5865F2   # Discord blurple — основной цвет бота
    SUCCESS   = 0x57F287   # Зелёный — успех, прибыль
    DANGER    = 0xED4245   # Красный — ошибка, убыток
    WARNING   = 0xFEE75C   # Жёлтый — предупреждение, устаревшие данные
    INFO      = 0x00B0F4   # Голубой — информация, Albion
    GOLD      = 0xF0B232   # Золотой — Pro, награды, топ
    MUTED     = 0x36393F   # Тёмный — нейтральный
    PREMIUM   = 0x00E5FF   # Циан — Premium
    PRO       = 0xFFD700   # Золото — Pro tier
    FREE      = 0x6b7fa3   # Серый — Free tier


def bar(value: float, max_val: float = 100, width: int = 10, filled: str = "█", empty: str = "░") -> str:
    """Прогресс-бар: bar(75) → ████████░░ 75%"""
    if max_val <= 0: return empty * width
    pct = min(value / max_val, 1.0)
    filled_n = round(pct * width)
    return filled * filled_n + empty * (width - filled_n)


def build_embed(color: int, description: str = "") -> discord.Embed:
    """Создаёт пустой эмбед. Всегда используй set_author() отдельно."""
    e = discord.Embed(color=color, description=description or None,
                      timestamp=datetime.datetime.utcnow())
    e.set_footer(text=f"Witness · {datetime.datetime.utcnow().strftime('%d.%m.%Y %H:%M')} UTC")
    return e


def risk_tag(level: str) -> str:
    return {
        "CRITICAL": "`● CRITICAL`",
        "HIGH":     "`◆ HIGH`",
        "MEDIUM":   "`▲ MEDIUM`",
        "LOW":      "`✓ LOW`",
    }.get(level.upper(), f"`{level}`")


def tier_tag(tier: int) -> str:
    return {0: "`FREE`", 1: "`⭐ PREMIUM €2.99`", 2: "`🛡️ SECURITY €4.99`"}.get(tier, "`?`")


def profit_color(pct: float) -> int:
    """Цвет по % профита"""
    if pct >= 30:  return C.SUCCESS
    if pct >= 10:  return C.GOLD
    if pct >= 0:   return C.INFO
    return C.DANGER


def make_embed(title: str = "", description: str = "", color: int = C.PRIMARY,
               footer: str = "", thumbnail: str = "") -> discord.Embed:
    """Создаёт эмбед в едином стиле Witness"""
    e = discord.Embed(title=title, description=description, color=color)
    ts = datetime.datetime.utcnow().strftime("%d.%m.%Y %H:%M UTC")
    e.set_footer(text=f"Witness · {footer + ' · ' if footer else ''}{ts}")
    if thumbnail:
        e.set_thumbnail(url=thumbnail)
    return e


def tier_badge(tier: int) -> str:
    return {0: "🔓 Free", 1: "⭐ Premium €2.99", 2: "🛡️ Security €4.99"}.get(tier, "?")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  UI КОМПОНЕНТЫ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class HelpView(discord.ui.View):
    """Select Menu для /help — выбор раздела"""
    PAGES = {
        "general":  "📖 Основные",
        "albion":   "⚔️ Albion",
        "games":    "🎮 Игры",
        "security": "🛡️ Безопасность",
        "pro":      "💎 Pro",
        "ai":       "🤖 AI",
    }

    def __init__(self, current_page: str, guild_tier: int):
        super().__init__(timeout=120)
        self.current_page = current_page
        self.guild_tier = guild_tier

        select = discord.ui.Select(
            placeholder=f"Раздел: {self.PAGES.get(current_page, '?')}",
            options=[
                discord.SelectOption(
                    label=label,
                    value=key,
                    default=(key == current_page),
                    emoji=label.split()[0]
                )
                for key, label in self.PAGES.items()
            ]
        )
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        page = interaction.data["values"][0]
        embed = build_help_embed(page, self.guild_tier)
        await interaction.response.edit_message(embed=embed, view=HelpView(page, self.guild_tier))

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class PaginatedView(discord.ui.View):
    """Кнопки пагинации для больших результатов"""

    def __init__(self, pages: list[discord.Embed], current: int = 0):
        super().__init__(timeout=180)
        self.pages = pages
        self.current = current
        self._update_buttons()

    def _update_buttons(self):
        self.prev_btn.disabled = self.current == 0
        self.next_btn.disabled = self.current >= len(self.pages) - 1
        self.counter.label = f"{self.current + 1} / {len(self.pages)}"

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current -= 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.current], view=self)

    @discord.ui.button(label="1 / 1", style=discord.ButtonStyle.secondary, disabled=True)
    async def counter(self, interaction: discord.Interaction, button: discord.ui.Button):
        pass

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current += 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.current], view=self)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class BlackmarketCategoryView(discord.ui.View):
    """Select Menu для выбора категории в /blackmarket"""

    def __init__(self, tier: int, server: str, current_cat: str):
        super().__init__(timeout=60)
        self.tier = tier
        self.server = server

        options = [
            discord.SelectOption(label="Оружие (все)", value="weapon", emoji="⚔️", default=current_cat=="weapon"),
            discord.SelectOption(label="Offhand", value="offhand", emoji="🛡️", default=current_cat=="offhand"),
            discord.SelectOption(label="Броня: Латы", value="armor_plate", emoji="🪖", default=current_cat=="armor_plate"),
            discord.SelectOption(label="Броня: Кожа", value="armor_leather", emoji="🧥", default=current_cat=="armor_leather"),
            discord.SelectOption(label="Броня: Ткань", value="armor_cloth", emoji="👘", default=current_cat=="armor_cloth"),
            discord.SelectOption(label="Сумки", value="bag", emoji="🎒", default=current_cat=="bag"),
        ]
        select = discord.ui.Select(placeholder="Выбрать категорию...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        cat = interaction.data["values"][0]
        await interaction.response.send_message(
            f"⏳ Загружаю **{cat}** T{self.tier} · {ALBION_SERVER_NAMES.get(self.server,'EU')}...",
            ephemeral=True
        )
        # Запускаем полный запрос
        await run_blackmarket(interaction, cat, self.tier, self.server, "no")


class ConfirmView(discord.ui.View):
    """Кнопки подтверждения Да/Нет"""

    def __init__(self):
        super().__init__(timeout=30)
        self.confirmed = None

    @discord.ui.button(label="✅ Да", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = True
        self.stop()
        await interaction.response.defer()

    @discord.ui.button(label="❌ Нет", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = False
        self.stop()
        await interaction.response.defer()


_cooldowns: dict = {}
_spam_tracker: dict = {}
_raid_tracker: dict = {}

def cooldown(seconds: int):
    def decorator(func):
        @wraps(func)
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            key = (interaction.user.id, func.__name__)
            now = time.time()
            remaining = seconds - (now - _cooldowns.get(key, 0))
            if remaining > 0:
                return await interaction.response.send_message(
                    f"⏳ Cooldown: **{remaining:.1f}s**", ephemeral=True)
            _cooldowns[key] = now
            return await func(interaction, *args, **kwargs)
        return wrapper
    return decorator



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LOCALIZATION — поддержка RU / EN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_guild_lang: dict = {}  # {guild_id: "ru" | "en"}

def get_lang(guild_id: int) -> str:
    """Синхронная версия — из кэша (быстро, для частых вызовов)"""
    return _guild_lang.get(guild_id, "ru")

async def load_lang(guild_id: int) -> str:
    """Загружает язык из БД в кэш"""
    if guild_id in _guild_lang:
        return _guild_lang[guild_id]
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT lang FROM guild_settings WHERE guild_id=?", (guild_id,)
        ) as c:
            row = await c.fetchone()
    lang = row[0] if row and row[0] else "ru"
    _guild_lang[guild_id] = lang
    return lang

STRINGS = {
    "ru": {
        # General
        "level":           "Уровень",
        "xp":              "XP",
        "coins":           "Монеты",
        "members":         "Участники",
        "channels":        "Каналы",
        "roles":           "Роли",
        "boost":           "Буст",
        "created":         "Создан",
        "age":             "Аккаунт",
        "on_server":       "На сервере",
        "progress":        "До уровня {next}",
        "top_active":      "Топ активных",
        "source":          "Источник",
        "request":         "Запрос",
        # Albion
        "guild":           "Гильдия",
        "alliance":        "Альянс",
        "kill_fame":       "Kill Fame",
        "death_fame":      "Death Fame",
        "pve_fame":        "PvE Fame",
        "kd":              "K/D",
        "kills":           "Убийств",
        "deaths":          "Смертей",
        "activity":        "Активность",
        "fav_target":      "Любимая жертва",
        "last_kills":      "Последние убийства",
        "last_deaths":     "Последние смерти",
        "not_found":       "❌ **{name}** не найден.",
        "no_kills":        "Нет недавних убийств у **{name}**.",
        "no_deaths":       "Нет недавних смертей у **{name}**.",
        "advantage":       "Преимущество",
        # Games
        "rank":            "Ранг",
        "peak":            "Пик",
        "wr":              "Винрейт",
        "headshots":       "HS%",
        "wins":            "Побед",
        "temp":            "Температура",
        "feels":           "Ощущается",
        "humidity":        "Влажность",
        "wind":            "Ветер",
        "description":     "Описание",
        "original":        "Оригинал",
        "translation":     "Перевод",
        # Security logs
        "joined":          "Вход",
        "left":            "Выход",
        "banned":          "Бан",
        "unbanned":        "Разбан",
        "muted":           "Мьют",
        "unmuted":         "Мьют снят",
        "nick_changed":    "Смена ника",
        "roles_changed":   "Смена ролей",
        "msg_deleted":     "Удалено сообщение",
        "msg_edited":      "Редактирование",
        "invite_created":  "Инвайт создан",
        "invite_deleted":  "Инвайт удалён",
        "voice_update":    "Голос",
        "channel_created": "Канал создан",
        "channel_deleted": "Канал удалён",
        "role_created":    "Роль создана",
        "role_deleted":    "Роль удалена",
        "server_edited":   "Сервер изменён",
        "was":             "Было",
        "now":             "Стало",
        "member":          "Участник",
        "moderator":       "Модератор",
        "reason":          "Причина",
        "channel":         "Канал",
        "text":            "Текст",
        "added":           "Добавлены",
        "removed":         "Убраны",
        "until":           "До",
        "code":            "Код",
        "expires":         "Истекает",
        "uses":            "Использований",
        "invited_by":      "Пригласил",
        "invite":          "Инвайт",
        "action":          "Действие",
        "timeout_auto":    "Таймаут 30 сек",
        "antispam_title":  "Анти-спам",
        "raid_title":      "РЕЙД ЗАБЛОКИРОВАН",
        "raid_reason":     "8+ входов за 10 сек",
        "suspicious":      "Подозрительный аккаунт",
        "account_age":     "Возраст",
        "never":           "никогда",
        "unknown":         "неизвестно",
        "enter_action":    "вошёл в",
        "left_action":     "вышел из",
        "moved":           "→",
        "no_data":         "Нет данных.",
        "days":            "дней",
    },
    "en": {
        # General
        "level":           "Level",
        "xp":              "XP",
        "coins":           "Coins",
        "members":         "Members",
        "channels":        "Channels",
        "roles":           "Roles",
        "boost":           "Boost",
        "created":         "Created",
        "age":             "Account age",
        "on_server":       "On server",
        "progress":        "Progress to level {next}",
        "top_active":      "Most active",
        "source":          "Source",
        "request":         "Request",
        # Albion
        "guild":           "Guild",
        "alliance":        "Alliance",
        "kill_fame":       "Kill Fame",
        "death_fame":      "Death Fame",
        "pve_fame":        "PvE Fame",
        "kd":              "K/D",
        "kills":           "Kills",
        "deaths":          "Deaths",
        "activity":        "Activity",
        "fav_target":      "Favourite target",
        "last_kills":      "Recent kills",
        "last_deaths":     "Recent deaths",
        "not_found":       "❌ **{name}** not found.",
        "no_kills":        "No recent kills for **{name}**.",
        "no_deaths":       "No recent deaths for **{name}**.",
        "advantage":       "Advantage",
        # Games
        "rank":            "Rank",
        "peak":            "Peak",
        "wr":              "Win rate",
        "headshots":       "HS%",
        "wins":            "Wins",
        "temp":            "Temperature",
        "feels":           "Feels like",
        "humidity":        "Humidity",
        "wind":            "Wind",
        "description":     "Description",
        "original":        "Original",
        "translation":     "Translation",
        # Security logs
        "joined":          "Joined",
        "left":            "Left",
        "banned":          "Banned",
        "unbanned":        "Unbanned",
        "muted":           "Muted",
        "unmuted":         "Unmuted",
        "nick_changed":    "Nickname changed",
        "roles_changed":   "Roles changed",
        "msg_deleted":     "Message deleted",
        "msg_edited":      "Message edited",
        "invite_created":  "Invite created",
        "invite_deleted":  "Invite deleted",
        "voice_update":    "Voice",
        "channel_created": "Channel created",
        "channel_deleted": "Channel deleted",
        "role_created":    "Role created",
        "role_deleted":    "Role deleted",
        "server_edited":   "Server updated",
        "was":             "Before",
        "now":             "After",
        "member":          "Member",
        "moderator":       "Moderator",
        "reason":          "Reason",
        "channel":         "Channel",
        "text":            "Content",
        "added":           "Added",
        "removed":         "Removed",
        "until":           "Until",
        "code":            "Code",
        "expires":         "Expires",
        "uses":            "Uses",
        "invited_by":      "Invited by",
        "invite":          "Invite",
        "action":          "Action",
        "timeout_auto":    "Timeout 30s",
        "antispam_title":  "Anti-spam",
        "raid_title":      "RAID BLOCKED",
        "raid_reason":     "8+ joins in 10s",
        "suspicious":      "Suspicious account",
        "account_age":     "Age",
        "never":           "never",
        "unknown":         "unknown",
        "enter_action":    "joined",
        "left_action":     "left",
        "moved":           "→",
        "no_data":         "No data.",
        "days":            "days",
    }
}

def t(guild_id: int, key: str, **kwargs) -> str:
    """Translate key for guild language"""
    lang = get_lang(guild_id)
    text = STRINGS.get(lang, STRINGS["ru"]).get(key, STRINGS["ru"].get(key, key))
    if kwargs:
        try:
            text = text.format(**kwargs)
        except Exception:
            pass
    return text

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DATABASE — per-guild isolation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                guild_id INTEGER PRIMARY KEY, tier INTEGER DEFAULT 0, expires_at TEXT);
            CREATE TABLE IF NOT EXISTS xp (
                guild_id INTEGER, user_id INTEGER, xp INTEGER DEFAULT 0,
                PRIMARY KEY (guild_id, user_id));
            CREATE TABLE IF NOT EXISTS economy (
                guild_id INTEGER, user_id INTEGER, coins INTEGER DEFAULT 0,
                PRIMARY KEY (guild_id, user_id));
            CREATE TABLE IF NOT EXISTS security_settings (
                guild_id INTEGER PRIMARY KEY, log_channel INTEGER DEFAULT 0, settings TEXT DEFAULT '{}');
            CREATE TABLE IF NOT EXISTS warnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL, mod_id INTEGER NOT NULL, reason TEXT, created_at TEXT);
            CREATE TABLE IF NOT EXISTS invite_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
                invite_code TEXT, inviter_id INTEGER, inviter_name TEXT,
                member_id INTEGER, member_name TEXT, joined_at TEXT,
                note TEXT DEFAULT '');
            CREATE INDEX IF NOT EXISTS idx_invite_log_code
                ON invite_log(guild_id, invite_code);
            CREATE TABLE IF NOT EXISTS birthdays (
                guild_id INTEGER, user_id INTEGER, birthday TEXT,
                PRIMARY KEY (guild_id, user_id));
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL, channel_id INTEGER, status TEXT DEFAULT 'open',
                created_at TEXT);
            CREATE TABLE IF NOT EXISTS suggestions (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL, text TEXT, votes_up INTEGER DEFAULT 0,
                votes_down INTEGER DEFAULT 0, message_id INTEGER, created_at TEXT);
            CREATE TABLE IF NOT EXISTS starboard (
                guild_id INTEGER, message_id INTEGER, starboard_msg_id INTEGER,
                PRIMARY KEY (guild_id, message_id));
            CREATE TABLE IF NOT EXISTS guild_settings (
                guild_id INTEGER PRIMARY KEY,
                starboard_channel INTEGER DEFAULT 0,
                starboard_threshold INTEGER DEFAULT 3,
                suggestion_channel INTEGER DEFAULT 0,
                ticket_category INTEGER DEFAULT 0,
                birthday_channel INTEGER DEFAULT 0,
                lockdown INTEGER DEFAULT 0,
                price_watch TEXT DEFAULT '{}',
                lang TEXT DEFAULT 'ru');
            CREATE TABLE IF NOT EXISTS price_watch (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL, item_id TEXT, threshold_pct REAL DEFAULT 5.0,
                last_price INTEGER DEFAULT 0, created_at TEXT);

            -- Persistent invite cache (переживает перезапуск)
            CREATE TABLE IF NOT EXISTS invite_cache_db (
                guild_id INTEGER NOT NULL,
                invite_code TEXT NOT NULL,
                uses INTEGER DEFAULT 0,
                inviter_id INTEGER DEFAULT 0,
                inviter_name TEXT DEFAULT '',
                max_uses INTEGER DEFAULT 0,
                created_at TEXT DEFAULT '',
                PRIMARY KEY (guild_id, invite_code));

            -- invite notes stored in invite_log.note column
        """)
        await db.commit()

async def get_tier(gid):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT tier,expires_at FROM subscriptions WHERE guild_id=?", (gid,)) as c:
            row = await c.fetchone()
            if not row: return TIER_FREE
            tier, exp = row
            if exp and datetime.datetime.utcnow() > datetime.datetime.fromisoformat(exp):
                await db.execute("UPDATE subscriptions SET tier=0 WHERE guild_id=?", (gid,))
                await db.commit(); return TIER_FREE
            return tier

async def set_tier(gid, tier, days=30):
    exp = (datetime.datetime.utcnow() + timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO subscriptions (guild_id,tier,expires_at) VALUES(?,?,?) ON CONFLICT(guild_id) DO UPDATE SET tier=excluded.tier,expires_at=excluded.expires_at", (gid,tier,exp))
        await db.commit()

async def get_xp(gid, uid):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT xp FROM xp WHERE guild_id=? AND user_id=?", (gid,uid)) as c:
            r = await c.fetchone(); return r[0] if r else 0

async def add_xp(gid, uid, amt=5):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO xp (guild_id,user_id,xp) VALUES(?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET xp=xp+?", (gid,uid,amt,amt))
        await db.commit()

async def get_coins(gid, uid):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT coins FROM economy WHERE guild_id=? AND user_id=?", (gid,uid)) as c:
            r = await c.fetchone(); return r[0] if r else 0

async def add_coins(gid, uid, amt):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO economy (guild_id,user_id,coins) VALUES(?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET coins=coins+?", (gid,uid,amt,amt))
        await db.commit()

async def get_leaderboard(gid, limit=10):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id,xp FROM xp WHERE guild_id=? ORDER BY xp DESC LIMIT ?", (gid,limit)) as c:
            return await c.fetchall()

DEFAULT_SEC = {
    "joins":True,"leaves":True,"bans":True,"timeouts":True,"msg_delete":True,
    "msg_edit":True,"invites":True,"suspicious":True,"anti_raid":True,"anti_spam":True,
    "nick_change":False,"role_change":False,"avatar_change":False,"voice":False,
    "channels":False,"roles":False,"server_edit":False,"reactions":False,
    "threads":False,"slash_commands":False,
}
SEC_NAMES = {
    "joins":"Входы","leaves":"Выходы","bans":"Баны","timeouts":"Таймауты",
    "msg_delete":"Удал. сообщения","msg_edit":"Редакт. сообщения","invites":"Инвайты",
    "suspicious":"Подозрительные","anti_raid":"Анти-рейд","anti_spam":"Анти-спам",
    "nick_change":"Смена ника","role_change":"Смена ролей","avatar_change":"Смена аватарки",
    "voice":"Голосовые","channels":"Каналы","roles":"Роли","server_edit":"Настройки сервера",
    "reactions":"Реакции","threads":"Треды","slash_commands":"Слэш-команды",
}

async def get_security(gid):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT log_channel,settings FROM security_settings WHERE guild_id=?", (gid,)) as c:
            row = await c.fetchone()
            if not row: return 0, DEFAULT_SEC.copy()
            ch, raw = row
            return ch, {**DEFAULT_SEC, **json.loads(raw or "{}")}

async def save_security(gid, log_channel, settings):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO security_settings (guild_id,log_channel,settings) VALUES(?,?,?) ON CONFLICT(guild_id) DO UPDATE SET log_channel=excluded.log_channel,settings=excluded.settings", (gid,log_channel,json.dumps(settings)))
        await db.commit()

async def is_enabled(gid, key):
    _, s = await get_security(gid); return s.get(key, False)

async def get_log_channel(guild):  # alias
    return await _get_log_ch_impl(guild)

async def get_log_ch(guild):
    ch_id, _ = await get_security(guild.id)
    if ch_id: return guild.get_channel(ch_id)
    return (discord.utils.get(guild.text_channels, name="logs") or
            discord.utils.get(guild.text_channels, name="bot-logs") or
            discord.utils.get(guild.text_channels, name="mod-logs"))

async def sec_check(guild, key):
    return await get_log_channel(guild)  # security logs free for all
    if not await is_enabled(guild.id, key): return None
    return await get_log_ch(guild)

async def add_warning(gid, uid, mod_id, reason):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO warnings (guild_id,user_id,mod_id,reason,created_at) VALUES(?,?,?,?,?)",
                         (gid,uid,mod_id,reason,datetime.datetime.utcnow().isoformat()))
        await db.commit()

async def get_warnings(gid, uid):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id,mod_id,reason,created_at FROM warnings WHERE guild_id=? AND user_id=? ORDER BY created_at DESC", (gid,uid)) as c:
            return await c.fetchall()

async def remove_warning(wid, gid):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM warnings WHERE id=? AND guild_id=?", (wid,gid)); await db.commit()

async def log_invite_use(gid, code, inviter_id, inviter_name, member_id, member_name):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO invite_log (guild_id,invite_code,inviter_id,inviter_name,member_id,member_name,joined_at) VALUES(?,?,?,?,?,?,?)",
                         (gid,code,inviter_id,inviter_name,member_id,member_name,datetime.datetime.utcnow().isoformat()))
        await db.commit()

async def get_invite_history(gid, code):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT member_name,member_id,joined_at FROM invite_log WHERE guild_id=? AND invite_code=? ORDER BY joined_at DESC", (gid,code)) as c:
            return await c.fetchall()

async def get_user_invites(gid, inviter_id):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT invite_code,member_name,joined_at FROM invite_log WHERE guild_id=? AND inviter_id=? ORDER BY joined_at DESC", (gid,inviter_id)) as c:
            return await c.fetchall()

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  BOT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

intents = discord.Intents.all()
bot = commands.Bot(command_prefix=["-", "!"], intents=intents)
OWNER_IDS = set{474658252840370176}
_owner_env = os.getenv("OWNER_IDS", "")  # через запятую: 123456789,987654321
if _owner_env:
    for _oid in _owner_env.split(","):
        try: OWNER_IDS.add(int(_oid.strip()))
        except ValueError: pass

def upsell_embed(req):
    e = build_embed(C.DANGER)
    e.set_author(name="Upgrade required",
        description=(
            f"Эта функция требует **{req}**\n\n"
            f"⭐ **Premium** — €4.99/мес\n"
            f"💎 **Pro** — €9.99/мес\n\n"
            f"witnessbot.gg/premium"
        ),
        color=C.DANGER
    )
    return e

# ── AI: Groq (free) → Gemini (free) → Claude (paid) ──────────
async def ask_ai(prompt, system="You are Witness, a helpful Discord assistant. Be concise."):
    if GROQ_KEY:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post("https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization":f"Bearer {GROQ_KEY}","Content-Type":"application/json"},
                    json={"model":"llama-3.3-70b-versatile","messages":[{"role":"system","content":system},{"role":"user","content":prompt}],"max_tokens":600},
                    timeout=aiohttp.ClientTimeout(total=20)) as r:
                    return (await r.json())["choices"][0]["message"]["content"]
        except Exception: pass
    if GEMINI_KEY:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}",
                    json={"contents":[{"parts":[{"text":f"{system}\n\n{prompt}"}]}]},
                    timeout=aiohttp.ClientTimeout(total=20)) as r:
                    return (await r.json())["candidates"][0]["content"]["parts"][0]["text"]
        except Exception: pass
    if ANTHROPIC_KEY:
        async with aiohttp.ClientSession() as s:
            async with s.post("https://api.anthropic.com/v1/messages",
                headers={"x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01","content-type":"application/json"},
                json={"model":"claude-haiku-4-5-20251001","max_tokens":600,"system":system,"messages":[{"role":"user","content":prompt}]},
                timeout=aiohttp.ClientTimeout(total=30)) as r:
                return (await r.json())["content"][0]["text"]
    return "❌ Добавь GROQ_API_KEY или GEMINI_API_KEY в .env (оба бесплатны)"

async def albion_find_player(session, name):
    async with session.get(f"{ALBION_BASE}/search?q={name}", timeout=aiohttp.ClientTimeout(total=10)) as r:
        if r.status != 200: return None, None
        p = (await r.json()).get("players", [])
        return (p[0]["Id"], p[0]["Name"]) if p else (None, None)

def fmt_item(item_id):
    if not item_id: return "—"
    parts = item_id.replace("@"," ✦").split("_")
    return " ".join(p for p in parts if not (p.startswith("T") and p[1:].isdigit())).title() or item_id

# ── Invite cache — инициализируем сразу на уровне бота ───────
# Ключ: "guild_id:invite_code" → uses (int)
# Это гарантирует что cache существует до on_ready
_invite_cache: dict = {}

async def refresh_invite_cache(guild) -> bool:
    """Загружает инвайты гильдии в кэш + сохраняет в БД для персистентности."""
    try:
        invites = await guild.invites()
        async with aiosqlite.connect(DB_PATH) as db:
            for inv in invites:
                key = f"{guild.id}:{inv.code}"
                _invite_cache[key] = inv.uses or 0
                # Сохраняем в БД
                await db.execute("""
                    INSERT INTO invite_cache_db
                        (guild_id, invite_code, uses, inviter_id, inviter_name, max_uses)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(guild_id, invite_code) DO UPDATE SET
                        uses=excluded.uses, inviter_id=excluded.inviter_id,
                        inviter_name=excluded.inviter_name
                """, (
                    guild.id, inv.code, inv.uses or 0,
                    inv.inviter.id if inv.inviter else 0,
                    inv.inviter.name if inv.inviter else "",
                    inv.max_uses or 0
                ))
            await db.commit()
        print(f"✅ Invite cache loaded for {guild.name}: {len(invites)} invites")
        return True
    except discord.Forbidden:
        # Нет прав — грузим из БД если есть
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT invite_code, uses FROM invite_cache_db WHERE guild_id=?", (guild.id,)
            ) as c:
                rows = await c.fetchall()
        for code, uses in rows:
            _invite_cache[f"{guild.id}:{code}"] = uses
        print(f"⚠️ [{guild.name}] No MANAGE_GUILD — loaded {len(rows)} invites from DB cache")
        return len(rows) > 0
    except Exception as ex:
        print(f"⚠️ [{guild.name}] Invite cache error: {ex}")
        return False

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  EVENTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.event
async def on_ready():
    await db_init()
    print(f"🗄️  DB path: {DB_PATH}")
    print(f"🔖 Witness build: 729b8616 | make_embed={True} | i18n={True}")

    # ── Загрузка Advanced Security Module ────────────────────
    try:
        from security_module import setup as security_setup
        await security_setup(bot)
    except ImportError:
        print("⚠️ security_module.py не найден — Advanced Security отключён")
    except Exception as ex:
        print(f"⚠️ Ошибка загрузки security модуля: {ex}")

    # Заполняем кэш инвайтов
    _invite_cache.clear()
    for guild in bot.guilds:
        await refresh_invite_cache(guild)

    # ── Синхронизация команд ──────────────────────────────────
    # НЕ очищаем tree — это удаляет все зарегистрированные команды!
    # Просто синхронизируем текущее состояние с Discord
    try:
        synced = await bot.tree.sync()
        print(f"✅ Синхронизировано {len(synced)} команд глобально")
        for cmd in sorted(synced, key=lambda c: c.name):
            print(f"   /{cmd.name}")
    except Exception as ex:
        print(f"❌ Ошибка синхронизации: {ex}")

    # ── Запуск фоновых задач ─────────────────────────────────
    if not hasattr(bot, "_tasks_started"):
        bot._tasks_started = True
        bot.loop.create_task(birthday_check_loop())
        bot.loop.create_task(price_watch_loop())
        print("✅ Фоновые задачи запущены")

    # ── Загружаем языки серверов из БД ───────────────────────
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT guild_id, lang FROM guild_settings WHERE lang IS NOT NULL") as c:
                lang_rows = await c.fetchall()
        for gid, lang in lang_rows:
            if lang in ("ru", "en"):
                _guild_lang[gid] = lang
        print(f"✅ Языки загружены: {len(_guild_lang)} серверов")
    except Exception as ex:
        print(f"⚠️ Ошибка загрузки языков: {ex}")

    print(f"✅ Witness v5 | {bot.user} | {len(bot.guilds)} серверов | {len(_invite_cache)} инвайтов в кэше")
    await bot.change_presence(activity=discord.Activity(
        type=discord.ActivityType.watching, name="/help | witnessbot.gg"))


@bot.event
async def on_app_command_error(interaction, error):
    try: await interaction.response.send_message(f"❌ {error}", ephemeral=True)
    except Exception:
        try: await interaction.followup.send(f"❌ {error}", ephemeral=True)
        except Exception: pass

@bot.event
async def on_message(message):
    if message.author.bot or not message.guild: return
    gid, uid = message.guild.id, message.author.id
    await add_xp(gid, uid, 5); await add_coins(gid, uid, 1)
    xp = await get_xp(gid, uid)
    if xp > 0 and xp % 100 < 5:
        await message.channel.send(f"⚡ {message.author.mention} → **Уровень {xp//100}**! 🎉", delete_after=10)
    if await is_enabled(gid, "anti_spam"):
        key = (gid, uid); now = time.time()
        _spam_tracker.setdefault(key, [])
        _spam_tracker[key] = [t for t in _spam_tracker[key] if now-t<5]
        _spam_tracker[key].append(now)
        if len(_spam_tracker[key]) >= 6:
            try:
                await message.delete()
                await message.author.timeout(timedelta(seconds=30), reason="Anti-spam")
                ch = await get_log_ch(message.guild)
                if ch:
                    e = build_embed(C.DANGER)
                    e.set_author(name=t(gid, "antispam_title"))
                    e.add_field(name=t(gid, "member"), value=message.author.mention)
                    e.add_field(name=t(gid, "action"), value=t(gid, "timeout_auto"))
                    await ch.send(embed=e)
            except Exception: pass
    await bot.process_commands(message)

@bot.event
async def on_invite_create(invite: discord.Invite):
    """Обновляем кэш при создании нового инвайта — без sleep, мгновенно."""
    if not invite.guild: return
    gid = invite.guild.id
    key = f"{gid}:{invite.code}"
    _invite_cache[key] = invite.uses or 0
    # Сохраняем в БД
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO invite_cache_db
                (guild_id, invite_code, uses, inviter_id, inviter_name, max_uses, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id, invite_code) DO UPDATE SET
                uses=excluded.uses, max_uses=excluded.max_uses
        """, (
            gid, invite.code, 0,
            invite.inviter.id if invite.inviter else 0,
            invite.inviter.name if invite.inviter else "",
            invite.max_uses or 0,
            datetime.datetime.utcnow().isoformat()
        ))
        await db.commit()
    print(f"[INVITE] Created: {invite.code} by {invite.inviter}")


@bot.event
async def on_invite_delete(invite: discord.Invite):
    """Убираем из кэша при удалении инвайта."""
    if not invite.guild: return
    gid = invite.guild.id
    key = f"{gid}:{invite.code}"
    _invite_cache.pop(key, None)
    # Удалять из БД не нужно — нужен для истории /invcheck
    print(f"[INVITE] Deleted: {invite.code}")


@bot.event
async def on_member_join(member):
    gid = member.guild.id
    if await is_enabled(gid, "anti_raid"):
        now = time.time()
        _raid_tracker.setdefault(gid, [])
        _raid_tracker[gid] = [t for t in _raid_tracker[gid] if now-t<10]
        _raid_tracker[gid].append(now)
        if len(_raid_tracker[gid]) >= 8:
            try:
                await member.kick(reason="Anti-raid")
                ch = await get_log_ch(member.guild)
                if ch:
                    e = build_embed(C.DANGER)
                    e.set_author(name=t(gid, "raid_title"))
                    e.add_field(name=t(gid, "member"), value=member.mention, inline=False)
                    e.add_field(name=t(gid, "reason"), value=t(gid, "raid_reason"), inline=False)
                    await ch.send(embed=e)
                return
            except Exception: pass

    # ── Invite tracking (переработано — без sleep, event-driven) ─
    # Снимок кэша СРАЗУ при входе (on_invite_create уже обновил кэш)
    old_snapshot = {k: v for k, v in _invite_cache.items() if k.startswith(f"{gid}:")}

    used_code   = None
    inviter_name = t(gid, "unknown")
    inviter_id   = 0

    # Retry логика: 3 попытки с нарастающей задержкой
    for _attempt in range(3):
        await asyncio.sleep(3 + _attempt * 2)  # 3с / 5с / 7с
        try:
            fresh_invites = await member.guild.invites()

            for inv in fresh_invites:
                cache_key = f"{gid}:{inv.code}"
                old_uses  = old_snapshot.get(cache_key, 0)
                new_uses  = inv.uses or 0
                if new_uses > old_uses:
                    used_code    = inv.code
                    inviter_id   = inv.inviter.id   if inv.inviter else 0
                    inviter_name = inv.inviter.name if inv.inviter else t(gid, "unknown")
                    _invite_cache[cache_key] = new_uses
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute(
                            "UPDATE invite_cache_db SET uses=? WHERE guild_id=? AND invite_code=?",
                            (new_uses, gid, inv.code)
                        )
                        await db.commit()
                    break

            # Синхронизируем кэш
            for inv in fresh_invites:
                _invite_cache[f"{gid}:{inv.code}"] = inv.uses or 0

            # Разовый инвайт — исчез из списка
            if not used_code:
                fresh_codes = {f"{gid}:{inv.code}" for inv in fresh_invites}
                for cache_key in list(old_snapshot.keys()):
                    if cache_key not in fresh_codes:
                        code = cache_key.split(":", 1)[1]
                        async with aiosqlite.connect(DB_PATH) as db:
                            async with db.execute(
                                "SELECT inviter_id, inviter_name FROM invite_cache_db WHERE guild_id=? AND invite_code=?",
                                (gid, code)
                            ) as c:
                                row = await c.fetchone()
                        if row and row[0]:
                            used_code    = code
                            inviter_id   = row[0]
                            inviter_name = row[1] or t(gid, "unknown")
                        _invite_cache.pop(cache_key, None)
                        break

            if used_code:
                print(f"[INVITE] ✅ Found on attempt {_attempt+1}: {used_code} → {inviter_name}")
                break  # нашли — выходим из retry loop

            print(f"[INVITE] Attempt {_attempt+1}: not found yet, retrying...")

        except discord.Forbidden:
            print(f"[INVITE] Forbidden on {member.guild.name}")
            break
        except Exception as ex:
            print(f"[INVITE] Error attempt {_attempt+1}: {ex}")

    # Пишем в БД
    if used_code and inviter_id:
        await log_invite_use(gid, used_code, inviter_id, inviter_name, member.id, member.name)

    age = (datetime.datetime.utcnow() - member.created_at.replace(tzinfo=None)).days
    ch = await sec_check(member.guild, "joins")
    if ch:
        sus = "🔴 Подозрительный (<7д)" if age<7 else "🟡 Новый (<30д)" if age<30 else "🟢 Обычный"
        e = build_embed(C.SUCCESS)
        e.set_thumbnail(url=member.display_avatar.url)
        e.add_field(name="Участник", value=f"{member.mention} (`{member.name}`)", inline=False)
        e.add_field(name="Аккаунт",  value=f"{age} дней — {sus}",                inline=True)
        e.add_field(name="Инвайт",   value=f"`{used_code}`" if used_code else "неизвестно", inline=True)
        e.add_field(name="Пригласил", value=f"{inviter_name} (`{inviter_id}`)" if inviter_id else "неизвестно", inline=True)
        # Заметка к инвайту (из invite_log.note)
        if used_code:
            async with aiosqlite.connect(DB_PATH) as _db_note:
                async with _db_note.execute(
                    "SELECT note FROM invite_log WHERE guild_id=? AND invite_code=? AND note!='' LIMIT 1",
                    (gid, used_code)
                ) as _cn:
                    _note_row = await _cn.fetchone()
            if _note_row and _note_row[0]:
                e.add_field(name="📝 Invite note", value=_note_row[0], inline=False)
        await ch.send(embed=e)

    ch2 = await sec_check(member.guild, "suspicious")
    if ch2 and age < 7:
        e = build_embed(C.DANGER)
        e.set_thumbnail(url=member.display_avatar.url)
        e.add_field(name="Участник", value=f"{member.mention}", inline=False)
        e.add_field(name="Возраст", value=f"{age} дней", inline=True)
        e.add_field(name="Инвайт", value=f"`{used_code}`" if used_code else "неизвестно", inline=True)
        await ch2.send(embed=e)

@bot.event
async def on_member_remove(member):
    ch = await sec_check(member.guild, "leaves")
    if not ch: return
    roles = [r.mention for r in member.roles if r.name != "@everyone"]
    gid2 = member.guild.id
    e = build_embed(C.DANGER, thumbnail=member.display_avatar.url)
    e.set_author(name=t(gid2, "left"))
    e.add_field(name=t(gid2, "member"), value=f"{member.mention} · `{member.name}`", inline=False)
    e.add_field(name=t(gid2, "roles"),  value=", ".join(roles) if roles else "—",    inline=False)
    await ch.send(embed=e)

@bot.event
async def on_member_ban(guild, user):
    ch = await sec_check(guild, "bans")
    if not ch: return
    gid3 = guild.id
    e = build_embed(C.DANGER)
    e.set_author(name=t(gid3, "banned"))
    e.add_field(name=t(gid3, "member"), value=f"{user.mention} · `{user.name}`", inline=False)
    await ch.send(embed=e)

@bot.event
async def on_member_unban(guild, user):
    ch = await sec_check(guild, "bans")
    if not ch: return
    gid4 = guild.id
    e = build_embed(C.SUCCESS)
    e.set_author(name=t(gid4, "unbanned"))
    e.add_field(name=t(gid4, "member"), value=user.mention, inline=False)
    await ch.send(embed=e)

@bot.event
async def on_member_update(before, after):
    if before.nick != after.nick:
        ch = await sec_check(after.guild, "nick_change")
        if ch:
            gid5 = after.guild.id
            e = build_embed(C.INFO)
            e.set_author(name=t(gid5, "nick_changed"))
            e.add_field(name=t(gid5, "member"), value=after.mention,              inline=False)
            e.add_field(name=t(gid5, "was"),    value=before.nick or before.name, inline=True)
            e.add_field(name=t(gid5, "now"),    value=after.nick or after.name,   inline=True)
            await ch.send(embed=e)
    added = set(after.roles)-set(before.roles); removed = set(before.roles)-set(after.roles)
    if added or removed:
        ch = await sec_check(after.guild, "role_change")
        if ch:
            gid6 = after.guild.id
            e = build_embed(C.PRIMARY)
            e.set_author(name=t(gid6, "roles_changed"))
            e.add_field(name=t(gid6, "member"),  value=after.mention, inline=False)
            if added:   e.add_field(name=t(gid6, "added"),   value=", ".join(r.mention for r in added),   inline=False)
            if removed: e.add_field(name=t(gid6, "removed"), value=", ".join(r.mention for r in removed), inline=False)
            await ch.send(embed=e)
    if before.timed_out_until != after.timed_out_until:
        ch = await sec_check(after.guild, "timeouts")
        if ch:
            gid7 = after.guild.id
            if after.timed_out_until:
                e = build_embed(C.WARNING)
                e.set_author(name=t(gid7, "muted"))
                e.add_field(name=t(gid7, "member"), value=after.mention, inline=False)
                e.add_field(name=t(gid7, "until"),  value=after.timed_out_until.strftime("%d.%m.%Y %H:%M"), inline=True)
            else:
                e = build_embed(C.SUCCESS)
                e.set_author(name=t(gid7, "unmuted"))
                e.add_field(name=t(gid7, "member"), value=after.mention, inline=False)
            await ch.send(embed=e)

@bot.event
async def on_message_delete(message):
    if message.author.bot or not message.guild: return
    ch = await sec_check(message.guild, "msg_delete")
    if not ch: return
    gid8 = message.guild.id
    e = build_embed(C.DANGER)
    e.set_author(name=t(gid8, "msg_deleted"))
    e.add_field(name=t(gid8, "member"),  value=message.author.mention,                                    inline=True)
    e.add_field(name=t(gid8, "channel"), value=getattr(message.channel, "mention", str(message.channel)), inline=True)
    e.add_field(name=t(gid8, "text"),    value=message.content[:1020] or "*(attachment)*",                inline=False)
    await ch.send(embed=e)

@bot.event
async def on_message_edit(before, after):
    if before.author.bot or not before.guild or before.content == after.content: return
    ch = await sec_check(before.guild, "msg_edit")
    if not ch: return
    gid9 = before.guild.id
    e = build_embed(C.WARNING)
    e.set_author(name=t(gid9, "msg_edited"))
    e.add_field(name=t(gid9, "member"), value=before.author.mention,        inline=False)
    e.add_field(name=t(gid9, "was"),    value=before.content[:512] or "—",  inline=False)
    e.add_field(name=t(gid9, "now"),    value=after.content[:512] or "—",   inline=False)
    e.add_field(name="Link",            value=f"[Jump]({after.jump_url})",   inline=True)
    await ch.send(embed=e)

@bot.event
async def on_invite_create(invite):
    _invite_cache[f"{invite.guild.id}:{invite.code}"] = invite.uses or 0
    print(f"[INVITE] Created: {invite.code} by {invite.inviter}")
    ch = await sec_check(invite.guild, "invites")
    if not ch: return
    gid10 = invite.guild.id
    e = build_embed(C.INFO)
    e.set_author(name=t(gid10, "invite_created"))
    e.add_field(name=t(gid10, "member"),  value=f"{invite.inviter.mention} · `{invite.inviter.name}`" if invite.inviter else "?", inline=True)
    e.add_field(name=t(gid10, "code"),    value=f"`{invite.code}`",                                                                inline=True)
    e.add_field(name=t(gid10, "uses"),    value=str(invite.max_uses) if invite.max_uses else "∞",                                  inline=True)
    e.add_field(name=t(gid10, "expires"), value=invite.expires_at.strftime("%d.%m.%Y %H:%M") if invite.expires_at else t(gid10, "never"), inline=True)
    await ch.send(embed=e)

@bot.event
async def on_invite_delete(invite):
    _invite_cache.pop(f"{invite.guild.id}:{invite.code}", None)
    print(f"[INVITE] Deleted: {invite.code}")
    ch = await sec_check(invite.guild, "invites")
    if not ch: return
    gid11 = invite.guild.id
    e = build_embed(C.MUTED)
    e.set_author(name=t(gid11, "invite_deleted"))
    e.add_field(name=t(gid11, "code"), value=f"`{invite.code}`", inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_join(guild):
    """Когда бот добавляется на новый сервер — сразу загружаем инвайты"""
    await refresh_invite_cache(guild)


@bot.event
async def on_voice_state_update(member, before, after):
    ch = await sec_check(member.guild, "voice")
    if not ch or before.channel == after.channel: return
    if before.channel is None: desc, color = f"вошёл в **{after.channel.name}**", discord.Color.green()
    elif after.channel is None: desc, color = f"вышел из **{before.channel.name}**", discord.Color.red()
    else: desc, color = f"**{before.channel.name}** → **{after.channel.name}**", discord.Color.blue()
    gid12 = member.guild.id
    e = build_embed(color)
    e.set_author(name=t(gid12, "voice_update"))
    e.add_field(name=t(gid12, "member"), value=member.mention, inline=True)
    e.add_field(name=t(gid12, "action"), value=desc,           inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_channel_create(channel_created):
    ch = await sec_check(channel_created.guild, "channels")
    if not ch: return
    e = build_embed(C.SUCCESS)
    e.set_author(name="Channel created")
    e.add_field(name="Канал", value=channel_created.mention, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_channel_delete(channel_deleted):
    ch = await sec_check(channel_deleted.guild, "channels")
    if not ch: return
    e = build_embed(C.DANGER)
    e.set_author(name="Channel deleted")
    e.add_field(name="Канал", value=channel_deleted.name, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_role_create(role):
    ch = await sec_check(role.guild, "roles")
    if not ch: return
    e = build_embed(C.SUCCESS)
    e.set_author(name="Role created")
    e.add_field(name="Роль", value=role.mention, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_role_delete(role):
    ch = await sec_check(role.guild, "roles")
    if not ch: return
    e = build_embed(C.DANGER)
    e.set_author(name="Role deleted")
    e.add_field(name="Роль", value=role.name, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_update(before, after):
    ch = await sec_check(after, "server_edit")
    if not ch or before.name == after.name: return
    e = build_embed(C.INFO)
    e.set_author(name="Server updated")
    e.add_field(name="Было", value=before.name, inline=True)
    e.add_field(name="Стало", value=after.name, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_user_update(before, after):
    if before.avatar == after.avatar: return
    for guild in bot.guilds:
        member = guild.get_member(after.id)
        if not member: continue
        ch = await sec_check(guild, "avatar_change")
        if not ch: continue
        e = build_embed(C.INFO)
        e.set_author(name="Avatar changed")
        e.add_field(name="Member", value=member.mention, inline=False)
        e.set_thumbnail(url=after.display_avatar.url)
        await ch.send(embed=e)


@bot.event
async def on_thread_create(thread):
    ch = await sec_check(thread.guild, "threads")
    if not ch: return
    e = build_embed(C.SUCCESS)
    e.set_author(name="Thread created")
    e.add_field(name="Тред", value=thread.mention, inline=True)
    if thread.parent: e.add_field(name="Канал", value=thread.parent.mention, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_interaction(interaction):
    if not await is_enabled(interaction.guild_id, "slash_commands"): return
    if interaction.type != discord.InteractionType.application_command: return
    ch = await get_log_ch(interaction.guild)
    if not ch: return
    e = build_embed(C.PRIMARY)
    e.set_author(name="Slash command")
    e.add_field(name="Пользователь", value=interaction.user.mention, inline=True)
    e.add_field(name="Команда", value=f"`/{interaction.data.get('name','?')}`", inline=True)
    await ch.send(embed=e)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ADMIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@bot.tree.command(name="lang", description="Set bot language / Установить язык бота")
@app_commands.describe(language="ru — Русский | en — English")
async def lang_cmd(interaction: discord.Interaction, language: str = "ru"):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Manage Server permission required.", ephemeral=True)
    lang = language.lower().strip()
    if lang not in ("ru", "en"):
        return await interaction.response.send_message("❌ Available: `ru` or `en`", ephemeral=True)
    _guild_lang[interaction.guild_id] = lang
    # Сохраняем в БД
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO guild_settings (guild_id, lang) VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET lang=excluded.lang
        """, (interaction.guild_id, lang))
        await db.commit()
    if lang == "en":
        e = build_embed(C.SUCCESS, description="All bot responses will now be in **English**.\nUse `/lang language:ru` to switch back.")
        e.set_author(name="Language → English 🇬🇧")
    else:
        e = build_embed(C.SUCCESS, description="Все ответы бота теперь на **русском** языке.\nИспользуй `/lang language:en` для переключения.")
        e.set_author(name="Язык → Русский 🇷🇺")
    await interaction.response.send_message(embed=e)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ACTION VIEWS — кнопки действий
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class MemberActionView(discord.ui.View):
    """Кнопки для /userinfo — быстрые действия над участником"""
    def __init__(self, member: discord.Member):
        super().__init__(timeout=120)
        self.member = member

    @discord.ui.button(label="Warn", style=discord.ButtonStyle.secondary)
    async def warn_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.manage_messages:
            return await interaction.response.send_message("No permission.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO warnings (guild_id,user_id,mod_id,reason,created_at) VALUES (?,?,?,?,?)",
                (interaction.guild_id, self.member.id, interaction.user.id,
                 "Quick warn via button", datetime.datetime.utcnow().isoformat())
            )
            await db.commit()
        await interaction.response.send_message(
            f"⚠️ **{self.member.display_name}** warned.", ephemeral=True)

    @discord.ui.button(label="Kick", style=discord.ButtonStyle.danger)
    async def kick_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.kick_members:
            return await interaction.response.send_message("No permission.", ephemeral=True)
        try:
            await self.member.kick(reason=f"Kicked by {interaction.user}")
            await interaction.response.send_message(f"✓ Kicked **{self.member.display_name}**.", ephemeral=True)
        except Exception as ex:
            await interaction.response.send_message(f"Error: {ex}", ephemeral=True)

    @discord.ui.button(label="Mute 10m", style=discord.ButtonStyle.secondary)
    async def mute_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.moderate_members:
            return await interaction.response.send_message("No permission.", ephemeral=True)
        try:
            until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(minutes=10)
            await self.member.timeout(until, reason=f"Muted by {interaction.user}")
            await interaction.response.send_message(f"✓ **{self.member.display_name}** muted 10 min.", ephemeral=True)
        except Exception as ex:
            await interaction.response.send_message(f"Error: {ex}", ephemeral=True)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class AlbionPlayerView(discord.ui.View):
    """Кнопки для /stats — быстрый переход к деталям"""
    def __init__(self, player_name: str):
        super().__init__(timeout=120)
        self.player_name = player_name
        # Кнопка-ссылка на профиль игрока
        self.add_item(discord.ui.Button(
            label="View on albionbb.com",
            style=discord.ButtonStyle.link,
            url=f"https://albionbb.com/player/{player_name}"
        ))

    @discord.ui.button(label="Kills", style=discord.ButtonStyle.secondary)
    async def kills_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            f"Use `/kills player:{self.player_name}`", ephemeral=True)

    @discord.ui.button(label="Deaths", style=discord.ButtonStyle.secondary)
    async def deaths_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            f"Use `/deaths player:{self.player_name}`", ephemeral=True)

    @discord.ui.button(label="History", style=discord.ButtonStyle.secondary)
    async def history_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            f"Use `/history player:{self.player_name}`", ephemeral=True)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class WarnActionView(discord.ui.View):
    """Кнопки после /warn — быстрые действия"""
    def __init__(self, member: discord.Member, warn_count: int):
        super().__init__(timeout=60)
        self.member = member
        self.warn_count = warn_count

    @discord.ui.button(label="Mute 1h", style=discord.ButtonStyle.secondary)
    async def mute_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.moderate_members:
            return await interaction.response.send_message("No permission.", ephemeral=True)
        try:
            until = datetime.datetime.now(datetime.timezone.utc) + datetime.timedelta(hours=1)
            await self.member.timeout(until, reason="Muted after warn")
            await interaction.response.send_message(f"✓ **{self.member.display_name}** muted 1h.", ephemeral=True)
        except Exception as ex:
            await interaction.response.send_message(f"Error: {ex}", ephemeral=True)

    @discord.ui.button(label="Kick", style=discord.ButtonStyle.danger)
    async def kick_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.kick_members:
            return await interaction.response.send_message("No permission.", ephemeral=True)
        try:
            await self.member.kick(reason="Kicked after warn")
            await interaction.response.send_message(f"✓ Kicked.", ephemeral=True)
        except Exception as ex:
            await interaction.response.send_message(f"Error: {ex}", ephemeral=True)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


@bot.tree.command(name="setpremium", description="[ADMIN] Установить тир")
@app_commands.describe(tier="0=Free 1=Premium(€2.99) 2=Security(€4.99)", days="Days")
async def setpremium(interaction: discord.Interaction, tier: int, days: int = 30):
    if interaction.user.id not in OWNER_IDS and not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ Нет доступа.", ephemeral=True)
    await set_tier(interaction.guild_id, tier, days)
    await interaction.response.send_message(f"✅ **{TIER_NAMES.get(tier,'?')}** на {days} дней.", ephemeral=True)

@bot.tree.command(name="sechelp", description="Advanced Security команды [-q prefix] — только для администраторов")
async def sechelp(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message(
            embed=build_embed(C.DANGER, description="Admins only."),
            ephemeral=True
        )
    e = build_embed(C.PRIMARY)





    e = build_embed(0x5865F2)
    e.set_author(name="Witness Advanced Security · -q commands")
    commands_list = [
        ("-q scan @user",      "Полное сканирование: threat intel + fingerprint + граф + подпись"),
        ("-q threat @user",    "Threat Intelligence: возраст, паттерны, impersonation, unicode spoofing"),
        ("-q graph @user",     "Граф социальных связей и кластерный анализ"),
        ("-q fp @user",        "Поведенческий fingerprint: активность, стиль, risk score"),
        ("-q nlp [текст]",     "NLP анализ токсичности, угроз и спама через AI"),
        ("-q forensics [id]",  "Криминалистика сообщения: хеш, EXIF изображений, дубли"),
        ("-q sig [id]",        "Проверить цифровую подпись модераторского действия"),
        ("-q alert",           "Последние алерты безопасности"),
        ("-q network",         "Статистика угроз и аномалий сервера"),
        ("-q whitelist @user", "Добавить в whitelist (исключить из проверок)"),
        ("-q blacklist @user", "Добавить в blacklist"),
        ("-q report @user",    "Отправить в глобальную базу угроз (между серверами)"),
        ("-q status",          "Статус всех систем: кэши, AI движок, HMAC"),
        ("-q help",            "Этот список прямо в чате"),
    ]
    for cmd, desc in commands_list:
        e.add_field(name=f"`{cmd}`", value=desc, inline=False)
    await interaction.response.send_message(embed=e, ephemeral=True)



    tier = await get_tier(interaction.guild_id)
    e = build_embed(TIER_COLORS[tier])
    e.set_author(name="Witness · Подписка")
    e.add_field(name="Тир", value=TIER_NAMES[tier], inline=True)
    if tier == TIER_FREE: e.add_field(name="Апгрейд", value="witnessbot.gg/premium", inline=True)
    await interaction.response.send_message(embed=e)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SECURITY COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

sec_grp = app_commands.Group(name="security", description="🛡️ Безопасность сервера [Premium]")

@sec_grp.command(name="status", description="Статус всех модулей")
async def sec_status(interaction: discord.Interaction):
    log_ch, settings = await get_security(interaction.guild_id)
    ch_obj = interaction.guild.get_channel(log_ch)
    e = build_embed(C.INFO)
    e.set_author(name="Security Status")
    e.add_field(name="Канал логов", value=ch_obj.mention if ch_obj else "не задан (/security setlog)", inline=False)
    on_lines = [f"✅ `{k}` — {n}" for k,n in SEC_NAMES.items() if settings.get(k,False)]
    off_lines = [f"❌ `{k}` — {n}" for k,n in SEC_NAMES.items() if not settings.get(k,False)]
    e.add_field(name="Включено", value="\n".join(on_lines) or "нет", inline=False)
    e.add_field(name="Выключено", value="\n".join(off_lines[:10]) or "нет", inline=False)
    await interaction.response.send_message(embed=e, ephemeral=True)

@sec_grp.command(name="toggle", description="Включить/выключить модуль")
@app_commands.describe(module="Название модуля")
async def sec_toggle(interaction: discord.Interaction, module: str):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if module not in SEC_NAMES:
        return await interaction.response.send_message(f"❌ Неизвестный модуль. Смотри `/security status`", ephemeral=True)
    log_ch, settings = await get_security(interaction.guild_id)
    settings[module] = not settings.get(module, False)
    await save_security(interaction.guild_id, log_ch, settings)
    state = "✅ включён" if settings[module] else "❌ выключен"
    await interaction.response.send_message(f"**{SEC_NAMES[module]}** (`{module}`) {state}", ephemeral=True)

@sec_grp.command(name="setlog", description="Задать канал логов")
@app_commands.describe(channel="Канал для логов")
async def sec_setlog(interaction: discord.Interaction, channel: discord.TextChannel):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    _, settings = await get_security(interaction.guild_id)
    await save_security(interaction.guild_id, channel.id, settings)
    await interaction.response.send_message(f"✅ Канал логов → {channel.mention}", ephemeral=True)

bot.tree.add_command(sec_grp)


@bot.tree.command(name="invnote", description="Добавить заметку к инвайт-коду")
@app_commands.describe(
    code="Код инвайта (без discord.gg/)",
    note="Заметка (например: 'Реклама Reddit', 'Партнёр XYZ', пусто = удалить)"
)
async def invnote(interaction: discord.Interaction, code: str, note: str = ""):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно право Manage Server.", ephemeral=True)
    gid = interaction.guild_id
    code = code.strip().removeprefix("discord.gg/").removeprefix("https://discord.gg/")
    now  = datetime.datetime.utcnow().isoformat()

    async with aiosqlite.connect(DB_PATH) as db:
        if note:
            # Обновляем note во всех записях с этим кодом
            await db.execute(
                "UPDATE invite_log SET note=? WHERE guild_id=? AND invite_code=?",
                (note, gid, code)
            )
            # Если записей нет ещё — вставляем placeholder
            async with db.execute(
                "SELECT COUNT(*) FROM invite_log WHERE guild_id=? AND invite_code=?",
                (gid, code)
            ) as c:
                count = (await c.fetchone())[0]
            if count == 0:
                await db.execute(
                    "INSERT INTO invite_log (guild_id, invite_code, note) VALUES (?,?,?)",
                    (gid, code, note)
                )
            await db.commit()
            e = build_embed(C.SUCCESS)
            e.set_author(name=f"Заметка сохранена · {code}")
            e.add_field(name="Код",     value=f"`{code}`",        inline=True)
            e.add_field(name="Заметка", value=note,               inline=True)
            e.add_field(name="Добавил", value=interaction.user.mention, inline=True)
        else:
            await db.execute(
                "UPDATE invite_log SET note='' WHERE guild_id=? AND invite_code=?",
                (gid, code)
            )
            await db.commit()
            e = build_embed(C.MUTED)
            e.set_author(name=f"Заметка удалена · {code}")

    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="invnotes", description="Список всех заметок к инвайтам")
async def invnotes(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно право Manage Server.", ephemeral=True)
    gid = interaction.guild_id

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT invite_code, note, set_by, updated_at FROM invite_notes WHERE guild_id=? ORDER BY updated_at DESC",
            (gid,)
        ) as c:
            rows = await c.fetchall()

    e = build_embed(C.INFO)
    e.set_author(name="Invite Notes")
    if not rows:
        e.description = "Заметок нет. Добавь через `/invnote code:КОД note:ЗАМЕТКА`"
    else:
        for code, note, set_by, updated in rows[:15]:
            mod = interaction.guild.get_member(set_by)
            mod_str = mod.display_name if mod else str(set_by)
            e.add_field(
                name=f"`{code}`",
                value=f"{note}\n*{mod_str} · {updated[:10]}*",
                inline=False
            )
        if len(rows) > 15:
            e.set_footer(text=f"Witness · Показано 15 из {len(rows)}")
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="invcheck", description="История инвайта [Premium]")
@app_commands.describe(code="Код инвайта")
async def invcheck(interaction: discord.Interaction, code: str):
    # basic security — free for all
    rows = await get_invite_history(interaction.guild_id, code)
    e = build_embed(C.INFO)
    e.add_field(name="Использований", value=str(len(rows)), inline=True)
    if rows:
        lines = [f"{i+1}. **{r[0]}** (`{r[1]}`) — {r[2][:10]}" for i,r in enumerate(rows[:15])]
        e.add_field(name="Кто зашёл", value="\n".join(lines), inline=False)
    else:
        e.add_field(name="Кто зашёл", value="никто", inline=False)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="invuser", description="Инвайты пользователя [Premium]")
@app_commands.describe(member="Пользователь")
async def invuser(interaction: discord.Interaction, member: discord.Member):
    # basic security — free for all
    rows = await get_user_invites(interaction.guild_id, member.id)
    e = build_embed(C.PRIMARY)
    e.set_author(name=f"Invites: {member.display_name}", icon_url=member.display_avatar.url)
    e.add_field(name="Всего приглашено", value=str(len(rows)), inline=True)
    if rows:
        lines = [f"`{r[0]}` — **{r[1]}** — {r[2][:10]}" for r in rows[:15]]
        e.add_field(name="Приглашённые", value="\n".join(lines), inline=False)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="invdel", description="Удалить инвайт [Premium]")
@app_commands.describe(code="Код инвайта")
async def invdel(interaction: discord.Interaction, code: str):
    # basic security — free for all
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно Manage Server.", ephemeral=True)
    try:
        inv = await interaction.guild.fetch_invite(code)
        await inv.delete()
        await interaction.response.send_message(f"✅ Инвайт `{code}` удалён.")
    except discord.NotFound:
        await interaction.response.send_message(f"❌ `{code}` не найден.", ephemeral=True)
    except Exception as ex:
        await interaction.response.send_message(f"❌ Ошибка: {ex}", ephemeral=True)

@bot.tree.command(name="warn", description="Выдать варн [Premium]")
@app_commands.describe(member="Пользователь", reason="Причина")
async def warn(interaction: discord.Interaction, member: discord.Member, reason: str = "Не указана"):
    # basic security — free for all
    if not interaction.user.guild_permissions.moderate_members:
        return await interaction.response.send_message("❌ Нужно Moderate Members.", ephemeral=True)
    await add_warning(interaction.guild_id, member.id, interaction.user.id, reason)
    warns = await get_warnings(interaction.guild_id, member.id)
    gid   = interaction.guild_id
    color = C.WARNING if len(warns) < 3 else C.DANGER
    e = build_embed(color)
    e.set_author(name=f"Warning — {member.display_name}", icon_url=member.display_avatar.url)
    e.set_thumbnail(url=member.display_avatar.url)
    e.add_field(name=t(gid,"member"),    value=member.mention,           inline=True)
    e.add_field(name=t(gid,"moderator"), value=interaction.user.mention, inline=True)
    e.add_field(name="Warns",            value=f"**{len(warns)}/5**",    inline=True)
    e.add_field(name=t(gid,"reason"),    value=reason,                   inline=False)
    view = WarnActionView(member, len(warns))
    await interaction.response.send_message(embed=e, view=view)
    if len(warns) >= 3:
        try:
            await member.timeout(timedelta(hours=1), reason=f"Авто-таймаут: {len(warns)} варнов")
            await interaction.channel.send(f"🔇 {member.mention} → авто-таймаут (3 варна)")
        except Exception: pass

@bot.tree.command(name="warnings", description="Список варнов [Premium]")
@app_commands.describe(member="Пользователь")
async def warnings(interaction: discord.Interaction, member: discord.Member):
    # basic security — free for all
    rows = await get_warnings(interaction.guild_id, member.id)
    gid   = interaction.guild_id
    color = C.DANGER if len(rows) >= 3 else C.WARNING if rows else C.SUCCESS
    e = build_embed(color)
    e.set_author(name=f"Warns — {member.display_name}", icon_url=member.display_avatar.url)
    e.set_thumbnail(url=member.display_avatar.url)
    if not rows:
        e.description = "No warnings."
    else:
        e.add_field(name="Total", value=f"**{len(rows)}/5**", inline=True)
        e.add_field(name="Status", value="`● HIGH`" if len(rows)>=3 else "`▲ MEDIUM`", inline=True)
        e.add_field(name="​", value="​", inline=True)
        for wid, mod_id, reason, created in rows:
            mod = interaction.guild.get_member(mod_id)
            e.add_field(
                name=f"#{wid} · {created[:10]}",
                value=f"By: {mod.mention if mod else mod_id}\nReason: {reason}",
                inline=False
            )
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="clearwarn", description="Снять варн [Premium]")
@app_commands.describe(warn_id="ID варна")
async def clearwarn(interaction: discord.Interaction, warn_id: int):
    # basic security — free for all
    if not interaction.user.guild_permissions.moderate_members:
        return await interaction.response.send_message("❌ Нужно Moderate Members.", ephemeral=True)
    await remove_warning(warn_id, interaction.guild_id)
    await interaction.response.send_message(f"✅ Варн `#{warn_id}` снят.", ephemeral=True)

@bot.tree.command(name="purge", description="Удалить N сообщений [Premium]")
@app_commands.describe(count="Количество (1-100)")
async def purge(interaction: discord.Interaction, count: int):
    # basic security — free for all
    if not interaction.user.guild_permissions.manage_messages:
        return await interaction.response.send_message("❌ Нужно Manage Messages.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    deleted = await interaction.channel.purge(limit=max(1, min(count, 100)))
    await interaction.followup.send(f"🗑️ Удалено **{len(deleted)}** сообщений.", ephemeral=True)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FREE COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def build_help_embed(page: str, guild_tier: int) -> discord.Embed:
    """Строит эмбед для /help по выбранному разделу"""
    PAGES = {
        "general": {
            "title": "📖 Основные команды",
            "color": C.PRIMARY,
            "fields": [
                ("🆓 Информация", "`/ping` `/userinfo` `/serverinfo` `/subinfo`"),
                ("🆓 XP и экономика", "`/rank` `/leaderboard` `/coins`"),
                ("🆓 Утилиты", "`/poll` `/remind` `/lfg` `/weather` `/translate`"),
                ("🆓 Комьюнити", "`/birthday` `/suggestion` `/ticket`"),
                ("⭐ Premium", "`/starboard` `/serverstats` `/giveaway` `/roast` `/summarize` `/imagine`"),
            ]
        },
        "albion": {
            "title": "⚔️ Albion Online",
            "color": C.INFO,
            "fields": [
                ("🆓 Игроки", "`/stats` `/kills` `/deaths` `/history` `/compare`"),
                ("🆓 Гильдии", "`/guild` `/battle` `/party`"),
                ("💎 Pro — Рынок", "`/blackmarket` — топ профитных предметов\n`/craftcalc` — таблица крафта → Google Sheets\n`/flipper` — арбитраж между городами"),
                ("💎 Pro — Прочее", "`/guildwar` `/pricewatch` `/askalbion`"),
            ]
        },
        "games": {
            "title": "🎮 Другие игры",
            "color": C.SUCCESS,
            "fields": [
                ("🆓 Minecraft", "`/mc [address]` — статус сервера"),
                ("🆓 Old School RuneScape", "`/rs [username]` — навыки игрока"),
                ("⭐ Valorant", "`/val [Name#TAG]` — ранг и RR"),
                ("⭐ CS2", "`/cs2 [steam_id]` — статистика"),
                ("⭐ League of Legends", "`/lol [summoner]` — ранг и WR"),
                ("⭐ Lost Ark", "`/lostark [character]` — item level"),
            ]
        },
        "security": {
            "title": "🛡️ Безопасность",
            "color": C.WARNING,
            "fields": [
                ("Настройка", "`/security status` · `toggle` · `setlog`"),
                ("Логирование", "`joins` `leaves` `bans` `timeouts`\n`msg_delete` `msg_edit` `invites` `suspicious`\n`nick_change` `role_change` `voice` и ещё 8 модулей"),
                ("Авто-защита", "`anti_raid` — кик при рейде\n`anti_spam` — таймаут при спаме\n`/lockdown` · `/slowmode`"),
                ("Инвайты", "`/invcheck` `/invuser` `/invdel`"),
                ("Модерация", "`/warn` `/warnings` `/clearwarn` `/purge` `/report`"),
                ("🔐 Advanced Security (только администраторы)",
                 "Префикс `-q` открывает расширенный модуль безопасности:\n"
                 "`-q scan @user` — полное сканирование участника\n"
                 "`-q threat @user` — threat intelligence проверка\n"
                 "`-q graph @user` — граф социальных связей\n"
                 "`-q fp @user` — поведенческий fingerprint\n"
                 "`-q nlp [текст]` — NLP анализ токсичности\n"
                 "`-q forensics [id]` — криминалистика сообщения\n"
                 "`-q alert` — последние алерты\n"
                 "`-q network` — статистика угроз сервера\n"
                 "`-q status` — статус всех систем\n"
                 "`-q help` — полный список команд"),
            ]
        },
        "pro": {
            "title": "💎 Pro — €9.99/мес",
            "color": C.GOLD,
            "fields": [
                ("Чёрный рынок", "`/blackmarket category:weapon tier:8`\nКатегории: `weapon` `offhand` `armor_plate` `armor_leather` `armor_cloth` `bag`\nⓘ Нажми кнопку Select в ответе бота для выбора категории"),
                ("Крафт-калькулятор", "`/craftcalc tier:8 server:eu tax:8`\nЭкспорт всех предметов T6–T8 в Google Sheets"),
                ("Торговля", "`/flipper` — арбитраж между городами\n`/pricewatch` — алерты на изменение цены"),
                ("Статистика", "`/guildwar` · `/party` · `/tournament`"),
            ]
        },
        "ai": {
            "title": "🤖 AI команды",
            "color": C.PREMIUM,
            "fields": [
                ("ⓘ AI движок", "Бот использует **Groq** (Llama 3.3 70B) и **Gemini** — бесплатно."),
                ("⭐ Текст", "`/ai` `/summarize` `/askalbion`"),
                ("⭐ Развлечения", "`/roast` — роаст участника\n`/imagine` — генерация изображений (Pollinations.ai)"),
                ("💎 Pro + AI", "`/party` — AI вердикт на состав группы"),
            ]
        },
    }

    if page not in PAGES:
        page = "general"

    data = PAGES[page]

    e = build_embed(data["color"])
    e.set_author(name=data["title"])

    for name, val in data["fields"]:
        e.add_field(name=name, value=val, inline=False)

    e.add_field(
        name="Подписка",
        value=f"{tier_badge(guild_tier)} `{tier_bar}` · witnessbot.gg/premium",
        inline=False
    )
    return e


@bot.tree.command(name="help", description="Все команды бота")
@app_commands.describe(page="Раздел: general / albion / games / security / pro / ai")
async def help_cmd(interaction: discord.Interaction, page: str = "general"):
    tier = await get_tier(interaction.guild_id)
    embed = build_help_embed(page.lower(), tier)
    view = HelpView(page.lower(), tier)
    await interaction.response.send_message(embed=embed, view=view)




@bot.tree.command(name="ping")
async def ping(interaction: discord.Interaction):
    ms = round(bot.latency * 1000)
    color = C.SUCCESS if ms < 100 else C.WARNING if ms < 200 else C.DANGER
    tag = "`✓ GOOD`" if ms < 100 else "`▲ OK`" if ms < 200 else "`● SLOW`"
    e = build_embed(color)
    e.set_author(name="Witness · Pong!")
    e.add_field(name="Latency", value=f"**{ms}ms**", inline=True)
    e.add_field(name="Status",  value=tag,            inline=True)
    e.add_field(name="Shards",  value=f"**{len(bot.guilds)}** servers", inline=True)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="userinfo")
@app_commands.describe(member="Пользователь")
async def userinfo(interaction: discord.Interaction, member: discord.Member = None):
    m = member or interaction.user
    e = build_embed(C.PRIMARY, thumbnail=m.display_avatar.url)
    e.set_thumbnail(url=m.display_avatar.url)
    e.add_field(name="ID", value=m.id, inline=True)
    e.add_field(name="Зашёл", value=m.joined_at.strftime("%d.%m.%Y"), inline=True)
    roles = [r.mention for r in m.roles[1:]]
    e.add_field(name=f"Роли ({len(roles)})", value=" ".join(roles) if roles else "нет", inline=False)
    xp = await get_xp(interaction.guild_id, m.id)
    coins = await get_coins(interaction.guild_id, m.id)
    e.add_field(name="XP/Ур.", value=f"{xp}/{xp//100}", inline=True)
    e.add_field(name="Монеты", value=str(coins), inline=True)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="serverinfo")
async def serverinfo(interaction: discord.Interaction):
    g     = interaction.guild
    tier  = await get_tier(g.id)
    bots  = sum(1 for m in g.members if m.bot)
    humans = g.member_count - bots
    age   = (datetime.datetime.utcnow() - g.created_at.replace(tzinfo=None)).days
    e = build_embed(TIER_COLORS[tier])
    e.set_author(name=g.name, icon_url=g.icon.url if g.icon else None)
    if g.icon: e.set_thumbnail(url=g.icon.url)
    e.add_field(name="Members",  value=f"**{humans}** humans · {bots} bots",                 inline=True)
    e.add_field(name="Channels", value=f"**{len(g.text_channels)}** text · {len(g.voice_channels)} voice", inline=True)
    e.add_field(name="Roles",    value=f"**{len(g.roles)}**",                                inline=True)
    e.add_field(name="Boost",    value=f"Level **{g.premium_tier}** · {g.premium_subscription_count}×", inline=True)
    e.add_field(name="Age",      value=f"**{age}** days",                                    inline=True)
    e.add_field(name="Plan",     value=tier_tag(tier),                                       inline=True)
    e.set_footer(text=f"ID: {g.id} · Witness")
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="rank")
async def rank(interaction: discord.Interaction):
    lang  = get_lang(interaction.guild_id)
    xp    = await get_xp(interaction.guild_id, interaction.user.id)
    coins = await get_coins(interaction.guild_id, interaction.user.id)
    lvl   = xp // 100
    prog  = xp % 100
    e = build_embed(C.PRIMARY)
    e.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
    e.set_thumbnail(url=interaction.user.display_avatar.url)
    e.add_field(name=t(interaction.guild_id,"level"), value=f"**{lvl}**",    inline=True)
    e.add_field(name="XP",                            value=f"**{xp:,}**",   inline=True)
    e.add_field(name=t(interaction.guild_id,"coins"), value=f"**{coins:,}**", inline=True)
    e.add_field(name=f"→ Level {lvl+1}", value=f"**{prog}/100 XP**", inline=False)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="leaderboard")
async def leaderboard(interaction: discord.Interaction):
    rows = await get_leaderboard(interaction.guild_id)
    medals = ["🥇","🥈","🥉","4.","5.","6.","7.","8.","9.","10."]
    e = build_embed(C.GOLD)
    e.set_author(name="Top members")
    if not rows:
        e.description = "Нет данных."
    else:
        lines = []
        for i, (uid, xp) in enumerate(rows):
            m = interaction.guild.get_member(uid)
            name = m.display_name if m else str(uid)
            b = bar(xp % 100, 100, 6)
            lines.append(f"{medals[i]} **{name}** — {xp:,} XP · ур. {xp//100} `{b}`")
        e.description = "\n".join(lines)
    view = MemberActionView(m)
    await interaction.response.send_message(embed=e, view=view)

@bot.tree.command(name="coins")
async def coins_cmd(interaction: discord.Interaction):
    c = await get_coins(interaction.guild_id, interaction.user.id)
    xp = await get_xp(interaction.guild_id, interaction.user.id)
    e = build_embed(C.GOLD)
    e.set_author(name="Balance")
    e.add_field(name="🪙 Монеты", value=f"**{c:,}**", inline=True)
    e.add_field(name="⚡ XP", value=f"**{xp:,}**", inline=True)
    e.add_field(name="🏆 Уровень", value=f"**{xp//100}**", inline=True)
    e.set_footer(text=f"Witness · +1 монета за каждое сообщение")
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="poll")
@app_commands.describe(question="Вопрос", option1="Вариант 1", option2="Вариант 2", option3="Вариант 3", option4="Вариант 4")
async def poll(interaction: discord.Interaction, question: str, option1: str, option2: str, option3: str = None, option4: str = None):
    options = [o for o in [option1,option2,option3,option4] if o]
    emojis = ["1️⃣","2️⃣","3️⃣","4️⃣"]
    e = build_embed(C.INFO)
    e.set_author(name=f"Poll: {question}")
    for i,opt in enumerate(options): e.add_field(name=f"{emojis[i]} {opt}", value="​", inline=False)
    await interaction.response.send_message(embed=e)
    msg = await interaction.original_response()
    for i in range(len(options)): await msg.add_reaction(emojis[i])

@bot.tree.command(name="remind")
@app_commands.describe(minutes="Через сколько минут", message="Текст")
async def remind(interaction: discord.Interaction, minutes: int, message: str):
    if not 1<=minutes<=10080: return await interaction.response.send_message("⚠️ 1–10080 мин.", ephemeral=True)
    await interaction.response.send_message(f"⏰ Напомню через **{minutes} мин**!", ephemeral=True)
    await asyncio.sleep(minutes*60)
    try:
        await interaction.user.send(embed=build_embed(0x5865F2, description=message, color=C.INFO))
    except discord.Forbidden: pass

@bot.tree.command(name="lfg")
@app_commands.describe(game="Игра", slots="Нужно игроков", note="Дополнительно")
async def lfg(interaction: discord.Interaction, game: str, slots: int = 1, note: str = ""):
    e = build_embed(C.SUCCESS)
    e.set_author(name=f"LFG — {game}", icon_url=interaction.user.display_avatar.url)
    e.description = f"**{interaction.user.display_name}** ищет **{slots}** игрока(-ов)"
    if note: e.add_field(name="📝", value=note, inline=False)
    e.add_field(name="Присоединиться", value=f"✅ или ЛС {interaction.user.mention}", inline=False)
    e.set_footer(text="Авто-удаление через 2 часа")
    await interaction.response.send_message(embed=e)
    msg = await interaction.original_response()
    await msg.add_reaction("✅"); await msg.add_reaction("❌")
    await asyncio.sleep(7200)
    try: await msg.delete()
    except Exception: pass

@bot.tree.command(name="weather")
@app_commands.describe(city="Город")
@cooldown(10)
async def weather(interaction: discord.Interaction, city: str):
    await interaction.response.defer()
    if not WEATHER_KEY: return await interaction.followup.send("❌ Функция временно недоступна.")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={WEATHER_KEY}&units=metric") as r:
                if r.status!=200: return await interaction.followup.send(f"❌ Город **{city}** не найден.")
                d = await r.json()
        desc     = d["weather"][0]["description"].capitalize()
        temp     = d["main"]["temp"]
        feels    = d["main"]["feels_like"]
        humidity = d["main"]["humidity"]
        wind     = d["wind"]["speed"]
        gid = interaction.guild_id
        e = build_embed(C.INFO)
        e.set_author(name=f"{d['name']}, {d['sys']['country']}")
        e.add_field(name=t(gid,"temp"),        value=f"**{temp:.1f}°C**",         inline=True)
        e.add_field(name=t(gid,"feels"),       value=f"**{feels:.1f}°C**",        inline=True)
        e.add_field(name=t(gid,"description"), value=desc,                         inline=True)
        e.add_field(name=t(gid,"humidity"),    value=f"**{humidity}%**",           inline=True)
        e.add_field(name=t(gid,"wind"),        value=f"**{wind} m/s**",            inline=True)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="translate")
@app_commands.describe(text="Текст", to="Язык: en/de/ru/ua")
@cooldown(5)
async def translate(interaction: discord.Interaction, text: str, to: str = "en"):
    await interaction.response.defer()
    langs = {"en":"English","de":"German","ru":"Russian","ua":"Ukrainian"}
    target = langs.get(to.lower(),"English")
    try:
        result = await ask_ai(f"Translate to {target}. Reply ONLY with translation:\n\n{text}", system="Precise translator. Output only translated text.")
        e = build_embed(C.INFO)
        e.add_field(name="Оригинал", value=text[:1024], inline=False)
        e.add_field(name="Перевод", value=result[:1024], inline=False)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

# ─── FREE GAME COMMANDS ───────────────────────────────────────

@bot.tree.command(name="stats")
@app_commands.describe(player="Ник игрока")
@cooldown(5)
async def stats(interaction: discord.Interaction, player: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            pid, pname = await albion_find_player(s, player)
            if not pid: return await interaction.followup.send(f"❌ **{player}** не найден.")
            async with s.get(f"{ALBION_BASE}/players/{pid}") as r: p = await r.json()
        kf  = p.get("KillFame", 0)
        df  = p.get("DeathFame", 0)
        pve = p.get("LifetimeStatistics", {}).get("PvE", {}).get("Total", 0)
        kd  = round(kf / df, 2) if df else "∞"
        gid = interaction.guild_id
        e = build_embed(C.INFO)
        e.set_author(name=pname, icon_url=f"https://render.albiononline.com/v1/player/{pname}/avatar?size=40")
        e.add_field(name=t(gid,"guild"),      value=p.get("GuildName") or "—",     inline=True)
        e.add_field(name=t(gid,"alliance"),   value=p.get("AllianceName") or "—",  inline=True)
        e.add_field(name=t(gid,"kd"),         value=f"**{kd}**",                   inline=True)
        e.add_field(name=t(gid,"kill_fame"),  value=f"**{kf:,}**",                 inline=True)
        e.add_field(name=t(gid,"death_fame"), value=f"**{df:,}**",                 inline=True)
        e.add_field(name=t(gid,"pve_fame"),   value=f"**{pve:,}**",                inline=True)
        e.set_footer(text=f"Albion Online · {ALBION_SERVER_NAMES.get('eu','EU')}")
        view = AlbionPlayerView(pname)
        await interaction.followup.send(embed=e, view=view)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="kills")
@app_commands.describe(player="Ник игрока")
@cooldown(5)
async def kills(interaction: discord.Interaction, player: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            pid, pname = await albion_find_player(s, player)
            if not pid: return await interaction.followup.send(f"❌ **{player}** не найден.")
            async with s.get(f"{ALBION_BASE}/players/{pid}/kills?limit=5") as r: evs = await r.json()
        if not evs: return await interaction.followup.send(f"No recent kills for **{pname}**.")
        gid = interaction.guild_id
        total = sum(ev.get("TotalVictimKillFame", 0) for ev in evs[:5])
        e = build_embed(C.DANGER, description=f"**{total:,}** fame from last {len(evs[:5])} kills")
        e.set_author(name=f"{pname} — {t(gid,'last_kills')}")
        for ev in evs[:5]:
            v    = ev.get("Victim", {})
            fame = ev.get("TotalVictimKillFame", 0)
            date = ev.get("TimeStamp", "")[:10]
            e.add_field(
                name=f"{v.get('Name','?')} · {date}",
                value=f"**{fame:,}** fame",
                inline=True
            )
        e.set_footer(text="Albion Online · EU")
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="deaths")
@app_commands.describe(player="Ник игрока")
@cooldown(5)
async def deaths(interaction: discord.Interaction, player: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            pid, pname = await albion_find_player(s, player)
            if not pid: return await interaction.followup.send(f"❌ **{player}** не найден.")
            async with s.get(f"{ALBION_BASE}/players/{pid}/deaths?limit=5") as r: evs = await r.json()
        if not evs: return await interaction.followup.send(f"No recent deaths for **{pname}**.")
        gid = interaction.guild_id
        total = sum(ev.get("TotalVictimKillFame", 0) for ev in evs[:5])
        e = build_embed(C.MUTED, description=f"**{total:,}** fame lost in last {len(evs[:5])} deaths")
        e.set_author(name=f"{pname} — {t(gid,'last_deaths')}")
        for ev in evs[:5]:
            k    = ev.get("Killer", {})
            fame = ev.get("TotalVictimKillFame", 0)
            date = ev.get("TimeStamp", "")[:10]
            e.add_field(
                name=f"Killed by: {k.get('Name','?')} · {date}",
                value=f"**{fame:,}** fame lost",
                inline=True
            )
        e.set_footer(text="Albion Online · EU")
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="guild")
@app_commands.describe(name="Гильдия")
@cooldown(10)
async def guild_cmd(interaction: discord.Interaction, name: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{ALBION_BASE}/search?q={name}") as r: guilds = (await r.json()).get("guilds",[])
            if not guilds: return await interaction.followup.send(f"❌ **{name}** не найдена.")
            gid = guilds[0]["Id"]
            async with s.get(f"{ALBION_BASE}/guilds/{gid}") as r: gdata = await r.json()
            async with s.get(f"{ALBION_BASE}/guilds/{gid}/members") as r: members = await r.json()
        e = build_embed(C.INFO)
        e.add_field(name="Участники", value=str(len(members)), inline=True)
        top = sorted(members, key=lambda m: m.get("KillFame",0), reverse=True)[:5]
        lines = [f"{i+1}. **{m.get('Name','?')}** — {m.get('KillFame',0):,}" for i,m in enumerate(top)]
        if lines: e.add_field(name="🏆 Топ по Fame", value="\n".join(lines), inline=False)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="battle")
@cooldown(15)
async def battle(interaction: discord.Interaction):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{ALBION_BASE}/battles?sort=recent&limit=5") as r: battles = await r.json()
        e = build_embed(C.DANGER, description=f"Last {len(battles[:5])} battles")
        e.set_author(name="Recent ZvZ Battles")
        for b in battles[:5]:
            guilds   = list(b.get("Guilds", {}).keys())[:3]
            name_str = " vs ".join(guilds) if guilds else "Open World"
            kills    = b.get("TotalKills", 0)
            fame     = b.get("TotalFame", 0)
            date     = b.get("StartTime", "")[:10]
            e.add_field(
                name=f"{name_str} · {date}",
                value=f"Kills: **{kills}** · Fame: **{fame:,}**",
                inline=False
            )
        e.set_footer(text="Albion Online · EU")
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="compare")
@app_commands.describe(player1="Игрок 1", player2="Игрок 2")
@cooldown(10)
async def compare(interaction: discord.Interaction, player1: str, player2: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            p1id,p1name = await albion_find_player(s,player1); p2id,p2name = await albion_find_player(s,player2)
            if not p1id: return await interaction.followup.send(f"❌ **{player1}** не найден.")
            if not p2id: return await interaction.followup.send(f"❌ **{player2}** не найден.")
            async with s.get(f"{ALBION_BASE}/players/{p1id}") as r: d1 = await r.json()
            async with s.get(f"{ALBION_BASE}/players/{p2id}") as r: d2 = await r.json()
        def kd(d): kf,df=d.get("KillFame",0),d.get("DeathFame",0); return round(kf/df,2) if df else float("inf")
        kf1,kf2 = d1.get("KillFame",0),d2.get("KillFame",0)
        def w(a,b): return ("✅","❌") if a>b else (("❌","✅") if b>a else ("🟡","🟡"))
        wf1,wf2 = w(kf1,kf2)
        e = build_embed(C.INFO)
        e.add_field(name=f"{wf1} {p1name}", value=f"Fame: **{kf1:,}**\nK/D: **{kd(d1)}**\n{d1.get('GuildName') or '—'}", inline=True)
        e.add_field(name="VS", value="​", inline=True)
        e.add_field(name=f"{wf2} {p2name}", value=f"Fame: **{kf2:,}**\nK/D: **{kd(d2)}**\n{d2.get('GuildName') or '—'}", inline=True)
        e.set_footer(text=f"Преимущество: {p1name if kf1>kf2 else p2name if kf2>kf1 else 'Ничья'}")
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="history")
@app_commands.describe(player="Ник игрока")
@cooldown(10)
async def history(interaction: discord.Interaction, player: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            pid,pname = await albion_find_player(s,player)
            if not pid: return await interaction.followup.send(f"❌ **{player}** не найден.")
            async with s.get(f"{ALBION_BASE}/players/{pid}/kills?limit=50") as r: ak = await r.json()
            async with s.get(f"{ALBION_BASE}/players/{pid}/deaths?limit=50") as r: ad = await r.json()
        cutoff = datetime.datetime.utcnow()-timedelta(days=7)
        def recent(evs):
            out=[]
            for ev in evs:
                try:
                    if datetime.datetime.fromisoformat(ev.get("TimeStamp","")[:19])>=cutoff: out.append(ev)
                except: pass
            return out
        wk,wd = recent(ak),recent(ad)
        fame = sum(e.get("TotalVictimKillFame",0) for e in wk)
        kd_week  = round(len(wk) / len(wd), 2) if wd else "∞"
        activity = "`✓ ACTIVE`" if len(wk) > 20 else "`▲ NORMAL`" if len(wk) > 5 else "`○ QUIET`"
        gid = interaction.guild_id
        e = build_embed(C.SUCCESS)
        e.set_author(name=f"{pname} — 7 days")
        e.add_field(name=t(gid,"kills"),    value=f"**{len(wk)}**",  inline=True)
        e.add_field(name=t(gid,"deaths"),   value=f"**{len(wd)}**",  inline=True)
        e.add_field(name=t(gid,"kd"),       value=f"**{kd_week}**",  inline=True)
        e.add_field(name="Fame",            value=f"**{fame:,}**",   inline=True)
        e.add_field(name=t(gid,"activity"), value=activity,          inline=True)
        if wk:
            victims = {}
            for ev in wk:
                n = ev.get("Victim", {}).get("Name", "?")
                victims[n] = victims.get(n, 0) + 1
            top = max(victims, key=victims.get)
            e.add_field(name=t(gid,"fav_target"), value=f"**{top}** × {victims[top]}", inline=True)
        e.set_footer(text="Albion Online · EU · 7 days")
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="rs")
@app_commands.describe(username="OSRS ник")
@cooldown(5)
async def rs(interaction: discord.Interaction, username: str):
    await interaction.response.defer()
    skills = ["Overall","Attack","Defence","Strength","Hitpoints","Ranged","Prayer","Magic","Cooking","Woodcutting","Fletching","Fishing","Firemaking","Crafting","Smithing","Mining"]
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://secure.runescape.com/m=hiscore_oldschool/index_lite.ws?player={username}") as r:
                if r.status!=200: return await interaction.followup.send(f"❌ **{username}** не найден.")
                lines = (await r.text()).strip().split("\n")
        e = build_embed(0xB5651D)
        overall = lines[0].split(",")
        e.add_field(name="Total Level", value=overall[1], inline=True)
        e.add_field(name="Total XP", value=f"{int(overall[2]):,}", inline=True)
        top=""
        for i in range(1,min(9,len(lines))):
            p=lines[i].split(",")
            if len(p)>=2 and int(p[1])>1: top+=f"**{skills[i]}**: {p[1]}\n"
        if top: e.add_field(name="Навыки",value=top,inline=False)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="mc")
@app_commands.describe(address="IP или домен")
@cooldown(10)
async def mc(interaction: discord.Interaction, address: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.mcstatus.io/v2/status/java/{address}") as r: d = await r.json()
        if not d.get("online"): return await interaction.followup.send(f"🔴 **{address}** оффлайн.")
        e = build_embed(C.SUCCESS)
        e.add_field(name="Игроки", value=f"{d['players']['online']}/{d['players']['max']}", inline=True)
        e.add_field(name="Версия", value=d.get("version",{}).get("name_clean","?"), inline=True)
        motd = d.get("motd",{}).get("clean","")
        if motd: e.add_field(name="MOTD",value=motd[:200],inline=False)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PREMIUM COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="ai")
@app_commands.describe(question="Вопрос")
@cooldown(5)
async def ai_cmd(interaction: discord.Interaction, question: str):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    try:
        answer = await ask_ai(question)
        e = build_embed(C.PREMIUM, description=answer[:4000])
        e.set_author(name="Witness AI", icon_url=interaction.user.display_avatar.url)
        e.add_field(name="Query", value=f"`{question[:100]}`", inline=False)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="summarize")
@app_commands.describe(count="Сообщений (макс 50)")
@cooldown(30)
async def summarize(interaction: discord.Interaction, count: int = 20):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    msgs = []
    async for msg in interaction.channel.history(limit=min(count,50)):
        if not msg.author.bot: msgs.append(f"{msg.author.display_name}: {msg.content}")
    msgs.reverse()
    try:
        summary = await ask_ai("\n".join(msgs), system="Summarize this Discord chat in 3-5 bullet points. Be concise.")
        sm = build_embed(0x5865F2, description=summary, color=C.INFO)
        sm.set_author(name=f"Summary — {count} messages")
        await interaction.followup.send(embed=sm)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="roast")
@app_commands.describe(member="Кого роастить")
@cooldown(10)
async def roast(interaction: discord.Interaction, member: discord.Member):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    roles=[r.name for r in member.roles[1:]]
    days=(datetime.datetime.utcnow()-member.joined_at.replace(tzinfo=None)).days
    try:
        text = await ask_ai(f"Funny 2-3 sentence roast: Name={member.display_name}, Roles={','.join(roles) or 'None'}, Days={days}. Playful, not offensive.", system="Write friendly roasts for Discord.")
        e = build_embed(0x5865F2, description=text, color=0xFF6B35, thumbnail=member.display_avatar.url)
        e.set_author(name=f"Roast: {member.display_name}", icon_url=member.display_avatar.url)
        e.set_thumbnail(url=member.display_avatar.url)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="giveaway")
@app_commands.describe(prize="Приз", duration="Минут")
async def giveaway(interaction: discord.Interaction, prize: str, duration: int = 60):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    e = build_embed(C.SUCCESS, description=f"**Prize:** {prize}\n🎮 — react to enter\n⏰ **{duration} min**")
    e.set_author(name="GIVEAWAY")
    await interaction.response.send_message(embed=e)
    msg = await interaction.original_response(); await msg.add_reaction("🎮")
    await asyncio.sleep(duration*60)
    msg = await interaction.channel.fetch_message(msg.id)
    reaction = discord.utils.get(msg.reactions, emoji="🎮")
    users = [u async for u in reaction.users() if not u.bot]
    winner = random.choice(users) if users else None
    await interaction.channel.send(f"🎊 {winner.mention} выиграл **{prize}**!" if winner else "😢 Никто не участвовал.")

@bot.tree.command(name="val")
@app_commands.describe(username="Riot ID (Player#TAG)")
@cooldown(10)
async def val(interaction: discord.Interaction, username: str):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    if "#" not in username: return await interaction.followup.send("❌ Формат: **Name#TAG**")
    name,tag = username.split("#",1)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.henrikdev.xyz/valorant/v2/mmr/eu/{name}/{tag}", headers={"Authorization":HENRIK_KEY} if HENRIK_KEY else {}) as r: d = await r.json()
        if d.get("status")!=200: return await interaction.followup.send(f"❌ **{username}** не найден.")
        data = d["data"]
        rank_name = data.get("currenttierpatched", "Unranked")
        rr   = data.get("ranking_in_tier", 0)
        peak = data.get("highest_rank", {}).get("patched_tier", "?")
        e = build_embed(0xFF4655)
        e.set_author(name=username)
        e.add_field(name="Rank", value=f"**{rank_name}**", inline=True)
        e.add_field(name="RR",   value=f"**{rr}/100**",    inline=True)
        e.add_field(name="Peak", value=f"**{peak}**",      inline=True)
        e.set_footer(text="Valorant")
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="cs2")
@app_commands.describe(steam_id="Steam ID64 или vanity name")
@cooldown(10)
async def cs2(interaction: discord.Interaction, steam_id: str):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    if not STEAM_KEY: return await interaction.followup.send("❌ Функция временно недоступна.")
    if not steam_id.isdigit():
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.steampowered.com/ISteamUser/ResolveVanityURL/v1/?key={STEAM_KEY}&vanityurl={steam_id}") as r:
                steam_id = (await r.json()).get("response",{}).get("steamid",steam_id)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.steampowered.com/ISteamUserStats/GetUserStatsForGame/v2/?appid=730&key={STEAM_KEY}&steamid={steam_id}") as r:
                sd = {s["name"]:s["value"] for s in (await r.json()).get("playerstats",{}).get("stats",[])}
        kills,deaths,wins,hs = sd.get("total_kills",0),sd.get("total_deaths",0),sd.get("total_wins",0),sd.get("total_kills_headshot",0)
        kd     = round(kills / deaths, 2) if deaths else "∞"
        hs_pct = round(hs / kills * 100, 1) if kills else 0
        e = build_embed(0xF0A500)
        e.set_author(name=f"CS2 — {steam_id}")
        e.add_field(name="K/D",    value=f"**{kd}**",        inline=True)
        e.add_field(name="Kills",  value=f"**{kills:,}**",   inline=True)
        e.add_field(name="Wins",   value=f"**{wins:,}**",    inline=True)
        e.add_field(name="HS%",    value=f"**{hs_pct}%**",   inline=True)
        e.set_footer(text="CS2 · Steam")
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="lol")
@app_commands.describe(summoner="Summoner name", region="Регион (euw1, na1...)")
@cooldown(10)
async def lol(interaction: discord.Interaction, summoner: str, region: str = "euw1"):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    if not RIOT_KEY: return await interaction.followup.send("❌ Функция временно недоступна.")
    headers={"X-Riot-Token":RIOT_KEY}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://{region}.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner}",headers=headers) as r:
                if r.status!=200: return await interaction.followup.send(f"❌ **{summoner}** не найден.")
                sid = (await r.json())["id"]
            async with s.get(f"https://{region}.api.riotgames.com/lol/league/v4/entries/by-summoner/{sid}",headers=headers) as r:
                entries = await r.json()
        e = build_embed(0xC89B3C)
        e.set_author(name=summoner)
        if not entries:
            e.description = "Unranked this season."
        for en in entries:
            w, l = en["wins"], en["losses"]
            wr   = round(w / (w + l) * 100, 1) if (w + l) else 0
            e.add_field(
                name=en["queueType"].replace("_", " ").title(),
                value=(
                    f"**{en['tier']} {en['rank']}** · {en['leaguePoints']} LP\n"
                    f"{w}W / {l}L · **{wr}%** WR"
                ),
                inline=True
            )
        e.set_footer(text=f"League of Legends · {region.upper()}")
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="lostark")
@app_commands.describe(character="Имя персонажа")
@cooldown(10)
async def lostark(interaction: discord.Interaction, character: str):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    if not LOSTARK_KEY: return await interaction.followup.send("❌ Функция временно недоступна.")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://developer-lostark.game.onstove.com/characters/{character}/siblings", headers={"Authorization":f"bearer {LOSTARK_KEY}"}) as r:
                if r.status!=200: return await interaction.followup.send(f"❌ **{character}** не найден.")
                chars = await r.json()
        e = build_embed(0x3D9BD4)
        for c in chars[:8]: e.add_field(name=c.get("CharacterName","?"), value=f"{c.get('CharacterClassName','?')}\niLvl: **{c.get('ItemMaxLevel','?')}**", inline=True)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PRO — BLACKMARKET v2
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BM_ITEMS = {
    # ══ AXES ══════════════════════════════════════════════════
    "battleaxe":      {"Battleaxe":           "T{t}_MAIN_AXE{e}"},
    "greataxe":       {"Greataxe":            "T{t}_2H_GREATAXE{e}"},
    "halberd":        {"Halberd":             "T{t}_2H_HALBERD{e}"},
    "bearpaws":       {"Bear Paws":           "T{t}_2H_DUALAXE_KEEPER{e}"},
    "infernalscythe": {"Infernal Scythe":     "T{t}_2H_SCYTHE_HELL{e}"},
    "carrioncaller":  {"Carrioncaller":       "T{t}_2H_HALBERD_MORGANA{e}"},
    "realmbreaker":   {"Realmbreaker":        "T{t}_2H_REALMBREAKER{e}"},
    # ══ SWORDS ════════════════════════════════════════════════
    "broadsword":     {"Broadsword":          "T{t}_MAIN_SWORD{e}"},
    "claymore":       {"Claymore":            "T{t}_2H_CLAYMORE{e}"},
    "clarentblade":   {"Clarent Blade":       "T{t}_MAIN_BROADSWORD{e}"},
    "dualswords":     {"Dual Swords":         "T{t}_2H_DUALSWORD{e}"},
    "carvingsword":   {"Carving Sword":       "T{t}_2H_CLEAVER_HELL{e}"},
    "galatinepair":   {"Galatine Pair":       "T{t}_2H_DUALSCIMITAR_UNDEAD{e}"},
    # ══ MACES ═════════════════════════════════════════════════
    "mace":           {"Mace":               "T{t}_MAIN_MACE{e}"},
    "heavymace":      {"Heavy Mace":         "T{t}_2H_HEAVYMACE{e}"},
    "morningstar":    {"Morning Star":       "T{t}_MAIN_MORNINGSTAR{e}"},
    "bedrockmace":    {"Bedrock Mace":       "T{t}_MAIN_ROCKMACE_KEEPER{e}"},
    "incubusmace":    {"Incubus Mace":       "T{t}_MAIN_INCUBUS{e}"},
    "camlannmace":    {"Camlann Mace":       "T{t}_MAIN_MACE_HELL{e}"},
    # ══ HAMMERS ═══════════════════════════════════════════════
    "hammer":         {"Hammer":             "T{t}_2H_HAMMER{e}"},
    "polehammer":     {"Polehammer":         "T{t}_2H_POLEHAMMER{e}"},
    "greathammer":    {"Great Hammer":       "T{t}_2H_HAMMER_UNDEAD{e}"},
    "forgehammers":   {"Forge Hammers":      "T{t}_2H_DUALHAMMER_HELL{e}"},
    "tombhammer":     {"Tombhammer":         "T{t}_2H_TOMBHAMMER{e}"},
    "grovekeeper":    {"Grovekeeper":        "T{t}_2H_GROVEKEEPER{e}"},
    # ══ WAR GLOVES ════════════════════════════════════════════
    "brawlergloves":  {"Brawler Gloves":     "T{t}_2H_KNUCKLES{e}"},
    "battlebracers":  {"Battle Bracers":     "T{t}_MAIN_GAUNTLET{e}"},
    "spikedgauntlet": {"Spiked Gauntlet":    "T{t}_MAIN_SPIKEDGAUNTLET{e}"},
    "ursinemaulers":  {"Ursine Maulers":     "T{t}_2H_URSINEHANDSCLAW{e}"},
    "hellfires":      {"Hellfire Hands":     "T{t}_2H_KNUCKLES_HELL{e}"},
    "ravenstrike":    {"Ravenstrike Cestus": "T{t}_MAIN_RAPIER_MORGANA{e}"},
    "fistsofavalon":  {"Fists of Avalon":    "T{t}_2H_FISTOFAVALON{e}"},
    # ══ CROSSBOWS ═════════════════════════════════════════════
    "crossbow":       {"Crossbow":           "T{t}_2H_CROSSBOW{e}"},
    "heavycrossbow":  {"Heavy Crossbow":     "T{t}_2H_HEAVYCROSSBOW{e}"},
    "boltcasters":    {"Boltcasters":        "T{t}_2H_DUALCROSSBOW_HELL{e}"},
    "lightcrossbow":  {"Light Crossbow":     "T{t}_MAIN_LIGHTCROSSBOW{e}"},
    "weepingrepeater":{"Weeping Repeater":   "T{t}_2H_WEEPINGREPEAT{e}"},
    "siegebow":       {"Siegebow":           "T{t}_2H_CROSSBOWLARGE_MORGANA{e}"},
    # ══ BOWS ══════════════════════════════════════════════════
    "bow":            {"Bow":               "T{t}_2H_BOW{e}"},
    "warbow":         {"Warbow":            "T{t}_2H_WARBOW{e}"},
    "longbow":        {"Longbow":           "T{t}_2H_LONGBOW{e}"},
    "whisperingbow":  {"Whispering Bow":    "T{t}_2H_WHISPERING_BOW{e}"},
    "bowofbadon":     {"Bow of Badon":      "T{t}_2H_BOW_KEEPER{e}"},
    "wailingbow":     {"Wailing Bow":       "T{t}_2H_BOW_HELL{e}"},
    "mistpiercer":    {"Mistpiercer":       "T{t}_2H_MISTCALLER{e}"},
    # ══ DAGGERS ═══════════════════════════════════════════════
    "dagger":         {"Dagger":            "T{t}_MAIN_DAGGER{e}"},
    "daggerpair":     {"Dagger Pair":       "T{t}_2H_DAGGERPAIR{e}"},
    "claws":          {"Claws":             "T{t}_2H_CLAWS{e}"},
    "bloodletter":    {"Bloodletter":       "T{t}_MAIN_BLOODLETTER{e}"},
    "demonfang":      {"Demonfang":         "T{t}_MAIN_DEMONFANG{e}"},
    "deathgivers":    {"Deathgivers":       "T{t}_2H_DUALSICKLE_UNDEAD{e}"},
    "bridledfury":    {"Bridled Fury":      "T{t}_2H_BRIDLEDFURY{e}"},
    # ══ SPEARS ════════════════════════════════════════════════
    "spear":          {"Spear":             "T{t}_MAIN_SPEAR{e}"},
    "pike":           {"Pike":              "T{t}_2H_PIKE{e}"},
    "glaive":         {"Glaive":            "T{t}_2H_GLAIVE{e}"},
    "heronspear":     {"Heron Spear":       "T{t}_MAIN_SPEAR_KEEPER{e}"},
    "spirithunter":   {"Spirit Hunter":     "T{t}_2H_HARPOON_HELL{e}"},
    "trinityspear":   {"Trinity Spear":     "T{t}_2H_TRIDENT_UNDEAD{e}"},
    "daybreaker":     {"Daybreaker":        "T{t}_2H_DAYBREAKER{e}"},
    # ══ QUARTERSTAFFS ═════════════════════════════════════════
    "quarterstaff":   {"Quarterstaff":      "T{t}_2H_QUARTERSTAFF{e}"},
    "ironcladstaff":  {"Iron-Clad Staff":   "T{t}_2H_IRONCLADSTAFF{e}"},
    "doublebladed":   {"Double Bladed Staff":"T{t}_2H_DOUBLEBLADEDSTAFF{e}"},
    "soulscythe":     {"Soulscythe":        "T{t}_2H_TWINSCYTHE_HELL{e}"},
    "grailseeker":    {"Grailseeker":       "T{t}_2H_GRAILSEEKER{e}"},
    "sweepingstaff":  {"Sweeping Staff":    "T{t}_2H_SWEEINGSTAFF{e}"},
    # ══ NATURE STAFF ══════════════════════════════════════════
    "naturestaff":    {"Nature Staff":      "T{t}_MAIN_NATURESTAFF{e}"},
    "wildstaff":      {"Wild Staff":        "T{t}_2H_WILDSTAFF{e}"},
    "greatnature":    {"Great Nature Staff":"T{t}_2H_NATURESTAFFGREAT{e}"},
    "druidicstaff":   {"Druidic Staff":     "T{t}_MAIN_NATURESTAFF_KEEPER{e}"},
    "blightstaff":    {"Blight Staff":      "T{t}_2H_BLIGHTSTAFF{e}"},
    "ironrootstaff":  {"Ironroot Staff":    "T{t}_2H_IRONROOTSTAFF{e}"},
    # ══ FIRE STAFF ════════════════════════════════════════════
    "firestaff":      {"Fire Staff":        "T{t}_MAIN_FIRESTAFF{e}"},
    "greatfire":      {"Great Fire Staff":  "T{t}_2H_FIRESTAFF{e}"},
    "infernalstaff":  {"Infernal Staff":    "T{t}_2H_INFERNOSTAFF{e}"},
    "wildfirestaff":  {"Wildfire Staff":    "T{t}_MAIN_FIRESTAFF_KEEPER{e}"},
    "brimstonestaff": {"Brimstone Staff":   "T{t}_2H_FIRESTAFF_HELL{e}"},
    "blazingstaff":   {"Blazing Staff":     "T{t}_2H_BLAZINGSTAFF{e}"},
    # ══ HOLY STAFF ════════════════════════════════════════════
    "holystaff":      {"Holy Staff":        "T{t}_MAIN_HOLYSTAFF{e}"},
    "greatholly":     {"Great Holy Staff":  "T{t}_2H_HOLYSTAFF{e}"},
    "divinestaff":    {"Divine Staff":      "T{t}_2H_DIVINESTAFF{e}"},
    "lifetouchstaff": {"Lifetouch Staff":   "T{t}_MAIN_LIFETOUCH{e}"},
    "fallenstaff":    {"Fallen Staff":      "T{t}_2H_HOLYSTAFF_HELL{e}"},
    "redemptionstaff":{"Redemption Staff":  "T{t}_2H_REDEMPTIONSTAFF{e}"},
    # ══ ARCANE STAFF ══════════════════════════════════════════
    "arcanestaff":    {"Arcane Staff":      "T{t}_MAIN_ARCANESTAFF{e}"},
    "greatarcane":    {"Great Arcane Staff":"T{t}_2H_ARCANESTAFF{e}"},
    "enigmaticstaff": {"Enigmatic Staff":   "T{t}_2H_ENIGMATICSTAFF{e}"},
    "witchworkstaff": {"Witchwork Staff":   "T{t}_MAIN_ARCANESTAFF_UNDEAD{e}"},
    "evensong":       {"Evensong":          "T{t}_2H_EVENSONG{e}"},
    "occultstaff":    {"Occult Staff":      "T{t}_2H_ARCANESTAFF_HELL{e}"},
    # ══ FROST STAFF ═══════════════════════════════════════════
    "froststaff":     {"Frost Staff":       "T{t}_MAIN_FROSTSTAFF{e}"},
    "greatfrost":     {"Great Frost Staff": "T{t}_2H_FROSTSTAFF{e}"},
    "glacialstaff":   {"Glacial Staff":     "T{t}_2H_GLACIALSTAFF{e}"},
    "hoarfroststaff": {"Hoarfrost Staff":   "T{t}_MAIN_FROSTSTAFF_KEEPER{e}"},
    "iciclestaff":    {"Icicle Staff":      "T{t}_2H_ICESTAFFFIRE{e}"},
    "permafrost":     {"Permafrost Staff":  "T{t}_2H_PERMAFROSTSTAFF{e}"},
    # ══ CURSED STAFF ══════════════════════════════════════════
    "cursedstaff":    {"Cursed Staff":      "T{t}_MAIN_CURSEDSTAFF{e}"},
    "greatcursed":    {"Great Cursed Staff":"T{t}_2H_CURSEDSTAFF{e}"},
    "demonicstaff":   {"Demonic Staff":     "T{t}_2H_DEMONICSTAFF{e}"},
    "cursedskull":    {"Cursed Skull":      "T{t}_MAIN_CURSEDSTAFF_UNDEAD{e}"},
    "lifecursestaff": {"Lifecurse Staff":   "T{t}_MAIN_LIFECURSESTAFF{e}"},
    "damnationstaff": {"Damnation Staff":   "T{t}_2H_CURSEDSTAFF_MORGANA{e}"},
    # ══ OFF-HAND ══════════════════════════════════════════════
    "shield":         {"Shield":            "T{t}_OFFHAND_SHIELD{e}"},
    "sarcophagus":    {"Sarcophagus":       "T{t}_OFFHAND_SHIELD_UNDEAD{e}"},
    "caitiffshield":  {"Caitiff Shield":    "T{t}_OFFHAND_SHIELD_HELL{e}"},
    "facebreaker":    {"Facebreaker":       "T{t}_OFFHAND_FACEBREAKER{e}"},
    "torch":          {"Torch":             "T{t}_OFFHAND_TORCH{e}"},
    "mistcaller":     {"Mistcaller":        "T{t}_OFFHAND_MISTCALLER{e}"},
    "leeringcane":    {"Leering Cane":      "T{t}_OFFHAND_LEERINGCANE{e}"},
    "taproot":        {"Taproot":           "T{t}_OFFHAND_TAPROOT{e}"},
    "muisak":         {"Muisak":            "T{t}_OFFHAND_MUISAK{e}"},
    "cryptcandle":    {"Cryptcandle":       "T{t}_OFFHAND_CRYPTCANDLE{e}"},
    "tomeofspells":   {"Tome of Spells":    "T{t}_OFFHAND_BOOK{e}"},
    # ══ PLATE ARMOR ═══════════════════════════════════════════
    "soldierhelm":    {"Soldier Helmet":    "T{t}_HEAD_PLATE_SET1{e}"},
    "soldierarmor":   {"Soldier Armor":     "T{t}_ARMOR_PLATE_SET1{e}"},
    "soldierboots":   {"Soldier Boots":     "T{t}_SHOES_PLATE_SET1{e}"},
    "knighthelm":     {"Knight Helmet":     "T{t}_HEAD_PLATE_SET2{e}"},
    "knightarmor":    {"Knight Armor":      "T{t}_ARMOR_PLATE_SET2{e}"},
    "knightboots":    {"Knight Boots":      "T{t}_SHOES_PLATE_SET2{e}"},
    "guardianhelm":   {"Guardian Helmet":   "T{t}_HEAD_PLATE_SET3{e}"},
    "guardianarmor":  {"Guardian Armor":    "T{t}_ARMOR_PLATE_SET3{e}"},
    "guardianboots":  {"Guardian Boots":    "T{t}_SHOES_PLATE_SET3{e}"},
    "graveguardhelm": {"Graveguard Helmet": "T{t}_HEAD_PLATE_UNDEAD{e}"},
    "graveguardarmor":{"Graveguard Armor":  "T{t}_ARMOR_PLATE_UNDEAD{e}"},
    "graveguardboots":{"Graveguard Boots":  "T{t}_SHOES_PLATE_UNDEAD{e}"},
    "judicatorhelm":  {"Judicator Helmet":  "T{t}_HEAD_PLATE_HELL{e}"},
    "judicatorarmor": {"Judicator Armor":   "T{t}_ARMOR_PLATE_HELL{e}"},
    "judicatorboots": {"Judicator Boots":   "T{t}_SHOES_PLATE_HELL{e}"},
    "demonhelm":      {"Demon Helmet":      "T{t}_HEAD_PLATE_MORGANA{e}"},
    "demonarmor":     {"Demon Armor":       "T{t}_ARMOR_PLATE_MORGANA{e}"},
    "demonboots":     {"Demon Boots":       "T{t}_SHOES_PLATE_MORGANA{e}"},
    # ══ LEATHER ARMOR ═════════════════════════════════════════
    "hunterhelm":     {"Hunter Hood":       "T{t}_HEAD_LEATHER_SET1{e}"},
    "hunterjacket":   {"Hunter Jacket":     "T{t}_ARMOR_LEATHER_SET1{e}"},
    "huntershoes":    {"Hunter Shoes":      "T{t}_SHOES_LEATHER_SET1{e}"},
    "assassinhelm":   {"Assassin Hood":     "T{t}_HEAD_LEATHER_SET2{e}"},
    "assassinjacket": {"Assassin Jacket":   "T{t}_ARMOR_LEATHER_SET2{e}"},
    "assassinshoes":  {"Assassin Shoes":    "T{t}_SHOES_LEATHER_SET2{e}"},
    "mercenaryhelm":  {"Mercenary Hood":    "T{t}_HEAD_LEATHER_SET3{e}"},
    "mercenaryarmor": {"Mercenary Jacket":  "T{t}_ARMOR_LEATHER_SET3{e}"},
    "mercenaryboots": {"Mercenary Shoes":   "T{t}_SHOES_LEATHER_SET3{e}"},
    "hellionhelm":    {"Hellion Hood":      "T{t}_HEAD_LEATHER_UNDEAD{e}"},
    "hellionjacket":  {"Hellion Jacket":    "T{t}_ARMOR_LEATHER_UNDEAD{e}"},
    "hellionshoes":   {"Hellion Shoes":     "T{t}_SHOES_LEATHER_UNDEAD{e}"},
    "specterhelm":    {"Specter Hood":      "T{t}_HEAD_LEATHER_HELL{e}"},
    "specterjacket":  {"Specter Jacket":    "T{t}_ARMOR_LEATHER_HELL{e}"},
    "spectershoes":   {"Specter Shoes":     "T{t}_SHOES_LEATHER_HELL{e}"},
    "mistwalkerhelm": {"Mistwalker Hood":   "T{t}_HEAD_LEATHER_MORGANA{e}"},
    "mistwalkerjacket":{"Mistwalker Jacket":"T{t}_ARMOR_LEATHER_MORGANA{e}"},
    "mistwalkershoes":{"Mistwalker Shoes":  "T{t}_SHOES_LEATHER_MORGANA{e}"},
    # ══ CLOTH ARMOR ═══════════════════════════════════════════
    "scholarcowl":    {"Scholar Cowl":      "T{t}_HEAD_CLOTH_SET1{e}"},
    "scholarrobe":    {"Scholar Robe":      "T{t}_ARMOR_CLOTH_SET1{e}"},
    "scholarsandals": {"Scholar Sandals":   "T{t}_SHOES_CLOTH_SET1{e}"},
    "clericcowl":     {"Cleric Cowl":       "T{t}_HEAD_CLOTH_SET2{e}"},
    "clericrobe":     {"Cleric Robe":       "T{t}_ARMOR_CLOTH_SET2{e}"},
    "clericsandals":  {"Cleric Sandals":    "T{t}_SHOES_CLOTH_SET2{e}"},
    "magecowl":       {"Mage Cowl":         "T{t}_HEAD_CLOTH_SET3{e}"},
    "magerobe":       {"Mage Robe":         "T{t}_ARMOR_CLOTH_SET3{e}"},
    "magesandals":    {"Mage Sandals":      "T{t}_SHOES_CLOTH_SET3{e}"},
    "cultistcowl":    {"Cultist Cowl":      "T{t}_HEAD_CLOTH_UNDEAD{e}"},
    "cultistrobe":    {"Cultist Robe":      "T{t}_ARMOR_CLOTH_UNDEAD{e}"},
    "cultistsandals": {"Cultist Sandals":   "T{t}_SHOES_CLOTH_UNDEAD{e}"},
    "feyscalehat":    {"Feyscale Hat":      "T{t}_HEAD_CLOTH_HELL{e}"},
    "feyscalerobe":   {"Feyscale Robe":     "T{t}_ARMOR_CLOTH_HELL{e}"},
    "feyscalesandals":{"Feyscale Sandals":  "T{t}_SHOES_CLOTH_HELL{e}"},
    # ══ BAGS ══════════════════════════════════════════════════
    "bag":            {"Bag":               "T{t}_BAG{e}"},
    "bagofinsight":   {"Bag of Insight":    "T{t}_BAG_INSIGHT{e}"},
}

# Группы для /blackmarket
BM_GROUPS = {
    "weapon": [
        "battleaxe","greataxe","halberd","bearpaws","infernalscythe","carrioncaller","realmbreaker",
        "broadsword","claymore","clarentblade","dualswords","carvingsword","galatinepair",
        "mace","heavymace","morningstar","bedrockmace","incubusmace","camlannmace",
        "hammer","polehammer","greathammer","forgehammers","tombhammer","grovekeeper",
        "brawlergloves","battlebracers","spikedgauntlet","ursinemaulers","hellfires","ravenstrike","fistsofavalon",
        "crossbow","heavycrossbow","boltcasters","lightcrossbow","weepingrepeater","siegebow",
        "bow","warbow","longbow","whisperingbow","bowofbadon","wailingbow","mistpiercer",
        "dagger","daggerpair","claws","bloodletter","demonfang","deathgivers","bridledfury",
        "spear","pike","glaive","heronspear","spirithunter","trinityspear","daybreaker",
        "quarterstaff","ironcladstaff","doublebladed","soulscythe","grailseeker","sweepingstaff",
        "naturestaff","wildstaff","greatnature","druidicstaff","blightstaff","ironrootstaff",
        "firestaff","greatfire","infernalstaff","wildfirestaff","brimstonestaff","blazingstaff",
        "holystaff","greatholly","divinestaff","lifetouchstaff","fallenstaff","redemptionstaff",
        "arcanestaff","greatarcane","enigmaticstaff","witchworkstaff","evensong","occultstaff",
        "froststaff","greatfrost","glacialstaff","hoarfroststaff","iciclestaff","permafrost",
        "cursedstaff","greatcursed","demonicstaff","cursedskull","lifecursestaff","damnationstaff",
    ],
    "offhand": [
        "shield","sarcophagus","caitiffshield","facebreaker",
        "torch","mistcaller","leeringcane","taproot","muisak","cryptcandle","tomeofspells",
    ],
    "armor_plate": [
        "soldierhelm","soldierarmor","soldierboots",
        "knighthelm","knightarmor","knightboots",
        "guardianhelm","guardianarmor","guardianboots",
        "graveguardhelm","graveguardarmor","graveguardboots",
        "judicatorhelm","judicatorarmor","judicatorboots",
        "demonhelm","demonarmor","demonboots",
    ],
    "armor_leather": [
        "hunterhelm","hunterjacket","huntershoes",
        "assassinhelm","assassinjacket","assassinshoes",
        "mercenaryhelm","mercenaryarmor","mercenaryboots",
        "hellionhelm","hellionjacket","hellionshoes",
        "specterhelm","specterjacket","spectershoes",
        "mistwalkerhelm","mistwalkerjacket","mistwalkershoes",
    ],
    "armor_cloth": [
        "scholarcowl","scholarrobe","scholarsandals",
        "clericcowl","clericrobe","clericsandals",
        "magecowl","magerobe","magesandals",
        "cultistcowl","cultistrobe","cultistsandals",
        "feyscalehat","feyscalerobe","feyscalesandals",
    ],
    "bag": ["bag","bagofinsight"],
}


def build_item_id(template, tier, enchant):
    return template.format(t=tier, e=f"@{enchant}" if enchant > 0 else "")


# ── Albion серверы ────────────────────────────────────────────
ALBION_SERVERS = {
    "eu":   "https://west.albion-online-data.com/api/v2",
    "us":   "https://east.albion-online-data.com/api/v2",
    "asia": "https://east.albion-online-data.com/api/v2",  # Asia uses east endpoint
}
ALBION_SERVER_NAMES = {"eu": "🇪🇺 Европа", "us": "🇺🇸 Америка", "asia": "🌏 Азия"}

# Все города кроме ЧР и Бреккилена
CITY_LOCATIONS = ["Bridgewatch", "Fort Sterling", "Lymhurst", "Martlock", "Thetford"]  # Caerleon исключён
CITY_NAMES_RU = {
    "Caerleon": "Кэрлеон",
    "Bridgewatch": "Бриджвотч",
    "Fort Sterling": "Форт Стерлинг",
    "Lymhurst": "Лимхёрст",
    "Martlock": "Мартлок",
    "Thetford": "Тетфорд",
    "Brecilien": "Бреккилен",
    "Black Market": "Чёрный рынок",
}

# Иконки предметов через Albion render API
def item_icon_url(item_id: str) -> str:
    return f"https://render.albiononline.com/v1/item/{item_id}.png?size=50"

async def fetch_bm_prices(category_keys: list, tier: int, server: str = "eu") -> list:
    """
    Возвращает список dict с детальными ценами по каждому городу.
    server: eu / us / asia
    """
    base_url = ALBION_SERVERS.get(server, ALBION_SERVERS["eu"])
    results = []

    # ВАЖНО: пробелы в названиях городов кодируем через %20, не +
    # Albion Data API чувствителен к этому
    all_locations = ",".join([
        "Black Market", "Brecilien",
        "Bridgewatch", "Fort Sterling",
        "Lymhurst", "Martlock", "Thetford"
    ])
    # URL-encode пробелы
    locations_param = all_locations.replace(" ", "%20")

    print(f"[BM DEBUG] Starting fetch: keys={len(category_keys)}, tier={tier}, server={server}")
    fetched = 0
    found = 0

    async with aiohttp.ClientSession() as s:
        for key in category_keys:
            item_data = BM_ITEMS.get(key)
            if not item_data: continue
            display, template = list(item_data.items())[0]

            for enchant in range(0, 5):
                item_id = build_item_id(template, tier, enchant)
                tier_label = f"{tier}.{enchant}"
                url = f"{base_url}/stats/prices/{item_id}?locations={locations_param}"

                try:
                    async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                        fetched += 1
                        if r.status != 200:
                            print(f"[BM DEBUG] {item_id}: HTTP {r.status}")
                            continue
                        prices = await r.json()
                except Exception as ex:
                    print(f"[BM DEBUG] {item_id}: request error: {ex}")
                    continue

                if not prices:
                    continue

                bm_price = 0
                brec_sell = 0
                brec_buy = 0
                city_data = {}
                bm_updated = ""   # дата обновления цены ЧР

                for p in prices:
                    city = p.get("city", "").strip()
                    sell = p.get("sell_price_min", 0) or 0
                    buy  = p.get("buy_price_max", 0) or 0
                    upd  = p.get("sell_price_min_date", "") or ""

                    if city == "Black Market":
                        if sell > 0:
                            bm_price = max(bm_price, sell)
                            bm_updated = upd  # сохраняем дату обновления
                    elif city == "Brecilien":
                        if sell > 0: brec_sell = sell
                        if buy > 0:  brec_buy  = buy
                    elif city in CITY_LOCATIONS:
                        if sell > 0 or buy > 0:
                            city_data[city] = {"sell": sell, "buy": buy}

                # Если нет цены на ЧР — предмет не торгуется там
                if bm_price == 0:
                    print(f"[BM DEBUG] {item_id}: no Black Market price, skip")
                    continue

                # Проверяем возраст данных
                data_age_hours = None
                data_stale = False
                if bm_updated:
                    try:
                        # Нормализуем разные форматы: 2026-04-27T14:23:00Z / +00:00 / без tz
                        upd_clean = bm_updated.strip()
                        if upd_clean.endswith("Z"):
                            upd_clean = upd_clean[:-1]  # убираем Z, делаем naive UTC
                        elif upd_clean.endswith("+00:00"):
                            upd_clean = upd_clean[:-6]  # убираем +00:00
                        elif "+" in upd_clean[10:]:
                            upd_clean = upd_clean[:upd_clean.rfind("+")]
                        upd_dt = datetime.datetime.fromisoformat(upd_clean)
                        data_age_hours = round(
                            (datetime.datetime.utcnow() - upd_dt).total_seconds() / 3600, 1
                        )
                        data_stale = data_age_hours > 3
                    except Exception as _date_ex:
                        print(f"[BM] Date parse error for '{bm_updated}': {_date_ex}")
                        data_age_hours = None
                        data_stale = False

                # Лучший город — минимальная цена продажи
                best_city = None
                best_city_sell = 9_999_999_999
                best_city_buy  = 0

                for city, pd in city_data.items():
                    sell = pd["sell"]
                    buy  = pd["buy"]
                    if sell > 0 and sell < best_city_sell:
                        best_city_sell = sell
                        best_city      = city
                        best_city_buy  = buy

                # Если нет цены в городах — используем ордер на покупку (buy)
                if not best_city:
                    for city, pd in city_data.items():
                        buy = pd["buy"]
                        if buy > 0 and buy < best_city_sell:
                            best_city_sell = buy
                            best_city      = city
                            best_city_buy  = buy

                city_sell_profit = bm_price - best_city_sell if best_city and best_city_sell < 9_999_999_999 else 0
                city_sell_pct    = round(city_sell_profit / best_city_sell * 100, 1) if city_sell_profit > 0 and best_city_sell > 0 else 0

                brec_price = brec_sell if brec_sell > 0 else brec_buy
                brec_is_buy_order = brec_buy > 0 and brec_sell == 0
                brec_profit = bm_price - brec_price if brec_price > 0 else 0
                brec_pct    = round(brec_profit / brec_price * 100, 1) if brec_price > 0 else 0

                found += 1
                print(f"[BM DEBUG] {item_id}: BM={bm_price} age={data_age_hours}h stale={data_stale} best_city={best_city}({best_city_sell if best_city_sell < 9_999_999_999 else 0}) profit={city_sell_profit}({city_sell_pct}%)")

                results.append({
                    "name":              f"{display} {tier_label}",
                    "display":           display,
                    "tier_label":        tier_label,
                    "item_id":           item_id,
                    "icon_url":          item_icon_url(item_id),
                    "bm":                bm_price,
                    "best_city":         best_city,
                    "best_city_sell":    best_city_sell if best_city_sell < 9_999_999_999 else 0,
                    "best_city_buy":     best_city_buy,
                    "city_profit":       city_sell_profit,
                    "city_pct":          city_sell_pct,
                    "city_data":         city_data,
                    "brec_price":        brec_price,
                    "brec_is_buy_order": brec_is_buy_order,
                    "brec_profit":       brec_profit,
                    "brec_pct":          brec_pct,
                    "data_age_hours":    data_age_hours,
                    "data_stale":        data_stale,
                })

    print(f"[BM DEBUG] Done: fetched={fetched}, results={found}")
    results.sort(key=lambda x: x["city_pct"], reverse=True)
    return results


@bot.tree.command(name="blackmarket", description="Albion: профит Чёрного рынка [Pro]")
@app_commands.describe(
    category="weapon/offhand/armor_plate/armor_leather/armor_cloth/bag или ключ предмета",
    tier="Тир: 6, 7 или 8",
    server="Сервер: eu / us / asia",
    sheets="Экспорт в Google Sheets: yes / no",
)
@cooldown(30)
async def blackmarket(
    interaction: discord.Interaction,
    category: str = "weapon",
    tier: int = 8,
    server: str = "eu",
    sheets: str = "no",
):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)
    await interaction.response.defer()

    if tier not in (6, 7, 8):
        return await interaction.followup.send("❌ Тир: 6, 7 или 8")
    if server not in ALBION_SERVERS:
        return await interaction.followup.send("❌ Сервер: eu / us / asia")

    cat_lower = category.lower()
    if cat_lower in BM_GROUPS:
        keys = BM_GROUPS[cat_lower]
        cat_label = cat_lower.capitalize()
    elif cat_lower in BM_ITEMS:
        keys = [cat_lower]
        cat_label = list(BM_ITEMS[cat_lower].keys())[0]
    else:
        avail = ", ".join(f"`{k}`" for k in list(BM_GROUPS.keys()) + list(BM_ITEMS.keys())[:8]) + "..."
        return await interaction.followup.send(f"❌ Неизвестная категория. Примеры: {avail}")

    server_name = ALBION_SERVER_NAMES[server]
    await interaction.followup.send(
        f"⏳ Загружаю цены **{cat_label} T{tier}** · {server_name}... (~20 сек)"
    )

    results = await fetch_bm_prices(keys, tier, server)

    if not results:
        return await interaction.channel.send("❌ Нет данных о ценах. Попробуй позже.")

    # ── Строим страницы с пагинацией (5 предметов на страницу) ──
    top = results[:20]
    stale_count = sum(1 for item in top if item.get("data_stale"))

    def build_bm_page(items_chunk: list, page_num: int, total_pages: int) -> discord.Embed:
        color = profit_color(items_chunk[0]["city_pct"] if items_chunk else 0)
        desc_lines = f"{server_name} · Sorted by profit %\n⚠️ BM prices are approximate — verify in game before selling!"
        if stale_count:
            desc_lines += f"\n⚠️ **{stale_count} items** with stale data (>3h)"
        e = build_embed(color, description=desc_lines)
        e.set_author(name=f"Black Market — {cat_label} T{tier}")
        e.set_footer(text=f"albion-online-data.com · {len(results)} items · p.{page_num}/{total_pages}")
        for item in items_chunk:
            city_ru = CITY_NAMES_RU.get(item["best_city"], item["best_city"]) if item["best_city"] else "—"
            age_h = item.get("data_age_hours")
            stale = item.get("data_stale", False)
            age_str = f"{'⚠️' if stale else '🕐'} {age_h}ч" if age_h is not None else "🕐 ?"

            if item["best_city"]:
                city_line = (
                    f"🏙️ **{city_ru}**: `{item['best_city_sell']:,}` → `{item['bm']:,}` "
                    f"(**+{item['city_pct']}%** / {item['city_profit']:,})"
                )
            else:
                city_line = "🏙️ Нет цены в городах"

            if item["brec_price"] > 0:
                brec_type = "ord" if item["brec_is_buy_order"] else "mkt"
                brec_line = (
                    f"🌿 Брек [{brec_type}]: `{item['brec_price']:,}` → `{item['bm']:,}` "
                    f"(**+{item['brec_pct']}%**)"
                )
            else:
                brec_line = "🌿 Брек: нет данных"

            icon = "⚠️" if stale else ("🟢" if item["city_pct"] >= 20 else "🟡" if item["city_pct"] >= 5 else "🔴")
            e.add_field(
                name=f"{icon} {item['name']} {age_str}",
                value=f"{city_line}\n{brec_line}",
                inline=False
            )
        if top:
            e.set_thumbnail(url=top[0]["icon_url"])
        return e

    chunks = [top[i:i+5] for i in range(0, len(top), 5)]
    pages = [build_bm_page(chunk, i+1, len(chunks)) for i, chunk in enumerate(chunks)]

    view = PaginatedView(pages)
    # Добавляем Select Menu для смены категории
    cat_view = BlackmarketCategoryView(tier, server, cat_lower)
    for item in cat_view.children:
        view.add_item(item)

    await interaction.channel.send(embed=pages[0], view=view)

    # ── Google Sheets export ───────────────────────────────────
    if sheets.lower() in ("yes", "да", "y"):
        if not GOOGLE_CREDS:
            await interaction.channel.send(
                "❌ Google Sheets: добавь `GOOGLE_CREDENTIALS` в .env\n"
                "Как получить: console.cloud.google.com → Service Accounts → Create Key (JSON) → скопируй содержимое одной строкой"
            )
            return
        if not SHEET_ID:
            await interaction.channel.send(
                "❌ Google Sheets: добавь `SHEET_ID` в .env\n"
                "Это ID из URL таблицы: `docs.google.com/spreadsheets/d/**ВОТ_ЭТО**/edit`"
            )
            return
        try:
            import gspread
            from google.oauth2.service_account import Credentials as GCredentials
            import json as _json

            # GOOGLE_CREDENTIALS должен быть JSON строкой или base64
            # НЕ пытаемся открывать как файл — в Railway это всегда строка
            raw = GOOGLE_CREDS.strip()

            creds_data = None
            if raw.startswith("{"):
                # Прямой JSON
                creds_data = _json.loads(raw)
            else:
                # Попробуем base64
                try:
                    import base64
                    decoded = base64.b64decode(raw + "==").decode("utf-8")
                    creds_data = _json.loads(decoded)
                except Exception:
                    await interaction.channel.send(
                        "❌ Google Sheets: не удалось разобрать `GOOGLE_CREDENTIALS`\n"
                        "Значение должно быть JSON содержимым файла одной строкой.\n"
                        "**Как сделать правильно:**\n"
                        "1. Скачай JSON ключ из Google Cloud Console\n"
                        "2. Открой файл текстовым редактором\n"
                        "3. Скопируй **всё содержимое** и вставь в переменную `GOOGLE_CREDENTIALS` в Railway\n"
                        "Значение должно начинаться с `{\"type\": \"service_account\"...`"
                    )
                    return

            creds = GCredentials.from_service_account_info(
                creds_data,
                scopes=[
                    "https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive",
                ]
            )
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(SHEET_ID)

            tab_name = f"BM T{tier} {cat_label} {server.upper()}"
            try:
                ws = sh.worksheet(tab_name)
                ws.clear()
            except gspread.WorksheetNotFound:
                ws = sh.add_worksheet(title=tab_name, rows=300, cols=15)

            header = [
                "Предмет", "Тир.Зач", "Item ID",
                "ЧР цена (рынок)",
                "Лучший город", "Цена в городе (рынок)", "Ордер покупки (город)",
                "Профит (город)", "% профит (город)",
                "Цена в Бреккилене", "Тип цены (Брек)",
                "Профит (Брек)", "% профит (Брек)",
                # Все города отдельно
                "Кэрлеон (продажа)", "Бриджвотч (продажа)", "Форт Стерлинг (продажа)",
                "Лимхёрст (продажа)", "Мартлок (продажа)", "Тетфорд (продажа)",
            ]
            rows = [header]

            for item in results:
                city_data = item.get("city_data", {})
                row = [
                    item["display"],
                    item["tier_label"],
                    item["item_id"],
                    item["bm"],
                    CITY_NAMES_RU.get(item["best_city"], item["best_city"] or "—"),
                    item["best_city_sell"] or "—",
                    item["best_city_buy"] or "—",
                    item["city_profit"] or "—",
                    f"{item['city_pct']}%" if item["city_pct"] else "—",
                    item["brec_price"] or "—",
                    "ордер покупки" if item["brec_is_buy_order"] else "рынок продажи",
                    item["brec_profit"] or "—",
                    f"{item['brec_pct']}%" if item["brec_pct"] else "—",
                    # По городам
                    city_data.get("Caerleon", {}).get("sell", "—") or "—",
                    city_data.get("Bridgewatch", {}).get("sell", "—") or "—",
                    city_data.get("Fort Sterling", {}).get("sell", "—") or "—",
                    city_data.get("Lymhurst", {}).get("sell", "—") or "—",
                    city_data.get("Martlock", {}).get("sell", "—") or "—",
                    city_data.get("Thetford", {}).get("sell", "—") or "—",
                ]
                rows.append(row)

            ws.update("A1", rows)
            ws.format("A1:S1", {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.15, "green": 0.15, "blue": 0.25},
            })

            sheet_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
            await interaction.channel.send(
                f"📊 **Google Sheets обновлён!**\n"
                f"{sheet_url}\n"
                f"Вкладка: **{tab_name}** · {len(results)} строк · {server_name}"
            )

        except ImportError:
            await interaction.channel.send("❌ Установи: `pip install gspread google-auth`")
        except _json.JSONDecodeError as ex:
            await interaction.channel.send(
                f"❌ Google Sheets: не удалось разобрать JSON из `GOOGLE_CREDENTIALS`\n"
                f"Убедись что значение — это содержимое JSON-файла одной строкой без переносов\n"
                f"Ошибка: `{ex}`"
            )
        except Exception as ex:
            await interaction.channel.send(f"❌ Ошибка Google Sheets: `{ex}`")


@bot.tree.command(name="party", description="Albion: анализ пати для статика [Pro]")
@app_commands.describe(p1="Игрок 1", p2="Игрок 2", p3="Игрок 3", p4="Игрок 4", p5="Игрок 5")
@cooldown(15)
async def party(interaction: discord.Interaction, p1: str, p2: str, p3: str, p4: str = None, p5: str = None):
    if await get_tier(interaction.guild_id)<TIER_PRO: return await interaction.response.send_message(embed=upsell_embed("Pro"),ephemeral=True)
    await interaction.response.defer()
    players=[p for p in [p1,p2,p3,p4,p5] if p]
    e = build_embed(C.SUCCESS, footer="EU · Albion Online")
    e.set_author(name=f"Анализ группы — {len(players)} игроков")
    total_kf=total_df=found=0; lines=[]
    async with aiohttp.ClientSession() as s:
        for name in players:
            pid,pname = await albion_find_player(s,name)
            if not pid: lines.append(f"❌ **{name}** — не найден"); continue
            async with s.get(f"{ALBION_BASE}/players/{pid}") as r: p = await r.json()
            kf,df = p.get("KillFame",0),p.get("DeathFame",0)
            total_kf+=kf; total_df+=df; found+=1
            lines.append(f"✅ **{pname}** [{p.get('GuildName') or '—'}]\n   K/D: `{round(kf/df,2) if df else '∞'}` · Fame: `{kf:,}`")
    e.description="\n".join(lines)
    if found>0:
        avg_kd=round(total_kf/total_df,2) if total_df else "∞"
        e.add_field(name="📊 Статистика", value=f"Найдено: **{found}/{len(players)}**\nFame: **{total_kf:,}**\nAvg K/D: **{avg_kd}**", inline=False)
        try:
            verdict = await ask_ai(f"2-sentence verdict on Albion static dungeon party: {found} players, kill fame {total_kf:,}, avg K/D {avg_kd}.", system="Albion Online expert. Brief game advice.")
            e.add_field(name="🤖 AI Вердикт", value=verdict, inline=False)
        except Exception: pass
    await interaction.followup.send(embed=e)

@bot.tree.command(name="tournament")
@app_commands.describe(name="Название", participants="Участники через запятую")
async def tournament(interaction: discord.Interaction, name: str, participants: str):
    if await get_tier(interaction.guild_id)<TIER_PRO: return await interaction.response.send_message(embed=upsell_embed("Pro"),ephemeral=True)
    players=[p.strip() for p in participants.split(",") if p.strip()]
    if len(players)<2: return await interaction.response.send_message("❌ Минимум 2 участника.", ephemeral=True)
    random.shuffle(players)
    matchups=[f"⚔️ **{players[i]}** vs **{players[i+1]}**" for i in range(0,len(players)-1,2)]
    if len(players)%2: matchups.append(f"👤 **{players[-1]}** — BYE")
    e = build_embed(C.GOLD)
    e.set_author(name=name)
    e.add_field(name=f"Раунд 1 ({len(matchups)} матчей)", value="\n".join(matchups), inline=False)
    e.set_footer(text=f"Создал {interaction.user.display_name} · Witness Pro")
    await interaction.response.send_message(embed=e)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  GUILD SETTINGS HELPERS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def get_guild_settings(gid: int) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT * FROM guild_settings WHERE guild_id=?", (gid,)) as c:
            row = await c.fetchone()
            if not row:
                return {"guild_id": gid, "starboard_channel": 0, "starboard_threshold": 3,
                        "suggestion_channel": 0, "ticket_category": 0, "birthday_channel": 0,
                        "lockdown": 0, "price_watch": "{}"}
            cols = [d[0] for d in c.description]
            return dict(zip(cols, row))

async def set_guild_setting(gid: int, key: str, value):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"""
            INSERT INTO guild_settings (guild_id, {key}) VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET {key}=excluded.{key}
        """, (gid, value))
        await db.commit()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🔒 SECURITY EXTRA: LOCKDOWN / SLOWMODE / AUTOBAN / REPORT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="lockdown", description="Режим локдауна — запрет входа новых участников [Premium]")
@app_commands.describe(action="on / off", min_age="Минимальный возраст аккаунта в днях (по умолчанию 7)")
async def lockdown(interaction: discord.Interaction, action: str = "on", min_age: int = 7):
    # basic security — free for all
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ Нужны права администратора.", ephemeral=True)
    enabled = action.lower() in ("on", "вкл", "yes", "1")
    await set_guild_setting(interaction.guild_id, "lockdown", 1 if enabled else 0)
    if enabled:
        e = build_embed(C.DANGER)
        e.add_field(name="Статус", value="Новые участники с аккаунтом младше **{} дней** будут автоматически кикнуты".format(min_age), inline=False)
        e.add_field(name="Выключить", value="`/lockdown action:off`", inline=False)
        # Store min_age in settings
        _, settings = await get_security(interaction.guild_id)
        settings["lockdown_min_age"] = min_age
        log_ch_id, _ = await get_security(interaction.guild_id)
        await save_security(interaction.guild_id, log_ch_id, settings)
    else:
        e = build_embed(C.SUCCESS)
        e.description = "Новые участники снова могут заходить свободно."
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="slowmode", description="Установить slow mode в канале [Premium]")
@app_commands.describe(seconds="Задержка в секундах (0 = выключить, макс 21600)")
async def slowmode(interaction: discord.Interaction, seconds: int = 0):
    # basic security — free for all
    if not interaction.user.guild_permissions.manage_channels:
        return await interaction.response.send_message("❌ Нужно Manage Channels.", ephemeral=True)
    seconds = max(0, min(seconds, 21600))
    await interaction.channel.edit(slowmode_delay=seconds)
    if seconds == 0:
        await interaction.response.send_message("✅ Slow mode выключен.")
    else:
        await interaction.response.send_message(f"✅ Slow mode: **{seconds} сек** между сообщениями.")

@bot.tree.command(name="report", description="Пожаловаться на сообщение модераторам [Premium]")
@app_commands.describe(message_id="ID сообщения", reason="Причина жалобы")
async def report(interaction: discord.Interaction, message_id: str, reason: str = "Не указана"):
    # basic security — free for all
    ch = await get_log_ch(interaction.guild)
    if not ch:
        return await interaction.response.send_message("❌ Канал логов не настроен. Используй `/security setlog`", ephemeral=True)
    try:
        msg_id = int(message_id)
        msg = await interaction.channel.fetch_message(msg_id)
        e = build_embed(C.DANGER)
        e.add_field(name="От кого", value=interaction.user.mention, inline=True)
        e.add_field(name="Автор сообщения", value=msg.author.mention, inline=True)
        e.add_field(name="Канал", value=interaction.channel.mention, inline=True)
        e.add_field(name="Причина", value=reason, inline=False)
        e.add_field(name="Содержимое", value=msg.content[:500] or "*(вложение/эмбед)*", inline=False)
        e.add_field(name="Ссылка", value=f"[Перейти]({msg.jump_url})", inline=True)
        await ch.send(embed=e)
        await interaction.response.send_message("✅ Жалоба отправлена модераторам.", ephemeral=True)
    except (ValueError, discord.NotFound):
        await interaction.response.send_message("❌ Сообщение не найдено. Убедись что ID правильный.", ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🎂 BIRTHDAYS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="birthday", description="Зарегистрировать день рождения")
@app_commands.describe(action="set / check / setchannel", date="Дата в формате ДД.ММ (напр. 25.12)", member="Участник для /birthday check")
async def birthday(interaction: discord.Interaction, action: str = "set", date: str = "", member: discord.Member = None):
    if action.lower() == "setchannel":
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message("❌ Нужно Manage Server.", ephemeral=True)
        await set_guild_setting(interaction.guild_id, "birthday_channel", interaction.channel_id)
        return await interaction.response.send_message(f"✅ Канал поздравлений → {interaction.channel.mention}")

    if action.lower() == "check":
        target = member or interaction.user
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT birthday FROM birthdays WHERE guild_id=? AND user_id=?",
                                  (interaction.guild_id, target.id)) as c:
                row = await c.fetchone()
        if row:
            await interaction.response.send_message(f"🎂 День рождения **{target.display_name}**: **{row[0]}**")
        else:
            await interaction.response.send_message(f"❓ У **{target.display_name}** не указан день рождения.")
        return

    # set
    if not date:
        return await interaction.response.send_message("❌ Укажи дату: `/birthday date:25.12`", ephemeral=True)
    try:
        parts = date.strip().split(".")
        if len(parts) != 2: raise ValueError
        day, month = int(parts[0]), int(parts[1])
        if not (1 <= day <= 31 and 1 <= month <= 12): raise ValueError
        formatted = f"{day:02d}.{month:02d}"
    except ValueError:
        return await interaction.response.send_message("❌ Формат даты: **ДД.ММ** (напр. `25.12`)", ephemeral=True)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""INSERT INTO birthdays (guild_id, user_id, birthday) VALUES (?,?,?)
            ON CONFLICT(guild_id,user_id) DO UPDATE SET birthday=excluded.birthday""",
            (interaction.guild_id, interaction.user.id, formatted))
        await db.commit()
    await interaction.response.send_message(f"🎂 День рождения сохранён: **{formatted}**", ephemeral=True)

async def birthday_check_loop():
    """Фоновая задача — проверяет дни рождения каждый день в 09:00 UTC."""
    await bot.wait_until_ready()
    while not bot.is_closed():
        now = datetime.datetime.utcnow()
        if now.hour == 9 and now.minute < 5:
            today = f"{now.day:02d}.{now.month:02d}"
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT guild_id, user_id FROM birthdays WHERE birthday=?", (today,)
                ) as c:
                    rows = await c.fetchall()
            for gid, uid in rows:
                settings = await get_guild_settings(gid)
                ch_id = settings.get("birthday_channel", 0)
                guild = bot.get_guild(gid)
                if not guild: continue
                ch = guild.get_channel(ch_id) or discord.utils.get(guild.text_channels, name="general")
                if not ch: continue
                member = guild.get_member(uid)
                if not member: continue
                e = build_embed(0xFF69B4)
                e.description = f"Сегодня день рождения у {member.mention}! 🎉\nПоздравьте его/её!"
                e.set_thumbnail(url=member.display_avatar.url)
                try:
                    await ch.send(embed=e)
                except Exception:
                    pass
        await asyncio.sleep(300)  # проверяем каждые 5 минут


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🎫 TICKET SYSTEM
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="ticket", description="Система тикетов [Premium]")
@app_commands.describe(action="open / close / setup", reason="Причина обращения")
async def ticket(interaction: discord.Interaction, action: str = "open", reason: str = "Обращение в поддержку"):
    # basic security — free for all

    if action.lower() == "setup":
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("❌ Нужны права администратора.", ephemeral=True)
        # Создаём категорию для тикетов
        cat = await interaction.guild.create_category("🎫 Tickets")
        await set_guild_setting(interaction.guild_id, "ticket_category", cat.id)
        e = build_embed(C.SUCCESS)
        e.add_field(name="Категория", value=cat.name, inline=True)
        e.add_field(name="Использование", value="Участники могут открывать тикеты: `/ticket`", inline=False)
        return await interaction.response.send_message(embed=e)

    if action.lower() == "close":
        # Закрываем тикет (удаляем канал)
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT id FROM tickets WHERE guild_id=? AND channel_id=? AND status='open'",
                (interaction.guild_id, interaction.channel_id)
            ) as c:
                row = await c.fetchone()
        if not row:
            return await interaction.response.send_message("❌ Это не тикет-канал.", ephemeral=True)
        await interaction.response.send_message("🔒 Тикет закрывается...")
        await asyncio.sleep(3)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE tickets SET status='closed' WHERE channel_id=?", (interaction.channel_id,))
            await db.commit()
        await interaction.channel.delete()
        return

    # open — создаём новый тикет
    settings = await get_guild_settings(interaction.guild_id)
    cat_id = settings.get("ticket_category", 0)
    category = interaction.guild.get_channel(cat_id) if cat_id else None

    # Проверяем нет ли уже открытого тикета
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT channel_id FROM tickets WHERE guild_id=? AND user_id=? AND status='open'",
            (interaction.guild_id, interaction.user.id)
        ) as c:
            existing = await c.fetchone()

    if existing:
        ch = interaction.guild.get_channel(existing[0])
        return await interaction.response.send_message(
            f"❌ У тебя уже есть открытый тикет: {ch.mention if ch else 'канал удалён'}",
            ephemeral=True
        )

    # Создаём канал тикета
    overwrites = {
        interaction.guild.default_role: discord.PermissionOverwrite(read_messages=False),
        interaction.user: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        interaction.guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True),
    }
    # Даём доступ модераторам
    for role in interaction.guild.roles:
        if role.permissions.manage_messages:
            overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

    ch_name = f"ticket-{interaction.user.name[:15].lower().replace(' ', '-')}"
    try:
        ticket_ch = await interaction.guild.create_text_channel(
            ch_name, category=category, overwrites=overwrites
        )
    except Exception as ex:
        return await interaction.response.send_message(f"❌ Не удалось создать канал: {ex}", ephemeral=True)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO tickets (guild_id,user_id,channel_id,status,created_at) VALUES (?,?,?,?,?)",
            (interaction.guild_id, interaction.user.id, ticket_ch.id, "open", datetime.datetime.utcnow().isoformat())
        )
        await db.commit()

    e = build_embed(C.INFO)
    e.add_field(name="Участник", value=interaction.user.mention, inline=True)
    e.add_field(name="Причина", value=reason, inline=True)
    e.add_field(name="Закрыть", value="`/ticket action:close`", inline=False)
    e.set_footer(text="Модераторы скоро ответят")
    await ticket_ch.send(embed=e)
    await interaction.response.send_message(f"✅ Тикет создан: {ticket_ch.mention}", ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  💡 SUGGESTIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="suggestion", description="Система предложений")
@app_commands.describe(action="submit / top / setchannel", text="Текст предложения")
async def suggestion(interaction: discord.Interaction, action: str = "submit", text: str = ""):
    if action.lower() == "setchannel":
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message("❌ Нужно Manage Server.", ephemeral=True)
        await set_guild_setting(interaction.guild_id, "suggestion_channel", interaction.channel_id)
        return await interaction.response.send_message(f"✅ Канал предложений → {interaction.channel.mention}")

    if action.lower() == "top":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT id, text, votes_up, votes_down, user_id FROM suggestions WHERE guild_id=? ORDER BY votes_up DESC LIMIT 5",
                (interaction.guild_id,)
            ) as c:
                rows = await c.fetchall()
        e = build_embed(C.INFO)
        if not rows:
            e.description = "Пока нет предложений. Добавь первое: `/suggestion text:...`"
        for i, (sid, text_s, up, down, uid) in enumerate(rows):
            user = interaction.guild.get_member(uid)
            e.add_field(
                name=f"#{sid} · 👍 {up} 👎 {down}",
                value=f"{text_s[:200]}\n*— {user.display_name if user else 'неизвестно'}*",
                inline=False
            )
        return await interaction.response.send_message(embed=e)

    # submit
    if not text:
        return await interaction.response.send_message("❌ Укажи текст: `/suggestion text:Моя идея`", ephemeral=True)

    settings = await get_guild_settings(interaction.guild_id)
    ch_id = settings.get("suggestion_channel", 0)
    ch = interaction.guild.get_channel(ch_id) if ch_id else interaction.channel

    e = build_embed(0x5865F2, description=text, color=0x7C3AED)
    e.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
    e.add_field(name="Статус", value="⏳ На рассмотрении", inline=True)
    e.set_footer(text="👍 — за  |  👎 — против")

    msg = await ch.send(embed=e)
    await msg.add_reaction("👍")
    await msg.add_reaction("👎")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO suggestions (guild_id,user_id,text,message_id,created_at) VALUES (?,?,?,?,?)",
            (interaction.guild_id, interaction.user.id, text, msg.id, datetime.datetime.utcnow().isoformat())
        )
        await db.commit()

    await interaction.response.send_message(f"✅ Предложение отправлено в {ch.mention}!", ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ⭐ STARBOARD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="starboard", description="Настроить starboard [Premium]")
@app_commands.describe(channel="Канал для starboard", threshold="Количество ⭐ для попадания (по умолч. 3)")
async def starboard_setup(interaction: discord.Interaction, channel: discord.TextChannel, threshold: int = 3):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно Manage Server.", ephemeral=True)
    await set_guild_setting(interaction.guild_id, "starboard_channel", channel.id)
    await set_guild_setting(interaction.guild_id, "starboard_threshold", threshold)
    await interaction.response.send_message(
        f"⭐ Starboard настроен → {channel.mention} · порог: **{threshold}** звёзд"
    )

@bot.event
async def on_reaction_add(reaction, user):
    if user.bot or not reaction.message.guild: return
    gid = reaction.message.guild.id

    # Starboard logic
    if str(reaction.emoji) == "⭐" and await get_tier(gid) >= TIER_PREMIUM:
        settings = await get_guild_settings(gid)
        sb_ch_id   = settings.get("starboard_channel", 0)
        threshold  = settings.get("starboard_threshold", 3)
        if sb_ch_id and reaction.count >= threshold:
            sb_ch = reaction.message.guild.get_channel(sb_ch_id)
            if not sb_ch: return
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT starboard_msg_id FROM starboard WHERE guild_id=? AND message_id=?",
                    (gid, reaction.message.id)
                ) as c:
                    existing = await c.fetchone()
            if existing: return  # уже добавлено
            msg = reaction.message
            e = discord.Embed(description=msg.content or "*(вложение)*", color=0xFFD700, timestamp=msg.created_at)
            e.set_author(name=msg.author.display_name, icon_url=msg.author.display_avatar.url)
            e.add_field(name="Источник", value=f"[Перейти]({msg.jump_url}) · {msg.channel.mention}", inline=False)
            if msg.attachments:
                e.set_image(url=msg.attachments[0].url)
            sb_msg = await sb_ch.send(content=f"⭐ **{reaction.count}** · {msg.channel.mention}", embed=e)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT INTO starboard (guild_id, message_id, starboard_msg_id) VALUES (?,?,?)",
                    (gid, msg.id, sb_msg.id)
                )
                await db.commit()

    # Security reaction log (из существующего кода)
    if not reaction.message.guild: return
    ch = await sec_check(reaction.message.guild, "reactions")
    if not ch: return
    e = build_embed(C.SUCCESS)
    e.set_author(name="Reaction added")
    e.add_field(name="Пользователь", value=user.mention, inline=True)
    e.add_field(name="Реакция", value=str(reaction.emoji), inline=True)
    e.add_field(name="Сообщение", value=f"[Перейти]({reaction.message.jump_url})", inline=True)
    await ch.send(embed=e)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  📊 SERVER STATS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_msg_activity: dict = {}  # {guild_id: {hour: count}}

@bot.event
async def on_message_for_stats(message):
    """Отдельный счётчик активности по часам."""
    if message.author.bot or not message.guild: return
    gid = message.guild.id
    hour = datetime.datetime.utcnow().hour
    _msg_activity.setdefault(gid, {})
    _msg_activity[gid][hour] = _msg_activity[gid].get(hour, 0) + 1

@bot.tree.command(name="serverstats", description="Статистика активности сервера [Premium]")
async def serverstats(interaction: discord.Interaction):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    await interaction.response.defer()
    g = interaction.guild
    gid = g.id

    # Считаем онлайн
    online = sum(1 for m in g.members if m.status != discord.Status.offline) if hasattr(g.members[0], 'status') else "N/A"
    bots   = sum(1 for m in g.members if m.bot)
    humans = g.member_count - bots

    # XP топ
    rows = await get_leaderboard(gid, 3)

    # Активность по часам
    activity = _msg_activity.get(gid, {})
    if activity:
        peak_hour = max(activity, key=activity.get)
        peak_msgs = activity[peak_hour]
        activity_str = f"Пик: **{peak_hour}:00 UTC** ({peak_msgs} сообщений)\n"
        activity_str += " ".join(
            f"`{h}:{'█' * min(activity.get(h,0)//5+1, 5)}`"
            for h in range(0, 24, 4)
        )
    else:
        activity_str = "Нет данных за текущую сессию"

    e = build_embed(TIER_COLORS[await get_tier(g.id)])
    if g.icon: e.set_thumbnail(url=g.icon.url)
    e.add_field(name="👥 Участников", value=f"**{g.member_count}**\n{humans} людей · {bots} ботов", inline=True)
    e.add_field(name="📁 Каналов", value=f"**{len(g.channels)}**\n{len(g.text_channels)} текст · {len(g.voice_channels)} голос", inline=True)
    e.add_field(name="🎭 Ролей", value=str(len(g.roles)), inline=True)
    e.add_field(name="📅 Создан", value=g.created_at.strftime("%d.%m.%Y"), inline=True)
    e.add_field(name="💎 Буст", value=f"Уровень {g.premium_tier} · {g.premium_subscription_count} бустов", inline=True)

    if rows:
        top_lines = []
        for i, (uid, xp) in enumerate(rows):
            u = g.get_member(uid)
            top_lines.append(f"{['🥇','🥈','🥉'][i]} {u.display_name if u else uid} — {xp} XP")
        e.add_field(name="🏆 Топ активных", value="\n".join(top_lines), inline=False)

    e.add_field(name="📈 Активность (сегодня)", value=activity_str, inline=False)
    await interaction.followup.send(embed=e)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🖼️ AI IMAGE GENERATION (Pollinations.ai — бесплатно)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="imagine", description="Генерация изображения по описанию [Premium]")
@app_commands.describe(prompt="Описание изображения на английском", style="realistic / anime / pixel / oil-painting")
@cooldown(15)
async def imagine(interaction: discord.Interaction, prompt: str, style: str = "realistic"):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    await interaction.response.defer()

    styles = {
        "realistic": "photorealistic, high quality, 8k",
        "anime": "anime style, manga, illustration",
        "pixel": "pixel art, 16-bit, retro game",
        "oil-painting": "oil painting, classical art, detailed brushwork"
    }
    style_prompt = styles.get(style.lower(), styles["realistic"])
    full_prompt = f"{prompt}, {style_prompt}"
    encoded = full_prompt.replace(" ", "%20").replace(",", "%2C")

    # Pollinations.ai — полностью бесплатный API генерации изображений
    url = f"https://image.pollinations.ai/prompt/{encoded}?width=768&height=768&nologo=true"

    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=60)) as r:
                if r.status != 200:
                    return await interaction.followup.send(f"❌ Ошибка генерации (HTTP {r.status})")
                img_data = await r.read()

        file = discord.File(
            fp=__import__("io").BytesIO(img_data),
            filename="generated.png"
        )
        e = build_embed(0x7C3AED)
        e.add_field(name="Запрос", value=prompt[:200], inline=False)
        e.add_field(name="Стиль", value=style, inline=True)
        e.set_footer(text=f"Запросил: {interaction.user.display_name} · Pollinations.ai (free)")
        e.set_image(url="attachment://generated.png")
        await interaction.followup.send(embed=e, file=file)
    except asyncio.TimeoutError:
        await interaction.followup.send("❌ Таймаут генерации (>60 сек). Попробуй более простой запрос.")
    except Exception as ex:
        await interaction.followup.send(f"❌ Ошибка: {ex}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ⚔️ ALBION: CRAFT CALCULATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Коэффициенты материалов для крафта (упрощённые, стандартные)
CRAFT_MATERIALS = {
    6: {"ore": 16, "wood": 16, "fiber": 16, "hide": 16, "rock": 16},
    7: {"ore": 20, "wood": 20, "fiber": 20, "hide": 20, "rock": 20},
    8: {"ore": 24, "wood": 24, "fiber": 24, "hide": 24, "rock": 24},
}
MATERIAL_ITEMS = {
    6: {"ore": "T6_ORE", "wood": "T6_WOOD", "fiber": "T6_FIBER", "hide": "T6_HIDE", "rock": "T6_ROCK"},
    7: {"ore": "T7_ORE", "wood": "T7_WOOD", "fiber": "T7_FIBER", "hide": "T7_HIDE", "rock": "T7_ROCK"},
    8: {"ore": "T8_ORE", "wood": "T8_WOOD", "fiber": "T8_FIBER", "hide": "T8_HIDE", "rock": "T8_ROCK"},
}
ITEM_MATERIAL_TYPE = {
    "sword": "ore", "claymore": "ore", "axe": "ore", "mace": "ore",
    "hammer": "ore", "crossbow": "ore", "shield": "ore", "spear": "ore",
    "bow": "wood", "quarterstaff": "wood", "naturestaff": "wood",
    "firestaff": "wood", "holystaff": "wood", "torch": "wood",
    "mercjacket": "ore", "mercboots": "ore", "merchood": "ore",
    "assjacket": "hide", "assboots": "hide", "asshood": "hide",
    "huntjacket": "fiber", "huntboots": "fiber", "hunthood": "fiber",
    "bag": "fiber",
}

@bot.tree.command(name="craftcalc", description="Таблица крафта Albion → Google Sheets [Pro]")
@app_commands.describe(
    tier="Тир: 6, 7 или 8",
    server="Сервер: eu / us / asia",
    tax="Налог рынка: 8 (обычный) или 4 (премиум город)"
)
@cooldown(20)
async def craftcalc(interaction: discord.Interaction, tier: int = 8, server: str = "eu", tax: int = 8):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)

    if not GOOGLE_CREDS or not SHEET_ID:
        e = build_embed(C.DANGER)
        e.description = (
            "Для использования `/craftcalc` нужно настроить Google Sheets.\n\n"
            "**1.** Зайди в [Google Cloud Console](https://console.cloud.google.com)\n"
            "**2.** Создай Service Account → скачай JSON ключ\n"
            "**3.** Открой JSON файл, скопируй содержимое\n"
            "**4.** В Railway Variables добавь:\n"
            "`GOOGLE_CREDENTIALS` = содержимое JSON одной строкой\n"
            "`SHEET_ID` = ID таблицы из URL\n\n"
            "**5.** Дай сервис-аккаунту доступ Editor к таблице"
        )
        return await interaction.response.send_message(embed=e, ephemeral=True)

    await interaction.response.defer()

    if tier not in (6, 7, 8):
        return await interaction.followup.send("❌ Тир: 6, 7 или 8")
    if tax not in (4, 8):
        return await interaction.followup.send("❌ Налог: 4 или 8")

    tax_rate = tax / 100
    server_name = ALBION_SERVER_NAMES.get(server, "EU")
    base_url = ALBION_SERVERS.get(server, ALBION_SERVERS["eu"])
    locations = "Black%20Market,Bridgewatch,Fort%20Sterling,Lymhurst,Martlock,Thetford,Brecilien"

    # Генерируем список из BM_ITEMS автоматически — материал определяем по типу предмета
    MAT_BY_TYPE = {
        "MAIN_SWORD":"ore","2H_CLAYMORE":"ore","2H_DUALSWORD":"ore","MAIN_BROADSWORD":"ore",
        "2H_ENERGYSHAPER":"ore","MAIN_DAGGER":"ore","2H_DAGGERPAIR":"ore","2H_CLAWS":"ore",
        "2H_DEATHGIGGLES":"ore","MAIN_AXE":"ore","2H_GREATAXE":"ore","2H_HALBERD":"ore",
        "2H_CARRIONCALLER":"ore","2H_DUALSCYTHE":"ore","MAIN_MACE":"ore","2H_HEAVYMACE":"ore",
        "MAIN_MORNINGSTAR":"ore","MAIN_INCUBUS":"ore","2H_ONEMACELAIR":"ore","2H_HAMMER":"ore",
        "2H_POLEHAMMER":"ore","2H_GRAILSEEKER":"ore","2H_KNUCKLES":"ore","MAIN_GAUNTLET":"ore",
        "MAIN_SPIKEDGAUNTLET":"ore","2H_URSINEHANDSCLAW":"ore","MAIN_SPEAR":"ore","2H_PIKE":"ore",
        "2H_GLAIVE":"ore","2H_SPIRITHUNTER":"ore","2H_TRINITYSPEAR":"ore","2H_CROSSBOW":"ore",
        "2H_HEAVYCROSSBOW":"ore","MAIN_LIGHTCROSSBOW":"ore","2H_BOLTCASTERS":"ore","2H_SIEGEBOW":"ore",
        "OFFHAND_SHIELD":"ore","OFFHAND_TOWERSHIELD":"ore",
        "HEAD_PLATE_SET1":"ore","ARMOR_PLATE_SET1":"ore","SHOES_PLATE_SET1":"ore",
        "HEAD_PLATE_SET2":"ore","ARMOR_PLATE_SET2":"ore","SHOES_PLATE_SET2":"ore",
        "HEAD_PLATE_SET3":"ore","ARMOR_PLATE_SET3":"ore","SHOES_PLATE_SET3":"ore",
        "HEAD_PLATE_UNDEAD":"ore","ARMOR_PLATE_UNDEAD":"ore","SHOES_PLATE_UNDEAD":"ore",
        "HEAD_PLATE_HELL":"ore","ARMOR_PLATE_HELL":"ore","SHOES_PLATE_HELL":"ore",
        "HEAD_PLATE_MORGANA":"ore","ARMOR_PLATE_MORGANA":"ore","SHOES_PLATE_MORGANA":"ore",
        "2H_QUARTERSTAFF":"wood","2H_IRONCLADSTAFF":"wood","2H_SHARPSTAFF":"wood",
        "2H_DOUBLEBLADEDSTAFF":"wood","MAIN_NATURESTAFF":"wood","2H_NATURESTAFFGREAT":"wood",
        "2H_WILDSTAFF":"wood","MAIN_LUSHFOLIAGE":"wood","MAIN_TORCH":"wood",
        "MAIN_FIRESTAFF":"wood","2H_FIRESTAFF":"wood","2H_INFERNOSTAFF":"wood",
        "2H_BLAZINGSERPENT":"wood","MAIN_HOLYSTAFF":"wood","2H_HOLYSTAFF":"wood",
        "2H_DIVINESTAFF":"wood","MAIN_LIFETOUCH":"wood","MAIN_ARCANESTAFF":"wood",
        "2H_ARCANESTAFF":"wood","2H_ENIGMATICSTAFF":"wood","MAIN_OCCULTSTAFF":"wood",
        "MAIN_CURSEDSTAFF":"wood","2H_CURSEDSTAFF":"wood","2H_DEMONICSTAFF":"wood",
        "MAIN_SHADOWCALLER":"wood","2H_BOW":"wood","2H_WARBOW":"wood","2H_LONGBOW":"wood",
        "2H_MISTCALLER":"wood","2H_WEEPINGREPEAT":"wood",
        "OFFHAND_TORCH":"wood","OFFHAND_SOULSCYTHE":"wood",
        "HEAD_LEATHER_SET1":"hide","ARMOR_LEATHER_SET1":"hide","SHOES_LEATHER_SET1":"hide",
        "HEAD_LEATHER_SET2":"hide","ARMOR_LEATHER_SET2":"hide","SHOES_LEATHER_SET2":"hide",
        "HEAD_LEATHER_SET3":"hide","ARMOR_LEATHER_SET3":"hide","SHOES_LEATHER_SET3":"hide",
        "HEAD_LEATHER_UNDEAD":"hide","ARMOR_LEATHER_UNDEAD":"hide","SHOES_LEATHER_UNDEAD":"hide",
        "HEAD_LEATHER_HELL":"hide","ARMOR_LEATHER_HELL":"hide","SHOES_LEATHER_HELL":"hide",
        "HEAD_LEATHER_MORGANA":"hide","ARMOR_LEATHER_MORGANA":"hide","SHOES_LEATHER_MORGANA":"hide",
        "OFFHAND_MISTCOVERDAGGER":"hide","OFFHAND_LEATHERBOOK":"hide",
        "HEAD_CLOTH_SET1":"fiber","ARMOR_CLOTH_SET1":"fiber","SHOES_CLOTH_SET1":"fiber",
        "HEAD_CLOTH_SET2":"fiber","ARMOR_CLOTH_SET2":"fiber","SHOES_CLOTH_SET2":"fiber",
        "HEAD_CLOTH_SET3":"fiber","ARMOR_CLOTH_SET3":"fiber","SHOES_CLOTH_SET3":"fiber",
        "HEAD_CLOTH_UNDEAD":"fiber","ARMOR_CLOTH_UNDEAD":"fiber","SHOES_CLOTH_UNDEAD":"fiber",
        "HEAD_CLOTH_HELL":"fiber","ARMOR_CLOTH_HELL":"fiber","SHOES_CLOTH_HELL":"fiber",
        "OFFHAND_SKULLORB":"fiber","OFFHAND_CODEXA":"fiber","OFFHAND_LANTERN":"fiber",
        "OFFHAND_MUISNT":"fiber","BAG":"fiber","BAG_INSIGHT":"fiber",
    }
    def get_mat(template_str):
        for part, mat in MAT_BY_TYPE.items():
            if part in template_str: return mat
        return "ore"

    ALL_CALC_ITEMS = []
    for key, item_dict in BM_ITEMS.items():
        display, template = list(item_dict.items())[0]
        mat = get_mat(template)
        ALL_CALC_ITEMS.append((display, key, mat))


    mat_names_ru = {"ore": "Руда", "wood": "Дерево", "fiber": "Волокно", "hide": "Кожа"}
    mat_item_ids = {
        "ore": f"T{tier}_ORE", "wood": f"T{tier}_WOOD",
        "fiber": f"T{tier}_FIBER", "hide": f"T{tier}_HIDE",
    }
    base_count = {6: 8, 7: 10, 8: 12}[tier]
    ench_mults = [1, 2, 4, 8, 16]

    await interaction.followup.send(f"⏳ Загружаю цены T{tier} · {server_name}... (~30 сек)")

    # ── Загружаем цены материалов ─────────────────────────────
    mat_prices = {}
    async with aiohttp.ClientSession() as s:
        for mat, mat_id in mat_item_ids.items():
            url = f"{base_url}/stats/prices/{mat_id}?locations={locations}"
            try:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200: continue
                    data = await r.json()
                best = 9_999_999_999; best_city = ""
                for p in data:
                    city = p.get("city", ""); sell = p.get("sell_price_min", 0) or 0
                    if sell > 0 and city not in ("Black Market", "Brecilien"):
                        if sell < best: best = sell; best_city = city
                mat_prices[mat] = {"price": best if best < 9_999_999_999 else 0, "city": best_city}
            except Exception:
                mat_prices[mat] = {"price": 0, "city": ""}

    # ── Загружаем цены всех предметов ─────────────────────────
    item_rows = []
    async with aiohttp.ClientSession() as s:
        for display, key, mat in ALL_CALC_ITEMS:
            bm_data = BM_ITEMS.get(key)
            if not bm_data: continue
            _, template = list(bm_data.items())[0]
            mat_price = mat_prices.get(mat, {}).get("price", 0)
            mat_city = CITY_NAMES_RU.get(mat_prices.get(mat, {}).get("city", ""), "—")

            for enchant in range(5):
                item_id = build_item_id(template, tier, enchant)
                mult = ench_mults[enchant]
                mats = base_count * mult
                cost = mats * mat_price

                url = f"{base_url}/stats/prices/{item_id}?locations={locations}"
                bm = market = brec = 0
                try:
                    async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status == 200:
                            prices = await r.json()
                            for p in prices:
                                city = p.get("city", ""); sell = p.get("sell_price_min", 0) or 0
                                if city == "Black Market" and sell > 0: bm = max(bm, sell)
                                elif city == "Brecilien" and sell > 0: brec = sell
                                elif sell > 0 and city not in ("Black Market", "Brecilien"):
                                    market = min(market, sell) if market > 0 else sell
                except Exception:
                    pass

                tax_val = tax_rate
                profit_bm   = round(bm   * (1-tax_val) - cost) if bm   > 0 and cost > 0 else ""
                profit_mkt  = round(market * (1-tax_val) - cost) if market > 0 and cost > 0 else ""
                profit_brec = round(brec * (1-tax_val) - cost) if brec  > 0 and cost > 0 else ""
                pct_bm   = round(profit_bm   / cost * 100, 1) if isinstance(profit_bm,   int) and cost > 0 else ""
                pct_mkt  = round(profit_mkt  / cost * 100, 1) if isinstance(profit_mkt,  int) and cost > 0 else ""
                pct_brec = round(profit_brec / cost * 100, 1) if isinstance(profit_brec, int) and cost > 0 else ""

                item_rows.append([
                    display,
                    f"T{tier}.{enchant}",
                    mat_names_ru.get(mat, mat),
                    mat_city,
                    mat_price or "",
                    mats,
                    cost or "",
                    bm or "",
                    profit_bm,
                    f"{pct_bm}%" if pct_bm != "" else "",
                    market or "",
                    profit_mkt,
                    f"{pct_mkt}%" if pct_mkt != "" else "",
                    brec or "",
                    profit_brec,
                    f"{pct_brec}%" if pct_brec != "" else "",
                ])

    # ── Google Sheets ─────────────────────────────────────────
    try:
        import gspread
        from google.oauth2.service_account import Credentials as GCreds
        import json as _json

        raw = GOOGLE_CREDS.strip()
        if raw.startswith("{"):
            creds_data = _json.loads(raw)
        else:
            import base64
            creds_data = _json.loads(base64.b64decode(raw + "==").decode("utf-8"))

        creds = GCreds.from_service_account_info(
            creds_data,
            scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)

        tab_name = f"Крафт T{tier} {server.upper()} налог{tax}%"
        try:
            ws = sh.worksheet(tab_name)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=tab_name, rows=300, cols=20)

        header = [
            "Предмет", "Тир.Зач", "Материал", "Лучший город мат.",
            "Цена мат. (1 шт)", "Кол-во мат.", "Себестоимость",
            "Цена ЧР", "Профит ЧР (−8% налог)", "% профит ЧР",
            "Цена рынок", "Профит рынок", "% профит рынок",
            "Цена Бреккилен", "Профит Брек.", "% профит Брек.",
        ]
        all_rows = [header] + item_rows
        ws.update("A1", all_rows)

        # Форматирование шапки
        ws.format("A1:P1", {
            "backgroundColor": {"red": 0.1, "green": 0.1, "blue": 0.2},
            "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            "horizontalAlignment": "CENTER",
        })

        # Заморозить шапку
        ws.freeze(rows=1)

        # Условное форматирование — зелёный если профит > 0, красный если < 0
        # (через gspread это сложно, поэтому красим вручную топ профитные строки)
        profit_col_idx = 9  # колонка "% профит ЧР" (I)
        green_rows = []
        red_rows = []
        for i, row in enumerate(item_rows, start=2):
            pct = row[9]  # % профит ЧР
            if pct == "": continue
            try:
                val = float(str(pct).replace("%",""))
                if val > 0: green_rows.append(i)
                elif val < 0: red_rows.append(i)
            except Exception: pass

        # Красим зелёные строки (топ 20)
        for row_i in green_rows[:20]:
            ws.format(f"A{row_i}:P{row_i}", {
                "backgroundColor": {"red": 0.85, "green": 0.95, "blue": 0.85}
            })
        # Красим красные строки (топ 10 убыточных)
        for row_i in red_rows[:10]:
            ws.format(f"A{row_i}:P{row_i}", {
                "backgroundColor": {"red": 0.95, "green": 0.85, "blue": 0.85}
            })

        sheet_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
        total = len(item_rows)
        profitable = len([r for r in item_rows if r[9] and str(r[9]) != "" and float(str(r[9]).replace("%","").replace("","0") or 0) > 0])

        e = build_embed(C.INFO)
        e.add_field(name="Таблица", value=f"[Открыть Google Sheets]({sheet_url})\nВкладка: **{tab_name}**", inline=False)
        e.add_field(name="Предметов", value=str(total), inline=True)
        e.add_field(name="Выгодных (ЧР)", value=str(profitable), inline=True)
        e.add_field(name="Зачаровки", value="T.0 → T.4 (все)", inline=True)
        mat_lines = "\n".join(
            f"**{mat_names_ru[m]}**: {mat_prices.get(m,{}).get('price',0):,} ({CITY_NAMES_RU.get(mat_prices.get(m,{}).get('city',''), '—')})"
            for m in ["ore", "wood", "fiber", "hide"]
        )
        e.add_field(name="📦 Цены материалов", value=mat_lines, inline=False)
        e.set_footer(text=f"Налог {tax}% учтён · albion-online-data.com · {datetime.datetime.utcnow().strftime('%d.%m.%Y %H:%M')} UTC")
        await interaction.channel.send(embed=e)

    except ImportError:
        await interaction.channel.send("❌ Установи: `pip install gspread google-auth`")
    except _json.JSONDecodeError as ex:
        await interaction.channel.send(f"❌ Ошибка JSON в GOOGLE_CREDENTIALS: `{ex}`")
    except Exception as ex:
        await interaction.channel.send(f"❌ Ошибка Google Sheets: `{ex}`")



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  💱 ALBION: CITY FLIPPER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="flipper", description="Торговый арбитраж между городами Albion [Pro]")
@app_commands.describe(
    category="weapon / offhand / armor_plate / armor_leather / armor_cloth / bag",
    tier="Тир: 6, 7 или 8",
    server="Сервер: eu / us / asia"
)
@cooldown(30)
async def flipper(interaction: discord.Interaction, category: str = "weapon", tier: int = 8, server: str = "eu"):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)
    await interaction.response.defer()

    if tier not in (6, 7, 8):
        return await interaction.followup.send("❌ Тир: 6, 7 или 8")

    cat_lower = category.lower()
    keys = BM_GROUPS.get(cat_lower, [cat_lower] if cat_lower in BM_ITEMS else None)
    if not keys:
        return await interaction.followup.send("❌ Категория: weapon / offhand / armor_plate / armor_leather / armor_cloth / bag")

    await interaction.followup.send(f"⏳ Ищу арбитраж для **{category} T{tier}**...")

    base_url = ALBION_SERVERS.get(server, ALBION_SERVERS["eu"])
    locations = ",".join(CITY_LOCATIONS).replace(" ", "%20")
    flips = []

    async with aiohttp.ClientSession() as s:
        for key in keys[:15]:  # лимит чтобы не долго
            item_data = BM_ITEMS.get(key)
            if not item_data: continue
            display, template = list(item_data.items())[0]

            for enchant in range(0, 3):  # только 0-2 для скорости
                item_id = build_item_id(template, tier, enchant)
                url = f"{base_url}/stats/prices/{item_id}?locations={locations}"
                try:
                    async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status != 200: continue
                        prices = await r.json()
                except Exception:
                    continue

                city_prices = {}
                for p in prices:
                    city = p.get("city", "")
                    sell = p.get("sell_price_min", 0) or 0
                    buy  = p.get("buy_price_max", 0) or 0
                    if city in CITY_LOCATIONS and sell > 0:
                        city_prices[city] = {"sell": sell, "buy": buy}

                if len(city_prices) < 2: continue

                # Найти максимальную разницу между городами
                cities = list(city_prices.keys())
                best_flip = None
                best_profit = 0
                for i in range(len(cities)):
                    for j in range(len(cities)):
                        if i == j: continue
                        buy_city = cities[i]
                        sell_city = cities[j]
                        buy_price  = city_prices[buy_city]["sell"]  # покупаем по рыночной цене
                        sell_price = city_prices[sell_city]["buy"]  # продаём по ордеру покупателя
                        if buy_price > 0 and sell_price > buy_price:
                            profit = sell_price - buy_price
                            pct = round(profit / buy_price * 100, 1)
                            if profit > best_profit:
                                best_profit = profit
                                best_flip = {
                                    "buy_city": buy_city, "buy_price": buy_price,
                                    "sell_city": sell_city, "sell_price": sell_price,
                                    "profit": profit, "pct": pct
                                }
                if best_flip and best_flip["pct"] > 5:
                    flips.append({
                        "name": f"{display} {tier}.{enchant}",
                        "item_id": item_id,
                        **best_flip
                    })

    if not flips:
        return await interaction.channel.send("😔 Нет выгодных флипов прямо сейчас. Рынок выровнен.")

    flips.sort(key=lambda x: x["pct"], reverse=True)
    e = discord.Embed(
        title=f"💱 Флиппер — {category.capitalize()} T{tier}",
        description=f"Купи в одном городе, продай в другом · {ALBION_SERVER_NAMES.get(server,'EU')}",
        color=0x00FF9D
    )
    for flip in flips[:8]:
        buy_ru  = CITY_NAMES_RU.get(flip["buy_city"],  flip["buy_city"])
        sell_ru = CITY_NAMES_RU.get(flip["sell_city"], flip["sell_city"])
        e.add_field(
            name=f"🗡️ {flip['name']} (+{flip['pct']}%)",
            value=(f"Купить в **{buy_ru}**: `{flip['buy_price']:,}`\n"
                   f"Продать в **{sell_ru}** (ордер): `{flip['sell_price']:,}`\n"
                   f"Профит: **{flip['profit']:,}** серебра"),
            inline=False
        )
    e.set_footer(text="Цены обновляются каждые ~15 мин · Учитывай налог 8%")
    await interaction.channel.send(embed=e)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🏰 ALBION: GUILD WAR STATS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="guildwar", description="Топ гильдий по активности ZvZ [Pro]")
@app_commands.describe(limit="Сколько гильдий показать (5-20)")
@cooldown(20)
async def guildwar(interaction: discord.Interaction, limit: int = 10):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)
    await interaction.response.defer()
    limit = max(5, min(limit, 20))
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{ALBION_BASE}/battles?sort=recent&limit=50",
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
                battles = await r.json() if r.status == 200 else []

        guild_stats = {}
        for b in battles:
            fame  = b.get("TotalFame", 0)
            kills = b.get("TotalKills", 0)
            for gname in b.get("Guilds", {}).keys():
                if gname not in guild_stats:
                    guild_stats[gname] = {"battles": 0, "fame": 0, "kills": 0}
                guild_stats[gname]["battles"] += 1
                guild_stats[gname]["fame"]    += fame
                guild_stats[gname]["kills"]   += kills

        if not guild_stats:
            return await interaction.followup.send("❌ Нет данных о недавних битвах.")

        top = sorted(guild_stats.items(), key=lambda x: x[1]["fame"], reverse=True)[:limit]

        e = build_embed(C.DANGER, description=f"Last 50 battles · {datetime.datetime.utcnow().strftime('%d.%m.%Y')}")
        e.set_author(name="Guild War — Top Guilds")
        e.set_footer(text="Albion Online · EU")
        for i, (gname, stats) in enumerate(top):
            e.add_field(
                name=f"{i+1}. {gname}",
                value=(
                    f"Battles: **{stats['battles']}** · Kills: **{stats['kills']}**\n"
                    f"Fame: **{stats['fame']:,}**"
                ),
                inline=True
            )
        await interaction.followup.send(embed=e)
    except Exception as ex:
        await interaction.followup.send(f"❌ Ошибка: {ex}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  📡 PRICE WATCH — подписка на изменение цены
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="pricewatch", description="Слежка за ценой предмета [Pro]")
@app_commands.describe(
    action="add / remove / list",
    item_key="Ключ предмета (напр. sword, bow)",
    tier="Тир: 6-8",
    threshold="Порог изменения цены в % (по умолч. 5%)"
)
async def pricewatch(interaction: discord.Interaction, action: str = "list",
                     item_key: str = "", tier: int = 8, threshold: float = 5.0):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)

    gid = interaction.guild_id

    if action.lower() == "list":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT id, item_id, threshold_pct, last_price FROM price_watch WHERE guild_id=?", (gid,)
            ) as c:
                rows = await c.fetchall()
        e = build_embed(C.INFO)
        if not rows:
            e.description = "Нет активных подписок. Добавь: `/pricewatch action:add item_key:sword`"
        for wid, iid, thr, last_p in rows:
            e.add_field(name=f"#{wid} · {iid}", value=f"Порог: {thr}% · Последняя цена: {last_p:,}", inline=False)
        return await interaction.response.send_message(embed=e)

    if action.lower() == "remove":
        try:
            watch_id = int(item_key)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("DELETE FROM price_watch WHERE id=? AND guild_id=?", (watch_id, gid))
                await db.commit()
            return await interaction.response.send_message(f"✅ Подписка `#{watch_id}` удалена.")
        except ValueError:
            return await interaction.response.send_message("❌ Укажи ID подписки: `/pricewatch action:remove item_key:1`", ephemeral=True)

    # add
    if not item_key:
        return await interaction.response.send_message("❌ Укажи `item_key`", ephemeral=True)
    item_data = BM_ITEMS.get(item_key.lower())
    if not item_data:
        return await interaction.response.send_message(f"❌ Предмет `{item_key}` не найден.", ephemeral=True)
    _, template = list(item_data.items())[0]
    item_id = build_item_id(template, tier, 0)

    async with aiosqlite.connect(DB_PATH) as db:
        # Проверяем лимит (макс 5 подписок на сервер)
        async with db.execute("SELECT COUNT(*) FROM price_watch WHERE guild_id=?", (gid,)) as c:
            count = (await c.fetchone())[0]
        if count >= 5:
            return await interaction.response.send_message("❌ Максимум 5 подписок на сервер. Удали старые через `/pricewatch action:remove`", ephemeral=True)
        await db.execute(
            "INSERT INTO price_watch (guild_id,channel_id,item_id,threshold_pct,last_price,created_at) VALUES (?,?,?,?,0,?)",
            (gid, interaction.channel_id, item_id, threshold, datetime.datetime.utcnow().isoformat())
        )
        await db.commit()

    await interaction.response.send_message(
        f"✅ Слежка добавлена: **{item_id}** · порог **{threshold}%** · уведомления в {interaction.channel.mention}"
    )

async def price_watch_loop():
    """Фоновая задача — проверяет цены каждые 15 минут."""
    await bot.wait_until_ready()
    while not bot.is_closed():
        await asyncio.sleep(900)  # 15 минут
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("SELECT id, guild_id, channel_id, item_id, threshold_pct, last_price FROM price_watch") as c:
                    watches = await c.fetchall()

            for wid, gid, ch_id, item_id, thr, last_price in watches:
                guild = bot.get_guild(gid)
                ch    = guild.get_channel(ch_id) if guild else None
                if not ch: continue

                url = f"{ALBION_DATA}/stats/prices/{item_id}?locations=Black%20Market,Caerleon"
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                            prices = await r.json() if r.status == 200 else []
                except Exception:
                    continue

                current_price = 0
                for p in prices:
                    if p.get("city") == "Black Market":
                        current_price = p.get("sell_price_min", 0) or 0
                        break
                if not current_price: continue

                if last_price > 0:
                    change_pct = abs(current_price - last_price) / last_price * 100
                    if change_pct >= thr:
                        direction = "📈 вырос" if current_price > last_price else "📉 упал"
                        e = build_embed(C.GOLD)
                        e.add_field(name="Цена на ЧР", value=f"**{direction}** на {change_pct:.1f}%", inline=False)
                        e.add_field(name="Было", value=f"{last_price:,}", inline=True)
                        e.add_field(name="Стало", value=f"{current_price:,}", inline=True)
                        try:
                            await ch.send(embed=e)
                        except Exception:
                            pass

                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("UPDATE price_watch SET last_price=? WHERE id=?", (current_price, wid))
                    await db.commit()
        except Exception as ex:
            print(f"[PriceWatch] Error: {ex}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🤖 AI: ASK ALBION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="askalbion", description="AI ответит на вопрос об Albion Online [Premium]")
@app_commands.describe(question="Вопрос об игре (билды, механики, советы)")
@cooldown(10)
async def askalbion(interaction: discord.Interaction, question: str):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    await interaction.response.defer()
    try:
        answer = await ask_ai(
            question,
            system=(
                "You are an expert Albion Online player and guide. "
                "Answer questions about builds, mechanics, economy, PvP, PvE, guilds, and all game systems. "
                "Be specific and practical. Use silver values when relevant. "
                "If asked in Russian, reply in Russian. Keep answers concise but complete."
            )
        )
        e = build_embed(0x5865F2, description=answer[:4000], color=0xC8A951)
        e.set_author(name="Albion Expert")
        e.set_footer(text=f"Вопрос: {question[:80]} · Witness AI")
        await interaction.followup.send(embed=e)
    except Exception as ex:
        await interaction.followup.send(f"❌ Ошибка: {ex}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  START BACKGROUND TASKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━





@bot.tree.command(name="bmtest", description="[DEBUG] Тест API Albion Data Project")
@app_commands.describe(item_id="ID предмета (напр. T8_MAIN_SWORD)")
async def bmtest(interaction: discord.Interaction, item_id: str = "T8_MAIN_SWORD"):
    """Быстрый тест — проверяет что API отвечает и возвращает цены."""
    await interaction.response.defer()
    url = f"https://west.albion-online-data.com/api/v2/stats/prices/{item_id}?locations=Black%20Market,Caerleon,Martlock,Brecilien"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                status = r.status
                data = await r.json()

        e = build_embed(C.INFO)
        e.add_field(name="HTTP Status", value=str(status), inline=True)
        e.add_field(name="Записей в ответе", value=str(len(data)), inline=True)
        e.add_field(name="URL", value=f"`{url}`", inline=False)

        lines = []
        for p in data:
            city  = p.get("city", "?")
            sell  = p.get("sell_price_min", 0) or 0
            buy   = p.get("buy_price_max", 0) or 0
            upd   = p.get("sell_price_min_date", "")[:10]
            lines.append(f"**{city}**: sell=`{sell:,}` buy=`{buy:,}` upd=`{upd}`")

        if lines:
            e.add_field(name="Цены", value="\n".join(lines), inline=False)
        else:
            e.add_field(name="Цены", value="❌ Пустой ответ", inline=False)

        await interaction.followup.send(embed=e)
    except Exception as ex:
        await interaction.followup.send(f"❌ Ошибка запроса: `{ex}`")


if __name__ == "__main__":
    bot.run(TOKEN)
