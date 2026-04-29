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
# Security module
HMAC_SECRET     = os.getenv("HMAC_SECRET", "")           # любая случайная строка, фиксированная!
ALBION_BASE   = "https://gameinfo.albiononline.com/api/gameinfo"
ALBION_DATA   = "https://west.albion-online-data.com/api/v2"

TIER_FREE, TIER_PREMIUM, TIER_PRO = 0, 1, 2
TIER_NAMES  = {0: "Free", 1: "⭐ Premium", 2: "💎 Pro"}
TIER_COLORS = {0: 0x6b7fa3, 1: 0x00E5FF, 2: 0xFFD700}

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
    return {0: "🔓 Free", 1: "⭐ Premium", 2: "💎 Pro"}.get(tier, "?")


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

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HEALTH CHECK — Railway мониторит порт 8080
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def health_check_server():
    """Простой HTTP сервер для Railway health check"""
    from aiohttp import web

    async def handle(request):
        guilds = len(bot.guilds) if bot.is_ready() else 0
        return web.Response(
            text=f"OK|guilds={guilds}|latency={round(bot.latency*1000)}ms",
            status=200
        )

    app = web.Application()
    app.router.add_get("/", handle)
    app.router.add_get("/health", handle)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 8080)))
    await site.start()
    print(f"✅ Health check server started on port {os.getenv('PORT', 8080)}")


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
                lang TEXT DEFAULT 'ru',
                tickets_enabled INTEGER DEFAULT 1);
            CREATE TABLE IF NOT EXISTS price_watch (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL, item_id TEXT, threshold_pct REAL DEFAULT 5.0,
                last_price INTEGER DEFAULT 0, created_at TEXT);

            -- Привязка Discord → Albion ник
            CREATE TABLE IF NOT EXISTS albion_registration (
                guild_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                player_name TEXT NOT NULL,
                player_id   TEXT NOT NULL,
                registered_at TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id));

            -- Reaction roles
            CREATE TABLE IF NOT EXISTS reaction_roles (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    INTEGER NOT NULL,
                channel_id  INTEGER NOT NULL,
                message_id  INTEGER NOT NULL,
                emoji       TEXT NOT NULL,
                role_id     INTEGER NOT NULL,
                style       TEXT DEFAULT 'toggle');
            CREATE INDEX IF NOT EXISTS idx_rr
                ON reaction_roles(guild_id, message_id, emoji);

            -- Модлог (история действий над участником)
            CREATE TABLE IF NOT EXISTS modlog (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                mod_id      INTEGER NOT NULL,
                action      TEXT NOT NULL,
                reason      TEXT DEFAULT '',
                duration    TEXT DEFAULT '',
                created_at  TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS idx_modlog
                ON modlog(guild_id, user_id);

            -- Временные баны
            CREATE TABLE IF NOT EXISTS temp_bans (
                guild_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                mod_id      INTEGER NOT NULL,
                reason      TEXT DEFAULT '',
                unban_at    TEXT NOT NULL,
                unbanned    INTEGER DEFAULT 0,
                PRIMARY KEY (guild_id, user_id));

            -- Карантинные роли
            CREATE TABLE IF NOT EXISTS quarantine (
                guild_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                quarantined_at TEXT NOT NULL,
                release_at  TEXT,
                released    INTEGER DEFAULT 0,
                PRIMARY KEY (guild_id, user_id));

            -- Настройки карантина
            CREATE TABLE IF NOT EXISTS quarantine_settings (
                guild_id    INTEGER PRIMARY KEY,
                role_id     INTEGER DEFAULT 0,
                duration_hours INTEGER DEFAULT 24,
                min_age_days   INTEGER DEFAULT 7,
                enabled     INTEGER DEFAULT 0);

            -- Albion watch (алерты на игрока)
            CREATE TABLE IF NOT EXISTS albion_watch (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                channel_id  INTEGER NOT NULL,
                player_name TEXT NOT NULL,
                player_id   TEXT NOT NULL,
                last_check  TEXT DEFAULT '',
                created_at  TEXT NOT NULL);

            -- Прогрессивные наказания (настройки)
            CREATE TABLE IF NOT EXISTS punishment_settings (
                guild_id    INTEGER PRIMARY KEY,
                warn2_action TEXT DEFAULT 'mute_1h',
                warn3_action TEXT DEFAULT 'mute_24h',
                warn4_action TEXT DEFAULT 'kick',
                warn5_action TEXT DEFAULT 'ban');

            -- Invite лидерборд
            CREATE TABLE IF NOT EXISTS invite_stats (
                guild_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                total_invites INTEGER DEFAULT 0,
                active_invites INTEGER DEFAULT 0,
                left_count  INTEGER DEFAULT 0,
                PRIMARY KEY (guild_id, user_id));

            -- Стилометрический профиль участника
            CREATE TABLE IF NOT EXISTS style_profiles (
                guild_id        INTEGER NOT NULL,
                user_id         INTEGER NOT NULL,
                msg_count       INTEGER DEFAULT 0,
                avg_word_len    REAL DEFAULT 0,
                avg_msg_len     REAL DEFAULT 0,
                punct_ratio     REAL DEFAULT 0,
                caps_ratio      REAL DEFAULT 0,
                emoji_ratio     REAL DEFAULT 0,
                no_punct_ratio  REAL DEFAULT 0,
                common_words    TEXT DEFAULT '{}',
                common_typos    TEXT DEFAULT '{}',
                sentence_enders TEXT DEFAULT '{}',
                active_hours    TEXT DEFAULT '{}',
                updated_at      TEXT DEFAULT '',
                PRIMARY KEY (guild_id, user_id));

            -- Найденные твинк-связи
            CREATE TABLE IF NOT EXISTS twin_links (
                id              INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id        INTEGER NOT NULL,
                user_a          INTEGER NOT NULL,
                user_b          INTEGER NOT NULL,
                similarity      REAL DEFAULT 0,
                reasons         TEXT DEFAULT '[]',
                confirmed       INTEGER DEFAULT 0,
                false_positive  INTEGER DEFAULT 0,
                detected_at     TEXT NOT NULL,
                confirmed_by    INTEGER DEFAULT 0);
            CREATE INDEX IF NOT EXISTS idx_twin_links
                ON twin_links(guild_id, user_a, user_b);
        """)
        await db.commit()

        # Миграции — добавляем новые колонки если их нет (для существующих БД)
        migrations = [
            "ALTER TABLE guild_settings ADD COLUMN tickets_enabled INTEGER DEFAULT 1",
            "ALTER TABLE guild_settings ADD COLUMN quarantine_role INTEGER DEFAULT 0",
            "ALTER TABLE guild_settings ADD COLUMN mod_channel INTEGER DEFAULT 0",
            "ALTER TABLE invite_log ADD COLUMN note TEXT DEFAULT ''",
        ]
        for sql in migrations:
            try:
                await db.execute(sql)
                await db.commit()
            except Exception:
                pass

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

async def get_log_ch(guild):
    ch_id, _ = await get_security(guild.id)
    if ch_id: return guild.get_channel(ch_id)
    return (discord.utils.get(guild.text_channels, name="logs") or
            discord.utils.get(guild.text_channels, name="bot-logs") or
            discord.utils.get(guild.text_channels, name="mod-logs"))

async def sec_check(guild, key):
    if await get_tier(guild.id) < TIER_PREMIUM: return None
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
OWNER_IDS = set()  # {YOUR_USER_ID}

def upsell_embed(req):
    e = make_embed(
        title="🔒 Требуется апгрейд",
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

# ── Кэш настроек сервера (TTL 60 сек) ────────────────────────
# Снижает нагрузку на БД: вместо запроса на каждое событие
# читаем из памяти и обновляем раз в минуту
_settings_cache: dict = {}       # {guild_id: {"data": {...}, "ts": float}}
_SETTINGS_TTL = 60               # секунд

async def get_guild_settings_cached(gid: int) -> dict:
    """Получает настройки сервера с кэшированием TTL 60 сек"""
    now = time.time()
    cached = _settings_cache.get(gid)
    if cached and (now - cached["ts"]) < _SETTINGS_TTL:
        return cached["data"]
    data = await get_guild_settings(gid)
    _settings_cache[gid] = {"data": data, "ts": now}
    return data

def invalidate_settings_cache(gid: int):
    """Сбрасывает кэш настроек при изменении"""
    _settings_cache.pop(gid, None)

async def refresh_invite_cache(guild) -> bool:
    """Загружает инвайты гильдии в кэш. Возвращает True если успешно."""
    try:
        invites = await guild.invites()
        for inv in invites:
            _invite_cache[f"{guild.id}:{inv.code}"] = inv.uses or 0
        print(f"✅ Invite cache loaded for {guild.name}: {len(invites)} invites")
        return True
    except discord.Forbidden:
        print(f"⚠️ [{guild.name}] Нет прав MANAGE_GUILD — инвайт-трекинг отключён")
        return False
    except Exception as ex:
        print(f"⚠️ [{guild.name}] Invite cache error: {ex}")
        return False

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  EVENTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MODLOG + PROGRESSIVE PUNISHMENTS + TEMPBAN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def add_modlog(guild_id: int, user_id: int, mod_id: int,
                     action: str, reason: str = "", duration: str = ""):
    """Записывает действие модератора в историю"""
    now = datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO modlog (guild_id,user_id,mod_id,action,reason,duration,created_at) VALUES (?,?,?,?,?,?,?)",
            (guild_id, user_id, mod_id, action, reason, duration, now)
        )
        await db.commit()


async def apply_progressive_punishment(member: discord.Member, warn_count: int, mod_id: int):
    """Применяет автоматическое наказание по количеству варнов"""
    gid = member.guild.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT warn2_action,warn3_action,warn4_action,warn5_action FROM punishment_settings WHERE guild_id=?",
            (gid,)
        ) as c:
            row = await c.fetchone()
    if not row:
        # Дефолтные правила
        row = ("mute_1h", "mute_24h", "kick", "ban")

    action_map = {
        2: row[0], 3: row[1], 4: row[2], 5: row[3]
    }
    action = action_map.get(warn_count)
    if not action:
        return

    reason = f"Auto: {warn_count} warnings"
    try:
        if action == "mute_1h":
            until = datetime.datetime.now(datetime.timezone.utc) + timedelta(hours=1)
            await member.timeout(until, reason=reason)
            await add_modlog(gid, member.id, mod_id, "AUTO_MUTE_1H", reason)
        elif action == "mute_24h":
            until = datetime.datetime.now(datetime.timezone.utc) + timedelta(hours=24)
            await member.timeout(until, reason=reason)
            await add_modlog(gid, member.id, mod_id, "AUTO_MUTE_24H", reason)
        elif action == "kick":
            await member.kick(reason=reason)
            await add_modlog(gid, member.id, mod_id, "AUTO_KICK", reason)
        elif action == "ban":
            await member.ban(reason=reason)
            await add_modlog(gid, member.id, mod_id, "AUTO_BAN", reason)
    except discord.Forbidden:
        pass


async def tempban_loop(bot_instance):
    """Фоновая задача — проверяет истёкшие временные баны"""
    await bot_instance.wait_until_ready()
    while not bot_instance.is_closed():
        try:
            now = datetime.datetime.utcnow().isoformat()
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT guild_id,user_id,reason FROM temp_bans WHERE unban_at<=? AND unbanned=0",
                    (now,)
                ) as c:
                    rows = await c.fetchall()
            for guild_id, user_id, reason in rows:
                guild = bot_instance.get_guild(guild_id)
                if guild:
                    try:
                        await guild.unban(discord.Object(id=user_id), reason=f"Tempban expired: {reason}")
                        async with aiosqlite.connect(DB_PATH) as db:
                            await db.execute(
                                "UPDATE temp_bans SET unbanned=1 WHERE guild_id=? AND user_id=?",
                                (guild_id, user_id)
                            )
                            await db.commit()
                        await add_modlog(guild_id, user_id, 0, "AUTO_UNBAN", "Tempban expired")
                        print(f"[TEMPBAN] Auto-unbanned {user_id} from {guild.name}")
                    except Exception as ex:
                        print(f"[TEMPBAN] Error unbanning {user_id}: {ex}")
        except Exception as ex:
            print(f"[TEMPBAN LOOP] Error: {ex}")
        await asyncio.sleep(60)  # проверяем каждую минуту


async def quarantine_loop(bot_instance):
    """Снимает карантинную роль по истечении времени"""
    await bot_instance.wait_until_ready()
    while not bot_instance.is_closed():
        try:
            now = datetime.datetime.utcnow().isoformat()
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT q.guild_id,q.user_id FROM quarantine q WHERE q.release_at<=? AND q.released=0",
                    (now,)
                ) as c:
                    rows = await c.fetchall()
            for guild_id, user_id in rows:
                guild = bot_instance.get_guild(guild_id)
                if not guild: continue
                member = guild.get_member(user_id)
                if not member: continue
                # Получаем настройки карантина
                async with aiosqlite.connect(DB_PATH) as db:
                    async with db.execute(
                        "SELECT role_id FROM quarantine_settings WHERE guild_id=?", (guild_id,)
                    ) as c:
                        qrow = await c.fetchone()
                if qrow and qrow[0]:
                    role = guild.get_role(qrow[0])
                    if role and role in member.roles:
                        try:
                            await member.remove_roles(role, reason="Quarantine expired")
                        except discord.Forbidden:
                            pass
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE quarantine SET released=1 WHERE guild_id=? AND user_id=?",
                        (guild_id, user_id)
                    )
                    await db.commit()
        except Exception as ex:
            print(f"[QUARANTINE LOOP] Error: {ex}")
        await asyncio.sleep(60)


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
        bot.loop.create_task(tempban_loop(bot))
        bot.loop.create_task(quarantine_loop(bot))
        bot.loop.create_task(albion_watch_loop(bot))
        bot.loop.create_task(health_check_server())
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

    # Стилометрия — обновляем профиль если есть текст
    if message.content and len(message.content) >= 3:
        asyncio.create_task(update_style_profile(gid, uid, message))
    xp = await get_xp(gid, uid)
    if xp > 0 and xp % 100 < 5:
        await message.channel.send(f"⚡ {message.author.mention} → **Уровень {xp//100}**! 🎉", delete_after=10)
    if await get_tier(gid) >= TIER_PREMIUM and await is_enabled(gid, "anti_spam"):
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
                    e = make_embed(color=C.DANGER)
                    e.set_author(name=t(gid, "antispam_title"))
                    e.add_field(name=t(gid, "member"), value=message.author.mention)
                    e.add_field(name=t(gid, "action"), value=t(gid, "timeout_auto"))
                    await ch.send(embed=e)
            except Exception: pass
    await bot.process_commands(message)

@bot.event
async def on_member_join(member):
    gid = member.guild.id
    if await get_tier(gid) >= TIER_PREMIUM and await is_enabled(gid, "anti_raid"):
        now = time.time()
        _raid_tracker.setdefault(gid, [])
        _raid_tracker[gid] = [t for t in _raid_tracker[gid] if now-t<10]
        _raid_tracker[gid].append(now)
        if len(_raid_tracker[gid]) >= 8:
            try:
                await member.kick(reason="Anti-raid")
                ch = await get_log_ch(member.guild)
                if ch:
                    e = make_embed(color=C.DANGER)
                    e.set_author(name=t(gid, "raid_title"))
                    e.add_field(name=t(gid, "member"), value=member.mention, inline=False)
                    e.add_field(name=t(gid, "reason"), value=t(gid, "raid_reason"), inline=False)
                    await ch.send(embed=e)
                return
            except Exception: pass

    # ── Invite tracking ────────────────────────────────────────
    # Снимок кэша ДО того как Discord обновит счётчики
    old_snapshot = {k: v for k, v in _invite_cache.items() if k.startswith(f"{gid}:")}
    print(f"[INVITE DEBUG] {member.name} joined {member.guild.name}. Cache snapshot: {old_snapshot}")

    used_code = None
    inviter_name = "неизвестно"
    inviter_id = 0

    # Ждём пока Discord обновит счётчик инвайта
    await asyncio.sleep(3)

    try:
        fresh_invites = await member.guild.invites()
        print(f"[INVITE DEBUG] Fresh invites: {[(inv.code, inv.uses) for inv in fresh_invites]}")

        for inv in fresh_invites:
            cache_key = f"{gid}:{inv.code}"
            old_uses = old_snapshot.get(cache_key, 0)
            new_uses = inv.uses or 0
            print(f"[INVITE DEBUG] {inv.code}: old={old_uses} new={new_uses}")
            if new_uses > old_uses:
                used_code = inv.code
                if inv.inviter:
                    inviter_name = inv.inviter.name
                    inviter_id = inv.inviter.id
                _invite_cache[cache_key] = new_uses
                print(f"[INVITE DEBUG] ✅ Found! code={used_code} inviter={inviter_name}")
                break

        # Синхронизируем весь кэш
        for inv in fresh_invites:
            _invite_cache[f"{gid}:{inv.code}"] = inv.uses or 0

    except discord.Forbidden:
        print(f"[INVITE DEBUG] ❌ Forbidden — нет прав MANAGE_GUILD на {member.guild.name}")
    except Exception as ex:
        print(f"[INVITE DEBUG] ❌ Error: {ex}")

    # Разовые инвайты — исчезли из списка после использования
    if not used_code:
        try:
            fresh_codes = {f"{gid}:{inv.code}" for inv in await member.guild.invites()}
            for cache_key in list(old_snapshot.keys()):
                if cache_key not in fresh_codes:
                    used_code = cache_key.split(":", 1)[1]
                    inviter_name = "неизвестно (разовый инвайт)"
                    _invite_cache.pop(cache_key, None)
                    print(f"[INVITE DEBUG] Single-use invite detected: {used_code}")
                    break
        except Exception:
            pass

    print(f"[INVITE DEBUG] Result: code={used_code}, inviter={inviter_name} ({inviter_id})")

    # Пишем в БД всегда (для /invcheck и /invuser)
    if used_code and inviter_id:
        await log_invite_use(gid, used_code, inviter_id, inviter_name, member.id, member.name)

    age = (datetime.datetime.utcnow() - member.created_at.replace(tzinfo=None)).days
    ch = await sec_check(member.guild, "joins")
    if ch:
        sus = "🔴 Подозрительный (<7д)" if age<7 else "🟡 Новый (<30д)" if age<30 else "🟢 Обычный"
        e = discord.Embed(title="📥 Вход", color=discord.Color.green(), timestamp=datetime.datetime.utcnow())
        e.set_thumbnail(url=member.display_avatar.url)
        e.add_field(name="Участник", value=f"{member.mention} (`{member.name}`)", inline=False)
        e.add_field(name="Аккаунт", value=f"{age} дней — {sus}", inline=True)
        e.add_field(name="Инвайт", value=f"`{used_code}`" if used_code else "неизвестно", inline=True)
        e.add_field(name="Пригласил", value=f"{inviter_name} (`{inviter_id}`)" if inviter_id else "неизвестно", inline=True)
        await ch.send(embed=e)

    ch2 = await sec_check(member.guild, "suspicious")
    if ch2 and age < 7:
        e = discord.Embed(title="🚨 Подозрительный аккаунт", color=discord.Color.red(), timestamp=datetime.datetime.utcnow())
        e.set_thumbnail(url=member.display_avatar.url)
        e.add_field(name="Участник", value=f"{member.mention}", inline=False)
        e.add_field(name="Возраст", value=f"{age} дней", inline=True)
        e.add_field(name="Инвайт", value=f"`{used_code}`" if used_code else "неизвестно", inline=True)
        await ch2.send(embed=e)

    # Карантинная роль для новых аккаунтов (auto-quarantine)
    async with aiosqlite.connect(DB_PATH) as _qdb:
        async with _qdb.execute(
            "SELECT role_id,duration_hours,min_age_days,enabled FROM quarantine_settings WHERE guild_id=?",
            (gid,)
        ) as _qc:
            qsettings = await _qc.fetchone()
    if qsettings and qsettings[3] and age < qsettings[2]:
        qrole = member.guild.get_role(qsettings[0])
        if qrole:
            try:
                await member.add_roles(qrole, reason=f"Auto-quarantine: account {age}d old")
                release_at = (datetime.datetime.utcnow() + timedelta(hours=qsettings[1])).isoformat()
                async with aiosqlite.connect(DB_PATH) as _qdb2:
                    await _qdb2.execute(
                        "INSERT INTO quarantine (guild_id,user_id,quarantined_at,release_at) VALUES (?,?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET released=0,release_at=excluded.release_at",
                        (gid, member.id, datetime.datetime.utcnow().isoformat(), release_at)
                    )
                    await _qdb2.commit()
            except discord.Forbidden:
                pass

@bot.event
async def on_member_remove(member):
    ch = await sec_check(member.guild, "leaves")
    if not ch: return
    roles = [r.mention for r in member.roles if r.name != "@everyone"]
    gid2 = member.guild.id
    e = make_embed(color=C.DANGER, thumbnail=member.display_avatar.url)
    e.set_author(name=t(gid2, "left"))
    e.add_field(name=t(gid2, "member"), value=f"{member.mention} · `{member.name}`", inline=False)
    e.add_field(name=t(gid2, "roles"),  value=", ".join(roles) if roles else "—",    inline=False)
    await ch.send(embed=e)

@bot.event
async def on_member_ban(guild, user):
    ch = await sec_check(guild, "bans")
    if not ch: return
    gid3 = guild.id
    e = make_embed(color=C.DANGER)
    e.set_author(name=t(gid3, "banned"))
    e.add_field(name=t(gid3, "member"), value=f"{user.mention} · `{user.name}`", inline=False)
    await ch.send(embed=e)

@bot.event
async def on_member_unban(guild, user):
    ch = await sec_check(guild, "bans")
    if not ch: return
    gid4 = guild.id
    e = make_embed(color=C.SUCCESS)
    e.set_author(name=t(gid4, "unbanned"))
    e.add_field(name=t(gid4, "member"), value=user.mention, inline=False)
    await ch.send(embed=e)

@bot.event
async def on_member_update(before, after):
    if before.nick != after.nick:
        ch = await sec_check(after.guild, "nick_change")
        if ch:
            gid5 = after.guild.id
            e = make_embed(color=C.INFO)
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
            e = make_embed(color=C.PRIMARY)
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
                e = make_embed(color=C.WARNING)
                e.set_author(name=t(gid7, "muted"))
                e.add_field(name=t(gid7, "member"), value=after.mention, inline=False)
                e.add_field(name=t(gid7, "until"),  value=after.timed_out_until.strftime("%d.%m.%Y %H:%M"), inline=True)
            else:
                e = make_embed(color=C.SUCCESS)
                e.set_author(name=t(gid7, "unmuted"))
                e.add_field(name=t(gid7, "member"), value=after.mention, inline=False)
            await ch.send(embed=e)

@bot.event
async def on_message_delete(message):
    if message.author.bot or not message.guild: return
    ch = await sec_check(message.guild, "msg_delete")
    if not ch: return
    gid8 = message.guild.id
    e = make_embed(color=C.DANGER)
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
    e = make_embed(color=C.WARNING)
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
    e = make_embed(color=C.INFO)
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
    e = make_embed(color=C.MUTED)
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
    e = make_embed(color=color)
    e.set_author(name=t(gid12, "voice_update"))
    e.add_field(name=t(gid12, "member"), value=member.mention, inline=True)
    e.add_field(name=t(gid12, "action"), value=desc,           inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_channel_create(channel_created):
    ch = await sec_check(channel_created.guild, "channels")
    if not ch: return
    e = make_embed(color=C.SUCCESS)
    e.set_author(name="Channel created")
    e.add_field(name="Канал", value=channel_created.mention, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_channel_delete(channel_deleted):
    ch = await sec_check(channel_deleted.guild, "channels")
    if not ch: return
    e = make_embed(color=C.DANGER)
    e.set_author(name="Channel deleted")
    e.add_field(name="Канал", value=channel_deleted.name, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_role_create(role):
    ch = await sec_check(role.guild, "roles")
    if not ch: return
    e = make_embed(color=C.SUCCESS)
    e.set_author(name="Role created")
    e.add_field(name="Роль", value=role.mention, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_role_delete(role):
    ch = await sec_check(role.guild, "roles")
    if not ch: return
    e = make_embed(color=C.DANGER)
    e.set_author(name="Role deleted")
    e.add_field(name="Роль", value=role.name, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_update(before, after):
    ch = await sec_check(after, "server_edit")
    if not ch or before.name == after.name: return
    e = make_embed(color=C.INFO)
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
        e = make_embed(color=C.INFO)
        e.set_author(name="Avatar changed")
        e.add_field(name="Member", value=member.mention, inline=False)
        e.set_thumbnail(url=after.display_avatar.url)
        await ch.send(embed=e)


@bot.event
async def on_thread_create(thread):
    ch = await sec_check(thread.guild, "threads")
    if not ch: return
    e = make_embed(color=C.SUCCESS)
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
    e = make_embed(color=C.PRIMARY)
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
        e = make_embed(
            title="Language set to English 🇬🇧",
            description="All bot responses will now be in **English**.\nUse `/lang language:ru` to switch back.",
            color=C.SUCCESS
        )
    else:
        e = make_embed(
            title="Язык изменён на Русский 🇷🇺",
            description="Все ответы бота теперь на **русском** языке.\nИспользуй `/lang language:en` для переключения.",
            color=C.SUCCESS
        )
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="setpremium", description="[ADMIN] Установить тир")
@app_commands.describe(tier="0=Free 1=Premium 2=Pro", days="Дней")
async def setpremium(interaction: discord.Interaction, tier: int, days: int = 30):
    if interaction.user.id not in OWNER_IDS and not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ Нет доступа.", ephemeral=True)
    await set_tier(interaction.guild_id, tier, days)
    await interaction.response.send_message(f"✅ **{TIER_NAMES.get(tier,'?')}** на {days} дней.", ephemeral=True)

@bot.tree.command(name="sechelp", description="Advanced Security команды [-q prefix] — только для администраторов")
async def sechelp(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message(
            embed=make_embed("🔒 Нет доступа", "Только для администраторов.", C.DANGER),
            ephemeral=True
        )
    e = make_embed(
        title="🔐 Advanced Security — префикс `-q`",
        description=(
            "Расширенный модуль безопасности с AI анализом.\n"
            "Все команды доступны **только администраторам**."
        ),
        color=0x5865F2,
        footer="Witness Advanced Security"
    )
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
    e = discord.Embed(title="📋 Подписка", color=TIER_COLORS[tier])
    e.add_field(name="Тир", value=TIER_NAMES[tier], inline=True)
    if tier == TIER_FREE: e.add_field(name="Апгрейд", value="witnessbot.gg/premium", inline=True)
    await interaction.response.send_message(embed=e)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SECURITY COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

sec_grp = app_commands.Group(name="security", description="🛡️ Безопасность сервера [Premium]")

@sec_grp.command(name="status", description="Статус всех модулей")
async def sec_status(interaction: discord.Interaction):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    log_ch, settings = await get_security(interaction.guild_id)
    ch_obj = interaction.guild.get_channel(log_ch)
    e = discord.Embed(title="🛡️ Security Status", color=0x00E5FF)
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
    note="Заметка (например: 'Реклама Reddit'). Пусто = удалить заметку"
)
async def invnote(interaction: discord.Interaction, code: str, note: str = ""):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно право Manage Server.", ephemeral=True)
    gid = interaction.guild_id
    code = code.strip().removeprefix("https://discord.gg/").removeprefix("discord.gg/")

    async with aiosqlite.connect(DB_PATH) as db:
        if note:
            # Обновляем note во всех записях с этим кодом
            await db.execute(
                "UPDATE invite_log SET note=? WHERE guild_id=? AND invite_code=?",
                (note, gid, code)
            )
            # Если записей ещё нет — вставляем placeholder
            async with db.execute(
                "SELECT COUNT(*) FROM invite_log WHERE guild_id=? AND invite_code=?", (gid, code)
            ) as c:
                count = (await c.fetchone())[0]
            if count == 0:
                await db.execute(
                    "INSERT INTO invite_log (guild_id, invite_code, note) VALUES (?,?,?)",
                    (gid, code, note)
                )
            await db.commit()
            e = discord.Embed(title="📝 Заметка сохранена", color=0x57F287)
            e.add_field(name="Код",      value=f"`{code}`",             inline=True)
            e.add_field(name="Заметка",  value=note,                    inline=True)
            e.add_field(name="Добавил",  value=interaction.user.mention, inline=True)
        else:
            await db.execute(
                "UPDATE invite_log SET note='' WHERE guild_id=? AND invite_code=?", (gid, code)
            )
            await db.commit()
            e = discord.Embed(title="🗑️ Заметка удалена", description=f"Код: `{code}`", color=0x36393F)

    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="invnotes", description="Список всех заметок к инвайтам")
async def invnotes(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно право Manage Server.", ephemeral=True)
    gid = interaction.guild_id

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT invite_code, note, MAX(joined_at) as last_use, COUNT(*) as uses
            FROM invite_log
            WHERE guild_id=? AND note != '' AND note IS NOT NULL
            GROUP BY invite_code
            ORDER BY last_use DESC
        """, (gid,)) as c:
            rows = await c.fetchall()

    e = discord.Embed(title="📝 Invite Notes", color=0x00B0F4)
    if not rows:
        e.description = "Заметок нет. Добавь через `/invnote code:КОД note:ЗАМЕТКА`"
    else:
        for code, note, last_use, uses in rows[:15]:
            date_str = last_use[:10] if last_use else "—"
            e.add_field(
                name=f"`{code}`",
                value=f"{note}\n*{uses} uses · last: {date_str}*",
                inline=False
            )
        if len(rows) > 15:
            e.set_footer(text=f"Показано 15 из {len(rows)}")
    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="invcheck", description="История инвайта [Premium]")
@app_commands.describe(code="Код инвайта")
async def invcheck(interaction: discord.Interaction, code: str):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    rows = await get_invite_history(interaction.guild_id, code)
    e = discord.Embed(title=f"🔗 Инвайт: {code}", color=discord.Color.teal())
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
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    rows = await get_user_invites(interaction.guild_id, member.id)
    e = discord.Embed(title=f"👤 Инвайты: {member.display_name}", color=discord.Color.blurple())
    e.add_field(name="Всего приглашено", value=str(len(rows)), inline=True)
    if rows:
        lines = [f"`{r[0]}` — **{r[1]}** — {r[2][:10]}" for r in rows[:15]]
        e.add_field(name="Приглашённые", value="\n".join(lines), inline=False)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="invdel", description="Удалить инвайт [Premium]")
@app_commands.describe(code="Код инвайта")
async def invdel(interaction: discord.Interaction, code: str):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
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
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.moderate_members:
        return await interaction.response.send_message("❌ Нужно Moderate Members.", ephemeral=True)
    await add_warning(interaction.guild_id, member.id, interaction.user.id, reason)
    warns = await get_warnings(interaction.guild_id, member.id)
    warn_bar = bar(len(warns), 5, 8)
    color = C.WARNING if len(warns) < 3 else C.DANGER
    e = make_embed(color=color)
    e.set_author(name=f"Предупреждение выдано · {member.display_name}", icon_url=member.display_avatar.url)
    e.add_field(name="Участник",    value=member.mention,    inline=True)
    e.add_field(name="Модератор",   value=interaction.user.mention, inline=True)
    e.add_field(name="Причина",     value=reason,            inline=False)
    e.add_field(name="Варнов всего", value=f"**{len(warns)}/5** `{warn_bar}`", inline=True)
    await interaction.response.send_message(embed=e)
    if len(warns) >= 3:
        try:
            await member.timeout(timedelta(hours=1), reason=f"Авто-таймаут: {len(warns)} варнов")
            await interaction.channel.send(f"🔇 {member.mention} → авто-таймаут (3 варна)")
        except Exception: pass

@bot.tree.command(name="warnings", description="Список варнов [Premium]")
@app_commands.describe(member="Пользователь")
async def warnings(interaction: discord.Interaction, member: discord.Member):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    rows = await get_warnings(interaction.guild_id, member.id)
    color = C.DANGER if len(rows) >= 3 else C.WARNING if rows else C.SUCCESS
    e = make_embed(color=color, thumbnail=member.display_avatar.url)
    e.set_author(name=f"Варны: {member.display_name}", icon_url=member.display_avatar.url)
    if not rows:
        e.description = "Варнов нет."
    else:
        warn_bar = bar(len(rows), 5, 8)
        e.add_field(name="Всего", value=f"**{len(rows)}/5** `{warn_bar}`", inline=False)
        for wid, mod_id, reason, created in rows:
            mod = interaction.guild.get_member(mod_id)
            e.add_field(
                name=f"#{wid} · {created[:10]}",
                value=f"Модератор: {mod.mention if mod else mod_id}\nПричина: {reason}",
                inline=False
            )
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="clearwarn", description="Снять варн [Premium]")
@app_commands.describe(warn_id="ID варна")
async def clearwarn(interaction: discord.Interaction, warn_id: int):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.moderate_members:
        return await interaction.response.send_message("❌ Нужно Moderate Members.", ephemeral=True)
    await remove_warning(warn_id, interaction.guild_id)
    await interaction.response.send_message(f"✅ Варн `#{warn_id}` снят.", ephemeral=True)

@bot.tree.command(name="purge", description="Удалить N сообщений [Premium]")
@app_commands.describe(count="Количество (1-100)")
async def purge(interaction: discord.Interaction, count: int):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
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
    tier_name = TIER_NAMES.get(guild_tier, "Free")
    tier_bar = bar(guild_tier, 2, 5)

    e = make_embed(
        title=data["title"],
        color=data["color"],
        footer=f"Тир: {tier_badge(guild_tier)} · witnessbot.gg"
    )

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
    quality = "Отлично" if ms < 100 else "Нормально" if ms < 200 else "Плохо"
    bar_str = bar(max(0, 200 - ms), 200, 10)
    e = make_embed(title="Witness · Pong!", color=color)
    e.add_field(name="Latency", value=f"**{ms}ms**", inline=True)
    e.add_field(name="Качество", value=quality, inline=True)
    e.add_field(name="Статус", value=f"`{bar_str}`", inline=True)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="userinfo")
@app_commands.describe(member="Пользователь")
async def userinfo(interaction: discord.Interaction, member: discord.Member = None):
    m = member or interaction.user
    e = discord.Embed(title=f"👤 {m.display_name}", color=0x00E5FF)
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
    e = make_embed(
        color=TIER_COLORS[tier],
        thumbnail=g.icon.url if g.icon else "",
        footer=f"ID: {g.id}"
    )
    e.set_author(name=g.name, icon_url=g.icon.url if g.icon else None)
    e.add_field(name="Участники",  value=f"**{humans}** люди · {bots} боты",                    inline=True)
    e.add_field(name="Каналы",     value=f"**{len(g.text_channels)}** текст · {len(g.voice_channels)} голос", inline=True)
    e.add_field(name="Роли",       value=f"**{len(g.roles)}**",                                  inline=True)
    e.add_field(name="Буст",       value=f"Уровень **{g.premium_tier}** · {g.premium_subscription_count}×", inline=True)
    e.add_field(name="Возраст",    value=f"**{age}** дней",                                      inline=True)
    e.add_field(name="Witness",    value=tier_badge(tier),                                        inline=True)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="rank")
async def rank(interaction: discord.Interaction):
    xp     = await get_xp(interaction.guild_id, interaction.user.id)
    coins  = await get_coins(interaction.guild_id, interaction.user.id)
    lvl    = xp // 100
    prog   = xp % 100
    bar_s  = bar(prog, 100, 12)
    e = make_embed(color=C.PRIMARY, thumbnail=interaction.user.display_avatar.url)
    e.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
    e.add_field(name="Уровень",  value=f"**{lvl}**",    inline=True)
    e.add_field(name="XP",       value=f"**{xp:,}**",   inline=True)
    e.add_field(name="Монеты",   value=f"**{coins:,}**", inline=True)
    e.add_field(
        name=f"До уровня {lvl+1} — {prog}/100 XP",
        value=f"`{bar_s}` **{prog}%**",
        inline=False
    )
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="leaderboard")
async def leaderboard(interaction: discord.Interaction):
    rows = await get_leaderboard(interaction.guild_id)
    medals = ["🥇","🥈","🥉","4.","5.","6.","7.","8.","9.","10."]
    e = make_embed(title="Топ активных участников", color=C.GOLD)
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
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="coins")
async def coins_cmd(interaction: discord.Interaction):
    c = await get_coins(interaction.guild_id, interaction.user.id)
    xp = await get_xp(interaction.guild_id, interaction.user.id)
    e = make_embed(title="💰 Баланс", color=C.GOLD)
    e.add_field(name="🪙 Монеты", value=f"**{c:,}**", inline=True)
    e.add_field(name="⚡ XP", value=f"**{xp:,}**", inline=True)
    e.add_field(name="🏆 Уровень", value=f"**{xp//100}**", inline=True)
    e.set_footer(text=f"Witness · +1 монета за каждое сообщение")
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="poll")
@app_commands.describe(question="Вопрос", option1="Вариант 1", option2="Вариант 2", option3="Вариант 3", option4="Вариант 4", duration="Длительность в минутах (0 = без ограничения)")
async def poll(interaction: discord.Interaction, question: str, option1: str, option2: str, option3: str = None, option4: str = None, duration: int = 0):
    options = [o for o in [option1, option2, option3, option4] if o]
    emojis  = ["1️⃣","2️⃣","3️⃣","4️⃣"]
    e = discord.Embed(title=f"📊 {question}", color=0x00E5FF)
    for i, opt in enumerate(options):
        e.add_field(name=f"{emojis[i]} {opt}", value="​", inline=False)
    if duration > 0:
        e.set_footer(text=f"Закроется через {duration} мин")
    await interaction.response.send_message(embed=e)
    msg = await interaction.original_response()
    for i in range(len(options)):
        await msg.add_reaction(emojis[i])
    # Авто-закрытие
    if duration > 0:
        await asyncio.sleep(duration * 60)
        try:
            msg = await msg.channel.fetch_message(msg.id)
            results = [f"{r.emoji} — {r.count-1} голосов" for r in msg.reactions]
            result_e = discord.Embed(color=0xFEE75C, timestamp=datetime.datetime.utcnow())
            result_e.set_author(name=f"Poll closed — {question}")
            result_e.description = "\n".join(results) or "Нет голосов."
            await msg.channel.send(embed=result_e, reference=msg)
        except Exception:
            pass

@bot.tree.command(name="remind")
@app_commands.describe(minutes="Через сколько минут", message="Текст", repeat="Повторять каждые N минут (0 = без повтора)")
async def remind(interaction: discord.Interaction, minutes: int, message: str, repeat: int = 0):
    if not 1 <= minutes <= 10080:
        return await interaction.response.send_message("⚠️ 1–10080 мин.", ephemeral=True)
    suffix = f" · повтор каждые {repeat} мин" if repeat > 0 else ""
    await interaction.response.send_message(f"⏰ Напомню через **{minutes} мин**!{suffix}", ephemeral=True)
    count = 0
    max_repeats = 20
    current_minutes = minutes
    while count == 0 or (repeat > 0 and count < max_repeats):
        await asyncio.sleep(current_minutes * 60)
        try:
            e = discord.Embed(description=f"⏰ {message}", color=0x00B0F4, timestamp=datetime.datetime.utcnow())
            e.set_author(name="Reminder")
            if repeat > 0:
                e.set_footer(text=f"Повтор {count+1}/{max_repeats} · каждые {repeat} мин")
            await interaction.user.send(embed=e)
        except discord.Forbidden:
            try:
                await interaction.channel.send(f"⏰ {interaction.user.mention} {message}")
            except Exception:
                pass
        count += 1
        current_minutes = repeat if repeat > 0 else minutes
        if repeat == 0:
            break

@bot.tree.command(name="lfg")
@app_commands.describe(game="Игра", slots="Нужно игроков", note="Дополнительно")
async def lfg(interaction: discord.Interaction, game: str, slots: int = 1, note: str = ""):
    e = discord.Embed(title=f"🎮 LFG — {game}", color=0x00FF9D)
    e.description = f"**{interaction.user.display_name}** ищет **{slots}** игрока(-ов)"
    if note:
        e.add_field(name="📝", value=note, inline=False)
    e.set_footer(text="Нажми Join чтобы вступить · авто-удаление через 2 часа")

    class LFGJoinView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=7200)
            self.joined = [interaction.user]

        @discord.ui.button(label=f"Join (1/{slots})", style=discord.ButtonStyle.success)
        async def join_btn(self, inter: discord.Interaction, button: discord.ui.Button):
            if inter.user in self.joined:
                return await inter.response.send_message("Ты уже в группе.", ephemeral=True)
            self.joined.append(inter.user)
            button.label = f"Join ({len(self.joined)}/{slots})"
            if len(self.joined) >= slots:
                button.disabled = True
                button.label = f"Full ({len(self.joined)}/{slots})"
                try:
                    names = ", ".join(m.display_name for m in self.joined)
                    await interaction.user.send(f"✅ Группа собрана! Участники: {names}")
                except Exception:
                    pass
            await inter.response.edit_message(view=self)
            await inter.followup.send(f"✅ Вступил в группу!", ephemeral=True)

        async def on_timeout(self):
            try:
                msg = await interaction.original_response()
                await msg.delete()
            except Exception:
                pass

    await interaction.response.send_message(embed=e, view=LFGJoinView())

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
        desc = d["weather"][0]["description"].capitalize()
        temp = d["main"]["temp"]
        feels = d["main"]["feels_like"]
        humidity = d["main"]["humidity"]
        wind = d["wind"]["speed"]
        e = make_embed(color=C.INFO)
        e.set_author(name=f"{d['name']}, {d['sys']['country']}")
        e.add_field(name="Температура", value=f"**{temp:.1f}°C** (ощущается {feels:.1f}°C)", inline=True)
        e.add_field(name="Описание",    value=desc,                                          inline=True)
        e.add_field(name="Влажность",   value=f"**{humidity}%**",                            inline=True)
        e.add_field(name="Ветер",       value=f"**{wind} м/с**",                             inline=True)
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
        e = discord.Embed(title=f"🌍 → {target}", color=0x00E5FF)
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
        kd_bar = bar(min(kf / max(df, 1), 5), 5, 8) if df else "████████"
        e = make_embed(color=C.INFO, footer=f"EU · Albion Online")
        e.set_author(name=pname, icon_url=f"https://render.albiononline.com/v1/player/{pname}/avatar?size=40")
        e.add_field(name="Гильдия",    value=p.get("GuildName") or "—",  inline=True)
        e.add_field(name="Альянс",     value=p.get("AllianceName") or "—", inline=True)
        e.add_field(name="K/D",        value=f"**{kd}** `{kd_bar}`",      inline=True)
        e.add_field(name="Kill Fame",  value=f"**{kf:,}**",               inline=True)
        e.add_field(name="Death Fame", value=f"**{df:,}**",               inline=True)
        e.add_field(name="PvE Fame",   value=f"**{pve:,}**",              inline=True)
        await interaction.followup.send(embed=e)
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
        if not evs: return await interaction.followup.send(f"Нет недавних убийств у **{pname}**.")
        e = make_embed(color=C.DANGER, footer=f"EU · Albion Online")
        e.set_author(name=f"{pname} — последние убийства")
        total_fame = sum(ev.get("TotalVictimKillFame", 0) for ev in evs[:5])
        e.description = f"За последние 5 убийств заработано **{total_fame:,}** fame"
        for ev in evs[:5]:
            v      = ev.get("Victim", {})
            weapon = fmt_item(v.get("Equipment", {}).get("MainHand", {}).get("Type", "") if v.get("Equipment") else "")
            fame   = ev.get("TotalVictimKillFame", 0)
            date   = ev.get("TimeStamp", "")[:10]
            e.add_field(
                name=f"{v.get('Name', '?')} · {date}",
                value=f"Fame: **{fame:,}** · {weapon}",
                inline=True
            )
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
        if not evs: return await interaction.followup.send(f"Нет недавних смертей у **{pname}**.")
        e = make_embed(color=C.MUTED, footer="EU · Albion Online")
        e.set_author(name=f"{pname} — последние смерти")
        total = sum(ev.get("TotalVictimKillFame", 0) for ev in evs[:5])
        e.description = f"Потеряно **{total:,}** fame в 5 последних смертях"
        for ev in evs[:5]:
            k    = ev.get("Killer", {})
            fame = ev.get("TotalVictimKillFame", 0)
            date = ev.get("TimeStamp", "")[:10]
            e.add_field(
                name=f"Убит: {k.get('Name', '?')} · {date}",
                value=f"Потеряно fame: **{fame:,}**",
                inline=True
            )
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
        e = discord.Embed(title=f"🏰 {gdata.get('Name',name)}", color=0x00E5FF)
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
        e = make_embed(
            title="Последние ZvZ битвы",
            description=f"Данные по {len(battles[:5])} последним сражениям",
            color=C.DANGER, footer="EU · Albion Online"
        )
        for b in battles[:5]:
            guilds = list(b.get("Guilds", {}).keys())[:3]
            name_str = " vs ".join(guilds) if guilds else "Open World"
            kills = b.get("TotalKills", 0)
            fame  = b.get("TotalFame", 0)
            date  = b.get("StartTime", "")[:10]
            e.add_field(
                name=f"{name_str} · {date}",
                value=f"Убийств: **{kills}** · Fame: **{fame:,}**",
                inline=False
            )
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
        e = discord.Embed(title=f"⚔️ {p1name} vs {p2name}", color=0x00E5FF)
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
        kd_week = round(len(wk) / len(wd), 2) if wd else "∞"
        activity = "Очень активен" if len(wk) > 20 else "Активен" if len(wk) > 5 else "Тихая неделя"
        act_bar  = bar(min(len(wk), 30), 30, 10)
        e = make_embed(color=C.SUCCESS, footer="EU · Albion Online · 7 дней")
        e.set_author(name=f"{pname} — активность за 7 дней")
        e.add_field(name="Убийств",  value=f"**{len(wk)}**",   inline=True)
        e.add_field(name="Смертей",  value=f"**{len(wd)}**",   inline=True)
        e.add_field(name="K/D",      value=f"**{kd_week}**",   inline=True)
        e.add_field(name="Fame",     value=f"**{fame:,}**",    inline=True)
        e.add_field(name="Активность", value=f"{activity} `{act_bar}`", inline=True)
        if wk:
            victims = {}
            for ev in wk:
                n = ev.get("Victim", {}).get("Name", "?")
                victims[n] = victims.get(n, 0) + 1
            top = max(victims, key=victims.get)
            e.add_field(name="Любимая жертва", value=f"**{top}** × {victims[top]}", inline=True)
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
        e = discord.Embed(title=f"⚔️ OSRS — {username}", color=0xB5651D)
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
        e = discord.Embed(title=f"🟢 {address}", color=0x00FF9D)
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
        e = make_embed(description=answer[:4000], color=C.PREMIUM)
        e.set_author(name="Witness AI", icon_url=interaction.user.display_avatar.url)
        e.add_field(name="Запрос", value=f"`{question[:100]}`", inline=False)
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
        await interaction.followup.send(embed=discord.Embed(title=f"📋 Резюме ({count} сообщений)", description=summary, color=0x00E5FF))
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
        e = discord.Embed(title=f"🔥 {member.display_name}", description=text, color=0xFF6B35)
        e.set_thumbnail(url=member.display_avatar.url)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="giveaway")
@app_commands.describe(prize="Приз", duration="Минут")
async def giveaway(interaction: discord.Interaction, prize: str, duration: int = 60):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    e = discord.Embed(title="🎉 РОЗЫГРЫШ", description=f"**Приз:** {prize}\n🎮 — участие\n⏰ **{duration} мин**", color=0x00FF9D)
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
        rr        = data.get("ranking_in_tier", 0)
        peak      = data.get("highest_rank", {}).get("patched_tier", "?")
        rr_bar    = bar(rr, 100, 10)
        e = make_embed(color=0xFF4655, footer="Valorant · EU")
        e.set_author(name=username)
        e.add_field(name="Ранг",       value=f"**{rank_name}**",         inline=True)
        e.add_field(name="RR",         value=f"**{rr}/100** `{rr_bar}`", inline=True)
        e.add_field(name="Пик",        value=f"**{peak}**",              inline=True)
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
        hs_bar = bar(hs_pct, 100, 8)
        e = make_embed(color=0xF0A500, footer="CS2 · Steam")
        e.set_author(name=f"CS2 — {steam_id}")
        e.add_field(name="K/D",      value=f"**{kd}**",                        inline=True)
        e.add_field(name="Убийств",  value=f"**{kills:,}**",                   inline=True)
        e.add_field(name="Побед",    value=f"**{wins:,}**",                    inline=True)
        e.add_field(name="HS%",      value=f"**{hs_pct}%** `{hs_bar}`",        inline=True)
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
        e = make_embed(color=0xC89B3C, footer=f"League of Legends · {region.upper()}")
        e.set_author(name=summoner)
        if not entries:
            e.description = "Unranked this season."
        for en in entries:
            w, l = en["wins"], en["losses"]
            wr   = round(w / (w + l) * 100, 1) if (w + l) else 0
            wr_b = bar(wr, 100, 8)
            e.add_field(
                name=en["queueType"].replace("_", " ").title(),
                value=(
                    f"**{en['tier']} {en['rank']}** · {en['leaguePoints']} LP\n"
                    f"{w}W / {l}L · **{wr}%** WR `{wr_b}`"
                ),
                inline=True
            )
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
        e = discord.Embed(title=f"⚔️ Lost Ark — {character}", color=0x3D9BD4)
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
                        upd_dt = datetime.datetime.fromisoformat(bm_updated.replace("Z", "+00:00").replace("+00:00", ""))
                        data_age_hours = round((datetime.datetime.utcnow() - upd_dt).total_seconds() / 3600, 1)
                        data_stale = data_age_hours > 3  # данные старше 3 часов — предупреждение
                    except Exception:
                        pass

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
    if await get_tier(interaction.guild_id) < TIER_PRO:
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
        desc_lines = f"{server_name} · Топ по % профиту\n⚠️ Цены ЧР приблизительные — проверяй в игре перед продажей!"
        if stale_count:
            desc_lines += f"\n⚠️ **{stale_count} предметов** с устаревшими данными (>3ч)"
        e = make_embed(
            title=f"💰 Чёрный рынок — {cat_label} T{tier}",
            description=desc_lines,
            color=color,
            footer=f"albion-online-data.com · {len(results)} предметов · стр. {page_num}/{total_pages}"
        )
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
    e = make_embed(color=C.SUCCESS, footer="EU · Albion Online")
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
    e = discord.Embed(title=f"🏆 {name}", color=0xFFD700)
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
                        "lockdown": 0, "price_watch": "{}", "tickets_enabled": 1}
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
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ Нужны права администратора.", ephemeral=True)
    enabled = action.lower() in ("on", "вкл", "yes", "1")
    await set_guild_setting(interaction.guild_id, "lockdown", 1 if enabled else 0)
    if enabled:
        e = discord.Embed(title="🔒 ЛОКДАУН ВКЛЮЧЁН", color=0xFF0000)
        e.add_field(name="Статус", value="Новые участники с аккаунтом младше **{} дней** будут автоматически кикнуты".format(min_age), inline=False)
        e.add_field(name="Выключить", value="`/lockdown action:off`", inline=False)
        # Store min_age in settings
        _, settings = await get_security(interaction.guild_id)
        settings["lockdown_min_age"] = min_age
        log_ch_id, _ = await get_security(interaction.guild_id)
        await save_security(interaction.guild_id, log_ch_id, settings)
    else:
        e = discord.Embed(title="🔓 Локдаун выключен", color=0x00FF9D)
        e.description = "Новые участники снова могут заходить свободно."
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="slowmode", description="Установить slow mode в канале [Premium]")
@app_commands.describe(seconds="Задержка в секундах (0 = выключить, макс 21600)")
async def slowmode(interaction: discord.Interaction, seconds: int = 0):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
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
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    ch = await get_log_ch(interaction.guild)
    if not ch:
        return await interaction.response.send_message("❌ Канал логов не настроен. Используй `/security setlog`", ephemeral=True)
    try:
        msg_id = int(message_id)
        msg = await interaction.channel.fetch_message(msg_id)
        e = discord.Embed(title="🚨 Жалоба на сообщение", color=0xFF4444, timestamp=datetime.datetime.utcnow())
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
                e = discord.Embed(title="🎂 День рождения!", color=0xFF69B4)
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
@app_commands.describe(action="open / close / setup / enable / disable", reason="Причина обращения")
async def ticket(interaction: discord.Interaction, action: str = "open", reason: str = "Обращение в поддержку"):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)

    if action.lower() == "setup":
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("❌ Нужны права администратора.", ephemeral=True)
        # Создаём категорию для тикетов
        cat = await interaction.guild.create_category("🎫 Tickets")
        await set_guild_setting(interaction.guild_id, "ticket_category", cat.id)
        await set_guild_setting(interaction.guild_id, "tickets_enabled", 1)
        e = discord.Embed(title="✅ Тикеты настроены", color=0x00E5FF)
        e.add_field(name="Категория", value=cat.name, inline=True)
        e.add_field(name="Статус", value="✅ Включены", inline=True)
        e.add_field(name="Использование", value="`/ticket` — открыть · `close` — закрыть · `disable/enable` — вкл/выкл", inline=False)
        return await interaction.response.send_message(embed=e)

    if action.lower() in ("disable", "enable"):
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("❌ Нужны права администратора.", ephemeral=True)
        enabled = action.lower() == "enable"
        await set_guild_setting(interaction.guild_id, "tickets_enabled", 1 if enabled else 0)
        status = "✅ Тикеты включены" if enabled else "🔒 Тикеты отключены"
        desc = "Участники могут открывать тикеты." if enabled else "Новые тикеты открыть нельзя. Существующие не затронуты."
        e = discord.Embed(title=status, description=desc, color=0x00E5FF if enabled else 0x36393F)
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

    # Проверяем включены ли тикеты
    if not settings.get("tickets_enabled", 1):
        return await interaction.response.send_message(
            "❌ Система тикетов отключена на этом сервере.", ephemeral=True
        )

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

    e = discord.Embed(title="🎫 Тикет открыт", color=0x00E5FF, timestamp=datetime.datetime.utcnow())
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
        e = discord.Embed(title="💡 Топ предложений", color=0x00E5FF)
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

    e = discord.Embed(title="💡 Предложение", description=text, color=0x7C3AED, timestamp=datetime.datetime.utcnow())
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
    e = make_embed(color=C.SUCCESS)
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

    e = discord.Embed(title=f"📊 Статистика: {g.name}", color=0x00E5FF, timestamp=datetime.datetime.utcnow())
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
        e = discord.Embed(title="🎨 Сгенерированное изображение", color=0x7C3AED)
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
    if await get_tier(interaction.guild_id) < TIER_PRO:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)

    if not GOOGLE_CREDS or not SHEET_ID:
        e = discord.Embed(title="⚙️ Настройка Google Sheets", color=0xFF4444)
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

        e = discord.Embed(title=f"📊 Таблица крафта T{tier} · {server_name}", color=0x00E5FF)
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
    if await get_tier(interaction.guild_id) < TIER_PRO:
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
    if await get_tier(interaction.guild_id) < TIER_PRO:
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

        e = make_embed(
            title="Топ гильдий по ZvZ",
            description=f"По данным последних 50 битв · {datetime.datetime.utcnow().strftime('%d.%m.%Y')}",
            color=C.DANGER, footer="EU · Albion Online"
        )
        for i, (gname, stats) in enumerate(top):
            fame_bar = bar(stats["fame"], top[0][1]["fame"] if top else 1, 6)
            e.add_field(
                name=f"{i+1}. {gname}",
                value=(
                    f"Битв: **{stats['battles']}** · Убийств: **{stats['kills']}**\n"
                    f"Fame: **{stats['fame']:,}** `{fame_bar}`"
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
    if await get_tier(interaction.guild_id) < TIER_PRO:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)

    gid = interaction.guild_id

    if action.lower() == "list":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT id, item_id, threshold_pct, last_price FROM price_watch WHERE guild_id=?", (gid,)
            ) as c:
                rows = await c.fetchall()
        e = discord.Embed(title="📡 Price Watch", color=0x00E5FF)
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
                        e = discord.Embed(title=f"📡 Price Alert: {item_id}", color=0xFFD700)
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
        e = discord.Embed(title="⚔️ Albion Expert", description=answer[:4000], color=0xC8A951)
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

        e = discord.Embed(title=f"🔧 BM Test — {item_id}", color=0x00E5FF)
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


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ВРЕМЕННЫЙ БАН
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="tempban", description="Временный бан участника")
@app_commands.describe(
    member="Участник",
    duration="Длительность: 1h / 12h / 1d / 7d / 30d",
    reason="Причина"
)
async def tempban(interaction: discord.Interaction, member: discord.Member,
                  duration: str = "1d", reason: str = "Нарушение правил"):
    if not interaction.user.guild_permissions.ban_members:
        return await interaction.response.send_message("❌ Нет прав.", ephemeral=True)

    # Парсим длительность
    unit_map = {"h": 1, "d": 24, "w": 168}
    try:
        num = int(duration[:-1])
        unit = duration[-1].lower()
        hours = num * unit_map.get(unit, 24)
    except Exception:
        return await interaction.response.send_message("❌ Формат: `1h` `12h` `1d` `7d` `30d`", ephemeral=True)

    unban_at = (datetime.datetime.utcnow() + timedelta(hours=hours)).isoformat()

    try:
        await member.ban(reason=f"[Tempban {duration}] {reason}")
    except discord.Forbidden:
        return await interaction.response.send_message("❌ Нет прав забанить.", ephemeral=True)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO temp_bans (guild_id,user_id,mod_id,reason,unban_at) VALUES (?,?,?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET unban_at=excluded.unban_at,unbanned=0,reason=excluded.reason",
            (interaction.guild_id, member.id, interaction.user.id, reason, unban_at)
        )
        await db.commit()

    await add_modlog(interaction.guild_id, member.id, interaction.user.id, "TEMPBAN", reason, duration)

    e = discord.Embed(color=0xED4245, timestamp=datetime.datetime.utcnow())
    e.set_author(name=f"Tempban — {member.display_name}", icon_url=member.display_avatar.url)
    e.add_field(name="Member",    value=member.mention,           inline=True)
    e.add_field(name="Duration",  value=f"**{duration}**",        inline=True)
    e.add_field(name="Unban at",  value=unban_at[:16],            inline=True)
    e.add_field(name="Reason",    value=reason,                   inline=False)
    await interaction.response.send_message(embed=e)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MODLOG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="modlog", description="История действий над участником")
@app_commands.describe(member="Участник")
async def modlog_cmd(interaction: discord.Interaction, member: discord.Member):
    if not interaction.user.guild_permissions.manage_messages:
        return await interaction.response.send_message("❌ Нет прав.", ephemeral=True)

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT action,reason,duration,mod_id,created_at FROM modlog WHERE guild_id=? AND user_id=? ORDER BY id DESC LIMIT 15",
            (interaction.guild_id, member.id)
        ) as c:
            rows = await c.fetchall()

    e = discord.Embed(color=0xFEE75C, timestamp=datetime.datetime.utcnow())
    e.set_author(name=f"Modlog — {member.display_name}", icon_url=member.display_avatar.url)
    e.set_thumbnail(url=member.display_avatar.url)

    if not rows:
        e.description = "No moderation history."
    else:
        for action, reason, duration, mod_id, created_at in rows:
            mod = interaction.guild.get_member(mod_id)
            mod_str = mod.display_name if mod else ("Auto" if mod_id == 0 else str(mod_id))
            dur_str = f" · {duration}" if duration else ""
            e.add_field(
                name=f"`{action}`{dur_str} · {created_at[:10]}",
                value=f"{reason or '—'} · by {mod_str}",
                inline=False
            )
    await interaction.response.send_message(embed=e, ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  КАРАНТИН НАСТРОЙКИ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="quarantine", description="Настройка карантина для новых аккаунтов")
@app_commands.describe(
    action="setup / enable / disable / release",
    role="Карантинная роль",
    hours="Длительность карантина в часах (по умолчанию 24)",
    min_age="Минимальный возраст аккаунта в днях (по умолчанию 7)",
    member="Участник для release"
)
async def quarantine_cmd(interaction: discord.Interaction,
                          action: str,
                          role: discord.Role = None,
                          hours: int = 24,
                          min_age: int = 7,
                          member: discord.Member = None):
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ Нужны права администратора.", ephemeral=True)

    gid = interaction.guild_id

    if action == "setup":
        if not role:
            return await interaction.response.send_message("❌ Укажи роль: `/quarantine action:setup role:@Quarantine`", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO quarantine_settings (guild_id,role_id,duration_hours,min_age_days,enabled)
                VALUES (?,?,?,?,1)
                ON CONFLICT(guild_id) DO UPDATE SET
                    role_id=excluded.role_id, duration_hours=excluded.duration_hours,
                    min_age_days=excluded.min_age_days, enabled=1
            """, (gid, role.id, hours, min_age))
            await db.commit()
        e = discord.Embed(color=0x57F287, timestamp=datetime.datetime.utcnow())
        e.set_author(name="Quarantine configured")
        e.add_field(name="Role",     value=role.mention,      inline=True)
        e.add_field(name="Duration", value=f"**{hours}h**",   inline=True)
        e.add_field(name="Min age",  value=f"**{min_age} days**", inline=True)
        return await interaction.response.send_message(embed=e)

    if action in ("enable", "disable"):
        enabled = action == "enable"
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE quarantine_settings SET enabled=? WHERE guild_id=?",
                (1 if enabled else 0, gid)
            )
            await db.commit()
        status = "✅ Карантин включён" if enabled else "🔒 Карантин выключен"
        return await interaction.response.send_message(status)

    if action == "release":
        if not member:
            return await interaction.response.send_message("❌ Укажи участника.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT role_id FROM quarantine_settings WHERE guild_id=?", (gid,)
            ) as c:
                row = await c.fetchone()
        if row and row[0]:
            qrole = interaction.guild.get_role(row[0])
            if qrole and qrole in member.roles:
                await member.remove_roles(qrole, reason=f"Released by {interaction.user}")
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE quarantine SET released=1 WHERE guild_id=? AND user_id=?",
                (gid, member.id)
            )
            await db.commit()
        e = discord.Embed(color=0x57F287, timestamp=datetime.datetime.utcnow())
        e.set_author(name=f"Released from quarantine — {member.display_name}")
        return await interaction.response.send_message(embed=e)

    await interaction.response.send_message("❌ Действие: `setup` `enable` `disable` `release`", ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PUNISHMENT SETTINGS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="punishments", description="Настройка прогрессивных наказаний")
@app_commands.describe(
    warn2="Действие при 2 варнах: mute_1h / mute_24h / kick / ban",
    warn3="Действие при 3 варнах",
    warn4="Действие при 4 варнах",
    warn5="Действие при 5 варнах"
)
async def punishments_cmd(interaction: discord.Interaction,
                           warn2: str = "mute_1h",
                           warn3: str = "mute_24h",
                           warn4: str = "kick",
                           warn5: str = "ban"):
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ Нужны права администратора.", ephemeral=True)
    valid = {"mute_1h", "mute_24h", "kick", "ban", "none"}
    for val in [warn2, warn3, warn4, warn5]:
        if val not in valid:
            return await interaction.response.send_message(
                f"❌ Допустимые значения: `mute_1h` `mute_24h` `kick` `ban` `none`", ephemeral=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO punishment_settings (guild_id,warn2_action,warn3_action,warn4_action,warn5_action)
            VALUES (?,?,?,?,?)
            ON CONFLICT(guild_id) DO UPDATE SET
                warn2_action=excluded.warn2_action, warn3_action=excluded.warn3_action,
                warn4_action=excluded.warn4_action, warn5_action=excluded.warn5_action
        """, (interaction.guild_id, warn2, warn3, warn4, warn5))
        await db.commit()

    e = discord.Embed(color=0x5865F2, timestamp=datetime.datetime.utcnow())
    e.set_author(name="Progressive Punishments")
    e.add_field(name="2 warns", value=f"`{warn2}`", inline=True)
    e.add_field(name="3 warns", value=f"`{warn3}`", inline=True)
    e.add_field(name="4 warns", value=f"`{warn4}`", inline=True)
    e.add_field(name="5 warns", value=f"`{warn5}`", inline=True)
    await interaction.response.send_message(embed=e)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  REACTION ROLES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="reactionrole", description="Настройка reaction roles")
@app_commands.describe(
    action="add / remove / list / clear",
    message_id="ID сообщения",
    emoji="Эмодзи",
    role="Роль"
)
async def reactionrole_cmd(interaction: discord.Interaction,
                            action: str,
                            message_id: str = "",
                            emoji: str = "",
                            role: discord.Role = None):
    if not interaction.user.guild_permissions.manage_roles:
        return await interaction.response.send_message("❌ Нужны права Manage Roles.", ephemeral=True)
    gid = interaction.guild_id

    if action == "add":
        if not message_id or not emoji or not role:
            return await interaction.response.send_message("❌ Нужны: `message_id` `emoji` `role`", ephemeral=True)
        try:
            mid = int(message_id)
        except ValueError:
            return await interaction.response.send_message("❌ message_id должен быть числом", ephemeral=True)

        # Находим сообщение и добавляем реакцию
        msg = None
        for ch in interaction.guild.text_channels:
            try:
                msg = await ch.fetch_message(mid)
                break
            except Exception:
                continue
        if not msg:
            return await interaction.response.send_message("❌ Сообщение не найдено.", ephemeral=True)

        await msg.add_reaction(emoji)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO reaction_roles (guild_id,channel_id,message_id,emoji,role_id) VALUES (?,?,?,?,?)",
                (gid, msg.channel.id, mid, emoji, role.id)
            )
            await db.commit()
        e = discord.Embed(color=0x57F287, timestamp=datetime.datetime.utcnow())
        e.set_author(name="Reaction Role added")
        e.add_field(name="Emoji", value=emoji,        inline=True)
        e.add_field(name="Role",  value=role.mention, inline=True)
        return await interaction.response.send_message(embed=e)

    if action == "list":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT message_id,emoji,role_id FROM reaction_roles WHERE guild_id=?", (gid,)
            ) as c:
                rows = await c.fetchall()
        e = discord.Embed(color=0x5865F2, timestamp=datetime.datetime.utcnow())
        e.set_author(name="Reaction Roles")
        if not rows:
            e.description = "Нет настроенных reaction roles."
        for mid, em, rid in rows:
            r = interaction.guild.get_role(rid)
            e.add_field(name=f"{em} · msg `{mid}`", value=r.mention if r else str(rid), inline=True)
        return await interaction.response.send_message(embed=e, ephemeral=True)

    if action == "remove":
        if not message_id or not emoji:
            return await interaction.response.send_message("❌ Нужны: `message_id` `emoji`", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM reaction_roles WHERE guild_id=? AND message_id=? AND emoji=?",
                (gid, int(message_id), emoji)
            )
            await db.commit()
        return await interaction.response.send_message("✅ Reaction role удалена.", ephemeral=True)

    if action == "clear":
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM reaction_roles WHERE guild_id=?", (gid,))
            await db.commit()
        return await interaction.response.send_message("✅ Все reaction roles удалены.", ephemeral=True)

    await interaction.response.send_message("❌ Действие: `add` `remove` `list` `clear`", ephemeral=True)


@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id: return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT role_id FROM reaction_roles WHERE guild_id=? AND message_id=? AND emoji=?",
            (payload.guild_id, payload.message_id, str(payload.emoji))
        ) as c:
            row = await c.fetchone()
    if not row: return
    guild = bot.get_guild(payload.guild_id)
    if not guild: return
    member = guild.get_member(payload.user_id)
    if not member: return
    role = guild.get_role(row[0])
    if role:
        try:
            await member.add_roles(role, reason="Reaction role")
        except discord.Forbidden:
            pass


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id: return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT role_id FROM reaction_roles WHERE guild_id=? AND message_id=? AND emoji=?",
            (payload.guild_id, payload.message_id, str(payload.emoji))
        ) as c:
            row = await c.fetchone()
    if not row: return
    guild = bot.get_guild(payload.guild_id)
    if not guild: return
    member = guild.get_member(payload.user_id)
    if not member: return
    role = guild.get_role(row[0])
    if role:
        try:
            await member.remove_roles(role, reason="Reaction role removed")
        except discord.Forbidden:
            pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ALBION REGISTRATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="register", description="Привязать Albion Online ник к Discord аккаунту")
@app_commands.describe(player="Ник в Albion Online")
async def register_albion(interaction: discord.Interaction, player: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            pid, pname = await albion_find_player(s, player)
        if not pid:
            return await interaction.followup.send(f"❌ Игрок **{player}** не найден.", ephemeral=True)
        now = datetime.datetime.utcnow().isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO albion_registration (guild_id,user_id,player_name,player_id,registered_at)
                VALUES (?,?,?,?,?)
                ON CONFLICT(guild_id,user_id) DO UPDATE SET
                    player_name=excluded.player_name, player_id=excluded.player_id,
                    registered_at=excluded.registered_at
            """, (interaction.guild_id, interaction.user.id, pname, pid, now))
            await db.commit()
        e = discord.Embed(color=0x00E5FF, timestamp=datetime.datetime.utcnow())
        e.set_author(name=f"Registered — {pname}", icon_url=interaction.user.display_avatar.url)
        e.add_field(name="Discord", value=interaction.user.mention, inline=True)
        e.add_field(name="Albion",  value=f"**{pname}**",           inline=True)
        e.description = "Теперь `/stats` без аргументов покажет твою статистику."
        await interaction.followup.send(embed=e)
    except Exception as ex:
        await interaction.followup.send(f"❌ {ex}", ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ALBION WATCH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="watch", description="Отслеживать активность Albion игрока [Premium]")
@app_commands.describe(
    action="add / remove / list",
    player="Ник игрока",
    channel="Канал для алертов"
)
async def albion_watch_cmd(interaction: discord.Interaction,
                            action: str,
                            player: str = "",
                            channel: discord.TextChannel = None):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)

    gid = interaction.guild_id

    if action == "add":
        if not player:
            return await interaction.response.send_message("❌ Укажи игрока.", ephemeral=True)
        ch = channel or interaction.channel
        await interaction.response.defer()
        async with aiohttp.ClientSession() as s:
            pid, pname = await albion_find_player(s, player)
        if not pid:
            return await interaction.followup.send(f"❌ Игрок **{player}** не найден.", ephemeral=True)
        now = datetime.datetime.utcnow().isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO albion_watch (guild_id,user_id,channel_id,player_name,player_id,created_at) VALUES (?,?,?,?,?,?)",
                (gid, interaction.user.id, ch.id, pname, pid, now)
            )
            await db.commit()
        e = discord.Embed(color=0x00E5FF, timestamp=datetime.datetime.utcnow())
        e.set_author(name=f"Watching — {pname}")
        e.add_field(name="Player",  value=f"**{pname}**",  inline=True)
        e.add_field(name="Channel", value=ch.mention,      inline=True)
        e.description = "Бот будет оповещать о новых убийствах и смертях каждый час."
        await interaction.followup.send(embed=e)

    elif action == "remove":
        if not player:
            return await interaction.response.send_message("❌ Укажи игрока.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM albion_watch WHERE guild_id=? AND player_name=?",
                (gid, player)
            )
            await db.commit()
        await interaction.response.send_message(f"✅ Слежка за **{player}** удалена.", ephemeral=True)

    elif action == "list":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT player_name,channel_id FROM albion_watch WHERE guild_id=?", (gid,)
            ) as c:
                rows = await c.fetchall()
        e = discord.Embed(color=0x00E5FF, timestamp=datetime.datetime.utcnow())
        e.set_author(name="Albion Watch List")
        if not rows:
            e.description = "Нет отслеживаемых игроков."
        for pname, cid in rows:
            ch_obj = interaction.guild.get_channel(cid)
            e.add_field(name=pname, value=ch_obj.mention if ch_obj else str(cid), inline=True)
        await interaction.response.send_message(embed=e, ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  INVITE STATS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="invstats", description="Статистика и лидерборд инвайтов")
async def invstats(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно право Manage Server.", ephemeral=True)
    gid = interaction.guild_id

    async with aiosqlite.connect(DB_PATH) as db:
        # Топ по количеству приглашённых
        async with db.execute("""
            SELECT inviter_id, inviter_name, COUNT(*) as total
            FROM invite_log
            WHERE guild_id=? AND inviter_id > 0
            GROUP BY inviter_id
            ORDER BY total DESC
            LIMIT 10
        """, (gid,)) as c:
            top_rows = await c.fetchall()

        # Самый используемый инвайт
        async with db.execute("""
            SELECT invite_code, COUNT(*) as uses, inviter_name
            FROM invite_log
            WHERE guild_id=? AND invite_code IS NOT NULL
            GROUP BY invite_code
            ORDER BY uses DESC
            LIMIT 5
        """, (gid,)) as c:
            code_rows = await c.fetchall()

        # Всего вошло
        async with db.execute(
            "SELECT COUNT(*) FROM invite_log WHERE guild_id=?", (gid,)
        ) as c:
            total = (await c.fetchone())[0]

    e = discord.Embed(color=0x5865F2, timestamp=datetime.datetime.utcnow())
    e.set_author(name="Invite Stats")
    e.add_field(name="Total joined via invites", value=f"**{total}**", inline=True)

    if top_rows:
        lines = []
        medals = ["🥇","🥈","🥉"] + [f"{i}." for i in range(4, 11)]
        for i, (uid, uname, total_inv) in enumerate(top_rows):
            m = interaction.guild.get_member(uid)
            name = m.display_name if m else uname
            lines.append(f"{medals[i]} **{name}** — {total_inv} invites")
        e.add_field(name="Top inviters", value="\n".join(lines), inline=False)

    if code_rows:
        lines = [f"`{code}` — {uses} uses ({inv})" for code, uses, inv in code_rows]
        e.add_field(name="Top invite codes", value="\n".join(lines), inline=False)

    await interaction.response.send_message(embed=e, ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ALBION WATCH BACKGROUND LOOP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def albion_watch_loop(bot_instance):
    """Проверяет новые убийства/смерти для отслеживаемых игроков — раз в час"""
    await bot_instance.wait_until_ready()
    while not bot_instance.is_closed():
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT id,guild_id,channel_id,player_name,player_id,last_check FROM albion_watch"
                ) as c:
                    watches = await c.fetchall()

            for wid, guild_id, channel_id, pname, pid, last_check in watches:
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(f"{ALBION_BASE}/players/{pid}/kills?limit=5") as r:
                            kills = await r.json() if r.status == 200 else []
                        async with s.get(f"{ALBION_BASE}/players/{pid}/deaths?limit=5") as r:
                            deaths = await r.json() if r.status == 200 else []

                    now_iso = datetime.datetime.utcnow().isoformat()
                    new_kills = [k for k in kills
                                 if k.get("TimeStamp","") > (last_check or "")]
                    new_deaths = [d for d in deaths
                                  if d.get("TimeStamp","") > (last_check or "")]

                    if new_kills or new_deaths:
                        ch = bot_instance.get_channel(channel_id)
                        if ch:
                            e = discord.Embed(color=0x00E5FF, timestamp=datetime.datetime.utcnow())
                            e.set_author(name=f"Albion Watch — {pname}")
                            if new_kills:
                                lines = [
                                    f"⚔️ Killed **{k.get('Victim',{}).get('Name','?')}** · {k.get('TotalVictimKillFame',0):,} fame"
                                    for k in new_kills[:3]
                                ]
                                e.add_field(name=f"Kills ({len(new_kills)})", value="\n".join(lines), inline=False)
                            if new_deaths:
                                lines = [
                                    f"💀 Died to **{d.get('Killer',{}).get('Name','?')}** · {d.get('TotalVictimKillFame',0):,} fame"
                                    for d in new_deaths[:3]
                                ]
                                e.add_field(name=f"Deaths ({len(new_deaths)})", value="\n".join(lines), inline=False)
                            await ch.send(embed=e)

                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute(
                            "UPDATE albion_watch SET last_check=? WHERE id=?", (now_iso, wid)
                        )
                        await db.commit()
                except Exception:
                    pass

        except Exception as ex:
            print(f"[ALBION WATCH] Error: {ex}")

        await asyncio.sleep(3600)  # раз в час


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  STYLOMETRY ENGINE — Распознавание твинков по стилю письма
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

import unicodedata as _ud

MIN_MSGS_FOR_COMPARE = 40
TWIN_THRESHOLD       = 72
SAVE_EVERY           = 10
MAX_COMPARE_USERS    = 200

_style_cache: dict = {}

_STOP_WORDS = {
    "и","в","не","на","что","я","с","как","а","то","он","но","за","по",
    "к","же","из","у","от","да","ну","так","это","все","ещё","уже","ты",
    "the","a","an","is","are","was","were","i","you","he","she","it","we",
    "they","in","on","at","to","of","and","or","but","not","this","that"
}

_COMMON_RU = {
    "привет","пока","как","дела","хорошо","плохо","нет","да","ладно",
    "окей","ок","спасибо","пожалуйста","конечно","понял","понятно",
    "вообще","короче","кстати","кароч","блин","слушай","смотри","думаю",
    "сейчас","потом","завтра","сегодня","всё","ничего","много","мало",
    "можно","нельзя","надо","нужно","хочу","буду","могу","пойду","знаю"
}


def _has_emoji(text: str) -> bool:
    """Проверяет наличие emoji в тексте"""
    for ch in text:
        cat = _ud.category(ch)
        cp  = ord(ch)
        if cat in ("So", "Sm") or 0x1F300 <= cp <= 0x1FAFF:
            return True
    return False


class StyleProfile:
    def __init__(self):
        self.msg_count       = 0
        self.total_word_len  = 0.0
        self.total_msg_len   = 0
        self.punct_msgs      = 0
        self.caps_msgs       = 0
        self.emoji_msgs      = 0
        self.word_freq: dict = {}
        self.sentence_enders: dict = {}
        self.active_hours: dict    = {}

    def update(self, text: str, hour: int):
        if not text or len(text) < 2:
            return
        self.msg_count += 1
        words = text.lower().split()
        if not words:
            return
        wlens = [len(w.strip(".,!?;:\"'()[]")) for w in words if len(w) > 1]
        if wlens:
            self.total_word_len += sum(wlens) / len(wlens)
        self.total_msg_len += len(text)
        last = text.rstrip()[-1] if text.rstrip() else ""
        if last in ".!?":
            self.punct_msgs += 1
            self.sentence_enders[last] = self.sentence_enders.get(last, 0) + 1
        else:
            self.sentence_enders["none"] = self.sentence_enders.get("none", 0) + 1
        letters = [c for c in text if c.isalpha()]
        if letters and sum(1 for c in letters if c.isupper()) / len(letters) > 0.6:
            self.caps_msgs += 1
        if _has_emoji(text):
            self.emoji_msgs += 1
        import re as _re2
        for word in words:
            w = _re2.sub(r"[^а-яёa-z]", "", word.lower())
            if len(w) >= 3 and w not in _STOP_WORDS:
                self.word_freq[w] = self.word_freq.get(w, 0) + 1
        self.active_hours[str(hour)] = self.active_hours.get(str(hour), 0) + 1

    def get_typos(self) -> set:
        return {w for w, cnt in self.word_freq.items()
                if cnt <= 2 and len(w) >= 4 and w not in _COMMON_RU}

    def get_top_words(self, n: int = 30) -> set:
        return {w for w, _ in sorted(self.word_freq.items(), key=lambda x: x[1], reverse=True)[:n]}

    def to_dict(self) -> dict:
        n = max(self.msg_count, 1)
        return {
            "msg_count":       self.msg_count,
            "avg_word_len":    round(self.total_word_len / n, 3),
            "avg_msg_len":     round(self.total_msg_len  / n, 1),
            "punct_ratio":     round(self.punct_msgs / n, 3),
            "caps_ratio":      round(self.caps_msgs  / n, 3),
            "emoji_ratio":     round(self.emoji_msgs / n, 3),
            "no_punct_ratio":  round(1 - self.punct_msgs / n, 3),
            "common_words":    json.dumps(list(self.get_top_words(30))),
            "common_typos":    json.dumps(list(self.get_typos())),
            "sentence_enders": json.dumps(self.sentence_enders),
            "active_hours":    json.dumps(self.active_hours),
        }


def _jaccard(a: set, b: set) -> float:
    if not a or not b: return 0.0
    return len(a & b) / len(a | b)


def _scalar_sim(a: float, b: float, tol: float) -> float:
    if tol == 0: return 1.0 if a == b else 0.0
    return max(0.0, 1.0 - abs(a - b) / tol)


def compare_profiles(p1: dict, p2: dict):
    score = 0.0; total = 0.0; reasons = []

    def add(w, sim, label):
        nonlocal score, total
        total += w; score += w * sim
        if sim >= 0.7: reasons.append(f"{label} ({sim:.0%})")

    add(10,  _scalar_sim(p1["avg_word_len"],   p2["avg_word_len"],   0.5),  "Длина слов")
    add(10,  _scalar_sim(p1["avg_msg_len"],    p2["avg_msg_len"],    20),   "Длина сообщений")
    add(15,  _scalar_sim(p1["no_punct_ratio"], p2["no_punct_ratio"], 0.15), "Отсутствие пунктуации")
    add(10,  _scalar_sim(p1["caps_ratio"],     p2["caps_ratio"],     0.1),  "Использование caps")
    add(10,  _scalar_sim(p1["emoji_ratio"],    p2["emoji_ratio"],    0.1),  "Использование emoji")

    try:
        w1 = set(json.loads(p1.get("common_words", "[]")))
        w2 = set(json.loads(p2.get("common_words", "[]")))
        add(20, _jaccard(w1, w2), "Общий словарный запас")
    except Exception:
        pass

    try:
        t1 = set(json.loads(p1.get("common_typos", "[]")))
        t2 = set(json.loads(p2.get("common_typos", "[]")))
        if t1 and t2:
            sim = _jaccard(t1, t2)
            common = t1 & t2
            if len(common) >= 3:
                sim = min(1.0, sim * 1.3)
                reasons.append("Одинаковые опечатки: " + ", ".join(list(common)[:5]))
            add(30, sim, "Совпадение опечаток")
    except Exception:
        pass

    try:
        e1 = json.loads(p1.get("sentence_enders", "{}"))
        e2 = json.loads(p2.get("sentence_enders", "{}"))
        keys = set(e1) | set(e2)
        if keys:
            s1 = sum(e1.values()) or 1; s2 = sum(e2.values()) or 1
            diff = sum(abs(e1.get(k,0)/s1 - e2.get(k,0)/s2) for k in keys)
            add(15, max(0.0, 1.0 - diff), "Паттерн пунктуации")
    except Exception:
        pass

    try:
        h1 = json.loads(p1.get("active_hours", "{}"))
        h2 = json.loads(p2.get("active_hours", "{}"))
        top1 = set(sorted(h1, key=h1.get, reverse=True)[:3])
        top2 = set(sorted(h2, key=h2.get, reverse=True)[:3])
        add(10, len(top1 & top2) / 3, "Одинаковые активные часы")
    except Exception:
        pass

    return round(score / total * 100, 1) if total > 0 else 0.0, reasons


async def update_style_profile(guild_id: int, user_id: int, message: discord.Message):
    key = (guild_id, user_id)
    if key not in _style_cache:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT msg_count,avg_word_len,avg_msg_len,punct_ratio,caps_ratio,emoji_ratio,"
                "no_punct_ratio,common_words,common_typos,sentence_enders,active_hours "
                "FROM style_profiles WHERE guild_id=? AND user_id=?",
                (guild_id, user_id)
            ) as c:
                row = await c.fetchone()
        sp = StyleProfile()
        if row:
            sp.msg_count       = row[0]
            sp.total_word_len  = row[1] * row[0]
            sp.total_msg_len   = int(row[2] * row[0])
            sp.punct_msgs      = int(row[3] * row[0])
            sp.caps_msgs       = int(row[4] * row[0])
            sp.emoji_msgs      = int(row[5] * row[0])
            try:
                for w in json.loads(row[7] or "[]"): sp.word_freq[w] = sp.word_freq.get(w, 0) + 2
                for w in json.loads(row[8] or "[]"): sp.word_freq[w] = sp.word_freq.get(w, 0) + 1
                sp.sentence_enders = json.loads(row[9] or "{}")
                sp.active_hours    = json.loads(row[10] or "{}")
            except Exception:
                pass
        _style_cache[key] = sp

    sp = _style_cache[key]
    sp.update(message.content, datetime.datetime.utcnow().hour)

    if sp.msg_count % SAVE_EVERY == 0:
        await _save_style_profile(guild_id, user_id, sp)

    if sp.msg_count == MIN_MSGS_FOR_COMPARE:
        asyncio.create_task(_run_twin_check(message.guild, user_id, sp))


async def _save_style_profile(guild_id: int, user_id: int, sp: StyleProfile):
    d   = sp.to_dict()
    now = datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO style_profiles
                (guild_id,user_id,msg_count,avg_word_len,avg_msg_len,
                 punct_ratio,caps_ratio,emoji_ratio,no_punct_ratio,
                 common_words,common_typos,sentence_enders,active_hours,updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(guild_id,user_id) DO UPDATE SET
                msg_count=excluded.msg_count, avg_word_len=excluded.avg_word_len,
                avg_msg_len=excluded.avg_msg_len, punct_ratio=excluded.punct_ratio,
                caps_ratio=excluded.caps_ratio, emoji_ratio=excluded.emoji_ratio,
                no_punct_ratio=excluded.no_punct_ratio, common_words=excluded.common_words,
                common_typos=excluded.common_typos, sentence_enders=excluded.sentence_enders,
                active_hours=excluded.active_hours, updated_at=excluded.updated_at
        """, (guild_id, user_id,
              d["msg_count"], d["avg_word_len"], d["avg_msg_len"],
              d["punct_ratio"], d["caps_ratio"], d["emoji_ratio"], d["no_punct_ratio"],
              d["common_words"], d["common_typos"], d["sentence_enders"], d["active_hours"], now))
        await db.commit()


async def _run_twin_check(guild: discord.Guild, target_uid: int, target_sp: StyleProfile):
    gid = guild.id
    tp  = target_sp.to_dict()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id,msg_count,avg_word_len,avg_msg_len,punct_ratio,caps_ratio,
                   emoji_ratio,no_punct_ratio,common_words,common_typos,sentence_enders,active_hours
            FROM style_profiles
            WHERE guild_id=? AND user_id!=? AND msg_count>=? LIMIT ?
        """, (gid, target_uid, MIN_MSGS_FOR_COMPARE, MAX_COMPARE_USERS)) as c:
            rows = await c.fetchall()

    cols = ["user_id","msg_count","avg_word_len","avg_msg_len","punct_ratio","caps_ratio",
            "emoji_ratio","no_punct_ratio","common_words","common_typos","sentence_enders","active_hours"]

    best_score = 0.0; best_uid = None; best_reasons = []
    for row in rows:
        p   = dict(zip(cols, row))
        uid = p["user_id"]
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT id FROM twin_links
                WHERE guild_id=? AND ((user_a=? AND user_b=?) OR (user_a=? AND user_b=?))
                  AND (confirmed=1 OR false_positive=1)
            """, (gid, target_uid, uid, uid, target_uid)) as c:
                if await c.fetchone(): continue
        score, reasons = compare_profiles(tp, p)
        if score > best_score:
            best_score = score; best_uid = uid; best_reasons = reasons

    if best_score >= TWIN_THRESHOLD and best_uid:
        await _create_twin_alert(guild, target_uid, best_uid, best_score, best_reasons)


async def _create_twin_alert(guild: discord.Guild, user_a: int, user_b: int,
                              score: float, reasons: list):
    gid = guild.id
    now = datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            INSERT INTO twin_links (guild_id,user_a,user_b,similarity,reasons,detected_at)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT DO NOTHING
        """, (gid, min(user_a,user_b), max(user_a,user_b), score, json.dumps(reasons), now))
        await db.commit()
        link_id = cur.lastrowid or 0

    ch = await get_log_ch(guild)
    if not ch: return

    m_a = guild.get_member(user_a); m_b = guild.get_member(user_b)
    na  = m_a.display_name if m_a else str(user_a)
    nb  = m_b.display_name if m_b else str(user_b)

    color = 0xED4245 if score >= 85 else 0xFEE75C
    e = discord.Embed(color=color, timestamp=datetime.datetime.utcnow())
    e.set_author(name="Возможный твинк-аккаунт")
    e.add_field(name="Участник A", value=f"{m_a.mention if m_a else user_a} (`{na}`)", inline=True)
    e.add_field(name="Участник B", value=f"{m_b.mention if m_b else user_b} (`{nb}`)", inline=True)
    e.add_field(name="Схожесть",   value=f"**{score}/100**",                           inline=True)
    if reasons:
        e.add_field(name="Совпадающие признаки",
                    value="\n".join(f"• {r}" for r in reasons[:6]), inline=False)
    e.add_field(name="Это предположение, не доказательство",
                value="Подтверди или отклони ниже:", inline=False)
    e.set_footer(text=f"Link ID: #{link_id} · Witness Stylometry")

    await ch.send(embed=e, view=TwinConfirmView(link_id, user_a, user_b))


class TwinConfirmView(discord.ui.View):
    def __init__(self, link_id, user_a, user_b):
        super().__init__(timeout=None)
        self.link_id = link_id
        self.user_a  = user_a
        self.user_b  = user_b

    @discord.ui.button(label="Подтвердить твинк", style=discord.ButtonStyle.danger)
    async def confirm_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.manage_messages:
            return await interaction.response.send_message("Нет прав.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE twin_links SET confirmed=1,confirmed_by=? WHERE id=?",
                             (interaction.user.id, self.link_id))
            await db.commit()
        for item in self.children: item.disabled = True
        await interaction.response.edit_message(
            content=f"Подтверждено как твинк · {interaction.user.mention}", view=self)

    @discord.ui.button(label="Ложное срабатывание", style=discord.ButtonStyle.secondary)
    async def fp_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        if not interaction.user.guild_permissions.manage_messages:
            return await interaction.response.send_message("Нет прав.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE twin_links SET false_positive=1,confirmed_by=? WHERE id=?",
                             (interaction.user.id, self.link_id))
            await db.commit()
        for item in self.children: item.disabled = True
        await interaction.response.edit_message(
            content=f"Отклонено как ложное · {interaction.user.mention}", view=self)

    @discord.ui.button(label="Подробнее", style=discord.ButtonStyle.primary)
    async def details_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        gid = interaction.guild_id
        e   = discord.Embed(color=0x5865F2, timestamp=datetime.datetime.utcnow())
        e.set_author(name="Детальное сравнение профилей")
        for uid in [self.user_a, self.user_b]:
            m  = interaction.guild.get_member(uid)
            nm = m.display_name if m else str(uid)
            sp = _style_cache.get((gid, uid))
            if sp:
                d = sp.to_dict()
                e.add_field(name=nm, value=(
                    f"Сообщений: **{d['msg_count']}**\n"
                    f"Ср. длина: **{d['avg_msg_len']:.0f}**\n"
                    f"Без пунктуации: **{d['no_punct_ratio']:.0%}**\n"
                    f"Опечатки: `{'`, `'.join(list(sp.get_typos())[:4]) or '—'}`"
                ), inline=True)
            else:
                e.add_field(name=nm, value="Нет данных в кэше", inline=True)
        await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="twincheck",
                  description="Сравнить двух участников по стилю письма")
@app_commands.describe(member1="Первый участник", member2="Второй участник")
async def twincheck_cmd(interaction: discord.Interaction,
                        member1: discord.Member, member2: discord.Member):
    if not interaction.user.guild_permissions.manage_messages:
        return await interaction.response.send_message("Нет прав.", ephemeral=True)
    if member1.id == member2.id:
        return await interaction.response.send_message("Укажи двух разных участников.", ephemeral=True)
    gid = interaction.guild_id

    profiles = {}
    async with aiosqlite.connect(DB_PATH) as db:
        for uid in [member1.id, member2.id]:
            async with db.execute(
                "SELECT msg_count,avg_word_len,avg_msg_len,punct_ratio,caps_ratio,"
                "emoji_ratio,no_punct_ratio,common_words,common_typos,sentence_enders,active_hours "
                "FROM style_profiles WHERE guild_id=? AND user_id=?", (gid, uid)
            ) as c:
                row = await c.fetchone()
            if row:
                cols = ["msg_count","avg_word_len","avg_msg_len","punct_ratio","caps_ratio",
                        "emoji_ratio","no_punct_ratio","common_words","common_typos",
                        "sentence_enders","active_hours"]
                profiles[uid] = dict(zip(cols, row))
    for uid in [member1.id, member2.id]:
        sp = _style_cache.get((gid, uid))
        if sp and uid not in profiles:
            profiles[uid] = sp.to_dict()

    e = discord.Embed(color=0x5865F2, timestamp=datetime.datetime.utcnow())
    e.set_author(name=f"Twincheck — {member1.display_name} vs {member2.display_name}")

    missing = [m.display_name for m in [member1, member2]
               if m.id not in profiles or profiles[m.id].get("msg_count", 0) < MIN_MSGS_FOR_COMPARE]
    if missing:
        e.color  = 0xFEE75C
        e.description = (
            f"Недостаточно данных: **{', '.join(missing)}**\n"
            f"Нужно минимум **{MIN_MSGS_FOR_COMPARE}** сообщений."
        )
        return await interaction.response.send_message(embed=e, ephemeral=True)

    score, reasons = compare_profiles(profiles[member1.id], profiles[member2.id])
    color = 0xED4245 if score >= TWIN_THRESHOLD else 0xFEE75C if score >= 50 else 0x57F287
    verdict = (
        "Высокая вероятность твинка" if score >= TWIN_THRESHOLD else
        "Умеренное сходство"         if score >= 50 else
        "Разные стили письма"
    )
    e.color = color
    e.add_field(name="Схожесть", value=f"**{score}/100**", inline=True)
    e.add_field(name="Вердикт",  value=verdict,             inline=True)
    p1 = profiles[member1.id]; p2 = profiles[member2.id]
    e.add_field(name=member1.display_name, value=(
        f"Сообщений: **{p1['msg_count']}**\n"
        f"Ср. длина: **{p1['avg_msg_len']:.0f}**\n"
        f"Без пунктуации: **{p1['no_punct_ratio']:.0%}**"
    ), inline=True)
    e.add_field(name=member2.display_name, value=(
        f"Сообщений: **{p2['msg_count']}**\n"
        f"Ср. длина: **{p2['avg_msg_len']:.0f}**\n"
        f"Без пунктуации: **{p2['no_punct_ratio']:.0%}**"
    ), inline=True)
    if reasons:
        e.add_field(name="Совпадающие признаки",
                    value="\n".join(f"• {r}" for r in reasons), inline=False)
    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="twinlinks",
                  description="Список найденных твинк-связей")
@app_commands.describe(status="all / confirmed / pending / false_positive")
async def twinlinks_cmd(interaction: discord.Interaction, status: str = "pending"):
    if not interaction.user.guild_permissions.manage_messages:
        return await interaction.response.send_message("Нет прав.", ephemeral=True)
    gid = interaction.guild_id
    where = {"all":"","confirmed":"AND confirmed=1",
             "pending":"AND confirmed=0 AND false_positive=0",
             "false_positive":"AND false_positive=1"}.get(status, "AND confirmed=0 AND false_positive=0")
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            f"SELECT id,user_a,user_b,similarity,reasons,confirmed,false_positive,detected_at "
            f"FROM twin_links WHERE guild_id=? {where} ORDER BY similarity DESC LIMIT 20",
            (gid,)
        ) as c:
            rows = await c.fetchall()
    e = discord.Embed(color=0x5865F2, timestamp=datetime.datetime.utcnow())
    e.set_author(name=f"Twin Links — {status}")
    if not rows:
        e.description = "Нет записей."
    else:
        for lid, ua, ub, sim, r_json, conf, fp, det in rows:
            ma = interaction.guild.get_member(ua); mb = interaction.guild.get_member(ub)
            na = ma.display_name if ma else str(ua)
            nb = mb.display_name if mb else str(ub)
            icon = "Подтверждён" if conf else ("Ложное" if fp else "Ожидает")
            try: rs = ", ".join(json.loads(r_json)[:3])
            except Exception: rs = "—"
            e.add_field(name=f"#{lid} · {na} ↔ {nb} · {sim:.0f}/100 · {icon}",
                        value=f"{rs}\n*{det[:10]}*", inline=False)
    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="styleprofile",
                  description="Стилометрический профиль участника")
@app_commands.describe(member="Участник (пусто = ты)")
async def styleprofile_cmd(interaction: discord.Interaction,
                           member: discord.Member = None):
    target = member or interaction.user
    gid    = interaction.guild_id
    if member and member != interaction.user and not interaction.user.guild_permissions.manage_messages:
        return await interaction.response.send_message("Нет прав.", ephemeral=True)

    sp = _style_cache.get((gid, target.id))
    row_data = None
    if not sp:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT msg_count,avg_msg_len,avg_word_len,no_punct_ratio,caps_ratio,"
                "emoji_ratio,common_typos,active_hours FROM style_profiles "
                "WHERE guild_id=? AND user_id=?", (gid, target.id)
            ) as c:
                row_data = await c.fetchone()

    e = discord.Embed(color=0x5865F2, timestamp=datetime.datetime.utcnow())
    e.set_author(name=f"Style Profile — {target.display_name}",
                 icon_url=target.display_avatar.url)
    e.set_thumbnail(url=target.display_avatar.url)

    if sp:
        d = sp.to_dict()
        mc = d["msg_count"]
        e.add_field(name="Сообщений",       value=f"**{mc}**",                  inline=True)
        e.add_field(name="Ср. длина",        value=f"**{d['avg_msg_len']:.0f}**", inline=True)
        e.add_field(name="Без пунктуации",   value=f"**{d['no_punct_ratio']:.0%}**", inline=True)
        e.add_field(name="Caps",             value=f"**{d['caps_ratio']:.0%}**",  inline=True)
        e.add_field(name="Emoji",            value=f"**{d['emoji_ratio']:.0%}**", inline=True)
        hours  = sp.active_hours
        peak   = max(hours, key=hours.get) if hours else "?"
        e.add_field(name="Пик активности",   value=f"**{peak}:00 UTC**",          inline=True)
        typos  = list(sp.get_typos())[:5]
        if typos:
            e.add_field(name="Характерные слова", value="`" + "`, `".join(typos) + "`", inline=False)
        ready = min(mc / MIN_MSGS_FOR_COMPARE * 100, 100)
        e.add_field(name="Готовность профиля",
                    value=f"**{ready:.0f}%** (нужно {MIN_MSGS_FOR_COMPARE} сообщений)", inline=False)
    elif row_data:
        mc,aml,awl,npr,cr,er,typos_json,hours_json = row_data
        try: typos = json.loads(typos_json)[:5]
        except Exception: typos = []
        try:
            hrs  = json.loads(hours_json)
            peak = max(hrs, key=hrs.get) if hrs else "?"
        except Exception: peak = "?"
        e.add_field(name="Сообщений",       value=f"**{mc}**",       inline=True)
        e.add_field(name="Ср. длина",        value=f"**{aml:.0f}**",  inline=True)
        e.add_field(name="Без пунктуации",   value=f"**{npr:.0%}**",  inline=True)
        e.add_field(name="Caps",             value=f"**{cr:.0%}**",   inline=True)
        e.add_field(name="Emoji",            value=f"**{er:.0%}**",   inline=True)
        e.add_field(name="Пик активности",   value=f"**{peak}:00 UTC**", inline=True)
        if typos:
            e.add_field(name="Характерные слова", value="`" + "`, `".join(typos) + "`", inline=False)
        ready = min(mc / MIN_MSGS_FOR_COMPARE * 100, 100)
        e.add_field(name="Готовность профиля",
                    value=f"**{ready:.0f}%** (нужно {MIN_MSGS_FOR_COMPARE} сообщений)", inline=False)
    else:
        e.description = (
            f"Нет данных для **{target.display_name}**.\n"
            f"Нужно минимум **{MIN_MSGS_FOR_COMPARE}** сообщений в канале."
        )
    await interaction.response.send_message(embed=e, ephemeral=True)


if __name__ == "__main__":
    bot.run(TOKEN)
