"""
NexusBot v5.0 — Discord Bot for Gaming Communities
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NEW IN v5:

  🤖 AI — бесплатная замена Anthropic:
     Groq API (Llama 3.3 70B) — БЕСПЛАТНО, console.groq.com
     Fallback: Google Gemini Flash — БЕСПЛАТНО, aistudio.google.com

  🛡️ БЕЗОПАСНОСТЬ (Premium tier):
     ✅ Invite tracker — кто кого пригласил, история по коду
     ✅ Invite logger  — логи создания/удаления инвайтов
     ✅ Anti-raid      — детект массового захода (>5 за 10 сек)
     ✅ Anti-spam      — детект флуда (>5 сообщений за 3 сек)
     ✅ Alt-detector   — флаг аккаунтов младше N дней
     ✅ Подозрительные аккаунты → автоматический алерт
     ✅ Логирование всех событий (настраиваемое):
        joins/leaves/bans/timeouts/msg_delete/msg_edit/
        nick_change/role_change/avatar_change/voice/
        channels/roles/server_edit/reactions/threads/slash_commands
     ✅ /logset        — включить/выключить конкретные логи
     ✅ /logstatus     — статус всех логов
     ✅ /invcheck      — история по коду инвайта
     ✅ /invuser       — все инвайты пользователя
     ✅ /invdel        — удалить инвайт
     ✅ /invtop        — топ по приглашённым
     ✅ /warn          — выдать предупреждение
     ✅ /warnings      — история варнов
     ✅ /clearwarns    — сбросить варны
     ✅ /ban /kick /mute /unmute — модерация через slash
     ✅ /lockdown      — заблокировать канал
     ✅ /unlock        — разблокировать канал

  🗄️ БЕЗОПАСНОСТЬ ДАННЫХ:
     Каждый сервер хранится в своей таблице (guild_{id}_*)
     Данные одного сервера недоступны другим серверам
     Полное удаление данных сервера при /datadelete

  Все функции v4 включены.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
API KEYS (.env):
  DISCORD_TOKEN      — discord.com/developers (бесплатно)
  GROQ_API_KEY       — console.groq.com (БЕСПЛАТНО, замена Claude)
  GEMINI_API_KEY     — aistudio.google.com (БЕСПЛАТНО, резерв)
  WEATHER_API_KEY    — openweathermap.org (бесплатно)
  HENRIK_API_KEY     — docs.henrikdev.xyz (бесплатно, Valorant)
  RIOT_API_KEY       — developer.riotgames.com (бесплатно)
  STEAM_API_KEY      — steamcommunity.com/dev/apikey (бесплатно)
  LOSTARK_API_KEY    — developer-lostark.game.onstove.com (бесплатно)

  Stripe (для приёма оплаты подписки):
  STRIPE_SECRET_KEY  — dashboard.stripe.com (2.9% + 0.30€ с транзакции)
  STRIPE_WEBHOOK_SECRET — для автоматической активации подписки

  Деньги от подписки поступают на твой банковский счёт через Stripe.
  Минимальный вывод: €2. Срок поступления: 2-7 рабочих дней.

Requirements:
  pip install discord.py aiohttp python-dotenv aiosqlite
"""

import discord
from discord.ext import commands
from discord import app_commands
import aiohttp
import aiosqlite
import os
import asyncio
import random
import time
import json
from datetime import datetime, timedelta
from collections import defaultdict
from dotenv import load_dotenv
from functools import wraps

load_dotenv()

TOKEN        = os.getenv("DISCORD_TOKEN")
GROQ_KEY     = os.getenv("GROQ_API")      # FREE — console.groq.com
GEMINI_KEY   = os.getenv("GEMINI_API")    # FREE fallback — aistudio.google.com
WEATHER_KEY  = os.getenv("WEATHER_API_KEY")
HENRIK_KEY   = os.getenv("VALORANT_API_KEY")
RIOT_KEY     = os.getenv("RIOT_API")
STEAM_KEY    = os.getenv("STEAM_API")
LOSTARK_KEY  = os.getenv("lostspark_api")
DB_PATH      = "nexusbot.db"

ALBION_BASE  = "https://gameinfo.albiononline.com/api/gameinfo"
ALBION_DATA  = "https://west.albion-online-data.com/api/v2"

TIER_FREE    = 0
TIER_PREMIUM = 1   # €4.99/mo — includes Security
TIER_PRO     = 2   # €9.99/mo
TIER_NAMES   = {0: "Free", 1: "⭐ Premium", 2: "💎 Pro"}
TIER_COLORS  = {0: 0x6b7fa3, 1: 0x00E5FF, 2: 0xFFD700}

OWNER_IDS = {474658252840370176}    # Добавь свой Discord ID: {123456789}

# ─── RATE LIMITING ───────────────────────────────────────────
_cooldowns: dict = {}

def cooldown(seconds: int):
    def decorator(func):
        @wraps(func)
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            key = (interaction.user.id, func.__name__)
            now = time.time()
            remaining = seconds - (now - _cooldowns.get(key, 0))
            if remaining > 0:
                return await interaction.response.send_message(
                    f"⏳ Cooldown! Try again in **{remaining:.1f}s**.", ephemeral=True
                )
            _cooldowns[key] = now
            return await func(interaction, *args, **kwargs)
        return wrapper
    return decorator

# ─── ANTI-SPAM / ANTI-RAID ───────────────────────────────────
_msg_tracker  = defaultdict(list)   # {user_id: [timestamps]}
_join_tracker = defaultdict(list)   # {guild_id: [timestamps]}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DATABASE — изолированное хранение по серверам
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# Структура таблиц:
#   guild_{id}_xp        — XP пользователей сервера
#   guild_{id}_economy   — монеты пользователей сервера
#   guild_{id}_invites   — история инвайтов сервера
#   guild_{id}_warnings  — варны пользователей сервера
#   guild_{id}_security  — настройки безопасности сервера
#   subscriptions        — подписки (общая, без личных данных)

async def db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        # Общая таблица подписок — без пользовательских данных
        await db.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                guild_id   INTEGER PRIMARY KEY,
                tier       INTEGER DEFAULT 0,
                expires_at TEXT
            )
        """)
        await db.commit()

async def ensure_guild_tables(guild_id: int):
    """Создаёт изолированные таблицы для конкретного сервера"""
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript(f"""
            CREATE TABLE IF NOT EXISTS guild_{gid}_xp (
                user_id INTEGER PRIMARY KEY,
                xp      INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS guild_{gid}_economy (
                user_id INTEGER PRIMARY KEY,
                coins   INTEGER DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS guild_{gid}_invites (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                invite_code TEXT,
                inviter_id  INTEGER,
                inviter_name TEXT,
                member_id   INTEGER,
                member_name TEXT,
                joined_at   TEXT
            );
            CREATE TABLE IF NOT EXISTS guild_{gid}_warnings (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                mod_id     INTEGER,
                reason     TEXT,
                issued_at  TEXT
            );
            CREATE TABLE IF NOT EXISTS guild_{gid}_security (
                key   TEXT PRIMARY KEY,
                value TEXT
            );
        """)
        await db.commit()

async def guild_delete_data(guild_id: int):
    """Полное удаление всех данных сервера (GDPR)"""
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        for table in ["xp", "economy", "invites", "warnings", "security"]:
            await db.execute(f"DROP TABLE IF EXISTS guild_{gid}_{table}")
        await db.execute("DELETE FROM subscriptions WHERE guild_id=?", (guild_id,))
        await db.commit()

# ─── Subscription ────────────────────────────────────────────
async def get_tier(guild_id: int) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT tier, expires_at FROM subscriptions WHERE guild_id=?", (guild_id,)
        ) as cur:
            row = await cur.fetchone()
            if not row:
                return TIER_FREE
            tier, exp = row
            if exp and datetime.utcnow() > datetime.fromisoformat(exp):
                await db.execute("UPDATE subscriptions SET tier=0 WHERE guild_id=?", (guild_id,))
                await db.commit()
                return TIER_FREE
            return tier

async def set_tier(guild_id: int, tier: int, days: int = 30):
    exp = (datetime.utcnow() + timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO subscriptions (guild_id, tier, expires_at) VALUES (?,?,?)
            ON CONFLICT(guild_id) DO UPDATE SET tier=excluded.tier, expires_at=excluded.expires_at
        """, (guild_id, tier, exp))
        await db.commit()

# ─── XP / Economy ────────────────────────────────────────────
async def get_xp(guild_id, user_id):
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(f"SELECT xp FROM guild_{gid}_xp WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            return row[0] if row else 0

async def add_xp(guild_id, user_id, amount=5):
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"""
            INSERT INTO guild_{gid}_xp (user_id, xp) VALUES (?,?)
            ON CONFLICT(user_id) DO UPDATE SET xp=xp+?
        """, (user_id, amount, amount))
        await db.commit()

async def get_coins(guild_id, user_id):
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(f"SELECT coins FROM guild_{gid}_economy WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
            return row[0] if row else 0

async def add_coins(guild_id, user_id, amount):
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"""
            INSERT INTO guild_{gid}_economy (user_id, coins) VALUES (?,?)
            ON CONFLICT(user_id) DO UPDATE SET coins=coins+?
        """, (user_id, amount, amount))
        await db.commit()

async def get_leaderboard(guild_id, limit=10):
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            f"SELECT user_id, xp FROM guild_{gid}_xp ORDER BY xp DESC LIMIT ?", (limit,)
        ) as c:
            return await c.fetchall()

# ─── Security settings ───────────────────────────────────────
DEFAULT_LOG_SETTINGS = {
    "joins": True, "leaves": True, "bans": True, "timeouts": True,
    "msg_delete": True, "msg_edit": True, "invites": True, "suspicious": True,
    "nick_change": False, "role_change": False, "avatar_change": False,
    "voice": False, "channels": False, "roles": False,
    "server_edit": False, "reactions": False, "threads": False, "slash_commands": False,
    "anti_spam": True, "anti_raid": True, "alt_detector": True,
    "alt_min_days": 7,
    "log_channel_id": 0,
    "raid_threshold": 5,    # joins per 10 seconds
    "spam_threshold": 5,    # messages per 3 seconds
}

LOG_NAMES = {
    "joins": "Входы на сервер", "leaves": "Выходы с сервера",
    "bans": "Баны/разбаны", "timeouts": "Таймауты",
    "msg_delete": "Удалённые сообщения", "msg_edit": "Отредактированные сообщения",
    "invites": "Инвайты", "suspicious": "Подозрительные аккаунты",
    "nick_change": "Смена ника", "role_change": "Смена ролей",
    "avatar_change": "Смена аватарки", "voice": "Голосовые каналы",
    "channels": "Каналы", "roles": "Роли",
    "server_edit": "Настройки сервера", "reactions": "Реакции",
    "threads": "Треды", "slash_commands": "Слэш-команды",
    "anti_spam": "Анти-спам", "anti_raid": "Анти-рейд",
    "alt_detector": "Детектор альтов",
}

async def get_security(guild_id: int, key: str):
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            f"SELECT value FROM guild_{gid}_security WHERE key=?", (key,)
        ) as c:
            row = await c.fetchone()
            if row:
                val = row[0]
                default = DEFAULT_LOG_SETTINGS.get(key)
                if isinstance(default, bool):
                    return val == "1"
                elif isinstance(default, int):
                    return int(val)
                return val
            return DEFAULT_LOG_SETTINGS.get(key)

async def set_security(guild_id: int, key: str, value):
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        str_val = "1" if value is True else "0" if value is False else str(value)
        await db.execute(f"""
            INSERT INTO guild_{gid}_security (key, value) VALUES (?,?)
            ON CONFLICT(key) DO UPDATE SET value=excluded.value
        """, (key, str_val))
        await db.commit()

async def get_log_channel(guild: discord.Guild):
    ch_id = await get_security(guild.id, "log_channel_id")
    if ch_id:
        return guild.get_channel(int(ch_id))
    return None

# ─── Invites ─────────────────────────────────────────────────
async def save_invite(guild_id, code, inviter_id, inviter_name, member_id, member_name):
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"INSERT INTO guild_{gid}_invites (invite_code,inviter_id,inviter_name,member_id,member_name,joined_at) VALUES(?,?,?,?,?,?)",
            (code, inviter_id, inviter_name, member_id, member_name, datetime.utcnow().strftime("%d.%m.%Y %H:%M"))
        )
        await db.commit()

async def get_invite_history(guild_id, code):
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            f"SELECT member_name, member_id, inviter_name, inviter_id, joined_at FROM guild_{gid}_invites WHERE invite_code=?",
            (code,)
        ) as c:
            return await c.fetchall()

async def get_user_invites(guild_id, inviter_id):
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            f"SELECT invite_code, member_name, joined_at FROM guild_{gid}_invites WHERE inviter_id=?",
            (inviter_id,)
        ) as c:
            return await c.fetchall()

async def get_invite_top(guild_id, limit=10):
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(f"""
            SELECT inviter_name, inviter_id, COUNT(*) as cnt
            FROM guild_{gid}_invites
            GROUP BY inviter_id
            ORDER BY cnt DESC LIMIT ?
        """, (limit,)) as c:
            return await c.fetchall()

# ─── Warnings ────────────────────────────────────────────────
async def add_warning(guild_id, user_id, mod_id, reason):
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            f"INSERT INTO guild_{gid}_warnings (user_id,mod_id,reason,issued_at) VALUES(?,?,?,?)",
            (user_id, mod_id, reason, datetime.utcnow().strftime("%d.%m.%Y %H:%M"))
        )
        await db.commit()

async def get_warnings(guild_id, user_id):
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            f"SELECT mod_id, reason, issued_at FROM guild_{gid}_warnings WHERE user_id=? ORDER BY id DESC",
            (user_id,)
        ) as c:
            return await c.fetchall()

async def clear_warnings(guild_id, user_id):
    gid = str(guild_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"DELETE FROM guild_{gid}_warnings WHERE user_id=?", (user_id,))
        await db.commit()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  AI — GROQ (БЕСПЛАТНО) + GEMINI FALLBACK
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def ask_ai(prompt: str, system: str = "You are NexusBot, a helpful Discord assistant for gaming communities. Be concise.") -> str:
    """
    Tries Groq first (free, fast), falls back to Gemini (free).
    No paid API needed!
    """
    # 1. Try Groq — Llama 3.3 70B, completely free
    if GROQ_KEY:
        try:
            headers = {
                "Authorization": f"Bearer {GROQ_KEY}",
                "Content-Type": "application/json"
            }
            payload = {
                "model": "llama-3.3-70b-versatile",
                "messages": [
                    {"role": "system", "content": system},
                    {"role": "user", "content": prompt}
                ],
                "max_tokens": 600,
                "temperature": 0.7
            }
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers=headers, json=payload,
                    timeout=aiohttp.ClientTimeout(total=20)
                ) as r:
                    data = await r.json()
                    return data["choices"][0]["message"]["content"]
        except Exception:
            pass  # Fall through to Gemini

    # 2. Fallback: Gemini Flash (also free, 1500 req/day)
    if GEMINI_KEY:
        try:
            url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={GEMINI_KEY}"
            payload = {
                "contents": [{"parts": [{"text": f"{system}\n\n{prompt}"}]}],
                "generationConfig": {"maxOutputTokens": 600}
            }
            async with aiohttp.ClientSession() as s:
                async with s.post(url, json=payload, timeout=aiohttp.ClientTimeout(total=20)) as r:
                    data = await r.json()
                    return data["candidates"][0]["content"]["parts"][0]["text"]
        except Exception:
            pass

    return "❌ AI unavailable. Add GROQ_API_KEY or GEMINI_API_KEY to .env (both free)"


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  BOT SETUP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

intents = discord.Intents.all()
bot = commands.Bot(command_prefix="!", intents=intents)  bot.invite_cache = {}

def upsell_embed(required: str) -> discord.Embed:
    return discord.Embed(
        title="🔒 Upgrade Required",
        description=f"This needs **{required}**.\n\n⭐ Premium — €4.99/mo\n💎 Pro — €9.99/mo\n\nnexusbot.gg/premium",
        color=0xFF4444
    )

# Albion helpers
async def albion_find_player(session, name):
    async with session.get(f"{ALBION_BASE}/search?q={name}", timeout=aiohttp.ClientTimeout(total=10)) as r:
        if r.status != 200:
            return None, None
        data = await r.json()
        players = data.get("players", [])
        if not players:
            return None, None
        return players[0]["Id"], players[0]["Name"]

def fmt_item(item_id):
    if not item_id:
        return "—"
    parts = item_id.replace("@", " ✦").split("_")
    return " ".join([p for p in parts if not (p.startswith("T") and p[1:].isdigit())]).title() or item_id


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  EVENTS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.event
async def on_ready():
    await db_init()
    for guild in bot.guilds:
        await ensure_guild_tables(guild.id)
        try:
    invites = await guild.invites()
    bot.invite_cache[guild.id] = {inv.code: inv.uses for inv in invites}
except:
    bot.invite_cache[guild.id] = {}
        except Exception:
            bot.invite_cache[guild.id] = {}
    print(f"✅ NexusBot v5 online as {bot.user} — {len(bot.guilds)} servers")
    await bot.tree.sync()
    await bot.change_presence(
        activity=discord.Activity(type=discord.ActivityType.watching, name="/help | nexusbot.gg")
    )

@bot.event
async def on_guild_join(guild):
    await ensure_guild_tables(guild.id)

@bot.event
async def on_guild_remove(guild):
    # Optionally delete data when bot is removed
    pass

@bot.event
async def on_app_command_error(interaction: discord.Interaction, error):
    try:
        await interaction.response.send_message(f"❌ Error: {error}", ephemeral=True)
    except Exception:
        try:
            await interaction.followup.send(f"❌ Error: {error}", ephemeral=True)
        except Exception:
            pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SECURITY EVENTS (Premium)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.event
async def on_member_join(member: discord.Member):
    await ensure_guild_tables(member.guild.id)

    tier = await get_tier(member.guild.id)
    log_ch = await get_log_channel(member.guild)

# ─── Invite tracking ──────────────────────────────────────
used_invite = None
inviter_name = "неизвестно"
inviter_id = 0
invite_code = "неизвестно"

if tier >= TIER_PREMIUM:
    old_cache = bot.invite_cache.get(member.guild.id, {}).copy()
    await asyncio.sleep(2)

    try:
        new_invites = await member.guild.invites()
        bot.invite_cache[member.guild.id] = {inv.code: inv.uses for inv in new_invites}

        for inv in new_invites:
            if inv.code in old_cache and inv.uses > old_cache[inv.code]:
                used_invite = inv
                break
    except:
        pass

    if used_invite and used_invite.inviter:
        inviter_name = used_invite.inviter.name
        inviter_id = used_invite.inviter.id
        invite_code = used_invite.code

        await save_invite(
            member.guild.id,
            invite_code,
            inviter_id,
            inviter_name,
            member.id,
            member.name
        )


    # ─── Anti-raid ────────────────────────────────────────────
    if tier >= TIER_PREMIUM and await get_security(member.guild.id, "anti_raid"):
        now = time.time()
        _join_tracker[member.guild.id].append(now)
        _join_tracker[member.guild.id] = [t for t in _join_tracker[member.guild.id] if now - t < 10]
        threshold = await get_security(member.guild.id, "raid_threshold") or 5
        if len(_join_tracker[member.guild.id]) >= threshold and log_ch:
            raid_embed = discord.Embed(
                title="🚨 ВОЗМОЖНЫЙ РЕЙД",
                description=f"**{len(_join_tracker[member.guild.id])}** пользователей зашли за последние 10 секунд!",
                color=0xFF0000,
                timestamp=datetime.utcnow()
            )
            raid_embed.add_field(name="Действие", value="Рассмотри временное включение верификации или lockdown", inline=False)
            await log_ch.send("@here", embed=raid_embed)

    # ─── Alt detector + suspicious ────────────────────────────
    age_days = (datetime.utcnow() - member.created_at.replace(tzinfo=None)).days
    alt_min = await get_security(member.guild.id, "alt_min_days") if tier >= TIER_PREMIUM else 7

    if age_days < 1:
        suspicion = "🔴 Очень подозрительный (< 1 дня)"
    elif age_days < alt_min:
        suspicion = f"🟡 Новый аккаунт (< {alt_min} дней)"
    else:
        suspicion = "🟢 Обычный"

    # ─── Join log ─────────────────────────────────────────────
    if log_ch and await get_security(member.guild.id, "joins"):
        embed = discord.Embed(
            title="📥 Участник вошёл",
            color=0x00FF00,
            timestamp=datetime.utcnow()
        )
        embed.set_thumbnail(url=member.display_avatar.url)
        embed.add_field(name="Участник", value=f"{member.mention} (`{member.name}`)", inline=False)
        embed.add_field(name="ID", value=member.id, inline=True)
        embed.add_field(name="Возраст аккаунта", value=f"{age_days} дней — {suspicion}", inline=True)
        if tier >= TIER_PREMIUM:
            embed.add_field(name="Инвайт", value=f"`{invite_code}`", inline=True)
            embed.add_field(name="Пригласил", value=f"{inviter_name} (`{inviter_id}`)", inline=True)
        await log_ch.send(embed=embed)

    # ─── Suspicious alert ─────────────────────────────────────
    if tier >= TIER_PREMIUM and log_ch and age_days < alt_min and await get_security(member.guild.id, "suspicious"):
        susp = discord.Embed(
            title="🚨 Подозрительный аккаунт",
            color=0xFF0000,
            timestamp=datetime.utcnow()
        )
        susp.set_thumbnail(url=member.display_avatar.url)
        susp.add_field(name="Участник", value=f"{member.mention} (`{member.name}`)", inline=False)
        susp.add_field(name="ID", value=member.id, inline=True)
        susp.add_field(name="Аккаунт создан", value=member.created_at.strftime("%d.%m.%Y"), inline=True)
        susp.add_field(name="Возраст", value=f"{age_days} дней", inline=True)
        susp.add_field(name="Инвайт", value=f"`{invite_code}`", inline=True)
        await log_ch.send(embed=susp)

@bot.event
async def on_member_remove(member: discord.Member):
    log_ch = await get_log_channel(member.guild)
    if not log_ch or not await get_security(member.guild.id, "leaves"):
        return
    roles = [r.mention for r in member.roles if r.name != "@everyone"]
    embed = discord.Embed(title="📤 Участник вышел", color=0xFF4444, timestamp=datetime.utcnow())
    embed.set_thumbnail(url=member.display_avatar.url)
    embed.add_field(name="Участник", value=f"{member.mention} (`{member.name}`)", inline=False)
    embed.add_field(name="ID", value=member.id, inline=True)
    embed.add_field(name="Роли", value=", ".join(roles) if roles else "нет", inline=False)
    await log_ch.send(embed=embed)

@bot.event
async def on_member_ban(guild, user):
    log_ch = await get_log_channel(guild)
    if not log_ch or not await get_security(guild.id, "bans"):
        return
    embed = discord.Embed(title="🔨 Участник забанен", color=0x8B0000, timestamp=datetime.utcnow())
    embed.add_field(name="Участник", value=f"{user.mention} (`{user.name}`)", inline=False)
    embed.add_field(name="ID", value=user.id, inline=True)
    await log_ch.send(embed=embed)

@bot.event
async def on_member_unban(guild, user):
    log_ch = await get_log_channel(guild)
    if not log_ch or not await get_security(guild.id, "bans"):
        return
    embed = discord.Embed(title="✅ Участник разбанен", color=0x00FF00, timestamp=datetime.utcnow())
    embed.add_field(name="Участник", value=f"{user.mention} (`{user.name}`)", inline=False)
    embed.add_field(name="ID", value=user.id, inline=True)
    await log_ch.send(embed=embed)

@bot.event
async def on_member_update(before: discord.Member, after: discord.Member):
    log_ch = await get_log_channel(after.guild)
    if not log_ch:
        return
    # Nick change
    if await get_security(after.guild.id, "nick_change") and before.nick != after.nick:
        embed = discord.Embed(title="✏️ Изменён ник", color=0x3498db, timestamp=datetime.utcnow())
        embed.add_field(name="Участник", value=after.mention, inline=False)
        embed.add_field(name="Было", value=before.nick or before.name, inline=True)
        embed.add_field(name="Стало", value=after.nick or after.name, inline=True)
        await log_ch.send(embed=embed)
    # Roles
    if await get_security(after.guild.id, "role_change"):
        added = set(after.roles) - set(before.roles)
        removed = set(before.roles) - set(after.roles)
        if added or removed:
            embed = discord.Embed(title="🎭 Изменены роли", color=0x9b59b6, timestamp=datetime.utcnow())
            embed.add_field(name="Участник", value=after.mention, inline=False)
            if added:
                embed.add_field(name="Добавлены", value=", ".join(r.mention for r in added), inline=False)
            if removed:
                embed.add_field(name="Убраны", value=", ".join(r.mention for r in removed), inline=False)
            await log_ch.send(embed=embed)
    # Timeout
    if await get_security(after.guild.id, "timeouts") and before.timed_out_until != after.timed_out_until:
        if after.timed_out_until:
            embed = discord.Embed(title="🔇 Участник замьючен", color=0xe67e22, timestamp=datetime.utcnow())
            embed.add_field(name="Участник", value=after.mention, inline=False)
            embed.add_field(name="До", value=after.timed_out_until.strftime("%d.%m.%Y %H:%M"), inline=True)
        else:
            embed = discord.Embed(title="🔊 Мут снят", color=0x2ecc71, timestamp=datetime.utcnow())
            embed.add_field(name="Участник", value=after.mention, inline=False)
        await log_ch.send(embed=embed)

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild:
        return
    gid, uid = message.guild.id, message.author.id
    await add_xp(gid, uid, 5)
    await add_coins(gid, uid, 1)
    # Level up
    xp = await get_xp(gid, uid)
    if xp > 0 and xp % 100 < 5:
        lvl = xp // 100
        await message.channel.send(f"⚡ {message.author.mention} reached **Level {lvl}**! 🎉", delete_after=10)
    # Anti-spam
    tier = await get_tier(gid)
    if tier >= TIER_PREMIUM and await get_security(gid, "anti_spam"):
        now = time.time()
        _msg_tracker[uid].append(now)
        _msg_tracker[uid] = [t for t in _msg_tracker[uid] if now - t < 3]
        threshold = await get_security(gid, "spam_threshold") or 5
        if len(_msg_tracker[uid]) >= threshold:
            try:
                await message.author.timeout(timedelta(minutes=5), reason="Auto: spam detected")
                log_ch = await get_log_channel(message.guild)
                if log_ch:
                    embed = discord.Embed(title="🤖 Авто-мут (спам)", color=0xe67e22, timestamp=datetime.utcnow())
                    embed.add_field(name="Участник", value=message.author.mention, inline=True)
                    embed.add_field(name="Канал", value=message.channel.mention, inline=True)
                    embed.add_field(name="Длительность", value="5 минут", inline=True)
                    await log_ch.send(embed=embed)
                _msg_tracker[uid] = []
            except Exception:
                pass
    await bot.process_commands(message)

@bot.event
async def on_message_delete(message: discord.Message):
    if message.author.bot or not message.guild:
        return
    log_ch = await get_log_channel(message.guild)
    if not log_ch or not await get_security(message.guild.id, "msg_delete"):
        return
    embed = discord.Embed(title="🗑️ Сообщение удалено", color=0xFF4444, timestamp=datetime.utcnow())
    embed.add_field(name="Автор", value=f"{message.author.mention} (`{message.author.name}`)", inline=False)
    embed.add_field(name="Канал", value=message.channel.mention, inline=True)
    embed.add_field(name="Текст", value=message.content[:1024] or "*(пусто/вложение)*", inline=False)
    if message.attachments:
        embed.add_field(name="Вложения", value="\n".join(a.url for a in message.attachments), inline=False)
    await log_ch.send(embed=embed)

@bot.event
async def on_message_edit(before: discord.Message, after: discord.Message):
    if before.author.bot or before.content == after.content or not before.guild:
        return
    log_ch = await get_log_channel(before.guild)
    if not log_ch or not await get_security(before.guild.id, "msg_edit"):
        return
    embed = discord.Embed(title="✏️ Сообщение отредактировано", color=0xFFFF00, timestamp=datetime.utcnow())
    embed.add_field(name="Автор", value=f"{before.author.mention}", inline=False)
    embed.add_field(name="Канал", value=before.channel.mention, inline=True)
    embed.add_field(name="Было", value=before.content[:512] or "*(пусто)*", inline=False)
    embed.add_field(name="Стало", value=after.content[:512] or "*(пусто)*", inline=False)
    embed.add_field(name="Ссылка", value=f"[Перейти]({after.jump_url})", inline=False)
    await log_ch.send(embed=embed)

@bot.event
async def on_voice_state_update(member, before, after):
    log_ch = await get_log_channel(member.guild)
    if not log_ch or not await get_security(member.guild.id, "voice"):
        return
    if before.channel is None and after.channel:
        desc, color = f"вошёл в **{after.channel.name}**", 0x00FF00
    elif before.channel and after.channel is None:
        desc, color = f"вышел из **{before.channel.name}**", 0xFF4444
    elif before.channel != after.channel:
        desc, color = f"перешёл **{before.channel.name}** → **{after.channel.name}**", 0x3498db
    else:
        return
    embed = discord.Embed(title="🔊 Голосовой канал", color=color, timestamp=datetime.utcnow())
    embed.add_field(name="Участник", value=f"{member.mention}", inline=True)
    embed.add_field(name="Действие", value=desc, inline=False)
    await log_ch.send(embed=embed)

@bot.event
async def on_guild_channel_create(channel_created):
    log_ch = await get_log_channel(channel_created.guild)
    if not log_ch or not await get_security(channel_created.guild.id, "channels"):
        return
    embed = discord.Embed(title="📁 Канал создан", color=0x00FF00, timestamp=datetime.utcnow())
    embed.add_field(name="Канал", value=channel_created.mention, inline=True)
    embed.add_field(name="Тип", value=str(channel_created.type), inline=True)
    await log_ch.send(embed=embed)

@bot.event
async def on_guild_channel_delete(channel_deleted):
    log_ch = await get_log_channel(channel_deleted.guild)
    if not log_ch or not await get_security(channel_deleted.guild.id, "channels"):
        return
    embed = discord.Embed(title="🗑️ Канал удалён", color=0xFF4444, timestamp=datetime.utcnow())
    embed.add_field(name="Канал", value=channel_deleted.name, inline=True)
    await log_ch.send(embed=embed)

@bot.event
async def on_guild_role_create(role):
    log_ch = await get_log_channel(role.guild)
    if not log_ch or not await get_security(role.guild.id, "roles"):
        return
    embed = discord.Embed(title="🎭 Роль создана", color=0x00FF00, timestamp=datetime.utcnow())
    embed.add_field(name="Роль", value=role.mention, inline=True)
    await log_ch.send(embed=embed)

@bot.event
async def on_guild_role_delete(role):
    log_ch = await get_log_channel(role.guild)
    if not log_ch or not await get_security(role.guild.id, "roles"):
        return
    embed = discord.Embed(title="🗑️ Роль удалена", color=0xFF4444, timestamp=datetime.utcnow())
    embed.add_field(name="Роль", value=role.name, inline=True)
    await log_ch.send(embed=embed)

@bot.event
async def on_guild_update(before, after):
    log_ch = await get_log_channel(after)
    if not log_ch or not await get_security(after.id, "server_edit"):
        return
    if before.name != after.name:
        embed = discord.Embed(title="⚙️ Название сервера изменено", color=0x3498db, timestamp=datetime.utcnow())
        embed.add_field(name="Было", value=before.name, inline=True)
        embed.add_field(name="Стало", value=after.name, inline=True)
        await log_ch.send(embed=embed)

@bot.event
async def on_thread_create(thread):
    log_ch = await get_log_channel(thread.guild)
    if not log_ch or not await get_security(thread.guild.id, "threads"):
        return
    embed = discord.Embed(title="🧵 Тред создан", color=0x00FF00, timestamp=datetime.utcnow())
    embed.add_field(name="Тред", value=thread.mention, inline=True)
    if thread.parent:
        embed.add_field(name="Канал", value=thread.parent.mention, inline=True)
    await log_ch.send(embed=embed)

@bot.event
async def on_invite_create(invite):
    gid = invite.guild.id
    if gid not in bot.invite_cache:
        bot.invite_cache[gid] = {}
    bot.invite_cache[gid][invite.code] = invite.uses
    log_ch = await get_log_channel(invite.guild)
    if not log_ch or not await get_security(gid, "invites"):
        return
    embed = discord.Embed(title="🔗 Инвайт создан", color=0x1abc9c, timestamp=datetime.utcnow())
    embed.add_field(name="Создал", value=f"{invite.inviter.mention} (`{invite.inviter.name}`)", inline=False)
    embed.add_field(name="Код", value=f"`{invite.code}`", inline=True)
    expires = invite.expires_at.strftime("%d.%m.%Y %H:%M") if invite.expires_at else "никогда"
    embed.add_field(name="Истекает", value=expires, inline=True)
    await log_ch.send(embed=embed)

@bot.event
async def on_invite_delete(invite):
    gid = invite.guild.id
    if gid in bot.invite_cache:
        bot.invite_cache[gid].pop(invite.code, None)
    log_ch = await get_log_channel(invite.guild)
    if not log_ch or not await get_security(gid, "invites"):
        return
    embed = discord.Embed(title="❌ Инвайт удалён", color=0x95a5a6, timestamp=datetime.utcnow())
    embed.add_field(name="Код", value=f"`{invite.code}`", inline=True)
    await log_ch.send(embed=embed)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ADMIN COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="setpremium", description="[ADMIN] Set server subscription")
@app_commands.describe(tier="0=Free 1=Premium 2=Pro", days="Duration")
async def setpremium(interaction: discord.Interaction, tier: int, days: int = 30):
    if interaction.user.id not in OWNER_IDS and not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ No permission.", ephemeral=True)
    await set_tier(interaction.guild_id, tier, days)
    await interaction.response.send_message(
        f"✅ **{TIER_NAMES.get(tier,'?')}** activated for {days} days.", ephemeral=True
    )

@bot.tree.command(name="subinfo", description="Check subscription status")
async def subinfo(interaction: discord.Interaction):
    tier = await get_tier(interaction.guild_id)
    embed = discord.Embed(title="📋 Subscription", color=TIER_COLORS[tier])
    embed.add_field(name="Tier", value=TIER_NAMES[tier], inline=True)
    if tier == TIER_FREE:
        embed.add_field(name="Upgrade at", value="nexusbot.gg/premium", inline=True)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="datadelete", description="[ADMIN] Delete ALL server data (GDPR)")
async def datadelete(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ Admins only.", ephemeral=True)
    await guild_delete_data(interaction.guild_id)
    await interaction.response.send_message(
        "✅ All server data deleted. Tables will be recreated on next bot action.", ephemeral=True
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SECURITY SLASH COMMANDS (Premium)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="logset", description="Set log channel or toggle a log event [Premium]")
@app_commands.describe(
    channel="Set this channel as the log channel",
    event="Event key to toggle (e.g. voice, reactions)",
    enabled="True to enable, False to disable"
)
async def logset(interaction: discord.Interaction,
                 channel: discord.TextChannel = None,
                 event: str = None,
                 enabled: bool = None):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно разрешение Manage Server.", ephemeral=True)
    msgs = []
    if channel:
        await set_security(interaction.guild_id, "log_channel_id", channel.id)
        msgs.append(f"✅ Лог-канал установлен: {channel.mention}")
    if event:
        if event not in LOG_NAMES:
            keys = ", ".join(f"`{k}`" for k in LOG_NAMES.keys())
            return await interaction.response.send_message(f"❌ Неизвестный ключ.\nДоступные: {keys}", ephemeral=True)
        val = enabled if enabled is not None else not await get_security(interaction.guild_id, event)
        await set_security(interaction.guild_id, event, val)
        state = "✅ включён" if val else "❌ выключен"
        msgs.append(f"Лог **{LOG_NAMES[event]}** {state}")
    if not msgs:
        return await interaction.response.send_message("Укажи channel и/или event.", ephemeral=True)
    await interaction.response.send_message("\n".join(msgs), ephemeral=True)

@bot.tree.command(name="logstatus", description="Show all log settings [Premium]")
async def logstatus(interaction: discord.Interaction):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    embed = discord.Embed(title="⚙️ Log Settings", color=0x00E5FF, timestamp=datetime.utcnow())
    on_list, off_list = [], []
    for key, name in LOG_NAMES.items():
        val = await get_security(interaction.guild_id, key)
        if val:
            on_list.append(f"✅ `{key}` — {name}")
        else:
            off_list.append(f"❌ `{key}` — {name}")
    embed.add_field(name="Включено", value="\n".join(on_list) or "нет", inline=False)
    embed.add_field(name="Выключено", value="\n".join(off_list) or "нет", inline=False)
    ch_id = await get_security(interaction.guild_id, "log_channel_id")
    ch = interaction.guild.get_channel(int(ch_id)) if ch_id else None
    embed.add_field(name="Лог-канал", value=ch.mention if ch else "не установлен — используй `/logset channel:#канал`", inline=False)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="invcheck", description="Invite history by code [Premium]")
@app_commands.describe(code="Invite code")
async def invcheck(interaction: discord.Interaction, code: str):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    rows = await get_invite_history(interaction.guild_id, code)
    embed = discord.Embed(title=f"🔗 Инвайт: {code}", color=0x1abc9c, timestamp=datetime.utcnow())
    embed.add_field(name="Использований", value=str(len(rows)), inline=True)
    if rows:
        inviter = f"{rows[0][2]} (`{rows[0][3]}`)"
        embed.add_field(name="Создал", value=inviter, inline=True)
        members = "\n".join(f"{i+1}. {r[0]} | `{r[1]}` | {r[4]}" for i, r in enumerate(rows[:15]))
        embed.add_field(name="Кто зашёл", value=members[:1024], inline=False)
    else:
        embed.description = "Никто не входил по этому инвайту (или данных нет)."
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="invuser", description="Show invites created by a user [Premium]")
@app_commands.describe(member="Member to check")
async def invuser(interaction: discord.Interaction, member: discord.Member):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    rows = await get_user_invites(interaction.guild_id, member.id)
    embed = discord.Embed(title=f"👤 Инвайты: {member.display_name}", color=0x9b59b6, timestamp=datetime.utcnow())
    embed.add_field(name="Приглашено всего", value=str(len(rows)), inline=True)
    if rows:
        lines = "\n".join(f"`{r[0]}` → {r[1]} | {r[2]}" for r in rows[:15])
        embed.add_field(name="История", value=lines[:1024], inline=False)
    else:
        embed.description = "Нет данных."
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="invtop", description="Top inviters on this server [Premium]")
async def invtop(interaction: discord.Interaction):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    rows = await get_invite_top(interaction.guild_id)
    embed = discord.Embed(title="🏆 Топ по приглашениям", color=0xFFD700, timestamp=datetime.utcnow())
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = [f"{medals[i]} **{r[0]}** — {r[2]} чел." for i, r in enumerate(rows)]
    embed.description = "\n".join(lines) if lines else "Нет данных."
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="invdel", description="Delete an invite [Premium]")
@app_commands.describe(code="Invite code to delete")
async def invdel(interaction: discord.Interaction, code: str):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно Manage Server.", ephemeral=True)
    try:
        invite = await interaction.guild.fetch_invite(code)
        await invite.delete()
        await interaction.response.send_message(f"✅ Инвайт `{code}` удалён.", ephemeral=True)
    except discord.NotFound:
        await interaction.response.send_message(f"❌ Инвайт `{code}` не найден.", ephemeral=True)

# ─── MODERATION COMMANDS ─────────────────────────────────────

@bot.tree.command(name="warn", description="Warn a member [Premium]")
@app_commands.describe(member="Member to warn", reason="Reason")
async def warn(interaction: discord.Interaction, member: discord.Member, reason: str = "No reason"):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.kick_members:
        return await interaction.response.send_message("❌ Нужно Kick Members.", ephemeral=True)
    await add_warning(interaction.guild_id, member.id, interaction.user.id, reason)
    warns = await get_warnings(interaction.guild_id, member.id)
    embed = discord.Embed(title="⚠️ Предупреждение выдано", color=0xFFFF00)
    embed.add_field(name="Участник", value=member.mention, inline=True)
    embed.add_field(name="Причина", value=reason, inline=True)
    embed.add_field(name="Всего варнов", value=str(len(warns)), inline=True)
    await interaction.response.send_message(embed=embed)
    try:
        await member.send(f"⚠️ Вы получили предупреждение на **{interaction.guild.name}**: {reason}")
    except Exception:
        pass

@bot.tree.command(name="warnings", description="Show warnings for a member [Premium]")
@app_commands.describe(member="Member to check")
async def warnings(interaction: discord.Interaction, member: discord.Member):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    rows = await get_warnings(interaction.guild_id, member.id)
    embed = discord.Embed(title=f"⚠️ Варны: {member.display_name}", color=0xFFFF00)
    embed.add_field(name="Всего", value=str(len(rows)), inline=True)
    if rows:
        lines = "\n".join(f"{i+1}. {r[2]} — {r[1]} (`<@{r[0]}>`)" for i, r in enumerate(rows[:10]))
        embed.add_field(name="История", value=lines[:1024], inline=False)
    else:
        embed.description = "Нет предупреждений."
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="clearwarns", description="Clear all warnings for a member [Premium]")
@app_commands.describe(member="Member to clear")
async def clearwarns(interaction: discord.Interaction, member: discord.Member):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.kick_members:
        return await interaction.response.send_message("❌ Нужно Kick Members.", ephemeral=True)
    await clear_warnings(interaction.guild_id, member.id)
    await interaction.response.send_message(f"✅ Варны {member.mention} сброшены.", ephemeral=True)

@bot.tree.command(name="ban", description="Ban a member [Premium]")
@app_commands.describe(member="Member to ban", reason="Reason")
async def ban_cmd(interaction: discord.Interaction, member: discord.Member, reason: str = "No reason"):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.ban_members:
        return await interaction.response.send_message("❌ Нужно Ban Members.", ephemeral=True)
    await member.ban(reason=reason)
    await interaction.response.send_message(f"🔨 {member.mention} забанен. Причина: {reason}")

@bot.tree.command(name="kick", description="Kick a member [Premium]")
@app_commands.describe(member="Member to kick", reason="Reason")
async def kick_cmd(interaction: discord.Interaction, member: discord.Member, reason: str = "No reason"):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.kick_members:
        return await interaction.response.send_message("❌ Нужно Kick Members.", ephemeral=True)
    await member.kick(reason=reason)
    await interaction.response.send_message(f"👟 {member.mention} кикнут. Причина: {reason}")

@bot.tree.command(name="mute", description="Timeout a member [Premium]")
@app_commands.describe(member="Member to mute", minutes="Duration in minutes", reason="Reason")
async def mute_cmd(interaction: discord.Interaction, member: discord.Member, minutes: int = 10, reason: str = "No reason"):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.moderate_members:
        return await interaction.response.send_message("❌ Нужно Moderate Members.", ephemeral=True)
    await member.timeout(timedelta(minutes=minutes), reason=reason)
    await interaction.response.send_message(f"🔇 {member.mention} замьючен на {minutes} мин. Причина: {reason}")

@bot.tree.command(name="unmute", description="Remove timeout from a member [Premium]")
@app_commands.describe(member="Member to unmute")
async def unmute_cmd(interaction: discord.Interaction, member: discord.Member):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.moderate_members:
        return await interaction.response.send_message("❌ Нужно Moderate Members.", ephemeral=True)
    await member.timeout(None)
    await interaction.response.send_message(f"🔊 Мут снят с {member.mention}")

@bot.tree.command(name="lockdown", description="Lock a channel [Premium]")
@app_commands.describe(channel="Channel to lock (default: current)")
async def lockdown(interaction: discord.Interaction, channel: discord.TextChannel = None):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.manage_channels:
        return await interaction.response.send_message("❌ Нужно Manage Channels.", ephemeral=True)
    ch = channel or interaction.channel
    await ch.set_permissions(interaction.guild.default_role, send_messages=False)
    embed = discord.Embed(title="🔒 Канал заблокирован", description=f"{ch.mention} закрыт для отправки сообщений.", color=0xFF4444)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="unlock", description="Unlock a channel [Premium]")
@app_commands.describe(channel="Channel to unlock (default: current)")
async def unlock(interaction: discord.Interaction, channel: discord.TextChannel = None):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.manage_channels:
        return await interaction.response.send_message("❌ Нужно Manage Channels.", ephemeral=True)
    ch = channel or interaction.channel
    await ch.set_permissions(interaction.guild.default_role, send_messages=None)
    embed = discord.Embed(title="🔓 Канал разблокирован", description=f"{ch.mention} снова открыт.", color=0x00FF00)
    await interaction.response.send_message(embed=embed)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FREE COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="help", description="Show all commands")
@app_commands.describe(lang="Language: en / de / ru / ua")
async def help_cmd(interaction: discord.Interaction, lang: str = "en"):
    tier = await get_tier(interaction.guild_id)
    titles = {"en": "📖 NexusBot v5", "de": "📖 Befehle", "ru": "📖 Команды", "ua": "📖 Команди"}
    embed = discord.Embed(title=titles.get(lang, titles["en"]), color=TIER_COLORS[tier])
    embed.add_field(name="🆓 Free", value="`/ping` `/userinfo` `/serverinfo` `/rank` `/leaderboard` `/coins` `/poll` `/remind` `/lfg` `/weather` `/translate` `/stats` `/kills` `/deaths` `/guild` `/battle` `/compare` `/history` `/rs` `/mc` `/subinfo`", inline=False)
    embed.add_field(name="⭐ Premium €4.99/mo", value="**AI:** `/ai` `/summarize` `/roast`\n**Games:** `/val` `/cs2` `/lol` `/lostark` `/giveaway`\n**Security:** `/logset` `/logstatus` `/invcheck` `/invuser` `/invtop` `/invdel` `/warn` `/warnings` `/clearwarns` `/ban` `/kick` `/mute` `/unmute` `/lockdown` `/unlock`", inline=False)
    embed.add_field(name="💎 Pro €9.99/mo", value="`/blackmarket` `/party` `/tournament` + all Premium", inline=False)
    embed.add_field(name="Current", value=TIER_NAMES[tier], inline=True)
    embed.set_footer(text="nexusbot.gg · AI by Groq (free) + Gemini (free)")
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="ping", description="Check latency")
async def ping(interaction: discord.Interaction):
    ms = round(bot.latency * 1000)
    color = 0x00FF9D if ms < 100 else 0xFFA500 if ms < 200 else 0xFF4444
    await interaction.response.send_message(embed=discord.Embed(title="🏓 Pong!", description=f"**{ms}ms**", color=color))

@bot.tree.command(name="userinfo", description="Show user info")
@app_commands.describe(member="Target user")
async def userinfo(interaction: discord.Interaction, member: discord.Member = None):
    m = member or interaction.user
    embed = discord.Embed(title=f"👤 {m.display_name}", color=0x00E5FF)
    embed.set_thumbnail(url=m.display_avatar.url)
    embed.add_field(name="ID", value=m.id, inline=True)
    embed.add_field(name="Joined", value=m.joined_at.strftime("%d.%m.%Y"), inline=True)
    embed.add_field(name="Created", value=m.created_at.strftime("%d.%m.%Y"), inline=True)
    roles = [r.mention for r in m.roles[1:]]
    embed.add_field(name=f"Roles ({len(roles)})", value=" ".join(roles) if roles else "None", inline=False)
    xp = await get_xp(interaction.guild_id, m.id)
    coins = await get_coins(interaction.guild_id, m.id)
    embed.add_field(name="XP / Lvl", value=f"{xp} / {xp//100}", inline=True)
    embed.add_field(name="Coins", value=str(coins), inline=True)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="serverinfo", description="Server info")
async def serverinfo(interaction: discord.Interaction):
    g, tier = interaction.guild, await get_tier(interaction.guild_id)
    embed = discord.Embed(title=f"🏠 {g.name}", color=TIER_COLORS[tier])
    if g.icon:
        embed.set_thumbnail(url=g.icon.url)
    embed.add_field(name="Owner", value=g.owner.mention, inline=True)
    embed.add_field(name="Members", value=g.member_count, inline=True)
    embed.add_field(name="Channels", value=len(g.channels), inline=True)
    embed.add_field(name="Roles", value=len(g.roles), inline=True)
    embed.add_field(name="NexusBot", value=TIER_NAMES[tier], inline=True)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="rank", description="Your XP rank")
async def rank(interaction: discord.Interaction):
    xp = await get_xp(interaction.guild_id, interaction.user.id)
    p = xp % 100
    bar = "█" * (p // 10) + "░" * (10 - p // 10)
    embed = discord.Embed(title=f"⚡ {interaction.user.display_name}", color=0x7C3AED)
    embed.add_field(name="Level", value=f"**{xp//100}**", inline=True)
    embed.add_field(name="XP", value=f"**{xp}**", inline=True)
    embed.add_field(name="Progress", value=f"`{bar}` {p}/100", inline=False)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="leaderboard", description="Top 10 active members")
async def leaderboard(interaction: discord.Interaction):
    rows = await get_leaderboard(interaction.guild_id)
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = []
    for i, (uid, xp) in enumerate(rows):
        u = interaction.guild.get_member(uid)
        name = u.display_name if u else f"User {uid}"
        lines.append(f"{medals[i]} **{name}** — {xp} XP · Lvl {xp//100}")
    embed = discord.Embed(title="🏆 XP Leaderboard",
                          description="\n".join(lines) if lines else "No data yet!", color=0xFFD700)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="coins", description="Your coin balance")
async def coins_cmd(interaction: discord.Interaction):
    c = await get_coins(interaction.guild_id, interaction.user.id)
    embed = discord.Embed(title="💰 Balance", description=f"**{c} coins** 🪙\n*+1 coin per message*", color=0xFFD700)
    await interaction.response.send_message(embed=embed)

@bot.tree.command(name="poll", description="Create a poll")
@app_commands.describe(question="Question", option1="Option 1", option2="Option 2",
                       option3="Option 3", option4="Option 4")
async def poll(interaction: discord.Interaction, question: str, option1: str, option2: str,
               option3: str = None, option4: str = None):
    options = [o for o in [option1, option2, option3, option4] if o]
    emojis = ["1️⃣","2️⃣","3️⃣","4️⃣"]
    embed = discord.Embed(title=f"📊 {question}", color=0x00E5FF)
    for i, opt in enumerate(options):
        embed.add_field(name=f"{emojis[i]} {opt}", value="​", inline=False)
    embed.set_footer(text=f"Poll by {interaction.user.display_name}")
    await interaction.response.send_message(embed=embed)
    msg = await interaction.original_response()
    for i in range(len(options)):
        await msg.add_reaction(emojis[i])

@bot.tree.command(name="remind", description="DM reminder")
@app_commands.describe(minutes="Remind in X minutes", message="Reminder text")
async def remind(interaction: discord.Interaction, minutes: int, message: str):
    if not 1 <= minutes <= 10080:
        return await interaction.response.send_message("⚠️ 1–10080 minutes only.", ephemeral=True)
    await interaction.response.send_message(f"⏰ Reminder set for **{minutes} min**!", ephemeral=True)
    await asyncio.sleep(minutes * 60)
    try:
        embed = discord.Embed(title="⏰ Reminder!", description=message, color=0x00E5FF)
        embed.set_footer(text=f"Set {minutes}m ago · {interaction.guild.name}")
        await interaction.user.send(embed=embed)
    except discord.Forbidden:
        pass

@bot.tree.command(name="lfg", description="Looking For Group")
@app_commands.describe(game="Game", slots="Players needed", note="Extra info")
async def lfg(interaction: discord.Interaction, game: str, slots: int = 1, note: str = ""):
    embed = discord.Embed(title=f"🎮 LFG — {game}", color=0x00FF9D)
    embed.description = f"**{interaction.user.display_name}** needs **{slots}** player(s)"
    if note:
        embed.add_field(name="📝", value=note, inline=False)
    embed.add_field(name="Join", value=f"React ✅ or DM {interaction.user.mention}", inline=False)
    embed.set_footer(text="Auto-deletes in 2 hours")
    await interaction.response.send_message(embed=embed)
    msg = await interaction.original_response()
    await msg.add_reaction("✅")
    await msg.add_reaction("❌")
    await asyncio.sleep(7200)
    try:
        await msg.delete()
    except Exception:
        pass

@bot.tree.command(name="weather", description="Check weather")
@app_commands.describe(city="City name")
@cooldown(10)
async def weather(interaction: discord.Interaction, city: str):
    await interaction.response.defer()
    if not WEATHER_KEY:
        return await interaction.followup.send("❌ Add WEATHER_API_KEY to .env")
    url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={WEATHER_KEY}&units=metric"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url) as r:
                if r.status != 200:
                    return await interaction.followup.send(f"❌ City **{city}** not found.")
                d = await r.json()
        embed = discord.Embed(title=f"🌤️ {d['name']}, {d['sys']['country']}", color=0x00E5FF)
        embed.add_field(name="🌡️", value=f"{d['main']['temp']:.1f}°C (feels {d['main']['feels_like']:.1f}°C)", inline=True)
        embed.add_field(name="☁️", value=d["weather"][0]["description"].capitalize(), inline=True)
        embed.add_field(name="💧", value=f"{d['main']['humidity']}%", inline=True)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")

@bot.tree.command(name="translate", description="Translate text")
@app_commands.describe(text="Text to translate", to="Target: en/de/ru/ua")
@cooldown(5)
async def translate(interaction: discord.Interaction, text: str, to: str = "en"):
    await interaction.response.defer()
    langs = {"en": "English", "de": "German", "ru": "Russian", "ua": "Ukrainian"}
    target = langs.get(to.lower(), "English")
    result = await ask_ai(f"Translate to {target}. Reply ONLY with the translation:\n\n{text}",
                          system="You are a precise translator. Output only the translated text.")
    embed = discord.Embed(title=f"🌍 → {target}", color=0x00E5FF)
    embed.add_field(name="Original", value=text[:1024], inline=False)
    embed.add_field(name="Translated", value=result[:1024], inline=False)
    await interaction.followup.send(embed=embed)


# ─── ALBION (FREE) ───────────────────────────────────────────

@bot.tree.command(name="stats", description="Albion Online player stats")
@app_commands.describe(player="Player name")
@cooldown(5)
async def stats(interaction: discord.Interaction, player: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            pid, pname = await albion_find_player(s, player)
            if not pid:
                return await interaction.followup.send(f"❌ Player **{player}** not found.")
            async with s.get(f"{ALBION_BASE}/players/{pid}") as r:
                p = await r.json()
        kf, df = p.get("KillFame", 0), p.get("DeathFame", 0)
        embed = discord.Embed(title=f"🗡️ {pname}", color=0x00E5FF)
        embed.add_field(name="Guild", value=p.get("GuildName") or "—", inline=True)
        embed.add_field(name="Alliance", value=p.get("AllianceName") or "—", inline=True)
        embed.add_field(name="K/D", value=str(round(kf/df, 2) if df else "∞"), inline=True)
        embed.add_field(name="Kill Fame", value=f"{kf:,}", inline=True)
        embed.add_field(name="Death Fame", value=f"{df:,}", inline=True)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")

@bot.tree.command(name="kills", description="Last 5 kills of an Albion player")
@app_commands.describe(player="Player name")
@cooldown(5)
async def kills(interaction: discord.Interaction, player: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            pid, pname = await albion_find_player(s, player)
            if not pid:
                return await interaction.followup.send(f"❌ Player **{player}** not found.")
            async with s.get(f"{ALBION_BASE}/players/{pid}/kills?limit=5") as r:
                events = await r.json()
        if not events:
            return await interaction.followup.send(f"📭 **{pname}** has no recent kills.")
        embed = discord.Embed(title=f"⚔️ {pname} — Last Kills", color=0xFF4444)
        for ev in events[:5]:
            victim = ev.get("Victim", {})
            vname = victim.get("Name", "?")
            fame = ev.get("TotalVictimKillFame", 0)
            ktime = ev.get("TimeStamp", "")[:10]
            weapon = fmt_item(victim.get("Equipment", {}).get("MainHand", {}).get("Type", ""))
            embed.add_field(name=f"🔪 {vname}", value=f"Fame: **{fame:,}** · Weapon: {weapon}\n📅 {ktime}", inline=False)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")

@bot.tree.command(name="deaths", description="Last 5 deaths of an Albion player")
@app_commands.describe(player="Player name")
@cooldown(5)
async def deaths(interaction: discord.Interaction, player: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            pid, pname = await albion_find_player(s, player)
            if not pid:
                return await interaction.followup.send(f"❌ Player **{player}** not found.")
            async with s.get(f"{ALBION_BASE}/players/{pid}/deaths?limit=5") as r:
                events = await r.json()
        if not events:
            return await interaction.followup.send(f"📭 **{pname}** has no recent deaths.")
        embed = discord.Embed(title=f"💀 {pname} — Last Deaths", color=0x888888)
        for ev in events[:5]:
            killer = ev.get("Killer", {})
            kname = killer.get("Name", "?")
            fame = ev.get("TotalVictimKillFame", 0)
            ktime = ev.get("TimeStamp", "")[:10]
            embed.add_field(name=f"☠️ Killed by {kname}", value=f"Fame lost: **{fame:,}**\n📅 {ktime}", inline=False)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")

@bot.tree.command(name="guild", description="Albion guild info")
@app_commands.describe(name="Guild name")
@cooldown(10)
async def guild_cmd(interaction: discord.Interaction, name: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{ALBION_BASE}/search?q={name}") as r:
                data = await r.json()
            guilds = data.get("guilds", [])
            if not guilds:
                return await interaction.followup.send(f"❌ Guild **{name}** not found.")
            gid = guilds[0]["Id"]
            async with s.get(f"{ALBION_BASE}/guilds/{gid}") as r:
                gdata = await r.json()
            async with s.get(f"{ALBION_BASE}/guilds/{gid}/members") as r:
                members = await r.json()
        embed = discord.Embed(title=f"🏰 {gdata.get('Name', name)}", color=0x00E5FF)
        embed.add_field(name="Alliance", value=gdata.get("AllianceName") or "—", inline=True)
        embed.add_field(name="Members", value=str(len(members)), inline=True)
        top = sorted(members, key=lambda m: m.get("KillFame", 0), reverse=True)[:5]
        if top:
            lines = "\n".join(f"{i+1}. **{m.get('Name','?')}** — {m.get('KillFame',0):,}" for i, m in enumerate(top))
            embed.add_field(name="🏆 Top by Fame", value=lines, inline=False)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")

@bot.tree.command(name="battle", description="Recent Albion battles")
@cooldown(15)
async def battle(interaction: discord.Interaction):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{ALBION_BASE}/battles?sort=recent&limit=5") as r:
                battles = await r.json()
        if not battles:
            return await interaction.followup.send("📭 No recent battles.")
        embed = discord.Embed(title="⚔️ Recent Battles", color=0xFF6B35)
        for b in battles[:5]:
            guilds = list(b.get("Guilds", {}).keys())[:3]
            guild_str = " vs ".join(guilds) if guilds else "Open world"
            embed.add_field(
                name=f"⚔️ {guild_str}",
                value=f"Kills: **{b.get('TotalKills',0)}** · Fame: **{b.get('TotalFame',0):,}**\n📅 {b.get('StartTime','')[:10]}",
                inline=False
            )
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")

@bot.tree.command(name="compare", description="Compare two Albion players")
@app_commands.describe(player1="Player 1", player2="Player 2")
@cooldown(10)
async def compare(interaction: discord.Interaction, player1: str, player2: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            p1id, p1name = await albion_find_player(s, player1)
            p2id, p2name = await albion_find_player(s, player2)
            if not p1id or not p2id:
                return await interaction.followup.send("❌ One or both players not found.")
            async with s.get(f"{ALBION_BASE}/players/{p1id}") as r:
                d1 = await r.json()
            async with s.get(f"{ALBION_BASE}/players/{p2id}") as r:
                d2 = await r.json()
        kf1, df1 = d1.get("KillFame", 0), d1.get("DeathFame", 0)
        kf2, df2 = d2.get("KillFame", 0), d2.get("DeathFame", 0)
        kd1 = round(kf1/df1, 2) if df1 else float("inf")
        kd2 = round(kf2/df2, 2) if df2 else float("inf")
        w1, w2 = ("✅","❌") if kf1 > kf2 else ("❌","✅") if kf2 > kf1 else ("🟡","🟡")
        embed = discord.Embed(title=f"⚔️ {p1name} vs {p2name}", color=0x00E5FF)
        embed.add_field(name=f"{w1} {p1name}", value=f"Kill Fame: **{kf1:,}**\nK/D: **{kd1}**\nGuild: {d1.get('GuildName') or '—'}", inline=True)
        embed.add_field(name="VS", value="​", inline=True)
        embed.add_field(name=f"{w2} {p2name}", value=f"Kill Fame: **{kf2:,}**\nK/D: **{kd2}**\nGuild: {d2.get('GuildName') or '—'}", inline=True)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")

@bot.tree.command(name="history", description="Albion player activity (last 7 days)")
@app_commands.describe(player="Player name")
@cooldown(10)
async def history(interaction: discord.Interaction, player: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            pid, pname = await albion_find_player(s, player)
            if not pid:
                return await interaction.followup.send(f"❌ Player **{player}** not found.")
            async with s.get(f"{ALBION_BASE}/players/{pid}/kills?limit=50") as rk:
                all_kills = await rk.json()
            async with s.get(f"{ALBION_BASE}/players/{pid}/deaths?limit=50") as rd:
                all_deaths = await rd.json()
        cutoff = datetime.utcnow() - timedelta(days=7)
        def recent(evs):
            out = []
            for ev in evs:
                try:
                    if datetime.fromisoformat(ev.get("TimeStamp","")[:19]) >= cutoff:
                        out.append(ev)
                except Exception:
                    pass
            return out
        wk, wd = recent(all_kills), recent(all_deaths)
        embed = discord.Embed(title=f"📅 {pname} — Last 7 Days", color=0x00FF9D)
        embed.add_field(name="⚔️ Kills", value=f"**{len(wk)}**", inline=True)
        embed.add_field(name="💀 Deaths", value=f"**{len(wd)}**", inline=True)
        embed.add_field(name="K/D", value=f"**{round(len(wk)/len(wd),2) if wd else '∞'}**", inline=True)
        fame = sum(e.get("TotalVictimKillFame",0) for e in wk)
        embed.add_field(name="Fame earned", value=f"**{fame:,}**", inline=True)
        if wk:
            victims = {}
            for ev in wk:
                v = ev.get("Victim",{}).get("Name","?")
                victims[v] = victims.get(v,0)+1
            top = max(victims, key=victims.get)
            embed.add_field(name="Favourite target 🎯", value=f"**{top}** ({victims[top]}x)", inline=True)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")

@bot.tree.command(name="rs", description="OSRS player skills")
@app_commands.describe(username="OSRS username")
@cooldown(5)
async def rs(interaction: discord.Interaction, username: str):
    await interaction.response.defer()
    skills = ["Overall","Attack","Defence","Strength","Hitpoints","Ranged","Prayer","Magic",
              "Cooking","Woodcutting","Fletching","Fishing","Firemaking","Crafting","Smithing","Mining"]
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://secure.runescape.com/m=hiscore_oldschool/index_lite.ws?player={username}") as r:
                if r.status != 200:
                    return await interaction.followup.send(f"❌ Player **{username}** not found.")
                lines = (await r.text()).strip().split("\n")
        embed = discord.Embed(title=f"⚔️ OSRS — {username}", color=0xB5651D)
        overall = lines[0].split(",")
        embed.add_field(name="Total Level", value=overall[1], inline=True)
        embed.add_field(name="Total XP", value=f"{int(overall[2]):,}", inline=True)
        top = ""
        for i in range(1, min(9, len(lines))):
            p = lines[i].split(",")
            if len(p) >= 2 and int(p[1]) > 1:
                top += f"**{skills[i]}**: {p[1]}\n"
        if top:
            embed.add_field(name="Skills", value=top, inline=False)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")

@bot.tree.command(name="mc", description="Minecraft server status")
@app_commands.describe(address="Server IP")
@cooldown(10)
async def mc(interaction: discord.Interaction, address: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.mcstatus.io/v2/status/java/{address}") as r:
                d = await r.json()
        if not d.get("online"):
            return await interaction.followup.send(f"🔴 **{address}** is offline.")
        embed = discord.Embed(title=f"🟢 {address}", color=0x00FF9D)
        embed.add_field(name="Players", value=f"{d['players']['online']}/{d['players']['max']}", inline=True)
        embed.add_field(name="Version", value=d.get("version",{}).get("name_clean","?"), inline=True)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PREMIUM AI + GAMES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="ai", description="Ask AI anything [Premium]")
@app_commands.describe(question="Your question")
@cooldown(5)
async def ai_cmd(interaction: discord.Interaction, question: str):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    await interaction.response.defer()
    answer = await ask_ai(question)
    embed = discord.Embed(title="🤖 NexusBot AI", description=answer[:4000], color=0x00E5FF)
    embed.set_footer(text=f"Powered by Groq/Gemini (free) · Asked by {interaction.user.display_name}")
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="summarize", description="AI summary of recent chat [Premium]")
@app_commands.describe(count="Messages (max 50)")
@cooldown(30)
async def summarize(interaction: discord.Interaction, count: int = 20):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    await interaction.response.defer()
    msgs = []
    async for msg in interaction.channel.history(limit=min(count, 50)):
        if not msg.author.bot:
            msgs.append(f"{msg.author.display_name}: {msg.content}")
    msgs.reverse()
    summary = await ask_ai("\n".join(msgs), system="Summarize this Discord chat in 3-5 bullet points. Be concise.")
    embed = discord.Embed(title=f"📋 Summary ({count} messages)", description=summary, color=0x00E5FF)
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="roast", description="AI roast a member 🔥 [Premium]")
@app_commands.describe(member="Who to roast")
@cooldown(10)
async def roast(interaction: discord.Interaction, member: discord.Member):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    await interaction.response.defer()
    roles = [r.name for r in member.roles[1:]]
    days = (datetime.utcnow() - member.joined_at.replace(tzinfo=None)).days
    text = await ask_ai(
        f"Funny 2-3 sentence roast for Discord user: Name={member.display_name}, "
        f"Roles={', '.join(roles) or 'None'}, Days in server={days}. Playful, never offensive.",
        system="Write friendly roasts for Discord servers. Never be genuinely mean."
    )
    embed = discord.Embed(title=f"🔥 Roasting {member.display_name}", description=text, color=0xFF6B35)
    embed.set_thumbnail(url=member.display_avatar.url)
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="giveaway", description="Start a giveaway [Premium]")
@app_commands.describe(prize="Prize", duration="Duration in minutes")
async def giveaway(interaction: discord.Interaction, prize: str, duration: int = 60):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    embed = discord.Embed(
        title="🎉 GIVEAWAY",
        description=f"**Prize:** {prize}\nReact 🎮 to enter!\n⏰ Ends in **{duration} min**",
        color=0x00FF9D
    )
    embed.set_footer(text=f"By {interaction.user.display_name}")
    await interaction.response.send_message(embed=embed)
    msg = await interaction.original_response()
    await msg.add_reaction("🎮")
    await asyncio.sleep(duration * 60)
    msg = await interaction.channel.fetch_message(msg.id)
    reaction = discord.utils.get(msg.reactions, emoji="🎮")
    users = [u async for u in reaction.users() if not u.bot]
    winner = random.choice(users) if users else None
    await interaction.channel.send(f"🎊 {winner.mention} won **{prize}**!" if winner else "😢 No entries.")

@bot.tree.command(name="val", description="Valorant rank [Premium]")
@app_commands.describe(username="Riot ID e.g. Player#EUW")
@cooldown(10)
async def val(interaction: discord.Interaction, username: str):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    await interaction.response.defer()
    if "#" not in username:
        return await interaction.followup.send("❌ Format: **Name#TAG**")
    name, tag = username.split("#", 1)
    headers = {"Authorization": HENRIK_KEY} if HENRIK_KEY else {}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.henrikdev.xyz/valorant/v2/mmr/eu/{name}/{tag}", headers=headers) as r:
                d = await r.json()
        if d.get("status") != 200:
            return await interaction.followup.send(f"❌ Player **{username}** not found.")
        data = d["data"]
        embed = discord.Embed(title=f"🔫 Valorant — {username}", color=0xFF4655)
        embed.add_field(name="Rank", value=data.get("currenttierpatched","Unranked"), inline=True)
        embed.add_field(name="RR", value=str(data.get("ranking_in_tier",0)), inline=True)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")

@bot.tree.command(name="cs2", description="CS2 stats [Premium]")
@app_commands.describe(steam_id="Steam ID64 or vanity name")
@cooldown(10)
async def cs2(interaction: discord.Interaction, steam_id: str):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    await interaction.response.defer()
    if not STEAM_KEY:
        return await interaction.followup.send("❌ Add STEAM_API_KEY to .env")
    if not steam_id.isdigit():
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.steampowered.com/ISteamUser/ResolveVanityURL/v1/?key={STEAM_KEY}&vanityurl={steam_id}") as r:
                steam_id = (await r.json()).get("response",{}).get("steamid", steam_id)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.steampowered.com/ISteamUserStats/GetUserStatsForGame/v2/?appid=730&key={STEAM_KEY}&steamid={steam_id}") as r:
                sd = {st["name"]: st["value"] for st in (await r.json()).get("playerstats",{}).get("stats",[])}
        kills, deaths = sd.get("total_kills",0), sd.get("total_deaths",0)
        embed = discord.Embed(title=f"🎯 CS2 — {steam_id}", color=0xF0A500)
        embed.add_field(name="K/D", value=str(round(kills/deaths,2) if deaths else "∞"), inline=True)
        embed.add_field(name="Kills", value=f"{kills:,}", inline=True)
        embed.add_field(name="Wins", value=f"{sd.get('total_wins',0):,}", inline=True)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")

@bot.tree.command(name="lol", description="League of Legends rank [Premium]")
@app_commands.describe(summoner="Summoner name", region="Region (euw1, na1...)")
@cooldown(10)
async def lol(interaction: discord.Interaction, summoner: str, region: str = "euw1"):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    await interaction.response.defer()
    if not RIOT_KEY:
        return await interaction.followup.send("❌ Add RIOT_API_KEY to .env")
    headers = {"X-Riot-Token": RIOT_KEY}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://{region}.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner}", headers=headers) as r:
                if r.status != 200:
                    return await interaction.followup.send(f"❌ Summoner **{summoner}** not found.")
                sid = (await r.json())["id"]
            async with s.get(f"https://{region}.api.riotgames.com/lol/league/v4/entries/by-summoner/{sid}", headers=headers) as r:
                entries = await r.json()
        embed = discord.Embed(title=f"🏆 LoL — {summoner} ({region.upper()})", color=0xC89B3C)
        if not entries:
            embed.description = "Unranked."
        for e in entries:
            w, l = e["wins"], e["losses"]
            embed.add_field(
                name=e["queueType"].replace("_"," ").title(),
                value=f"**{e['tier']} {e['rank']}** · {e['leaguePoints']} LP\n{w}W/{l}L",
                inline=True
            )
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")

@bot.tree.command(name="lostark", description="Lost Ark character [Premium]")
@app_commands.describe(character="Character name")
@cooldown(10)
async def lostark(interaction: discord.Interaction, character: str):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    await interaction.response.defer()
    if not LOSTARK_KEY:
        return await interaction.followup.send("❌ Add LOSTARK_API_KEY to .env")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(
                f"https://developer-lostark.game.onstove.com/characters/{character}/siblings",
                headers={"Authorization": f"bearer {LOSTARK_KEY}"}
            ) as r:
                if r.status != 200:
                    return await interaction.followup.send(f"❌ Character **{character}** not found.")
                chars = await r.json()
        embed = discord.Embed(title=f"⚔️ Lost Ark — {character}", color=0x3D9BD4)
        for c in chars[:8]:
            embed.add_field(name=c.get("CharacterName","?"), value=f"{c.get('CharacterClassName','?')}\niLvl: **{c.get('ItemMaxLevel','?')}**", inline=True)
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PRO COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="blackmarket", description="Albion Black Market profits [Pro]")
@app_commands.describe(category="weapon / armor / bag / mount")
@cooldown(30)
async def blackmarket(interaction: discord.Interaction, category: str = "weapon"):
    if await get_tier(interaction.guild_id) < TIER_PRO:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)
    await interaction.response.defer()
    items = {
        "weapon": ["T8_MAIN_SWORD","T8_2H_CLAYMORE","T8_MAIN_SPEAR","T7_MAIN_SWORD"],
        "armor":  ["T8_ARMOR_PLATE_SET1","T8_ARMOR_LEATHER_SET1","T8_ARMOR_CLOTH_SET1"],
        "bag":    ["T8_BAG","T7_BAG","T6_BAG"],
        "mount":  ["T8_MOUNT_HORSE","T7_MOUNT_HORSE"]
    }.get(category.lower(), ["T8_MAIN_SWORD","T7_MAIN_SWORD"])
    results = []
    try:
        async with aiohttp.ClientSession() as s:
            for item_id in items[:4]:
                async with s.get(f"{ALBION_DATA}/stats/prices/{item_id}?locations=Black+Market,Caerleon,Bridgewatch") as r:
                    if r.status != 200:
                        continue
                    prices = await r.json()
                bm, city_min = 0, 9_999_999_999
                for p in prices:
                    if p.get("city") == "Black Market":
                        bm = max(bm, p.get("sell_price_min",0))
                    elif p.get("sell_price_min",0) > 0:
                        city_min = min(city_min, p.get("sell_price_min",0))
                if bm > 0 and city_min < 9_999_999_999:
                    profit = bm - city_min
                    margin = round(profit/city_min*100,1) if city_min else 0
                    results.append((item_id, city_min, bm, profit, margin))
        results.sort(key=lambda x: x[3], reverse=True)
        embed = discord.Embed(title=f"💰 Black Market — {category.capitalize()}", color=0xFFD700)
        for item_id, buy, sell, profit, margin in results:
            embed.add_field(
                name=item_id.replace("_"," ").title(),
                value=f"Buy: **{buy:,}** → Sell: **{sell:,}**\nProfit: **{profit:,}** ({margin}%)",
                inline=False
            )
        if not results:
            embed.description = "No price data right now."
        embed.set_footer(text="albion-online-data.com · ~15min refresh")
        await interaction.followup.send(embed=embed)
    except Exception as e:
        await interaction.followup.send(f"❌ Error: {e}")

@bot.tree.command(name="party", description="Analyze static dungeon party [Pro]")
@app_commands.describe(p1="Player 1", p2="Player 2", p3="Player 3", p4="Player 4", p5="Player 5")
@cooldown(15)
async def party(interaction: discord.Interaction, p1: str, p2: str, p3: str, p4: str = None, p5: str = None):
    if await get_tier(interaction.guild_id) < TIER_PRO:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)
    await interaction.response.defer()
    players = [p for p in [p1,p2,p3,p4,p5] if p]
    embed = discord.Embed(title=f"⚔️ Party Analysis ({len(players)} players)", color=0x00FF9D)
    total_kf = total_df = found = 0
    lines = []
    async with aiohttp.ClientSession() as s:
        for name in players:
            pid, pname = await albion_find_player(s, name)
            if not pid:
                lines.append(f"❌ **{name}** — not found")
                continue
            async with s.get(f"{ALBION_BASE}/players/{pid}") as r:
                p = await r.json()
            kf, df = p.get("KillFame",0), p.get("DeathFame",0)
            kd = round(kf/df,2) if df else "∞"
            total_kf += kf; total_df += df; found += 1
            lines.append(f"✅ **{pname}** [{p.get('GuildName') or '—'}]\n   K/D: `{kd}` · Fame: `{kf:,}`")
    embed.description = "\n".join(lines)
    if found > 0:
        avg_kd = round(total_kf/total_df,2) if total_df else "∞"
        embed.add_field(name="📊 Stats", value=f"Found: **{found}/{len(players)}**\nFame: **{total_kf:,}**\nAvg K/D: **{avg_kd}**", inline=False)
        verdict = await ask_ai(
            f"2-sentence verdict on Albion static party: {found} players, fame {total_kf:,}, avg K/D {avg_kd}.",
            system="You are an Albion Online expert. Give brief advice."
        )
        embed.add_field(name="🤖 AI Verdict", value=verdict, inline=False)
    await interaction.followup.send(embed=embed)

@bot.tree.command(name="tournament", description="Generate tournament bracket [Pro]")
@app_commands.describe(name="Tournament name", participants="Names separated by commas")
async def tournament(interaction: discord.Interaction, name: str, participants: str):
    if await get_tier(interaction.guild_id) < TIER_PRO:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)
    players = [p.strip() for p in participants.split(",") if p.strip()]
    if len(players) < 2:
        return await interaction.response.send_message("❌ Need at least 2 players.", ephemeral=True)
    random.shuffle(players)
    matchups = [f"⚔️ **{players[i]}** vs **{players[i+1]}**" for i in range(0,len(players)-1,2)]
    if len(players) % 2:
        matchups.append(f"👤 **{players[-1]}** — BYE")
    embed = discord.Embed(title=f"🏆 {name}", color=0xFFD700)
    embed.add_field(name=f"Round 1 ({len(matchups)} matches)", value="\n".join(matchups), inline=False)
    embed.set_footer(text=f"By {interaction.user.display_name} · NexusBot Pro")
    await interaction.response.send_message(embed=embed)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  RUN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

bot.invite_cache = {}  # {guild_id: {code: uses}}

if __name__ == "__main__":
    bot.run(TOKEN)
