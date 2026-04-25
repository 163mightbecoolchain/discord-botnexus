"""
NexusBot v5.0 — Discord Bot for Gaming Communities
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
GROQ_KEY      = os.getenv("GROQ_API_KEY")        # FREE: console.groq.com
GEMINI_KEY    = os.getenv("GEMINI_API_KEY")       # FREE: aistudio.google.com
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")    # paid fallback
WEATHER_KEY   = os.getenv("WEATHER_API_KEY")
HENRIK_KEY    = os.getenv("HENRIK_API_KEY")
RIOT_KEY      = os.getenv("RIOT_API_KEY")
STEAM_KEY     = os.getenv("STEAM_API_KEY")
LOSTARK_KEY   = os.getenv("LOSTARK_API_KEY")
# Google Sheets (опционально, для /blackmarket sheets:yes)
# GOOGLE_CREDENTIALS = весь JSON service account ключа одной строкой
# SHEET_ID = ID таблицы из URL: docs.google.com/spreadsheets/d/ВОТ_ЭТО/edit
GOOGLE_CREDS  = os.getenv("GOOGLE_CREDENTIALS")
SHEET_ID      = os.getenv("SHEET_ID")

DB_PATH     = "nexusbot.db"
ALBION_BASE = "https://gameinfo.albiononline.com/api/gameinfo"
ALBION_DATA = "https://west.albion-online-data.com/api/v2"

TIER_FREE, TIER_PREMIUM, TIER_PRO = 0, 1, 2
TIER_NAMES  = {0: "Free", 1: "⭐ Premium", 2: "💎 Pro"}
TIER_COLORS = {0: 0x6b7fa3, 1: 0x00E5FF, 2: 0xFFD700}

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
                member_id INTEGER, member_name TEXT, joined_at TEXT);
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
                price_watch TEXT DEFAULT '{}');
            CREATE TABLE IF NOT EXISTS price_watch (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
                channel_id INTEGER NOT NULL, item_id TEXT, threshold_pct REAL DEFAULT 5.0,
                last_price INTEGER DEFAULT 0, created_at TEXT);
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
bot = commands.Bot(command_prefix="!", intents=intents)
OWNER_IDS = set()  # {YOUR_USER_ID}

def upsell_embed(req):
    return discord.Embed(title="🔒 Требуется апгрейд",
        description=f"Нужен **{req}**\n\n⭐ Premium — €4.99/мес\n💎 Pro — €9.99/мес\nnexusbot.gg/premium",
        color=0xFF4444)

# ── AI: Groq (free) → Gemini (free) → Claude (paid) ──────────
async def ask_ai(prompt, system="You are NexusBot, a helpful Discord assistant. Be concise."):
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

@bot.event
async def on_ready():
    await db_init()
    # Заполняем глобальный кэш инвайтов для всех серверов
    _invite_cache.clear()
    for guild in bot.guilds:
        await refresh_invite_cache(guild)
    await bot.tree.sync()
    print(f"✅ NexusBot v5 | {bot.user} | Invite cache: {len(_invite_cache)} entries")
    await bot.change_presence(activity=discord.Activity(type=discord.ActivityType.watching, name="/help | nexusbot.gg"))

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
                    e = discord.Embed(title="🚫 Анти-спам", color=0xFF4444, timestamp=datetime.datetime.utcnow())
                    e.add_field(name="Участник", value=message.author.mention)
                    e.add_field(name="Действие", value="Таймаут 30 сек")
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
                    e = discord.Embed(title="🚨 РЕЙД ЗАБЛОКИРОВАН", color=0xFF0000, timestamp=datetime.datetime.utcnow())
                    e.add_field(name="Кикнут", value=f"{member.mention}", inline=False)
                    e.add_field(name="Причина", value="8+ входов за 10 сек", inline=False)
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

@bot.event
async def on_member_remove(member):
    ch = await sec_check(member.guild, "leaves")
    if not ch: return
    roles = [r.mention for r in member.roles if r.name != "@everyone"]
    e = discord.Embed(title="📤 Выход", color=discord.Color.red(), timestamp=datetime.datetime.utcnow())
    e.set_thumbnail(url=member.display_avatar.url)
    e.add_field(name="Участник", value=f"{member.mention} (`{member.name}`)", inline=False)
    e.add_field(name="Роли", value=", ".join(roles) if roles else "нет", inline=False)
    await ch.send(embed=e)

@bot.event
async def on_member_ban(guild, user):
    ch = await sec_check(guild, "bans")
    if not ch: return
    e = discord.Embed(title="🔨 Бан", color=discord.Color.dark_red(), timestamp=datetime.datetime.utcnow())
    e.add_field(name="Участник", value=f"{user.mention} (`{user.name}`)", inline=False)
    await ch.send(embed=e)

@bot.event
async def on_member_unban(guild, user):
    ch = await sec_check(guild, "bans")
    if not ch: return
    e = discord.Embed(title="✅ Разбан", color=discord.Color.green(), timestamp=datetime.datetime.utcnow())
    e.add_field(name="Участник", value=f"{user.mention}", inline=False)
    await ch.send(embed=e)

@bot.event
async def on_member_update(before, after):
    if before.nick != after.nick:
        ch = await sec_check(after.guild, "nick_change")
        if ch:
            e = discord.Embed(title="✏️ Смена ника", color=discord.Color.blue(), timestamp=datetime.datetime.utcnow())
            e.add_field(name="Участник", value=after.mention, inline=False)
            e.add_field(name="Было", value=before.nick or before.name, inline=True)
            e.add_field(name="Стало", value=after.nick or after.name, inline=True)
            await ch.send(embed=e)
    added = set(after.roles)-set(before.roles); removed = set(before.roles)-set(after.roles)
    if added or removed:
        ch = await sec_check(after.guild, "role_change")
        if ch:
            e = discord.Embed(title="🎭 Смена ролей", color=discord.Color.blurple(), timestamp=datetime.datetime.utcnow())
            e.add_field(name="Участник", value=after.mention, inline=False)
            if added: e.add_field(name="Добавлены", value=", ".join(r.mention for r in added), inline=False)
            if removed: e.add_field(name="Убраны", value=", ".join(r.mention for r in removed), inline=False)
            await ch.send(embed=e)
    if before.timed_out_until != after.timed_out_until:
        ch = await sec_check(after.guild, "timeouts")
        if ch:
            if after.timed_out_until:
                e = discord.Embed(title="🔇 Мьют", color=discord.Color.orange(), timestamp=datetime.datetime.utcnow())
                e.add_field(name="Участник", value=after.mention, inline=False)
                e.add_field(name="До", value=after.timed_out_until.strftime("%d.%m.%Y %H:%M"), inline=True)
            else:
                e = discord.Embed(title="🔊 Мьют снят", color=discord.Color.green(), timestamp=datetime.datetime.utcnow())
                e.add_field(name="Участник", value=after.mention, inline=False)
            await ch.send(embed=e)

@bot.event
async def on_message_delete(message):
    if message.author.bot or not message.guild: return
    ch = await sec_check(message.guild, "msg_delete")
    if not ch: return
    e = discord.Embed(title="🗑️ Удалено сообщение", color=discord.Color.red(), timestamp=datetime.datetime.utcnow())
    e.add_field(name="Автор", value=f"{message.author.mention}", inline=True)
    e.add_field(name="Канал", value=getattr(message.channel, 'mention', str(message.channel)), inline=True)
    e.add_field(name="Текст", value=message.content[:1020] or "*(вложение)*", inline=False)
    await ch.send(embed=e)

@bot.event
async def on_message_edit(before, after):
    if before.author.bot or not before.guild or before.content == after.content: return
    ch = await sec_check(before.guild, "msg_edit")
    if not ch: return
    e = discord.Embed(title="✏️ Редактирование", color=discord.Color.yellow(), timestamp=datetime.datetime.utcnow())
    e.add_field(name="Автор", value=before.author.mention, inline=False)
    e.add_field(name="Было", value=before.content[:512] or "*(пусто)*", inline=False)
    e.add_field(name="Стало", value=after.content[:512] or "*(пусто)*", inline=False)
    e.add_field(name="Ссылка", value=f"[Перейти]({after.jump_url})", inline=True)
    await ch.send(embed=e)

@bot.event
async def on_invite_create(invite):
    _invite_cache[f"{invite.guild.id}:{invite.code}"] = invite.uses or 0
    print(f"[INVITE] Created: {invite.code} by {invite.inviter}")
    ch = await sec_check(invite.guild, "invites")
    if not ch: return
    e = discord.Embed(title="🔗 Инвайт создан", color=discord.Color.teal(), timestamp=datetime.datetime.utcnow())
    e.add_field(name="Создал", value=f"{invite.inviter.mention} (`{invite.inviter.name}`)" if invite.inviter else "?", inline=True)
    e.add_field(name="Код", value=f"`{invite.code}`", inline=True)
    e.add_field(name="Использований", value=str(invite.max_uses) if invite.max_uses else "∞", inline=True)
    e.add_field(name="Истекает", value=invite.expires_at.strftime("%d.%m.%Y %H:%M") if invite.expires_at else "никогда", inline=True)
    await ch.send(embed=e)

@bot.event
async def on_invite_delete(invite):
    _invite_cache.pop(f"{invite.guild.id}:{invite.code}", None)
    print(f"[INVITE] Deleted: {invite.code}")
    ch = await sec_check(invite.guild, "invites")
    if not ch: return
    e = discord.Embed(title="❌ Инвайт удалён", color=discord.Color.dark_gray(), timestamp=datetime.datetime.utcnow())
    e.add_field(name="Код", value=f"`{invite.code}`", inline=True)
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
    e = discord.Embed(title="🔊 Голос", color=color, timestamp=datetime.datetime.utcnow())
    e.add_field(name="Участник", value=member.mention, inline=True)
    e.add_field(name="Действие", value=desc, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_channel_create(channel_created):
    ch = await sec_check(channel_created.guild, "channels")
    if not ch: return
    e = discord.Embed(title="📁 Канал создан", color=discord.Color.green(), timestamp=datetime.datetime.utcnow())
    e.add_field(name="Канал", value=channel_created.mention, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_channel_delete(channel_deleted):
    ch = await sec_check(channel_deleted.guild, "channels")
    if not ch: return
    e = discord.Embed(title="🗑️ Канал удалён", color=discord.Color.red(), timestamp=datetime.datetime.utcnow())
    e.add_field(name="Канал", value=channel_deleted.name, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_role_create(role):
    ch = await sec_check(role.guild, "roles")
    if not ch: return
    e = discord.Embed(title="🎭 Роль создана", color=discord.Color.green(), timestamp=datetime.datetime.utcnow())
    e.add_field(name="Роль", value=role.mention, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_role_delete(role):
    ch = await sec_check(role.guild, "roles")
    if not ch: return
    e = discord.Embed(title="🗑️ Роль удалена", color=discord.Color.red(), timestamp=datetime.datetime.utcnow())
    e.add_field(name="Роль", value=role.name, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_guild_update(before, after):
    ch = await sec_check(after, "server_edit")
    if not ch or before.name == after.name: return
    e = discord.Embed(title="⚙️ Сервер изменён", color=discord.Color.blue(), timestamp=datetime.datetime.utcnow())
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
        e = discord.Embed(title="🖼️ Аватарка изменена", color=discord.Color.blue(), timestamp=datetime.datetime.utcnow())
        e.add_field(name="Участник", value=member.mention, inline=False)
        e.set_thumbnail(url=after.display_avatar.url)
        await ch.send(embed=e)

@bot.event
async def on_reaction_add(reaction, user):
    if user.bot or not reaction.message.guild: return
    ch = await sec_check(reaction.message.guild, "reactions")
    if not ch: return
    e = discord.Embed(title="😀 Реакция", color=discord.Color.green(), timestamp=datetime.datetime.utcnow())
    e.add_field(name="Пользователь", value=user.mention, inline=True)
    e.add_field(name="Реакция", value=str(reaction.emoji), inline=True)
    e.add_field(name="Сообщение", value=f"[Перейти]({reaction.message.jump_url})", inline=True)
    await ch.send(embed=e)

@bot.event
async def on_thread_create(thread):
    ch = await sec_check(thread.guild, "threads")
    if not ch: return
    e = discord.Embed(title="🧵 Тред создан", color=discord.Color.green(), timestamp=datetime.datetime.utcnow())
    e.add_field(name="Тред", value=thread.mention, inline=True)
    if thread.parent: e.add_field(name="Канал", value=thread.parent.mention, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_interaction(interaction):
    if not await is_enabled(interaction.guild_id, "slash_commands"): return
    if interaction.type != discord.InteractionType.application_command: return
    ch = await get_log_ch(interaction.guild)
    if not ch: return
    e = discord.Embed(title="⚡ Слэш-команда", color=discord.Color.blurple(), timestamp=datetime.datetime.utcnow())
    e.add_field(name="Пользователь", value=interaction.user.mention, inline=True)
    e.add_field(name="Команда", value=f"`/{interaction.data.get('name','?')}`", inline=True)
    await ch.send(embed=e)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ADMIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="setpremium", description="[ADMIN] Установить тир")
@app_commands.describe(tier="0=Free 1=Premium 2=Pro", days="Дней")
async def setpremium(interaction: discord.Interaction, tier: int, days: int = 30):
    if interaction.user.id not in OWNER_IDS and not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ Нет доступа.", ephemeral=True)
    await set_tier(interaction.guild_id, tier, days)
    await interaction.response.send_message(f"✅ **{TIER_NAMES.get(tier,'?')}** на {days} дней.", ephemeral=True)

@bot.tree.command(name="subinfo", description="Статус подписки")
async def subinfo(interaction: discord.Interaction):
    tier = await get_tier(interaction.guild_id)
    e = discord.Embed(title="📋 Подписка", color=TIER_COLORS[tier])
    e.add_field(name="Тир", value=TIER_NAMES[tier], inline=True)
    if tier == TIER_FREE: e.add_field(name="Апгрейд", value="nexusbot.gg/premium", inline=True)
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
    e = discord.Embed(title="⚠️ Предупреждение", color=0xFFA500)
    e.add_field(name="Участник", value=member.mention, inline=True)
    e.add_field(name="Причина", value=reason, inline=True)
    e.add_field(name="Всего варнов", value=str(len(warns)), inline=True)
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
    e = discord.Embed(title=f"⚠️ Варны: {member.display_name}", color=0xFFA500)
    if not rows: e.description = "Нет варнов."
    for wid, mod_id, reason, created in rows:
        mod = interaction.guild.get_member(mod_id)
        e.add_field(name=f"#{wid} · {created[:10]}", value=f"Модератор: {mod.mention if mod else mod_id}\nПричина: {reason}", inline=False)
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

@bot.tree.command(name="help", description="Все команды")
async def help_cmd(interaction: discord.Interaction):
    tier = await get_tier(interaction.guild_id)
    e = discord.Embed(title="📖 NexusBot v5", color=TIER_COLORS[tier])
    e.add_field(name="🆓 Free", value="`/ping` `/userinfo` `/serverinfo` `/rank` `/leaderboard` `/coins` `/poll` `/remind` `/lfg` `/weather` `/translate` `/stats` `/kills` `/deaths` `/guild` `/battle` `/compare` `/history` `/rs` `/mc`", inline=False)
    e.add_field(name="⭐ Premium €4.99", value="`/security` `/invcheck` `/invuser` `/invdel` `/warn` `/warnings` `/clearwarn` `/purge` `/ai` `/summarize` `/roast` `/giveaway` `/val` `/cs2` `/lol` `/lostark`", inline=False)
    e.add_field(name="💎 Pro €9.99", value="`/blackmarket` `/party` `/tournament`", inline=False)
    e.add_field(name="Текущий тир", value=TIER_NAMES[tier], inline=True)
    e.set_footer(text="nexusbot.gg · AI: Groq/Gemini (free) · Albion API (free)")
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="ping")
async def ping(interaction: discord.Interaction):
    ms = round(bot.latency * 1000)
    color = 0x00FF9D if ms<100 else 0xFFA500 if ms<200 else 0xFF4444
    await interaction.response.send_message(embed=discord.Embed(title="🏓 Pong!", description=f"**{ms}ms**", color=color))

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
    g = interaction.guild; tier = await get_tier(g.id)
    e = discord.Embed(title=f"🏠 {g.name}", color=TIER_COLORS[tier])
    if g.icon: e.set_thumbnail(url=g.icon.url)
    e.add_field(name="Участники", value=g.member_count, inline=True)
    e.add_field(name="Каналы", value=len(g.channels), inline=True)
    e.add_field(name="NexusBot", value=TIER_NAMES[tier], inline=True)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="rank")
async def rank(interaction: discord.Interaction):
    xp = await get_xp(interaction.guild_id, interaction.user.id)
    p = xp % 100; bar = "█"*(p//10)+"░"*(10-p//10)
    e = discord.Embed(title=f"⚡ {interaction.user.display_name}", color=0x7C3AED)
    e.add_field(name="Уровень", value=f"**{xp//100}**", inline=True)
    e.add_field(name="XP", value=f"**{xp}**", inline=True)
    e.add_field(name="Прогресс", value=f"`{bar}` {p}/100", inline=False)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="leaderboard")
async def leaderboard(interaction: discord.Interaction):
    rows = await get_leaderboard(interaction.guild_id)
    medals = ["🥇","🥈","🥉","4️⃣","5️⃣","6️⃣","7️⃣","8️⃣","9️⃣","🔟"]
    lines = [f"{medals[i]} **{interaction.guild.get_member(uid).display_name if interaction.guild.get_member(uid) else uid}** — {xp} XP · Ур.{xp//100}" for i,(uid,xp) in enumerate(rows)]
    e = discord.Embed(title="🏆 Лидерборд", description="\n".join(lines) if lines else "Нет данных.", color=0xFFD700)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="coins")
async def coins_cmd(interaction: discord.Interaction):
    c = await get_coins(interaction.guild_id, interaction.user.id)
    await interaction.response.send_message(embed=discord.Embed(title="💰 Баланс", description=f"**{interaction.user.display_name}** — **{c} монет** 🪙", color=0xFFD700))

@bot.tree.command(name="poll")
@app_commands.describe(question="Вопрос", option1="Вариант 1", option2="Вариант 2", option3="Вариант 3", option4="Вариант 4")
async def poll(interaction: discord.Interaction, question: str, option1: str, option2: str, option3: str = None, option4: str = None):
    options = [o for o in [option1,option2,option3,option4] if o]
    emojis = ["1️⃣","2️⃣","3️⃣","4️⃣"]
    e = discord.Embed(title=f"📊 {question}", color=0x00E5FF)
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
        await interaction.user.send(embed=discord.Embed(title="⏰ Напоминание!", description=message, color=0x00E5FF))
    except discord.Forbidden: pass

@bot.tree.command(name="lfg")
@app_commands.describe(game="Игра", slots="Нужно игроков", note="Дополнительно")
async def lfg(interaction: discord.Interaction, game: str, slots: int = 1, note: str = ""):
    e = discord.Embed(title=f"🎮 LFG — {game}", color=0x00FF9D)
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
    if not WEATHER_KEY: return await interaction.followup.send("❌ Добавь WEATHER_API_KEY в .env")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={WEATHER_KEY}&units=metric") as r:
                if r.status!=200: return await interaction.followup.send(f"❌ Город **{city}** не найден.")
                d = await r.json()
        e = discord.Embed(title=f"🌤️ {d['name']}, {d['sys']['country']}", color=0x00E5FF)
        e.add_field(name="🌡️", value=f"{d['main']['temp']:.1f}°C (ощущается {d['main']['feels_like']:.1f}°C)", inline=True)
        e.add_field(name="☁️", value=d["weather"][0]["description"].capitalize(), inline=True)
        e.add_field(name="💧", value=f"{d['main']['humidity']}%", inline=True)
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
        kf,df = p.get("KillFame",0), p.get("DeathFame",0)
        e = discord.Embed(title=f"🗡️ {pname}", color=0x00E5FF)
        e.add_field(name="Гильдия", value=p.get("GuildName") or "—", inline=True)
        e.add_field(name="K/D", value=str(round(kf/df,2) if df else "∞"), inline=True)
        e.add_field(name="Kill Fame", value=f"{kf:,}", inline=True)
        e.add_field(name="Death Fame", value=f"{df:,}", inline=True)
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
        if not evs: return await interaction.followup.send(f"📭 У **{pname}** нет недавних убийств.")
        e = discord.Embed(title=f"⚔️ {pname} — Последние убийства", color=0xFF4444)
        for ev in evs[:5]:
            v = ev.get("Victim",{}); weapon = fmt_item(v.get("Equipment",{}).get("MainHand",{}).get("Type","") if v.get("Equipment") else "")
            e.add_field(name=f"🔪 {v.get('Name','?')}", value=f"Fame: **{ev.get('TotalVictimKillFame',0):,}** · {weapon}\n📅 {ev.get('TimeStamp','')[:10]}", inline=False)
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
        if not evs: return await interaction.followup.send(f"📭 У **{pname}** нет недавних смертей.")
        e = discord.Embed(title=f"💀 {pname} — Последние смерти", color=0x888888)
        for ev in evs[:5]:
            k = ev.get("Killer",{})
            e.add_field(name=f"☠️ {k.get('Name','?')}", value=f"Fame: **{ev.get('TotalVictimKillFame',0):,}**\n📅 {ev.get('TimeStamp','')[:10]}", inline=False)
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
        e = discord.Embed(title="⚔️ Последние битвы Albion", color=0xFF6B35)
        for b in battles[:5]:
            guilds = list(b.get("Guilds",{}).keys())[:3]
            e.add_field(name=f"⚔️ {' vs '.join(guilds) or 'Open world'}",
                        value=f"Убийств: **{b.get('TotalKills',0)}** · Fame: **{b.get('TotalFame',0):,}**\n📅 {b.get('StartTime','')[:10]}", inline=False)
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
        e = discord.Embed(title=f"📅 {pname} — 7 дней", color=0x00FF9D)
        e.add_field(name="⚔️ Убийств", value=f"**{len(wk)}**", inline=True)
        e.add_field(name="💀 Смертей", value=f"**{len(wd)}**", inline=True)
        e.add_field(name="K/D", value=f"**{round(len(wk)/len(wd),2) if wd else '∞'}**", inline=True)
        e.add_field(name="Fame", value=f"**{fame:,}**", inline=True)
        if wk:
            victims={}
            for ev in wk: victims[ev.get("Victim",{}).get("Name","?")] = victims.get(ev.get("Victim",{}).get("Name","?"),0)+1
            top=max(victims,key=victims.get); e.add_field(name="Жертва 🎯", value=f"**{top}** ({victims[top]}x)", inline=True)
        e.add_field(name="Активность", value="🔥 Очень активен" if len(wk)>20 else "⚡ Активен" if len(wk)>5 else "😴 Тихая неделя", inline=True)
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
        e = discord.Embed(title="🤖 NexusBot AI", description=answer[:4000], color=0x00E5FF)
        e.set_footer(text=f"Спросил: {interaction.user.display_name}")
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
        e = discord.Embed(title=f"🔫 Valorant — {username}", color=0xFF4655)
        e.add_field(name="Ранг", value=data.get("currenttierpatched","Unranked"), inline=True)
        e.add_field(name="RR", value=str(data.get("ranking_in_tier",0)), inline=True)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="cs2")
@app_commands.describe(steam_id="Steam ID64 или vanity name")
@cooldown(10)
async def cs2(interaction: discord.Interaction, steam_id: str):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    if not STEAM_KEY: return await interaction.followup.send("❌ Добавь STEAM_API_KEY в .env")
    if not steam_id.isdigit():
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.steampowered.com/ISteamUser/ResolveVanityURL/v1/?key={STEAM_KEY}&vanityurl={steam_id}") as r:
                steam_id = (await r.json()).get("response",{}).get("steamid",steam_id)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.steampowered.com/ISteamUserStats/GetUserStatsForGame/v2/?appid=730&key={STEAM_KEY}&steamid={steam_id}") as r:
                sd = {s["name"]:s["value"] for s in (await r.json()).get("playerstats",{}).get("stats",[])}
        kills,deaths,wins,hs = sd.get("total_kills",0),sd.get("total_deaths",0),sd.get("total_wins",0),sd.get("total_kills_headshot",0)
        e = discord.Embed(title=f"🎯 CS2 — {steam_id}", color=0xF0A500)
        e.add_field(name="K/D", value=str(round(kills/deaths,2) if deaths else "∞"), inline=True)
        e.add_field(name="Убийств", value=f"{kills:,}", inline=True)
        e.add_field(name="HS%", value=f"{round(hs/kills*100,1) if kills else 0}%", inline=True)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="lol")
@app_commands.describe(summoner="Summoner name", region="Регион (euw1, na1...)")
@cooldown(10)
async def lol(interaction: discord.Interaction, summoner: str, region: str = "euw1"):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    if not RIOT_KEY: return await interaction.followup.send("❌ Добавь RIOT_API_KEY в .env")
    headers={"X-Riot-Token":RIOT_KEY}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://{region}.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner}",headers=headers) as r:
                if r.status!=200: return await interaction.followup.send(f"❌ **{summoner}** не найден.")
                sid = (await r.json())["id"]
            async with s.get(f"https://{region}.api.riotgames.com/lol/league/v4/entries/by-summoner/{sid}",headers=headers) as r:
                entries = await r.json()
        e = discord.Embed(title=f"🏆 LoL — {summoner}", color=0xC89B3C)
        if not entries: e.description="Unranked."
        for en in entries:
            w,l = en["wins"],en["losses"]
            e.add_field(name=en["queueType"].replace("_"," ").title(), value=f"**{en['tier']} {en['rank']}** · {en['leaguePoints']} LP\n{w}W/{l}L · {round(w/(w+l)*100,1) if (w+l) else 0}% WR", inline=True)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="lostark")
@app_commands.describe(character="Имя персонажа")
@cooldown(10)
async def lostark(interaction: discord.Interaction, character: str):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    if not LOSTARK_KEY: return await interaction.followup.send("❌ Добавь LOSTARK_API_KEY в .env")
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
    # ── МЕЧИ / КЛИНКИ ─────────────────────────────────────────
    "sword":        {"Меч":                  "T{t}_MAIN_SWORD{e}"},
    "broadsword":   {"Палаш":                "T{t}_MAIN_BROADSWORD{e}"},
    "claymore":     {"Клеймор":              "T{t}_2H_CLAYMORE{e}"},
    "dualsword":    {"Парные мечи":          "T{t}_2H_DUALSWORD{e}"},
    "dagger":       {"Кинжал":              "T{t}_MAIN_DAGGER{e}"},
    "daggerpair":   {"Парные кинжалы":       "T{t}_2H_DAGGERPAIR{e}"},
    "claws":        {"Когти":               "T{t}_2H_CLAWS{e}"},
    # ── ТОПОРЫ ────────────────────────────────────────────────
    "axe":          {"Боевой топор":         "T{t}_MAIN_AXE{e}"},
    "greataxe":     {"Большой топор":        "T{t}_2H_GREATAXE{e}"},
    "halberd":      {"Алебарда":             "T{t}_2H_HALBERD{e}"},
    # ── БУЛАВЫ / МОЛОТЫ ───────────────────────────────────────
    "mace":         {"Булава":              "T{t}_MAIN_MACE{e}"},
    "heavymace":    {"Большая булава":       "T{t}_2H_HEAVYMACE{e}"},
    "morningstar":  {"Моргенштерн":          "T{t}_MAIN_MORNINGSTAR{e}"},
    "hammer":       {"Молот":               "T{t}_2H_HAMMER{e}"},
    "polehammer":   {"Чекан / Большой молот":"T{t}_2H_POLEHAMMER{e}"},
    # ── ДУБИНЫ / КУЛАКИ ───────────────────────────────────────
    "knuckles":     {"Перчатки крушителя":   "T{t}_2H_KNUCKLES{e}"},
    "gauntlet":     {"Боевые наручи":        "T{t}_MAIN_GAUNTLET{e}"},
    "spikedgauntlet":{"Шипастые рукавицы":  "T{t}_MAIN_SPIKEDGAUNTLET{e}"},
    # ── КОПЬЯ ─────────────────────────────────────────────────
    "spear":        {"Копьё":               "T{t}_MAIN_SPEAR{e}"},
    "pike":         {"Пика":                "T{t}_2H_PIKE{e}"},
    "glaive":       {"Глефа":               "T{t}_2H_GLAIVE{e}"},
    # ── ПОСОХИ ────────────────────────────────────────────────
    "quarterstaff": {"Боевой шест":          "T{t}_2H_QUARTERSTAFF{e}"},
    "ironclad":     {"Железный шест":        "T{t}_2H_IRONCLADSTAFF{e}"},
    "sharpstaff":   {"Острый шест":          "T{t}_2H_SHARPSTAFF{e}"},
    "naturestaff":  {"Древесный посох":      "T{t}_MAIN_NATURESTAFF{e}"},
    "greatnature":  {"Большой древесный":    "T{t}_2H_NATURESTAFFGREAT{e}"},
    "wildstaff":    {"Дикий посох":          "T{t}_2H_WILDSTAFF{e}"},
    "torch":        {"Факел":               "T{t}_MAIN_TORCH{e}"},
    "firestaff":    {"Огненный посох":       "T{t}_MAIN_FIRESTAFF{e}"},
    "greatfire":    {"Большой огненный":     "T{t}_2H_FIRESTAFF{e}"},
    "infernostaff": {"Адский посох":         "T{t}_2H_INFERNOSTAFF{e}"},
    "holystaff":    {"Священный посох":      "T{t}_MAIN_HOLYSTAFF{e}"},
    "greatholly":   {"Большой священный":    "T{t}_2H_HOLYSTAFF{e}"},
    "divinestaff":  {"Божественный посох":   "T{t}_2H_DIVINESTAFF{e}"},
    # ── ЛУКИ ──────────────────────────────────────────────────
    "bow":          {"Лук":                 "T{t}_2H_BOW{e}"},
    "warbow":       {"Боевой лук":           "T{t}_2H_WARBOW{e}"},
    "longbow":      {"Длинный лук":          "T{t}_2H_LONGBOW{e}"},
    # ── АРБАЛЕТЫ ──────────────────────────────────────────────
    "crossbow":     {"Арбалет":             "T{t}_2H_CROSSBOW{e}"},
    "heavycrossbow":{"Тяжелый арбалет":      "T{t}_2H_HEAVYCROSSBOW{e}"},
    "lightcrossbow":{"Лёгкий арбалет":       "T{t}_MAIN_LIGHTCROSSBOW{e}"},
    # ── ЩИТЫ ──────────────────────────────────────────────────
    "shield":       {"Щит":                 "T{t}_OFFHAND_SHIELD{e}"},
    # ── БРОНЯ НАЁМНИКА ────────────────────────────────────────
    "mercboots":    {"Ботинки наёмника":     "T{t}_SHOES_PLATE_SET1{e}"},
    "mercjacket":   {"Куртка наёмника":      "T{t}_ARMOR_PLATE_SET1{e}"},
    "merchood":     {"Капюшон наёмника":     "T{t}_HEAD_PLATE_SET1{e}"},
    # ── БРОНЯ УБИЙЦЫ ──────────────────────────────────────────
    "assboots":     {"Ботинки убийцы":       "T{t}_SHOES_LEATHER_SET1{e}"},
    "assjacket":    {"Куртка убийцы":        "T{t}_ARMOR_LEATHER_SET1{e}"},
    "asshood":      {"Капюшон убийцы":       "T{t}_HEAD_LEATHER_SET1{e}"},
    # ── БРОНЯ ОХОТНИКА ────────────────────────────────────────
    "huntboots":    {"Ботинки охотника":     "T{t}_SHOES_CLOTH_SET1{e}"},
    "huntjacket":   {"Куртка охотника":      "T{t}_ARMOR_CLOTH_SET1{e}"},
    "hunthood":     {"Капюшон охотника":     "T{t}_HEAD_CLOTH_SET1{e}"},
    # ── СТАРЫЕ КАТЕГОРИИ (совместимость) ──────────────────────
    "bag":          {"Сумка":               "T{t}_BAG{e}"},
}

# Группы для удобного выбора
BM_GROUPS = {
    "weapon":  ["sword","broadsword","claymore","dualsword","dagger","daggerpair","claws",
                "axe","greataxe","halberd","mace","heavymace","morningstar","hammer","polehammer",
                "knuckles","gauntlet","spikedgauntlet","spear","pike","glaive",
                "quarterstaff","ironclad","sharpstaff","naturestaff","greatnature","wildstaff",
                "torch","firestaff","greatfire","infernostaff","holystaff","greatholly","divinestaff",
                "bow","warbow","longbow","crossbow","heavycrossbow","lightcrossbow","shield"],
    "armor":   ["mercboots","mercjacket","merchood","assboots","assjacket","asshood",
                "huntboots","huntjacket","hunthood"],
    "bag":     ["bag"],
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
CITY_LOCATIONS = ["Caerleon", "Bridgewatch", "Fort Sterling", "Lymhurst", "Martlock", "Thetford"]
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
        "Caerleon", "Bridgewatch", "Fort Sterling",
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

                for p in prices:
                    city = p.get("city", "").strip()
                    sell = p.get("sell_price_min", 0) or 0
                    buy  = p.get("buy_price_max", 0) or 0

                    if city == "Black Market":
                        if sell > 0: bm_price = max(bm_price, sell)
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
                print(f"[BM DEBUG] {item_id}: BM={bm_price} best_city={best_city}({best_city_sell if best_city_sell < 9_999_999_999 else 0}) profit={city_sell_profit}({city_sell_pct}%)")

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
                })

    print(f"[BM DEBUG] Done: fetched={fetched}, results={found}")
    results.sort(key=lambda x: x["city_pct"], reverse=True)
    return results


@bot.tree.command(name="blackmarket", description="Albion: профит Чёрного рынка [Pro]")
@app_commands.describe(
    category="weapon / armor / bag / конкретный ключ (sword, bow, axe...)",
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

    # ── Discord embeds (по 5 предметов на embed из-за лимита полей) ──
    top = results[:10]
    chunks = [top[i:i+5] for i in range(0, len(top), 5)]

    for chunk_idx, chunk in enumerate(chunks):
        title = (f"💰 Чёрный рынок — {cat_label} T{tier} · {server_name}"
                 if chunk_idx == 0 else f"💰 (продолжение)")
        desc = (f"Топ по % профиту (рыночная цена продажи)\n"
                f"`ЧР`=Чёрный рынок · `Брек`=Бреккилен · сервер: {server_name}")

        e = discord.Embed(title=title, description=desc, color=0xFFD700)

        for item in chunk:
            city_ru   = CITY_NAMES_RU.get(item["best_city"], item["best_city"]) if item["best_city"] else "—"
            brec_ru   = CITY_NAMES_RU["Brecilien"]

            # Строка для лучшего города
            if item["best_city"]:
                city_line = (
                    f"🏙️ **{city_ru}** (рынок продажи): `{item['best_city_sell']:,}` → ЧР: `{item['bm']:,}`\n"
                    f"   Профит: **{item['city_profit']:,}** (**{item['city_pct']}%**)"
                )
                if item["best_city_buy"] > 0:
                    city_line += f"\n   *(ордер покупки в городе: `{item['best_city_buy']:,}`)*"
            else:
                city_line = "🏙️ Нет цены в городах"

            # Строка для Бреккилена
            if item["brec_price"] > 0:
                price_type = "ордер покупки" if item["brec_is_buy_order"] else "рынок продажи"
                brec_line = (
                    f"🌿 **{brec_ru}** ({price_type}): `{item['brec_price']:,}` → ЧР: `{item['bm']:,}`\n"
                    f"   Профит: **{item['brec_profit']:,}** (**{item['brec_pct']}%**)"
                )
            else:
                brec_line = f"🌿 **{brec_ru}**: нет данных"

            e.add_field(
                name=f"🗡️ {item['name']}",
                value=f"{city_line}\n{brec_line}",
                inline=False
            )
            # Иконка предмета на первом элементе чанка
            if chunk_idx == 0 and item == chunk[0]:
                e.set_thumbnail(url=item["icon_url"])

        e.set_footer(
            text=f"albion-online-data.com · {len(results)} предметов · Только T{tier}.0–T{tier}.4"
        )
        await interaction.channel.send(embed=e)

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
    e = discord.Embed(title=f"⚔️ Анализ пати ({len(players)} игроков)", color=0x00FF9D)
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
    e.set_footer(text=f"Создал {interaction.user.display_name} · NexusBot Pro")
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
@app_commands.describe(action="open / close / setup", reason="Причина обращения")
async def ticket(interaction: discord.Interaction, action: str = "open", reason: str = "Обращение в поддержку"):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)

    if action.lower() == "setup":
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("❌ Нужны права администратора.", ephemeral=True)
        # Создаём категорию для тикетов
        cat = await interaction.guild.create_category("🎫 Tickets")
        await set_guild_setting(interaction.guild_id, "ticket_category", cat.id)
        e = discord.Embed(title="✅ Тикеты настроены", color=0x00E5FF)
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
    e = discord.Embed(title="😀 Реакция", color=discord.Color.green(), timestamp=datetime.datetime.utcnow())
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

@bot.tree.command(name="craftcalc", description="Калькулятор крафта Albion [Pro]")
@app_commands.describe(
    item="Ключ предмета (напр. sword, bow, mercjacket)",
    tier="Тир: 6, 7 или 8",
    enchant="Зачаровка: 0-4",
    server="Сервер: eu / us / asia"
)
@cooldown(10)
async def craftcalc(interaction: discord.Interaction, item: str, tier: int = 8, enchant: int = 0, server: str = "eu"):
    if await get_tier(interaction.guild_id) < TIER_PRO:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)
    await interaction.response.defer()

    if tier not in (6, 7, 8) or not (0 <= enchant <= 4):
        return await interaction.followup.send("❌ Тир: 6-8, зачаровка: 0-4")

    item_data = BM_ITEMS.get(item.lower())
    if not item_data:
        return await interaction.followup.send(f"❌ Предмет `{item}` не найден. Используй ключи из `/blackmarket`")

    display, template = list(item_data.items())[0]
    item_id = build_item_id(template, tier, enchant)
    mat_type = ITEM_MATERIAL_TYPE.get(item.lower(), "ore")
    mat_id = MATERIAL_ITEMS[tier][mat_type]
    mat_count = CRAFT_MATERIALS[tier][mat_type]
    base_url = ALBION_SERVERS.get(server, ALBION_SERVERS["eu"])
    locations = "Black%20Market,Caerleon,Bridgewatch,Fort%20Sterling,Lymhurst,Martlock,Thetford"

    try:
        async with aiohttp.ClientSession() as s:
            # Цена готового предмета
            async with s.get(f"{base_url}/stats/prices/{item_id}?locations={locations}",
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
                item_prices = await r.json() if r.status == 200 else []
            # Цена материала
            async with s.get(f"{base_url}/stats/prices/{mat_id}?locations={locations}",
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
                mat_prices = await r.json() if r.status == 200 else []
    except Exception as ex:
        return await interaction.followup.send(f"❌ Ошибка API: {ex}")

    # Парсим цены
    item_bm = item_sell_min = 0
    mat_sell_min = 9_999_999_999
    mat_best_city = None

    for p in item_prices:
        city = p.get("city", ""); sell = p.get("sell_price_min", 0) or 0
        if city == "Black Market" and sell > 0: item_bm = max(item_bm, sell)
        elif sell > 0 and city != "Black Market": item_sell_min = min(item_sell_min, sell) if item_sell_min > 0 else sell

    for p in mat_prices:
        city = p.get("city", ""); sell = p.get("sell_price_min", 0) or 0
        if sell > 0 and city not in ("Black Market", "Brecilien"):
            if sell < mat_sell_min:
                mat_sell_min = sell
                mat_best_city = city

    if mat_sell_min == 9_999_999_999: mat_sell_min = 0

    craft_cost = mat_sell_min * mat_count
    profit_bm = item_bm - craft_cost if item_bm > 0 and craft_cost > 0 else 0
    profit_sell = item_sell_min - craft_cost if item_sell_min > 0 and craft_cost > 0 else 0
    margin_bm = round(profit_bm / craft_cost * 100, 1) if craft_cost > 0 else 0
    margin_sell = round(profit_sell / craft_cost * 100, 1) if craft_cost > 0 else 0

    city_ru = CITY_NAMES_RU.get(mat_best_city, mat_best_city or "неизвестно")
    color = 0x00FF9D if profit_bm > 0 else 0xFF4444

    e = discord.Embed(title=f"🔨 Крафт: {display} {tier}.{enchant}", color=color)
    e.set_thumbnail(url=item_icon_url(item_id))
    e.add_field(name="📦 Материал", value=f"`{mat_id}` × {mat_count}\nЛучшая цена: **{mat_sell_min:,}** ({city_ru})", inline=False)
    e.add_field(name="💰 Себестоимость", value=f"**{craft_cost:,}** серебра", inline=True)
    e.add_field(name="🏪 Цена на рынке", value=f"**{item_sell_min:,}**" if item_sell_min else "нет данных", inline=True)
    e.add_field(name="🔴 Цена на ЧР", value=f"**{item_bm:,}**" if item_bm else "нет данных", inline=True)

    if craft_cost > 0:
        e.add_field(
            name="📈 Профит",
            value=(f"Продажа на рынке: **{profit_sell:,}** ({margin_sell}%)\n"
                   f"Продажа на ЧР: **{profit_bm:,}** ({margin_bm}%)"),
            inline=False
        )
        verdict = "✅ Крафт выгоден!" if profit_bm > 0 else "❌ Крафт убыточен — дешевле купить готовое"
        e.add_field(name="Вывод", value=verdict, inline=False)
    else:
        e.add_field(name="⚠️", value="Недостаточно данных для расчёта", inline=False)

    e.set_footer(text=f"albion-online-data.com · {ALBION_SERVER_NAMES.get(server,'EU')}")
    await interaction.followup.send(embed=e)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  💱 ALBION: CITY FLIPPER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="flipper", description="Торговый арбитраж между городами Albion [Pro]")
@app_commands.describe(
    category="weapon / armor / bag",
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
        return await interaction.followup.send("❌ Категория: weapon / armor / bag")

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

        e = discord.Embed(title="⚔️ Топ гильдий по ZvZ активности", color=0xFF6B35,
                          description=f"По данным последних 50 битв · {datetime.datetime.utcnow().strftime('%d.%m.%Y')}")
        medals = ["🥇","🥈","🥉"] + [f"{i}." for i in range(4, limit+1)]
        for i, (gname, stats) in enumerate(top):
            e.add_field(
                name=f"{medals[i]} {gname}",
                value=(f"Битв: **{stats['battles']}** · Убийств: **{stats['kills']}**\n"
                       f"Fame: **{stats['fame']:,}**"),
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
        e.set_footer(text=f"Вопрос: {question[:80]} · NexusBot AI")
        await interaction.followup.send(embed=e)
    except Exception as ex:
        await interaction.followup.send(f"❌ Ошибка: {ex}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  START BACKGROUND TASKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.event
async def on_connect():
    bot.loop.create_task(birthday_check_loop())
    bot.loop.create_task(price_watch_loop())



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


if __name__ == "__main__":
    bot.run(TOKEN)
