"""
База данных SQLite: создание таблиц и функции чтения/записи.
"""

import discord
import aiosqlite
import os, asyncio, random, time, json, datetime
from datetime import timedelta
from .config import DB_PATH

async def db_init():
    # Создаём папку для БД если не существует
    _db_dir = os.path.dirname(DB_PATH)
    if _db_dir:
        os.makedirs(_db_dir, exist_ok=True)
    async with aiosqlite.connect(DB_PATH) as db:
        # WAL mode: параллельные чтения, меньше блокировок, быстрее запись на Volume
        await db.execute("PRAGMA journal_mode=WAL")
        # 8MB page cache — снижает I/O на Railway Volume
        await db.execute("PRAGMA cache_size=-8192")
        # NORMAL sync — безопасно и быстрее чем FULL
        await db.execute("PRAGMA synchronous=NORMAL")
        # Temp таблицы в памяти, не на диске
        await db.execute("PRAGMA temp_store=MEMORY")
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
            CREATE TABLE IF NOT EXISTS threat_settings (
                guild_id INTEGER PRIMARY KEY, settings TEXT DEFAULT '{}');
            CREATE TABLE IF NOT EXISTS protection_settings (
                guild_id INTEGER PRIMARY KEY, settings TEXT DEFAULT '{}');
            CREATE TABLE IF NOT EXISTS warnings (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL, mod_id INTEGER NOT NULL, reason TEXT, created_at TEXT);
            CREATE TABLE IF NOT EXISTS invite_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT, guild_id INTEGER NOT NULL,
                invite_code TEXT, inviter_id INTEGER, inviter_name TEXT,
                member_id INTEGER, member_name TEXT, joined_at TEXT,
                note TEXT DEFAULT '');
            CREATE TABLE IF NOT EXISTS tier_warnings (
                guild_id   INTEGER NOT NULL,
                days_left  INTEGER NOT NULL,
                sent_at    TEXT    NOT NULL,
                PRIMARY KEY (guild_id, days_left)
            );

            CREATE TABLE IF NOT EXISTS active_mutes (
                guild_id   INTEGER NOT NULL,
                user_id    INTEGER NOT NULL,
                until      TEXT    NOT NULL,
                reason     TEXT    DEFAULT '',
                notified   INTEGER DEFAULT 0,
                created_at TEXT    NOT NULL,
                PRIMARY KEY (guild_id, user_id)
            );
            CREATE INDEX IF NOT EXISTS idx_active_mutes
                ON active_mutes(notified, until);

            CREATE TABLE IF NOT EXISTS ban_requests (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                username    TEXT DEFAULT '',
                mod_id      INTEGER NOT NULL,
                reason      TEXT DEFAULT '',
                warn_count  INTEGER DEFAULT 3,
                status      TEXT DEFAULT 'pending',  -- pending / approved / rejected
                reviewer_id INTEGER DEFAULT 0,
                created_at  TEXT NOT NULL,
                reviewed_at TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_ban_requests
                ON ban_requests(guild_id, status);

            CREATE TABLE IF NOT EXISTS invite_autoclean_settings (
                guild_id    INTEGER PRIMARY KEY,
                enabled     INTEGER DEFAULT 0,
                max_age_days INTEGER DEFAULT 7,
                log_channel INTEGER DEFAULT 0);
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
                guild_id     INTEGER PRIMARY KEY,
                mute1_days   INTEGER DEFAULT 7,   -- 1й варн: дней таймаута
                mute2_days   INTEGER DEFAULT 7,   -- 2й варн: дней таймаута
                ban3_days    INTEGER DEFAULT 30,  -- 3й варн: дней бана
                updated_at   TEXT DEFAULT '');

            -- Invite лидерборд
            CREATE TABLE IF NOT EXISTS invite_stats (
                guild_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                total_invites INTEGER DEFAULT 0,
                active_invites INTEGER DEFAULT 0,
                left_count  INTEGER DEFAULT 0,
                PRIMARY KEY (guild_id, user_id));

            -- Напоминания (persistent, переживают перезапуск)
            CREATE TABLE IF NOT EXISTS reminders (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                channel_id  INTEGER NOT NULL,
                message     TEXT NOT NULL,
                fire_at     TEXT NOT NULL,
                repeat_mins INTEGER DEFAULT 0,
                repeat_count INTEGER DEFAULT 0,
                max_repeats INTEGER DEFAULT 20,
                done        INTEGER DEFAULT 0,
                created_at  TEXT NOT NULL);
            CREATE INDEX IF NOT EXISTS idx_reminders
                ON reminders(fire_at, done);

            CREATE TABLE IF NOT EXISTS appeals (
                id           INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id     INTEGER NOT NULL,
                user_id      INTEGER NOT NULL,
                username     TEXT DEFAULT '',
                action_type  TEXT NOT NULL,         -- BAN / KICK / MUTE / WARN / TEMPBAN
                action_ref   INTEGER DEFAULT 0,     -- ID связанной записи в modlog
                original_reason TEXT DEFAULT '',
                appeal_text  TEXT NOT NULL,
                why_change   TEXT DEFAULT '',
                status       TEXT DEFAULT 'pending', -- pending / accepted / rejected / expired
                reviewer_id  INTEGER DEFAULT 0,
                reviewer_note TEXT DEFAULT '',
                submitted_at TEXT NOT NULL,
                reviewed_at  TEXT DEFAULT ''
            );
            CREATE INDEX IF NOT EXISTS idx_appeals_guild_status
                ON appeals(guild_id, status);
            CREATE INDEX IF NOT EXISTS idx_appeals_user
                ON appeals(guild_id, user_id);


            -- #21/22 Связь участник → пригласивший
            CREATE TABLE IF NOT EXISTS member_inviter (
                guild_id      INTEGER NOT NULL,
                user_id       INTEGER NOT NULL,
                inviter_id    INTEGER DEFAULT 0,
                invite_code   TEXT DEFAULT '',
                joined_at     TEXT DEFAULT '',
                PRIMARY KEY (guild_id, user_id));

            -- Лог таймаутов для лидерборда
            CREATE TABLE IF NOT EXISTS mute_log (
                guild_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                total_seconds INTEGER DEFAULT 0,
                mute_count  INTEGER DEFAULT 0,
                last_muted  TEXT DEFAULT '',
                PRIMARY KEY (guild_id, user_id));
        """)
        await db.commit()

        # Миграции — добавляем новые колонки если их нет (для существующих БД)
        migrations = [
            "ALTER TABLE guild_settings ADD COLUMN tickets_enabled INTEGER DEFAULT 1",
            "ALTER TABLE guild_settings ADD COLUMN quarantine_role INTEGER DEFAULT 0",
            "ALTER TABLE guild_settings ADD COLUMN mod_channel INTEGER DEFAULT 0",
            "ALTER TABLE invite_log ADD COLUMN note TEXT DEFAULT ''",
            "ALTER TABLE guild_settings ADD COLUMN setup_done INTEGER DEFAULT 0",
            # Новая 3-варн система наказаний (вместо старой warn2-5)
            "ALTER TABLE punishment_settings ADD COLUMN mute1_days INTEGER DEFAULT 7",
            "ALTER TABLE punishment_settings ADD COLUMN mute2_days INTEGER DEFAULT 7",
            "ALTER TABLE punishment_settings ADD COLUMN ban3_days INTEGER DEFAULT 30",
            "ALTER TABLE punishment_settings ADD COLUMN updated_at TEXT DEFAULT ''",
            "ALTER TABLE punishment_settings ADD COLUMN warn3_type TEXT DEFAULT 'ban'",
            # Quarantine on/off тоггл (отдельно от роли — можно временно выключить)
            "ALTER TABLE guild_settings ADD COLUMN quarantine_enabled INTEGER DEFAULT 1",
            # Колонки, появившиеся в CREATE TABLE позже создания старых баз.
            # CREATE TABLE IF NOT EXISTS существующую таблицу не меняет, поэтому
            # без этих ALTER на старой базе будет "no such column".
            "ALTER TABLE guild_settings ADD COLUMN lang TEXT DEFAULT 'ru'",
            "ALTER TABLE guild_settings ADD COLUMN starboard_channel INTEGER DEFAULT 0",
            "ALTER TABLE guild_settings ADD COLUMN starboard_threshold INTEGER DEFAULT 3",
            "ALTER TABLE guild_settings ADD COLUMN suggestion_channel INTEGER DEFAULT 0",
            "ALTER TABLE guild_settings ADD COLUMN ticket_category INTEGER DEFAULT 0",
            "ALTER TABLE guild_settings ADD COLUMN birthday_channel INTEGER DEFAULT 0",
            "ALTER TABLE guild_settings ADD COLUMN lockdown INTEGER DEFAULT 0",
            "ALTER TABLE guild_settings ADD COLUMN price_watch INTEGER DEFAULT 0",
            # Индексы на таблицах с частыми выборками. Без них SQLite
            # сканирует таблицу целиком, и время растёт вместе с историей.
            "CREATE INDEX IF NOT EXISTS idx_invite_member ON invite_log(guild_id, member_id)",
            "CREATE INDEX IF NOT EXISTS idx_invite_code   ON invite_log(guild_id, invite_code)",
            "CREATE INDEX IF NOT EXISTS idx_warnings      ON warnings(guild_id, user_id)",
            "CREATE INDEX IF NOT EXISTS idx_tempbans      ON temp_bans(unbanned, unban_at)",
            "CREATE INDEX IF NOT EXISTS idx_modlog_date   ON modlog(guild_id, created_at)",
            # Самые крупные таблицы — без индексов очистка сканирует их целиком
            "CREATE INDEX IF NOT EXISTS idx_hashes_date   ON content_hashes(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_alerts_guild  ON security_alerts(guild_id, created_at)",
            "CREATE INDEX IF NOT EXISTS idx_alerts_date   ON security_alerts(resolved, created_at)",
            # Стилометрия, fingerprint-профили и граф связей удалены из бота.
            # Удаляем накопленные ими данные, чтобы бот их больше не хранил.
            "DROP TABLE IF EXISTS style_profiles",
            "DROP TABLE IF EXISTS twin_links",
            "DROP TABLE IF EXISTS interaction_graph",
            "DROP TABLE IF EXISTS banned_profiles",
            "DROP TABLE IF EXISTS style_watchlist",
            "DROP TABLE IF EXISTS fingerprints",
            "DROP TABLE IF EXISTS social_graph",
        ]
        for sql in migrations:
            try:
                await db.execute(sql)
                await db.commit()
            except Exception:
                pass

async def set_tier(gid, tier, days=30):
    """days=0 — бессрочно (expires_at пустой)"""
    exp = "" if not days else (datetime.datetime.utcnow() + timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO subscriptions (guild_id,tier,expires_at) VALUES(?,?,?) ON CONFLICT(guild_id) DO UPDATE SET tier=excluded.tier,expires_at=excluded.expires_at", (gid,tier,exp))
        # Подписку продлили — прошлые предупреждения больше не актуальны
        await db.execute("DELETE FROM tier_warnings WHERE guild_id=?", (gid,))
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

# Режимы правил защиты: off — выключено, alert — только алерт в лог-канал,
# block — блокировать сообщение и прислать алерт. Применяются в automod.py
# (правила AutoMod) и в security_module.py (проверка текста самим ботом).
THREAT_MODES   = ("off", "alert", "block")
# custom — свой список слов и доменов сервера (правило «свои слова»)
DEFAULT_THREATS = {"phishing": "block", "suspicious": "alert", "spam": "alert", "custom": "off"}
# Исключения и список слов хранятся рядом с режимами в том же JSON.
# Лимиты — как у Discord AutoMod: 20 ролей, 50 каналов, 1000 слов до 60 символов.
THREAT_LISTS = {"exempt_roles": 20, "exempt_channels": 50, "custom_words": 1000}
_threat_cache: dict = {}

async def get_threat_config(gid):
    """Режимы + исключения + свои слова: {"phishing": "block", …, "exempt_roles": [ids], …}."""
    if gid not in _threat_cache:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT settings FROM threat_settings WHERE guild_id=?", (gid,)) as c:
                row = await c.fetchone()
        saved = json.loads(row[0] or "{}") if row else {}
        cfg = {k: saved[k] if saved.get(k) in THREAT_MODES else v for k, v in DEFAULT_THREATS.items()}
        for k in THREAT_LISTS:
            cfg[k] = list(saved.get(k) or [])
        _threat_cache[gid] = cfg
    cfg = _threat_cache[gid]
    return {k: (list(v) if isinstance(v, list) else v) for k, v in cfg.items()}

async def get_threats(gid):
    """Только режимы правил."""
    cfg = await get_threat_config(gid)
    return {k: cfg[k] for k in DEFAULT_THREATS}

async def save_threats(gid, data):
    """Сохраняет режимы и списки. Неизвестные ключи и значения отбрасываются,
    списки обрезаются до лимитов. Проверку слов на безобидные ссылки и
    принадлежность ролей/каналов серверу делает вызывающий код."""
    clean = await get_threat_config(gid)
    clean.update({k: v for k, v in data.items() if k in DEFAULT_THREATS and v in THREAT_MODES})
    for k, limit in THREAT_LISTS.items():
        if isinstance(data.get(k), list):
            items = [str(x) for x in data[k] if isinstance(x, (str, int)) and str(x).strip()]
            clean[k] = list(dict.fromkeys(items))[:limit]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO threat_settings (guild_id,settings) VALUES(?,?) "
                         "ON CONFLICT(guild_id) DO UPDATE SET settings=excluded.settings",
                         (gid, json.dumps(clean, ensure_ascii=False)))
        await db.commit()
    _threat_cache[gid] = clean
    return await get_threat_config(gid)

# ── Анти-нюк, анти-рейд, сгорание варнов ──────────────────────
# Всё настраивается из дашборда (вкладка «Безопасность» и «Модерация»).
# limits: действие → [сколько, за сколько секунд]; сколько = 0 — лимит выключен. Действия:
#   ban, kick, mute — наказания; channel, role — удаления; webhook — создание вебхуков.
# antinuke_action: alert — только алерт; strip — снять опасные роли;
#   strip_timeout — снять роли и выдать таймаут на час.
# bot_add: off | alert | kick (выгнать бота, добавленного не владельцем и не доверенной ролью).
# escalation (кто-то выдал роли «Администратор» и т.п.): off | alert | revert.
# raid_action: alert | kick | quarantine — что делать со всеми, кто зашёл в волне рейда.
# warn_expiry_days: через сколько дней варн перестаёт считаться (0 — никогда).
PROTECTION_DEFAULTS = {
    "antinuke_enabled": True,
    "antinuke_action": "strip",
    "limits": {"ban": [5, 30], "kick": [5, 30], "mute": [8, 30],
               "channel": [3, 20], "role": [3, 20], "webhook": [3, 60]},
    "trusted_roles": [],
    "bot_add": "alert",
    "escalation": "alert",
    "raid_joins": 8,
    "raid_window": 10,
    "raid_action": "kick",
    "raid_lockdown_minutes": 10,
    "warn_expiry_days": 0,
}
PROTECTION_CHOICES = {
    "antinuke_action": ("alert", "strip", "strip_timeout"),
    "bot_add": ("off", "alert", "kick"),
    "escalation": ("off", "alert", "revert"),
    "raid_action": ("alert", "kick", "quarantine"),
}
# (минимум, максимум) для чисел; лимиты анти-нюка — те же для всех действий
PROTECTION_RANGES = {
    "raid_joins": (3, 100), "raid_window": (3, 120), "raid_lockdown_minutes": (0, 1440),
    "warn_expiry_days": (0, 3650), "limit_count": (2, 100), "limit_window": (5, 600),
}
_protection_cache: dict = {}


def _clamp(v, lo, hi, default):
    try:
        return max(lo, min(hi, int(v)))
    except (TypeError, ValueError):
        return default


def _clean_protection(data, base):
    """Сливает data поверх base, отбрасывая неизвестное и обрезая числа по диапазонам."""
    out = json.loads(json.dumps(base))
    if not isinstance(data, dict):
        return out
    if "antinuke_enabled" in data:
        out["antinuke_enabled"] = bool(data["antinuke_enabled"])
    for k, choices in PROTECTION_CHOICES.items():
        if data.get(k) in choices:
            out[k] = data[k]
    for k in ("raid_joins", "raid_window", "raid_lockdown_minutes", "warn_expiry_days"):
        if k in data:
            out[k] = _clamp(data[k], *PROTECTION_RANGES[k], out[k])
    if isinstance(data.get("limits"), dict):
        for act, cur in out["limits"].items():
            v = data["limits"].get(act)
            if isinstance(v, (list, tuple)) and len(v) == 2:
                count = 0 if v[0] in (0, "0") else _clamp(v[0], *PROTECTION_RANGES["limit_count"], cur[0])
                out["limits"][act] = [count,
                                      _clamp(v[1], *PROTECTION_RANGES["limit_window"], cur[1])]
    if isinstance(data.get("trusted_roles"), list):
        ids = [str(r) for r in data["trusted_roles"] if str(r).isdigit()]
        out["trusted_roles"] = list(dict.fromkeys(ids))[:25]
    return out


async def get_protection(gid):
    if gid not in _protection_cache:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT settings FROM protection_settings WHERE guild_id=?", (gid,)) as c:
                row = await c.fetchone()
        saved = json.loads(row[0] or "{}") if row else {}
        cfg = _clean_protection(saved, PROTECTION_DEFAULTS)
        # Время конца авто-локдауна — служебное поле, в дашборде не редактируется
        cfg["lockdown_until"] = saved.get("lockdown_until") or ""
        _protection_cache[gid] = cfg
    return json.loads(json.dumps(_protection_cache[gid]))


async def save_protection(gid, data):
    cur = await get_protection(gid)
    clean = _clean_protection(data, cur)
    clean["lockdown_until"] = data.get("lockdown_until", cur.get("lockdown_until", "")) \
        if isinstance(data, dict) else cur.get("lockdown_until", "")
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO protection_settings (guild_id,settings) VALUES(?,?) "
                         "ON CONFLICT(guild_id) DO UPDATE SET settings=excluded.settings",
                         (gid, json.dumps(clean)))
        await db.commit()
    _protection_cache[gid] = clean
    return await get_protection(gid)


async def get_active_warnings(gid, uid):
    """Варны, которые ещё считаются (с учётом warn_expiry_days)."""
    warns = await get_warnings(gid, uid)
    days = (await get_protection(gid))["warn_expiry_days"]
    if not days:
        return warns
    cutoff = (datetime.datetime.utcnow() - timedelta(days=days)).isoformat()
    return [w for w in warns if (w[3] or "") >= cutoff]


async def is_enabled(gid, key):
    _, s = await get_security(gid); return s.get(key, False)

async def get_log_ch(guild):
    ch_id, _ = await get_security(guild.id)
    if ch_id: return guild.get_channel(ch_id)
    return (discord.utils.get(guild.text_channels, name="logs") or
            discord.utils.get(guild.text_channels, name="bot-logs") or
            discord.utils.get(guild.text_channels, name="mod-logs"))

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


async def get_punishment_settings(gid: int) -> dict:
    """
    Возвращает настройки прогрессивных наказаний для сервера.
    Используется и ботом (/warn, /punishments) и сайтом (dashboard) —
    единый источник правды, изменения в одном месте видны в другом.
    """
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT mute1_days, mute2_days, ban3_days, COALESCE(warn3_type,'ban') "
            "FROM punishment_settings WHERE guild_id=?",
            (gid,)
        ) as c:
            row = await c.fetchone()
    if row:
        return {"mute1_days": row[0] or 7, "mute2_days": row[1] or 7,
                "ban3_days": row[2] or 30, "warn3_type": row[3] or "ban"}
    return {"mute1_days": 7, "mute2_days": 7, "ban3_days": 30, "warn3_type": "ban"}


async def set_punishment_settings(gid: int, mute1_days: int = None,
                                   mute2_days: int = None, ban3_days: int = None,
                                   warn3_type: str = None):
    """Обновляет настройки наказаний. None значения не трогаются."""
    current = await get_punishment_settings(gid)
    m1 = mute1_days  if mute1_days  is not None else current["mute1_days"]
    m2 = mute2_days  if mute2_days  is not None else current["mute2_days"]
    b3 = ban3_days   if ban3_days   is not None else current["ban3_days"]
    w3 = warn3_type  if warn3_type  is not None else current["warn3_type"]
    now = datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO punishment_settings
                (guild_id, mute1_days, mute2_days, ban3_days, warn3_type, updated_at)
            VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET
                mute1_days=excluded.mute1_days, mute2_days=excluded.mute2_days,
                ban3_days=excluded.ban3_days,   warn3_type=excluded.warn3_type,
                updated_at=excluded.updated_at
        """, (gid, m1, m2, b3, w3, now))
        await db.commit()
    return {"mute1_days": m1, "mute2_days": m2, "ban3_days": b3, "warn3_type": w3}


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
