"""
История правок сообщений (дашборд → «Правки сообщений»). Включается тумблером
edits_enabled на той же странице, по умолчанию выключено.

Последние HOT_LIMIT правок каждого участника лежат в основной базе (message_edits) —
их панель показывает сразу. Более старые переезжают в отдельный файл-архив
(edits_archive.db рядом с основной базой): основная база и её ежедневный бэкап
не растут, а архив открывается только когда модератор листает дальше.
Правки старше edits_keep_days дней удаляются и там, и там.

Текст «до» берётся из кэша сообщений бота. Если сообщения в кэше нет (старое
или бот перезапускался), «до» — текст из прошлой записанной правки этого
сообщения, а если её нет — неизвестно (before = NULL).
"""

import asyncio
import datetime
import os

import aiosqlite
import discord

from .config import DB_PATH
from .core import bot
from .database import get_protection

HOT_LIMIT = 100                  # правок на участника в основной базе
ARCHIVE_PAGE = 50
ARCHIVE_PATH = os.path.join(os.path.dirname(os.path.abspath(DB_PATH)), "edits_archive.db")

SCHEMA = """
    CREATE TABLE IF NOT EXISTS message_edits (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        guild_id    INTEGER NOT NULL,
        user_id     INTEGER NOT NULL,
        user_name   TEXT    DEFAULT '',
        channel_id  INTEGER NOT NULL,
        message_id  INTEGER NOT NULL,
        before      TEXT,
        after       TEXT    NOT NULL,
        edited_at   TEXT    NOT NULL);
    CREATE INDEX IF NOT EXISTS idx_edits_user ON message_edits(guild_id, user_id, id);
    CREATE INDEX IF NOT EXISTS idx_edits_msg  ON message_edits(message_id);
"""
COLS = "id, guild_id, user_id, user_name, channel_id, message_id, before, after, edited_at"

_ready = False
_lock = asyncio.Lock()


async def init():
    global _ready
    if _ready:
        return
    for path in (DB_PATH, ARCHIVE_PATH):
        async with aiosqlite.connect(path) as db:
            await db.executescript(SCHEMA)
            await db.commit()
    _ready = True


async def _enabled(gid) -> bool:
    return bool(gid) and (await get_protection(gid))["edits_enabled"]


async def _last_text(message_id):
    """Последний записанный текст сообщения — «до» для правки, которой нет в кэше."""
    for path in (DB_PATH, ARCHIVE_PATH):
        async with aiosqlite.connect(path) as db:
            async with db.execute("SELECT after FROM message_edits WHERE message_id=? ORDER BY id DESC LIMIT 1",
                                  (message_id,)) as c:
                row = await c.fetchone()
        if row:
            return row[0]
    return None


async def record(gid, uid, name, channel_id, message_id, before, after):
    await init()
    now = datetime.datetime.utcnow().isoformat()
    async with _lock:                 # перенос в архив не должен идти параллельно
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO message_edits (guild_id,user_id,user_name,channel_id,message_id,before,after,edited_at) "
                "VALUES (?,?,?,?,?,?,?,?)", (gid, uid, name, channel_id, message_id, before, after, now))
            await db.commit()
            async with db.execute(f"SELECT {COLS} FROM message_edits WHERE guild_id=? AND user_id=? "
                                  "ORDER BY id DESC LIMIT -1 OFFSET ?", (gid, uid, HOT_LIMIT)) as c:
                old = await c.fetchall()
            if not old:
                return
            async with aiosqlite.connect(ARCHIVE_PATH) as arch:
                await arch.executemany(f"INSERT OR IGNORE INTO message_edits ({COLS}) VALUES (?,?,?,?,?,?,?,?,?)", old)
                await arch.commit()
            await db.executemany("DELETE FROM message_edits WHERE id=?", [(r[0],) for r in old])
            await db.commit()


@bot.listen("on_message_edit")
async def _edits_cached(before, after):
    if (after.author.bot or not after.guild or before.content == after.content
            or not await _enabled(after.guild.id)):
        return
    try:
        await record(after.guild.id, after.author.id, str(after.author), after.channel.id, after.id,
                     before.content, after.content)
    except Exception as ex:
        print(f"[EDITS] {ex}")


@bot.listen("on_raw_message_edit")
async def _edits_raw(payload: discord.RawMessageUpdateEvent):
    if payload.cached_message is not None:
        return                                   # это обработал _edits_cached
    data = payload.data or {}
    author = data.get("author") or {}
    if (not payload.guild_id or author.get("bot") or "content" not in data
            or not data.get("edited_timestamp") or not await _enabled(payload.guild_id)):
        return
    # Discord присылает обновление и когда подгружает превью ссылки — это не правка.
    # Настоящая правка — свежая метка edited_timestamp.
    try:
        edited = datetime.datetime.fromisoformat(data["edited_timestamp"])
        if (discord.utils.utcnow() - edited).total_seconds() > 120:
            return
    except (TypeError, ValueError):
        return
    after = data["content"]
    try:
        before = await _last_text(payload.message_id)
        if before == after:
            return
        name = author.get("username", "")
        if author.get("global_name"):
            name = f"{author['global_name']} ({name})"
        await record(payload.guild_id, int(author.get("id") or 0), name, payload.channel_id,
                     payload.message_id, before, after)
    except Exception as ex:
        print(f"[EDITS] raw: {ex}")


# ── Чтение для панели ────────────────────────────────────────

def _row(r):
    return {"id": r[0], "user_id": str(r[2]), "user_name": r[3], "channel_id": str(r[4]),
            "message_id": str(r[5]), "before": r[6], "after": r[7], "edited_at": r[8]}


def _text_filter(q):
    if not q:
        return "", ()
    like = f"%{q}%"
    return " AND (IFNULL(before,'') LIKE ? OR after LIKE ?)", (like, like)


async def recent(gid, uid, q=""):
    """Последние правки участника из основной базы (до HOT_LIMIT)."""
    await init()
    flt, args = _text_filter(q)
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(f"SELECT {COLS} FROM message_edits WHERE guild_id=? AND user_id=?{flt} "
                              "ORDER BY id DESC LIMIT ?", (gid, uid, *args, HOT_LIMIT)) as c:
            return [_row(r) for r in await c.fetchall()]


async def archived(gid, uid, q="", before_id=0):
    """Страница архива (новые сначала). Возвращает (правки, есть ли ещё)."""
    await init()
    flt, args = _text_filter(q)
    cond = " AND id < ?" if before_id else ""
    extra = (before_id,) if before_id else ()
    async with aiosqlite.connect(ARCHIVE_PATH) as db:
        async with db.execute(f"SELECT {COLS} FROM message_edits WHERE guild_id=? AND user_id=?{flt}{cond} "
                              "ORDER BY id DESC LIMIT ?", (gid, uid, *args, *extra, ARCHIVE_PAGE + 1)) as c:
            rows = await c.fetchall()
    return [_row(r) for r in rows[:ARCHIVE_PAGE]], len(rows) > ARCHIVE_PAGE


async def counts(gid, uid) -> dict:
    await init()
    out = {}
    for key, path in (("recent", DB_PATH), ("archive", ARCHIVE_PATH)):
        async with aiosqlite.connect(path) as db:
            async with db.execute("SELECT COUNT(*) FROM message_edits WHERE guild_id=? AND user_id=?",
                                  (gid, uid)) as c:
                out[key] = (await c.fetchone())[0]
    return out


async def authors(gid, q, limit=20):
    """Кто правил сообщения, по нику на момент правки — находит и ушедших с сервера."""
    await init()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, user_name, COUNT(*) FROM message_edits WHERE guild_id=? "
                              "AND (user_name LIKE ? OR CAST(user_id AS TEXT)=?) GROUP BY user_id "
                              "ORDER BY MAX(id) DESC LIMIT ?", (gid, f"%{q}%", q, limit)) as c:
            return await c.fetchall()


async def clear(gid, uid=None) -> int:
    """Удаляет историю правок сервера или одного участника из обеих баз."""
    await init()
    n = 0
    cond, args = ("guild_id=? AND user_id=?", (gid, uid)) if uid else ("guild_id=?", (gid,))
    for path in (DB_PATH, ARCHIVE_PATH):
        async with aiosqlite.connect(path) as db:
            cur = await db.execute(f"DELETE FROM message_edits WHERE {cond}", args)
            n += cur.rowcount
            await db.commit()
    return n


async def cleanup():
    """Срок хранения — свой у каждого сервера, для основной базы и архива. Раз в сутки."""
    await init()
    for path in (DB_PATH, ARCHIVE_PATH):
        async with aiosqlite.connect(path) as db:
            async with db.execute("SELECT DISTINCT guild_id FROM message_edits") as c:
                gids = [r[0] for r in await c.fetchall()]
            for gid in gids:
                days = (await get_protection(gid))["edits_keep_days"]
                cutoff = (datetime.datetime.utcnow() - datetime.timedelta(days=days)).isoformat()
                await db.execute("DELETE FROM message_edits WHERE guild_id=? AND edited_at < ?", (gid, cutoff))
            await db.commit()
