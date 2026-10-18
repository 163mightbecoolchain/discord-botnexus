"""
История правок сообщений (дашборд → «Правки сообщений»). Включается тумблером
edits_enabled на той же странице, по умолчанию выключено.

Последние HOT_LIMIT правок каждого участника лежат в основной базе (message_edits) —
их панель показывает сразу. Более старые переезжают в отдельный файл-архив
(edits_archive.db рядом с основной базой): основная база и её ежедневный бэкап
не растут, а архив открывается только когда модератор листает дальше.
Правки старше edits_keep_days дней удаляются и там, и там. Порядок везде —
по времени правки (edited_at), а не по порядку записи: импорт из лога
добавляет старые правки позже новых.

Текст «до» берётся из кэша сообщений бота. Если сообщения в кэше нет (старое
или бот перезапускался), «до» — текст из прошлой записанной правки этого
сообщения, а если её нет — неизвестно (before = NULL).

Импорт из лог-канала (import_from_log): бот один раз читает свои логи
редактирования за IMPORT_DAYS дней и складывает правки сюда же с source='log'.
В логе бывает только часть текста (дифф изменённых строк, обрезка) — такие
записи помечены partial=1.
"""

import asyncio
import datetime
import os
import re

import aiohttp
import aiosqlite
import discord

from .config import DB_PATH
from .core import bot
from .database import get_log_ch, get_protection

HOT_LIMIT = 100                  # правок на участника в основной базе
ARCHIVE_PAGE = 50
IMPORT_DAYS = 90
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
        edited_at   TEXT    NOT NULL,
        source      TEXT    DEFAULT 'live',
        partial     INTEGER DEFAULT 0);
    CREATE INDEX IF NOT EXISTS idx_edits_user ON message_edits(guild_id, user_id, edited_at);
    CREATE INDEX IF NOT EXISTS idx_edits_msg  ON message_edits(message_id);
"""
COLS = ("id, guild_id, user_id, user_name, channel_id, message_id, before, after, edited_at, "
        "source, partial")
NEWEST = "ORDER BY edited_at DESC, id DESC"

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


def display_name(user) -> str:
    """«Глобальное имя (логин)» — чтобы искалось и по тому, и по другому."""
    gname = getattr(user, "global_name", None)
    return f"{gname} ({user.name})" if gname and gname != user.name else user.name


async def _last_text(message_id):
    """Последний записанный текст сообщения — «до» для правки, которой нет в кэше."""
    for path in (DB_PATH, ARCHIVE_PATH):
        async with aiosqlite.connect(path) as db:
            async with db.execute(f"SELECT after FROM message_edits WHERE message_id=? AND partial=0 {NEWEST} LIMIT 1",
                                  (message_id,)) as c:
                row = await c.fetchone()
        if row:
            return row[0]
    return None


async def _rebalance(db, gid, uid):
    """Оставляет в основной базе HOT_LIMIT новейших правок участника, остальное — в архив."""
    async with db.execute(f"SELECT {COLS} FROM message_edits WHERE guild_id=? AND user_id=? "
                          f"{NEWEST} LIMIT -1 OFFSET ?", (gid, uid, HOT_LIMIT)) as c:
        old = await c.fetchall()
    if not old:
        return
    async with aiosqlite.connect(ARCHIVE_PATH) as arch:
        await arch.executemany(f"INSERT OR IGNORE INTO message_edits ({COLS}) VALUES (?,?,?,?,?,?,?,?,?,?,?)", old)
        await arch.commit()
    await db.executemany("DELETE FROM message_edits WHERE id=?", [(r[0],) for r in old])
    await db.commit()


async def record(gid, uid, name, channel_id, message_id, before, after):
    await init()
    now = datetime.datetime.utcnow().isoformat()
    async with _lock:                 # перенос в архив не должен идти параллельно
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO message_edits (guild_id,user_id,user_name,channel_id,message_id,before,after,edited_at) "
                "VALUES (?,?,?,?,?,?,?,?)", (gid, uid, name, channel_id, message_id, before, after, now))
            await db.commit()
            await _rebalance(db, gid, uid)


@bot.listen("on_message_edit")
async def _edits_cached(before, after):
    if (after.author.bot or not after.guild or before.content == after.content
            or not await _enabled(after.guild.id)):
        return
    try:
        await record(after.guild.id, after.author.id, display_name(after.author), after.channel.id, after.id,
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
        if author.get("global_name") and author["global_name"] != name:
            name = f"{author['global_name']} ({name})"
        await record(payload.guild_id, int(author.get("id") or 0), name, payload.channel_id,
                     payload.message_id, before, after)
    except Exception as ex:
        print(f"[EDITS] raw: {ex}")


# ── Импорт из лог-канала ─────────────────────────────────────
# Форматы лога редактирования, которые бот писал за последние месяцы:
#  1) до 13.10.2026 — поля «участник / было / стало / Link», текст до 512 символов;
#  2) с 13.10.2026 — описание «@автор в #канал · [перейти](ссылка) · `±N симв.`» и
#     дифф изменённых строк ```diff (- было, + стало); недолго к длинным правкам
#     прикладывался edit.txt с полным текстом.

EDIT_TITLES = {"Редактирование", "Message edited"}
MSG_URL = re.compile(r"https://(?:\w+\.)?discord(?:app)?\.com/channels/(\d+)/(\d+)/(\d+)")
MENTION = re.compile(r"<@!?(\d+)>")
DIFF_BLOCK = re.compile(r"```diff\n(.*?)\n```", re.S)
FIELD_LIMIT = 512

_import_state: dict = {}          # gid → {"running", "scanned", "found", "imported", "error", "finished_at"}


def import_status(gid) -> dict:
    return dict(_import_state.get(gid) or {"running": False})


def _parse_fields(emb):
    """Старый формат: поля участник / было / стало / ссылка."""
    if len(emb.fields) < 3:
        return None
    m = MENTION.search(emb.fields[0].value or "")
    url = MSG_URL.search(" ".join(f.value or "" for f in emb.fields))
    if not m or not url:
        return None
    unfill = lambda v: "" if v == "—" else (v or "")
    before, after = unfill(emb.fields[1].value), unfill(emb.fields[2].value)
    partial = len(before) >= FIELD_LIMIT or len(after) >= FIELD_LIMIT
    return int(m.group(1)), url, before, after, partial


def _parse_diff(emb):
    """Новый формат: описание с упоминанием, ссылкой и диффом изменённых строк."""
    text = emb.description or ""
    m, url, block = MENTION.search(text), MSG_URL.search(text), DIFF_BLOCK.search(text)
    if not m or not url or not block:
        return None                   # «изменены только пробелы» — текста нет
    # В диффе только изменённые строки: неизменённые строки сообщения в лог не попадали,
    # поэтому полным текст из такого лога не считаем (кроме edit.txt — см. import_from_log)
    olds, news = [], []
    for line in block.group(1).split("\n"):
        line = line.replace("ʼʼʼ", "```")
        if line.startswith("- "):
            olds.append(line[2:])
        elif line.startswith("+ "):
            news.append(line[2:])
        elif line.strip() == "⋯":     # разрыв между изменёнными кусками
            olds.append("…")
            news.append("…")
    return int(m.group(1)), url, "\n".join(olds), "\n".join(news), True


async def _full_text(msg):
    """Полный текст из edit.txt, если он один на сообщение лога."""
    files = [a for a in msg.attachments if a.filename == "edit.txt"]
    if len(files) != 1 or files[0].size > 64_000:
        return None
    try:
        raw = (await files[0].read()).decode("utf-8", "replace")
    except (discord.HTTPException, aiohttp.ClientError):
        return None
    m = re.search(r"===== БЫЛО =====\n(.*)\n\n===== СТАЛО =====\n(.*)\n?$", raw, re.S)
    return (m.group(1), m.group(2).rstrip("\n")) if m else None


async def _names(guild, uids) -> dict:
    out = {}
    for uid in uids:
        u = guild.get_member(uid) or bot.get_user(uid)
        if u is None:
            try:
                u = await bot.fetch_user(uid)
            except discord.HTTPException:
                u = None
        out[uid] = display_name(u) if u else str(uid)
    return out


async def import_from_log(guild):
    """Читает лог-канал за IMPORT_DAYS дней и добавляет найденные правки. Запускается из панели."""
    gid = guild.id
    st = _import_state[gid] = {"running": True, "scanned": 0, "found": 0, "imported": 0,
                               "error": "", "finished_at": "", "days": IMPORT_DAYS}
    try:
        await init()
        ch = await get_log_ch(guild)
        if not ch:
            raise RuntimeError("no_log_channel")
        perms = ch.permissions_for(guild.me)
        if not (perms.read_message_history and perms.view_channel):
            raise RuntimeError("no_history_access")
        keep = (await get_protection(gid))["edits_keep_days"]
        since = discord.utils.utcnow() - datetime.timedelta(days=min(IMPORT_DAYS, keep))

        # Что уже записано (живьём или прошлым импортом): message_id → [времена правок]
        known: dict = {}
        for path in (DB_PATH, ARCHIVE_PATH):
            async with aiosqlite.connect(path) as db:
                async with db.execute("SELECT message_id, edited_at FROM message_edits WHERE guild_id=?",
                                      (gid,)) as c:
                    for mid, ts in await c.fetchall():
                        known.setdefault(mid, []).append(datetime.datetime.fromisoformat(ts))

        rows = []
        async for msg in ch.history(limit=None, after=since, oldest_first=False):
            st["scanned"] += 1
            if msg.author.id != bot.user.id or not msg.embeds:
                continue
            edits = [e for e in msg.embeds if e.author and e.author.name in EDIT_TITLES]
            for emb in edits:
                parsed = _parse_fields(emb) if emb.fields else _parse_diff(emb)
                if not parsed:
                    continue
                uid, url, before, after, partial = parsed
                if partial and len(edits) == 1:
                    full = await _full_text(msg)
                    if full:
                        before, after, partial = full[0], full[1], False
                g, c_id, m_id = (int(x) for x in url.groups())
                if g != gid:
                    continue
                at = msg.created_at.replace(tzinfo=None)
                if any(abs((at - t).total_seconds()) < 180 for t in known.get(m_id, [])):
                    continue              # эта правка уже есть
                known.setdefault(m_id, []).append(at)
                rows.append((gid, uid, c_id, m_id, before, after, at.isoformat(), int(partial)))
                st["found"] += 1

        names = await _names(guild, {r[1] for r in rows})
        async with _lock:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany(
                    "INSERT INTO message_edits (guild_id,user_id,user_name,channel_id,message_id,before,after,"
                    "edited_at,source,partial) VALUES (?,?,?,?,?,?,?,?,'log',?)",
                    [(g, u, names.get(u, str(u)), c, m, b, a, t, p) for g, u, c, m, b, a, t, p in rows])
                await db.commit()
                for uid in {r[1] for r in rows}:
                    await _rebalance(db, gid, uid)
        st["imported"] = len(rows)
    except RuntimeError as ex:
        st["error"] = str(ex)
    except discord.HTTPException as ex:
        st["error"] = f"discord: {ex.status}"
    except Exception as ex:
        print(f"[EDITS] import {gid}: {ex}")
        st["error"] = "failed"
    finally:
        st["running"] = False
        st["finished_at"] = datetime.datetime.utcnow().isoformat()


def start_import(guild) -> bool:
    """Запускает импорт в фоне. False — уже идёт."""
    if import_status(guild.id).get("running"):
        return False
    _import_state[guild.id] = {"running": True, "scanned": 0, "found": 0, "imported": 0, "error": "",
                               "finished_at": "", "days": IMPORT_DAYS}
    asyncio.create_task(import_from_log(guild))
    return True


# ── Чтение для панели ────────────────────────────────────────

def _row(r):
    return {"id": r[0], "user_id": str(r[2]), "user_name": r[3], "channel_id": str(r[4]),
            "message_id": str(r[5]), "before": r[6], "after": r[7], "edited_at": r[8],
            "source": r[9] or "live", "partial": bool(r[10])}


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
                              f"{NEWEST} LIMIT ?", (gid, uid, *args, HOT_LIMIT)) as c:
            return [_row(r) for r in await c.fetchall()]


async def archived(gid, uid, q="", before_at="", before_id=0):
    """Страница архива (новые сначала), продолжение — после правки (before_at, before_id).
    Возвращает (правки, есть ли ещё)."""
    await init()
    flt, args = _text_filter(q)
    cond, extra = "", ()
    if before_at:
        cond, extra = " AND (edited_at < ? OR (edited_at = ? AND id < ?))", (before_at, before_at, before_id)
    async with aiosqlite.connect(ARCHIVE_PATH) as db:
        async with db.execute(f"SELECT {COLS} FROM message_edits WHERE guild_id=? AND user_id=?{flt}{cond} "
                              f"{NEWEST} LIMIT ?", (gid, uid, *args, *extra, ARCHIVE_PAGE + 1)) as c:
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
                              "ORDER BY MAX(edited_at) DESC LIMIT ?", (gid, f"%{q}%", q, limit)) as c:
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
