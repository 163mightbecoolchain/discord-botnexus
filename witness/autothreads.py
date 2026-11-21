"""
Автоветки: в выбранных каналах бот открывает ветку на каждое сообщение участника.
Настраивается на сайте: «Автоматизация → Автоветки» (web_server.api_automation_*).
"""

import datetime
import discord
import aiosqlite
from .config import DB_PATH

DEFAULT_NAME = "{author}: {text}"
ARCHIVE_CHOICES = (60, 1440, 4320, 10080)   # варианты Discord: час, сутки, 3 дня, неделя
MAX_CHANNELS = 25
_cache: dict = {}                           # gid → {channel_id: {"name", "archive"}}
_warned: set = set()                        # (gid, channel_id) — про нехватку прав уже писали в консоль


async def get_auto_threads(gid: int) -> dict:
    if gid not in _cache:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT channel_id, name, archive_minutes FROM auto_threads WHERE guild_id=?",
                                  (gid,)) as c:
                _cache[gid] = {ch: {"name": name or "", "archive": arch or 1440}
                               for ch, name, arch in await c.fetchall()}
    return _cache[gid]


async def set_auto_threads(gid: int, rows: list):
    """Заменяет весь список. rows: [{"channel_id", "name", "archive"}]."""
    clean = {}
    for r in rows[:MAX_CHANNELS]:
        try:
            ch = int(r.get("channel_id") or 0)
        except (TypeError, ValueError):
            continue
        try:
            arch = int(r.get("archive") or 1440)
        except (TypeError, ValueError):
            arch = 1440
        if not ch:
            continue
        clean[ch] = {"name": str(r.get("name") or "").strip()[:100],
                     "archive": arch if arch in ARCHIVE_CHOICES else 1440}
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM auto_threads WHERE guild_id=?", (gid,))
        await db.executemany("INSERT INTO auto_threads (guild_id, channel_id, name, archive_minutes) VALUES (?,?,?,?)",
                             [(gid, ch, v["name"], v["archive"]) for ch, v in clean.items()])
        await db.commit()
    _cache[gid] = clean


def thread_name(template: str, message) -> str:
    """{author} — ник, {text} — начало сообщения, {date} — дата. Discord ограничивает 100 символами."""
    text = " ".join((message.clean_content or "").split())
    if not text and message.attachments:
        text = message.attachments[0].filename
    if len(text) > 60:
        text = text[:59].rstrip() + "…"
    name = (template or DEFAULT_NAME)
    name = (name.replace("{author}", message.author.display_name)
                .replace("{text}", text or "обсуждение")
                .replace("{date}", (message.created_at or datetime.datetime.utcnow()).strftime("%d.%m.%Y")))
    name = " ".join(name.split()).strip(" :—-") or "Обсуждение"
    return name[:100]


async def on_message(message):
    """Вызывается из events.on_message (боты и ЛС туда не доходят)."""
    if message.type not in (discord.MessageType.default, discord.MessageType.reply):
        return
    if not isinstance(message.channel, discord.TextChannel):   # в ветках/форумах/голосовых не нужно
        return
    cfg = (await get_auto_threads(message.guild.id)).get(message.channel.id)
    if not cfg:
        return
    try:
        await message.create_thread(name=thread_name(cfg["name"], message),
                                    auto_archive_duration=cfg["archive"])
    except discord.Forbidden:
        key = (message.guild.id, message.channel.id)
        if key not in _warned:
            _warned.add(key)
            print(f"[AUTOTHREAD] Нет права «Создавать публичные ветки» в #{message.channel} ({message.guild})")
    except discord.HTTPException as ex:     # ветка уже есть, сообщение удалили и т.п.
        print(f"[AUTOTHREAD] {ex}")
