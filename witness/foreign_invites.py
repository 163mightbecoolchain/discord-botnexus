"""
Инвайты на чужие серверы в сообщениях.

AutoMod так не умеет: он ищет текст, а не смотрит, куда ведёт ссылка. Поэтому
проверяет сам бот (нужен Message Content Intent):
  1) достаёт из сообщения коды discord.gg/…, discord.com/invite/…;
  2) код из кэша инвайтов сервера или его vanity — свой, дальше не смотрим;
  3) остальное спрашивает у Discord (fetch_invite) и запоминает ответ на час.
Сообщение не удаляется: режим alert — алерт в лог-канал, ping — то же с пингом
ролей модераторов. Мёртвые ссылки и серверы-партнёры из списка пропускаются.
"""

import re
import time
import discord

from .core import bot, _invite_cache
from .database import get_log_ch
from .ui import C

INVITE_RE = re.compile(
    r"(?i)(?:https?://)?(?:www\.)?(?:discord\.gg|(?:discord|discordapp)\.com/invite)/([a-z0-9-]{2,32})")
MAX_CODES = 5                 # больше кодов в одном сообщении не проверяем
RESOLVE_TTL = 60 * 60
ALERT_COOLDOWN = 10 * 60      # один участник + один чужой сервер — не чаще раза в 10 минут

# код → (время, id сервера или 0 для мёртвой ссылки, название сервера)
_resolved: dict = {}
_last_alert: dict = {}


def invite_codes(text: str) -> list:
    return list(dict.fromkeys(m.group(1) for m in INVITE_RE.finditer(text or "")))[:MAX_CODES]


async def resolve_invite(code: str):
    """(id сервера, название) по коду. (0, "") — ссылка мёртвая или ведёт не на сервер,
    None — Discord не ответил (повторим в следующий раз)."""
    hit = _resolved.get(code)
    if hit and time.time() - hit[0] < RESOLVE_TTL:
        return hit[1], hit[2]
    try:
        inv = await bot.fetch_invite(code, with_counts=False, with_expiration=False)
        g = inv.guild
        res = (g.id, g.name) if g else (0, "")
    except discord.NotFound:
        res = (0, "")
    except discord.HTTPException as ex:
        print(f"[INVITES] fetch {code}: {ex}")
        return None
    if len(_resolved) > 5000:
        _resolved.clear()
    _resolved[code] = (time.time(), *res)
    return res


async def find_foreign(guild: discord.Guild, text: str, allow_guilds=()) -> list:
    """Инвайты из текста, которые ведут на другие серверы: [{code, guild_id, guild_name}]."""
    out = []
    for code in invite_codes(text):
        if f"{guild.id}:{code}" in _invite_cache or code == guild.vanity_url_code:
            continue
        res = await resolve_invite(code)
        if not res or not res[0] or res[0] == guild.id or str(res[0]) in allow_guilds:
            continue
        out.append({"code": code, "guild_id": res[0], "guild_name": res[1]})
    return out


async def check_message(message: discord.Message, modes: dict):
    """Вызывается из security_module.on_message после исключений."""
    mode = modes.get("invites", "off")
    if mode == "off" or not message.content:
        return
    found = await find_foreign(message.guild, message.content, set(modes.get("invite_allow_guilds") or []))
    now = time.time()
    fresh = []
    for f in found:
        key = (message.guild.id, message.author.id, f["guild_id"])
        if now - _last_alert.get(key, 0) >= ALERT_COOLDOWN:
            _last_alert[key] = now
            fresh.append(f)
    if len(_last_alert) > 5000:
        _last_alert.clear()
    if not fresh:
        return

    first = fresh[0]
    servers = ", ".join(f"{f['guild_name']} ({f['guild_id']})" for f in fresh)
    try:
        from security_module import create_alert
        await create_alert(message.guild.id, "foreign_invite", "MEDIUM", message.author.id,
                           f"Инвайт на чужой сервер: {first['guild_name'] or first['guild_id']}",
                           {"source": "bot", "matched": " ".join(f"discord.gg/{f['code']}" for f in fresh),
                            "servers": fresh, "channel_id": message.channel.id,
                            "message_id": message.id})
    except Exception as ex:
        print(f"[INVITES] alert: {ex}")

    ch = await get_log_ch(message.guild)
    if not ch:
        return
    e = discord.Embed(color=C.WARNING, timestamp=discord.utils.utcnow(),
                      description=f"[Перейти к сообщению]({message.jump_url})")
    e.set_author(name="🔗 Инвайт на чужой сервер")
    e.add_field(name="Автор", value=f"{message.author.mention} (`{message.author.id}`)", inline=True)
    e.add_field(name="Канал", value=message.channel.mention, inline=True)
    e.add_field(name="Куда ведёт", value=servers[:1000], inline=False)
    e.add_field(name="Сообщение", value=message.content[:500], inline=False)
    e.set_footer(text="Witness Security · сообщение не удалено")
    content, mentions = None, discord.AllowedMentions.none()
    if mode == "ping":
        roles = [r for r in (message.guild.get_role(int(x)) for x in modes.get("invite_ping_roles") or []
                             if str(x).isdigit()) if r]
        if roles:
            content = " ".join(r.mention for r in roles)
            mentions = discord.AllowedMentions(roles=roles, users=False, everyone=False)
    try:
        await ch.send(content=content, embed=e, allowed_mentions=mentions)
    except discord.HTTPException as ex:
        print(f"[INVITES] log channel: {ex}")
