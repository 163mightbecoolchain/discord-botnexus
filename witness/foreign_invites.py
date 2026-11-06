"""
Инвайты на чужие серверы в сообщениях.

AutoMod так не умеет: он ищет текст, а не смотрит, куда ведёт ссылка. Поэтому
проверяет сам бот (нужен Message Content Intent):
  1) достаёт из сообщения коды discord.gg/…, discord.com/invite/…;
  2) код из кэша инвайтов сервера или его vanity — свой, дальше не смотрим;
  3) остальное спрашивает у Discord (fetch_invite) и запоминает ответ на час.
Режим alert — алерт в лог-канал, ping — то же с пингом ролей модераторов,
delete — бот удаляет сообщение (нужно право «Управлять сообщениями») и пишет в
лог-канал. Сообщения модераторов (Manage Messages / Administrator) не удаляются —
по ним только алерт. Мёртвые ссылки и серверы-партнёры из списка пропускаются.
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
# id сообщений, которые удалил этот модуль, → время. Лог удалений (events.py) их
# пропускает: в лог-канал уже ушла карточка про инвайт.
_deleted_by_us: dict = {}


def deleted_by_witness(message_id: int) -> bool:
    return _deleted_by_us.pop(message_id, None) is not None


def _is_moderator(member) -> bool:
    p = getattr(member, "guild_permissions", None)
    return bool(p and (p.administrator or p.manage_messages))


async def _delete(message: discord.Message):
    """(удалено ли, причина, если нет)."""
    if not message.channel.permissions_for(message.guild.me).manage_messages:
        return False, "у бота нет права «Управлять сообщениями» в этом канале"
    now = time.time()
    if len(_deleted_by_us) > 500:
        for k in [k for k, t in _deleted_by_us.items() if now - t > 60]:
            _deleted_by_us.pop(k, None)
    _deleted_by_us[message.id] = now
    try:
        await message.delete()
    except discord.NotFound:
        pass                        # уже удалили — считаем удалённым
    except discord.HTTPException as ex:
        _deleted_by_us.pop(message.id, None)
        return False, f"Discord не дал удалить: {ex.status}"
    return True, None


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


async def only_allowed_invites(text: str, allow_guilds) -> bool:
    """True, если в тексте есть инвайты и все они ведут на серверы из allow_guilds.
    Нужна логу удалений: такие сообщения из белого списка логов не пишем.
    Мёртвая или не проверенная ссылка — не из списка."""
    allow = {str(g) for g in allow_guilds or []}
    codes = invite_codes(text)
    if not allow or not codes:
        return False
    for code in codes:
        res = await resolve_invite(code)
        if not res or not res[0] or str(res[0]) not in allow:
            return False
    return True


async def check_message(message: discord.Message, modes: dict):
    """Вызывается из security_module.on_message после исключений."""
    mode = modes.get("invites", "off")
    if mode == "off" or not message.content:
        return
    found = await find_foreign(message.guild, message.content, set(modes.get("invite_allow_guilds") or []))
    if not found:
        return
    deleted, why_not = False, None
    if mode == "delete":
        if _is_moderator(message.author):
            why_not = "автор — модератор"
        else:
            deleted, why_not = await _delete(message)
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
                           f"Инвайт на чужой сервер: {first['guild_name'] or first['guild_id']}"
                           + (" — удалено" if deleted else ""),
                           {"source": "bot", "matched": " ".join(f"discord.gg/{f['code']}" for f in fresh),
                            "servers": fresh, "channel_id": message.channel.id,
                            "message_id": message.id, "deleted": deleted})
    except Exception as ex:
        print(f"[INVITES] alert: {ex}")

    ch = await get_log_ch(message.guild)
    if not ch:
        return
    e = discord.Embed(color=C.WARNING, timestamp=discord.utils.utcnow(),
                      description=None if deleted else f"[Перейти к сообщению]({message.jump_url})")
    e.set_author(name="🔗 Инвайт на чужой сервер" + (" — удалён" if deleted else ""))
    e.add_field(name="Автор", value=f"{message.author.mention} (`{message.author.id}`)", inline=True)
    e.add_field(name="Канал", value=message.channel.mention, inline=True)
    e.add_field(name="Куда ведёт", value=servers[:1000], inline=False)
    e.add_field(name="Сообщение", value=message.content[:500], inline=False)
    if deleted:
        footer = "сообщение удалено"
    elif why_not:
        footer = f"сообщение не удалено: {why_not}"
    else:
        footer = "сообщение не удалено"
    e.set_footer(text=f"Witness Security · {footer}")
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
