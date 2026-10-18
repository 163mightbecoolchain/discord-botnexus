"""
Дополнительная защита. Каждая функция включается своим тумблером в дашборде
(«Безопасность» и «Модерация»), по умолчанию всё выключено — см. PROTECTION_DEFAULTS
в database.py. Здесь:
  • канал-ловушка, одинаковые сообщения в разных каналах, «нагрев» (heat)
    и проверка ссылок по открытым базам фишинга — _guard_on_message;
  • фильтр на входе и ответ на рейд (удалить инвайт волны, приостановить
    инвайты и ЛС через Discord) — вызываются из protection.handle_join;
  • режим наблюдения: фильтры только пишут в лог, что сделали бы;
  • защита от взломанного модератора: модераторов фильтры проверяют
    только когда она включена, и наказывают лишь таймаутом;
  • кнопка «Отменить» под логом автоматического наказания (UndoButton);
  • /note и /case — заметки модераторов и кейсы (кейс = номер записи modlog).
"""

import asyncio
import datetime
import hashlib
import re
import time

import aiohttp
import aiosqlite
import discord
from discord import app_commands

from .config import DB_PATH
from .core import bot, is_trusted, resolve_member, _invite_cache
from .database import add_modlog, get_log_ch, get_protection, get_threat_config

# ── Общее ─────────────────────────────────────────────────────

RECOVERY_TIPS = (
    "Похоже, ваш аккаунт взломан: с него рассылался спам.\n"
    "1. Смените пароль Discord (это завершит все сессии).\n"
    "2. Включите двухфакторную аутентификацию.\n"
    "3. Проверьте «Настройки → Авторизованные приложения» и удалите незнакомые.\n"
    "После этого можно вернуться на сервер."
)
ACTION_NAMES = {"timeout": "таймаут", "softban": "софтбан (сообщения за сутки удалены, можно вернуться)",
                "ban": "бан", "kick": "кик", "quarantine": "карантин", "delete": "сообщение удалено",
                "alert": "только уведомление"}
MAX_TIMEOUT_MIN = 40320          # 28 дней — предел таймаута в Discord


def _utcnow():
    return datetime.datetime.utcnow()


def _is_mod(member) -> bool:
    p = getattr(member, "guild_permissions", None)
    return bool(p and (p.administrator or p.manage_guild or p.manage_messages or p.moderate_members
                       or p.ban_members or p.kick_members))


async def _skip_member(member, cfg) -> tuple[bool, bool]:
    """(пропустить, это модератор). Владельца и доверенные роли не проверяем никогда,
    модераторов — только при включённой защите от взломанного модератора."""
    if member.bot or await is_trusted(member.guild, member.id, cfg):
        return True, False
    mod = _is_mod(member)
    return mod and not cfg["mod_guard"], mod


async def _quarantine(member, reason) -> bool:
    """Выдаёт роль карантина из «Модерация → Карантин». False — роль не настроена."""
    gid = member.guild.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT role_id, duration_hours FROM quarantine_settings WHERE guild_id=?",
                              (gid,)) as c:
            q = await c.fetchone()
    role = member.guild.get_role(q[0]) if q and q[0] else None
    if not role:
        return False
    await member.add_roles(role, reason=reason)
    release_at = (_utcnow() + datetime.timedelta(hours=q[1] or 24)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO quarantine (guild_id,user_id,quarantined_at,release_at) VALUES (?,?,?,?) "
            "ON CONFLICT(guild_id,user_id) DO UPDATE SET released=0,release_at=excluded.release_at",
            (gid, member.id, _utcnow().isoformat(), release_at))
        await db.commit()
    return True


async def punish(member, action, cfg, reason, minutes=10, is_mod=False, dm_tips=False) -> tuple[str, str]:
    """Применяет наказание с учётом режима наблюдения и модераторов.
    Возвращает (что сделано — текст для лога, действие для кнопки «Отменить» или "")."""
    if is_mod and action not in ("alert", "delete"):
        action = "timeout"                    # модератора не баним — он мог быть взломан
    if cfg["observe_mode"]:
        return f"Режим наблюдения: было бы — {ACTION_NAMES.get(action, action)}. Ничего не сделано.", ""
    if action in ("alert", "delete"):
        return ACTION_NAMES[action], ""
    guild = member.guild
    if dm_tips and action in ("softban", "ban", "kick"):
        try:                                  # после бана в ЛС уже не написать
            await member.send(f"**{guild.name}**: {RECOVERY_TIPS}")
        except discord.HTTPException:
            pass
    try:
        if action == "timeout":
            minutes = max(1, min(MAX_TIMEOUT_MIN, int(minutes)))
            await member.timeout(datetime.timedelta(minutes=minutes), reason=reason)
            done, undo = f"таймаут на {_fmt_minutes(minutes)}", "timeout"
            if dm_tips:
                try:
                    await member.send(f"**{guild.name}**: {RECOVERY_TIPS}")
                except discord.HTTPException:
                    pass
        elif action == "softban":
            await guild.ban(member, reason=reason, delete_message_seconds=86400)
            await guild.unban(member, reason="Witness: софтбан — сразу разбан")
            done, undo = ACTION_NAMES["softban"], ""
        elif action == "ban":
            await guild.ban(member, reason=reason, delete_message_seconds=86400)
            done, undo = "бан (сообщения за сутки удалены)", "ban"
        elif action == "kick":
            await member.kick(reason=reason)
            done, undo = "кик", ""
        elif action == "quarantine":
            if await _quarantine(member, reason):
                done, undo = "карантин", "quarantine"
            else:
                await member.kick(reason=reason + " (роль карантина не настроена)")
                done, undo = "кик (роль карантина не настроена)", ""
        else:
            return ACTION_NAMES.get(action, action), ""
    except discord.HTTPException as ex:
        why = "роль участника выше роли Witness" if isinstance(ex, discord.Forbidden) else str(ex)
        return f"Не удалось ({ACTION_NAMES.get(action, action)}): {why}", ""
    await add_modlog(guild.id, member.id, 0, f"AUTO_{action.upper()}", reason,
                     _fmt_minutes(minutes) if action == "timeout" else "")
    return done, undo


def _fmt_minutes(m) -> str:
    m = int(m)
    if m % 1440 == 0:
        return f"{m // 1440} дн."
    if m % 60 == 0:
        return f"{m // 60} ч."
    return f"{m} мин."


async def report(guild, title, fields, *, color=0xED4245, cfg=None, undo=None,
                 alert=None, dm_owner=False):
    """Лог-канал (с кнопкой «Отменить», если включена), лента угроз дашборда,
    при dm_owner — личка владельцу. undo = (действие, user_id); alert = (тип, уровень, user_id, текст)."""
    e = discord.Embed(color=color, timestamp=_utcnow())
    e.set_author(name=title)
    for name, value, inline in fields:
        e.add_field(name=name, value=str(value)[:1024] or "—", inline=inline)
    view = None
    if undo and undo[0] and cfg and cfg["undo_buttons"]:
        view = discord.ui.View(timeout=None)
        view.add_item(UndoButton(undo[0], undo[1]))
    ch = await get_log_ch(guild)
    if ch:
        try:
            await ch.send(embed=e, view=view)
        except discord.HTTPException:
            pass
    if dm_owner and guild.owner:
        try:
            await guild.owner.send(embed=e)
        except discord.HTTPException:
            pass
    if alert:
        try:
            from security_module import create_alert
            await create_alert(guild.id, alert[0], alert[1], alert[2] or 0, alert[3], {"source": "guard"})
        except Exception as ex:
            print(f"[GUARD] alert: {ex}")


async def _delete_quiet(channel, message_id):
    try:
        await channel.get_partial_message(message_id).delete()
    except discord.HTTPException:
        pass


# ── Кнопка «Отменить» ────────────────────────────────────────

UNDO_PERMS = {"timeout": "moderate_members", "ban": "ban_members", "quarantine": "manage_roles"}


class UndoButton(discord.ui.DynamicItem[discord.ui.Button], template=r"wundo:(?P<act>[a-z]+):(?P<uid>\d+)"):
    def __init__(self, act: str, uid: int):
        super().__init__(discord.ui.Button(label="Отменить", emoji="↩️", style=discord.ButtonStyle.secondary,
                                           custom_id=f"wundo:{act}:{uid}"))
        self.act, self.uid = act, int(uid)

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(match["act"], int(match["uid"]))

    async def callback(self, interaction: discord.Interaction):
        guild = interaction.guild
        perm = UNDO_PERMS.get(self.act)
        if not guild or not perm or not getattr(interaction.user.guild_permissions, perm, False):
            return await interaction.response.send_message("❌ Нет прав на отмену.", ephemeral=True)
        try:
            if self.act == "timeout":
                m = await resolve_member(guild, self.uid)
                if m:
                    await m.timeout(None, reason=f"Отменено: {interaction.user}")
            elif self.act == "ban":
                await guild.unban(discord.Object(id=self.uid), reason=f"Отменено: {interaction.user}")
            elif self.act == "quarantine":
                async with aiosqlite.connect(DB_PATH) as db:
                    async with db.execute("SELECT role_id FROM quarantine_settings WHERE guild_id=?",
                                          (guild.id,)) as c:
                        row = await c.fetchone()
                    await db.execute("UPDATE quarantine SET released=1 WHERE guild_id=? AND user_id=?",
                                     (guild.id, self.uid))
                    await db.commit()
                m = await resolve_member(guild, self.uid)
                role = guild.get_role(row[0]) if row and row[0] else None
                if m and role and role in m.roles:
                    await m.remove_roles(role, reason=f"Отменено: {interaction.user}")
        except discord.NotFound:
            pass                                  # уже снято вручную
        except discord.HTTPException as ex:
            return await interaction.response.send_message(f"❌ Не удалось отменить: {ex}", ephemeral=True)
        await add_modlog(guild.id, self.uid, interaction.user.id, "UNDO", f"Отмена авто-действия: {self.act}")
        e = interaction.message.embeds[0] if interaction.message and interaction.message.embeds else None
        if e:
            e.add_field(name="Отменено", value=interaction.user.mention, inline=False)
            e.color = 0x57F287
        await interaction.response.edit_message(embed=e, view=None)


# ── Открытые базы фишинговых доменов ─────────────────────────
# Списки скачиваются целиком, домены проверяются локально — ссылки из сообщений
# никуда не отправляются. Качаем, только если хотя бы один сервер включил проверку.

PHISH_SOURCES = (
    ("https://phish.sinking.yachts/v2/all", "json"),
    ("https://raw.githubusercontent.com/Discord-AntiScam/scam-links/main/list.txt", "txt"),
)
PHISH_REFRESH = 6 * 3600
# Крупные легальные домены — на случай, если в списки попадёт ошибочная запись
PHISH_SAFE = {"discord.com", "discord.gg", "discordapp.com", "discord.media", "discordapp.net",
              "steamcommunity.com", "steampowered.com", "youtube.com", "youtu.be", "google.com",
              "github.com", "twitch.tv", "tenor.com", "giphy.com", "imgur.com", "reddit.com",
              "twitter.com", "x.com", "albiononline.com", "roblox.com", "epicgames.com"}
URL_RE = re.compile(r"(?:https?://|www\.)([a-z0-9.-]+\.[a-z]{2,})", re.I)

_phish_domains: set = set()
_phish_loaded_at = 0.0
_phish_needed = False


async def _load_phish_lists():
    global _phish_domains, _phish_loaded_at
    found = set()
    headers = {"X-Identity": "Witness Discord bot", "User-Agent": "Witness-bot"}
    async with aiohttp.ClientSession(headers=headers, timeout=aiohttp.ClientTimeout(total=60)) as s:
        for url, kind in PHISH_SOURCES:
            try:
                async with s.get(url) as r:
                    if r.status != 200:
                        print(f"[GUARD] phish list {url}: HTTP {r.status}")
                        continue
                    if kind == "json":
                        items = await r.json(content_type=None)
                    else:
                        items = (await r.text()).splitlines()
                found.update(d.strip().lower() for d in items if isinstance(d, str) and "." in d)
            except Exception as ex:
                print(f"[GUARD] phish list {url}: {ex}")
    found -= PHISH_SAFE
    _phish_loaded_at = time.time()
    if found:                                 # при сбое оставляем прошлый список
        _phish_domains = found
        print(f"[GUARD] база фишинга: {len(found)} доменов")


async def phish_list_loop():
    while True:
        try:
            if _phish_needed and time.time() - _phish_loaded_at > PHISH_REFRESH:
                await _load_phish_lists()
        except Exception as ex:
            print(f"[GUARD] phish loop: {ex}")
        await asyncio.sleep(60)


def phish_hits(text: str) -> list[str]:
    hits = []
    for host in URL_RE.findall(text or ""):
        parts = host.lower().rstrip(".").split(".")
        # sub.evil.com → проверяем и evil.com
        for i in range(len(parts) - 1):
            d = ".".join(parts[i:])
            if d in PHISH_SAFE:
                break
            if d in _phish_domains:
                hits.append(d)
                break
    return list(dict.fromkeys(hits))


# ── Проверка сообщений ───────────────────────────────────────

_dup_seen: dict = {}      # (gid, uid) → [(время, хеш, channel_id, message_id)]
_heat: dict = {}          # (gid, uid) → [очки, время последнего обновления]
_heat_strikes: dict = {}  # (gid, uid) → [время срабатываний за сутки]
HEAT_LIMIT = {"low": 18, "medium": 12, "high": 8}   # «высокая» чувствительность — ниже порог
HEAT_DECAY = 0.4                                     # очков в секунду


def _dup_key(message) -> str:
    text = re.sub(r"\s+", " ", (message.content or "").strip().lower())
    files = sorted(f"{a.filename}:{a.size}" for a in message.attachments)
    if len(text) < 8 and not files and "http" not in text:
        return ""                             # «+», «ок» и т.п. в разных каналах — не спам
    return hashlib.sha1((text + "|" + "|".join(files)).encode()).hexdigest()


def _heat_points(message) -> float:
    text = message.content or ""
    pts = 1.0
    pts += 0.5 * len(message.raw_mentions) + 0.5 * len(message.raw_role_mentions)
    if "@everyone" in text or "@here" in text:
        pts += 3
    pts += 1.5 * len(URL_RE.findall(text))
    pts += 1.0 * len(message.attachments)
    pts += min(2.0, len(text) / 500)
    return pts


def _exempt_by_threats(message, tcfg) -> bool:
    """Роли и каналы-исключения из «Безопасность → Исключения» (как у AutoMod)."""
    chans = set(tcfg["exempt_channels"])
    roles = set(tcfg["exempt_roles"])
    return (str(message.channel.id) in chans
            or str(getattr(message.channel, "category_id", "")) in chans
            or any(str(r.id) in roles for r in getattr(message.author, "roles", [])))


@bot.listen("on_message")
async def _guard_on_message(message: discord.Message):
    global _phish_needed
    if message.author.bot or not message.guild or not isinstance(message.author, discord.Member):
        return
    cfg = await get_protection(message.guild.id)
    if not (cfg["trap_enabled"] or cfg["dup_enabled"] or cfg["heat_enabled"] or cfg["phishdb_mode"] != "off"):
        return
    skip, is_mod = await _skip_member(message.author, cfg)
    if skip:
        return
    try:
        if cfg["trap_enabled"] and str(message.channel.id) == cfg["trap_channel"]:
            return await _on_trap(message, cfg, is_mod)
        tcfg = await get_threat_config(message.guild.id)
        if _exempt_by_threats(message, tcfg):
            return
        if cfg["phishdb_mode"] != "off":
            _phish_needed = True
            if await _on_phish(message, cfg):
                return
        if cfg["dup_enabled"] and await _on_dup(message, cfg, is_mod):
            return
        if cfg["heat_enabled"]:
            await _on_heat(message, cfg, is_mod)
    except Exception as ex:
        print(f"[GUARD] on_message: {ex}")


async def _on_trap(message, cfg, is_mod):
    m = message.author
    if not cfg["observe_mode"]:
        await _delete_quiet(message.channel, message.id)
    done, undo = await punish(m, cfg["trap_action"], cfg, "Witness: сообщение в канале-ловушке",
                              minutes=1440, is_mod=is_mod, dm_tips=True)
    await report(message.guild, "🪤 Сообщение в канале-ловушке", [
        ("Участник", f"{m.mention} (`{m}`)", True),
        ("Канал", message.channel.mention, True),
        ("Что сделано", done, False),
        ("Сообщение", message.content[:500] or "*(вложение)*", False),
    ] + ([("⚠️ Это модератор", "Возможно, аккаунт взломан — проверьте.", False)] if is_mod else []),
        cfg=cfg, undo=(undo, m.id), alert=("honeypot", "HIGH", m.id, "Сообщение в канале-ловушке"),
        dm_owner=is_mod)


async def _on_phish(message, cfg) -> bool:
    hits = phish_hits(message.content)
    if not hits:
        return False
    block = cfg["phishdb_mode"] == "block" and not cfg["observe_mode"]
    if block:
        await _delete_quiet(message.channel, message.id)
    done = ("сообщение удалено" if block else
            "Режим наблюдения: сообщение было бы удалено" if cfg["phishdb_mode"] == "block" else
            "только уведомление")
    await report(message.guild, "🎣 Ссылка из базы фишинга", [
        ("Автор", message.author.mention, True),
        ("Канал", message.channel.mention, True),
        ("Домены", ", ".join(f"`{d}`" for d in hits[:5]), False),
        ("Что сделано", done, False),
    ], cfg=cfg, alert=("phishing_detected", "HIGH", message.author.id,
                       "Ссылка из базы фишинга: " + ", ".join(hits[:3])))
    return block


async def _on_dup(message, cfg, is_mod) -> bool:
    key = _dup_key(message)
    if not key:
        return False
    k = (message.guild.id, message.author.id)
    now = time.time()
    seen = [x for x in _dup_seen.get(k, []) if now - x[0] < cfg["dup_window"]]
    seen.append((now, key, message.channel.id, message.id))
    _dup_seen[k] = seen
    same = [x for x in seen if x[1] == key]
    if len(same) < cfg["dup_count"] or len({x[2] for x in same}) < 2:
        return False
    _dup_seen.pop(k, None)
    if not cfg["observe_mode"]:
        for _, _, ch_id, msg_id in same:
            ch = message.guild.get_channel(ch_id)
            if ch:
                await _delete_quiet(ch, msg_id)
    m = message.author
    done, undo = await punish(m, "timeout", cfg, "Witness: одинаковые сообщения в разных каналах",
                              minutes=cfg["dup_timeout"], is_mod=is_mod, dm_tips=True)
    if not cfg["observe_mode"]:
        done += f"; удалено копий: {len(same)}"
    await report(message.guild, "📨 Одно сообщение в нескольких каналах", [
        ("Участник", f"{m.mention} (`{m}`)", True),
        ("Каналы", " ".join(f"<#{c}>" for c in dict.fromkeys(x[2] for x in same)), True),
        ("Что сделано", done, False),
        ("Сообщение", message.content[:500] or "*(вложение)*", False),
    ] + ([("⚠️ Это модератор", "Возможно, аккаунт взломан — проверьте.", False)] if is_mod else []),
        cfg=cfg, undo=(undo, m.id),
        alert=("duplicate_content", "HIGH", m.id, f"Одно сообщение в {len({x[2] for x in same})} каналах"),
        dm_owner=is_mod)
    return True


async def _on_heat(message, cfg, is_mod):
    k = (message.guild.id, message.author.id)
    now = time.time()
    heat, last = _heat.get(k, (0.0, now))
    heat = max(0.0, heat - (now - last) * HEAT_DECAY) + _heat_points(message)
    _heat[k] = [heat, now]
    if heat < HEAT_LIMIT[cfg["heat_level"]]:
        return
    _heat.pop(k, None)
    strikes = [t for t in _heat_strikes.get(k, []) if now - t < 86400] + [now]
    _heat_strikes[k] = strikes
    minutes = cfg["heat_timeout"] * 2 ** (len(strikes) - 1)   # повторно за сутки — вдвое дольше
    m = message.author
    done, undo = await punish(m, "timeout", cfg, "Witness: перегрев (слишком много спама)",
                              minutes=minutes, is_mod=is_mod)
    await report(message.guild, "🔥 Перегрев: слишком активный спам", [
        ("Участник", f"{m.mention} (`{m}`)", True),
        ("Канал", message.channel.mention, True),
        ("Срабатываний за сутки", str(len(strikes)), True),
        ("Что сделано", done, False),
    ], color=0xFEE75C, cfg=cfg, undo=(undo, m.id),
        alert=("spam_heat", "MEDIUM", m.id, f"Перегрев, срабатывание №{len(strikes)} за сутки"),
        dm_owner=is_mod)


def cleanup_memory():
    """Чистка устаревших записей; вызывается из memory_cleanup_loop."""
    now = time.time()
    for d, ttl in ((_dup_seen, 300),):
        for k in [k for k, v in d.items() if not v or now - v[-1][0] > ttl]:
            d.pop(k, None)
    for k in [k for k, v in _heat.items() if now - v[1] > 600]:
        _heat.pop(k, None)
    for k in [k for k, v in _heat_strikes.items() if not v or now - v[-1] > 86400]:
        _heat_strikes.pop(k, None)


# ── Вход участника ───────────────────────────────────────────

async def join_filter(member) -> bool:
    """Проверка новых участников вне локдауна. True — участник удалён."""
    cfg = await get_protection(member.guild.id)
    if not cfg["joinfilter_enabled"] or member.bot or await is_trusted(member.guild, member.id, cfg):
        return False
    reasons = []
    age = (_utcnow() - member.created_at.replace(tzinfo=None)).days
    if cfg["joinfilter_age"] and age < cfg["joinfilter_age"]:
        reasons.append(f"аккаунту {age} дн. (меньше {cfg['joinfilter_age']})")
    if cfg["joinfilter_noavatar"] and member.avatar is None:
        reasons.append("нет аватарки")
    if not reasons:
        return False
    action = cfg["joinfilter_action"]
    done, undo = await punish(member, action, cfg, "Witness фильтр входа: " + ", ".join(reasons))
    await report(member.guild, "🚪 Фильтр входа", [
        ("Участник", f"{member.mention} (`{member}`)", True),
        ("Почему", ", ".join(reasons), True),
        ("Что сделано", done, False),
    ], color=0xFEE75C, cfg=cfg, undo=(undo, member.id))
    return done.startswith("кик")


async def on_raid(guild, cfg) -> list[str]:
    """Ответ на волну рейда (раз на волну): пауза инвайтов/ЛС и удаление инвайта волны.
    Возвращает строки для алерта о рейде."""
    lines = []
    if cfg["incident_enabled"]:
        until = discord.utils.utcnow() + datetime.timedelta(minutes=cfg["incident_minutes"])
        what = "инвайты" + (" и ЛС между участниками" if cfg["incident_dms"] else "")
        if cfg["observe_mode"]:
            lines.append(f"Режим наблюдения: {what} были бы приостановлены на {_fmt_minutes(cfg['incident_minutes'])}")
        else:
            try:
                kw = {"invites_disabled_until": until}
                if cfg["incident_dms"]:
                    kw["dms_disabled_until"] = until
                await guild.edit(**kw, reason="Witness анти-рейд")
                lines.append(f"Приостановлены {what} до {until.strftime('%H:%M')} UTC")
            except (discord.HTTPException, TypeError) as ex:
                lines.append(f"Приостановить {what} не удалось: {ex}")
    if cfg["raid_invite_delete"]:
        asyncio.create_task(_delete_raid_invites(guild, cfg))
    return lines


async def _delete_raid_invites(guild, cfg):
    """Ищет инвайт, по которому шла волна: рост счётчика против кэша и журнал инвайтов."""
    await asyncio.sleep(4)                    # даём on_member_join записать использованные инвайты
    counts: dict = {}
    try:
        invites = await guild.invites()
    except discord.HTTPException:
        return
    for inv in invites:
        old = _invite_cache.get(f"{guild.id}:{inv.code}")
        if old is not None and (inv.uses or 0) > old:
            counts[inv.code] = (inv.uses or 0) - old
    since = (_utcnow() - datetime.timedelta(seconds=cfg["raid_window"] * 3 + 30)).isoformat()
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT invite_code, COUNT(*) FROM invite_log WHERE guild_id=? "
                                  "AND joined_at > ? GROUP BY invite_code", (guild.id, since)) as c:
                for code, n in await c.fetchall():
                    counts[code] = max(counts.get(code, 0), n)
    except Exception as ex:
        print(f"[GUARD] raid invites: {ex}")
    total = sum(counts.values())
    bad = [code for code, n in counts.items() if n >= 3 or (total and n * 2 >= total and n >= 2)]
    by_code = {inv.code: inv for inv in invites}
    done = []
    for code in bad:
        inv = by_code.get(code)
        if not inv:
            continue
        if cfg["observe_mode"]:
            done.append(f"`{code}` — был бы удалён ({counts[code]} входов)")
            continue
        try:
            await inv.delete(reason="Witness анти-рейд: по инвайту шла волна входов")
            _invite_cache.pop(f"{guild.id}:{code}", None)
            done.append(f"`{code}` удалён ({counts[code]} входов, создал {inv.inviter or '—'})")
        except discord.HTTPException as ex:
            done.append(f"`{code}` — удалить не удалось: {ex}")
    if done:
        await report(guild, "🔗 Анти-рейд: инвайт волны", [("Инвайты", "\n".join(done), False)],
                     cfg=cfg, alert=("raid", "HIGH", 0, "Анти-рейд: " + "; ".join(done)[:200]))


# ── Кейсы и заметки ──────────────────────────────────────────

async def _cases_allowed(interaction) -> bool:
    p = interaction.user.guild_permissions
    if not (p.manage_messages or p.moderate_members or p.administrator):
        await interaction.response.send_message("❌ Нет прав.", ephemeral=True)
        return False
    if not (await get_protection(interaction.guild_id))["cases_enabled"]:
        await interaction.response.send_message(
            "ℹ️ Кейсы и заметки выключены. Включить: дашборд → «Модерация» → «Кейсы и заметки».",
            ephemeral=True)
        return False
    return True


async def get_notes(gid, uid, limit=25):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id, mod_id, text, created_at FROM mod_notes WHERE guild_id=? AND user_id=? "
                              "ORDER BY id DESC LIMIT ?", (gid, uid, limit)) as c:
            return await c.fetchall()


async def add_note(gid, uid, mod_id, text) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("INSERT INTO mod_notes (guild_id,user_id,mod_id,text,created_at) VALUES (?,?,?,?,?)",
                               (gid, uid, mod_id, text[:1000], _utcnow().isoformat()))
        await db.commit()
        return cur.lastrowid


async def delete_note(gid, note_id) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("DELETE FROM mod_notes WHERE guild_id=? AND id=?", (gid, note_id))
        await db.commit()
        return cur.rowcount > 0


note_grp = app_commands.Group(name="note", description="Заметки модераторов об участнике")


@note_grp.command(name="add", description="Добавить заметку (участник её не видит)")
@app_commands.describe(member="Участник", text="Текст заметки")
async def note_add(interaction: discord.Interaction, member: discord.Member, text: app_commands.Range[str, 1, 1000]):
    if not await _cases_allowed(interaction):
        return
    nid = await add_note(interaction.guild_id, member.id, interaction.user.id, text)
    await interaction.response.send_message(f"📝 Заметка #{nid} о {member.mention} сохранена.", ephemeral=True)


@note_grp.command(name="list", description="Заметки об участнике")
@app_commands.describe(member="Участник")
async def note_list(interaction: discord.Interaction, member: discord.User):
    if not await _cases_allowed(interaction):
        return
    rows = await get_notes(interaction.guild_id, member.id)
    e = discord.Embed(color=0x5865F2, timestamp=_utcnow())
    e.set_author(name=f"Заметки — {member.display_name}", icon_url=member.display_avatar.url)
    e.description = "Заметок нет." if not rows else None
    for nid, mod_id, text, created in rows[:10]:
        e.add_field(name=f"#{nid} · {created[:10]}", value=f"{text[:1000]}\n— <@{mod_id}>", inline=False)
    await interaction.response.send_message(embed=e, ephemeral=True)


@note_grp.command(name="remove", description="Удалить заметку по номеру")
@app_commands.describe(note_id="Номер заметки")
async def note_remove(interaction: discord.Interaction, note_id: int):
    if not await _cases_allowed(interaction):
        return
    ok = await delete_note(interaction.guild_id, note_id)
    await interaction.response.send_message(f"🗑️ Заметка #{note_id} удалена." if ok else "❌ Заметка не найдена.",
                                            ephemeral=True)


bot.tree.add_command(note_grp)

case_grp = app_commands.Group(name="case", description="Кейсы модерации (номер — из /modlog)")


async def _get_case(gid, case_id):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, mod_id, action, reason, duration, created_at FROM modlog "
                              "WHERE guild_id=? AND id=?", (gid, case_id)) as c:
            return await c.fetchone()


@case_grp.command(name="show", description="Показать кейс")
@app_commands.describe(case_id="Номер кейса")
async def case_show(interaction: discord.Interaction, case_id: int):
    if not await _cases_allowed(interaction):
        return
    row = await _get_case(interaction.guild_id, case_id)
    if not row:
        return await interaction.response.send_message("❌ Кейс не найден.", ephemeral=True)
    uid, mod_id, action, reason, duration, created = row
    e = discord.Embed(color=0xFEE75C, title=f"Кейс #{case_id} · {action}")
    e.add_field(name="Участник", value=f"<@{uid}> (`{uid}`)", inline=True)
    e.add_field(name="Модератор", value=f"<@{mod_id}>" if mod_id else "Witness (авто)", inline=True)
    e.add_field(name="Когда", value=created[:16].replace("T", " ") + " UTC", inline=True)
    if duration:
        e.add_field(name="Срок", value=duration, inline=True)
    e.add_field(name="Причина", value=(reason or "—")[:1024], inline=False)
    await interaction.response.send_message(embed=e, ephemeral=True)


@case_grp.command(name="reason", description="Изменить причину в кейсе")
@app_commands.describe(case_id="Номер кейса", reason="Новая причина")
async def case_reason(interaction: discord.Interaction, case_id: int, reason: app_commands.Range[str, 1, 500]):
    if not await _cases_allowed(interaction):
        return
    row = await _get_case(interaction.guild_id, case_id)
    if not row:
        return await interaction.response.send_message("❌ Кейс не найден.", ephemeral=True)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE modlog SET reason=? WHERE guild_id=? AND id=?",
                         (f"{reason} (изм. {interaction.user})", interaction.guild_id, case_id))
        await db.commit()
    await interaction.response.send_message(f"✏️ Причина в кейсе #{case_id} обновлена.", ephemeral=True)


bot.tree.add_command(case_grp)


# ── ИИ-разбор жалоб ──────────────────────────────────────────

async def ai_report_field(gid, text) -> tuple[str, int] | None:
    """Оценка жалобы ИИ для /report: (текст поля, цвет). None — выключено или ИИ недоступен."""
    if not text or not (await get_protection(gid))["ai_reports"]:
        return None
    from .config import GROQ_KEY, GEMINI_KEY
    from security_module import nlp_analyze
    try:
        r = await asyncio.wait_for(nlp_analyze(text, GROQ_KEY or "", GEMINI_KEY or ""), timeout=20)
    except Exception as ex:
        print(f"[GUARD] ai report: {ex}")
        return None
    if not r.get("source") or "rule" in r["source"].lower():
        return None                           # без ИИ-ответа оценку не показываем

    def pct(v):
        try:
            return round(float(v) * 100)
        except (TypeError, ValueError):
            return 0
    tox, thr, spam = pct(r.get("toxicity")), pct(r.get("threat")), pct(r.get("spam"))
    worst = max(tox, thr, spam)
    level = "🔴 высокий" if worst >= 70 else "🟡 средний" if worst >= 40 else "🟢 низкий"
    color = 0xED4245 if worst >= 70 else 0xFEE75C if worst >= 40 else 0x57F287
    body = (f"Приоритет: **{level}**\nТоксичность {tox}% · угрозы {thr}% · спам {spam}%\n"
            f"{str(r.get('summary') or '')[:300]}\n-# Подсказка ИИ ({r['source']}), решение — за модератором")
    return body, color
