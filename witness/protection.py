"""
Защита сервера: анти-рейд, локдаун и наблюдение за журналом аудита.

Все пороги и ответные действия — из настроек сервера (database.get_protection),
их меняют в дашборде. Здесь:
  • handle_join — локдаун (кик слишком новых аккаунтов) и анти-рейд: при волне
    входов действие применяется ко всем участникам волны, а не только к последним,
    и на заданное время включается авто-локдаун;
  • журнал аудита — удаления каналов и ролей, создание вебхуков (лимиты анти-нюка),
    добавление ботов и выдача опасных прав (алерт или откат).
"""

import datetime
import time
import discord
import aiosqlite

from .config import DB_PATH, TIER_PREMIUM
from .core import bot, antinuke_check, get_tier, is_trusted, queue_log, resolve_member
from .database import (
    get_guild_settings,
    get_log_ch,
    get_protection,
    get_security,
    is_enabled,
    save_protection,
)
from .ui import build_embed, C

# Права, выдачу которых считаем повышением привилегий
ESCALATION_PERMS = ("administrator", "manage_guild", "manage_roles", "ban_members", "manage_webhooks")
PERM_NAMES = {"administrator": "Администратор", "manage_guild": "Управлять сервером",
              "manage_roles": "Управлять ролями", "ban_members": "Банить участников",
              "manage_webhooks": "Управлять вебхуками"}

_raid_wave: dict = {}        # gid → [(время входа, member_id)]
_raid_handled: dict = {}     # gid → {member_id} — уже обработанные в текущей волне
_raid_alerted: dict = {}     # gid → время последнего алерта о рейде


def _utcnow():
    return datetime.datetime.utcnow()


async def _notify(guild, embed, alert_type, severity, user_id, text, meta, dm_owner=False):
    """Лог-канал, лента угроз дашборда и (для важного) личка владельцу."""
    ch = await get_log_ch(guild)
    if ch:
        queue_log(ch, embed)
    if dm_owner and guild.owner:
        try:
            await guild.owner.send(embed=embed)
        except Exception:
            pass
    try:
        from security_module import create_alert
        await create_alert(guild.id, alert_type, severity, user_id or 0, text, meta)
    except Exception as ex:
        print(f"[PROTECTION] alert: {ex}")


# ── Локдаун и анти-рейд ──────────────────────────────────────

async def lockdown_active(guild) -> bool:
    if (await get_guild_settings(guild.id)).get("lockdown"):
        return True                      # включён вручную через /lockdown
    until = (await get_protection(guild.id)).get("lockdown_until") or ""
    return bool(until) and until > _utcnow().isoformat()


async def handle_join(member) -> bool:
    """Вызывается первым в on_member_join. True — участник удалён, дальше не обрабатывать."""
    guild = member.guild
    gid = guild.id
    age_days = (_utcnow() - member.created_at.replace(tzinfo=None)).days

    if await lockdown_active(guild):
        _, sec = await get_security(gid)
        min_age = int(sec.get("lockdown_min_age", 7) or 7)
        if age_days < min_age:
            try:
                await member.kick(reason=f"Witness локдаун: аккаунту {age_days} дн. (< {min_age})")
                e = build_embed(C.WARNING)
                e.set_author(name="🔒 Локдаун: вход отклонён")
                e.add_field(name="Участник", value=f"{member.mention} (`{member.name}`)", inline=True)
                e.add_field(name="Возраст аккаунта", value=f"{age_days} дн. (минимум {min_age})", inline=True)
                ch = await get_log_ch(guild)
                if ch:
                    queue_log(ch, e)
                return True
            except discord.HTTPException:
                pass

    if not (await get_tier(gid) >= TIER_PREMIUM and await is_enabled(gid, "anti_raid")):
        return False
    cfg = await get_protection(gid)
    now = time.time()
    wave = [(t, uid) for t, uid in _raid_wave.get(gid, []) if now - t < cfg["raid_window"]]
    wave.append((now, member.id))
    _raid_wave[gid] = wave
    if not wave or len(wave) < cfg["raid_joins"]:
        if len(wave) == 1:
            _raid_handled.pop(gid, None)     # новая волна — старый список не нужен
        return False

    handled = _raid_handled.setdefault(gid, set())
    targets = [uid for _, uid in wave if uid not in handled]
    handled.update(targets)
    removed_self = False
    done = 0
    qrole = None
    if cfg["raid_action"] == "quarantine":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT role_id, duration_hours FROM quarantine_settings WHERE guild_id=?",
                                  (gid,)) as c:
                q = await c.fetchone()
        qrole = guild.get_role(q[0]) if q and q[0] else None
        qhours = q[1] if q else 24
    for uid in targets:
        m = member if uid == member.id else await resolve_member(guild, uid)
        if not m or await is_trusted(guild, uid, cfg):
            continue
        try:
            if cfg["raid_action"] == "kick" or (cfg["raid_action"] == "quarantine" and not qrole):
                await m.kick(reason="Witness анти-рейд")
                removed_self |= uid == member.id
                done += 1
            elif cfg["raid_action"] == "quarantine":
                await m.add_roles(qrole, reason="Witness анти-рейд: карантин")
                release_at = (_utcnow() + datetime.timedelta(hours=qhours)).isoformat()
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "INSERT INTO quarantine (guild_id,user_id,quarantined_at,release_at) VALUES (?,?,?,?) "
                        "ON CONFLICT(guild_id,user_id) DO UPDATE SET released=0,release_at=excluded.release_at",
                        (gid, uid, _utcnow().isoformat(), release_at))
                    await db.commit()
                done += 1
        except discord.HTTPException:
            pass

    # Алерт и авто-локдаун — один раз на волну, дальше только досчитываем
    if now - _raid_alerted.get(gid, 0) > 60:
        _raid_alerted[gid] = now
        minutes = cfg["raid_lockdown_minutes"]
        until_txt = "выключен в настройках"
        if minutes:
            until = _utcnow() + datetime.timedelta(minutes=minutes)
            await save_protection(gid, {"lockdown_until": until.isoformat()})
            until_txt = f"на {minutes} мин. (до {until.strftime('%H:%M')} UTC)"
        action_txt = {"kick": "кикнуты", "quarantine": "отправлены в карантин" if qrole else
                      "кикнуты (роль карантина не настроена)", "alert": "не тронуты (только алерт)"}
        e = build_embed(C.DANGER)
        e.set_author(name="🚨 Анти-рейд: волна входов")
        e.add_field(name="Входов", value=f"{len(wave)} за {cfg['raid_window']} сек.", inline=True)
        e.add_field(name="Участники волны", value=f"{done} {action_txt[cfg['raid_action']]}", inline=True)
        e.add_field(name="Авто-локдаун", value=until_txt, inline=False)
        await _notify(guild, e, "raid", "CRITICAL", 0,
                      f"Анти-рейд: {len(wave)} входов за {cfg['raid_window']} сек.",
                      {"source": "antiraid", "joined": len(wave), "handled": done}, dm_owner=True)
    return removed_self


# ── Журнал аудита ─────────────────────────────────────────────

@bot.listen("on_audit_log_entry_create")
async def _protection_audit(entry):
    A = discord.AuditLogAction
    guild, actor = entry.guild, entry.user_id
    try:
        if entry.action == A.channel_delete:
            await antinuke_check(guild, actor, "channel")
        elif entry.action == A.role_delete:
            await antinuke_check(guild, actor, "role")
        elif entry.action == A.webhook_create:
            await antinuke_check(guild, actor, "webhook")
        elif entry.action == A.bot_add:
            await _on_bot_add(entry)
        elif entry.action == A.role_update:
            await _on_role_update(entry)
        elif entry.action == A.member_role_update:
            await _on_member_role_grant(entry)
    except Exception as ex:
        print(f"[PROTECTION] audit {entry.action}: {ex}")


async def _on_bot_add(entry):
    guild, actor = entry.guild, entry.user_id
    cfg = await get_protection(guild.id)
    if cfg["bot_add"] == "off" or await is_trusted(guild, actor, cfg):
        return
    target = entry.target
    taken = "Только уведомление."
    if cfg["bot_add"] == "kick":
        m = await resolve_member(guild, target.id)
        try:
            if m:
                await m.kick(reason="Witness: бот добавлен не владельцем и не доверенной ролью")
                taken = "Бот выгнан с сервера."
        except discord.HTTPException:
            taken = "Выгнать бота не удалось (его роль выше роли Witness)."
    adder = await resolve_member(guild, actor)
    e = build_embed(C.WARNING)
    e.set_author(name="🤖 На сервер добавлен бот")
    e.add_field(name="Бот", value=f"<@{target.id}> (`{target.id}`)", inline=True)
    e.add_field(name="Добавил", value=adder.mention if adder else f"`{actor}`", inline=True)
    e.add_field(name="Что сделано", value=taken, inline=False)
    await _notify(guild, e, "bot_added", "HIGH", actor, f"Добавлен бот {target.id}",
                  {"source": "antinuke", "bot_id": str(target.id), "taken": taken}, dm_owner=True)


async def _on_role_update(entry):
    before = getattr(entry.before, "permissions", None)
    after = getattr(entry.after, "permissions", None)
    if before is None or after is None:
        return
    gained = [p for p in ESCALATION_PERMS if getattr(after, p) and not getattr(before, p)]
    if not gained:
        return
    guild, actor = entry.guild, entry.user_id
    cfg = await get_protection(guild.id)
    if cfg["escalation"] == "off" or await is_trusted(guild, actor, cfg):
        return
    role = guild.get_role(entry.target.id)
    taken = "Только уведомление."
    if cfg["escalation"] == "revert" and role:
        try:
            if role < guild.me.top_role:
                await role.edit(permissions=before, reason="Witness: откат опасных прав роли")
                taken = "Права роли откатены."
            else:
                taken = "Откатить не удалось: роль выше роли Witness."
        except discord.HTTPException as ex:
            taken = f"Откатить не удалось: {ex}"
    await _escalation_alert(guild, actor, f"Роли {role.mention if role else entry.target.id} выданы права",
                            gained, taken)


async def _on_member_role_grant(entry):
    added = [guild_role for r in getattr(entry.after, "roles", []) or []
             if (guild_role := entry.guild.get_role(r.id))]
    risky = [r for r in added if any(getattr(r.permissions, p) for p in ESCALATION_PERMS)]
    if not risky:
        return
    guild, actor = entry.guild, entry.user_id
    cfg = await get_protection(guild.id)
    if cfg["escalation"] == "off" or await is_trusted(guild, actor, cfg):
        return
    target = await resolve_member(guild, entry.target.id)
    taken = "Только уведомление."
    if cfg["escalation"] == "revert" and target:
        removable = [r for r in risky if r < guild.me.top_role and not r.managed]
        try:
            if removable:
                await target.remove_roles(*removable, reason="Witness: откат выдачи опасной роли")
                taken = "Сняты роли: " + ", ".join(r.mention for r in removable)
            if len(removable) < len(risky):
                taken += "\nЧасть ролей выше роли Witness — не сняты."
        except discord.HTTPException as ex:
            taken = f"Снять роли не удалось: {ex}"
    gained = sorted({p for r in risky for p in ESCALATION_PERMS if getattr(r.permissions, p)})
    who = target.mention if target else f"`{entry.target.id}`"
    await _escalation_alert(guild, actor, f"{who} выданы роли {', '.join(r.mention for r in risky)}",
                            gained, taken)


async def _escalation_alert(guild, actor, what, perms, taken):
    by = await resolve_member(guild, actor)
    e = build_embed(C.DANGER)
    e.set_author(name="⚠️ Выданы опасные права")
    e.add_field(name="Что", value=what[:1024], inline=False)
    e.add_field(name="Права", value=", ".join(PERM_NAMES.get(p, p) for p in perms), inline=True)
    e.add_field(name="Кто выдал", value=by.mention if by else f"`{actor}`", inline=True)
    e.add_field(name="Что сделано", value=taken[:1024], inline=False)
    await _notify(guild, e, "permission_escalation", "HIGH", actor,
                  "Выданы опасные права: " + ", ".join(PERM_NAMES.get(p, p) for p in perms),
                  {"source": "antinuke", "taken": taken}, dm_owner=True)
