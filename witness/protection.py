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
    undo = {}
    if cfg["escalation"] == "revert" and target:
        removable = [r for r in risky if r < guild.me.top_role and not r.managed]
        try:
            if removable:
                await target.remove_roles(*removable, reason="Witness: откат выдачи опасной роли")
                taken = "Сняты роли: " + ", ".join(r.mention for r in removable)
                undo = {"removed_roles": [str(r.id) for r in removable], "member_id": str(target.id)}
            if len(removable) < len(risky):
                taken += "\nЧасть ролей выше роли Witness — не сняты."
        except discord.HTTPException as ex:
            taken = f"Снять роли не удалось: {ex}"
    gained = sorted({p for r in risky for p in ESCALATION_PERMS if getattr(r.permissions, p)})
    who = target.mention if target else f"`{entry.target.id}`"
    await _escalation_alert(guild, actor, f"{who} выданы роли {', '.join(r.mention for r in risky)}",
                            gained, taken, undo)


async def _escalation_alert(guild, actor, what, perms, taken, undo=None):
    by = await resolve_member(guild, actor)
    e = build_embed(C.DANGER)
    e.set_author(name="⚠️ Выданы опасные права")
    e.add_field(name="Что", value=what[:1024], inline=False)
    e.add_field(name="Права", value=", ".join(PERM_NAMES.get(p, p) for p in perms), inline=True)
    e.add_field(name="Кто выдал", value=by.mention if by else f"`{actor}`", inline=True)
    e.add_field(name="Что сделано", value=taken[:1024], inline=False)
    await _notify(guild, e, "permission_escalation", "HIGH", actor,
                  "Выданы опасные права: " + ", ".join(PERM_NAMES.get(p, p) for p in perms),
                  {"source": "antinuke", "taken": taken, **(undo or {})}, dm_owner=True)


# ── Откат ответа анти-нюка (кнопка «Вернуть роли» в дашборде) ─

async def restore_from_alert(guild, alert_id: int, by_user_id: int) -> dict:
    """Возвращает роли (и снимает таймаут), снятые анти-нюком по алерту alert_id.
    Роли, которые с тех пор удалили или подняли выше роли Witness, пропускаются."""
    import json
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT user_id, metadata FROM security_alerts WHERE id=? AND guild_id=?",
                              (alert_id, guild.id)) as c:
            row = await c.fetchone()
    if not row:
        return {"error": "not_found"}
    meta = json.loads(row[1] or "{}")
    if meta.get("restored"):
        return {"error": "already_restored"}
    role_ids = meta.get("removed_roles") or []
    if not role_ids and not meta.get("timed_out"):
        return {"error": "nothing_to_restore"}
    member = await resolve_member(guild, int(meta.get("member_id") or row[0] or 0))
    if not member:
        return {"error": "member_gone"}

    me = guild.me
    roles = [r for rid in role_ids if (r := guild.get_role(int(rid)))]
    addable = [r for r in roles if r < me.top_role and not r.managed and r not in member.roles]
    skipped = len(role_ids) - len(roles) + len([r for r in roles if r not in addable and r not in member.roles])
    restored = []
    try:
        if addable:
            await member.add_roles(*addable, reason=f"Witness: роли возвращены из дашборда ({by_user_id})")
            restored = [r.mention for r in addable]
        if meta.get("timed_out"):
            await member.timeout(None, reason="Witness: таймаут анти-нюка снят из дашборда")
    except discord.HTTPException as ex:
        return {"error": "discord", "detail": str(ex)}

    meta["restored"] = {"by": str(by_user_id), "at": _utcnow().isoformat(), "roles": [str(r.id) for r in addable]}
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE security_alerts SET metadata=?, resolved=1 WHERE id=? AND guild_id=?",
                         (json.dumps(meta), alert_id, guild.id))
        await db.commit()

    by = await resolve_member(guild, by_user_id)
    e = build_embed(C.SUCCESS)
    e.set_author(name="↩️ Анти-нюк: роли возвращены")
    e.add_field(name="Участник", value=member.mention, inline=True)
    e.add_field(name="Вернул", value=by.mention if by else f"`{by_user_id}`", inline=True)
    e.add_field(name="Роли", value=", ".join(restored) or "—", inline=False)
    if meta.get("timed_out"):
        e.add_field(name="Таймаут", value="снят", inline=True)
    if skipped:
        e.add_field(name="Пропущено", value=f"{skipped} — роль удалена или выше роли Witness", inline=True)
    ch = await get_log_ch(guild)
    if ch:
        queue_log(ch, e)
    return {"ok": True, "restored": len(addable), "skipped": skipped, "untimed": bool(meta.get("timed_out"))}


# ── Проверка готовности (блок на «Обзоре» дашборда) ───────────
# Каждый пункт: {"key", "level": ok|info|warn|error, "data": {...}}; info — к сведению,
# не проблема. Тексты рисует
# дашборд по key — так их можно перевести.
NEEDED_PERMS = {
    # право: насколько критично без него
    "manage_guild": "error",      # правила AutoMod, инвайты
    "view_audit_log": "error",    # анти-нюк, кики и роли без интентов
    "manage_roles": "error",      # снять роли анти-нюком, карантин
    "ban_members": "warn", "kick_members": "warn", "moderate_members": "warn",
    "manage_messages": "warn", "send_messages": "warn", "embed_links": "warn",
}


async def readiness(guild) -> list:
    from .core import DANGEROUS_PERMS, intents
    me = guild.me
    cfg = await get_protection(guild.id)
    _, sec = await get_security(guild.id)
    out = []

    perms = me.guild_permissions
    missing = [p for p in NEEDED_PERMS if not getattr(perms, p)]
    level = "ok" if not missing else ("error" if any(NEEDED_PERMS[p] == "error" for p in missing) else "warn")
    out.append({"key": "bot_perms", "level": level, "data": {"missing": missing}})

    ch = await get_log_ch(guild)
    if not ch:
        out.append({"key": "log_channel", "level": "error", "data": {"state": "missing"}})
    else:
        cp = ch.permissions_for(me)
        ok = cp.view_channel and cp.send_messages and cp.embed_links
        out.append({"key": "log_channel", "level": "ok" if ok else "error",
                    "data": {"state": "ok" if ok else "no_access", "channel": ch.name}})

    if cfg["antinuke_enabled"] and cfg["antinuke_action"] != "alert":
        above = [r.name for r in guild.roles
                 if not r.is_default() and not r.managed and r >= me.top_role and r not in me.roles
                 and any(getattr(r.permissions, p) for p in DANGEROUS_PERMS)]
        out.append({"key": "role_position", "level": "warn" if above else "ok",
                    "data": {"roles": above[:10], "bot_role": me.top_role.name}})
        out.append({"key": "trusted_roles", "level": "ok" if cfg["trusted_roles"] else "warn",
                    "data": {"count": len(cfg["trusted_roles"])}})

    if not intents.members:
        on = bool(guild.system_channel and guild.system_channel_flags.join_notifications)
        out.append({"key": "join_messages", "level": "ok" if on else "warn", "data": {}})

    if sec.get("anti_raid"):
        tier_ok = await get_tier(guild.id) >= TIER_PREMIUM
        # Анти-рейд включён по умолчанию у всех — без Premium это не ошибка настройки
        out.append({"key": "raid_premium", "level": "ok" if tier_ok else "info", "data": {}})
        if cfg["raid_action"] == "quarantine":
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("SELECT role_id FROM quarantine_settings WHERE guild_id=?",
                                      (guild.id,)) as c:
                    q = await c.fetchone()
            role = guild.get_role(q[0]) if q and q[0] else None
            state = "missing" if not role else ("too_high" if role >= me.top_role else "ok")
            out.append({"key": "quarantine_role", "level": "ok" if state == "ok" else "warn",
                        "data": {"state": state, "role": role.name if role else None}})
    return out
