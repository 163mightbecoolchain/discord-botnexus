"""
Инвайт-трекинг и автоочистка инвайтов.
"""

import discord
from discord import app_commands
import aiosqlite
import os, asyncio, random, time, json, datetime
from .config import DB_PATH, TIER_PREMIUM
from .database import get_invite_history, get_log_ch, get_user_invites
from .ui import build_embed, C
from .core import (
    bot,
    get_tier,
    _invite_cache,
    upsell_embed,
)

@bot.tree.command(name="invnote", description="Добавить заметку к инвайт-коду")
@app_commands.describe(
    code="Код инвайта (без discord.gg/)",
    note="Заметка (например: 'Реклама Reddit'). Пусто = удалить заметку"
)
async def invnote(interaction: discord.Interaction, code: str, note: str = ""):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно право Manage Server.", ephemeral=True)
    gid = interaction.guild_id
    code = clean_code(code)

    await set_invite_note(gid, code, note)
    if note:
        e = discord.Embed(title="📝 Заметка сохранена", color=0x57F287)
        e.add_field(name="Код",      value=f"`{code}`",             inline=True)
        e.add_field(name="Заметка",  value=note,                    inline=True)
        e.add_field(name="Добавил",  value=interaction.user.mention, inline=True)
    else:
        e = discord.Embed(title="🗑️ Заметка удалена", description=f"Код: `{code}`", color=0x36393F)

    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="invnotes", description="Список всех заметок к инвайтам")
async def invnotes(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно право Manage Server.", ephemeral=True)
    gid = interaction.guild_id

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT invite_code, note, MAX(joined_at) as last_use, SUM(member_id IS NOT NULL) as uses
            FROM invite_log
            WHERE guild_id=? AND note != '' AND note IS NOT NULL
            GROUP BY invite_code
            ORDER BY last_use DESC
        """, (gid,)) as c:
            rows = await c.fetchall()

    e = discord.Embed(title="📝 Invite Notes", color=0x00B0F4)
    if not rows:
        e.description = "Заметок нет. Добавь через `/invnote code:КОД note:ЗАМЕТКА`"
    else:
        for code, note, last_use, uses in rows[:15]:
            date_str = last_use[:10] if last_use else "—"
            e.add_field(
                name=f"`{code}`",
                value=f"{note}\n*{uses} uses · last: {date_str}*",
                inline=False
            )
        if len(rows) > 15:
            e.set_footer(text=f"Показано 15 из {len(rows)}")
    await interaction.response.send_message(embed=e, ephemeral=True)


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


async def invite_autoclean_loop():
    """
    Каждые 2 дня проверяет инвайты на всех серверах где включена авто-очистка.
    Удаляет инвайты по которым никто не пришёл за max_age_days дней.
    """
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT guild_id, max_age_days, log_channel FROM invite_autoclean_settings WHERE enabled=1"
                ) as c:
                    guilds_to_check = await c.fetchall()

            for gid, max_age_days, log_ch_id in guilds_to_check:
                guild = bot.get_guild(gid)
                if not guild:
                    continue
                try:
                    await _run_invite_autoclean(guild, max_age_days, log_ch_id)
                except Exception as ex:
                    print(f"[INVITE_CLEAN] Guild {gid} error: {ex}")

        except Exception as ex:
            print(f"[INVITE_CLEAN] Loop error: {ex}")

        await asyncio.sleep(172800)  # 2 дня = 48 часов


async def _run_invite_autoclean(guild: discord.Guild, max_age_days: int, log_ch_id: int):
    """
    Получает список инвайтов на сервере и удаляет те у которых:
    - uses == 0 (никто не перешёл)
    - созданы более max_age_days дней назад
    """
    try:
        invites = await guild.invites()
    except discord.Forbidden:
        return

    now         = datetime.datetime.now(datetime.timezone.utc)
    deleted     = []
    failed      = []
    min_age     = datetime.timedelta(days=max_age_days)

    for inv in invites:
        # Пропускаем инвайты без даты создания
        if not inv.created_at:
            continue
        # Пропускаем инвайты у которых уже были переходы
        if (inv.uses or 0) > 0:
            continue
        # Пропускаем слишком свежие (ещё не прошло max_age_days)
        age = now - inv.created_at
        if age < min_age:
            continue

        # Удаляем
        try:
            await inv.delete(reason=f"Auto-clean: 0 uses за {age.days} дн.")
            deleted.append(inv)
            # Убираем из кэша
            _invite_cache.pop(f"{guild.id}:{inv.code}", None)
        except discord.Forbidden:
            failed.append(inv.code)
        except Exception as ex:
            print(f"[INVITE_CLEAN] Delete error {inv.code}: {ex}")

    if not deleted:
        return

    # Отправляем только итоговое сообщение с количеством
    log_ch = guild.get_channel(log_ch_id) if log_ch_id else await get_log_ch(guild)
    if log_ch:
        try:
            await log_ch.send(
                f"🔗 Авто-очистка инвайтов: было удалено **{len(deleted)}** инвайт "
                f"{'код' if len(deleted) == 1 else 'кодов' if 5 <= len(deleted) % 100 <= 20 or len(deleted) % 10 >= 5 or len(deleted) % 10 == 0 else 'кода'}."
            )
        except Exception as ex:
            print(f"[INVITE_CLEAN] Лог не отправлен: {ex}")

    print(f"[INVITE_CLEAN] {guild.name}: удалено {len(deleted)} инвайтов")


@bot.tree.command(name="invclean", description="Настройка авто-очистки пустых инвайтов")
@app_commands.describe(
    action="enable / disable / status / run",
    max_age_days="Удалять инвайты старше N дней без переходов (по умолчанию 7)"
)
async def invclean(interaction: discord.Interaction,
                   action: str = "status",
                   max_age_days: int = 7):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно Manage Server.", ephemeral=True)

    gid = interaction.guild_id

    if action == "enable":
        max_age_days = max(1, min(365, max_age_days))
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO invite_autoclean_settings (guild_id, enabled, max_age_days)
                VALUES (?, 1, ?)
                ON CONFLICT(guild_id) DO UPDATE SET
                    enabled=1, max_age_days=excluded.max_age_days
            """, (gid, max_age_days))
            await db.commit()
        e = build_embed(C.SUCCESS)
        e.set_author(name="✅ Авто-очистка инвайтов включена")
        e.add_field(name="Удалять инвайты старше", value=f"**{max_age_days} дней** без переходов", inline=True)
        e.add_field(name="Проверка",               value="Каждые **2 дня**", inline=True)
        e.set_footer(text="Используй /invclean run для немедленной проверки")
        return await interaction.response.send_message(embed=e)

    elif action == "disable":
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE invite_autoclean_settings SET enabled=0 WHERE guild_id=?", (gid,)
            )
            await db.commit()
        return await interaction.response.send_message("⏹ Авто-очистка инвайтов **отключена**.", ephemeral=True)

    elif action == "run":
        await interaction.response.defer()
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT max_age_days, log_channel FROM invite_autoclean_settings WHERE guild_id=?",
                (gid,)
            ) as c:
                row = await c.fetchone()
        days = row[0] if row else max_age_days
        log_ch = row[1] if row else 0
        await _run_invite_autoclean(interaction.guild, days, log_ch)
        return await interaction.followup.send(
            f"✅ Проверка завершена. Смотри лог-канал для деталей.", ephemeral=True
        )

    else:  # status
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT enabled, max_age_days FROM invite_autoclean_settings WHERE guild_id=?",
                (gid,)
            ) as c:
                row = await c.fetchone()
        e = build_embed(C.PRIMARY)
        e.set_author(name="🔗 Авто-очистка инвайтов")
        if row and row[0]:
            e.add_field(name="Статус", value="✅ Включена", inline=True)
            e.add_field(name="Удалять старше", value=f"**{row[1]} дней**", inline=True)
            e.add_field(name="Периодичность", value="Каждые 2 дня", inline=True)
        else:
            e.add_field(name="Статус", value="⏹ Отключена", inline=True)
            e.description = "Включи: `/invclean enable max_age_days:7`"
        e.set_footer(text="/invclean enable · /invclean disable · /invclean run (немедленная проверка)")
        return await interaction.response.send_message(embed=e, ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  INVITE STATS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="invstats", description="Статистика и лидерборд инвайтов")
async def invstats(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно право Manage Server.", ephemeral=True)
    gid = interaction.guild_id

    async with aiosqlite.connect(DB_PATH) as db:
        # Топ по количеству приглашённых
        async with db.execute("""
            SELECT inviter_id, inviter_name, COUNT(*) as total
            FROM invite_log
            WHERE guild_id=? AND inviter_id > 0
            GROUP BY inviter_id
            ORDER BY total DESC
            LIMIT 10
        """, (gid,)) as c:
            top_rows = await c.fetchall()

        # Самый используемый инвайт
        async with db.execute("""
            SELECT invite_code, COUNT(*) as uses, inviter_name
            FROM invite_log
            WHERE guild_id=? AND invite_code != '' AND member_id IS NOT NULL
            GROUP BY invite_code
            ORDER BY uses DESC
            LIMIT 5
        """, (gid,)) as c:
            code_rows = await c.fetchall()

        # Всего вошло
        async with db.execute(
            "SELECT COUNT(*) FROM invite_log WHERE guild_id=? AND member_id IS NOT NULL", (gid,)
        ) as c:
            total = (await c.fetchone())[0]

    e = discord.Embed(color=0x5865F2, timestamp=datetime.datetime.utcnow())
    e.set_author(name="Invite Stats")
    e.add_field(name="Total joined via invites", value=f"**{total}**", inline=True)

    if top_rows:
        lines = []
        medals = ["🥇","🥈","🥉"] + [f"{i}." for i in range(4, 11)]
        for i, (uid, uname, total_inv) in enumerate(top_rows):
            m = interaction.guild.get_member(uid)
            name = m.display_name if m else uname
            lines.append(f"{medals[i]} **{name}** — {total_inv} invites")
        e.add_field(name="Top inviters", value="\n".join(lines), inline=False)

    if code_rows:
        lines = [f"`{code}` — {uses} uses ({inv})" for code, uses, inv in code_rows]
        e.add_field(name="Top invite codes", value="\n".join(lines), inline=False)

    await interaction.response.send_message(embed=e, ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ДАННЫЕ ДЛЯ СТРАНИЦЫ «ИНВАЙТЫ» (web_server.api_invites*)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
# invite_log — журнал входов: код ('' — источник не определён), кто создал, кто зашёл.
# Строки без member_id — заглушки, в которых /invnote хранит заметку к коду без входов.
# Активные инвайты берём у Discord (кэш ACTIVE_TTL сек), удержание и наказания считаем
# по тем, кто зашёл: на сервере ли сейчас, есть ли наказание в modlog.

ACTIVE_TTL = 30
PUNISH_ACTIONS = ("BAN", "TEMPBAN", "KICK", "MUTE", "WARN", "AUTO_BAN", "AUTO_SOFTBAN", "AUTO_KICK",
                  "AUTO_TIMEOUT", "AUTO_MUTE", "AUTO_QUARANTINE")
UNKNOWN = ""                      # код записи «источник не определён»
_active_cache: dict = {}          # gid → (время, [инвайты])


def clean_code(raw: str) -> str:
    """discord.gg/abc, https://discord.com/invite/abc → abc."""
    s = (raw or "").strip()
    for p in ("https://", "http://", "www."):
        s = s.removeprefix(p)
    for p in ("discord.gg/", "discord.com/invite/", "discordapp.com/invite/"):
        s = s.removeprefix(p)
    return s.split("?")[0].strip("/ ")


def _iso(dt):
    return dt.replace(tzinfo=None).isoformat() if dt else ""


async def active_invites(guild, fresh=False):
    """Действующие инвайты сервера, вместе с vanity. None — у бота нет права их читать."""
    hit = _active_cache.get(guild.id)
    if hit and not fresh and time.time() - hit[0] < ACTIVE_TTL:
        return hit[1]
    try:
        invs = await guild.invites()
    except discord.HTTPException:
        return None
    out = [{
        "code": inv.code, "inviter_id": str(inv.inviter.id) if inv.inviter else "",
        "inviter_name": inv.inviter.display_name if isinstance(inv.inviter, discord.Member)
                        else (str(inv.inviter) if inv.inviter else ""),
        "channel": getattr(inv.channel, "name", "") or "", "uses": inv.uses or 0,
        "max_uses": inv.max_uses or 0, "max_age": inv.max_age or 0, "temporary": bool(inv.temporary),
        "created_at": _iso(inv.created_at), "expires_at": _iso(inv.expires_at), "vanity": False,
    } for inv in invs]
    if guild.vanity_url_code:
        try:
            vi = await guild.vanity_invite()
            if vi:
                out.append({"code": vi.code, "inviter_id": "", "inviter_name": "vanity-ссылка сервера",
                            "channel": "", "uses": vi.uses or 0, "max_uses": 0, "max_age": 0,
                            "temporary": False, "created_at": "", "expires_at": "", "vanity": True})
        except discord.HTTPException:
            pass
    _active_cache[guild.id] = (time.time(), out)
    return out


async def _punished_ids(db, gid) -> set:
    marks = ",".join("?" * len(PUNISH_ACTIONS))
    async with db.execute(f"SELECT DISTINCT user_id FROM modlog WHERE guild_id=? AND action IN ({marks})",
                          (gid, *PUNISH_ACTIONS)) as c:
        return {r[0] for r in await c.fetchall()}


async def invite_overview(guild, days: int = 30) -> dict:
    """Всё для страницы: инвайты (активные + из журнала), итоги за период, входы по дням, топ."""
    gid = guild.id
    since = (datetime.datetime.utcnow() - datetime.timedelta(days=days)).isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT invite_code, MAX(inviter_id), MAX(inviter_name), MAX(note),
                   MAX(CASE WHEN member_id IS NOT NULL THEN joined_at END)
            FROM invite_log WHERE guild_id=? GROUP BY invite_code""", (gid,)) as c:
            logged = {r[0] or UNKNOWN: r for r in await c.fetchall()}
        async with db.execute("""SELECT invite_code, member_id, joined_at FROM invite_log
                                 WHERE guild_id=? AND member_id IS NOT NULL AND joined_at >= ?""",
                              (gid, since)) as c:
            joins = await c.fetchall()
        async with db.execute("SELECT invite_code, reason FROM invite_flags WHERE guild_id=?", (gid,)) as c:
            flags = dict(await c.fetchall())
        punished = await _punished_ids(db, gid)
    rules = await get_invite_rules(gid)

    per: dict = {}
    days_map: dict = {}
    for code, uid, at in joins:
        code = code or UNKNOWN
        s = per.setdefault(code, {"joins": 0, "stayed": 0, "punished": 0})
        s["joins"] += 1
        s["stayed"] += guild.get_member(uid) is not None
        s["punished"] += uid in punished
        d = days_map.setdefault(at[:10], {"known": 0, "unknown": 0})
        d["unknown" if code == UNKNOWN else "known"] += 1

    active = await active_invites(guild)
    rows = {}
    for a in active or []:
        rows[a["code"]] = {**a, "status": "vanity" if a["vanity"] else "active"}
    for code, (c, iid, iname, note, last) in logged.items():
        r = rows.setdefault(code, {"code": code, "inviter_id": str(iid or ""), "inviter_name": iname or "",
                                   "channel": "", "uses": None, "max_uses": 0, "max_age": 0, "temporary": False,
                                   "created_at": "", "expires_at": "", "vanity": False,
                                   "status": "unknown" if code == UNKNOWN else "gone"})
        r["note"], r["last_join"] = note or "", last or ""
    for code, r in rows.items():
        s = per.get(code, {"joins": 0, "stayed": 0, "punished": 0})
        r.update(s)
        r.setdefault("note", "")
        r.setdefault("last_join", "")
        r["retention"] = round(100 * s["stayed"] / s["joins"]) if s["joins"] else None
        rule = rules.get(code)
        r["rule"] = None if not rule else {"action": rule["action"], "expired": rule["expired"],
                                           "summary": describe_rule(rule), "hits": rule["hits"]}
        r["flag"] = flags.get(code, "")
        if not r["flag"] and rule and not rule["expired"]:
            r["flag"] = "помечена вручную: " + describe_rule(rule)
        if not r["flag"] and s["joins"] >= 3 and s["punished"] * 2 >= s["joins"]:
            r["flag"] = f"наказаны {s['punished']} из {s['joins']} зашедших"
        r["suspicious"] = bool(r["flag"])

    total = sum(s["joins"] for s in per.values())
    stayed = sum(s["stayed"] for s in per.values())
    # Топ пригласивших — по людям, а не по ссылкам
    top: dict = {}
    for r in rows.values():
        if r["status"] in ("vanity", "unknown") or not r["inviter_id"] or not r["joins"]:
            continue
        t = top.setdefault(r["inviter_id"], {"id": r["inviter_id"], "name": r["inviter_name"],
                                             "joins": 0, "stayed": 0, "invites": 0})
        t["joins"] += r["joins"]
        t["stayed"] += r["stayed"]
        t["invites"] += 1
    for t in top.values():
        m = guild.get_member(int(t["id"]))
        if m:
            t["name"] = m.display_name
        t["retention"] = round(100 * t["stayed"] / t["joins"])
    today = datetime.datetime.utcnow().date()
    series = [{"day": d, **days_map.get(d, {"known": 0, "unknown": 0})}
              for d in ((today - datetime.timedelta(days=i)).isoformat() for i in range(days - 1, -1, -1))]
    return {
        "days": days, "invites": sorted(rows.values(), key=lambda r: (-r["joins"], r["code"])),
        "can_read_invites": active is not None,
        "totals": {"joins": total, "unknown": per.get(UNKNOWN, {}).get("joins", 0),
                   "retention": round(100 * stayed / total) if total else None,
                   "active": sum(1 for r in rows.values() if r["status"] in ("active", "vanity")),
                   "suspicious": sum(1 for r in rows.values() if r["suspicious"])},
        "series": series,
        "top": sorted(top.values(), key=lambda t: -t["joins"])[:10],
    }


async def invite_details(guild, code: str, limit: int = 300) -> dict:
    """Карточка инвайта: кто по нему зашёл (за всё время журнала) и кто из них где сейчас."""
    gid = guild.id
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""SELECT member_id, member_name, joined_at FROM invite_log
                                 WHERE guild_id=? AND invite_code=? AND member_id IS NOT NULL
                                 ORDER BY joined_at DESC LIMIT ?""", (gid, code, limit)) as c:
            rows = await c.fetchall()
        async with db.execute("SELECT MAX(inviter_id), MAX(inviter_name), MAX(note) FROM invite_log "
                              "WHERE guild_id=? AND invite_code=?", (gid, code)) as c:
            iid, iname, note = await c.fetchone()
        async with db.execute("SELECT reason FROM invite_flags WHERE guild_id=? AND invite_code=?", (gid, code)) as c:
            flag = await c.fetchone()
        punished = await _punished_ids(db, gid)
    active = next((a for a in await active_invites(guild) or [] if a["code"] == code), None)
    members = [{"id": str(uid), "name": name or str(uid), "joined_at": at,
                "on_server": guild.get_member(uid) is not None, "punished": uid in punished}
               for uid, name, at in rows]
    from .database import get_protection
    return {"code": code, "active": active, "inviter_id": str(iid or ""), "inviter_name": iname or "",
            "note": note or "", "flag": flag[0] if flag else "", "members": members,
            "rule": await get_invite_rule(gid, code) if code else None,
            "observe_mode": (await get_protection(gid))["observe_mode"],
            "joins": len(members), "stayed": sum(m["on_server"] for m in members),
            "punished": sum(m["punished"] for m in members)}


async def set_invite_note(gid: int, code: str, note: str):
    """Заметка к коду (общая с /invnote). Пустая — удалить."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE invite_log SET note=? WHERE guild_id=? AND invite_code=?", (note, gid, code))
        if note:
            async with db.execute("SELECT 1 FROM invite_log WHERE guild_id=? AND invite_code=? LIMIT 1",
                                  (gid, code)) as c:
                if not await c.fetchone():
                    await db.execute("INSERT INTO invite_log (guild_id, invite_code, note) VALUES (?,?,?)",
                                     (gid, code, note))
        await db.commit()


async def delete_invite(guild, code: str, by) -> str:
    """Удаляет действующий инвайт. '' — удалён, иначе код ошибки."""
    inv = next((i for i in await guild.invites() if i.code == code), None) if code else None
    if not inv:
        return "not_found"
    try:
        await inv.delete(reason=f"Удалён с сайта: {getattr(by, 'display_name', by)}")
    except discord.Forbidden:
        return "forbidden"
    except discord.HTTPException:
        return "failed"
    _active_cache.pop(guild.id, None)
    _invite_cache.pop(f"{guild.id}:{code}", None)
    ch = await get_log_ch(guild)
    if ch:
        e = build_embed(C.MUTED)
        e.set_author(name="🔗 Инвайт удалён с сайта")
        e.add_field(name="Код", value=f"`{code}`", inline=True)
        e.add_field(name="Кто удалил", value=getattr(by, "mention", str(by)), inline=True)
        try:
            await ch.send(embed=e)
        except discord.HTTPException:
            pass
    return ""


async def invite_search(guild, q: str, limit: int = 8) -> dict:
    """Поиск сразу по трём разделам: инвайты (код, заметка), создатели, зашедшие."""
    gid = guild.id
    raw = q.strip()
    code_q = clean_code(raw).lower()
    ql = raw.lower()

    def name_now(uid):
        m = guild.get_member(int(uid)) if uid else None
        return f"{m.display_name} {m.name}" if m else ""

    # Сравниваем в Python: LOWER() в SQLite понимает только латиницу,
    # и «реклама» не находила заметку «Реклама Reddit»
    has = lambda text: bool(ql) and ql in (text or "").lower()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT invite_code, MAX(inviter_name), MAX(note), SUM(member_id IS NOT NULL)
            FROM invite_log WHERE guild_id=? AND invite_code != ''
            GROUP BY invite_code ORDER BY 4 DESC""", (gid,)) as c:
            inv_rows = [r for r in await c.fetchall()
                        if (code_q and code_q in r[0].lower()) or has(r[2])][:limit]
        async with db.execute("""
            SELECT inviter_id, MAX(inviter_name), COUNT(DISTINCT invite_code), SUM(member_id IS NOT NULL)
            FROM invite_log WHERE guild_id=? AND inviter_id > 0
            GROUP BY inviter_id ORDER BY 4 DESC""", (gid,)) as c:
            creators = [r for r in await c.fetchall()
                        if has(r[1]) or str(r[0]) == raw or has(name_now(r[0]))][:limit]
        # Зашедшие: по нику на момент входа, по ID и по нынешнему нику на сервере
        async with db.execute("""
            SELECT member_id, member_name, invite_code, inviter_name, joined_at FROM invite_log
            WHERE guild_id=? AND member_id IS NOT NULL ORDER BY joined_at DESC""", (gid,)) as c:
            joined = [r for r in await c.fetchall()
                      if has(r[1]) or str(r[0]) == raw or has(name_now(r[0]))][:limit]
    invites = {r[0]: {"code": r[0], "inviter_name": r[1] or "", "note": r[2] or "", "joins": r[3] or 0}
               for r in inv_rows}
    # Активные инвайты без входов в журнале тоже ищутся по коду и создателю
    for a in await active_invites(guild) or []:
        if a["code"] not in invites and (code_q and code_q in a["code"].lower()):
            invites[a["code"]] = {"code": a["code"], "inviter_name": a["inviter_name"], "note": "", "joins": 0}
    creators = list(creators)
    seen = {str(r[0]) for r in creators}
    for a in await active_invites(guild) or []:
        if a["inviter_id"] and a["inviter_id"] not in seen and ql and ql in a["inviter_name"].lower():
            seen.add(a["inviter_id"])
            creators.append((a["inviter_id"], a["inviter_name"], 1, 0))

    def name_of(uid, fallback):
        m = guild.get_member(int(uid))
        return m.display_name if m else (fallback or str(uid))
    return {
        "invites": list(invites.values())[:limit],
        "creators": [{"id": str(i), "name": name_of(i, n), "invites": k, "joins": j or 0}
                     for i, n, k, j in creators],
        "joined": [{"id": str(u), "name": name_of(u, n), "code": c or "", "inviter_name": iname or "",
                    "joined_at": at} for u, n, c, iname, at in joined],
    }


# ── Правила для подозрительных инвайтов ──────────────────────
# Модератор помечает код на сайте и задаёт, что делать с теми, кто по нему зашёл.
# Проверяется в on_member_join после того, как определён инвайт.

RULE_ACTIONS = ("ban", "kick", "quarantine", "timeout", "alert")
RULE_ACTION_NAMES = {"ban": "бан", "kick": "кик", "quarantine": "карантин",
                     "timeout": "мут", "alert": "только оповещение"}
RULE_DELETE_DAYS = (0, 1, 7)
RULE_FIELDS = ("action", "max_age_days", "no_avatar", "timeout_minutes", "delete_days",
               "reason", "expires_at", "created_by", "created_at", "hits", "last_hit")


def _rule_dict(row) -> dict:
    r = dict(zip(RULE_FIELDS, row))
    r["no_avatar"] = bool(r["no_avatar"])
    r["created_by"] = str(r["created_by"] or "")
    r["expired"] = bool(r["expires_at"]) and r["expires_at"] <= datetime.datetime.utcnow().isoformat()
    return r


async def get_invite_rules(gid: int) -> dict:
    """{код: правило} для сервера — для таблицы на сайте."""
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(f"SELECT invite_code, {','.join(RULE_FIELDS)} FROM invite_rules "
                              "WHERE guild_id=?", (gid,)) as c:
            return {r[0]: _rule_dict(r[1:]) for r in await c.fetchall()}


async def get_invite_rule(gid: int, code: str):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(f"SELECT {','.join(RULE_FIELDS)} FROM invite_rules "
                              "WHERE guild_id=? AND invite_code=?", (gid, code)) as c:
            row = await c.fetchone()
    return _rule_dict(row) if row else None


def clean_rule(data: dict) -> dict:
    """Проверяет то, что пришло с сайта; числа обрезаются по допустимым пределам."""
    def num(key, lo, hi, default):
        try:
            return max(lo, min(hi, int(data.get(key) if data.get(key) not in (None, "") else default)))
        except (TypeError, ValueError):
            return default
    hours = num("hours", 0, 24 * 365, 0)            # срок действия правила, 0 — бессрочно
    delete_days = num("delete_days", 0, 7, 0)
    return {
        "action": data.get("action") if data.get("action") in RULE_ACTIONS else "alert",
        "max_age_days": num("max_age_days", 0, 3650, 0),
        "no_avatar": bool(data.get("no_avatar")),
        "timeout_minutes": num("timeout_minutes", 1, 40320, 60),
        "delete_days": delete_days if delete_days in RULE_DELETE_DAYS else 1,
        "reason": str(data.get("reason") or "").strip()[:200],
        "expires_at": (datetime.datetime.utcnow() + datetime.timedelta(hours=hours)).isoformat() if hours else "",
    }


def describe_rule(r: dict) -> str:
    """«бан — аккаунты младше 7 дн., без аватарки» — для лога."""
    action = RULE_ACTION_NAMES[r["action"]]
    if r["action"] == "timeout":
        from .guard import _fmt_minutes
        action += f" на {_fmt_minutes(r['timeout_minutes'])}"
    cond = []
    if r["max_age_days"]:
        cond.append(f"аккаунты младше {r['max_age_days']} дн.")
    if r["no_avatar"]:
        cond.append("без аватарки")
    return f"{action} — {', '.join(cond) if cond else 'все зашедшие'}"


async def save_invite_rule(guild, code: str, data: dict, by) -> dict:
    """Помечает инвайт или меняет правило. Счётчик срабатываний сохраняется.
    hours="keep" — оставить прежний срок действия."""
    r = clean_rule(data)
    if data.get("hours") == "keep":
        old = await get_invite_rule(guild.id, code)
        r["expires_at"] = old["expires_at"] if old else ""
    now = datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO invite_rules (guild_id, invite_code, action, max_age_days, no_avatar,
                timeout_minutes, delete_days, reason, expires_at, created_by, created_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            ON CONFLICT(guild_id, invite_code) DO UPDATE SET action=excluded.action,
                max_age_days=excluded.max_age_days, no_avatar=excluded.no_avatar,
                timeout_minutes=excluded.timeout_minutes, delete_days=excluded.delete_days,
                reason=excluded.reason, expires_at=excluded.expires_at,
                created_by=excluded.created_by, created_at=excluded.created_at""",
            (guild.id, code, r["action"], r["max_age_days"], int(r["no_avatar"]), r["timeout_minutes"],
             r["delete_days"], r["reason"], r["expires_at"], getattr(by, "id", 0), now))
        await db.commit()
    rule = await get_invite_rule(guild.id, code)
    until = f"до {rule['expires_at'][:16].replace('T', ' ')} UTC" if rule["expires_at"] else "бессрочно"
    await _rule_log(guild, "⚠️ Инвайт помечен подозрительным", code, by, [
        ("Правило", describe_rule(rule), False),
        ("Действует", until, True),
        *([("Причина", rule["reason"], True)] if rule["reason"] else []),
    ])
    return rule


async def delete_invite_rule(guild, code: str, by) -> bool:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("DELETE FROM invite_rules WHERE guild_id=? AND invite_code=?", (guild.id, code))
        await db.commit()
    if cur.rowcount:
        await _rule_log(guild, "✅ Пометка с инвайта снята", code, by, [])
    return bool(cur.rowcount)


async def _rule_log(guild, title, code, by, fields):
    ch = await get_log_ch(guild)
    if not ch:
        return
    e = build_embed(C.WARNING if fields else C.MUTED)
    e.set_author(name=title)
    e.add_field(name="Код", value=f"`{code}`", inline=True)
    e.add_field(name="Кто", value=getattr(by, "mention", str(by)), inline=True)
    for name, value, inline in fields:
        e.add_field(name=name, value=value, inline=inline)
    try:
        await ch.send(embed=e)
    except discord.HTTPException:
        pass


async def apply_invite_rule(member, code: str, certain: bool) -> bool:
    """Правило для инвайта, по которому зашёл участник. True — участник удалён.
    certain=False — бот не уверен в инвайте (несколько входов разом, разовая ссылка):
    тогда только оповещение, чтобы не наказать человека за чужую ссылку."""
    if not code or member.bot:
        return False
    guild = member.guild
    rule = await get_invite_rule(guild.id, code)
    if not rule or rule["expired"]:
        return False
    age = (datetime.datetime.utcnow() - member.created_at.replace(tzinfo=None)).days
    if rule["max_age_days"] and age >= rule["max_age_days"]:
        return False
    if rule["no_avatar"] and member.avatar is not None:
        return False
    from .database import get_protection
    from .core import is_trusted
    from .guard import punish, report
    cfg = await get_protection(guild.id)
    if await is_trusted(guild, member.id, cfg):
        return False
    why = [f"зашёл по ссылке `{code}`", f"аккаунту {age} дн."]
    if rule["no_avatar"]:
        why.append("нет аватарки")
    reason = f"Witness, подозрительный инвайт {code}" + (f": {rule['reason']}" if rule["reason"] else "")
    if certain:
        done, undo = await punish(member, rule["action"], cfg, reason[:500], minutes=rule["timeout_minutes"],
                                  delete_days=rule["delete_days"])
    else:
        done, undo = ("Ничего не сделано: бот не уверен, что участник зашёл именно по этой ссылке "
                      "(несколько входов одновременно или разовая ссылка). Проверьте вручную."), ""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE invite_rules SET hits=hits+1, last_hit=? WHERE guild_id=? AND invite_code=?",
                         (datetime.datetime.utcnow().isoformat(), guild.id, code))
        await db.commit()
    await report(guild, "⚠️ Вход по подозрительному инвайту", [
        ("Участник", f"{member.mention} (`{member}`)", True),
        ("Почему", ", ".join(why), True),
        ("Правило", describe_rule(rule), False),
        *([("Причина пометки", rule["reason"], False)] if rule["reason"] else []),
        ("Что сделано", done, False),
    ], color=0xED4245 if certain else 0xFEE75C, cfg=cfg, undo=(undo, member.id))
    return done.startswith(("бан", "кик"))
