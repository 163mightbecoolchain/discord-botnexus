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
    code = code.strip().removeprefix("https://discord.gg/").removeprefix("discord.gg/")

    async with aiosqlite.connect(DB_PATH) as db:
        if note:
            # Обновляем note во всех записях с этим кодом
            await db.execute(
                "UPDATE invite_log SET note=? WHERE guild_id=? AND invite_code=?",
                (note, gid, code)
            )
            # Если записей ещё нет — вставляем placeholder
            async with db.execute(
                "SELECT COUNT(*) FROM invite_log WHERE guild_id=? AND invite_code=?", (gid, code)
            ) as c:
                count = (await c.fetchone())[0]
            if count == 0:
                await db.execute(
                    "INSERT INTO invite_log (guild_id, invite_code, note) VALUES (?,?,?)",
                    (gid, code, note)
                )
            await db.commit()
            e = discord.Embed(title="📝 Заметка сохранена", color=0x57F287)
            e.add_field(name="Код",      value=f"`{code}`",             inline=True)
            e.add_field(name="Заметка",  value=note,                    inline=True)
            e.add_field(name="Добавил",  value=interaction.user.mention, inline=True)
        else:
            await db.execute(
                "UPDATE invite_log SET note='' WHERE guild_id=? AND invite_code=?", (gid, code)
            )
            await db.commit()
            e = discord.Embed(title="🗑️ Заметка удалена", description=f"Код: `{code}`", color=0x36393F)

    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="invnotes", description="Список всех заметок к инвайтам")
async def invnotes(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно право Manage Server.", ephemeral=True)
    gid = interaction.guild_id

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT invite_code, note, MAX(joined_at) as last_use, COUNT(*) as uses
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
            WHERE guild_id=? AND invite_code IS NOT NULL
            GROUP BY invite_code
            ORDER BY uses DESC
            LIMIT 5
        """, (gid,)) as c:
            code_rows = await c.fetchall()

        # Всего вошло
        async with db.execute(
            "SELECT COUNT(*) FROM invite_log WHERE guild_id=?", (gid,)
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
