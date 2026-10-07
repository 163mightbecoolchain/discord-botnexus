"""
Модерация: варны, муты, баны, карантин, апелляции, заявки на бан, наказания.
"""

import discord
from discord import app_commands
import aiosqlite
import os, asyncio, random, time, json, datetime
from datetime import timedelta
from .config import DB_PATH, TIER_PREMIUM
from .database import (
    add_modlog,
    add_warning,
    get_log_ch,
    get_punishment_settings,
    get_security,
    get_warnings,
    remove_warning,
    save_security,
    set_guild_setting,
    set_punishment_settings,
)
from .ui import (
    bar,
    build_embed,
    C,
    PaginatedView,
)
from .core import (
    antinuke_check,
    bot,
    get_tier,
    resolve_member,
    _suppress_next_timeout_dm,
    upsell_embed,
)

async def apply_progressive_punishment(member: discord.Member, warn_count: int,
                                        mod_id: int, silent_dm: bool = False) -> str:
    """
    Автоматическое наказание по количеству варнов.
    Читает warn3_type из punishment_settings:
      "ban"     → авто-бан (дефолт)
      "mute"    → авто-мут как 2й варн
      "request" → заявка на бан, решение за администратором
    """
    gid = member.guild.id
    ps  = await get_punishment_settings(gid)
    label = ""

    try:
        if warn_count in (1, 2):
            days  = ps["mute1_days"] if warn_count == 1 else ps["mute2_days"]
            until = datetime.datetime.now(datetime.timezone.utc) + timedelta(days=days)
            if silent_dm:
                _suppress_next_timeout_dm.add((gid, member.id))
            await member.timeout(until, reason=f"Auto: {warn_count} warnings")
            await add_modlog(gid, member.id, mod_id, "AUTO_MUTE", f"Auto: {warn_count} warns", f"{days}d")
            label = f"🔇 Таймаут на {days} дней"

        elif warn_count >= 3:
            w3 = ps.get("warn3_type", "ban")

            if w3 == "mute":
                days  = ps["mute2_days"]
                until = datetime.datetime.now(datetime.timezone.utc) + timedelta(days=days)
                if silent_dm:
                    _suppress_next_timeout_dm.add((gid, member.id))
                await member.timeout(until, reason="Auto: 3 warnings")
                await add_modlog(gid, member.id, mod_id, "AUTO_MUTE", "Auto: 3 warns", f"{days}d")
                label = f"🔇 Таймаут на {days} дней (3й варн)"

            elif w3 == "request":
                reason = "Накоплено 3 варна"
                await create_ban_request(member.guild, member, mod_id, reason, warn_count)
                label = "⚖️ Создана заявка на бан"

            else:
                ban_days = ps.get("ban3_days", 30)
                unban_at = (datetime.datetime.utcnow() + timedelta(days=ban_days)).isoformat()
                if not silent_dm:
                    try:
                        await send_appeal_dm(member, member.guild, "TEMPBAN",
                            f"Авто-бан на {ban_days} дней · накоплено {warn_count} варнов")
                    except Exception:
                        pass
                await member.ban(reason="Auto: 3 warnings")
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("""
                        INSERT INTO temp_bans (guild_id, user_id, mod_id, reason, unban_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(guild_id, user_id) DO UPDATE SET
                            unban_at=excluded.unban_at, unbanned=0, reason=excluded.reason
                    """, (gid, member.id, mod_id, "Auto: 3 warnings", unban_at))
                    await db.commit()
                label = f"🔨 Бан на {ban_days} дней"
                await add_modlog(gid, member.id, mod_id, "AUTO_TEMPBAN", "Auto: 3 warns", f"{ban_days}d")

    except discord.Forbidden:
        pass
    return label



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  APPEAL SYSTEM
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━




async def create_ban_request(guild: discord.Guild, member: discord.Member,
                              mod_id: int, reason: str, warn_count: int) -> int:
    """Создаёт заявку на бан и уведомляет администраторов с кнопками."""
    now = datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            INSERT INTO ban_requests (guild_id, user_id, username, mod_id, reason, warn_count, created_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (guild.id, member.id, str(member), mod_id, reason, warn_count, now))
        request_id = cur.lastrowid
        await db.commit()

    # Находим канал для уведомления
    ch = await get_log_ch(guild)
    if not ch:
        return request_id

    mod = guild.get_member(mod_id)
    e = build_embed(C.DANGER)
    e.set_author(name=f"⚖️ Заявка на бан #{request_id}",
                  icon_url=member.display_avatar.url)
    e.set_thumbnail(url=member.display_avatar.url)
    e.add_field(name="Участник",  value=f"{member.mention} (`{member}`)", inline=True)
    e.add_field(name="Варнов",    value=f"**{warn_count}/3**",           inline=True)
    e.add_field(name="Инициатор", value=mod.mention if mod else str(mod_id), inline=True)
    e.add_field(name="Причина",   value=reason,                          inline=False)
    e.set_footer(text="Администратор должен одобрить или отклонить бан")

    view = BanRequestView(request_id, member.id, guild.id)
    try:
        await ch.send(embed=e, view=view)
    except Exception:
        pass

    return request_id

class _BanRequestActions:
    """Общая логика одобрения/отклонения заявки на бан.
    Вынесена отдельно чтобы её использовали и DynamicItem-кнопки."""

    @staticmethod
    async def approve(interaction: discord.Interaction,
                      request_id: int, user_id: int, guild_id: int):
        if not interaction.user.guild_permissions.ban_members:
            return await interaction.response.send_message("❌ Нужно право Ban Members.", ephemeral=True)
        await interaction.response.defer()
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT status, reason FROM ban_requests WHERE id=?", (request_id,)
            ) as c:
                row = await c.fetchone()
        if not row or row[0] != "pending":
            return await interaction.followup.send("Заявка уже рассмотрена.", ephemeral=True)

        reason  = row[1]
        guild   = interaction.guild
        member  = await resolve_member(guild, user_id)
        ps      = await get_punishment_settings(guild_id)
        ban_days = ps.get("ban3_days", 30)
        unban_at = (datetime.datetime.utcnow() + timedelta(days=ban_days)).isoformat()

        ban_ok = False
        try:
            if member:
                try:
                    await send_appeal_dm(member, guild, "TEMPBAN",
                                          f"Бан на {ban_days} дней · {reason}")
                except Exception:
                    pass
            user = member or await bot.fetch_user(user_id)
            await guild.ban(user, reason=f"[Заявка #{request_id}] {reason}")
            ban_ok = True
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("""
                    INSERT INTO temp_bans (guild_id, user_id, mod_id, reason, unban_at)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(guild_id, user_id) DO UPDATE SET
                        unban_at=excluded.unban_at, unbanned=0, reason=excluded.reason
                """, (guild_id, user_id, interaction.user.id, reason, unban_at))
                await db.commit()
        except discord.Forbidden:
            pass

        now = datetime.datetime.utcnow().isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                UPDATE ban_requests SET status='approved', reviewer_id=?, reviewed_at=?
                WHERE id=?
            """, (interaction.user.id, now, request_id))
            await db.commit()

        await add_modlog(guild_id, user_id, interaction.user.id,
                         "AUTO_TEMPBAN", reason, f"{ban_days}d")

        new_e = build_embed(C.SUCCESS)
        new_e.set_author(name=f"✅ Бан одобрен · #{request_id}")
        new_e.add_field(name="Одобрил", value=interaction.user.mention, inline=True)
        new_e.add_field(name="Бан",     value=f"{ban_days} дней {'✅' if ban_ok else '⚠️ нет прав'}", inline=True)
        try:
            await interaction.message.edit(embed=new_e, view=None)
        except Exception:
            pass
        await interaction.followup.send(
            f"✅ Бан на **{ban_days} дней** {'выдан' if ban_ok else 'НЕ выдан (нет прав)'}.",
            ephemeral=True
        )

    @staticmethod
    async def reject(interaction: discord.Interaction,
                     request_id: int, user_id: int, guild_id: int):
        if not interaction.user.guild_permissions.ban_members:
            return await interaction.response.send_message("❌ Нужно право Ban Members.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT status FROM ban_requests WHERE id=?", (request_id,)
            ) as c:
                row = await c.fetchone()
        if not row or row[0] != "pending":
            return await interaction.response.send_message("Заявка уже рассмотрена.", ephemeral=True)

        now = datetime.datetime.utcnow().isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                UPDATE ban_requests SET status='rejected', reviewer_id=?, reviewed_at=?
                WHERE id=?
            """, (interaction.user.id, now, request_id))
            await db.commit()

        new_e = build_embed(C.MUTED)
        new_e.set_author(name=f"❌ Бан отклонён · #{request_id}")
        new_e.add_field(name="Отклонил", value=interaction.user.mention, inline=True)
        try:
            await interaction.message.edit(embed=new_e, view=None)
        except Exception:
            pass
        await interaction.response.send_message("Заявка отклонена.", ephemeral=True)


class BanApproveButton(discord.ui.DynamicItem[discord.ui.Button],
                        template=r"banreq_ok:(?P<rid>\d+):(?P<uid>\d+):(?P<gid>\d+)"):
    """Persistent-кнопка одобрения — переживает рестарт бота"""
    def __init__(self, request_id: int, user_id: int, guild_id: int):
        self.request_id = request_id
        self.user_id    = user_id
        self.guild_id   = guild_id
        super().__init__(discord.ui.Button(
            label="✅ Одобрить бан", style=discord.ButtonStyle.danger,
            custom_id=f"banreq_ok:{request_id}:{user_id}:{guild_id}"))

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(int(match["rid"]), int(match["uid"]), int(match["gid"]))

    async def callback(self, interaction: discord.Interaction):
        await _BanRequestActions.approve(interaction, self.request_id, self.user_id, self.guild_id)


class BanRejectButton(discord.ui.DynamicItem[discord.ui.Button],
                       template=r"banreq_no:(?P<rid>\d+):(?P<uid>\d+):(?P<gid>\d+)"):
    """Persistent-кнопка отклонения — переживает рестарт бота"""
    def __init__(self, request_id: int, user_id: int, guild_id: int):
        self.request_id = request_id
        self.user_id    = user_id
        self.guild_id   = guild_id
        super().__init__(discord.ui.Button(
            label="❌ Отклонить", style=discord.ButtonStyle.secondary,
            custom_id=f"banreq_no:{request_id}:{user_id}:{guild_id}"))

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(int(match["rid"]), int(match["uid"]), int(match["gid"]))

    async def callback(self, interaction: discord.Interaction):
        await _BanRequestActions.reject(interaction, self.request_id, self.user_id, self.guild_id)


class BanRequestView(discord.ui.View):
    """View с persistent-кнопками заявки на бан"""
    def __init__(self, request_id: int, user_id: int, guild_id: int):
        super().__init__(timeout=None)
        self.add_item(BanApproveButton(request_id, user_id, guild_id))
        self.add_item(BanRejectButton(request_id, user_id, guild_id))


class AppealModal(discord.ui.Modal, title="Подать апелляцию"):
    """Discord Modal для ввода апелляции"""
    def __init__(self, guild_id: int, action_type: str, original_reason: str = ""):
        super().__init__()
        self.guild_id        = guild_id
        self.action_type     = action_type
        self.original_reason = original_reason

    appeal_text = discord.ui.TextInput(
        label="Почему ты считаешь наказание несправедливым?",
        style=discord.TextStyle.long,
        max_length=1500,
        required=True,
        placeholder="Опиши ситуацию со своей стороны..."
    )
    why_change = discord.ui.TextInput(
        label="Что изменишь чтобы это не повторилось?",
        style=discord.TextStyle.long,
        max_length=500,
        required=False,
        placeholder="Необязательно, но улучшит шансы..."
    )

    async def on_submit(self, interaction: discord.Interaction):
        now = datetime.datetime.utcnow().isoformat()
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                # Проверяем нет ли pending апелляции от этого юзера
                async with db.execute(
                    "SELECT id FROM appeals WHERE guild_id=? AND user_id=? AND status='pending'",
                    (self.guild_id, interaction.user.id)
                ) as c:
                    existing = await c.fetchone()
                if existing:
                    return await interaction.response.send_message(
                        "У тебя уже есть активная апелляция. Дождись решения.",
                        ephemeral=True
                    )
                cur = await db.execute("""
                    INSERT INTO appeals
                        (guild_id, user_id, username, action_type, original_reason,
                         appeal_text, why_change, submitted_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (self.guild_id, interaction.user.id, str(interaction.user),
                      self.action_type, self.original_reason,
                      str(self.appeal_text), str(self.why_change), now))
                appeal_id = cur.lastrowid
                await db.commit()

            await interaction.response.send_message(
                f"✅ Апелляция #{appeal_id} отправлена. Модераторы рассмотрят её и ответят.",
                ephemeral=True
            )

            # Уведомляем модераторов в лог-канале
            guild = bot.get_guild(self.guild_id)
            if guild:
                ch = await get_log_ch(guild)
                if ch:
                    e = build_embed(C.WARNING)
                    e.set_author(name=f"📩 Новая апелляция #{appeal_id}")
                    e.add_field(name="Участник",      value=f"{interaction.user.mention} (`{interaction.user}`)", inline=False)
                    e.add_field(name="Тип наказания", value=self.action_type, inline=True)
                    if self.original_reason:
                        e.add_field(name="Изначальная причина", value=self.original_reason[:200], inline=False)
                    e.add_field(name="Объяснение", value=str(self.appeal_text)[:1000], inline=False)
                    if str(self.why_change).strip():
                        e.add_field(name="Что изменит", value=str(self.why_change)[:500], inline=False)
                    e.set_footer(text=f"/appeal accept {appeal_id} · /appeal reject {appeal_id} <причина>")
                    await ch.send(embed=e)
        except Exception as ex:
            print(f"[APPEAL] Submit error: {ex}")
            try:
                await interaction.response.send_message(
                    "Ошибка при отправке апелляции. Попробуй позже.", ephemeral=True
                )
            except Exception:
                pass


class AppealButton(discord.ui.DynamicItem[discord.ui.Button],
                   template=r"appeal:(?P<guild_id>\d+):(?P<action>\w+)"):
    """
    Persistent-кнопка апелляции. Данные закодированы в custom_id,
    поэтому кнопка работает даже после перезапуска бота
    (обычный View с timeout=None теряет callbacks при рестарте —
    из-за этого люди получали "ошибка взаимодействия").
    Причина наказания подтягивается из modlog при клике.
    """
    def __init__(self, guild_id: int, action_type: str):
        self.guild_id    = guild_id
        self.action_type = action_type
        super().__init__(discord.ui.Button(
            label="Подать апелляцию",
            style=discord.ButtonStyle.primary,
            emoji="📩",
            custom_id=f"appeal:{guild_id}:{action_type}",
        ))

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(int(match["guild_id"]), match["action"])

    async def callback(self, interaction: discord.Interaction):
        # Причину берём из последней записи modlog
        reason = ""
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("""
                    SELECT reason FROM modlog
                    WHERE guild_id=? AND user_id=?
                    ORDER BY id DESC LIMIT 1
                """, (self.guild_id, interaction.user.id)) as c:
                    row = await c.fetchone()
            if row and row[0]:
                reason = row[0]
        except Exception:
            pass
        modal = AppealModal(self.guild_id, self.action_type, reason)
        await interaction.response.send_modal(modal)


class AppealButtonView(discord.ui.View):
    """View-обёртка для persistent кнопки апелляции"""
    def __init__(self, guild_id: int, action_type: str, original_reason: str = ""):
        super().__init__(timeout=None)
        self.add_item(AppealButton(guild_id, action_type))


async def send_appeal_dm(member, guild, action_type: str, reason: str = ""):
    """Отправляет DM участнику с кнопкой подать апелляцию"""
    try:
        action_labels = {
            "BAN":     "🔨 Перманентный бан",
            "TEMPBAN": "⏱️ Временный бан",
            "KICK":    "👢 Кик",
            "MUTE":    "🔇 Мут",
            "WARN":    "⚠️ Предупреждение",
        }
        label = action_labels.get(action_type, action_type)

        # Вызывающий код передаёт строку вида "Тайм-аут на 2 часа · причина".
        # Длительность выносим в заголовок, в блоке причины оставляем только саму причину.
        raw = (reason or "").strip()
        prefix, clean = "", raw
        if " · " in raw:
            head, tail = raw.split(" · ", 1)
            prefix, clean = head.strip(), tail.strip()
        elif raw.startswith(("Тайм-аут", "Мут на", "Бан на", "Авто-бан",
                             "Временный бан", "Кик")):
            prefix, clean = raw, ""

        # Из фразы вида «Тайм-аут на 2 часа» берём только «2 часа»,
        # чтобы в заголовке не было «Мут (Тайм-аут на 2 часа)»
        duration_part = ""
        if " на " in prefix:
            duration_part = prefix.split(" на ", 1)[1].strip()

        head_line = f"{label} ({duration_part})" if duration_part else label

        e = build_embed(C.DANGER if action_type in ("BAN", "TEMPBAN", "KICK") else C.WARNING)
        e.set_author(
            name=f"Наказание · {guild.name}",
            icon_url=guild.icon.url if guild.icon else None
        )
        # Причина — главное в сообщении. Раньше она терялась среди полей,
        # и люди писали в апелляциях «не понимаю за что».
        if clean:
            e.description = (
                f"### {head_line}\n\n"
                f"**Причина наказания:**\n"
                f">>> {clean[:900]}"
            )
        else:
            e.description = (
                f"### {head_line}\n\n"
                f"**Причина наказания:**\n"
                f">>> *Модератор не указал причину. "
                f"Уточнить её можно в апелляции.*"
            )
        e.add_field(
            name="\u200b",
            value=("**Не согласен с наказанием?**\n"
                   "Нажми кнопку ниже и опиши свою позицию — "
                   "модераторы рассмотрят обращение."),
            inline=False
        )
        e.set_footer(text=f"{guild.name} · сохрани это сообщение, оно понадобится для апелляции")
        view = AppealButtonView(guild.id, action_type, reason)
        await member.send(embed=e, view=view)
        return True
    except (discord.Forbidden, discord.HTTPException):
        return False


async def notify_appeal_result(user_id: int, guild_id: int, appeal_id: int,
                                 accepted: bool, reviewer_note: str = ""):
    """Уведомляет участника о решении по апелляции"""
    user = bot.get_user(user_id)
    if not user:
        try:
            user = await bot.fetch_user(user_id)
        except Exception:
            return False
    guild = bot.get_guild(guild_id)
    guild_name = guild.name if guild else "сервере"
    try:
        if accepted:
            e = build_embed(C.SUCCESS)
            e.set_author(name=f"Апелляция #{appeal_id} принята")
            e.description = f"Твоя апелляция на {guild_name} **принята**. Наказание снято."
        else:
            e = build_embed(C.DANGER)
            e.set_author(name=f"Апелляция #{appeal_id} отклонена")
            e.description = f"Твоя апелляция на {guild_name} **отклонена**."
        if reviewer_note:
            e.add_field(name="Комментарий модератора", value=reviewer_note[:500], inline=False)
        await user.send(embed=e)
        return True
    except (discord.Forbidden, discord.HTTPException):
        return False


@bot.tree.command(name="warn", description="Выдать варн [Premium]")
@app_commands.describe(member="Пользователь", reason="Причина")
async def warn(interaction: discord.Interaction, member: discord.Member, reason: str = "Не указана"):
    if not interaction.user.guild_permissions.moderate_members:
        return await interaction.response.send_message("❌ Нужно Moderate Members.", ephemeral=True)
    await add_warning(interaction.guild_id, member.id, interaction.user.id, reason)
    warns = await get_warnings(interaction.guild_id, member.id)
    warn_count = len(warns)
    color = C.WARNING if warn_count < 3 else C.DANGER
    e = build_embed(color)
    e.set_author(name=f"Предупреждение выдано · {member.display_name}", icon_url=member.display_avatar.url)
    e.set_thumbnail(url=member.display_avatar.url)
    e.add_field(name="Участник",     value=member.mention,           inline=True)
    e.add_field(name="Модератор",    value=interaction.user.mention, inline=True)
    e.add_field(name="Варнов всего", value=f"**{warn_count}/3**",    inline=True)
    e.add_field(name="Причина",      value=reason,                   inline=False)
    await interaction.response.send_message(embed=e)

    # Записываем в modlog
    await add_modlog(interaction.guild_id, member.id, interaction.user.id, "WARN", reason)

    # Применяем прогрессивное наказание ДО отправки DM —
    # чтобы объединить варн+мут/бан в одно сообщение
    punishment_label = await apply_progressive_punishment(
        member, warn_count, interaction.user.id, silent_dm=True
    )

    # Единое DM участнику: варн (+ наказание если применилось)
    try:
        dm_e = build_embed(color)
        dm_e.set_author(name=f"Предупреждение на {interaction.guild.name}",
                        icon_url=interaction.guild.icon.url if interaction.guild.icon else None)
        dm_e.add_field(name="Причина",      value=reason,                inline=False)
        dm_e.add_field(name="Варнов всего", value=f"**{warn_count}/3**", inline=True)
        if punishment_label:
            dm_e.add_field(name="Наказание", value=punishment_label, inline=True)
        dm_e.set_footer(text="Не согласен? Нажми кнопку ниже чтобы подать апелляцию")
        appeal_view = AppealButtonView(interaction.guild_id, "WARN", reason)
        await member.send(embed=dm_e, view=appeal_view)
    except (discord.Forbidden, discord.HTTPException):
        pass  # DM закрыты

@bot.tree.command(name="warnings", description="Список варнов [Premium]")
@app_commands.describe(member="Пользователь")
async def warnings(interaction: discord.Interaction, member: discord.Member):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    rows = await get_warnings(interaction.guild_id, member.id)
    color = C.DANGER if len(rows) >= 3 else C.WARNING if rows else C.SUCCESS
    e = build_embed(color, thumbnail=member.display_avatar.url)
    e.set_author(name=f"Варны: {member.display_name}", icon_url=member.display_avatar.url)
    if not rows:
        e.description = "Варнов нет."
    else:
        warn_bar = bar(len(rows), 3, 8)
        e.add_field(name="Всего", value=f"**{len(rows)}/3** `{warn_bar}`", inline=False)
        for wid, mod_id, reason, created in rows:
            mod = interaction.guild.get_member(mod_id)
            e.add_field(
                name=f"#{wid} · {created[:10]}",
                value=f"Модератор: {mod.mention if mod else mod_id}\nПричина: {reason}",
                inline=False
            )
    await interaction.response.send_message(embed=e, ephemeral=True)

MUTE_DURATIONS = {
    "2 часа":  timedelta(hours=2),   "3 часа":  timedelta(hours=3),
    "4 часа":  timedelta(hours=4),   "5 часов": timedelta(hours=5),
    "6 часов": timedelta(hours=6),
    "2 дня":   timedelta(days=2),    "3 дня":   timedelta(days=3),
    "4 дня":   timedelta(days=4),    "5 дней":  timedelta(days=5),
    "6 дней":  timedelta(days=6),    "7 дней":  timedelta(days=7),
    "14 дней": timedelta(days=14),   "21 день": timedelta(days=21),
    "27 дней": timedelta(days=27),   "28 дней": timedelta(days=28),
}


@bot.tree.command(name="mute", description="Выдать таймаут участнику")
@app_commands.describe(member="Участник", duration="Длительность", reason="Причина")
@app_commands.choices(duration=[
    app_commands.Choice(name=k, value=k) for k in MUTE_DURATIONS
])
async def mute_cmd(interaction: discord.Interaction, member: discord.Member,
                   duration: str, reason: str = "Не указана"):
    if not interaction.user.guild_permissions.moderate_members:
        return await interaction.response.send_message("❌ Нужно Moderate Members.", ephemeral=True)

    delta = MUTE_DURATIONS.get(duration)
    if not delta:
        return await interaction.response.send_message(
            "❌ Неверная длительность. Выбери из списка.", ephemeral=True)

    try:
        # Анти-нюк проверка
        if await antinuke_check(interaction.guild, interaction.user.id, "mute"):
            return await interaction.response.send_message(
                "⚠️ Слишком много мутов за короткое время. Подожди немного.", ephemeral=True)
        # Подавляем on_member_update DM — сами отправим с нужным текстом
        _suppress_next_timeout_dm.add((interaction.guild_id, member.id))
        await member.timeout(delta, reason=reason)
        await add_modlog(interaction.guild_id, member.id, interaction.user.id,
                         "MUTE", reason, duration)
        # DM участнику
        try:
            dm_reason = f"Тайм-аут на {duration}" + (f" · {reason}" if reason != "Не указана" else "")
            await send_appeal_dm(member, interaction.guild, "MUTE", dm_reason)
        except Exception:
            pass

        e = build_embed(C.WARNING)
        e.set_author(name=f"Mute — {member.display_name}", icon_url=member.display_avatar.url)
        e.add_field(name="Участник",     value=member.mention,           inline=True)
        e.add_field(name="Длительность", value=f"**{duration}**",        inline=True)
        e.add_field(name="Модератор",    value=interaction.user.mention, inline=True)
        e.add_field(name="Причина",      value=reason,                   inline=False)
        await interaction.response.send_message(embed=e)
    except discord.Forbidden:
        _suppress_next_timeout_dm.discard((interaction.guild_id, member.id))
        await interaction.response.send_message("❌ Нет прав выдать таймаут.", ephemeral=True)


@bot.tree.command(name="unmute", description="Снять мьют с участника")
@app_commands.describe(member="Участник", reason="Причина")
async def unmute(interaction: discord.Interaction,
                 member: discord.Member, reason: str = "Снятие мьюта"):
    if not interaction.user.guild_permissions.moderate_members:
        return await interaction.response.send_message("❌ Нужно Moderate Members.", ephemeral=True)
    try:
        await member.timeout(None, reason=reason)
        await add_modlog(interaction.guild_id, member.id,
                         interaction.user.id, "UNMUTE", reason)
        e = discord.Embed(color=0x57F287, timestamp=datetime.datetime.utcnow())
        e.set_author(name=f"Unmuted — {member.display_name}",
                     icon_url=member.display_avatar.url)
        e.add_field(name="Member",    value=member.mention,          inline=True)
        e.add_field(name="By",        value=interaction.user.mention, inline=True)
        e.add_field(name="Reason",    value=reason,                  inline=False)
        await interaction.response.send_message(embed=e)
        # DM участнику
        try:
            dm_e = discord.Embed(color=0x57F287, timestamp=datetime.datetime.utcnow())
            dm_e.set_author(name=f"Unmuted in {interaction.guild.name}")
            dm_e.add_field(name="Reason", value=reason, inline=False)
            await member.send(embed=dm_e)
        except discord.Forbidden:
            pass
    except discord.Forbidden:
        await interaction.response.send_message("❌ Нет прав снять мьют.", ephemeral=True)


@bot.tree.command(name="clearwarn", description="Снять варн [Premium]")
@app_commands.describe(warn_id="ID варна")
async def clearwarn(interaction: discord.Interaction, warn_id: int):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.moderate_members:
        return await interaction.response.send_message("❌ Нужно Moderate Members.", ephemeral=True)
    await remove_warning(warn_id, interaction.guild_id)
    await interaction.response.send_message(f"✅ Варн `#{warn_id}` снят.", ephemeral=True)

@bot.tree.command(name="purge", description="Удалить N сообщений [Premium]")
@app_commands.describe(count="Количество (1-100)")
async def purge(interaction: discord.Interaction, count: int):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.manage_messages:
        return await interaction.response.send_message("❌ Нужно Manage Messages.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    deleted = await interaction.channel.purge(limit=max(1, min(count, 100)))
    await interaction.followup.send(f"🗑️ Удалено **{len(deleted)}** сообщений.", ephemeral=True)



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  /appeal команды
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="banrequests", description="Заявки на бан — просмотр и рассмотрение")
@app_commands.describe(status="pending (по умолчанию) / approved / rejected / all")
async def banrequests_cmd(interaction: discord.Interaction, status: str = "pending"):
    if not interaction.user.guild_permissions.ban_members:
        return await interaction.response.send_message("❌ Нужно право Ban Members.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)

    gid = interaction.guild_id
    async with aiosqlite.connect(DB_PATH) as db:
        if status == "all":
            async with db.execute("""
                SELECT id, user_id, username, mod_id, reason, warn_count, status, created_at
                FROM ban_requests WHERE guild_id=? ORDER BY id DESC LIMIT 20
            """, (gid,)) as c:
                rows = await c.fetchall()
        else:
            async with db.execute("""
                SELECT id, user_id, username, mod_id, reason, warn_count, status, created_at
                FROM ban_requests WHERE guild_id=? AND status=? ORDER BY id DESC LIMIT 20
            """, (gid, status)) as c:
                rows = await c.fetchall()

    if not rows:
        return await interaction.followup.send(
            f"Заявок со статусом **{status}** нет.", ephemeral=True)

    icons = {"pending": "⏳", "approved": "✅", "rejected": "❌"}
    e = build_embed(C.PRIMARY)
    e.set_author(name=f"⚖️ Заявки на бан · {status}")
    lines = []
    for rid, uid, uname, mid, reason, wc, st, ts in rows:
        ic = icons.get(st, "•")
        lines.append(f"{ic} **#{rid}** · `{uname}` · {reason[:40]} · *{ts[:10]}*")
    e.description = "\n".join(lines)
    e.set_footer(text="Активные заявки с кнопками — в лог-канале")
    await interaction.followup.send(embed=e, ephemeral=True)


@bot.tree.command(name="appeal", description="Апелляции и заявки на бан")
@app_commands.describe(action="submit / list / view / accept / reject / banrequests",
                       appeal_id="ID апелляции или заявки на бан",
                       note="Комментарий (для accept/reject)")
async def appeal_cmd(interaction: discord.Interaction,
                     action: str = "submit",
                     appeal_id: int = 0,
                     note: str = ""):
    gid = interaction.guild_id

    # ── BANREQUESTS — заявки на бан ────────────────────────────
    if action == "banrequests":
        if not interaction.user.guild_permissions.ban_members:
            return await interaction.response.send_message("❌ Нужно право Ban Members.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT id, user_id, username, reason, status, created_at
                FROM ban_requests WHERE guild_id=? AND status='pending'
                ORDER BY id DESC LIMIT 20
            """, (gid,)) as c:
                rows = await c.fetchall()
        if not rows:
            return await interaction.followup.send("✅ Нет активных заявок на бан.", ephemeral=True)
        e = build_embed(C.DANGER)
        e.set_author(name="⚖️ Заявки на бан — ожидают решения")
        lines = []
        for rid, uid, uname, reason, status, ts in rows:
            m = interaction.guild.get_member(uid)
            name = m.display_name if m else uname
            lines.append(f"⏳ **#{rid}** · `{name}` · {reason[:40]} · *{ts[:10]}*")
        e.description = "\n".join(lines)
        e.set_footer(text="Кнопки для одобрения/отклонения находятся в лог-канале")
        return await interaction.followup.send(embed=e, ephemeral=True)

    # ── SUBMIT — открываем модал ───────────────────────────────
    if action == "submit":
        # Проверяем что у участника есть активное наказание
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT action, reason FROM modlog
                WHERE guild_id=? AND user_id=?
                  AND action IN ('BAN','KICK','MUTE','WARN','TEMPBAN','AUTO_MUTE','AUTO_KICK','AUTO_BAN')
                ORDER BY id DESC LIMIT 1
            """, (gid, interaction.user.id)) as c:
                last = await c.fetchone()

        action_type = last[0].replace("AUTO_", "") if last else "WARN"
        reason      = last[1] if last and last[1] else ""

        modal = AppealModal(gid, action_type, reason)
        return await interaction.response.send_modal(modal)

    # Остальные действия требуют прав модератора
    if not interaction.user.guild_permissions.manage_messages:
        return await interaction.response.send_message(
            "❌ Нужно право `Manage Messages` для управления апелляциями.",
            ephemeral=True
        )

    # ── LIST — список апелляций ────────────────────────────────
    if action == "list":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT id, user_id, username, action_type, status, submitted_at
                FROM appeals WHERE guild_id=?
                ORDER BY
                    CASE status WHEN 'pending' THEN 0 WHEN 'accepted' THEN 1
                                WHEN 'rejected' THEN 2 ELSE 3 END,
                    id DESC
                LIMIT 25
            """, (gid,)) as c:
                rows = await c.fetchall()

        e = build_embed(C.PRIMARY)
        e.set_author(name=f"📋 Апелляции · {interaction.guild.name}")
        if not rows:
            e.description = "Апелляций пока нет."
            return await interaction.response.send_message(embed=e, ephemeral=True)

        icons = {"pending":"⏳","accepted":"✅","rejected":"❌","expired":"⌛"}
        lines = []
        for aid, uid, uname, atype, status, ts in rows:
            ic = icons.get(status, "•")
            lines.append(f"{ic} **#{aid}** · `{uname}` · {atype} · *{ts[:10]}*")
        e.description = "\n".join(lines)
        e.set_footer(text="/appeal view <id> — детали · /appeal accept <id> · /appeal reject <id>")
        return await interaction.response.send_message(embed=e, ephemeral=True)

    # ── VIEW — детали апелляции ────────────────────────────────
    if action == "view":
        if not appeal_id:
            return await interaction.response.send_message("Укажи `appeal_id`.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT user_id, username, action_type, original_reason,
                       appeal_text, why_change, status, reviewer_id, reviewer_note,
                       submitted_at, reviewed_at
                FROM appeals WHERE id=? AND guild_id=?
            """, (appeal_id, gid)) as c:
                r = await c.fetchone()
        if not r:
            return await interaction.response.send_message("Апелляция не найдена.", ephemeral=True)

        uid, uname, atype, orig_reason, atext, why, status, rev_id, rev_note, sub_at, rev_at = r
        member = interaction.guild.get_member(uid)
        color  = {"pending":C.WARNING,"accepted":C.SUCCESS,
                  "rejected":C.DANGER,"expired":C.MUTED}.get(status, C.PRIMARY)
        e = build_embed(color)
        e.set_author(name=f"Апелляция #{appeal_id} · {status}")
        e.add_field(name="Участник", value=f"{member.mention if member else uname} (`{uname}`)", inline=True)
        e.add_field(name="Наказание", value=atype, inline=True)
        e.add_field(name="Подана",   value=sub_at[:10], inline=True)
        if orig_reason:
            e.add_field(name="Изначальная причина", value=orig_reason[:300], inline=False)
        e.add_field(name="Объяснение от участника", value=atext[:1000], inline=False)
        if why:
            e.add_field(name="Что изменит", value=why[:500], inline=False)
        if status != "pending":
            reviewer = interaction.guild.get_member(rev_id)
            e.add_field(
                name="Рассмотрено",
                value=f"{reviewer.mention if reviewer else rev_id} — *{rev_at[:10]}*",
                inline=False
            )
            if rev_note:
                e.add_field(name="Комментарий", value=rev_note[:500], inline=False)
        return await interaction.response.send_message(embed=e, ephemeral=True)

    # ── ACCEPT — принять + снять наказание ─────────────────────
    if action == "accept":
        if not appeal_id:
            return await interaction.response.send_message("Укажи `appeal_id`.", ephemeral=True)
        await interaction.response.defer(ephemeral=True)

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT user_id, action_type, status FROM appeals WHERE id=? AND guild_id=?",
                (appeal_id, gid)
            ) as c:
                r = await c.fetchone()
        if not r:
            return await interaction.followup.send("Апелляция не найдена.", ephemeral=True)
        uid, atype, status = r
        if status != "pending":
            return await interaction.followup.send(
                f"Апелляция уже {status}.", ephemeral=True
            )

        # Автоматическое снятие наказания
        unbanned_msg = ""
        try:
            if atype in ("BAN", "TEMPBAN"):
                try:
                    user = await bot.fetch_user(uid)
                    await interaction.guild.unban(user, reason=f"Апелляция #{appeal_id} принята")
                    unbanned_msg = " · разбанен"
                    # Снимаем активный tempban
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute(
                            "UPDATE temp_bans SET unbanned=1 WHERE guild_id=? AND user_id=? AND unbanned=0",
                            (gid, uid)
                        )
                        await db.commit()
                except discord.NotFound:
                    unbanned_msg = " · уже разбанен"
                except discord.Forbidden:
                    unbanned_msg = " · ⚠️ нет прав разбанить"
            elif atype == "MUTE":
                member = await resolve_member(interaction.guild, uid)
                if member and member.is_timed_out():
                    try:
                        await member.timeout(None, reason=f"Апелляция #{appeal_id}")
                        unbanned_msg = " · мут снят"
                    except discord.Forbidden:
                        unbanned_msg = " · ⚠️ нет прав снять мут"
                # Мут снят по апелляции — уведомление об окончании не нужно
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE active_mutes SET notified=1 WHERE guild_id=? AND user_id=?",
                        (gid, uid))
                    await db.commit()
            elif atype == "WARN":
                # Снимаем последний варн
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("""
                        DELETE FROM warnings WHERE id IN (
                            SELECT id FROM warnings
                            WHERE guild_id=? AND user_id=?
                            ORDER BY id DESC LIMIT 1
                        )
                    """, (gid, uid))
                    await db.commit()
                unbanned_msg = " · варн снят"
        except Exception as ex:
            print(f"[APPEAL] Auto-unpunish error: {ex}")

        now = datetime.datetime.utcnow().isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                UPDATE appeals SET status='accepted', reviewer_id=?,
                                    reviewer_note=?, reviewed_at=?
                WHERE id=? AND guild_id=?
            """, (interaction.user.id, note, now, appeal_id, gid))
            await db.commit()

        await notify_appeal_result(uid, gid, appeal_id, True, note)
        await add_modlog(gid, uid, interaction.user.id, "APPEAL_ACCEPTED", f"#{appeal_id}: {note}")
        await interaction.followup.send(
            f"✅ Апелляция #{appeal_id} принята{unbanned_msg}.", ephemeral=True
        )
        return

    # ── REJECT — отклонить ─────────────────────────────────────
    if action == "reject":
        if not appeal_id:
            return await interaction.response.send_message("Укажи `appeal_id`.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT user_id, status FROM appeals WHERE id=? AND guild_id=?",
                (appeal_id, gid)
            ) as c:
                r = await c.fetchone()
        if not r:
            return await interaction.response.send_message("Апелляция не найдена.", ephemeral=True)
        uid, status = r
        if status != "pending":
            return await interaction.response.send_message(
                f"Апелляция уже {status}.", ephemeral=True
            )

        now = datetime.datetime.utcnow().isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                UPDATE appeals SET status='rejected', reviewer_id=?,
                                    reviewer_note=?, reviewed_at=?
                WHERE id=? AND guild_id=?
            """, (interaction.user.id, note, now, appeal_id, gid))
            await db.commit()

        await notify_appeal_result(uid, gid, appeal_id, False, note)
        await add_modlog(gid, uid, interaction.user.id, "APPEAL_REJECTED", f"#{appeal_id}: {note}")
        return await interaction.response.send_message(
            f"❌ Апелляция #{appeal_id} отклонена.", ephemeral=True
        )

    return await interaction.response.send_message(
        "Действие: `submit`, `list`, `view`, `accept`, `reject`.", ephemeral=True
    )

@bot.tree.command(name="muteboard", description="Лидерборд по времени в муте")
@app_commands.describe(page="Страница (по умолчанию 1)")
async def muteboard(interaction: discord.Interaction, page: int = 1):
    gid = interaction.guild_id
    offset = (page - 1) * 10

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id, total_seconds, mute_count
            FROM mute_log
            WHERE guild_id=?
            ORDER BY total_seconds DESC
            LIMIT 10 OFFSET ?
        """, (gid, offset)) as c:
            rows = await c.fetchall()
        async with db.execute(
            "SELECT COUNT(*) FROM mute_log WHERE guild_id=?", (gid,)
        ) as c:
            total = (await c.fetchone())[0]

    e = build_embed(C.DANGER)
    e.set_author(name=f"Mute Leaderboard · Стр. {page}")

    medals = ["🥇","🥈","🥉"] + [f"**{i}.**" for i in range(4, 11)]

    if not rows:
        e.description = "Никто ещё не получал таймаут на этом сервере. 🎉"
    else:
        lines = []
        for i, (uid, secs, count) in enumerate(rows):
            m = interaction.guild.get_member(uid)
            name = m.display_name if m else str(uid)
            # Форматируем длительность
            minutes = secs // 60
            hours   = minutes // 60
            days    = hours // 24
            months  = days // 30
            if months >= 1:
                dur = f"{months}мес {days % 30}д"
            elif days >= 1:
                dur = f"{days}д {hours % 24}ч"
            elif hours >= 1:
                dur = f"{hours}ч {minutes % 60}мин"
            else:
                dur = f"{minutes}мин"
            lines.append(
                f"{medals[i]} **{name}** — "
                f"⏱ {dur} · {count} раз{'а' if count in (2,3,4) else ''}"
            )
        e.description = "\n".join(lines)

    pages = (total + 9) // 10
    e.set_footer(text=f"Witness · Страница {page}/{pages} · Всего {total} участников")
    await interaction.response.send_message(embed=e)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🔒 SECURITY EXTRA: LOCKDOWN / SLOWMODE / AUTOBAN / REPORT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="lockdown", description="Режим локдауна — запрет входа новых участников [Premium]")
@app_commands.describe(action="on / off", min_age="Минимальный возраст аккаунта в днях (по умолчанию 7)")
async def lockdown(interaction: discord.Interaction, action: str = "on", min_age: int = 7):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ Нужны права администратора.", ephemeral=True)
    enabled = action.lower() in ("on", "вкл", "yes", "1")
    await set_guild_setting(interaction.guild_id, "lockdown", 1 if enabled else 0)
    if enabled:
        e = discord.Embed(title="🔒 ЛОКДАУН ВКЛЮЧЁН", color=0xFF0000)
        e.add_field(name="Статус", value="Новые участники с аккаунтом младше **{} дней** будут автоматически кикнуты".format(min_age), inline=False)
        e.add_field(name="Выключить", value="`/lockdown action:off`", inline=False)
        # Store min_age in settings
        _, settings = await get_security(interaction.guild_id)
        settings["lockdown_min_age"] = min_age
        log_ch_id, _ = await get_security(interaction.guild_id)
        await save_security(interaction.guild_id, log_ch_id, settings)
    else:
        e = discord.Embed(title="🔓 Локдаун выключен", color=0x00FF9D)
        e.description = "Новые участники снова могут заходить свободно."
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="slowmode", description="Установить slow mode в канале [Premium]")
@app_commands.describe(seconds="Задержка в секундах (0 = выключить, макс 21600)")
async def slowmode(interaction: discord.Interaction, seconds: int = 0):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.manage_channels:
        return await interaction.response.send_message("❌ Нужно Manage Channels.", ephemeral=True)
    seconds = max(0, min(seconds, 21600))
    await interaction.channel.edit(slowmode_delay=seconds)
    if seconds == 0:
        await interaction.response.send_message("✅ Slow mode выключен.")
    else:
        await interaction.response.send_message(f"✅ Slow mode: **{seconds} сек** между сообщениями.")

@bot.tree.command(name="report", description="Пожаловаться на сообщение модераторам [Premium]")
@app_commands.describe(message_id="ID сообщения", reason="Причина жалобы")
async def report(interaction: discord.Interaction, message_id: str, reason: str = "Не указана"):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    ch = await get_log_ch(interaction.guild)
    if not ch:
        return await interaction.response.send_message("❌ Канал логов не настроен. Используй `/security setlog`", ephemeral=True)
    try:
        msg_id = int(message_id)
        msg = await interaction.channel.fetch_message(msg_id)
        e = discord.Embed(title="🚨 Жалоба на сообщение", color=0xFF4444, timestamp=datetime.datetime.utcnow())
        e.add_field(name="От кого", value=interaction.user.mention, inline=True)
        e.add_field(name="Автор сообщения", value=msg.author.mention, inline=True)
        e.add_field(name="Канал", value=interaction.channel.mention, inline=True)
        e.add_field(name="Причина", value=reason, inline=False)
        e.add_field(name="Содержимое", value=msg.content[:500] or "*(вложение/эмбед)*", inline=False)
        e.add_field(name="Ссылка", value=f"[Перейти]({msg.jump_url})", inline=True)
        await ch.send(embed=e)
        await interaction.response.send_message("✅ Жалоба отправлена модераторам.", ephemeral=True)
    except (ValueError, discord.NotFound):
        await interaction.response.send_message("❌ Сообщение не найдено. Убедись что ID правильный.", ephemeral=True)


@bot.tree.command(name="tempban", description="Временный бан участника")
@app_commands.describe(
    member="Участник",
    duration="Длительность: 1h / 12h / 1d / 7d / 30d",
    reason="Причина"
)
async def tempban(interaction: discord.Interaction, member: discord.Member,
                  duration: str = "1d", reason: str = "Нарушение правил"):
    if not interaction.user.guild_permissions.ban_members:
        return await interaction.response.send_message("❌ Нет прав.", ephemeral=True)

    # Парсим длительность
    unit_map = {"h": 1, "d": 24, "w": 168}
    try:
        num = int(duration[:-1])
        unit = duration[-1].lower()
        hours = num * unit_map.get(unit, 24)
    except Exception:
        return await interaction.response.send_message("❌ Формат: `1h` `12h` `1d` `7d` `30d`", ephemeral=True)

    unban_at = (datetime.datetime.utcnow() + timedelta(hours=hours)).isoformat()

    try:
        # Анти-нюк проверка
        if await antinuke_check(interaction.guild, interaction.user.id, "ban"):
            return await interaction.response.send_message(
                "⚠️ Слишком много банов за короткое время. Подожди немного.", ephemeral=True)
        # DM с кнопкой апелляции — до бана, иначе бот не сможет написать
        try:
            await send_appeal_dm(member, interaction.guild, "TEMPBAN",
                                  f"Временный бан на {duration} · {reason}")
        except Exception: pass
        await member.ban(reason=f"[Tempban {duration}] {reason}")
    except discord.Forbidden:
        return await interaction.response.send_message("❌ Нет прав забанить.", ephemeral=True)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO temp_bans (guild_id,user_id,mod_id,reason,unban_at) VALUES (?,?,?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET unban_at=excluded.unban_at,unbanned=0,reason=excluded.reason",
            (interaction.guild_id, member.id, interaction.user.id, reason, unban_at)
        )
        await db.commit()

    await add_modlog(interaction.guild_id, member.id, interaction.user.id, "TEMPBAN", reason, duration)

    e = discord.Embed(color=0xED4245, timestamp=datetime.datetime.utcnow())
    e.set_author(name=f"Tempban — {member.display_name}", icon_url=member.display_avatar.url)
    e.add_field(name="Member",    value=member.mention,           inline=True)
    e.add_field(name="Duration",  value=f"**{duration}**",        inline=True)
    e.add_field(name="Unban at",  value=unban_at[:16],            inline=True)
    e.add_field(name="Reason",    value=reason,                   inline=False)
    await interaction.response.send_message(embed=e)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  MODLOG
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="modlog", description="История действий над участником")
@app_commands.describe(member="Участник")
async def modlog_cmd(interaction: discord.Interaction, member: discord.Member):
    if not interaction.user.guild_permissions.manage_messages:
        return await interaction.response.send_message("❌ Нет прав.", ephemeral=True)

    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT action,reason,duration,mod_id,created_at FROM modlog "
            "WHERE guild_id=? AND user_id=? ORDER BY id DESC LIMIT 50",
            (interaction.guild_id, member.id)
        ) as c:
            rows = await c.fetchall()

    if not rows:
        e = discord.Embed(color=0xFEE75C, timestamp=datetime.datetime.utcnow())
        e.set_author(name=f"Modlog — {member.display_name}", icon_url=member.display_avatar.url)
        e.description = "No moderation history."
        return await interaction.response.send_message(embed=e, ephemeral=True)

    chunks = [rows[i:i+5] for i in range(0, len(rows), 5)]
    pages  = []
    for chunk in chunks:
        pe = discord.Embed(color=0xFEE75C, timestamp=datetime.datetime.utcnow())
        pe.set_author(name=f"Modlog — {member.display_name}", icon_url=member.display_avatar.url)
        pe.set_thumbnail(url=member.display_avatar.url)
        for action, reason, duration, mod_id, created_at in chunk:
            mod     = interaction.guild.get_member(mod_id)
            mod_str = mod.display_name if mod else ("Auto" if mod_id == 0 else str(mod_id))
            dur_str = f" · {duration}" if duration else ""
            pe.add_field(
                name=f"`{action}`{dur_str} · {created_at[:10]}",
                value=f"{reason or '—'} · by {mod_str}",
                inline=False
            )
        pages.append(pe)

    if len(pages) == 1:
        await interaction.response.send_message(embed=pages[0], ephemeral=True)
    else:
        view = PaginatedView(pages)
        await interaction.response.send_message(embed=pages[0], view=view, ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  КАРАНТИН НАСТРОЙКИ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="quarantine", description="Настройка карантина для новых аккаунтов")
@app_commands.describe(
    action="setup / enable / disable / release",
    role="Карантинная роль",
    hours="Длительность карантина в часах (по умолчанию 24)",
    min_age="Минимальный возраст аккаунта в днях (по умолчанию 7)",
    member="Участник для release"
)
async def quarantine_cmd(interaction: discord.Interaction,
                          action: str,
                          role: discord.Role = None,
                          hours: int = 24,
                          min_age: int = 7,
                          member: discord.Member = None):
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ Нужны права администратора.", ephemeral=True)

    gid = interaction.guild_id

    if action == "setup":
        if not role:
            return await interaction.response.send_message("❌ Укажи роль: `/quarantine action:setup role:@Quarantine`", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO quarantine_settings (guild_id,role_id,duration_hours,min_age_days,enabled)
                VALUES (?,?,?,?,1)
                ON CONFLICT(guild_id) DO UPDATE SET
                    role_id=excluded.role_id, duration_hours=excluded.duration_hours,
                    min_age_days=excluded.min_age_days, enabled=1
            """, (gid, role.id, hours, min_age))
            await db.commit()
        e = discord.Embed(color=0x57F287, timestamp=datetime.datetime.utcnow())
        e.set_author(name="Quarantine configured")
        e.add_field(name="Role",     value=role.mention,      inline=True)
        e.add_field(name="Duration", value=f"**{hours}h**",   inline=True)
        e.add_field(name="Min age",  value=f"**{min_age} days**", inline=True)
        return await interaction.response.send_message(embed=e)

    if action in ("enable", "disable"):
        enabled = action == "enable"
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE quarantine_settings SET enabled=? WHERE guild_id=?",
                (1 if enabled else 0, gid)
            )
            await db.commit()
        status = "✅ Карантин включён" if enabled else "🔒 Карантин выключен"
        return await interaction.response.send_message(status)

    if action == "release":
        if not member:
            return await interaction.response.send_message("❌ Укажи участника.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT role_id FROM quarantine_settings WHERE guild_id=?", (gid,)
            ) as c:
                row = await c.fetchone()
        if row and row[0]:
            qrole = interaction.guild.get_role(row[0])
            if qrole and qrole in member.roles:
                await member.remove_roles(qrole, reason=f"Released by {interaction.user}")
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "UPDATE quarantine SET released=1 WHERE guild_id=? AND user_id=?",
                (gid, member.id)
            )
            await db.commit()
        e = discord.Embed(color=0x57F287, timestamp=datetime.datetime.utcnow())
        e.set_author(name=f"Released from quarantine — {member.display_name}")
        return await interaction.response.send_message(embed=e)

    await interaction.response.send_message("❌ Действие: `setup` `enable` `disable` `release`", ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PUNISHMENT SETTINGS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="punishments", description="Настройка наказаний за варны")
@app_commands.describe(
    action="view / set",
    mute1_days="Дней таймаута за 1-й варн (1-27)",
    mute2_days="Дней таймаута за 2-й варн (1-27)",
    ban3_days="Дней бана за 3-й варн",
    warn3_type="Действие на 3-м варне: ban / mute / request"
)
@app_commands.choices(warn3_type=[
    app_commands.Choice(name="🔨 Бан (авто)",           value="ban"),
    app_commands.Choice(name="🔇 Мут (как 2-й варн)",   value="mute"),
    app_commands.Choice(name="⚖️ Заявка на бан (вручную)", value="request"),
])
async def punishments_cmd(interaction: discord.Interaction,
                           action: str = "view",
                           mute1_days: int = None,
                           mute2_days: int = None,
                           ban3_days: int = None,
                           warn3_type: str = None):
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ Нужны права администратора.", ephemeral=True)

    gid = interaction.guild_id

    if action == "set":
        if mute1_days is not None and not (1 <= mute1_days <= 27):
            return await interaction.response.send_message(
                "❌ mute1_days: 1–27 (лимит Discord).", ephemeral=True)
        if mute2_days is not None and not (1 <= mute2_days <= 27):
            return await interaction.response.send_message(
                "❌ mute2_days: 1–27.", ephemeral=True)
        if ban3_days is not None and ban3_days < 1:
            return await interaction.response.send_message(
                "❌ ban3_days должен быть больше 0.", ephemeral=True)
        if warn3_type and warn3_type not in ("ban", "mute", "request"):
            return await interaction.response.send_message(
                "❌ warn3_type: ban / mute / request", ephemeral=True)
        cfg = await set_punishment_settings(gid, mute1_days, mute2_days, ban3_days, warn3_type)
    else:
        cfg = await get_punishment_settings(gid)

    w3_labels = {
        "ban":     f"🔨 Авто-бан **{cfg['ban3_days']}** дн.",
        "mute":    f"🔇 Мут как 2-й варн (**{cfg['mute2_days']}** дн.)",
        "request": "⚖️ Заявка на бан (одобряет администратор)",
    }
    e = build_embed(C.PRIMARY)
    e.set_author(name="⚖️ Прогрессивные наказания")
    e.add_field(name="1-й варн", value=f"🔇 Таймаут **{cfg['mute1_days']}** дн.", inline=True)
    e.add_field(name="2-й варн", value=f"🔇 Таймаут **{cfg['mute2_days']}** дн.", inline=True)
    e.add_field(name="3-й варн", value=w3_labels.get(cfg.get("warn3_type","ban"), "?"), inline=True)
    if cfg.get("warn3_type") == "request":
        e.add_field(name="Заявки на бан",
                    value="Смотри `/appeal action:banrequests` или `/banrequests`",
                    inline=False)
    e.set_footer(text="/punishments action:set warn3_type:request — сменить режим")
    await interaction.response.send_message(embed=e)
