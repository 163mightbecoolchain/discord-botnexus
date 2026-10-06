"""
Админ-команды: язык, премиум, бэкапы, /security, /setup.
"""

import discord
from discord import app_commands
import aiosqlite
import os, asyncio, random, time, json, datetime
from datetime import timedelta
from .config import (
    DB_PATH,
    TIER_COLORS,
    TIER_FREE,
    TIER_NAMES,
    TIER_PREMIUM,
)
from .database import (
    get_guild_settings,
    get_security,
    save_security,
    SEC_NAMES,
    set_guild_setting,
    set_tier,
)
from .i18n import _guild_lang
from .ui import (
    build_embed,
    C,
    make_embed,
    tier_badge,
)
from .core import (
    bot,
    get_tier,
    OWNER_IDS,
    upsell_embed,
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ADMIN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


@bot.tree.command(name="lang", description="Set bot language / Установить язык бота")
@app_commands.describe(language="ru — Русский | en — English")
async def lang_cmd(interaction: discord.Interaction, language: str = "ru"):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Manage Server permission required.", ephemeral=True)
    lang = language.lower().strip()
    if lang not in ("ru", "en"):
        return await interaction.response.send_message("❌ Available: `ru` or `en`", ephemeral=True)
    _guild_lang[interaction.guild_id] = lang
    # Сохраняем в БД
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO guild_settings (guild_id, lang) VALUES (?, ?)
            ON CONFLICT(guild_id) DO UPDATE SET lang=excluded.lang
        """, (interaction.guild_id, lang))
        await db.commit()
    if lang == "en":
        e = make_embed(
            title="Language set to English 🇬🇧",
            description="All bot responses will now be in **English**.\nUse `/lang language:ru` to switch back.",
            color=C.SUCCESS
        )
    else:
        e = make_embed(
            title="Язык изменён на Русский 🇷🇺",
            description="Все ответы бота теперь на **русском** языке.\nИспользуй `/lang language:en` для переключения.",
            color=C.SUCCESS
        )
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="dbstats", description="[ADMIN] Размер базы и таблиц")
async def dbstats(interaction: discord.Interaction):
    if interaction.user.id not in OWNER_IDS and not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ Нет доступа.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    try:
        size_mb = os.path.getsize(DB_PATH) / 1024 / 1024
        rows = []
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT name FROM sqlite_master WHERE type='table' "
                "AND name NOT LIKE 'sqlite_%' ORDER BY name") as c:
                tables = [r[0] for r in await c.fetchall()]
            for t in tables:
                try:
                    async with db.execute(f"SELECT COUNT(*) FROM {t}") as c:
                        rows.append((t, (await c.fetchone())[0]))
                except Exception:
                    pass
        rows.sort(key=lambda x: -x[1])

        e = build_embed(C.PRIMARY)
        e.set_author(name="🗄️ База данных")
        e.add_field(name="Размер файла", value=f"**{size_mb:.1f} МБ**", inline=True)
        e.add_field(name="Таблиц",       value=str(len(rows)),          inline=True)
        top = "\n".join(f"`{n:<22}` {c:>8,}".replace(",", " ")
                        for n, c in rows[:15] if c)
        e.add_field(name="Записей по таблицам", value=top or "—", inline=False)
        e.set_footer(text="Резервная копия отправляется раз в сутки в сжатом виде")
        await interaction.followup.send(embed=e, ephemeral=True)
    except Exception as ex:
        await interaction.followup.send(f"❌ Ошибка: {ex}", ephemeral=True)


@bot.tree.command(name="backupnow", description="[ADMIN] Сделать резервную копию сейчас")
async def backupnow(interaction: discord.Interaction):
    if interaction.user.id not in OWNER_IDS and not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ Нет доступа.", ephemeral=True)
    ch_id = int(os.getenv("BACKUP_CHANNEL_ID", "0") or 0)
    if not ch_id:
        return await interaction.response.send_message(
            "❌ BACKUP_CHANNEL_ID не задан.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    tmp, gz = "/tmp/manual_backup.db", "/tmp/manual_backup.db.gz"
    try:
        for f in (tmp, gz):
            if os.path.exists(f): os.remove(f)
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("PRAGMA wal_checkpoint(TRUNCATE)") as c:
                await c.fetchall()
            await db.execute("VACUUM INTO ?", (tmp,))
        def _gzip():
            import gzip, shutil
            with open(tmp,"rb") as fi, gzip.open(gz,"wb",compresslevel=9) as fo:
                shutil.copyfileobj(fi, fo, length=1024*1024)
        await asyncio.to_thread(_gzip)
        raw_mb = os.path.getsize(tmp)/1024/1024
        gz_mb  = os.path.getsize(gz)/1024/1024
        limit  = float(os.getenv("BACKUP_LIMIT_MB", "9"))
        if gz_mb > limit:
            return await interaction.followup.send(
                f"⚠️ Архив {gz_mb:.1f} МБ больше лимита {limit} МБ — "
                f"суточный бэкап отправит его частями.", ephemeral=True)
        ch = bot.get_channel(ch_id) or await bot.fetch_channel(ch_id)
        stamp = datetime.datetime.utcnow().strftime("%Y-%m-%d_%H%M")
        e = build_embed(C.PRIMARY)
        e.set_author(name="💾 Резервная копия (вручную)")
        e.add_field(name="База",     value=f"{raw_mb:.1f} МБ", inline=True)
        e.add_field(name="В архиве", value=f"{gz_mb:.1f} МБ",  inline=True)
        await ch.send(embed=e, file=discord.File(gz, f"witness_{stamp}.db.gz"))
        await interaction.followup.send(
            f"✅ Копия отправлена ({gz_mb:.1f} МБ).", ephemeral=True)
    except Exception as ex:
        await interaction.followup.send(f"❌ Ошибка: {ex}", ephemeral=True)
    finally:
        for f in (tmp, gz):
            try:
                if os.path.exists(f): os.remove(f)
            except Exception: pass


@bot.tree.command(name="setpremium", description="[ADMIN] Установить тир")
@app_commands.describe(tier="0=Free 1=Premium 2=Pro", days="Дней (0 = бессрочно)")
async def setpremium(interaction: discord.Interaction, tier: int, days: int = 0):
    if interaction.user.id not in OWNER_IDS and not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ Нет доступа.", ephemeral=True)
    if tier not in (0, 1, 2):
        return await interaction.response.send_message("❌ Тир: 0, 1 или 2", ephemeral=True)
    await set_tier(interaction.guild_id, tier, days)
    e = build_embed(C.SUCCESS if tier else C.MUTED)
    e.set_author(name="Тариф изменён")
    e.add_field(name="Тариф", value=f"**{TIER_NAMES.get(tier,'?')}**", inline=True)
    if days:
        until = (datetime.datetime.utcnow() + timedelta(days=days)).strftime("%d.%m.%Y")
        e.add_field(name="Действует до", value=until, inline=True)
        e.set_footer(text="Предупреждения придут за 7, 3 и 1 день до конца")
    else:
        e.add_field(name="Срок", value="бессрочно", inline=True)
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="sechelp", description="Advanced Security команды [/q] — только для администраторов")
async def sechelp(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message(
            embed=build_embed(C.DANGER, description="Admins only."),
            ephemeral=True
        )
    e = make_embed(
        title="🔐 Advanced Security — префикс `/q`",
        description=(
            "Расширенный модуль безопасности с AI анализом.\n"
            "Все команды доступны **только администраторам**."
        ),
        color=0x5865F2,
        footer="Witness Advanced Security"
    )
    commands_list = [
        ("/q scan @user",      "Сканирование: threat intel + подпись действия"),
        ("/q threat @user",    "Threat Intelligence: возраст, паттерны, impersonation, unicode spoofing"),
        ("/q nlp [текст]",     "NLP анализ токсичности, угроз и спама через AI"),
        ("/q forensics [id]",  "Криминалистика сообщения: хеш, EXIF изображений, дубли"),
        ("/q sig [id]",        "Проверить цифровую подпись модераторского действия"),
        ("/q alert",           "Последние алерты безопасности"),
        ("/q network",         "Статистика угроз и аномалий сервера"),
        ("/q whitelist @user", "Добавить в whitelist (исключить из проверок)"),
        ("/q blacklist @user", "Добавить в blacklist"),
        ("/q report @user",    "Отправить в глобальную базу угроз (между серверами)"),
        ("/q status",          "Статус всех систем: кэши, AI движок, HMAC"),
        ("/q help",            "Этот список прямо в чате"),
    ]
    for cmd, desc in commands_list:
        e.add_field(name=f"`{cmd}`", value=desc, inline=False)
    await interaction.response.send_message(embed=e, ephemeral=True)



    tier = await get_tier(interaction.guild_id)
    e = discord.Embed(title="📋 Подписка", color=TIER_COLORS[tier])
    e.add_field(name="Тир", value=TIER_NAMES[tier], inline=True)
    if tier == TIER_FREE: e.add_field(name="Апгрейд", value="witnessbot.gg/premium", inline=True)
    await interaction.response.send_message(embed=e)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SECURITY COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

sec_grp = app_commands.Group(name="security", description="🛡️ Безопасность сервера [Premium]")

@sec_grp.command(name="status", description="Статус всех модулей")
async def sec_status(interaction: discord.Interaction):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    log_ch, settings = await get_security(interaction.guild_id)
    ch_obj = interaction.guild.get_channel(log_ch)
    e = discord.Embed(title="🛡️ Security Status", color=0x00E5FF)
    e.add_field(name="Канал логов", value=ch_obj.mention if ch_obj else "не задан (/security setlog)", inline=False)
    on_lines = [f"✅ `{k}` — {n}" for k,n in SEC_NAMES.items() if settings.get(k,False)]
    off_lines = [f"❌ `{k}` — {n}" for k,n in SEC_NAMES.items() if not settings.get(k,False)]
    e.add_field(name="Включено", value="\n".join(on_lines) or "нет", inline=False)
    e.add_field(name="Выключено", value="\n".join(off_lines[:10]) or "нет", inline=False)
    await interaction.response.send_message(embed=e, ephemeral=True)

@sec_grp.command(name="toggle", description="Включить/выключить модуль")
@app_commands.describe(module="Название модуля")
async def sec_toggle(interaction: discord.Interaction, module: str):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if module not in SEC_NAMES:
        return await interaction.response.send_message(f"❌ Неизвестный модуль. Смотри `/security status`", ephemeral=True)
    log_ch, settings = await get_security(interaction.guild_id)
    settings[module] = not settings.get(module, False)
    await save_security(interaction.guild_id, log_ch, settings)
    state = "✅ включён" if settings[module] else "❌ выключен"
    await interaction.response.send_message(f"**{SEC_NAMES[module]}** (`{module}`) {state}", ephemeral=True)

@sec_grp.command(name="setlog", description="Задать канал логов")
@app_commands.describe(channel="Канал для логов")
async def sec_setlog(interaction: discord.Interaction, channel: discord.TextChannel):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    _, settings = await get_security(interaction.guild_id)
    await save_security(interaction.guild_id, channel.id, settings)
    await interaction.response.send_message(f"✅ Канал логов → {channel.mention}", ephemeral=True)

bot.tree.add_command(sec_grp)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ВРЕМЕННЫЙ БАН
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="setup", description="Пошаговая настройка Witness для сервера")
async def setup_cmd(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message("❌ Нужны права администратора.", ephemeral=True)

    guild = interaction.guild
    gid   = guild.id

    e = discord.Embed(color=0x5865F2, timestamp=datetime.datetime.utcnow())
    e.set_author(name=f"Witness Setup — {guild.name}",
                 icon_url=bot.user.display_avatar.url)
    e.description = "Быстрая настройка за 3 шага:"

    # Проверяем что уже настроено
    settings = await get_guild_settings(gid)
    log_ch_id, sec_settings = await get_security(gid)
    tier = await get_tier(gid)

    # Статус настроек
    items = [
        ("Лог-канал",       "✅" if log_ch_id else "❌", "/security setlog #channel"),
        ("Язык",            "✅", f"/lang language:{settings.get('lang','ru')}"),
        ("Тикеты",          "✅" if settings.get('ticket_category') else "❌", "/ticket action:setup"),
        ("Карантин",        "✅" if settings.get('quarantine_role') else "❌", "/quarantine action:setup role:@Role"),
        ("Reaction Roles",  "⚙️", "/reactionrole action:add"),
        ("Прогр. наказания","⚙️", "/punishments"),
        ("Подписка",        tier_badge(tier), "/subinfo"),
    ]

    for name, status, cmd_str in items:
        e.add_field(name=f"{status} {name}", value=f"`{cmd_str}`", inline=True)

    e.add_field(
        name="Следующий шаг",
        value=(
            "**Минимальная настройка:**\n"
            "1. `/security setlog #logs` — куда слать логи\n"
            "2. `/security toggle joins:on leaves:on bans:on`\n"
            "3. `/lang language:ru` — выбрать язык\n\n"
            "**Рекомендуется:**\n"
            "4. `/quarantine action:setup role:@Quarantine hours:24 min_age:7`\n"
            "5. `/punishments` — настроить авто-наказания\n"
            "6. `/ticket action:setup` — система тикетов"
        ),
        inline=False
    )
    e.add_field(
        name="Следующий шаг",
        value=(
            "**Минимальная настройка:**\n"
            "1. `/security setlog #logs` — куда слать логи\n"
            "2. `/security toggle joins:on leaves:on bans:on` — включить события\n"
            "3. `/lang language:ru` — выбрать язык\n\n"
            "**Рекомендуется:**\n"
            "4. `/quarantine action:setup role:@Quarantine hours:24 min_age:7`\n"
            "5. `/punishments` — настроить авто-наказания\n"
            "6. `/ticket action:setup` — система тикетов"
        ),
        inline=False
    )
    e.set_footer(text="Witness · /help для полного списка команд")

    await interaction.response.send_message(embed=e, ephemeral=True)
    # Помечаем что setup был показан
    await set_guild_setting(gid, "setup_done", 1)
