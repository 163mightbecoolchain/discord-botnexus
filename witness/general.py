"""
Общие команды: help, профиль, экономика, утилиты, тикеты, предложения, роли за реакции.
"""

import discord
from discord import app_commands
import aiohttp
import aiosqlite
import os, asyncio, random, time, json, datetime
from .config import (
    BOT_ID,
    DB_PATH,
    get_invite_url,
    SUPPORT_URL,
    TIER_COLORS,
    TIER_PREMIUM,
    WEATHER_KEY,
)
from .database import (
    get_coins,
    get_guild_settings,
    get_leaderboard,
    get_xp,
    set_guild_setting,
)
from .ui import (
    bar,
    build_embed,
    C,
    make_embed,
    tier_badge,
)
from .core import (
    ask_ai,
    bot,
    cooldown,
    get_tier,
    upsell_embed,
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  UI КОМПОНЕНТЫ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class HelpView(discord.ui.View):
    """Select Menu для /help — выбор раздела"""
    PAGES = {
        "general":  "📖 Основные",
        "albion":   "⚔️ Albion",
        "games":    "🎮 Игры",
        "security": "🛡️ Безопасность",
        "pro":      "💎 Pro",
        "ai":       "🤖 AI",
        "new":      "🆕 Новинки",
    }

    def __init__(self, current_page: str, guild_tier: int):
        super().__init__(timeout=120)
        self.current_page = current_page
        self.guild_tier = guild_tier

        select = discord.ui.Select(
            placeholder=f"Раздел: {self.PAGES.get(current_page, '?')}",
            options=[
                discord.SelectOption(
                    label=label,
                    value=key,
                    default=(key == current_page),
                    emoji=label.split()[0]
                )
                for key, label in self.PAGES.items()
            ]
        )
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        page = interaction.data["values"][0]
        embed = build_help_embed(page, self.guild_tier)
        await interaction.response.edit_message(embed=embed, view=HelpView(page, self.guild_tier))

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FREE COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━


def build_help_embed(page: str, guild_tier: int) -> discord.Embed:
    """Строит эмбед для /help по выбранному разделу"""
    PAGES = {
        "general": {
            "title": "📖 Основные команды",
            "color": C.PRIMARY,
            "fields": [
                ("🆓 Информация", "`/ping` `/userinfo` `/serverinfo` `/subinfo`"),
                ("🆓 XP и экономика", "`/rank` `/leaderboard` `/coins`"),
                ("🆓 Утилиты", "`/poll` (с таймером) · `/remind` (повторяющиеся) · `/lfg` (с Join кнопкой)\n`/weather` · `/translate`"),
                ("🆓 Комьюнити", "`/birthday` `/suggestion` `/ticket` (вкл/выкл)"),
                ("🆓 Albion", "`/register` — привязать ник · `/watch` — алерты на игрока"),
                ("⭐ Premium", "`/starboard` `/serverstats` `/giveaway` `/roast` `/summarize` `/imagine`"),
            ]
        },
        "albion": {
            "title": "⚔️ Albion Online",
            "color": C.INFO,
            "fields": [
                ("🆓 Игроки", "`/stats` `/kills` `/deaths` `/history` `/compare`"),
                ("🆓 Гильдии", "`/guild` `/battle` `/party`"),
                ("💎 Pro — Рынок", "`/blackmarket` — топ профитных предметов\n`/craftcalc` — таблица крафта → Google Sheets\n`/flipper` — арбитраж между городами"),
                ("💎 Pro — Прочее", "`/guildwar` `/pricewatch` `/askalbion`"),
            ]
        },
        "games": {
            "title": "🎮 Другие игры",
            "color": C.SUCCESS,
            "fields": [
                ("🆓 Minecraft", "`/mc [address]` — статус сервера"),
                ("🆓 Old School RuneScape", "`/rs [username]` — навыки игрока"),
                ("⭐ Valorant", "`/val [Name#TAG]` — ранг и RR"),
                ("⭐ CS2", "`/cs2 [steam_id]` — статистика"),
                ("⭐ League of Legends", "`/lol [summoner]` — ранг и WR"),
                ("⭐ Lost Ark", "`/lostark [character]` — item level"),
            ]
        },
        "security": {
            "title": "🛡️ Безопасность",
            "color": C.WARNING,
            "fields": [
                ("Настройка", "`/setup` — пошаговая настройка\n`/security status` · `toggle` · `setlog`"),
                ("Логирование", "`joins` `leaves` `bans` `timeouts` `msg_delete` `msg_edit`\n`invites` `suspicious` `nick_change` `role_change` `voice`"),
                ("Авто-защита", "`anti_raid` · `anti_spam` · `/lockdown` · `/slowmode`\n`/quarantine` — карантин для новых аккаунтов"),
                ("Модерация", "`/warn` `/unmute` `/tempban` `/warnings` `/clearwarn`\n`/purge` `/report` `/modlog` `/punishments`"),
                ("Инвайты", "`/invcheck` `/invuser` `/invdel` `/invnote` `/invnotes` `/invstats`"),
                ("Reaction Roles", "`/reactionrole add/remove/list/clear`"),
                ("🔐 Advanced Security — префикс `/q` (Security plan)",
                 "`/q scan` · `/q threat`\n"
                 "`/q nlp` · `/q forensics` · `/q sig` · `/q alert`\n"
                 "`/q network` · `/q status` · `/q help`\nПодробнее: `/sechelp`"),
            ]
        },
        "pro": {
            "title": "💎 Pro — €9.99/мес",
            "color": C.GOLD,
            "fields": [
                ("Чёрный рынок", "`/blackmarket category:weapon tier:8`\nКатегории: `weapon` `offhand` `armor_plate` `armor_leather` `armor_cloth` `bag`\nⓘ Нажми кнопку Select в ответе бота для выбора категории"),
                ("Крафт-калькулятор", "`/craftcalc tier:8 server:eu tax:8`\nЭкспорт всех предметов T6–T8 в Google Sheets"),
                ("Торговля", "`/flipper` — арбитраж между городами\n`/pricewatch` — алерты на изменение цены"),
                ("Статистика", "`/guildwar` · `/party` · `/tournament`"),
            ]
        },
        "new": {
            "title": "🆕 Новые команды",
            "color": C.SUCCESS,
            "fields": [
                ("🔐 Security", "`/tempban` `/unmute` `/modlog` `/punishments` `/quarantine`"),
                ("🎮 Albion", "`/register` `/watch add/remove/list`"),
                ("📨 Инвайты", "`/invnote` `/invnotes` `/invstats`"),
                ("⚙️ Настройка", "`/setup` `/reactionrole` `/ticket disable/enable`"),
                ("🤖 Утилиты", "`/remind` (повтор) · `/poll` (таймер) · `/lfg` (Join кнопка)"),
                ("📣 Бот", "`/invite` `/vote` `/botinfo`"),
            ]
        },
        "ai": {
            "title": "🤖 AI команды",
            "color": C.PREMIUM,
            "fields": [
                ("ⓘ AI движок", "Бот использует **Groq** (Llama 3.3 70B) и **Gemini** — бесплатно."),
                ("⭐ Текст", "`/ai` `/summarize` `/askalbion`"),
                ("⭐ Развлечения", "`/roast` — роаст участника\n`/imagine` — генерация изображений (Pollinations.ai)"),
                ("💎 Pro + AI", "`/party` — AI вердикт на состав группы"),
            ]
        },
    }

    if page not in PAGES:
        page = "general"

    data = PAGES[page]

    e = build_embed(data["color"])
    e.set_author(name=data["title"])
    e.set_footer(text=f"Plan: {tier_badge(guild_tier)} · witnessbot.gg")

    for name, val in data["fields"]:
        e.add_field(name=name, value=val, inline=False)

    return e


@bot.tree.command(name="help", description="Все команды бота")
@app_commands.describe(page="Раздел: general / albion / games / security / pro / ai")
async def help_cmd(interaction: discord.Interaction, page: str = "general"):
    tier = await get_tier(interaction.guild_id)
    embed = build_help_embed(page.lower(), tier)
    view = HelpView(page.lower(), tier)
    await interaction.response.send_message(embed=embed, view=view)




@bot.tree.command(name="ping")
async def ping(interaction: discord.Interaction):
    ms = round(bot.latency * 1000)
    color = C.SUCCESS if ms < 100 else C.WARNING if ms < 200 else C.DANGER
    quality = "Отлично" if ms < 100 else "Нормально" if ms < 200 else "Плохо"
    bar_str = bar(max(0, 200 - ms), 200, 10)
    e = build_embed(color)
    e.set_author(name="Witness · Pong!")
    e.add_field(name="Latency", value=f"**{ms}ms**", inline=True)
    e.add_field(name="Качество", value=quality, inline=True)
    e.add_field(name="Статус", value=f"`{bar_str}`", inline=True)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="userinfo")
@app_commands.describe(member="Пользователь")
async def userinfo(interaction: discord.Interaction, member: discord.Member = None):
    m = member or interaction.user
    e = discord.Embed(title=f"👤 {m.display_name}", color=0x00E5FF)
    e.set_thumbnail(url=m.display_avatar.url)
    e.add_field(name="ID", value=m.id, inline=True)
    e.add_field(name="Зашёл", value=m.joined_at.strftime("%d.%m.%Y"), inline=True)
    roles = [r.mention for r in m.roles[1:]]
    e.add_field(name=f"Роли ({len(roles)})", value=" ".join(roles) if roles else "нет", inline=False)
    xp = await get_xp(interaction.guild_id, m.id)
    coins = await get_coins(interaction.guild_id, m.id)
    e.add_field(name="XP/Ур.", value=f"{xp}/{xp//100}", inline=True)
    e.add_field(name="Монеты", value=str(coins), inline=True)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="serverinfo")
async def serverinfo(interaction: discord.Interaction):
    g     = interaction.guild
    tier  = await get_tier(g.id)
    bots  = sum(1 for m in g.members if m.bot)
    humans = g.member_count - bots
    age   = (datetime.datetime.utcnow() - g.created_at.replace(tzinfo=None)).days
    e = make_embed(
        color=TIER_COLORS[tier],
        thumbnail=g.icon.url if g.icon else "",
        footer=f"ID: {g.id}"
    )
    e.set_author(name=g.name, icon_url=g.icon.url if g.icon else None)
    e.add_field(name="Участники",  value=f"**{humans}** люди · {bots} боты",                    inline=True)
    e.add_field(name="Каналы",     value=f"**{len(g.text_channels)}** текст · {len(g.voice_channels)} голос", inline=True)
    e.add_field(name="Роли",       value=f"**{len(g.roles)}**",                                  inline=True)
    e.add_field(name="Буст",       value=f"Уровень **{g.premium_tier}** · {g.premium_subscription_count}×", inline=True)
    e.add_field(name="Возраст",    value=f"**{age}** дней",                                      inline=True)
    e.add_field(name="Witness",    value=tier_badge(tier),                                        inline=True)
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="rank")
async def rank(interaction: discord.Interaction):
    xp     = await get_xp(interaction.guild_id, interaction.user.id)
    coins  = await get_coins(interaction.guild_id, interaction.user.id)
    lvl    = xp // 100
    prog   = xp % 100
    bar_s  = bar(prog, 100, 12)
    e = build_embed(C.PRIMARY, thumbnail=interaction.user.display_avatar.url)
    e.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
    e.add_field(name="Уровень",  value=f"**{lvl}**",    inline=True)
    e.add_field(name="XP",       value=f"**{xp:,}**",   inline=True)
    e.add_field(name="Монеты",   value=f"**{coins:,}**", inline=True)
    e.add_field(
        name=f"До уровня {lvl+1} — {prog}/100 XP",
        value=f"`{bar_s}` **{prog}%**",
        inline=False
    )
    await interaction.response.send_message(embed=e)

@bot.tree.command(name="leaderboard")
async def leaderboard(interaction: discord.Interaction):
    rows = await get_leaderboard(interaction.guild_id)
    medals = ["🥇","🥈","🥉","4.","5.","6.","7.","8.","9.","10."]
    e = build_embed(C.GOLD)
    e.set_author(name="Топ активных участников")
    if not rows:
        e.description = "Нет данных."
    else:
        lines = []
        for i, (uid, xp) in enumerate(rows):
            m = interaction.guild.get_member(uid)
            name = m.display_name if m else str(uid)
            b = bar(xp % 100, 100, 6)
            lines.append(f"{medals[i]} **{name}** — {xp:,} XP · ур. {xp//100} `{b}`")
        e.description = "\n".join(lines)
    await interaction.response.send_message(embed=e)


@bot.tree.command(name="coins")
async def coins_cmd(interaction: discord.Interaction):
    c = await get_coins(interaction.guild_id, interaction.user.id)
    xp = await get_xp(interaction.guild_id, interaction.user.id)
    e = build_embed(C.GOLD)
    e.add_field(name="🪙 Монеты", value=f"**{c:,}**", inline=True)
    e.add_field(name="⚡ XP", value=f"**{xp:,}**", inline=True)
    e.add_field(name="🏆 Уровень", value=f"**{xp//100}**", inline=True)
    e.set_footer(text=f"Witness · +1 монета за каждое сообщение")
    await interaction.response.send_message(embed=e, ephemeral=True)

@bot.tree.command(name="remind")
@app_commands.describe(
    minutes="Через сколько минут (1-10080)",
    message="Текст напоминания",
    repeat="Повторять каждые N минут (0 = без повтора)"
)
async def remind(interaction: discord.Interaction,
                 minutes: int, message: str, repeat: int = 0):
    if not 1 <= minutes <= 10080:
        return await interaction.response.send_message("❌ Минуты: 1–10080.", ephemeral=True)
    fire_at = (datetime.datetime.utcnow() + datetime.timedelta(minutes=minutes)).isoformat()
    now     = datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO reminders "
            "(guild_id,user_id,channel_id,message,fire_at,repeat_mins,max_repeats,created_at) "
            "VALUES (?,?,?,?,?,?,?,?)",
            (interaction.guild_id, interaction.user.id, interaction.channel_id,
             message, fire_at, repeat, 20, now)
        )
        await db.commit()
    e = discord.Embed(color=0x00B0F4, timestamp=datetime.datetime.utcnow())
    e.set_author(name="Reminder set")
    e.add_field(name="Message", value=message[:200], inline=False)
    e.add_field(name="In",      value=f"**{minutes} min**", inline=True)
    e.add_field(name="Repeat",  value=f"Every {repeat} min" if repeat else "Once", inline=True)
    e.set_footer(text="Напомню в личку. Напоминания сохраняются при перезапуске.")
    await interaction.response.send_message(embed=e, ephemeral=True)


@bot.tree.command(name="poll")
@app_commands.describe(question="Вопрос", option1="Вариант 1", option2="Вариант 2", option3="Вариант 3", option4="Вариант 4", duration="Длительность в минутах (0 = без ограничения)")
async def poll(interaction: discord.Interaction, question: str, option1: str, option2: str, option3: str = None, option4: str = None, duration: int = 0):
    options = [o for o in [option1, option2, option3, option4] if o]
    emojis  = ["1️⃣","2️⃣","3️⃣","4️⃣"]
    e = discord.Embed(title=f"📊 {question}", color=0x00E5FF)
    for i, opt in enumerate(options):
        e.add_field(name=f"{emojis[i]} {opt}", value="​", inline=False)
    if duration > 0:
        e.set_footer(text=f"Закроется через {duration} мин")
    await interaction.response.send_message(embed=e)
    msg = await interaction.original_response()
    for i in range(len(options)):
        await msg.add_reaction(emojis[i])
    # Авто-закрытие
    if duration > 0:
        await asyncio.sleep(duration * 60)
        try:
            msg = await msg.channel.fetch_message(msg.id)
            results = [f"{r.emoji} — {r.count-1} голосов" for r in msg.reactions]
            result_e = discord.Embed(color=0xFEE75C, timestamp=datetime.datetime.utcnow())
            result_e.set_author(name=f"Poll closed — {question}")
            result_e.description = "\n".join(results) or "Нет голосов."
            await msg.channel.send(embed=result_e, reference=msg)
        except Exception:
            pass

@bot.tree.command(name="lfg")
@app_commands.describe(game="Игра", slots="Нужно игроков", note="Дополнительно")
async def lfg(interaction: discord.Interaction, game: str, slots: int = 1, note: str = ""):
    e = discord.Embed(title=f"🎮 LFG — {game}", color=0x00FF9D)
    e.description = f"**{interaction.user.display_name}** ищет **{slots}** игрока(-ов)"
    if note:
        e.add_field(name="📝", value=note, inline=False)
    e.set_footer(text="Нажми Join чтобы вступить · авто-удаление через 2 часа")

    class LFGJoinView(discord.ui.View):
        def __init__(self):
            super().__init__(timeout=7200)
            self.joined = [interaction.user]

        @discord.ui.button(label=f"Join (1/{slots})", style=discord.ButtonStyle.success)
        async def join_btn(self, inter: discord.Interaction, button: discord.ui.Button):
            if inter.user in self.joined:
                return await inter.response.send_message("Ты уже в группе.", ephemeral=True)
            self.joined.append(inter.user)
            button.label = f"Join ({len(self.joined)}/{slots})"
            if len(self.joined) >= slots:
                button.disabled = True
                button.label = f"Full ({len(self.joined)}/{slots})"
                try:
                    names = ", ".join(m.display_name for m in self.joined)
                    await interaction.user.send(f"✅ Группа собрана! Участники: {names}")
                except Exception:
                    pass
            await inter.response.edit_message(view=self)
            await inter.followup.send(f"✅ Вступил в группу!", ephemeral=True)

        async def on_timeout(self):
            try:
                msg = await interaction.original_response()
                await msg.delete()
            except Exception:
                pass

    await interaction.response.send_message(embed=e, view=LFGJoinView())

@bot.tree.command(name="weather")
@app_commands.describe(city="Город")
@cooldown(10)
async def weather(interaction: discord.Interaction, city: str):
    await interaction.response.defer()
    if not WEATHER_KEY: return await interaction.followup.send("❌ Функция временно недоступна.")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={WEATHER_KEY}&units=metric") as r:
                if r.status!=200: return await interaction.followup.send(f"❌ Город **{city}** не найден.")
                d = await r.json()
        desc = d["weather"][0]["description"].capitalize()
        temp = d["main"]["temp"]
        feels = d["main"]["feels_like"]
        humidity = d["main"]["humidity"]
        wind = d["wind"]["speed"]
        e = build_embed(C.INFO)
        e.set_author(name=f"{d['name']}, {d['sys']['country']}")
        e.add_field(name="Температура", value=f"**{temp:.1f}°C** (ощущается {feels:.1f}°C)", inline=True)
        e.add_field(name="Описание",    value=desc,                                          inline=True)
        e.add_field(name="Влажность",   value=f"**{humidity}%**",                            inline=True)
        e.add_field(name="Ветер",       value=f"**{wind} м/с**",                             inline=True)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="translate")
@app_commands.describe(text="Текст", to="Язык: en/de/ru/ua")
@cooldown(5)
async def translate(interaction: discord.Interaction, text: str, to: str = "en"):
    await interaction.response.defer()
    langs = {"en":"English","de":"German","ru":"Russian","ua":"Ukrainian"}
    target = langs.get(to.lower(),"English")
    try:
        result = await ask_ai(f"Translate to {target}. Reply ONLY with translation:\n\n{text}", system="Precise translator. Output only translated text.")
        e = discord.Embed(title=f"🌍 → {target}", color=0x00E5FF)
        e.add_field(name="Оригинал", value=text[:1024], inline=False)
        e.add_field(name="Перевод", value=result[:1024], inline=False)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="giveaway")
@app_commands.describe(prize="Приз", duration="Минут")
async def giveaway(interaction: discord.Interaction, prize: str, duration: int = 60):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    e = discord.Embed(title="🎉 РОЗЫГРЫШ", description=f"**Приз:** {prize}\n🎮 — участие\n⏰ **{duration} мин**", color=0x00FF9D)
    await interaction.response.send_message(embed=e)
    msg = await interaction.original_response(); await msg.add_reaction("🎮")
    await asyncio.sleep(duration*60)
    msg = await interaction.channel.fetch_message(msg.id)
    reaction = discord.utils.get(msg.reactions, emoji="🎮")
    users = [u async for u in reaction.users() if not u.bot]
    winner = random.choice(users) if users else None
    await interaction.channel.send(f"🎊 {winner.mention} выиграл **{prize}**!" if winner else "😢 Никто не участвовал.")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🎂 BIRTHDAYS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="birthday", description="Зарегистрировать день рождения")
@app_commands.describe(action="set / check / setchannel", date="Дата в формате ДД.ММ (напр. 25.12)", member="Участник для /birthday check")
async def birthday(interaction: discord.Interaction, action: str = "set", date: str = "", member: discord.Member = None):
    if action.lower() == "setchannel":
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message("❌ Нужно Manage Server.", ephemeral=True)
        await set_guild_setting(interaction.guild_id, "birthday_channel", interaction.channel_id)
        return await interaction.response.send_message(f"✅ Канал поздравлений → {interaction.channel.mention}")

    if action.lower() == "check":
        target = member or interaction.user
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT birthday FROM birthdays WHERE guild_id=? AND user_id=?",
                                  (interaction.guild_id, target.id)) as c:
                row = await c.fetchone()
        if row:
            await interaction.response.send_message(f"🎂 День рождения **{target.display_name}**: **{row[0]}**")
        else:
            await interaction.response.send_message(f"❓ У **{target.display_name}** не указан день рождения.")
        return

    # set
    if not date:
        return await interaction.response.send_message("❌ Укажи дату: `/birthday date:25.12`", ephemeral=True)
    try:
        parts = date.strip().split(".")
        if len(parts) != 2: raise ValueError
        day, month = int(parts[0]), int(parts[1])
        if not (1 <= day <= 31 and 1 <= month <= 12): raise ValueError
        formatted = f"{day:02d}.{month:02d}"
    except ValueError:
        return await interaction.response.send_message("❌ Формат даты: **ДД.ММ** (напр. `25.12`)", ephemeral=True)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""INSERT INTO birthdays (guild_id, user_id, birthday) VALUES (?,?,?)
            ON CONFLICT(guild_id,user_id) DO UPDATE SET birthday=excluded.birthday""",
            (interaction.guild_id, interaction.user.id, formatted))
        await db.commit()
    await interaction.response.send_message(f"🎂 День рождения сохранён: **{formatted}**", ephemeral=True)




@bot.tree.command(name="ticket", description="Система тикетов [Premium]")
@app_commands.describe(action="open / close / setup / enable / disable", reason="Причина обращения")
async def ticket(interaction: discord.Interaction, action: str = "open", reason: str = "Обращение в поддержку"):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)

    if action.lower() == "setup":
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("❌ Нужны права администратора.", ephemeral=True)
        # Создаём категорию для тикетов
        cat = await interaction.guild.create_category("🎫 Tickets")
        await set_guild_setting(interaction.guild_id, "ticket_category", cat.id)
        await set_guild_setting(interaction.guild_id, "tickets_enabled", 1)
        e = discord.Embed(title="✅ Тикеты настроены", color=0x00E5FF)
        e.add_field(name="Категория", value=cat.name, inline=True)
        e.add_field(name="Статус", value="✅ Включены", inline=True)
        e.add_field(name="Использование", value="`/ticket` — открыть · `close` — закрыть · `disable/enable` — вкл/выкл", inline=False)
        return await interaction.response.send_message(embed=e)

    if action.lower() in ("disable", "enable"):
        if not interaction.user.guild_permissions.administrator:
            return await interaction.response.send_message("❌ Нужны права администратора.", ephemeral=True)
        enabled = action.lower() == "enable"
        await set_guild_setting(interaction.guild_id, "tickets_enabled", 1 if enabled else 0)
        status = "✅ Тикеты включены" if enabled else "🔒 Тикеты отключены"
        desc = "Участники могут открывать тикеты." if enabled else "Новые тикеты открыть нельзя. Существующие не затронуты."
        e = discord.Embed(title=status, description=desc, color=0x00E5FF if enabled else 0x36393F)
        return await interaction.response.send_message(embed=e)

    if action.lower() == "close":
        # Закрываем тикет (удаляем канал)
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT id FROM tickets WHERE guild_id=? AND channel_id=? AND status='open'",
                (interaction.guild_id, interaction.channel_id)
            ) as c:
                row = await c.fetchone()
        if not row:
            return await interaction.response.send_message("❌ Это не тикет-канал.", ephemeral=True)
        await interaction.response.send_message("🔒 Тикет закрывается...")
        await asyncio.sleep(3)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("UPDATE tickets SET status='closed' WHERE channel_id=?", (interaction.channel_id,))
            await db.commit()
        await interaction.channel.delete()
        return

    # open — создаём новый тикет
    settings = await get_guild_settings(interaction.guild_id)

    # Проверяем включены ли тикеты
    if not settings.get("tickets_enabled", 1):
        return await interaction.response.send_message(
            "❌ Система тикетов отключена на этом сервере.", ephemeral=True
        )

    cat_id = settings.get("ticket_category", 0)
    category = interaction.guild.get_channel(cat_id) if cat_id else None

    # Проверяем нет ли уже открытого тикета
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT channel_id FROM tickets WHERE guild_id=? AND user_id=? AND status='open'",
            (interaction.guild_id, interaction.user.id)
        ) as c:
            existing = await c.fetchone()

    if existing:
        ch = interaction.guild.get_channel(existing[0])
        return await interaction.response.send_message(
            f"❌ У тебя уже есть открытый тикет: {ch.mention if ch else 'канал удалён'}",
            ephemeral=True
        )

    # Создаём канал тикета
    overwrites = {
        interaction.guild.default_role: discord.PermissionOverwrite(read_messages=False),
        interaction.user: discord.PermissionOverwrite(read_messages=True, send_messages=True),
        interaction.guild.me: discord.PermissionOverwrite(read_messages=True, send_messages=True),
    }
    # Даём доступ модераторам
    for role in interaction.guild.roles:
        if role.permissions.manage_messages:
            overwrites[role] = discord.PermissionOverwrite(read_messages=True, send_messages=True)

    ch_name = f"ticket-{interaction.user.name[:15].lower().replace(' ', '-')}"
    try:
        ticket_ch = await interaction.guild.create_text_channel(
            ch_name, category=category, overwrites=overwrites
        )
    except Exception as ex:
        return await interaction.response.send_message(f"❌ Не удалось создать канал: {ex}", ephemeral=True)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO tickets (guild_id,user_id,channel_id,status,created_at) VALUES (?,?,?,?,?)",
            (interaction.guild_id, interaction.user.id, ticket_ch.id, "open", datetime.datetime.utcnow().isoformat())
        )
        await db.commit()

    e = discord.Embed(title="🎫 Тикет открыт", color=0x00E5FF, timestamp=datetime.datetime.utcnow())
    e.add_field(name="Участник", value=interaction.user.mention, inline=True)
    e.add_field(name="Причина", value=reason, inline=True)
    e.add_field(name="Закрыть", value="`/ticket action:close`", inline=False)
    e.set_footer(text="Модераторы скоро ответят")
    await ticket_ch.send(embed=e)
    await interaction.response.send_message(f"✅ Тикет создан: {ticket_ch.mention}", ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  💡 SUGGESTIONS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="suggestion", description="Система предложений")
@app_commands.describe(action="submit / top / setchannel", text="Текст предложения")
async def suggestion(interaction: discord.Interaction, action: str = "submit", text: str = ""):
    if action.lower() == "setchannel":
        if not interaction.user.guild_permissions.manage_guild:
            return await interaction.response.send_message("❌ Нужно Manage Server.", ephemeral=True)
        await set_guild_setting(interaction.guild_id, "suggestion_channel", interaction.channel_id)
        return await interaction.response.send_message(f"✅ Канал предложений → {interaction.channel.mention}")

    if action.lower() == "top":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT id, text, votes_up, votes_down, user_id FROM suggestions WHERE guild_id=? ORDER BY votes_up DESC LIMIT 5",
                (interaction.guild_id,)
            ) as c:
                rows = await c.fetchall()
        e = discord.Embed(title="💡 Топ предложений", color=0x00E5FF)
        if not rows:
            e.description = "Пока нет предложений. Добавь первое: `/suggestion text:...`"
        for i, (sid, text_s, up, down, uid) in enumerate(rows):
            user = interaction.guild.get_member(uid)
            e.add_field(
                name=f"#{sid} · 👍 {up} 👎 {down}",
                value=f"{text_s[:200]}\n*— {user.display_name if user else 'неизвестно'}*",
                inline=False
            )
        return await interaction.response.send_message(embed=e)

    # submit
    if not text:
        return await interaction.response.send_message("❌ Укажи текст: `/suggestion text:Моя идея`", ephemeral=True)

    settings = await get_guild_settings(interaction.guild_id)
    ch_id = settings.get("suggestion_channel", 0)
    ch = interaction.guild.get_channel(ch_id) if ch_id else interaction.channel

    e = discord.Embed(title="💡 Предложение", description=text, color=0x7C3AED, timestamp=datetime.datetime.utcnow())
    e.set_author(name=interaction.user.display_name, icon_url=interaction.user.display_avatar.url)
    e.add_field(name="Статус", value="⏳ На рассмотрении", inline=True)
    e.set_footer(text="👍 — за  |  👎 — против")

    msg = await ch.send(embed=e)
    await msg.add_reaction("👍")
    await msg.add_reaction("👎")

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO suggestions (guild_id,user_id,text,message_id,created_at) VALUES (?,?,?,?,?)",
            (interaction.guild_id, interaction.user.id, text, msg.id, datetime.datetime.utcnow().isoformat())
        )
        await db.commit()

    await interaction.response.send_message(f"✅ Предложение отправлено в {ch.mention}!", ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ⭐ STARBOARD
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="starboard", description="Настроить starboard [Premium]")
@app_commands.describe(channel="Канал для starboard", threshold="Количество ⭐ для попадания (по умолч. 3)")
async def starboard_setup(interaction: discord.Interaction, channel: discord.TextChannel, threshold: int = 3):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно Manage Server.", ephemeral=True)
    await set_guild_setting(interaction.guild_id, "starboard_channel", channel.id)
    await set_guild_setting(interaction.guild_id, "starboard_threshold", threshold)
    await interaction.response.send_message(
        f"⭐ Starboard настроен → {channel.mention} · порог: **{threshold}** звёзд"
    )


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  REACTION ROLES
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="reactionrole", description="Настройка reaction roles")
@app_commands.describe(
    action="add / remove / list / clear",
    message_id="ID сообщения",
    emoji="Эмодзи",
    role="Роль"
)
async def reactionrole_cmd(interaction: discord.Interaction,
                            action: str,
                            message_id: str = "",
                            emoji: str = "",
                            role: discord.Role = None):
    if not interaction.user.guild_permissions.manage_roles:
        return await interaction.response.send_message("❌ Нужны права Manage Roles.", ephemeral=True)
    gid = interaction.guild_id

    if action == "add":
        if not message_id or not emoji or not role:
            return await interaction.response.send_message("❌ Нужны: `message_id` `emoji` `role`", ephemeral=True)
        try:
            mid = int(message_id)
        except ValueError:
            return await interaction.response.send_message("❌ message_id должен быть числом", ephemeral=True)

        # Находим сообщение и добавляем реакцию
        msg = None
        for ch in interaction.guild.text_channels:
            try:
                msg = await ch.fetch_message(mid)
                break
            except Exception:
                continue
        if not msg:
            return await interaction.response.send_message("❌ Сообщение не найдено.", ephemeral=True)

        await msg.add_reaction(emoji)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT OR REPLACE INTO reaction_roles (guild_id,channel_id,message_id,emoji,role_id) VALUES (?,?,?,?,?)",
                (gid, msg.channel.id, mid, emoji, role.id)
            )
            await db.commit()
        e = discord.Embed(color=0x57F287, timestamp=datetime.datetime.utcnow())
        e.set_author(name="Reaction Role added")
        e.add_field(name="Emoji", value=emoji,        inline=True)
        e.add_field(name="Role",  value=role.mention, inline=True)
        return await interaction.response.send_message(embed=e)

    if action == "list":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT message_id,emoji,role_id FROM reaction_roles WHERE guild_id=?", (gid,)
            ) as c:
                rows = await c.fetchall()
        e = discord.Embed(color=0x5865F2, timestamp=datetime.datetime.utcnow())
        e.set_author(name="Reaction Roles")
        if not rows:
            e.description = "Нет настроенных reaction roles."
        for mid, em, rid in rows:
            r = interaction.guild.get_role(rid)
            e.add_field(name=f"{em} · msg `{mid}`", value=r.mention if r else str(rid), inline=True)
        return await interaction.response.send_message(embed=e, ephemeral=True)

    if action == "remove":
        if not message_id or not emoji:
            return await interaction.response.send_message("❌ Нужны: `message_id` `emoji`", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM reaction_roles WHERE guild_id=? AND message_id=? AND emoji=?",
                (gid, int(message_id), emoji)
            )
            await db.commit()
        return await interaction.response.send_message("✅ Reaction role удалена.", ephemeral=True)

    if action == "clear":
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM reaction_roles WHERE guild_id=?", (gid,))
            await db.commit()
        return await interaction.response.send_message("✅ Все reaction roles удалены.", ephemeral=True)

    await interaction.response.send_message("❌ Действие: `add` `remove` `list` `clear`", ephemeral=True)


@bot.tree.command(name="invite", description="Добавить Witness на свой сервер")
async def invite_cmd(interaction: discord.Interaction):
    invite_url = get_invite_url()
    e = discord.Embed(color=0x5865F2, timestamp=datetime.datetime.utcnow())
    e.set_author(name="Add Witness to your server",
                 icon_url=bot.user.display_avatar.url if bot.user else None)
    e.description = (
        "Witness — модерация, безопасность и Albion Online статистика в одном боте.\n\n"
        f"**[Нажми здесь чтобы добавить]({invite_url})**"
    )
    e.add_field(name="Free",     value="Albion · Games · Basic Security", inline=True)
    e.add_field(name="Premium",  value="AI · BM · Craft · €2.99/mo",      inline=True)
    e.add_field(name="Security", value="Advanced /q security tools · €4.99/mo", inline=True)

    view = discord.ui.View()
    if BOT_ID:
        view.add_item(discord.ui.Button(
            label="Add to Server",
            style=discord.ButtonStyle.link,
            url=invite_url,
            emoji="➕"
        ))
    view.add_item(discord.ui.Button(
        label="Support Server",
        style=discord.ButtonStyle.link,
        url=SUPPORT_URL,
        emoji="💬"
    ))
    if BOT_ID:
        view.add_item(discord.ui.Button(
            label="Vote on top.gg",
            style=discord.ButtonStyle.link,
            url=f"https://top.gg/bot/{BOT_ID}/vote",
            emoji="⬆️"
        ))
    await interaction.response.send_message(embed=e, view=view)


@bot.tree.command(name="vote", description="Проголосовать за Witness на top.gg")
async def vote_cmd(interaction: discord.Interaction):
    e = discord.Embed(color=0xFF3366, timestamp=datetime.datetime.utcnow())
    e.set_author(name="Vote for Witness on top.gg",
                 icon_url=bot.user.display_avatar.url if bot.user else None)
    e.description = (
        "Голосование помогает боту подняться выше в поиске на top.gg\n"
        "и привлечь новых пользователей. Голосовать можно каждые 12 часов!"
    )
    e.add_field(name="Зачем голосовать?", value=(
        "• Больше серверов узнают о Witness\n"
        "• Бот поднимается в рейтинге top.gg\n"
        "• Поддерживаешь разработку"
    ), inline=False)

    view = discord.ui.View()
    if BOT_ID:
        view.add_item(discord.ui.Button(
            label="Vote on top.gg",
            style=discord.ButtonStyle.link,
            url=f"https://top.gg/bot/{BOT_ID}/vote",
            emoji="⬆️"
        ))
        view.add_item(discord.ui.Button(
            label="Vote on discord.bots.gg",
            style=discord.ButtonStyle.link,
            url=f"https://discord.bots.gg/bots/{BOT_ID}",
            emoji="🗳️"
        ))
    await interaction.response.send_message(embed=e, view=view)


@bot.tree.command(name="botinfo", description="Информация о боте Witness")
async def botinfo_cmd(interaction: discord.Interaction):
    guilds  = len(bot.guilds)
    members = sum(g.member_count for g in bot.guilds if g.member_count)
    ping    = round(bot.latency * 1000)

    e = discord.Embed(color=0x5865F2, timestamp=datetime.datetime.utcnow())
    e.set_author(name="Witness Bot",
                 icon_url=bot.user.display_avatar.url if bot.user else None)
    e.set_thumbnail(url=bot.user.display_avatar.url if bot.user else None)
    e.description = (
        "All-in-one модерация, продвинутая безопасность с AI "
        "и полная интеграция с Albion Online."
    )
    e.add_field(name="Серверов",   value=f"**{guilds:,}**",   inline=True)
    e.add_field(name="Участников", value=f"**{members:,}**",  inline=True)
    e.add_field(name="Ping",       value=f"**{ping}ms**",     inline=True)
    e.add_field(name="Команд",     value="**71**",            inline=True)
    e.add_field(name="Версия",     value="**v1.0**",          inline=True)
    e.add_field(name="Библиотека", value="**discord.py 2.x**", inline=True)
    e.add_field(name="Уникальные функции", value=(
        "🛡️ Advanced Security (/q)\n"
        "⚔️ Albion Online интеграция\n"
        "🔐 Цифровые подписи модераторских действий"
    ), inline=False)

    view = discord.ui.View()
    invite_url = get_invite_url()
    if BOT_ID:
        view.add_item(discord.ui.Button(label="Add to Server", style=discord.ButtonStyle.link,
                                         url=invite_url, emoji="➕"))
    view.add_item(discord.ui.Button(label="Support", style=discord.ButtonStyle.link,
                                     url=SUPPORT_URL, emoji="💬"))
    if BOT_ID:
        view.add_item(discord.ui.Button(label="Vote", style=discord.ButtonStyle.link,
                                         url=f"https://top.gg/bot/{BOT_ID}/vote", emoji="⬆️"))
    await interaction.response.send_message(embed=e, view=view)
