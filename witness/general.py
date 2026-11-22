"""
Общие команды: help, профиль, экономика, утилиты. Тикеты — witness/tickets.py.
"""

import discord
from discord import app_commands
import aiohttp
import os, asyncio, random, time, json, datetime
from .config import (
    BOT_ID,
    get_invite_url,
    SUPPORT_URL,
    TIER_COLORS,
    TIER_PREMIUM,
    WEATHER_KEY,
)
from .database import (
    get_coins,
    get_leaderboard,
    get_xp,
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
        "security": "🛡️ Безопасность",
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
                ("🆓 Утилиты", "`/poll` (с таймером) · `/lfg` (с Join кнопкой)\n`/weather` · `/translate`"),
                ("🆓 Комьюнити", "`/ticket open`"),
                ("⭐ Premium", "`/serverstats` `/giveaway` `/roast` `/summarize` `/imagine`"),
            ]
        },
        "security": {
            "title": "🛡️ Безопасность",
            "color": C.WARNING,
            "fields": [
                ("Настройка", "`/setup` — пошаговая настройка\n`/security status` · `toggle` · `setlog`"),
                ("Логирование", "`joins` `leaves` `bans` `timeouts` `msg_delete` `msg_edit`\n`invites` `suspicious` `nick_change` `role_change` `voice`"),
                ("Авто-защита", "`anti_raid` · `anti_spam` · `/lockdown` · `/slowmode`\n`/quarantine` — карантин для новых аккаунтов"),
                ("Модерация", "`/warn` `/unmute` `/tempban` `/warnings` `/clearwarn`\n`/purge` `/report` `/modlog` `/punishments` `/case`"),
                ("Инвайты", "`/invcheck` `/invuser` `/invdel` `/invnote` `/invnotes` `/invstats`"),
                ("Роли", "`/roles panel create/add/remove/edit/delete/list` — кнопки и меню (включается в дашборде)"),
                ("🔐 Advanced Security — префикс `/q` (Security plan)",
                 "`/q scan` · `/q threat`\n"
                 "`/q nlp` · `/q forensics` · `/q sig` · `/q alert`\n"
                 "`/q network` · `/q status` · `/q help`\nПодробнее: `/sechelp`"),
            ]
        },
        "new": {
            "title": "🆕 Новые команды",
            "color": C.SUCCESS,
            "fields": [
                ("🔐 Security", "`/tempban` `/unmute` `/modlog` `/punishments` `/quarantine`"),
                ("📨 Инвайты", "`/invnote` `/invnotes` `/invstats`"),
                ("⚙️ Настройка", "`/setup` `/roles panel` `/ticket setup`"),
                ("🤖 Утилиты", "`/poll` (таймер) · `/lfg` (Join кнопка)"),
                ("📣 Бот", "`/invite` `/vote` `/botinfo`"),
            ]
        },
        "ai": {
            "title": "🤖 AI команды",
            "color": C.PREMIUM,
            "fields": [
                ("ⓘ AI движок", "Бот использует **Groq** (Llama 3.3 70B) и **Gemini** — бесплатно."),
                ("⭐ Текст", "`/ai` `/summarize`"),
                ("⭐ Развлечения", "`/roast` — роаст участника\n`/imagine` — генерация изображений (Pollinations.ai)"),
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
@app_commands.describe(page="Раздел: general / security / ai / new")
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


@bot.tree.command(name="invite", description="Добавить Witness на свой сервер")
async def invite_cmd(interaction: discord.Interaction):
    invite_url = get_invite_url()
    e = discord.Embed(color=0x5865F2, timestamp=datetime.datetime.utcnow())
    e.set_author(name="Add Witness to your server",
                 icon_url=bot.user.display_avatar.url if bot.user else None)
    e.description = (
        "Witness — модерация, безопасность, тикеты и роли в одном боте.\n\n"
        f"**[Нажми здесь чтобы добавить]({invite_url})**"
    )
    e.add_field(name="Free",     value="Moderation · Tickets · Basic Security", inline=True)
    e.add_field(name="Premium",  value="AI · Giveaways · €2.99/mo",      inline=True)
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
        "тикеты, роли и учёт приглашений."
    )
    e.add_field(name="Серверов",   value=f"**{guilds:,}**",   inline=True)
    e.add_field(name="Участников", value=f"**{members:,}**",  inline=True)
    e.add_field(name="Ping",       value=f"**{ping}ms**",     inline=True)
    e.add_field(name="Команд",     value=f"**{len(bot.tree.get_commands())}**",            inline=True)
    e.add_field(name="Версия",     value="**v1.0**",          inline=True)
    e.add_field(name="Библиотека", value="**discord.py 2.x**", inline=True)
    e.add_field(name="Уникальные функции", value=(
        "🛡️ Advanced Security (/q)\n"
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
