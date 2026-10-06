"""
ИИ-команды и статистика активности сервера.
"""

import discord
from discord import app_commands
import aiohttp
import os, asyncio, random, time, json, datetime
from .config import TIER_PREMIUM
from .database import get_leaderboard
from .ui import build_embed, C
from .core import (
    ask_ai,
    bot,
    cooldown,
    get_tier,
    upsell_embed,
)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PREMIUM COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="ai")
@app_commands.describe(question="Вопрос")
@cooldown(5)
async def ai_cmd(interaction: discord.Interaction, question: str):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    try:
        answer = await ask_ai(question)
        e = build_embed(C.PREMIUM, description=answer[:4000])
        e.set_author(name="Witness AI", icon_url=interaction.user.display_avatar.url)
        e.add_field(name="Запрос", value=f"`{question[:100]}`", inline=False)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="summarize")
@app_commands.describe(count="Сообщений (макс 50)")
@cooldown(30)
async def summarize(interaction: discord.Interaction, count: int = 20):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    msgs = []
    async for msg in interaction.channel.history(limit=min(count,50)):
        if not msg.author.bot: msgs.append(f"{msg.author.display_name}: {msg.content}")
    msgs.reverse()
    try:
        summary = await ask_ai("\n".join(msgs), system="Summarize this Discord chat in 3-5 bullet points. Be concise.")
        await interaction.followup.send(embed=discord.Embed(title=f"📋 Резюме ({count} сообщений)", description=summary, color=0x00E5FF))
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="roast")
@app_commands.describe(member="Кого роастить")
@cooldown(10)
async def roast(interaction: discord.Interaction, member: discord.Member):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    roles=[r.name for r in member.roles[1:]]
    days=(datetime.datetime.utcnow()-member.joined_at.replace(tzinfo=None)).days
    try:
        text = await ask_ai(f"Funny 2-3 sentence roast: Name={member.display_name}, Roles={','.join(roles) or 'None'}, Days={days}. Playful, not offensive.", system="Write friendly roasts for Discord.")
        e = discord.Embed(title=f"🔥 {member.display_name}", description=text, color=0xFF6B35)
        e.set_thumbnail(url=member.display_avatar.url)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  📊 SERVER STATS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_msg_activity: dict = {}  # {guild_id: {hour: count}}


def count_message_activity(message):
    """Считает сообщения по часам (UTC) для /serverstats. Вызывается из on_message."""
    gid = message.guild.id
    hour = datetime.datetime.utcnow().hour
    _msg_activity.setdefault(gid, {})
    _msg_activity[gid][hour] = _msg_activity[gid].get(hour, 0) + 1

@bot.tree.command(name="serverstats", description="Статистика активности сервера [Premium]")
async def serverstats(interaction: discord.Interaction):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    await interaction.response.defer()
    g = interaction.guild
    gid = g.id

    # Считаем онлайн
    online = sum(1 for m in g.members if m.status != discord.Status.offline) if hasattr(g.members[0], 'status') else "N/A"
    bots   = sum(1 for m in g.members if m.bot)
    humans = g.member_count - bots

    # XP топ
    rows = await get_leaderboard(gid, 3)

    # Активность по часам
    activity = _msg_activity.get(gid, {})
    if activity:
        peak_hour = max(activity, key=activity.get)
        peak_msgs = activity[peak_hour]
        activity_str = f"Пик: **{peak_hour}:00 UTC** ({peak_msgs} сообщений)\n"
        activity_str += " ".join(
            f"`{h}:{'█' * min(activity.get(h,0)//5+1, 5)}`"
            for h in range(0, 24, 4)
        )
    else:
        activity_str = "Нет данных за текущую сессию"

    e = discord.Embed(title=f"📊 Статистика: {g.name}", color=0x00E5FF, timestamp=datetime.datetime.utcnow())
    if g.icon: e.set_thumbnail(url=g.icon.url)
    e.add_field(name="👥 Участников", value=f"**{g.member_count}**\n{humans} людей · {bots} ботов", inline=True)
    e.add_field(name="📁 Каналов", value=f"**{len(g.channels)}**\n{len(g.text_channels)} текст · {len(g.voice_channels)} голос", inline=True)
    e.add_field(name="🎭 Ролей", value=str(len(g.roles)), inline=True)
    e.add_field(name="📅 Создан", value=g.created_at.strftime("%d.%m.%Y"), inline=True)
    e.add_field(name="💎 Буст", value=f"Уровень {g.premium_tier} · {g.premium_subscription_count} бустов", inline=True)

    if rows:
        top_lines = []
        for i, (uid, xp) in enumerate(rows):
            u = g.get_member(uid)
            top_lines.append(f"{['🥇','🥈','🥉'][i]} {u.display_name if u else uid} — {xp} XP")
        e.add_field(name="🏆 Топ активных", value="\n".join(top_lines), inline=False)

    e.add_field(name="📈 Активность (сегодня)", value=activity_str, inline=False)
    await interaction.followup.send(embed=e)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🖼️ AI IMAGE GENERATION (Pollinations.ai — бесплатно)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="imagine", description="Генерация изображения по описанию [Premium]")
@app_commands.describe(prompt="Описание изображения на английском", style="realistic / anime / pixel / oil-painting")
@cooldown(15)
async def imagine(interaction: discord.Interaction, prompt: str, style: str = "realistic"):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    await interaction.response.defer()

    styles = {
        "realistic": "photorealistic, high quality, 8k",
        "anime": "anime style, manga, illustration",
        "pixel": "pixel art, 16-bit, retro game",
        "oil-painting": "oil painting, classical art, detailed brushwork"
    }
    style_prompt = styles.get(style.lower(), styles["realistic"])
    full_prompt = f"{prompt}, {style_prompt}"
    encoded = full_prompt.replace(" ", "%20").replace(",", "%2C")

    # Pollinations.ai — полностью бесплатный API генерации изображений
    url = f"https://image.pollinations.ai/prompt/{encoded}?width=768&height=768&nologo=true"

    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=60)) as r:
                if r.status != 200:
                    return await interaction.followup.send(f"❌ Ошибка генерации (HTTP {r.status})")
                img_data = await r.read()

        file = discord.File(
            fp=__import__("io").BytesIO(img_data),
            filename="generated.png"
        )
        e = discord.Embed(title="🎨 Сгенерированное изображение", color=0x7C3AED)
        e.add_field(name="Запрос", value=prompt[:200], inline=False)
        e.add_field(name="Стиль", value=style, inline=True)
        e.set_footer(text=f"Запросил: {interaction.user.display_name} · Pollinations.ai (free)")
        e.set_image(url="attachment://generated.png")
        await interaction.followup.send(embed=e, file=file)
    except asyncio.TimeoutError:
        await interaction.followup.send("❌ Таймаут генерации (>60 сек). Попробуй более простой запрос.")
    except Exception as ex:
        await interaction.followup.send(f"❌ Ошибка: {ex}")
