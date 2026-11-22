"""
Статистика активности сервера (/serverstats).
"""

import discord
from discord import app_commands
import aiohttp
import os, asyncio, random, time, json, datetime
from .config import TIER_PREMIUM
from .database import get_leaderboard
from .ui import build_embed, C
from .core import (
    bot,
    cooldown,
    get_tier,
    intents,
    upsell_embed,
)

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
