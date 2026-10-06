"""
Игровая статистика: Valorant, CS2, LoL, Lost Ark, RuneScape, Minecraft.
"""

import discord
from discord import app_commands
import aiohttp
from .config import (
    HENRIK_KEY,
    LOSTARK_KEY,
    RIOT_KEY,
    STEAM_KEY,
    TIER_PREMIUM,
)
from .ui import bar, build_embed
from .core import (
    bot,
    cooldown,
    get_tier,
    upsell_embed,
)

@bot.tree.command(name="rs")
@app_commands.describe(username="OSRS ник")
@cooldown(5)
async def rs(interaction: discord.Interaction, username: str):
    await interaction.response.defer()
    skills = ["Overall","Attack","Defence","Strength","Hitpoints","Ranged","Prayer","Magic","Cooking","Woodcutting","Fletching","Fishing","Firemaking","Crafting","Smithing","Mining"]
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://secure.runescape.com/m=hiscore_oldschool/index_lite.ws?player={username}") as r:
                if r.status!=200: return await interaction.followup.send(f"❌ **{username}** не найден.")
                lines = (await r.text()).strip().split("\n")
        e = discord.Embed(title=f"⚔️ OSRS — {username}", color=0xB5651D)
        overall = lines[0].split(",")
        e.add_field(name="Total Level", value=overall[1], inline=True)
        e.add_field(name="Total XP", value=f"{int(overall[2]):,}", inline=True)
        top=""
        for i in range(1,min(9,len(lines))):
            p=lines[i].split(",")
            if len(p)>=2 and int(p[1])>1: top+=f"**{skills[i]}**: {p[1]}\n"
        if top: e.add_field(name="Навыки",value=top,inline=False)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="mc")
@app_commands.describe(address="IP или домен")
@cooldown(10)
async def mc(interaction: discord.Interaction, address: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.mcstatus.io/v2/status/java/{address}") as r: d = await r.json()
        if not d.get("online"): return await interaction.followup.send(f"🔴 **{address}** оффлайн.")
        e = discord.Embed(title=f"🟢 {address}", color=0x00FF9D)
        e.add_field(name="Игроки", value=f"{d['players']['online']}/{d['players']['max']}", inline=True)
        e.add_field(name="Версия", value=d.get("version",{}).get("name_clean","?"), inline=True)
        motd = d.get("motd",{}).get("clean","")
        if motd: e.add_field(name="MOTD",value=motd[:200],inline=False)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="val")
@app_commands.describe(username="Riot ID (Player#TAG)")
@cooldown(10)
async def val(interaction: discord.Interaction, username: str):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    if "#" not in username: return await interaction.followup.send("❌ Формат: **Name#TAG**")
    name,tag = username.split("#",1)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.henrikdev.xyz/valorant/v2/mmr/eu/{name}/{tag}", headers={"Authorization":HENRIK_KEY} if HENRIK_KEY else {}) as r: d = await r.json()
        if d.get("status")!=200: return await interaction.followup.send(f"❌ **{username}** не найден.")
        data = d["data"]
        rank_name = data.get("currenttierpatched", "Unranked")
        rr        = data.get("ranking_in_tier", 0)
        peak      = data.get("highest_rank", {}).get("patched_tier", "?")
        rr_bar    = bar(rr, 100, 10)
        e = build_embed(0xFF4655, footer="Valorant · EU")
        e.set_author(name=username)
        e.add_field(name="Ранг",       value=f"**{rank_name}**",         inline=True)
        e.add_field(name="RR",         value=f"**{rr}/100** `{rr_bar}`", inline=True)
        e.add_field(name="Пик",        value=f"**{peak}**",              inline=True)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="cs2")
@app_commands.describe(steam_id="Steam ID64 или vanity name")
@cooldown(10)
async def cs2(interaction: discord.Interaction, steam_id: str):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    if not STEAM_KEY: return await interaction.followup.send("❌ Функция временно недоступна.")
    if not steam_id.isdigit():
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.steampowered.com/ISteamUser/ResolveVanityURL/v1/?key={STEAM_KEY}&vanityurl={steam_id}") as r:
                steam_id = (await r.json()).get("response",{}).get("steamid",steam_id)
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://api.steampowered.com/ISteamUserStats/GetUserStatsForGame/v2/?appid=730&key={STEAM_KEY}&steamid={steam_id}") as r:
                sd = {s["name"]:s["value"] for s in (await r.json()).get("playerstats",{}).get("stats",[])}
        kills,deaths,wins,hs = sd.get("total_kills",0),sd.get("total_deaths",0),sd.get("total_wins",0),sd.get("total_kills_headshot",0)
        kd     = round(kills / deaths, 2) if deaths else "∞"
        hs_pct = round(hs / kills * 100, 1) if kills else 0
        hs_bar = bar(hs_pct, 100, 8)
        e = build_embed(0xF0A500, footer="CS2 · Steam")
        e.set_author(name=f"CS2 — {steam_id}")
        e.add_field(name="K/D",      value=f"**{kd}**",                        inline=True)
        e.add_field(name="Убийств",  value=f"**{kills:,}**",                   inline=True)
        e.add_field(name="Побед",    value=f"**{wins:,}**",                    inline=True)
        e.add_field(name="HS%",      value=f"**{hs_pct}%** `{hs_bar}`",        inline=True)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="lol")
@app_commands.describe(summoner="Summoner name", region="Регион (euw1, na1...)")
@cooldown(10)
async def lol(interaction: discord.Interaction, summoner: str, region: str = "euw1"):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    if not RIOT_KEY: return await interaction.followup.send("❌ Функция временно недоступна.")
    headers={"X-Riot-Token":RIOT_KEY}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://{region}.api.riotgames.com/lol/summoner/v4/summoners/by-name/{summoner}",headers=headers) as r:
                if r.status!=200: return await interaction.followup.send(f"❌ **{summoner}** не найден.")
                sid = (await r.json())["id"]
            async with s.get(f"https://{region}.api.riotgames.com/lol/league/v4/entries/by-summoner/{sid}",headers=headers) as r:
                entries = await r.json()
        e = build_embed(0xC89B3C, footer=f"League of Legends · {region.upper()}")
        e.set_author(name=summoner)
        if not entries:
            e.description = "Unranked this season."
        for en in entries:
            w, l = en["wins"], en["losses"]
            wr   = round(w / (w + l) * 100, 1) if (w + l) else 0
            wr_b = bar(wr, 100, 8)
            e.add_field(
                name=en["queueType"].replace("_", " ").title(),
                value=(
                    f"**{en['tier']} {en['rank']}** · {en['leaguePoints']} LP\n"
                    f"{w}W / {l}L · **{wr}%** WR `{wr_b}`"
                ),
                inline=True
            )
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="lostark")
@app_commands.describe(character="Имя персонажа")
@cooldown(10)
async def lostark(interaction: discord.Interaction, character: str):
    if await get_tier(interaction.guild_id)<TIER_PREMIUM: return await interaction.response.send_message(embed=upsell_embed("Premium"),ephemeral=True)
    await interaction.response.defer()
    if not LOSTARK_KEY: return await interaction.followup.send("❌ Функция временно недоступна.")
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"https://developer-lostark.game.onstove.com/characters/{character}/siblings", headers={"Authorization":f"bearer {LOSTARK_KEY}"}) as r:
                if r.status!=200: return await interaction.followup.send(f"❌ **{character}** не найден.")
                chars = await r.json()
        e = discord.Embed(title=f"⚔️ Lost Ark — {character}", color=0x3D9BD4)
        for c in chars[:8]: e.add_field(name=c.get("CharacterName","?"), value=f"{c.get('CharacterClassName','?')}\niLvl: **{c.get('ItemMaxLevel','?')}**", inline=True)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")
