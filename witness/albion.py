"""
Albion Online: статистика, чёрный рынок, крафт, флиппер, слежение за игроками.
"""

import discord
from discord import app_commands
import aiohttp
import aiosqlite
import os, asyncio, random, time, json, datetime
from datetime import timedelta
from .config import (
    ALBION_BASE,
    ALBION_DATA,
    DB_PATH,
    GOOGLE_CREDS,
    SHEET_ID,
    TIER_PREMIUM,
    TIER_PRO,
)
from .ui import (
    bar,
    build_embed,
    C,
    make_embed,
    PaginatedView,
    profit_color,
)
from .core import (
    ask_ai,
    bot,
    cooldown,
    get_tier,
    upsell_embed,
)

class BlackmarketCategoryView(discord.ui.View):
    """Select Menu для выбора категории в /blackmarket"""

    def __init__(self, tier: int, server: str, current_cat: str):
        super().__init__(timeout=60)
        self.tier = tier
        self.server = server

        options = [
            discord.SelectOption(label="Оружие (все)", value="weapon", emoji="⚔️", default=current_cat=="weapon"),
            discord.SelectOption(label="Offhand", value="offhand", emoji="🛡️", default=current_cat=="offhand"),
            discord.SelectOption(label="Броня: Латы", value="armor_plate", emoji="🪖", default=current_cat=="armor_plate"),
            discord.SelectOption(label="Броня: Кожа", value="armor_leather", emoji="🧥", default=current_cat=="armor_leather"),
            discord.SelectOption(label="Броня: Ткань", value="armor_cloth", emoji="👘", default=current_cat=="armor_cloth"),
            discord.SelectOption(label="Сумки", value="bag", emoji="🎒", default=current_cat=="bag"),
        ]
        select = discord.ui.Select(placeholder="Выбрать категорию...", options=options)
        select.callback = self.on_select
        self.add_item(select)

    async def on_select(self, interaction: discord.Interaction):
        cat = interaction.data["values"][0]
        await interaction.response.send_message(
            f"⏳ Загружаю **{cat}** T{self.tier} · {ALBION_SERVER_NAMES.get(self.server,'EU')}...",
            ephemeral=True
        )
        # Запускаем полный запрос
        await run_blackmarket(interaction, cat, self.tier, self.server, "no")

async def albion_find_player(session, name):
    async with session.get(f"{ALBION_BASE}/search?q={name}", timeout=aiohttp.ClientTimeout(total=10)) as r:
        if r.status != 200: return None, None
        p = (await r.json()).get("players", [])
        return (p[0]["Id"], p[0]["Name"]) if p else (None, None)

def fmt_item(item_id):
    if not item_id: return "—"
    parts = item_id.replace("@"," ✦").split("_")
    return " ".join(p for p in parts if not (p.startswith("T") and p[1:].isdigit())).title() or item_id
# Семафор для Albion API — не более 3 параллельных запросов
_albion_api_semaphore = None  # инициализируется в on_ready

# ─── FREE GAME COMMANDS ───────────────────────────────────────


async def albion_fetch(session, url: str):
    """Обёртка для Albion API запросов с семафором и таймаутом"""
    global _albion_api_semaphore
    if not _albion_api_semaphore:
        _albion_api_semaphore = asyncio.Semaphore(3)
    try:
        async with _albion_api_semaphore:
            async with session.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status == 200:
                    return await r.json()
                return None
    except asyncio.TimeoutError:
        return None
    except Exception:
        return None


@bot.tree.command(name="stats")
@app_commands.describe(player="Ник игрока")
@cooldown(5)
async def stats(interaction: discord.Interaction, player: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            pid, pname = await albion_find_player(s, player)
            if not pid: return await interaction.followup.send(f"❌ **{player}** не найден.")
            async with s.get(f"{ALBION_BASE}/players/{pid}") as r: p = await r.json()
        kf  = p.get("KillFame", 0)
        df  = p.get("DeathFame", 0)
        pve = p.get("LifetimeStatistics", {}).get("PvE", {}).get("Total", 0)
        kd  = round(kf / df, 2) if df else "∞"
        kd_bar = bar(min(kf / max(df, 1), 5), 5, 8) if df else "████████"
        e = build_embed(C.INFO, footer="EU · Albion Online")
        e.set_author(name=pname, icon_url=f"https://render.albiononline.com/v1/player/{pname}/avatar?size=40")
        e.add_field(name="Гильдия",    value=p.get("GuildName") or "—",  inline=True)
        e.add_field(name="Альянс",     value=p.get("AllianceName") or "—", inline=True)
        e.add_field(name="K/D",        value=f"**{kd}** `{kd_bar}`",      inline=True)
        e.add_field(name="Kill Fame",  value=f"**{kf:,}**",               inline=True)
        e.add_field(name="Death Fame", value=f"**{df:,}**",               inline=True)
        e.add_field(name="PvE Fame",   value=f"**{pve:,}**",              inline=True)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="kills")
@app_commands.describe(player="Ник игрока")
@cooldown(5)
async def kills(interaction: discord.Interaction, player: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            pid, pname = await albion_find_player(s, player)
            if not pid: return await interaction.followup.send(f"❌ **{player}** не найден.")
            async with s.get(f"{ALBION_BASE}/players/{pid}/kills?limit=5") as r: evs = await r.json()
        if not evs: return await interaction.followup.send(f"Нет недавних убийств у **{pname}**.")
        e = build_embed(C.DANGER, footer="EU · Albion Online")
        e.set_author(name=f"{pname} — последние убийства")
        total_fame = sum(ev.get("TotalVictimKillFame", 0) for ev in evs[:5])
        e.description = f"За последние 5 убийств заработано **{total_fame:,}** fame"
        for ev in evs[:5]:
            v      = ev.get("Victim", {})
            weapon = fmt_item(v.get("Equipment", {}).get("MainHand", {}).get("Type", "") if v.get("Equipment") else "")
            fame   = ev.get("TotalVictimKillFame", 0)
            date   = ev.get("TimeStamp", "")[:10]
            e.add_field(
                name=f"{v.get('Name', '?')} · {date}",
                value=f"Fame: **{fame:,}** · {weapon}",
                inline=True
            )
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="deaths")
@app_commands.describe(player="Ник игрока")
@cooldown(5)
async def deaths(interaction: discord.Interaction, player: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            pid, pname = await albion_find_player(s, player)
            if not pid: return await interaction.followup.send(f"❌ **{player}** не найден.")
            async with s.get(f"{ALBION_BASE}/players/{pid}/deaths?limit=5") as r: evs = await r.json()
        if not evs: return await interaction.followup.send(f"Нет недавних смертей у **{pname}**.")
        e = build_embed(C.MUTED, footer="EU · Albion Online")
        e.set_author(name=f"{pname} — последние смерти")
        total = sum(ev.get("TotalVictimKillFame", 0) for ev in evs[:5])
        e.description = f"Потеряно **{total:,}** fame в 5 последних смертях"
        for ev in evs[:5]:
            k    = ev.get("Killer", {})
            fame = ev.get("TotalVictimKillFame", 0)
            date = ev.get("TimeStamp", "")[:10]
            e.add_field(
                name=f"Убит: {k.get('Name', '?')} · {date}",
                value=f"Потеряно fame: **{fame:,}**",
                inline=True
            )
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="guild")
@app_commands.describe(name="Гильдия")
@cooldown(10)
async def guild_cmd(interaction: discord.Interaction, name: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{ALBION_BASE}/search?q={name}") as r: guilds = (await r.json()).get("guilds",[])
            if not guilds: return await interaction.followup.send(f"❌ **{name}** не найдена.")
            gid = guilds[0]["Id"]
            async with s.get(f"{ALBION_BASE}/guilds/{gid}") as r: gdata = await r.json()
            async with s.get(f"{ALBION_BASE}/guilds/{gid}/members") as r: members = await r.json()
        e = discord.Embed(title=f"🏰 {gdata.get('Name',name)}", color=0x00E5FF)
        e.add_field(name="Участники", value=str(len(members)), inline=True)
        top = sorted(members, key=lambda m: m.get("KillFame",0), reverse=True)[:5]
        lines = [f"{i+1}. **{m.get('Name','?')}** — {m.get('KillFame',0):,}" for i,m in enumerate(top)]
        if lines: e.add_field(name="🏆 Топ по Fame", value="\n".join(lines), inline=False)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="battle")
@cooldown(15)
async def battle(interaction: discord.Interaction):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{ALBION_BASE}/battles?sort=recent&limit=5") as r: battles = await r.json()
        e = make_embed(
            title="Последние ZvZ битвы",
            description=f"Данные по {len(battles[:5])} последним сражениям",
            color=C.DANGER, footer="EU · Albion Online"
        )
        for b in battles[:5]:
            guilds = list(b.get("Guilds", {}).keys())[:3]
            name_str = " vs ".join(guilds) if guilds else "Open World"
            kills = b.get("TotalKills", 0)
            fame  = b.get("TotalFame", 0)
            date  = b.get("StartTime", "")[:10]
            e.add_field(
                name=f"{name_str} · {date}",
                value=f"Убийств: **{kills}** · Fame: **{fame:,}**",
                inline=False
            )
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="compare")
@app_commands.describe(player1="Игрок 1", player2="Игрок 2")
@cooldown(10)
async def compare(interaction: discord.Interaction, player1: str, player2: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            p1id,p1name = await albion_find_player(s,player1); p2id,p2name = await albion_find_player(s,player2)
            if not p1id: return await interaction.followup.send(f"❌ **{player1}** не найден.")
            if not p2id: return await interaction.followup.send(f"❌ **{player2}** не найден.")
            async with s.get(f"{ALBION_BASE}/players/{p1id}") as r: d1 = await r.json()
            async with s.get(f"{ALBION_BASE}/players/{p2id}") as r: d2 = await r.json()
        def kd(d): kf,df=d.get("KillFame",0),d.get("DeathFame",0); return round(kf/df,2) if df else float("inf")
        kf1,kf2 = d1.get("KillFame",0),d2.get("KillFame",0)
        def w(a,b): return ("✅","❌") if a>b else (("❌","✅") if b>a else ("🟡","🟡"))
        wf1,wf2 = w(kf1,kf2)
        e = discord.Embed(title=f"⚔️ {p1name} vs {p2name}", color=0x00E5FF)
        e.add_field(name=f"{wf1} {p1name}", value=f"Fame: **{kf1:,}**\nK/D: **{kd(d1)}**\n{d1.get('GuildName') or '—'}", inline=True)
        e.add_field(name="VS", value="​", inline=True)
        e.add_field(name=f"{wf2} {p2name}", value=f"Fame: **{kf2:,}**\nK/D: **{kd(d2)}**\n{d2.get('GuildName') or '—'}", inline=True)
        e.set_footer(text=f"Преимущество: {p1name if kf1>kf2 else p2name if kf2>kf1 else 'Ничья'}")
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

@bot.tree.command(name="history")
@app_commands.describe(player="Ник игрока")
@cooldown(10)
async def history(interaction: discord.Interaction, player: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            pid,pname = await albion_find_player(s,player)
            if not pid: return await interaction.followup.send(f"❌ **{player}** не найден.")
            async with s.get(f"{ALBION_BASE}/players/{pid}/kills?limit=50") as r: ak = await r.json()
            async with s.get(f"{ALBION_BASE}/players/{pid}/deaths?limit=50") as r: ad = await r.json()
        cutoff = datetime.datetime.utcnow()-timedelta(days=7)
        def recent(evs):
            out=[]
            for ev in evs:
                try:
                    if datetime.datetime.fromisoformat(ev.get("TimeStamp","")[:19])>=cutoff: out.append(ev)
                except: pass
            return out
        wk,wd = recent(ak),recent(ad)
        fame = sum(e.get("TotalVictimKillFame",0) for e in wk)
        kd_week = round(len(wk) / len(wd), 2) if wd else "∞"
        activity = "Очень активен" if len(wk) > 20 else "Активен" if len(wk) > 5 else "Тихая неделя"
        act_bar  = bar(min(len(wk), 30), 30, 10)
        e = build_embed(C.SUCCESS, footer="EU · Albion Online · 7 дней")
        e.set_author(name=f"{pname} — активность за 7 дней")
        e.add_field(name="Убийств",  value=f"**{len(wk)}**",   inline=True)
        e.add_field(name="Смертей",  value=f"**{len(wd)}**",   inline=True)
        e.add_field(name="K/D",      value=f"**{kd_week}**",   inline=True)
        e.add_field(name="Fame",     value=f"**{fame:,}**",    inline=True)
        e.add_field(name="Активность", value=f"{activity} `{act_bar}`", inline=True)
        if wk:
            victims = {}
            for ev in wk:
                n = ev.get("Victim", {}).get("Name", "?")
                victims[n] = victims.get(n, 0) + 1
            top = max(victims, key=victims.get)
            e.add_field(name="Любимая жертва", value=f"**{top}** × {victims[top]}", inline=True)
        await interaction.followup.send(embed=e)
    except Exception as ex: await interaction.followup.send(f"❌ {ex}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PRO — BLACKMARKET v2
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BM_ITEMS = {
    # ══ AXES ══════════════════════════════════════════════════
    "battleaxe":      {"Battleaxe":           "T{t}_MAIN_AXE{e}"},
    "greataxe":       {"Greataxe":            "T{t}_2H_GREATAXE{e}"},
    "halberd":        {"Halberd":             "T{t}_2H_HALBERD{e}"},
    "bearpaws":       {"Bear Paws":           "T{t}_2H_DUALAXE_KEEPER{e}"},
    "infernalscythe": {"Infernal Scythe":     "T{t}_2H_SCYTHE_HELL{e}"},
    "carrioncaller":  {"Carrioncaller":       "T{t}_2H_HALBERD_MORGANA{e}"},
    "realmbreaker":   {"Realmbreaker":        "T{t}_2H_REALMBREAKER{e}"},
    # ══ SWORDS ════════════════════════════════════════════════
    "broadsword":     {"Broadsword":          "T{t}_MAIN_SWORD{e}"},
    "claymore":       {"Claymore":            "T{t}_2H_CLAYMORE{e}"},
    "clarentblade":   {"Clarent Blade":       "T{t}_MAIN_BROADSWORD{e}"},
    "dualswords":     {"Dual Swords":         "T{t}_2H_DUALSWORD{e}"},
    "carvingsword":   {"Carving Sword":       "T{t}_2H_CLEAVER_HELL{e}"},
    "galatinepair":   {"Galatine Pair":       "T{t}_2H_DUALSCIMITAR_UNDEAD{e}"},
    # ══ MACES ═════════════════════════════════════════════════
    "mace":           {"Mace":               "T{t}_MAIN_MACE{e}"},
    "heavymace":      {"Heavy Mace":         "T{t}_2H_HEAVYMACE{e}"},
    "morningstar":    {"Morning Star":       "T{t}_MAIN_MORNINGSTAR{e}"},
    "bedrockmace":    {"Bedrock Mace":       "T{t}_MAIN_ROCKMACE_KEEPER{e}"},
    "incubusmace":    {"Incubus Mace":       "T{t}_MAIN_INCUBUS{e}"},
    "camlannmace":    {"Camlann Mace":       "T{t}_MAIN_MACE_HELL{e}"},
    # ══ HAMMERS ═══════════════════════════════════════════════
    "hammer":         {"Hammer":             "T{t}_2H_HAMMER{e}"},
    "polehammer":     {"Polehammer":         "T{t}_2H_POLEHAMMER{e}"},
    "greathammer":    {"Great Hammer":       "T{t}_2H_HAMMER_UNDEAD{e}"},
    "forgehammers":   {"Forge Hammers":      "T{t}_2H_DUALHAMMER_HELL{e}"},
    "tombhammer":     {"Tombhammer":         "T{t}_2H_TOMBHAMMER{e}"},
    "grovekeeper":    {"Grovekeeper":        "T{t}_2H_GROVEKEEPER{e}"},
    # ══ WAR GLOVES ════════════════════════════════════════════
    "brawlergloves":  {"Brawler Gloves":     "T{t}_2H_KNUCKLES{e}"},
    "battlebracers":  {"Battle Bracers":     "T{t}_MAIN_GAUNTLET{e}"},
    "spikedgauntlet": {"Spiked Gauntlet":    "T{t}_MAIN_SPIKEDGAUNTLET{e}"},
    "ursinemaulers":  {"Ursine Maulers":     "T{t}_2H_URSINEHANDSCLAW{e}"},
    "hellfires":      {"Hellfire Hands":     "T{t}_2H_KNUCKLES_HELL{e}"},
    "ravenstrike":    {"Ravenstrike Cestus": "T{t}_MAIN_RAPIER_MORGANA{e}"},
    "fistsofavalon":  {"Fists of Avalon":    "T{t}_2H_FISTOFAVALON{e}"},
    # ══ CROSSBOWS ═════════════════════════════════════════════
    "crossbow":       {"Crossbow":           "T{t}_2H_CROSSBOW{e}"},
    "heavycrossbow":  {"Heavy Crossbow":     "T{t}_2H_HEAVYCROSSBOW{e}"},
    "boltcasters":    {"Boltcasters":        "T{t}_2H_DUALCROSSBOW_HELL{e}"},
    "lightcrossbow":  {"Light Crossbow":     "T{t}_MAIN_LIGHTCROSSBOW{e}"},
    "weepingrepeater":{"Weeping Repeater":   "T{t}_2H_WEEPINGREPEAT{e}"},
    "siegebow":       {"Siegebow":           "T{t}_2H_CROSSBOWLARGE_MORGANA{e}"},
    # ══ BOWS ══════════════════════════════════════════════════
    "bow":            {"Bow":               "T{t}_2H_BOW{e}"},
    "warbow":         {"Warbow":            "T{t}_2H_WARBOW{e}"},
    "longbow":        {"Longbow":           "T{t}_2H_LONGBOW{e}"},
    "whisperingbow":  {"Whispering Bow":    "T{t}_2H_WHISPERING_BOW{e}"},
    "bowofbadon":     {"Bow of Badon":      "T{t}_2H_BOW_KEEPER{e}"},
    "wailingbow":     {"Wailing Bow":       "T{t}_2H_BOW_HELL{e}"},
    "mistpiercer":    {"Mistpiercer":       "T{t}_2H_MISTCALLER{e}"},
    # ══ DAGGERS ═══════════════════════════════════════════════
    "dagger":         {"Dagger":            "T{t}_MAIN_DAGGER{e}"},
    "daggerpair":     {"Dagger Pair":       "T{t}_2H_DAGGERPAIR{e}"},
    "claws":          {"Claws":             "T{t}_2H_CLAWS{e}"},
    "bloodletter":    {"Bloodletter":       "T{t}_MAIN_BLOODLETTER{e}"},
    "demonfang":      {"Demonfang":         "T{t}_MAIN_DEMONFANG{e}"},
    "deathgivers":    {"Deathgivers":       "T{t}_2H_DUALSICKLE_UNDEAD{e}"},
    "bridledfury":    {"Bridled Fury":      "T{t}_2H_BRIDLEDFURY{e}"},
    # ══ SPEARS ════════════════════════════════════════════════
    "spear":          {"Spear":             "T{t}_MAIN_SPEAR{e}"},
    "pike":           {"Pike":              "T{t}_2H_PIKE{e}"},
    "glaive":         {"Glaive":            "T{t}_2H_GLAIVE{e}"},
    "heronspear":     {"Heron Spear":       "T{t}_MAIN_SPEAR_KEEPER{e}"},
    "spirithunter":   {"Spirit Hunter":     "T{t}_2H_HARPOON_HELL{e}"},
    "trinityspear":   {"Trinity Spear":     "T{t}_2H_TRIDENT_UNDEAD{e}"},
    "daybreaker":     {"Daybreaker":        "T{t}_2H_DAYBREAKER{e}"},
    # ══ QUARTERSTAFFS ═════════════════════════════════════════
    "quarterstaff":   {"Quarterstaff":      "T{t}_2H_QUARTERSTAFF{e}"},
    "ironcladstaff":  {"Iron-Clad Staff":   "T{t}_2H_IRONCLADSTAFF{e}"},
    "doublebladed":   {"Double Bladed Staff":"T{t}_2H_DOUBLEBLADEDSTAFF{e}"},
    "soulscythe":     {"Soulscythe":        "T{t}_2H_TWINSCYTHE_HELL{e}"},
    "grailseeker":    {"Grailseeker":       "T{t}_2H_GRAILSEEKER{e}"},
    "sweepingstaff":  {"Sweeping Staff":    "T{t}_2H_SWEEINGSTAFF{e}"},
    # ══ NATURE STAFF ══════════════════════════════════════════
    "naturestaff":    {"Nature Staff":      "T{t}_MAIN_NATURESTAFF{e}"},
    "wildstaff":      {"Wild Staff":        "T{t}_2H_WILDSTAFF{e}"},
    "greatnature":    {"Great Nature Staff":"T{t}_2H_NATURESTAFFGREAT{e}"},
    "druidicstaff":   {"Druidic Staff":     "T{t}_MAIN_NATURESTAFF_KEEPER{e}"},
    "blightstaff":    {"Blight Staff":      "T{t}_2H_BLIGHTSTAFF{e}"},
    "ironrootstaff":  {"Ironroot Staff":    "T{t}_2H_IRONROOTSTAFF{e}"},
    # ══ FIRE STAFF ════════════════════════════════════════════
    "firestaff":      {"Fire Staff":        "T{t}_MAIN_FIRESTAFF{e}"},
    "greatfire":      {"Great Fire Staff":  "T{t}_2H_FIRESTAFF{e}"},
    "infernalstaff":  {"Infernal Staff":    "T{t}_2H_INFERNOSTAFF{e}"},
    "wildfirestaff":  {"Wildfire Staff":    "T{t}_MAIN_FIRESTAFF_KEEPER{e}"},
    "brimstonestaff": {"Brimstone Staff":   "T{t}_2H_FIRESTAFF_HELL{e}"},
    "blazingstaff":   {"Blazing Staff":     "T{t}_2H_BLAZINGSTAFF{e}"},
    # ══ HOLY STAFF ════════════════════════════════════════════
    "holystaff":      {"Holy Staff":        "T{t}_MAIN_HOLYSTAFF{e}"},
    "greatholly":     {"Great Holy Staff":  "T{t}_2H_HOLYSTAFF{e}"},
    "divinestaff":    {"Divine Staff":      "T{t}_2H_DIVINESTAFF{e}"},
    "lifetouchstaff": {"Lifetouch Staff":   "T{t}_MAIN_LIFETOUCH{e}"},
    "fallenstaff":    {"Fallen Staff":      "T{t}_2H_HOLYSTAFF_HELL{e}"},
    "redemptionstaff":{"Redemption Staff":  "T{t}_2H_REDEMPTIONSTAFF{e}"},
    # ══ ARCANE STAFF ══════════════════════════════════════════
    "arcanestaff":    {"Arcane Staff":      "T{t}_MAIN_ARCANESTAFF{e}"},
    "greatarcane":    {"Great Arcane Staff":"T{t}_2H_ARCANESTAFF{e}"},
    "enigmaticstaff": {"Enigmatic Staff":   "T{t}_2H_ENIGMATICSTAFF{e}"},
    "witchworkstaff": {"Witchwork Staff":   "T{t}_MAIN_ARCANESTAFF_UNDEAD{e}"},
    "evensong":       {"Evensong":          "T{t}_2H_EVENSONG{e}"},
    "occultstaff":    {"Occult Staff":      "T{t}_2H_ARCANESTAFF_HELL{e}"},
    # ══ FROST STAFF ═══════════════════════════════════════════
    "froststaff":     {"Frost Staff":       "T{t}_MAIN_FROSTSTAFF{e}"},
    "greatfrost":     {"Great Frost Staff": "T{t}_2H_FROSTSTAFF{e}"},
    "glacialstaff":   {"Glacial Staff":     "T{t}_2H_GLACIALSTAFF{e}"},
    "hoarfroststaff": {"Hoarfrost Staff":   "T{t}_MAIN_FROSTSTAFF_KEEPER{e}"},
    "iciclestaff":    {"Icicle Staff":      "T{t}_2H_ICESTAFFFIRE{e}"},
    "permafrost":     {"Permafrost Staff":  "T{t}_2H_PERMAFROSTSTAFF{e}"},
    # ══ CURSED STAFF ══════════════════════════════════════════
    "cursedstaff":    {"Cursed Staff":      "T{t}_MAIN_CURSEDSTAFF{e}"},
    "greatcursed":    {"Great Cursed Staff":"T{t}_2H_CURSEDSTAFF{e}"},
    "demonicstaff":   {"Demonic Staff":     "T{t}_2H_DEMONICSTAFF{e}"},
    "cursedskull":    {"Cursed Skull":      "T{t}_MAIN_CURSEDSTAFF_UNDEAD{e}"},
    "lifecursestaff": {"Lifecurse Staff":   "T{t}_MAIN_LIFECURSESTAFF{e}"},
    "damnationstaff": {"Damnation Staff":   "T{t}_2H_CURSEDSTAFF_MORGANA{e}"},
    # ══ OFF-HAND ══════════════════════════════════════════════
    "shield":         {"Shield":            "T{t}_OFFHAND_SHIELD{e}"},
    "sarcophagus":    {"Sarcophagus":       "T{t}_OFFHAND_SHIELD_UNDEAD{e}"},
    "caitiffshield":  {"Caitiff Shield":    "T{t}_OFFHAND_SHIELD_HELL{e}"},
    "facebreaker":    {"Facebreaker":       "T{t}_OFFHAND_FACEBREAKER{e}"},
    "torch":          {"Torch":             "T{t}_OFFHAND_TORCH{e}"},
    "mistcaller":     {"Mistcaller":        "T{t}_OFFHAND_MISTCALLER{e}"},
    "leeringcane":    {"Leering Cane":      "T{t}_OFFHAND_LEERINGCANE{e}"},
    "taproot":        {"Taproot":           "T{t}_OFFHAND_TAPROOT{e}"},
    "muisak":         {"Muisak":            "T{t}_OFFHAND_MUISAK{e}"},
    "cryptcandle":    {"Cryptcandle":       "T{t}_OFFHAND_CRYPTCANDLE{e}"},
    "tomeofspells":   {"Tome of Spells":    "T{t}_OFFHAND_BOOK{e}"},
    # ══ PLATE ARMOR ═══════════════════════════════════════════
    "soldierhelm":    {"Soldier Helmet":    "T{t}_HEAD_PLATE_SET1{e}"},
    "soldierarmor":   {"Soldier Armor":     "T{t}_ARMOR_PLATE_SET1{e}"},
    "soldierboots":   {"Soldier Boots":     "T{t}_SHOES_PLATE_SET1{e}"},
    "knighthelm":     {"Knight Helmet":     "T{t}_HEAD_PLATE_SET2{e}"},
    "knightarmor":    {"Knight Armor":      "T{t}_ARMOR_PLATE_SET2{e}"},
    "knightboots":    {"Knight Boots":      "T{t}_SHOES_PLATE_SET2{e}"},
    "guardianhelm":   {"Guardian Helmet":   "T{t}_HEAD_PLATE_SET3{e}"},
    "guardianarmor":  {"Guardian Armor":    "T{t}_ARMOR_PLATE_SET3{e}"},
    "guardianboots":  {"Guardian Boots":    "T{t}_SHOES_PLATE_SET3{e}"},
    "graveguardhelm": {"Graveguard Helmet": "T{t}_HEAD_PLATE_UNDEAD{e}"},
    "graveguardarmor":{"Graveguard Armor":  "T{t}_ARMOR_PLATE_UNDEAD{e}"},
    "graveguardboots":{"Graveguard Boots":  "T{t}_SHOES_PLATE_UNDEAD{e}"},
    "judicatorhelm":  {"Judicator Helmet":  "T{t}_HEAD_PLATE_HELL{e}"},
    "judicatorarmor": {"Judicator Armor":   "T{t}_ARMOR_PLATE_HELL{e}"},
    "judicatorboots": {"Judicator Boots":   "T{t}_SHOES_PLATE_HELL{e}"},
    "demonhelm":      {"Demon Helmet":      "T{t}_HEAD_PLATE_MORGANA{e}"},
    "demonarmor":     {"Demon Armor":       "T{t}_ARMOR_PLATE_MORGANA{e}"},
    "demonboots":     {"Demon Boots":       "T{t}_SHOES_PLATE_MORGANA{e}"},
    # ══ LEATHER ARMOR ═════════════════════════════════════════
    "hunterhelm":     {"Hunter Hood":       "T{t}_HEAD_LEATHER_SET1{e}"},
    "hunterjacket":   {"Hunter Jacket":     "T{t}_ARMOR_LEATHER_SET1{e}"},
    "huntershoes":    {"Hunter Shoes":      "T{t}_SHOES_LEATHER_SET1{e}"},
    "assassinhelm":   {"Assassin Hood":     "T{t}_HEAD_LEATHER_SET2{e}"},
    "assassinjacket": {"Assassin Jacket":   "T{t}_ARMOR_LEATHER_SET2{e}"},
    "assassinshoes":  {"Assassin Shoes":    "T{t}_SHOES_LEATHER_SET2{e}"},
    "mercenaryhelm":  {"Mercenary Hood":    "T{t}_HEAD_LEATHER_SET3{e}"},
    "mercenaryarmor": {"Mercenary Jacket":  "T{t}_ARMOR_LEATHER_SET3{e}"},
    "mercenaryboots": {"Mercenary Shoes":   "T{t}_SHOES_LEATHER_SET3{e}"},
    "hellionhelm":    {"Hellion Hood":      "T{t}_HEAD_LEATHER_UNDEAD{e}"},
    "hellionjacket":  {"Hellion Jacket":    "T{t}_ARMOR_LEATHER_UNDEAD{e}"},
    "hellionshoes":   {"Hellion Shoes":     "T{t}_SHOES_LEATHER_UNDEAD{e}"},
    "specterhelm":    {"Specter Hood":      "T{t}_HEAD_LEATHER_HELL{e}"},
    "specterjacket":  {"Specter Jacket":    "T{t}_ARMOR_LEATHER_HELL{e}"},
    "spectershoes":   {"Specter Shoes":     "T{t}_SHOES_LEATHER_HELL{e}"},
    "mistwalkerhelm": {"Mistwalker Hood":   "T{t}_HEAD_LEATHER_MORGANA{e}"},
    "mistwalkerjacket":{"Mistwalker Jacket":"T{t}_ARMOR_LEATHER_MORGANA{e}"},
    "mistwalkershoes":{"Mistwalker Shoes":  "T{t}_SHOES_LEATHER_MORGANA{e}"},
    # ══ CLOTH ARMOR ═══════════════════════════════════════════
    "scholarcowl":    {"Scholar Cowl":      "T{t}_HEAD_CLOTH_SET1{e}"},
    "scholarrobe":    {"Scholar Robe":      "T{t}_ARMOR_CLOTH_SET1{e}"},
    "scholarsandals": {"Scholar Sandals":   "T{t}_SHOES_CLOTH_SET1{e}"},
    "clericcowl":     {"Cleric Cowl":       "T{t}_HEAD_CLOTH_SET2{e}"},
    "clericrobe":     {"Cleric Robe":       "T{t}_ARMOR_CLOTH_SET2{e}"},
    "clericsandals":  {"Cleric Sandals":    "T{t}_SHOES_CLOTH_SET2{e}"},
    "magecowl":       {"Mage Cowl":         "T{t}_HEAD_CLOTH_SET3{e}"},
    "magerobe":       {"Mage Robe":         "T{t}_ARMOR_CLOTH_SET3{e}"},
    "magesandals":    {"Mage Sandals":      "T{t}_SHOES_CLOTH_SET3{e}"},
    "cultistcowl":    {"Cultist Cowl":      "T{t}_HEAD_CLOTH_UNDEAD{e}"},
    "cultistrobe":    {"Cultist Robe":      "T{t}_ARMOR_CLOTH_UNDEAD{e}"},
    "cultistsandals": {"Cultist Sandals":   "T{t}_SHOES_CLOTH_UNDEAD{e}"},
    "feyscalehat":    {"Feyscale Hat":      "T{t}_HEAD_CLOTH_HELL{e}"},
    "feyscalerobe":   {"Feyscale Robe":     "T{t}_ARMOR_CLOTH_HELL{e}"},
    "feyscalesandals":{"Feyscale Sandals":  "T{t}_SHOES_CLOTH_HELL{e}"},
    # ══ BAGS ══════════════════════════════════════════════════
    "bag":            {"Bag":               "T{t}_BAG{e}"},
    "bagofinsight":   {"Bag of Insight":    "T{t}_BAG_INSIGHT{e}"},
}

# Группы для /blackmarket
BM_GROUPS = {
    "weapon": [
        "battleaxe","greataxe","halberd","bearpaws","infernalscythe","carrioncaller","realmbreaker",
        "broadsword","claymore","clarentblade","dualswords","carvingsword","galatinepair",
        "mace","heavymace","morningstar","bedrockmace","incubusmace","camlannmace",
        "hammer","polehammer","greathammer","forgehammers","tombhammer","grovekeeper",
        "brawlergloves","battlebracers","spikedgauntlet","ursinemaulers","hellfires","ravenstrike","fistsofavalon",
        "crossbow","heavycrossbow","boltcasters","lightcrossbow","weepingrepeater","siegebow",
        "bow","warbow","longbow","whisperingbow","bowofbadon","wailingbow","mistpiercer",
        "dagger","daggerpair","claws","bloodletter","demonfang","deathgivers","bridledfury",
        "spear","pike","glaive","heronspear","spirithunter","trinityspear","daybreaker",
        "quarterstaff","ironcladstaff","doublebladed","soulscythe","grailseeker","sweepingstaff",
        "naturestaff","wildstaff","greatnature","druidicstaff","blightstaff","ironrootstaff",
        "firestaff","greatfire","infernalstaff","wildfirestaff","brimstonestaff","blazingstaff",
        "holystaff","greatholly","divinestaff","lifetouchstaff","fallenstaff","redemptionstaff",
        "arcanestaff","greatarcane","enigmaticstaff","witchworkstaff","evensong","occultstaff",
        "froststaff","greatfrost","glacialstaff","hoarfroststaff","iciclestaff","permafrost",
        "cursedstaff","greatcursed","demonicstaff","cursedskull","lifecursestaff","damnationstaff",
    ],
    "offhand": [
        "shield","sarcophagus","caitiffshield","facebreaker",
        "torch","mistcaller","leeringcane","taproot","muisak","cryptcandle","tomeofspells",
    ],
    "armor_plate": [
        "soldierhelm","soldierarmor","soldierboots",
        "knighthelm","knightarmor","knightboots",
        "guardianhelm","guardianarmor","guardianboots",
        "graveguardhelm","graveguardarmor","graveguardboots",
        "judicatorhelm","judicatorarmor","judicatorboots",
        "demonhelm","demonarmor","demonboots",
    ],
    "armor_leather": [
        "hunterhelm","hunterjacket","huntershoes",
        "assassinhelm","assassinjacket","assassinshoes",
        "mercenaryhelm","mercenaryarmor","mercenaryboots",
        "hellionhelm","hellionjacket","hellionshoes",
        "specterhelm","specterjacket","spectershoes",
        "mistwalkerhelm","mistwalkerjacket","mistwalkershoes",
    ],
    "armor_cloth": [
        "scholarcowl","scholarrobe","scholarsandals",
        "clericcowl","clericrobe","clericsandals",
        "magecowl","magerobe","magesandals",
        "cultistcowl","cultistrobe","cultistsandals",
        "feyscalehat","feyscalerobe","feyscalesandals",
    ],
    "bag": ["bag","bagofinsight"],
}


def build_item_id(template, tier, enchant):
    return template.format(t=tier, e=f"@{enchant}" if enchant > 0 else "")


# ── Albion серверы ────────────────────────────────────────────
ALBION_SERVERS = {
    "eu":   "https://west.albion-online-data.com/api/v2",
    "us":   "https://east.albion-online-data.com/api/v2",
    "asia": "https://east.albion-online-data.com/api/v2",  # Asia uses east endpoint
}
ALBION_SERVER_NAMES = {"eu": "🇪🇺 Европа", "us": "🇺🇸 Америка", "asia": "🌏 Азия"}

# Все города кроме ЧР и Бреккилена
CITY_LOCATIONS = ["Bridgewatch", "Fort Sterling", "Lymhurst", "Martlock", "Thetford"]  # Caerleon исключён
CITY_NAMES_RU = {
    "Caerleon": "Кэрлеон",
    "Bridgewatch": "Бриджвотч",
    "Fort Sterling": "Форт Стерлинг",
    "Lymhurst": "Лимхёрст",
    "Martlock": "Мартлок",
    "Thetford": "Тетфорд",
    "Brecilien": "Бреккилен",
    "Black Market": "Чёрный рынок",
}

# Иконки предметов через Albion render API
def item_icon_url(item_id: str) -> str:
    return f"https://render.albiononline.com/v1/item/{item_id}.png?size=50"

async def fetch_bm_prices(category_keys: list, tier: int, server: str = "eu") -> list:
    """
    Возвращает список dict с детальными ценами по каждому городу.
    server: eu / us / asia
    """
    base_url = ALBION_SERVERS.get(server, ALBION_SERVERS["eu"])
    results = []

    # ВАЖНО: пробелы в названиях городов кодируем через %20, не +
    # Albion Data API чувствителен к этому
    all_locations = ",".join([
        "Black Market", "Brecilien",
        "Bridgewatch", "Fort Sterling",
        "Lymhurst", "Martlock", "Thetford"
    ])
    # URL-encode пробелы
    locations_param = all_locations.replace(" ", "%20")

    print(f"[BM DEBUG] Starting fetch: keys={len(category_keys)}, tier={tier}, server={server}")
    fetched = 0
    found = 0

    async with aiohttp.ClientSession() as s:
        for key in category_keys:
            item_data = BM_ITEMS.get(key)
            if not item_data: continue
            display, template = list(item_data.items())[0]

            for enchant in range(0, 5):
                item_id = build_item_id(template, tier, enchant)
                tier_label = f"{tier}.{enchant}"
                url = f"{base_url}/stats/prices/{item_id}?locations={locations_param}"

                try:
                    async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                        fetched += 1
                        if r.status != 200:
                            print(f"[BM DEBUG] {item_id}: HTTP {r.status}")
                            continue
                        prices = await r.json()
                except Exception as ex:
                    print(f"[BM DEBUG] {item_id}: request error: {ex}")
                    continue

                if not prices:
                    continue

                bm_price = 0
                brec_sell = 0
                brec_buy = 0
                city_data = {}
                bm_updated = ""   # дата обновления цены ЧР

                for p in prices:
                    city = p.get("city", "").strip()
                    sell = p.get("sell_price_min", 0) or 0
                    buy  = p.get("buy_price_max", 0) or 0
                    upd  = p.get("sell_price_min_date", "") or ""

                    if city == "Black Market":
                        if sell > 0:
                            bm_price = max(bm_price, sell)
                            bm_updated = upd  # сохраняем дату обновления
                    elif city == "Brecilien":
                        if sell > 0: brec_sell = sell
                        if buy > 0:  brec_buy  = buy
                    elif city in CITY_LOCATIONS:
                        if sell > 0 or buy > 0:
                            city_data[city] = {"sell": sell, "buy": buy}

                # Если нет цены на ЧР — предмет не торгуется там
                if bm_price == 0:
                    print(f"[BM DEBUG] {item_id}: no Black Market price, skip")
                    continue

                # Проверяем возраст данных
                data_age_hours = None
                data_stale = False
                if bm_updated:
                    try:
                        upd_dt = datetime.datetime.fromisoformat(bm_updated.replace("Z", "+00:00").replace("+00:00", ""))
                        data_age_hours = round((datetime.datetime.utcnow() - upd_dt).total_seconds() / 3600, 1)
                        data_stale = data_age_hours > 3  # данные старше 3 часов — предупреждение
                    except Exception:
                        pass

                # Лучший город — минимальная цена продажи
                best_city = None
                best_city_sell = 9_999_999_999
                best_city_buy  = 0

                for city, pd in city_data.items():
                    sell = pd["sell"]
                    buy  = pd["buy"]
                    if sell > 0 and sell < best_city_sell:
                        best_city_sell = sell
                        best_city      = city
                        best_city_buy  = buy

                # Если нет цены в городах — используем ордер на покупку (buy)
                if not best_city:
                    for city, pd in city_data.items():
                        buy = pd["buy"]
                        if buy > 0 and buy < best_city_sell:
                            best_city_sell = buy
                            best_city      = city
                            best_city_buy  = buy

                city_sell_profit = bm_price - best_city_sell if best_city and best_city_sell < 9_999_999_999 else 0
                city_sell_pct    = round(city_sell_profit / best_city_sell * 100, 1) if city_sell_profit > 0 and best_city_sell > 0 else 0

                brec_price = brec_sell if brec_sell > 0 else brec_buy
                brec_is_buy_order = brec_buy > 0 and brec_sell == 0
                brec_profit = bm_price - brec_price if brec_price > 0 else 0
                brec_pct    = round(brec_profit / brec_price * 100, 1) if brec_price > 0 else 0

                found += 1
                print(f"[BM DEBUG] {item_id}: BM={bm_price} age={data_age_hours}h stale={data_stale} best_city={best_city}({best_city_sell if best_city_sell < 9_999_999_999 else 0}) profit={city_sell_profit}({city_sell_pct}%)")

                results.append({
                    "name":              f"{display} {tier_label}",
                    "display":           display,
                    "tier_label":        tier_label,
                    "item_id":           item_id,
                    "icon_url":          item_icon_url(item_id),
                    "bm":                bm_price,
                    "best_city":         best_city,
                    "best_city_sell":    best_city_sell if best_city_sell < 9_999_999_999 else 0,
                    "best_city_buy":     best_city_buy,
                    "city_profit":       city_sell_profit,
                    "city_pct":          city_sell_pct,
                    "city_data":         city_data,
                    "brec_price":        brec_price,
                    "brec_is_buy_order": brec_is_buy_order,
                    "brec_profit":       brec_profit,
                    "brec_pct":          brec_pct,
                    "data_age_hours":    data_age_hours,
                    "data_stale":        data_stale,
                })

    print(f"[BM DEBUG] Done: fetched={fetched}, results={found}")
    results.sort(key=lambda x: x["city_pct"], reverse=True)
    return results


@bot.tree.command(name="blackmarket", description="Albion: профит Чёрного рынка [Pro]")
@app_commands.describe(
    category="weapon/offhand/armor_plate/armor_leather/armor_cloth/bag или ключ предмета",
    tier="Тир: 6, 7 или 8",
    server="Сервер: eu / us / asia",
    sheets="Экспорт в Google Sheets: yes / no",
)
@cooldown(30)
async def blackmarket(
    interaction: discord.Interaction,
    category: str = "weapon",
    tier: int = 8,
    server: str = "eu",
    sheets: str = "no",
):
    if await get_tier(interaction.guild_id) < TIER_PRO:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)
    await interaction.response.defer()
    await run_blackmarket(interaction, category, tier, server, sheets)


async def run_blackmarket(interaction: discord.Interaction,
                           category: str = "weapon", tier: int = 8,
                           server: str = "eu", sheets: str = "no"):
    """
    Общая логика /blackmarket. Вынесена отдельно, потому что её вызывает
    ещё и Select Menu смены категории (BlackmarketCategoryView).
    Ответ на interaction должен быть уже отправлен вызывающим кодом.
    """
    if tier not in (6, 7, 8):
        return await interaction.followup.send("❌ Тир: 6, 7 или 8")
    if server not in ALBION_SERVERS:
        return await interaction.followup.send("❌ Сервер: eu / us / asia")

    cat_lower = category.lower()
    if cat_lower in BM_GROUPS:
        keys = BM_GROUPS[cat_lower]
        cat_label = cat_lower.capitalize()
    elif cat_lower in BM_ITEMS:
        keys = [cat_lower]
        cat_label = list(BM_ITEMS[cat_lower].keys())[0]
    else:
        avail = ", ".join(f"`{k}`" for k in list(BM_GROUPS.keys()) + list(BM_ITEMS.keys())[:8]) + "..."
        return await interaction.followup.send(f"❌ Неизвестная категория. Примеры: {avail}")

    server_name = ALBION_SERVER_NAMES[server]
    await interaction.followup.send(
        f"⏳ Загружаю цены **{cat_label} T{tier}** · {server_name}... (~20 сек)"
    )

    results = await fetch_bm_prices(keys, tier, server)

    if not results:
        return await interaction.channel.send("❌ Нет данных о ценах. Попробуй позже.")

    # ── Строим страницы с пагинацией (5 предметов на страницу) ──
    top = results[:20]
    stale_count = sum(1 for item in top if item.get("data_stale"))

    def build_bm_page(items_chunk: list, page_num: int, total_pages: int) -> discord.Embed:
        color = profit_color(items_chunk[0]["city_pct"] if items_chunk else 0)
        desc_lines = f"{server_name} · Топ по % профиту\n⚠️ Цены ЧР приблизительные — проверяй в игре перед продажей!"
        if stale_count:
            desc_lines += f"\n⚠️ **{stale_count} предметов** с устаревшими данными (>3ч)"
        e = make_embed(
            title=f"💰 Чёрный рынок — {cat_label} T{tier}",
            description=desc_lines,
            color=color,
            footer=f"albion-online-data.com · {len(results)} предметов · стр. {page_num}/{total_pages}"
        )
        for item in items_chunk:
            city_ru = CITY_NAMES_RU.get(item["best_city"], item["best_city"]) if item["best_city"] else "—"
            age_h = item.get("data_age_hours")
            stale = item.get("data_stale", False)
            age_str = f"{'⚠️' if stale else '🕐'} {age_h}ч" if age_h is not None else "🕐 ?"

            if item["best_city"]:
                city_line = (
                    f"🏙️ **{city_ru}**: `{item['best_city_sell']:,}` → `{item['bm']:,}` "
                    f"(**+{item['city_pct']}%** / {item['city_profit']:,})"
                )
            else:
                city_line = "🏙️ Нет цены в городах"

            if item["brec_price"] > 0:
                brec_type = "ord" if item["brec_is_buy_order"] else "mkt"
                brec_line = (
                    f"🌿 Брек [{brec_type}]: `{item['brec_price']:,}` → `{item['bm']:,}` "
                    f"(**+{item['brec_pct']}%**)"
                )
            else:
                brec_line = "🌿 Брек: нет данных"

            icon = "⚠️" if stale else ("🟢" if item["city_pct"] >= 20 else "🟡" if item["city_pct"] >= 5 else "🔴")
            e.add_field(
                name=f"{icon} {item['name']} {age_str}",
                value=f"{city_line}\n{brec_line}",
                inline=False
            )
        if top:
            e.set_thumbnail(url=top[0]["icon_url"])
        return e

    chunks = [top[i:i+5] for i in range(0, len(top), 5)]
    pages = [build_bm_page(chunk, i+1, len(chunks)) for i, chunk in enumerate(chunks)]

    view = PaginatedView(pages)
    # Добавляем Select Menu для смены категории
    cat_view = BlackmarketCategoryView(tier, server, cat_lower)
    for item in cat_view.children:
        view.add_item(item)

    await interaction.channel.send(embed=pages[0], view=view)

    # ── Google Sheets export ───────────────────────────────────
    if sheets.lower() in ("yes", "да", "y"):
        if not GOOGLE_CREDS:
            await interaction.channel.send(
                "❌ Google Sheets: добавь `GOOGLE_CREDENTIALS` в .env\n"
                "Как получить: console.cloud.google.com → Service Accounts → Create Key (JSON) → скопируй содержимое одной строкой"
            )
            return
        if not SHEET_ID:
            await interaction.channel.send(
                "❌ Google Sheets: добавь `SHEET_ID` в .env\n"
                "Это ID из URL таблицы: `docs.google.com/spreadsheets/d/**ВОТ_ЭТО**/edit`"
            )
            return
        try:
            import gspread
            from google.oauth2.service_account import Credentials as GCredentials
            import json as _json

            # GOOGLE_CREDENTIALS должен быть JSON строкой или base64
            # НЕ пытаемся открывать как файл — в Railway это всегда строка
            raw = GOOGLE_CREDS.strip()

            creds_data = None
            if raw.startswith("{"):
                # Прямой JSON
                creds_data = _json.loads(raw)
            else:
                # Попробуем base64
                try:
                    import base64
                    decoded = base64.b64decode(raw + "==").decode("utf-8")
                    creds_data = _json.loads(decoded)
                except Exception:
                    await interaction.channel.send(
                        "❌ Google Sheets: не удалось разобрать `GOOGLE_CREDENTIALS`\n"
                        "Значение должно быть JSON содержимым файла одной строкой.\n"
                        "**Как сделать правильно:**\n"
                        "1. Скачай JSON ключ из Google Cloud Console\n"
                        "2. Открой файл текстовым редактором\n"
                        "3. Скопируй **всё содержимое** и вставь в переменную `GOOGLE_CREDENTIALS` в Railway\n"
                        "Значение должно начинаться с `{\"type\": \"service_account\"...`"
                    )
                    return

            creds = GCredentials.from_service_account_info(
                creds_data,
                scopes=[
                    "https://spreadsheets.google.com/feeds",
                    "https://www.googleapis.com/auth/drive",
                ]
            )
            gc = gspread.authorize(creds)
            sh = gc.open_by_key(SHEET_ID)

            tab_name = f"BM T{tier} {cat_label} {server.upper()}"
            try:
                ws = sh.worksheet(tab_name)
                ws.clear()
            except gspread.WorksheetNotFound:
                ws = sh.add_worksheet(title=tab_name, rows=300, cols=15)

            header = [
                "Предмет", "Тир.Зач", "Item ID",
                "ЧР цена (рынок)",
                "Лучший город", "Цена в городе (рынок)", "Ордер покупки (город)",
                "Профит (город)", "% профит (город)",
                "Цена в Бреккилене", "Тип цены (Брек)",
                "Профит (Брек)", "% профит (Брек)",
                # Все города отдельно
                "Кэрлеон (продажа)", "Бриджвотч (продажа)", "Форт Стерлинг (продажа)",
                "Лимхёрст (продажа)", "Мартлок (продажа)", "Тетфорд (продажа)",
            ]
            rows = [header]

            for item in results:
                city_data = item.get("city_data", {})
                row = [
                    item["display"],
                    item["tier_label"],
                    item["item_id"],
                    item["bm"],
                    CITY_NAMES_RU.get(item["best_city"], item["best_city"] or "—"),
                    item["best_city_sell"] or "—",
                    item["best_city_buy"] or "—",
                    item["city_profit"] or "—",
                    f"{item['city_pct']}%" if item["city_pct"] else "—",
                    item["brec_price"] or "—",
                    "ордер покупки" if item["brec_is_buy_order"] else "рынок продажи",
                    item["brec_profit"] or "—",
                    f"{item['brec_pct']}%" if item["brec_pct"] else "—",
                    # По городам
                    city_data.get("Caerleon", {}).get("sell", "—") or "—",
                    city_data.get("Bridgewatch", {}).get("sell", "—") or "—",
                    city_data.get("Fort Sterling", {}).get("sell", "—") or "—",
                    city_data.get("Lymhurst", {}).get("sell", "—") or "—",
                    city_data.get("Martlock", {}).get("sell", "—") or "—",
                    city_data.get("Thetford", {}).get("sell", "—") or "—",
                ]
                rows.append(row)

            ws.update("A1", rows)
            ws.format("A1:S1", {
                "textFormat": {"bold": True},
                "backgroundColor": {"red": 0.15, "green": 0.15, "blue": 0.25},
            })

            sheet_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
            await interaction.channel.send(
                f"📊 **Google Sheets обновлён!**\n"
                f"{sheet_url}\n"
                f"Вкладка: **{tab_name}** · {len(results)} строк · {server_name}"
            )

        except ImportError:
            await interaction.channel.send("❌ Установи: `pip install gspread google-auth`")
        except _json.JSONDecodeError as ex:
            await interaction.channel.send(
                f"❌ Google Sheets: не удалось разобрать JSON из `GOOGLE_CREDENTIALS`\n"
                f"Убедись что значение — это содержимое JSON-файла одной строкой без переносов\n"
                f"Ошибка: `{ex}`"
            )
        except Exception as ex:
            await interaction.channel.send(f"❌ Ошибка Google Sheets: `{ex}`")


@bot.tree.command(name="party", description="Albion: анализ пати для статика [Pro]")
@app_commands.describe(p1="Игрок 1", p2="Игрок 2", p3="Игрок 3", p4="Игрок 4", p5="Игрок 5")
@cooldown(15)
async def party(interaction: discord.Interaction, p1: str, p2: str, p3: str, p4: str = None, p5: str = None):
    if await get_tier(interaction.guild_id)<TIER_PRO: return await interaction.response.send_message(embed=upsell_embed("Pro"),ephemeral=True)
    await interaction.response.defer()
    players=[p for p in [p1,p2,p3,p4,p5] if p]
    e = build_embed(C.SUCCESS, footer="EU · Albion Online")
    e.set_author(name=f"Анализ группы — {len(players)} игроков")
    total_kf=total_df=found=0; lines=[]
    async with aiohttp.ClientSession() as s:
        for name in players:
            pid,pname = await albion_find_player(s,name)
            if not pid: lines.append(f"❌ **{name}** — не найден"); continue
            async with s.get(f"{ALBION_BASE}/players/{pid}") as r: p = await r.json()
            kf,df = p.get("KillFame",0),p.get("DeathFame",0)
            total_kf+=kf; total_df+=df; found+=1
            lines.append(f"✅ **{pname}** [{p.get('GuildName') or '—'}]\n   K/D: `{round(kf/df,2) if df else '∞'}` · Fame: `{kf:,}`")
    e.description="\n".join(lines)
    if found>0:
        avg_kd=round(total_kf/total_df,2) if total_df else "∞"
        e.add_field(name="📊 Статистика", value=f"Найдено: **{found}/{len(players)}**\nFame: **{total_kf:,}**\nAvg K/D: **{avg_kd}**", inline=False)
        try:
            verdict = await ask_ai(f"2-sentence verdict on Albion static dungeon party: {found} players, kill fame {total_kf:,}, avg K/D {avg_kd}.", system="Albion Online expert. Brief game advice.")
            e.add_field(name="🤖 AI Вердикт", value=verdict, inline=False)
        except Exception: pass
    await interaction.followup.send(embed=e)

@bot.tree.command(name="tournament")
@app_commands.describe(name="Название", participants="Участники через запятую")
async def tournament(interaction: discord.Interaction, name: str, participants: str):
    if await get_tier(interaction.guild_id)<TIER_PRO: return await interaction.response.send_message(embed=upsell_embed("Pro"),ephemeral=True)
    players=[p.strip() for p in participants.split(",") if p.strip()]
    if len(players)<2: return await interaction.response.send_message("❌ Минимум 2 участника.", ephemeral=True)
    random.shuffle(players)
    matchups=[f"⚔️ **{players[i]}** vs **{players[i+1]}**" for i in range(0,len(players)-1,2)]
    if len(players)%2: matchups.append(f"👤 **{players[-1]}** — BYE")
    e = discord.Embed(title=f"🏆 {name}", color=0xFFD700)
    e.add_field(name=f"Раунд 1 ({len(matchups)} матчей)", value="\n".join(matchups), inline=False)
    e.set_footer(text=f"Создал {interaction.user.display_name} · Witness Pro")
    await interaction.response.send_message(embed=e)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ⚔️ ALBION: CRAFT CALCULATOR
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Коэффициенты материалов для крафта (упрощённые, стандартные)
CRAFT_MATERIALS = {
    6: {"ore": 16, "wood": 16, "fiber": 16, "hide": 16, "rock": 16},
    7: {"ore": 20, "wood": 20, "fiber": 20, "hide": 20, "rock": 20},
    8: {"ore": 24, "wood": 24, "fiber": 24, "hide": 24, "rock": 24},
}
MATERIAL_ITEMS = {
    6: {"ore": "T6_ORE", "wood": "T6_WOOD", "fiber": "T6_FIBER", "hide": "T6_HIDE", "rock": "T6_ROCK"},
    7: {"ore": "T7_ORE", "wood": "T7_WOOD", "fiber": "T7_FIBER", "hide": "T7_HIDE", "rock": "T7_ROCK"},
    8: {"ore": "T8_ORE", "wood": "T8_WOOD", "fiber": "T8_FIBER", "hide": "T8_HIDE", "rock": "T8_ROCK"},
}
ITEM_MATERIAL_TYPE = {
    "sword": "ore", "claymore": "ore", "axe": "ore", "mace": "ore",
    "hammer": "ore", "crossbow": "ore", "shield": "ore", "spear": "ore",
    "bow": "wood", "quarterstaff": "wood", "naturestaff": "wood",
    "firestaff": "wood", "holystaff": "wood", "torch": "wood",
    "mercjacket": "ore", "mercboots": "ore", "merchood": "ore",
    "assjacket": "hide", "assboots": "hide", "asshood": "hide",
    "huntjacket": "fiber", "huntboots": "fiber", "hunthood": "fiber",
    "bag": "fiber",
}

@bot.tree.command(name="craftcalc", description="Таблица крафта Albion → Google Sheets [Pro]")
@app_commands.describe(
    tier="Тир: 6, 7 или 8",
    server="Сервер: eu / us / asia",
    tax="Налог рынка: 8 (обычный) или 4 (премиум город)"
)
@cooldown(20)
async def craftcalc(interaction: discord.Interaction, tier: int = 8, server: str = "eu", tax: int = 8):
    if await get_tier(interaction.guild_id) < TIER_PRO:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)
    await interaction.response.defer()

    if not GOOGLE_CREDS or not SHEET_ID:
        e = discord.Embed(title="⚙️ Настройка Google Sheets", color=0xFF4444)
        e.description = (
            "Для использования `/craftcalc` нужно настроить Google Sheets.\n\n"
            "**1.** Зайди в [Google Cloud Console](https://console.cloud.google.com)\n"
            "**2.** Создай Service Account → скачай JSON ключ\n"
            "**3.** Открой JSON файл, скопируй содержимое\n"
            "**4.** В Railway Variables добавь:\n"
            "`GOOGLE_CREDENTIALS` = содержимое JSON одной строкой\n"
            "`SHEET_ID` = ID таблицы из URL\n\n"
            "**5.** Дай сервис-аккаунту доступ Editor к таблице"
        )
        # defer уже выполнен выше — отвечаем через followup
        return await interaction.followup.send(embed=e, ephemeral=True)

    if tier not in (6, 7, 8):
        return await interaction.followup.send("❌ Тир: 6, 7 или 8")
    if tax not in (4, 8):
        return await interaction.followup.send("❌ Налог: 4 или 8")

    tax_rate = tax / 100
    server_name = ALBION_SERVER_NAMES.get(server, "EU")
    base_url = ALBION_SERVERS.get(server, ALBION_SERVERS["eu"])
    locations = "Black%20Market,Bridgewatch,Fort%20Sterling,Lymhurst,Martlock,Thetford,Brecilien"

    # Генерируем список из BM_ITEMS автоматически — материал определяем по типу предмета
    MAT_BY_TYPE = {
        "MAIN_SWORD":"ore","2H_CLAYMORE":"ore","2H_DUALSWORD":"ore","MAIN_BROADSWORD":"ore",
        "2H_ENERGYSHAPER":"ore","MAIN_DAGGER":"ore","2H_DAGGERPAIR":"ore","2H_CLAWS":"ore",
        "2H_DEATHGIGGLES":"ore","MAIN_AXE":"ore","2H_GREATAXE":"ore","2H_HALBERD":"ore",
        "2H_CARRIONCALLER":"ore","2H_DUALSCYTHE":"ore","MAIN_MACE":"ore","2H_HEAVYMACE":"ore",
        "MAIN_MORNINGSTAR":"ore","MAIN_INCUBUS":"ore","2H_ONEMACELAIR":"ore","2H_HAMMER":"ore",
        "2H_POLEHAMMER":"ore","2H_GRAILSEEKER":"ore","2H_KNUCKLES":"ore","MAIN_GAUNTLET":"ore",
        "MAIN_SPIKEDGAUNTLET":"ore","2H_URSINEHANDSCLAW":"ore","MAIN_SPEAR":"ore","2H_PIKE":"ore",
        "2H_GLAIVE":"ore","2H_SPIRITHUNTER":"ore","2H_TRINITYSPEAR":"ore","2H_CROSSBOW":"ore",
        "2H_HEAVYCROSSBOW":"ore","MAIN_LIGHTCROSSBOW":"ore","2H_BOLTCASTERS":"ore","2H_SIEGEBOW":"ore",
        "OFFHAND_SHIELD":"ore","OFFHAND_TOWERSHIELD":"ore",
        "HEAD_PLATE_SET1":"ore","ARMOR_PLATE_SET1":"ore","SHOES_PLATE_SET1":"ore",
        "HEAD_PLATE_SET2":"ore","ARMOR_PLATE_SET2":"ore","SHOES_PLATE_SET2":"ore",
        "HEAD_PLATE_SET3":"ore","ARMOR_PLATE_SET3":"ore","SHOES_PLATE_SET3":"ore",
        "HEAD_PLATE_UNDEAD":"ore","ARMOR_PLATE_UNDEAD":"ore","SHOES_PLATE_UNDEAD":"ore",
        "HEAD_PLATE_HELL":"ore","ARMOR_PLATE_HELL":"ore","SHOES_PLATE_HELL":"ore",
        "HEAD_PLATE_MORGANA":"ore","ARMOR_PLATE_MORGANA":"ore","SHOES_PLATE_MORGANA":"ore",
        "2H_QUARTERSTAFF":"wood","2H_IRONCLADSTAFF":"wood","2H_SHARPSTAFF":"wood",
        "2H_DOUBLEBLADEDSTAFF":"wood","MAIN_NATURESTAFF":"wood","2H_NATURESTAFFGREAT":"wood",
        "2H_WILDSTAFF":"wood","MAIN_LUSHFOLIAGE":"wood","MAIN_TORCH":"wood",
        "MAIN_FIRESTAFF":"wood","2H_FIRESTAFF":"wood","2H_INFERNOSTAFF":"wood",
        "2H_BLAZINGSERPENT":"wood","MAIN_HOLYSTAFF":"wood","2H_HOLYSTAFF":"wood",
        "2H_DIVINESTAFF":"wood","MAIN_LIFETOUCH":"wood","MAIN_ARCANESTAFF":"wood",
        "2H_ARCANESTAFF":"wood","2H_ENIGMATICSTAFF":"wood","MAIN_OCCULTSTAFF":"wood",
        "MAIN_CURSEDSTAFF":"wood","2H_CURSEDSTAFF":"wood","2H_DEMONICSTAFF":"wood",
        "MAIN_SHADOWCALLER":"wood","2H_BOW":"wood","2H_WARBOW":"wood","2H_LONGBOW":"wood",
        "2H_MISTCALLER":"wood","2H_WEEPINGREPEAT":"wood",
        "OFFHAND_TORCH":"wood","OFFHAND_SOULSCYTHE":"wood",
        "HEAD_LEATHER_SET1":"hide","ARMOR_LEATHER_SET1":"hide","SHOES_LEATHER_SET1":"hide",
        "HEAD_LEATHER_SET2":"hide","ARMOR_LEATHER_SET2":"hide","SHOES_LEATHER_SET2":"hide",
        "HEAD_LEATHER_SET3":"hide","ARMOR_LEATHER_SET3":"hide","SHOES_LEATHER_SET3":"hide",
        "HEAD_LEATHER_UNDEAD":"hide","ARMOR_LEATHER_UNDEAD":"hide","SHOES_LEATHER_UNDEAD":"hide",
        "HEAD_LEATHER_HELL":"hide","ARMOR_LEATHER_HELL":"hide","SHOES_LEATHER_HELL":"hide",
        "HEAD_LEATHER_MORGANA":"hide","ARMOR_LEATHER_MORGANA":"hide","SHOES_LEATHER_MORGANA":"hide",
        "OFFHAND_MISTCOVERDAGGER":"hide","OFFHAND_LEATHERBOOK":"hide",
        "HEAD_CLOTH_SET1":"fiber","ARMOR_CLOTH_SET1":"fiber","SHOES_CLOTH_SET1":"fiber",
        "HEAD_CLOTH_SET2":"fiber","ARMOR_CLOTH_SET2":"fiber","SHOES_CLOTH_SET2":"fiber",
        "HEAD_CLOTH_SET3":"fiber","ARMOR_CLOTH_SET3":"fiber","SHOES_CLOTH_SET3":"fiber",
        "HEAD_CLOTH_UNDEAD":"fiber","ARMOR_CLOTH_UNDEAD":"fiber","SHOES_CLOTH_UNDEAD":"fiber",
        "HEAD_CLOTH_HELL":"fiber","ARMOR_CLOTH_HELL":"fiber","SHOES_CLOTH_HELL":"fiber",
        "OFFHAND_SKULLORB":"fiber","OFFHAND_CODEXA":"fiber","OFFHAND_LANTERN":"fiber",
        "OFFHAND_MUISNT":"fiber","BAG":"fiber","BAG_INSIGHT":"fiber",
    }
    def get_mat(template_str):
        for part, mat in MAT_BY_TYPE.items():
            if part in template_str: return mat
        return "ore"

    ALL_CALC_ITEMS = []
    for key, item_dict in BM_ITEMS.items():
        display, template = list(item_dict.items())[0]
        mat = get_mat(template)
        ALL_CALC_ITEMS.append((display, key, mat))


    mat_names_ru = {"ore": "Руда", "wood": "Дерево", "fiber": "Волокно", "hide": "Кожа"}
    mat_item_ids = {
        "ore": f"T{tier}_ORE", "wood": f"T{tier}_WOOD",
        "fiber": f"T{tier}_FIBER", "hide": f"T{tier}_HIDE",
    }
    base_count = {6: 8, 7: 10, 8: 12}[tier]
    ench_mults = [1, 2, 4, 8, 16]

    await interaction.followup.send(f"⏳ Загружаю цены T{tier} · {server_name}... (~30 сек)")

    # ── Загружаем цены материалов ─────────────────────────────
    mat_prices = {}
    async with aiohttp.ClientSession() as s:
        for mat, mat_id in mat_item_ids.items():
            url = f"{base_url}/stats/prices/{mat_id}?locations={locations}"
            try:
                async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                    if r.status != 200: continue
                    data = await r.json()
                best = 9_999_999_999; best_city = ""
                for p in data:
                    city = p.get("city", ""); sell = p.get("sell_price_min", 0) or 0
                    if sell > 0 and city not in ("Black Market", "Brecilien"):
                        if sell < best: best = sell; best_city = city
                mat_prices[mat] = {"price": best if best < 9_999_999_999 else 0, "city": best_city}
            except Exception:
                mat_prices[mat] = {"price": 0, "city": ""}

    # ── Загружаем цены всех предметов ─────────────────────────
    item_rows = []
    async with aiohttp.ClientSession() as s:
        for display, key, mat in ALL_CALC_ITEMS:
            bm_data = BM_ITEMS.get(key)
            if not bm_data: continue
            _, template = list(bm_data.items())[0]
            mat_price = mat_prices.get(mat, {}).get("price", 0)
            mat_city = CITY_NAMES_RU.get(mat_prices.get(mat, {}).get("city", ""), "—")

            for enchant in range(5):
                item_id = build_item_id(template, tier, enchant)
                mult = ench_mults[enchant]
                mats = base_count * mult
                cost = mats * mat_price

                url = f"{base_url}/stats/prices/{item_id}?locations={locations}"
                bm = market = brec = 0
                try:
                    async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status == 200:
                            prices = await r.json()
                            for p in prices:
                                city = p.get("city", ""); sell = p.get("sell_price_min", 0) or 0
                                if city == "Black Market" and sell > 0: bm = max(bm, sell)
                                elif city == "Brecilien" and sell > 0: brec = sell
                                elif sell > 0 and city not in ("Black Market", "Brecilien"):
                                    market = min(market, sell) if market > 0 else sell
                except Exception:
                    pass

                tax_val = tax_rate
                profit_bm   = round(bm   * (1-tax_val) - cost) if bm   > 0 and cost > 0 else ""
                profit_mkt  = round(market * (1-tax_val) - cost) if market > 0 and cost > 0 else ""
                profit_brec = round(brec * (1-tax_val) - cost) if brec  > 0 and cost > 0 else ""
                pct_bm   = round(profit_bm   / cost * 100, 1) if isinstance(profit_bm,   int) and cost > 0 else ""
                pct_mkt  = round(profit_mkt  / cost * 100, 1) if isinstance(profit_mkt,  int) and cost > 0 else ""
                pct_brec = round(profit_brec / cost * 100, 1) if isinstance(profit_brec, int) and cost > 0 else ""

                item_rows.append([
                    display,
                    f"T{tier}.{enchant}",
                    mat_names_ru.get(mat, mat),
                    mat_city,
                    mat_price or "",
                    mats,
                    cost or "",
                    bm or "",
                    profit_bm,
                    f"{pct_bm}%" if pct_bm != "" else "",
                    market or "",
                    profit_mkt,
                    f"{pct_mkt}%" if pct_mkt != "" else "",
                    brec or "",
                    profit_brec,
                    f"{pct_brec}%" if pct_brec != "" else "",
                ])

    # ── Google Sheets ─────────────────────────────────────────
    try:
        import gspread
        from google.oauth2.service_account import Credentials as GCreds
        import json as _json

        raw = GOOGLE_CREDS.strip()
        if raw.startswith("{"):
            creds_data = _json.loads(raw)
        else:
            import base64
            creds_data = _json.loads(base64.b64decode(raw + "==").decode("utf-8"))

        creds = GCreds.from_service_account_info(
            creds_data,
            scopes=["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
        )
        gc = gspread.authorize(creds)
        sh = gc.open_by_key(SHEET_ID)

        tab_name = f"Крафт T{tier} {server.upper()} налог{tax}%"
        try:
            ws = sh.worksheet(tab_name)
            ws.clear()
        except gspread.WorksheetNotFound:
            ws = sh.add_worksheet(title=tab_name, rows=300, cols=20)

        header = [
            "Предмет", "Тир.Зач", "Материал", "Лучший город мат.",
            "Цена мат. (1 шт)", "Кол-во мат.", "Себестоимость",
            "Цена ЧР", "Профит ЧР (−8% налог)", "% профит ЧР",
            "Цена рынок", "Профит рынок", "% профит рынок",
            "Цена Бреккилен", "Профит Брек.", "% профит Брек.",
        ]
        all_rows = [header] + item_rows
        ws.update("A1", all_rows)

        # Форматирование шапки
        ws.format("A1:P1", {
            "backgroundColor": {"red": 0.1, "green": 0.1, "blue": 0.2},
            "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
            "horizontalAlignment": "CENTER",
        })

        # Заморозить шапку
        ws.freeze(rows=1)

        # Условное форматирование — зелёный если профит > 0, красный если < 0
        # (через gspread это сложно, поэтому красим вручную топ профитные строки)
        profit_col_idx = 9  # колонка "% профит ЧР" (I)
        green_rows = []
        red_rows = []
        for i, row in enumerate(item_rows, start=2):
            pct = row[9]  # % профит ЧР
            if pct == "": continue
            try:
                val = float(str(pct).replace("%",""))
                if val > 0: green_rows.append(i)
                elif val < 0: red_rows.append(i)
            except Exception: pass

        # Красим зелёные строки (топ 20)
        for row_i in green_rows[:20]:
            ws.format(f"A{row_i}:P{row_i}", {
                "backgroundColor": {"red": 0.85, "green": 0.95, "blue": 0.85}
            })
        # Красим красные строки (топ 10 убыточных)
        for row_i in red_rows[:10]:
            ws.format(f"A{row_i}:P{row_i}", {
                "backgroundColor": {"red": 0.95, "green": 0.85, "blue": 0.85}
            })

        sheet_url = f"https://docs.google.com/spreadsheets/d/{SHEET_ID}"
        total = len(item_rows)
        profitable = len([r for r in item_rows if r[9] and str(r[9]) != "" and float(str(r[9]).replace("%","").replace("","0") or 0) > 0])

        e = discord.Embed(title=f"📊 Таблица крафта T{tier} · {server_name}", color=0x00E5FF)
        e.add_field(name="Таблица", value=f"[Открыть Google Sheets]({sheet_url})\nВкладка: **{tab_name}**", inline=False)
        e.add_field(name="Предметов", value=str(total), inline=True)
        e.add_field(name="Выгодных (ЧР)", value=str(profitable), inline=True)
        e.add_field(name="Зачаровки", value="T.0 → T.4 (все)", inline=True)
        mat_lines = "\n".join(
            f"**{mat_names_ru[m]}**: {mat_prices.get(m,{}).get('price',0):,} ({CITY_NAMES_RU.get(mat_prices.get(m,{}).get('city',''), '—')})"
            for m in ["ore", "wood", "fiber", "hide"]
        )
        e.add_field(name="📦 Цены материалов", value=mat_lines, inline=False)
        e.set_footer(text=f"Налог {tax}% учтён · albion-online-data.com · {datetime.datetime.utcnow().strftime('%d.%m.%Y %H:%M')} UTC")
        await interaction.channel.send(embed=e)

    except ImportError:
        await interaction.channel.send("❌ Установи: `pip install gspread google-auth`")
    except _json.JSONDecodeError as ex:
        await interaction.channel.send(f"❌ Ошибка JSON в GOOGLE_CREDENTIALS: `{ex}`")
    except Exception as ex:
        await interaction.channel.send(f"❌ Ошибка Google Sheets: `{ex}`")



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  💱 ALBION: CITY FLIPPER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="flipper", description="Торговый арбитраж между городами Albion [Pro]")
@app_commands.describe(
    category="weapon / offhand / armor_plate / armor_leather / armor_cloth / bag",
    tier="Тир: 6, 7 или 8",
    server="Сервер: eu / us / asia"
)
@cooldown(30)
async def flipper(interaction: discord.Interaction, category: str = "weapon", tier: int = 8, server: str = "eu"):
    if await get_tier(interaction.guild_id) < TIER_PRO:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)
    await interaction.response.defer()

    if tier not in (6, 7, 8):
        return await interaction.followup.send("❌ Тир: 6, 7 или 8")

    cat_lower = category.lower()
    keys = BM_GROUPS.get(cat_lower, [cat_lower] if cat_lower in BM_ITEMS else None)
    if not keys:
        return await interaction.followup.send("❌ Категория: weapon / offhand / armor_plate / armor_leather / armor_cloth / bag")

    await interaction.followup.send(f"⏳ Ищу арбитраж для **{category} T{tier}**...")

    base_url = ALBION_SERVERS.get(server, ALBION_SERVERS["eu"])
    locations = ",".join(CITY_LOCATIONS).replace(" ", "%20")
    flips = []

    async with aiohttp.ClientSession() as s:
        for key in keys[:15]:  # лимит чтобы не долго
            item_data = BM_ITEMS.get(key)
            if not item_data: continue
            display, template = list(item_data.items())[0]

            for enchant in range(0, 3):  # только 0-2 для скорости
                item_id = build_item_id(template, tier, enchant)
                url = f"{base_url}/stats/prices/{item_id}?locations={locations}"
                try:
                    async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                        if r.status != 200: continue
                        prices = await r.json()
                except Exception:
                    continue

                city_prices = {}
                for p in prices:
                    city = p.get("city", "")
                    sell = p.get("sell_price_min", 0) or 0
                    buy  = p.get("buy_price_max", 0) or 0
                    if city in CITY_LOCATIONS and sell > 0:
                        city_prices[city] = {"sell": sell, "buy": buy}

                if len(city_prices) < 2: continue

                # Найти максимальную разницу между городами
                cities = list(city_prices.keys())
                best_flip = None
                best_profit = 0
                for i in range(len(cities)):
                    for j in range(len(cities)):
                        if i == j: continue
                        buy_city = cities[i]
                        sell_city = cities[j]
                        buy_price  = city_prices[buy_city]["sell"]  # покупаем по рыночной цене
                        sell_price = city_prices[sell_city]["buy"]  # продаём по ордеру покупателя
                        if buy_price > 0 and sell_price > buy_price:
                            profit = sell_price - buy_price
                            pct = round(profit / buy_price * 100, 1)
                            if profit > best_profit:
                                best_profit = profit
                                best_flip = {
                                    "buy_city": buy_city, "buy_price": buy_price,
                                    "sell_city": sell_city, "sell_price": sell_price,
                                    "profit": profit, "pct": pct
                                }
                if best_flip and best_flip["pct"] > 5:
                    flips.append({
                        "name": f"{display} {tier}.{enchant}",
                        "item_id": item_id,
                        **best_flip
                    })

    if not flips:
        return await interaction.channel.send("😔 Нет выгодных флипов прямо сейчас. Рынок выровнен.")

    flips.sort(key=lambda x: x["pct"], reverse=True)
    e = discord.Embed(
        title=f"💱 Флиппер — {category.capitalize()} T{tier}",
        description=f"Купи в одном городе, продай в другом · {ALBION_SERVER_NAMES.get(server,'EU')}",
        color=0x00FF9D
    )
    for flip in flips[:8]:
        buy_ru  = CITY_NAMES_RU.get(flip["buy_city"],  flip["buy_city"])
        sell_ru = CITY_NAMES_RU.get(flip["sell_city"], flip["sell_city"])
        e.add_field(
            name=f"🗡️ {flip['name']} (+{flip['pct']}%)",
            value=(f"Купить в **{buy_ru}**: `{flip['buy_price']:,}`\n"
                   f"Продать в **{sell_ru}** (ордер): `{flip['sell_price']:,}`\n"
                   f"Профит: **{flip['profit']:,}** серебра"),
            inline=False
        )
    e.set_footer(text="Цены обновляются каждые ~15 мин · Учитывай налог 8%")
    await interaction.channel.send(embed=e)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🏰 ALBION: GUILD WAR STATS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="guildwar", description="Топ гильдий по активности ZvZ [Pro]")
@app_commands.describe(limit="Сколько гильдий показать (5-20)")
@cooldown(20)
async def guildwar(interaction: discord.Interaction, limit: int = 10):
    if await get_tier(interaction.guild_id) < TIER_PRO:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)
    await interaction.response.defer()
    limit = max(5, min(limit, 20))
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(f"{ALBION_BASE}/battles?sort=recent&limit=50",
                             timeout=aiohttp.ClientTimeout(total=15)) as r:
                battles = await r.json() if r.status == 200 else []

        guild_stats = {}
        for b in battles:
            fame  = b.get("TotalFame", 0)
            kills = b.get("TotalKills", 0)
            for gname in b.get("Guilds", {}).keys():
                if gname not in guild_stats:
                    guild_stats[gname] = {"battles": 0, "fame": 0, "kills": 0}
                guild_stats[gname]["battles"] += 1
                guild_stats[gname]["fame"]    += fame
                guild_stats[gname]["kills"]   += kills

        if not guild_stats:
            return await interaction.followup.send("❌ Нет данных о недавних битвах.")

        top = sorted(guild_stats.items(), key=lambda x: x[1]["fame"], reverse=True)[:limit]

        e = make_embed(
            title="Топ гильдий по ZvZ",
            description=f"По данным последних 50 битв · {datetime.datetime.utcnow().strftime('%d.%m.%Y')}",
            color=C.DANGER, footer="EU · Albion Online"
        )
        for i, (gname, stats) in enumerate(top):
            fame_bar = bar(stats["fame"], top[0][1]["fame"] if top else 1, 6)
            e.add_field(
                name=f"{i+1}. {gname}",
                value=(
                    f"Битв: **{stats['battles']}** · Убийств: **{stats['kills']}**\n"
                    f"Fame: **{stats['fame']:,}** `{fame_bar}`"
                ),
                inline=True
            )
        await interaction.followup.send(embed=e)
    except Exception as ex:
        await interaction.followup.send(f"❌ Ошибка: {ex}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  📡 PRICE WATCH — подписка на изменение цены
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="pricewatch", description="Слежка за ценой предмета [Pro]")
@app_commands.describe(
    action="add / remove / list",
    item_key="Ключ предмета (напр. sword, bow)",
    tier="Тир: 6-8",
    threshold="Порог изменения цены в % (по умолч. 5%)"
)
async def pricewatch(interaction: discord.Interaction, action: str = "list",
                     item_key: str = "", tier: int = 8, threshold: float = 5.0):
    if await get_tier(interaction.guild_id) < TIER_PRO:
        return await interaction.response.send_message(embed=upsell_embed("Pro"), ephemeral=True)

    gid = interaction.guild_id

    if action.lower() == "list":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT id, item_id, threshold_pct, last_price FROM price_watch WHERE guild_id=?", (gid,)
            ) as c:
                rows = await c.fetchall()
        e = discord.Embed(title="📡 Price Watch", color=0x00E5FF)
        if not rows:
            e.description = "Нет активных подписок. Добавь: `/pricewatch action:add item_key:sword`"
        for wid, iid, thr, last_p in rows:
            e.add_field(name=f"#{wid} · {iid}", value=f"Порог: {thr}% · Последняя цена: {last_p:,}", inline=False)
        return await interaction.response.send_message(embed=e)

    if action.lower() == "remove":
        try:
            watch_id = int(item_key)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("DELETE FROM price_watch WHERE id=? AND guild_id=?", (watch_id, gid))
                await db.commit()
            return await interaction.response.send_message(f"✅ Подписка `#{watch_id}` удалена.")
        except ValueError:
            return await interaction.response.send_message("❌ Укажи ID подписки: `/pricewatch action:remove item_key:1`", ephemeral=True)

    # add
    if not item_key:
        return await interaction.response.send_message("❌ Укажи `item_key`", ephemeral=True)
    item_data = BM_ITEMS.get(item_key.lower())
    if not item_data:
        return await interaction.response.send_message(f"❌ Предмет `{item_key}` не найден.", ephemeral=True)
    _, template = list(item_data.items())[0]
    item_id = build_item_id(template, tier, 0)

    async with aiosqlite.connect(DB_PATH) as db:
        # Проверяем лимит (макс 5 подписок на сервер)
        async with db.execute("SELECT COUNT(*) FROM price_watch WHERE guild_id=?", (gid,)) as c:
            count = (await c.fetchone())[0]
        if count >= 5:
            return await interaction.response.send_message("❌ Максимум 5 подписок на сервер. Удали старые через `/pricewatch action:remove`", ephemeral=True)
        await db.execute(
            "INSERT INTO price_watch (guild_id,channel_id,item_id,threshold_pct,last_price,created_at) VALUES (?,?,?,?,0,?)",
            (gid, interaction.channel_id, item_id, threshold, datetime.datetime.utcnow().isoformat())
        )
        await db.commit()

    await interaction.response.send_message(
        f"✅ Слежка добавлена: **{item_id}** · порог **{threshold}%** · уведомления в {interaction.channel.mention}"
    )

async def price_watch_loop():
    """Фоновая задача — проверяет цены каждые 15 минут."""
    await bot.wait_until_ready()
    while not bot.is_closed():
        await asyncio.sleep(900)  # 15 минут
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("SELECT id, guild_id, channel_id, item_id, threshold_pct, last_price FROM price_watch") as c:
                    watches = await c.fetchall()

            for wid, gid, ch_id, item_id, thr, last_price in watches:
                guild = bot.get_guild(gid)
                ch    = guild.get_channel(ch_id) if guild else None
                if not ch: continue

                url = f"{ALBION_DATA}/stats/prices/{item_id}?locations=Black%20Market,Caerleon"
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                            prices = await r.json() if r.status == 200 else []
                except Exception:
                    continue

                current_price = 0
                for p in prices:
                    if p.get("city") == "Black Market":
                        current_price = p.get("sell_price_min", 0) or 0
                        break
                if not current_price: continue

                if last_price > 0:
                    change_pct = abs(current_price - last_price) / last_price * 100
                    if change_pct >= thr:
                        direction = "📈 вырос" if current_price > last_price else "📉 упал"
                        e = discord.Embed(title=f"📡 Price Alert: {item_id}", color=0xFFD700)
                        e.add_field(name="Цена на ЧР", value=f"**{direction}** на {change_pct:.1f}%", inline=False)
                        e.add_field(name="Было", value=f"{last_price:,}", inline=True)
                        e.add_field(name="Стало", value=f"{current_price:,}", inline=True)
                        try:
                            await ch.send(embed=e)
                        except Exception:
                            pass

                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("UPDATE price_watch SET last_price=? WHERE id=?", (current_price, wid))
                    await db.commit()
        except Exception as ex:
            print(f"[PriceWatch] Error: {ex}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  🤖 AI: ASK ALBION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="askalbion", description="AI ответит на вопрос об Albion Online [Premium]")
@app_commands.describe(question="Вопрос об игре (билды, механики, советы)")
@cooldown(10)
async def askalbion(interaction: discord.Interaction, question: str):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    await interaction.response.defer()
    try:
        answer = await ask_ai(
            question,
            system=(
                "You are an expert Albion Online player and guide. "
                "Answer questions about builds, mechanics, economy, PvP, PvE, guilds, and all game systems. "
                "Be specific and practical. Use silver values when relevant. "
                "If asked in Russian, reply in Russian. Keep answers concise but complete."
            )
        )
        e = discord.Embed(title="⚔️ Albion Expert", description=answer[:4000], color=0xC8A951)
        e.set_footer(text=f"Вопрос: {question[:80]} · Witness AI")
        await interaction.followup.send(embed=e)
    except Exception as ex:
        await interaction.followup.send(f"❌ Ошибка: {ex}")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  START BACKGROUND TASKS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━





@bot.tree.command(name="bmtest", description="[DEBUG] Тест API Albion Data Project")
@app_commands.describe(item_id="ID предмета (напр. T8_MAIN_SWORD)")
async def bmtest(interaction: discord.Interaction, item_id: str = "T8_MAIN_SWORD"):
    """Быстрый тест — проверяет что API отвечает и возвращает цены."""
    if not interaction.user.guild_permissions.administrator:
        return await interaction.response.send_message(
            "❌ Отладочная команда, доступна только администраторам.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    url = f"https://west.albion-online-data.com/api/v2/stats/prices/{item_id}?locations=Black%20Market,Caerleon,Martlock,Brecilien"
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(url, timeout=aiohttp.ClientTimeout(total=15)) as r:
                status = r.status
                data = await r.json()

        e = discord.Embed(title=f"🔧 BM Test — {item_id}", color=0x00E5FF)
        e.add_field(name="HTTP Status", value=str(status), inline=True)
        e.add_field(name="Записей в ответе", value=str(len(data)), inline=True)
        e.add_field(name="URL", value=f"`{url}`", inline=False)

        lines = []
        for p in data:
            city  = p.get("city", "?")
            sell  = p.get("sell_price_min", 0) or 0
            buy   = p.get("buy_price_max", 0) or 0
            upd   = p.get("sell_price_min_date", "")[:10]
            lines.append(f"**{city}**: sell=`{sell:,}` buy=`{buy:,}` upd=`{upd}`")

        if lines:
            e.add_field(name="Цены", value="\n".join(lines), inline=False)
        else:
            e.add_field(name="Цены", value="❌ Пустой ответ", inline=False)

        await interaction.followup.send(embed=e)
    except Exception as ex:
        await interaction.followup.send(f"❌ Ошибка запроса: `{ex}`")


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ALBION REGISTRATION
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="register", description="Привязать Albion Online ник к Discord аккаунту")
@app_commands.describe(player="Ник в Albion Online")
async def register_albion(interaction: discord.Interaction, player: str):
    await interaction.response.defer()
    try:
        async with aiohttp.ClientSession() as s:
            pid, pname = await albion_find_player(s, player)
        if not pid:
            return await interaction.followup.send(f"❌ Игрок **{player}** не найден.", ephemeral=True)
        now = datetime.datetime.utcnow().isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO albion_registration (guild_id,user_id,player_name,player_id,registered_at)
                VALUES (?,?,?,?,?)
                ON CONFLICT(guild_id,user_id) DO UPDATE SET
                    player_name=excluded.player_name, player_id=excluded.player_id,
                    registered_at=excluded.registered_at
            """, (interaction.guild_id, interaction.user.id, pname, pid, now))
            await db.commit()
        e = discord.Embed(color=0x00E5FF, timestamp=datetime.datetime.utcnow())
        e.set_author(name=f"Registered — {pname}", icon_url=interaction.user.display_avatar.url)
        e.add_field(name="Discord", value=interaction.user.mention, inline=True)
        e.add_field(name="Albion",  value=f"**{pname}**",           inline=True)
        e.description = "Теперь `/stats` без аргументов покажет твою статистику."
        await interaction.followup.send(embed=e)
    except Exception as ex:
        await interaction.followup.send(f"❌ {ex}", ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ALBION WATCH
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

@bot.tree.command(name="watch", description="Отслеживать активность Albion игрока [Premium]")
@app_commands.describe(
    action="add / remove / list",
    player="Ник игрока",
    channel="Канал для алертов"
)
async def albion_watch_cmd(interaction: discord.Interaction,
                            action: str,
                            player: str = "",
                            channel: discord.TextChannel = None):
    if await get_tier(interaction.guild_id) < TIER_PREMIUM:
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)

    gid = interaction.guild_id

    if action == "add":
        if not player:
            return await interaction.response.send_message("❌ Укажи игрока.", ephemeral=True)
        ch = channel or interaction.channel
        await interaction.response.defer()
        async with aiohttp.ClientSession() as s:
            pid, pname = await albion_find_player(s, player)
        if not pid:
            return await interaction.followup.send(f"❌ Игрок **{player}** не найден.", ephemeral=True)
        now = datetime.datetime.utcnow().isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO albion_watch (guild_id,user_id,channel_id,player_name,player_id,created_at) VALUES (?,?,?,?,?,?)",
                (gid, interaction.user.id, ch.id, pname, pid, now)
            )
            await db.commit()
        e = discord.Embed(color=0x00E5FF, timestamp=datetime.datetime.utcnow())
        e.set_author(name=f"Watching — {pname}")
        e.add_field(name="Player",  value=f"**{pname}**",  inline=True)
        e.add_field(name="Channel", value=ch.mention,      inline=True)
        e.description = "Бот будет оповещать о новых убийствах и смертях каждый час."
        await interaction.followup.send(embed=e)

    elif action == "remove":
        if not player:
            return await interaction.response.send_message("❌ Укажи игрока.", ephemeral=True)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "DELETE FROM albion_watch WHERE guild_id=? AND player_name=?",
                (gid, player)
            )
            await db.commit()
        await interaction.response.send_message(f"✅ Слежка за **{player}** удалена.", ephemeral=True)

    elif action == "list":
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT player_name,channel_id FROM albion_watch WHERE guild_id=?", (gid,)
            ) as c:
                rows = await c.fetchall()
        e = discord.Embed(color=0x00E5FF, timestamp=datetime.datetime.utcnow())
        e.set_author(name="Albion Watch List")
        if not rows:
            e.description = "Нет отслеживаемых игроков."
        for pname, cid in rows:
            ch_obj = interaction.guild.get_channel(cid)
            e.add_field(name=pname, value=ch_obj.mention if ch_obj else str(cid), inline=True)
        await interaction.response.send_message(embed=e, ephemeral=True)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ALBION WATCH BACKGROUND LOOP
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def albion_watch_loop(bot_instance):
    """Проверяет новые убийства/смерти для отслеживаемых игроков — раз в час"""
    await bot_instance.wait_until_ready()
    while not bot_instance.is_closed():
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT id,guild_id,channel_id,player_name,player_id,last_check FROM albion_watch"
                ) as c:
                    watches = await c.fetchall()

            for wid, guild_id, channel_id, pname, pid, last_check in watches:
                try:
                    async with aiohttp.ClientSession() as s:
                        async with s.get(f"{ALBION_BASE}/players/{pid}/kills?limit=5") as r:
                            kills = await r.json() if r.status == 200 else []
                        async with s.get(f"{ALBION_BASE}/players/{pid}/deaths?limit=5") as r:
                            deaths = await r.json() if r.status == 200 else []

                    now_iso = datetime.datetime.utcnow().isoformat()
                    new_kills = [k for k in kills
                                 if k.get("TimeStamp","") > (last_check or "")]
                    new_deaths = [d for d in deaths
                                  if d.get("TimeStamp","") > (last_check or "")]

                    if new_kills or new_deaths:
                        ch = bot_instance.get_channel(channel_id)
                        if ch:
                            e = discord.Embed(color=0x00E5FF, timestamp=datetime.datetime.utcnow())
                            e.set_author(name=f"Albion Watch — {pname}")
                            if new_kills:
                                lines = [
                                    f"⚔️ Killed **{k.get('Victim',{}).get('Name','?')}** · {k.get('TotalVictimKillFame',0):,} fame"
                                    for k in new_kills[:3]
                                ]
                                e.add_field(name=f"Kills ({len(new_kills)})", value="\n".join(lines), inline=False)
                            if new_deaths:
                                lines = [
                                    f"💀 Died to **{d.get('Killer',{}).get('Name','?')}** · {d.get('TotalVictimKillFame',0):,} fame"
                                    for d in new_deaths[:3]
                                ]
                                e.add_field(name=f"Deaths ({len(new_deaths)})", value="\n".join(lines), inline=False)
                            await ch.send(embed=e)

                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute(
                            "UPDATE albion_watch SET last_check=? WHERE id=?", (now_iso, wid)
                        )
                        await db.commit()
                except Exception as ex:
                    print(f"[ALBION] Watch save error: {ex}")

        except Exception as ex:
            print(f"[ALBION WATCH] Error: {ex}")

        await asyncio.sleep(3600)  # раз в час
