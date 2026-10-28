"""
Самовыдача ролей (self-roles) через кнопки и выпадающие меню — без реакций.

  • /roles panel create|add|remove|edit|delete|list — управление панелями;
  • кнопка — переключатель роли, меню — набор ролей из панели;
  • режим «только одна роль»: новая роль из панели снимает остальные;
  • компоненты — DynamicItem, переживают рестарт (custom_id: srole:...).

Функция выключена по умолчанию: включается только тумблером в дашборде
(страница «Роли», guild_settings.selfroles_enabled). Управлять панелями могут
администраторы (Manage Server) и роль из guild_settings.selfroles_manager_role.
"""

import datetime
import re
import time

import aiosqlite
import discord
from discord import app_commands

from .config import DB_PATH
from .core import bot, report_error
from .database import get_guild_settings_cached

MAX_ROLES = 25                    # предел кнопок в сообщении и опций в меню
CLICK_COOLDOWN = 3                # секунд между нажатиями одного участника
DEFAULT_COLOR = 0x5865F2

# Права, с которыми роль нельзя раздавать всем желающим
UNSAFE_PERMS = ("administrator", "manage_guild", "manage_roles", "manage_channels",
                "manage_webhooks", "manage_messages", "manage_nicknames", "manage_threads",
                "manage_expressions", "manage_events", "ban_members", "kick_members",
                "moderate_members", "mention_everyone", "view_audit_log")
PERM_NAMES = {"administrator": "Администратор", "manage_guild": "Управление сервером",
              "manage_roles": "Управление ролями", "manage_channels": "Управление каналами",
              "manage_webhooks": "Управление вебхуками", "manage_messages": "Управление сообщениями",
              "manage_nicknames": "Управление никнеймами", "manage_threads": "Управление ветками",
              "manage_expressions": "Управление эмодзи", "manage_events": "Управление событиями",
              "ban_members": "Бан", "kick_members": "Кик", "moderate_members": "Таймаут",
              "mention_everyone": "Упоминание @everyone", "view_audit_log": "Журнал аудита"}

_click_cd: dict = {}              # user_id → время последнего нажатия
_panel_msgs: set | None = None    # message_id всех панелей — чтобы не ходить в БД на каждое удаление


def _utcnow():
    return datetime.datetime.utcnow()


# ── База ──────────────────────────────────────────────────────

async def get_panel(gid, pid) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM role_panels WHERE guild_id=? AND id=?", (gid, pid)) as c:
            row = await c.fetchone()
    return dict(row) if row else None


async def get_panels(gid) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM role_panels WHERE guild_id=? ORDER BY id", (gid,)) as c:
            return [dict(r) for r in await c.fetchall()]


async def get_panel_roles(pid) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM role_panel_roles WHERE panel_id=? ORDER BY position, rowid",
                              (pid,)) as c:
            return [dict(r) for r in await c.fetchall()]


async def _delete_panel_rows(pids):
    pids = [int(p) for p in pids]
    if not pids:
        return
    marks = ",".join("?" * len(pids))
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(f"SELECT message_id FROM role_panels WHERE id IN ({marks})", pids) as c:
            mids = [r[0] for r in await c.fetchall()]
        await db.execute(f"DELETE FROM role_panel_roles WHERE panel_id IN ({marks})", pids)
        await db.execute(f"DELETE FROM role_panels WHERE id IN ({marks})", pids)
        await db.commit()
    if _panel_msgs is not None:
        _panel_msgs.difference_update(mids)


async def _known_panel_msgs() -> set:
    global _panel_msgs
    if _panel_msgs is None:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT message_id FROM role_panels WHERE message_id != 0") as c:
                _panel_msgs = {r[0] for r in await c.fetchall()}
    return _panel_msgs


async def is_enabled(gid) -> bool:
    return bool((await get_guild_settings_cached(gid)).get("selfroles_enabled"))


# ── Проверки ──────────────────────────────────────────────────

def role_problem(guild: discord.Guild, role: discord.Role, actor: discord.Member = None) -> str | None:
    """Почему эту роль нельзя раздавать через панель. None — можно."""
    if role.is_default():
        return "@everyone выдать нельзя."
    if role.managed:
        return f"{role.mention} управляется интеграцией (буст, бот, подписка) — её нельзя выдавать вручную."
    me = guild.me
    if not me.guild_permissions.manage_roles:
        return "У бота нет права «Управлять ролями»."
    if role >= me.top_role:
        return (f"{role.mention} выше роли бота или равна ей. "
                f"Перетащи роль бота выше в «Настройки сервера → Роли».")
    bad = [PERM_NAMES[p] for p in UNSAFE_PERMS if getattr(role.permissions, p, False)]
    if bad:
        return f"У {role.mention} опасные права: {', '.join(bad)}. Такие роли через панель не выдаются."
    if actor is not None and actor.id != guild.owner_id and role >= actor.top_role:
        return f"{role.mention} не ниже твоей высшей роли — добавить её в панель может только тот, кто выше."
    return None


async def can_manage(member: discord.Member) -> bool:
    p = member.guild_permissions
    if p.administrator or p.manage_guild:
        return True
    rid = (await get_guild_settings_cached(member.guild.id)).get("selfroles_manager_role") or 0
    return bool(rid) and any(r.id == int(rid) for r in member.roles)


def _parse_color(value: str | None) -> int | None:
    if not value:
        return None
    m = re.fullmatch(r"#?([0-9a-fA-F]{6})", value.strip())
    return int(m.group(1), 16) if m else None


_CUSTOM_EMOJI = re.compile(r"<a?:[A-Za-z0-9_~]{2,32}:(\d{15,21})>")
_EMOJI_JOINERS = {0x200D, 0xFE0E, 0xFE0F, 0x20E3}       # ZWJ, вариации, рамка цифры 1️⃣
# Блоки Unicode, где лежат эмодзи (обычные стрелки и значки вроде → туда не входят)
_EMOJI_RANGES = ((0xA9, 0xA9), (0xAE, 0xAE), (0x203C, 0x203C), (0x2049, 0x2049), (0x2122, 0x2122),
                 (0x2139, 0x2139), (0x2194, 0x2199), (0x21A9, 0x21AA), (0x231A, 0x23FF), (0x24C2, 0x24C2),
                 (0x25AA, 0x25FE), (0x2600, 0x27BF), (0x2934, 0x2935), (0x2B05, 0x2B55), (0x3030, 0x3030),
                 (0x303D, 0x303D), (0x3297, 0x3299), (0x1F000, 0x1FAFF), (0xE0020, 0xE007F))


def _is_unicode_emoji(s: str) -> bool:
    """Грубая проверка, что строка — эмодзи, а не текст. Discord на текст
    в поле emoji отвечает «Invalid emoji» и отклоняет всё сообщение панели."""
    if not s or len(s) > 16 or any(ch.isspace() for ch in s):
        return False
    keycap = "⃣" in s
    seen = False
    for ch in s:
        o = ord(ch)
        if o in _EMOJI_JOINERS or 0x1F3FB <= o <= 0x1F3FF:  # тон кожи
            continue
        if any(lo <= o <= hi for lo, hi in _EMOJI_RANGES):
            seen = True
        elif keycap and ch in "0123456789#*":
            seen = True
        else:
            return False                # буквы, цифры, :shortcode: и прочий текст
    return seen


def normalize_emoji(guild: discord.Guild, value: str | None) -> tuple[str, str | None]:
    """(эмодзи для сохранения, ошибка). Понимает обычный эмодзи, <:name:id>
    и имя серверного эмодзи (name или :name:)."""
    value = (value or "").strip()
    if not value:
        return "", None
    m = _CUSTOM_EMOJI.fullmatch(value)
    if m:
        if bot.get_emoji(int(m.group(1))) is None:
            return "", "Бот не видит этот эмодзи: он должен быть с сервера, где есть Witness."
        return value, None
    name = value.strip(":")
    if re.fullmatch(r"[A-Za-z0-9_~]{2,32}", name):
        e = discord.utils.get(guild.emojis, name=name) or \
            next((x for x in guild.emojis if x.name.lower() == name.lower()), None)
        if e is None:
            return "", (f"Эмодзи «{value}» не нашёл на сервере. Вставь сам эмодзи "
                        f"(на Windows — Win + .) или имя серверного эмодзи.")
        if not e.is_usable():
            return "", f"Эмодзи «{value}» сейчас недоступен боту (например, выключен из-за бустов)."
        return str(e), None
    if _is_unicode_emoji(value):
        return value, None
    return "", "Это не похоже на эмодзи. Вставь один эмодзи, например ⚔️, или серверный в виде <:name:id>."


def _parse_emoji(value: str | None):
    """Эмодзи для кнопки или опции меню. Недоступный или кривой — None,
    чтобы одна роль не ломала отправку всей панели."""
    value = (value or "").strip()
    if not value:
        return None
    m = _CUSTOM_EMOJI.fullmatch(value)
    if m:
        return discord.PartialEmoji.from_str(value) if bot.get_emoji(int(m.group(1))) else None
    return discord.PartialEmoji(name=value) if _is_unicode_emoji(value) else None


def _text(value: str | None) -> str:
    # Слэш-команда не даёт ввести перенос строки — разрешаем писать \n
    return (value or "").replace("\\n", "\n").strip()


# ── Сообщение панели ──────────────────────────────────────────

def build_panel(panel: dict, roles: list[dict]) -> tuple[discord.Embed, discord.ui.View]:
    e = discord.Embed(title=panel["title"][:256], description=(panel["description"] or None),
                      color=panel["color"] or DEFAULT_COLOR)
    if not roles:
        e.add_field(name="​", value="*Роли ещё не добавлены.*", inline=False)
    hint = "Можно выбрать только одну роль" if panel["single"] else "Нажми ещё раз, чтобы снять роль"
    if panel["mode"] == "select":
        hint = "Можно выбрать только одну роль" if panel["single"] else \
               "Выбранное заменяет твои роли из этого списка"
    enabled = panel.get("enabled", 1)
    e.set_footer(text=hint if enabled else "Панель временно выключена")
    view = discord.ui.View(timeout=None)
    if not roles or not enabled:
        return e, view
    if panel["mode"] == "select":
        view.add_item(RoleSelect(panel["id"], panel["single"], roles))
        view.add_item(RoleClearButton(panel["id"]))
    else:
        for r in roles:
            view.add_item(RoleButton(panel["id"], r["role_id"], r["label"], r["emoji"]))
    return e, view


async def _panel_channel(guild, channel_id):
    ch = guild.get_channel(channel_id)
    if ch is None:
        try:
            ch = await guild.fetch_channel(channel_id)
        except (discord.NotFound, discord.Forbidden):
            return None
    return ch


async def refresh_panel(guild: discord.Guild, pid: int) -> str | None:
    """Перерисовывает сообщение панели. Возвращает текст ошибки или None.
    Сообщение или канал удалены — запись панели удаляется."""
    panel = await get_panel(guild.id, pid)
    if not panel:
        return "Панель не найдена."
    roles = await get_panel_roles(pid)
    # Роли, удалённые с сервера, из панели убираем
    gone = [r["role_id"] for r in roles if guild.get_role(r["role_id"]) is None]
    if gone:
        await _drop_roles(pid, gone)
        roles = [r for r in roles if r["role_id"] not in gone]
    ch = await _panel_channel(guild, panel["channel_id"])
    if ch is None:
        await _delete_panel_rows([pid])
        return "Канал панели удалён — панель убрана."
    embed, view = build_panel(panel, roles)
    try:
        msg = ch.get_partial_message(panel["message_id"])
        await msg.edit(embed=embed, view=view)
    except discord.NotFound:
        await _delete_panel_rows([pid])
        return "Сообщение панели удалено вручную — панель убрана."
    except discord.Forbidden as ex:
        await report_error("SELFROLES", ex, f"Обновление панели #{pid} на сервере {guild.id}")
        return f"Нет прав редактировать сообщение в {ch.mention}."
    except discord.HTTPException as ex:
        return f"Discord отклонил обновление панели: {ex.text or ex}"
    return None


async def _drop_roles(pid, role_ids):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executemany("DELETE FROM role_panel_roles WHERE panel_id=? AND role_id=?",
                             [(pid, rid) for rid in role_ids])
        await db.commit()


# ── Операции (общие для команд и дашборда) ────────────────────

async def create_panel(guild, channel, actor_id, title, description, mode, single, color) -> tuple[int | None, str | None]:
    panel = {"id": 0, "title": title, "description": description, "mode": mode,
             "single": int(single), "color": color or DEFAULT_COLOR}
    embed, view = build_panel(panel, [])
    try:
        msg = await channel.send(embed=embed, view=view)
    except discord.Forbidden:
        return None, f"У бота нет прав писать в {channel.mention} (нужны «Отправлять сообщения» и «Встраивать ссылки»)."
    except discord.HTTPException as ex:
        return None, f"Не удалось отправить панель: {ex.text or ex}"
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            "INSERT INTO role_panels (guild_id, channel_id, message_id, mode, title, description, "
            "color, single, created_by, created_at) VALUES (?,?,?,?,?,?,?,?,?,?)",
            (guild.id, channel.id, msg.id, mode, title, description, panel["color"], int(single),
             actor_id, _utcnow().isoformat()))
        await db.commit()
        pid = cur.lastrowid
    (await _known_panel_msgs()).add(msg.id)
    return pid, None


async def add_role(guild, actor, pid, role, label=None, emoji=None, description=None) -> str | None:
    panel = await get_panel(guild.id, pid)
    if not panel:
        return "Панель не найдена."
    problem = role_problem(guild, role, actor)
    if problem:
        return problem
    roles = await get_panel_roles(pid)
    if any(r["role_id"] == role.id for r in roles):
        return f"{role.mention} уже есть в этой панели."
    if len(roles) >= MAX_ROLES:
        return f"В панели уже {MAX_ROLES} ролей — это предел Discord."
    emoji, bad = normalize_emoji(guild, emoji)
    if bad:
        return bad
    label = (label or role.name).strip()[:80]
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO role_panel_roles (panel_id, guild_id, role_id, label, emoji, description, position) "
            "VALUES (?,?,?,?,?,?,?)",
            (pid, guild.id, role.id, label, emoji, (description or "").strip()[:100], len(roles)))
        await db.commit()
    err = await refresh_panel(guild, pid)
    if err and await get_panel(guild.id, pid):
        # Обычно это эмодзи, недоступный боту: откатываем, чтобы панель не сломалась
        await _drop_roles(pid, [role.id])
        await refresh_panel(guild, pid)
        return err
    return err


async def remove_role(guild, pid, role_id) -> str | None:
    if not await get_panel(guild.id, pid):
        return "Панель не найдена."
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("DELETE FROM role_panel_roles WHERE panel_id=? AND role_id=?", (pid, role_id))
        await db.commit()
    if not cur.rowcount:
        return "Этой роли нет в панели."
    return await refresh_panel(guild, pid)


async def edit_panel(guild, pid, **fields) -> str | None:
    fields = {k: v for k, v in fields.items() if v is not None}
    if not await get_panel(guild.id, pid):
        return "Панель не найдена."
    if fields:
        sets = ", ".join(f"{k}=?" for k in fields)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(f"UPDATE role_panels SET {sets} WHERE guild_id=? AND id=?",
                             (*fields.values(), guild.id, pid))
            await db.commit()
    return await refresh_panel(guild, pid)


async def delete_panel(guild, pid) -> str | None:
    panel = await get_panel(guild.id, pid)
    if not panel:
        return "Панель не найдена."
    await _delete_panel_rows([pid])
    ch = await _panel_channel(guild, panel["channel_id"])
    if ch is not None:
        try:
            await ch.get_partial_message(panel["message_id"]).delete()
        except discord.NotFound:
            pass
        except discord.HTTPException as ex:
            return f"Панель удалена, но сообщение убрать не удалось: {ex.text or ex}"
    return None


# ── Нажатия ───────────────────────────────────────────────────

async def _apply(interaction: discord.Interaction, pid: int, wanted: set[int], scope: set[int] | None):
    """Выдаёт роли wanted и снимает остальные роли панели из scope.
    scope=None — кнопка: wanted состоит из одной роли, которая переключается."""
    guild, member = interaction.guild, interaction.user
    now = time.time()
    if now - _click_cd.get(member.id, 0) < CLICK_COOLDOWN:
        return await interaction.response.send_message("⏳ Не так быстро — подожди пару секунд.", ephemeral=True)
    _click_cd[member.id] = now
    if len(_click_cd) > 5000:
        _click_cd.clear()
    if not await is_enabled(guild.id):
        return await interaction.response.send_message("ℹ️ Выдача ролей на этом сервере выключена.", ephemeral=True)
    panel = await get_panel(guild.id, pid)
    if not panel:
        return await interaction.response.send_message("❌ Эта панель больше не работает.", ephemeral=True)
    if not panel["enabled"]:
        return await interaction.response.send_message("ℹ️ Эта панель сейчас выключена.", ephemeral=True)
    await interaction.response.defer(ephemeral=True, thinking=True)

    rows = await get_panel_roles(pid)
    panel_ids = {r["role_id"] for r in rows}
    gone = [rid for rid in panel_ids if guild.get_role(rid) is None]
    if gone:
        await _drop_roles(pid, gone)
        await refresh_panel(guild, pid)
        panel_ids -= set(gone)
    wanted &= panel_ids
    have = {r.id for r in member.roles} & panel_ids

    if scope is None:                         # кнопка-переключатель
        rid = next(iter(wanted), None)
        if rid is None:
            return await interaction.followup.send("❌ Этой роли больше нет.", ephemeral=True)
        if rid in have:
            add, remove = set(), {rid}
        else:
            add = {rid}
            remove = (have - {rid}) if panel["single"] else set()
    else:                                     # меню: выбранное заменяет роли панели
        add, remove = wanted - have, (have & scope) - wanted

    add_roles = [guild.get_role(r) for r in add]
    blocked = [r for r in add_roles if role_problem(guild, r)]
    add_roles = [r for r in add_roles if r not in blocked]
    remove_roles = [guild.get_role(r) for r in remove]
    reason = f"Witness: панель ролей #{pid}"
    lines = []
    try:
        if remove_roles:
            await member.remove_roles(*remove_roles, reason=reason)
            lines.append("➖ Сняты: " + ", ".join(r.mention for r in remove_roles))
        if add_roles:
            await member.add_roles(*add_roles, reason=reason)
            lines.append("➕ Выданы: " + ", ".join(r.mention for r in add_roles))
    except discord.Forbidden as ex:
        await report_error("SELFROLES", ex, f"Сервер {guild.id} ({guild.name}), панель #{pid}, "
                                            f"участник {member.id}: нет прав выдать/снять роли")
        lines.append("❌ У бота не хватает прав изменить роли. Администрация уведомлена.")
    except discord.HTTPException as ex:
        await report_error("SELFROLES", ex, f"Сервер {guild.id}, панель #{pid}, участник {member.id}")
        lines.append("❌ Discord не дал изменить роли, попробуй позже.")
    if blocked:
        await report_error("SELFROLES", RuntimeError("роль стала небезопасной или недоступной"),
                           f"Сервер {guild.id}, панель #{pid}: " + ", ".join(f"{r.name} ({r.id})" for r in blocked))
        lines.append("⚠️ Не выданы (роль изменилась и стала недоступна): " +
                     ", ".join(r.mention for r in blocked))
    await interaction.followup.send("\n".join(lines) or "Ничего не изменилось.", ephemeral=True)


class RoleButton(discord.ui.DynamicItem[discord.ui.Button], template=r"srole:b:(?P<pid>\d+):(?P<rid>\d+)"):
    """Кнопка-переключатель роли — переживает рестарт бота"""
    def __init__(self, panel_id: int, role_id: int, label: str = None, emoji: str = None):
        self.panel_id = panel_id
        self.role_id = role_id
        super().__init__(discord.ui.Button(
            label=(label or None), emoji=_parse_emoji(emoji), style=discord.ButtonStyle.secondary,
            custom_id=f"srole:b:{panel_id}:{role_id}"))

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(int(match["pid"]), int(match["rid"]))

    async def callback(self, interaction: discord.Interaction):
        await _apply(interaction, self.panel_id, {self.role_id}, None)


class RoleSelect(discord.ui.DynamicItem[discord.ui.Select], template=r"srole:s:(?P<pid>\d+)"):
    """Меню выбора ролей — переживает рестарт бота"""
    def __init__(self, panel_id: int, single: int = 0, roles: list[dict] = (), select=None):
        self.panel_id = panel_id
        if select is None:
            options = [discord.SelectOption(label=r["label"][:100], value=str(r["role_id"]),
                                            emoji=_parse_emoji(r["emoji"]),
                                            description=(r["description"] or None))
                       for r in roles]
            select = discord.ui.Select(
                custom_id=f"srole:s:{panel_id}", options=options, min_values=0,
                max_values=1 if single else len(options),
                placeholder="Выбери роль" if single else "Выбери роли")
        super().__init__(select)

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(int(match["pid"]), select=item)

    async def callback(self, interaction: discord.Interaction):
        scope = {int(o.value) for o in self.item.options}
        await _apply(interaction, self.panel_id, {int(v) for v in self.item.values}, scope)


class RoleClearButton(discord.ui.DynamicItem[discord.ui.Button], template=r"srole:c:(?P<pid>\d+)"):
    """Снять все роли этой панели — в меню пустой выбор отправить нельзя"""
    def __init__(self, panel_id: int):
        self.panel_id = panel_id
        super().__init__(discord.ui.Button(label="Снять роли из списка", style=discord.ButtonStyle.secondary,
                                           custom_id=f"srole:c:{panel_id}"))

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(int(match["pid"]))

    async def callback(self, interaction: discord.Interaction):
        scope = {r["role_id"] for r in await get_panel_roles(self.panel_id)}
        await _apply(interaction, self.panel_id, set(), scope)


# ── Чистка при ручном удалении ────────────────────────────────

async def _drop_panels_by_messages(message_ids):
    mids = [m for m in message_ids if m in await _known_panel_msgs()]
    if not mids:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(f"SELECT id FROM role_panels WHERE message_id IN ({','.join('?' * len(mids))})",
                              mids) as c:
            pids = [r[0] for r in await c.fetchall()]
    await _delete_panel_rows(pids)


@bot.listen("on_raw_message_delete")
async def _sr_message_delete(payload):
    await _drop_panels_by_messages([payload.message_id])


@bot.listen("on_raw_bulk_message_delete")
async def _sr_bulk_delete(payload):
    await _drop_panels_by_messages(payload.message_ids)


@bot.listen("on_guild_channel_delete")
async def _sr_channel_delete(channel):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT id FROM role_panels WHERE guild_id=? AND channel_id=?",
                              (channel.guild.id, channel.id)) as c:
            pids = [r[0] for r in await c.fetchall()]
    await _delete_panel_rows(pids)


@bot.listen("on_guild_role_delete")
async def _sr_role_delete(role):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT DISTINCT panel_id FROM role_panel_roles WHERE guild_id=? AND role_id=?",
                              (role.guild.id, role.id)) as c:
            pids = [r[0] for r in await c.fetchall()]
        await db.execute("DELETE FROM role_panel_roles WHERE guild_id=? AND role_id=?", (role.guild.id, role.id))
        await db.commit()
    for pid in pids:
        err = await refresh_panel(role.guild, pid)
        if err:
            print(f"[SELFROLES] панель #{pid} после удаления роли: {err}")


# ── Команды ───────────────────────────────────────────────────

roles_grp = app_commands.Group(name="roles", description="Выдача ролей кнопками и меню", guild_only=True)
panel_grp = app_commands.Group(name="panel", description="Панели ролей", parent=roles_grp)


async def _guard(interaction: discord.Interaction) -> bool:
    if not await is_enabled(interaction.guild_id):
        await interaction.response.send_message(
            "ℹ️ Выдача ролей выключена. Включить: дашборд → «Роли» → тумблер «Включено».", ephemeral=True)
        return False
    if not await can_manage(interaction.user):
        await interaction.response.send_message(
            "❌ Управлять панелями могут администраторы и роль, выбранная в дашборде.", ephemeral=True)
        return False
    return True


async def _panel_autocomplete(interaction: discord.Interaction, current: str):
    out = []
    for p in await get_panels(interaction.guild_id):
        ch = interaction.guild.get_channel(p["channel_id"])
        name = f"#{p['id']} · {p['title']} ({'#' + ch.name if ch else 'канал удалён'})"
        if current.lower() in name.lower():
            out.append(app_commands.Choice(name=name[:100], value=p["id"]))
    return out[:25]


async def _done(interaction, ok_text, err):
    await interaction.followup.send(f"❌ {err}" if err else ok_text, ephemeral=True)


@panel_grp.command(name="create", description="Создать панель ролей в канале")
@app_commands.describe(channel="Канал для панели", title="Заголовок", mode="Кнопки или выпадающее меню",
                       description="Описание (перенос строки — \\n)", single="Только одна роль из панели",
                       color="Цвет в HEX, например #5865F2")
@app_commands.choices(mode=[app_commands.Choice(name="Кнопки", value="buttons"),
                            app_commands.Choice(name="Выпадающее меню", value="select")])
async def panel_create(interaction: discord.Interaction, channel: discord.TextChannel,
                       title: app_commands.Range[str, 1, 256], mode: app_commands.Choice[str],
                       description: app_commands.Range[str, 0, 2000] = None, single: bool = False,
                       color: str = None):
    if not await _guard(interaction):
        return
    if color and _parse_color(color) is None:
        return await interaction.response.send_message("❌ Цвет — HEX из 6 цифр, например `#5865F2`.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    pid, err = await create_panel(interaction.guild, channel, interaction.user.id, title.strip(),
                                  _text(description), mode.value, single, _parse_color(color))
    await _done(interaction, f"✅ Панель **#{pid}** создана в {channel.mention}. "
                             f"Добавь роли: `/roles panel add panel:{pid}`.", err)


@panel_grp.command(name="add", description="Добавить роль в панель")
@app_commands.describe(panel="Панель", role="Роль", label="Подпись (по умолчанию — имя роли)",
                       emoji="Эмодзи", description="Пояснение в меню (только для режима меню)")
@app_commands.autocomplete(panel=_panel_autocomplete)
async def panel_add(interaction: discord.Interaction, panel: int, role: discord.Role,
                    label: app_commands.Range[str, 1, 80] = None, emoji: str = None,
                    description: app_commands.Range[str, 1, 100] = None):
    if not await _guard(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    err = await add_role(interaction.guild, interaction.user, panel, role, label, emoji, description)
    await _done(interaction, f"✅ {role.mention} добавлена в панель #{panel}.", err)


@panel_grp.command(name="remove", description="Убрать роль из панели")
@app_commands.describe(panel="Панель", role="Роль")
@app_commands.autocomplete(panel=_panel_autocomplete)
async def panel_remove(interaction: discord.Interaction, panel: int, role: discord.Role):
    if not await _guard(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    err = await remove_role(interaction.guild, panel, role.id)
    await _done(interaction, f"✅ {role.mention} убрана из панели #{panel}.", err)


@panel_grp.command(name="edit", description="Изменить заголовок, описание, цвет или режим одной роли")
@app_commands.describe(panel="Панель", title="Новый заголовок", description="Новое описание (\\n — перенос, - — очистить)",
                       color="Цвет в HEX", single="Только одна роль из панели",
                       enabled="Включить или выключить панель (выключенная остаётся в канале без кнопок)")
@app_commands.autocomplete(panel=_panel_autocomplete)
async def panel_edit(interaction: discord.Interaction, panel: int,
                     title: app_commands.Range[str, 1, 256] = None,
                     description: app_commands.Range[str, 1, 2000] = None,
                     color: str = None, single: bool = None, enabled: bool = None):
    if not await _guard(interaction):
        return
    if color and _parse_color(color) is None:
        return await interaction.response.send_message("❌ Цвет — HEX из 6 цифр, например `#5865F2`.", ephemeral=True)
    await interaction.response.defer(ephemeral=True)
    desc = None if description is None else ("" if description.strip() == "-" else _text(description))
    err = await edit_panel(interaction.guild, panel, title=title and title.strip(), description=desc,
                           color=_parse_color(color), single=None if single is None else int(single),
                           enabled=None if enabled is None else int(enabled))
    await _done(interaction, f"✅ Панель #{panel} обновлена.", err)


@panel_grp.command(name="delete", description="Удалить панель и её сообщение")
@app_commands.describe(panel="Панель")
@app_commands.autocomplete(panel=_panel_autocomplete)
async def panel_delete(interaction: discord.Interaction, panel: int):
    if not await _guard(interaction):
        return
    await interaction.response.defer(ephemeral=True)
    err = await delete_panel(interaction.guild, panel)
    await _done(interaction, f"🗑️ Панель #{panel} удалена.", err)


@panel_grp.command(name="list", description="Все панели ролей на сервере")
async def panel_list(interaction: discord.Interaction):
    if not await _guard(interaction):
        return
    panels = await get_panels(interaction.guild_id)
    e = discord.Embed(title="Панели ролей", color=DEFAULT_COLOR)
    if not panels:
        e.description = "Панелей нет. Создать: `/roles panel create`."
    for p in panels[:25]:
        roles = await get_panel_roles(p["id"])
        link = f"https://discord.com/channels/{p['guild_id']}/{p['channel_id']}/{p['message_id']}"
        mode = ("меню" if p["mode"] == "select" else "кнопки") + (", одна роль" if p["single"] else "") + \
               ("" if p["enabled"] else ", **выключена**")
        names = ", ".join(f"<@&{r['role_id']}>" for r in roles) or "—"
        e.add_field(name=f"#{p['id']} · {p['title'][:80]}",
                    value=f"<#{p['channel_id']}> · {mode} · [сообщение]({link})\n{names}"[:1024], inline=False)
    await interaction.response.send_message(embed=e, ephemeral=True)


bot.tree.add_command(roles_grp)
