"""
Тикеты поддержки.

  • панель с кнопкой «Открыть тикет» → окно с вопросами сервера (анкета, до 5) → приватный канал;
  • в канале тикета кнопки «Взять в работу» и «Закрыть» (закрытие — с причиной);
  • при закрытии транскрипт переписки уходит в лог-канал (и автору в ЛС, если включено);
  • /ticket open|close|claim|add|remove|setup — то же самое командами;
  • канал удалили вручную — тикет закрывается в базе, человек может открыть новый.

Настройки — таблица ticket_settings, страница дашборда «Тикеты» (web_server.api_tickets*).
Функция выключена, пока админ её не включит. Работает на тарифе Premium.
Компоненты — DynamicItem, переживают рестарт (custom_id: tkt:...).
"""

import asyncio
import datetime
import io
import json

import aiosqlite
import discord
from discord import app_commands

from .config import DB_PATH, TIER_PREMIUM
from .core import bot, get_tier, report_error, upsell_embed

DEFAULT_COLOR   = 0x5865F2
DEFAULT_TITLE   = "Поддержка"
DEFAULT_DESC    = "Нажми кнопку ниже, чтобы открыть приватный тикет. Его увидишь только ты и команда поддержки."
DEFAULT_BUTTON  = "Открыть тикет"
DEFAULT_WELCOME = "Опиши проблему подробно — команда поддержки скоро ответит."
MAX_OPEN_LIMIT  = 5
TRANSCRIPT_MAX  = 5000          # сообщений в транскрипте
CLOSE_DELAY     = 5             # секунд до удаления канала

MAX_QUESTIONS   = 5             # предел полей в окне Discord
DEFAULT_QUESTION = {"label": "Что случилось?", "placeholder": "Коротко опиши вопрос — так ответят быстрее",
                    "style": "long", "required": False}

SETTING_KEYS = ("enabled", "category_id", "support_role", "log_channel", "max_open", "ping_support",
                "dm_transcript", "welcome", "panel_channel", "panel_message", "panel_title",
                "panel_desc", "panel_button", "panel_color", "counter", "questions")
DEFAULTS = {"enabled": 0, "category_id": 0, "support_role": 0, "log_channel": 0, "max_open": 1,
            "ping_support": 1, "dm_transcript": 0, "welcome": "", "panel_channel": 0, "panel_message": 0,
            "panel_title": "", "panel_desc": "", "panel_button": "", "panel_color": 0, "counter": 0,
            "questions": "[]"}

_opening: set = set()           # (guild_id, user_id) — защита от двойного нажатия
_closing: set = set()           # id тикетов, которые сейчас закрываются


def _now() -> str:
    return datetime.datetime.utcnow().isoformat(timespec="seconds")


def clean_questions(raw) -> list[dict]:
    """Список вопросов анкеты в безопасном виде (лимиты Discord: подпись 45, подсказка 100)."""
    if isinstance(raw, str):
        try:
            raw = json.loads(raw or "[]")
        except ValueError:
            raw = []
    out = []
    for q in raw if isinstance(raw, list) else []:
        if not isinstance(q, dict):
            continue
        label = " ".join(str(q.get("label") or "").split())[:45]
        if not label:
            continue
        out.append({"label": label,
                    "placeholder": " ".join(str(q.get("placeholder") or "").split())[:100],
                    "style": "long" if q.get("style") == "long" else "short",
                    "required": bool(q.get("required"))})
        if len(out) >= MAX_QUESTIONS:
            break
    return out


def questions_for(cfg: dict) -> list[dict]:
    """Вопросы сервера, а если их нет — один общий «Что случилось?»."""
    return clean_questions(cfg.get("questions")) or [DEFAULT_QUESTION]


def parse_answers(raw) -> list[dict]:
    try:
        data = json.loads(raw or "[]")
    except (TypeError, ValueError):
        return []
    return [a for a in data if isinstance(a, dict) and "q" in a] if isinstance(data, list) else []


# ── База ──────────────────────────────────────────────────────

async def get_settings(gid: int) -> dict:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM ticket_settings WHERE guild_id=?", (gid,)) as c:
            row = await c.fetchone()
    out = dict(DEFAULTS)
    if row:
        out.update({k: row[k] for k in row.keys() if k in DEFAULTS and row[k] is not None})
    return out


async def save_settings(gid: int, **values):
    values = {k: v for k, v in values.items() if k in SETTING_KEYS}
    if not values:
        return
    cols = ", ".join(values)
    marks = ", ".join("?" * len(values))
    upd = ", ".join(f"{k}=excluded.{k}" for k in values)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(f"INSERT INTO ticket_settings (guild_id, {cols}) VALUES (?, {marks}) "
                         f"ON CONFLICT(guild_id) DO UPDATE SET {upd}", (gid, *values.values()))
        await db.commit()


async def get_ticket(tid: int) -> dict | None:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tickets WHERE id=?", (tid,)) as c:
            row = await c.fetchone()
    return dict(row) if row else None


async def ticket_by_channel(channel_id: int, only_open=True) -> dict | None:
    q = "SELECT * FROM tickets WHERE channel_id=?" + (" AND status='open'" if only_open else "") + \
        " ORDER BY id DESC LIMIT 1"
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(q, (channel_id,)) as c:
            row = await c.fetchone()
    return dict(row) if row else None


async def list_tickets(gid: int, status: str, limit: int = 50) -> list[dict]:
    order = "id ASC" if status == "open" else "closed_at DESC, id DESC"
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute(f"SELECT * FROM tickets WHERE guild_id=? AND status=? ORDER BY {order} LIMIT ?",
                              (gid, status, limit)) as c:
            return [dict(r) for r in await c.fetchall()]


async def _user_open(gid: int, uid: int) -> list[dict]:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        async with db.execute("SELECT * FROM tickets WHERE guild_id=? AND user_id=? AND status='open'",
                              (gid, uid)) as c:
            return [dict(r) for r in await c.fetchall()]


async def _mark_closed(tid: int, closed_by: int, reason: str) -> bool:
    """True, если тикет был открыт и мы его закрыли (второй вызов вернёт False)."""
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("UPDATE tickets SET status='closed', closed_by=?, closed_at=?, close_reason=? "
                               "WHERE id=? AND status='open'", (closed_by, _now(), reason[:500], tid))
        await db.commit()
        return cur.rowcount > 0


# ── Проверки ──────────────────────────────────────────────────

def is_support(member: discord.Member, cfg: dict) -> bool:
    p = member.guild_permissions
    if p.administrator or p.manage_guild or p.manage_channels:
        return True
    rid = int(cfg.get("support_role") or 0)
    if rid:
        return any(r.id == rid for r in member.roles)
    return p.manage_messages


def _support_targets(guild: discord.Guild, cfg: dict) -> list[discord.Role]:
    """Роли, которым открывается канал тикета."""
    rid = int(cfg.get("support_role") or 0)
    role = guild.get_role(rid) if rid else None
    if role:
        return [role]
    return [r for r in guild.roles if r.permissions.manage_messages and not r.is_default()
            and not r.permissions.administrator]


def _allow(me: discord.Member, **perms) -> discord.PermissionOverwrite:
    """Overwrite только с теми правами, что есть у бота: чужие права Discord выдать не даст."""
    mine = me.guild_permissions
    return discord.PermissionOverwrite(**{k: v for k, v in perms.items()
                                          if mine.administrator or getattr(mine, k, False)})


MEMBER_PERMS = dict(view_channel=True, send_messages=True, read_message_history=True,
                    attach_files=True, embed_links=True)


def setup_problem(guild: discord.Guild) -> str | None:
    """Почему бот не сможет создавать тикеты. None — всё в порядке."""
    p = guild.me.guild_permissions
    if not (p.administrator or p.manage_channels):
        return "У бота нет права «Управлять каналами» — он не сможет создавать каналы тикетов."
    if not (p.administrator or p.manage_roles):
        return "У бота нет права «Управлять ролями» — он не сможет закрыть канал тикета от остальных."
    return None


async def premium_ok(gid: int) -> bool:
    return await get_tier(gid) >= TIER_PREMIUM


# ── Сообщения ─────────────────────────────────────────────────

def panel_embed(cfg: dict) -> discord.Embed:
    e = discord.Embed(title=(cfg.get("panel_title") or DEFAULT_TITLE)[:256],
                      description=(cfg.get("panel_desc") or DEFAULT_DESC)[:4000],
                      color=int(cfg.get("panel_color") or DEFAULT_COLOR))
    return e


def panel_view(cfg: dict) -> discord.ui.View:
    v = discord.ui.View(timeout=None)
    v.add_item(OpenButton(label=cfg.get("panel_button") or DEFAULT_BUTTON))
    return v


def ticket_view(tid: int, claimed_name: str | None = None) -> discord.ui.View:
    v = discord.ui.View(timeout=None)
    v.add_item(ClaimButton(tid, claimed_name))
    v.add_item(CloseButton(tid))
    return v


async def send_panel(guild: discord.Guild, channel: discord.TextChannel) -> str | None:
    """Отправляет панель (или обновляет, если она уже в этом канале). Возвращает текст ошибки."""
    cfg = await get_settings(guild.id)
    old_ch = guild.get_channel(int(cfg["panel_channel"] or 0))
    if old_ch and cfg["panel_message"] and old_ch.id != channel.id:
        try:
            await old_ch.get_partial_message(cfg["panel_message"]).delete()
        except discord.HTTPException:
            pass
    elif old_ch and cfg["panel_message"]:
        try:
            await old_ch.get_partial_message(cfg["panel_message"]).edit(embed=panel_embed(cfg), view=panel_view(cfg))
            return None
        except discord.NotFound:
            pass
        except discord.HTTPException as ex:
            return f"Не удалось обновить панель: {ex.text or ex}"
    try:
        msg = await channel.send(embed=panel_embed(cfg), view=panel_view(cfg))
    except discord.Forbidden:
        return f"У бота нет прав писать в #{channel.name} (нужны «Отправлять сообщения» и «Встраивать ссылки»)."
    except discord.HTTPException as ex:
        return f"Discord не принял сообщение: {ex.text or ex}"
    await save_settings(guild.id, panel_channel=channel.id, panel_message=msg.id)
    return None


async def refresh_panel(guild: discord.Guild) -> str | None:
    """Обновляет уже отправленную панель после смены текста. Нет панели — ничего не делает."""
    cfg = await get_settings(guild.id)
    ch = guild.get_channel(int(cfg["panel_channel"] or 0))
    if not ch or not cfg["panel_message"]:
        return None
    try:
        await ch.get_partial_message(cfg["panel_message"]).edit(embed=panel_embed(cfg), view=panel_view(cfg))
    except discord.NotFound:
        await save_settings(guild.id, panel_channel=0, panel_message=0)
        return "Сообщение панели удалено — отправь её заново."
    except discord.HTTPException as ex:
        return f"Не удалось обновить панель: {ex.text or ex}"
    return None


async def remove_panel(guild: discord.Guild):
    cfg = await get_settings(guild.id)
    ch = guild.get_channel(int(cfg["panel_channel"] or 0))
    if ch and cfg["panel_message"]:
        try:
            await ch.get_partial_message(cfg["panel_message"]).delete()
        except discord.HTTPException:
            pass
    await save_settings(guild.id, panel_channel=0, panel_message=0)


async def ensure_category(guild: discord.Guild, cfg: dict | None = None) -> tuple[int, str | None]:
    """Существующая категория тикетов или новая «🎫 Tickets». Повторно не создаёт."""
    cfg = cfg or await get_settings(guild.id)
    cat = guild.get_channel(int(cfg.get("category_id") or 0))
    if isinstance(cat, discord.CategoryChannel):
        return cat.id, None
    cat = discord.utils.find(lambda c: c.name == "🎫 Tickets", guild.categories)
    if cat is None:
        try:
            cat = await guild.create_category("🎫 Tickets", reason="Тикеты: категория для каналов")
        except discord.HTTPException as ex:
            return 0, f"Не удалось создать категорию: {getattr(ex, 'text', '') or ex}"
    await save_settings(guild.id, category_id=cat.id)
    return cat.id, None


# ── Открытие ──────────────────────────────────────────────────

async def open_ticket(member: discord.Member, answers: list[dict]) -> tuple[discord.TextChannel | None, str | None]:
    """answers — [{"q": вопрос, "a": ответ}] из окна анкеты."""
    guild = member.guild
    answers = [{"q": str(x.get("q") or "")[:45], "a": str(x.get("a") or "").strip()[:1000]} for x in answers][:MAX_QUESTIONS]
    # Короткая «причина» для списков на сайте и в логе — первый непустой ответ
    reason = next((" ".join(x["a"].split()) for x in answers if x["a"]), "")[:300] or "Без описания"
    if not await premium_ok(guild.id):
        return None, "Тикеты работают на тарифе **Premium**."
    cfg = await get_settings(guild.id)
    if not cfg["enabled"]:
        return None, "Тикеты на этом сервере выключены."
    problem = setup_problem(guild)
    if problem:
        return None, problem + " Администрация уведомлена."

    key = (guild.id, member.id)
    if key in _opening:
        return None, "Тикет уже создаётся, подожди секунду."
    _opening.add(key)
    try:
        mine = await _user_open(guild.id, member.id)
        # Канал удалили, а запись осталась (бот был выключен) — закрываем такие записи
        alive = []
        for t in mine:
            if guild.get_channel(t["channel_id"] or 0):
                alive.append(t)
            else:
                await _mark_closed(t["id"], 0, "Канал удалён")
        limit = max(1, min(MAX_OPEN_LIMIT, int(cfg["max_open"] or 1)))
        if len(alive) >= limit:
            links = ", ".join(f"<#{t['channel_id']}>" for t in alive)
            return None, (f"У тебя уже есть открытый тикет: {links}" if limit == 1
                          else f"Можно держать не больше {limit} открытых тикетов: {links}")

        me = guild.me
        overwrites = {
            guild.default_role: discord.PermissionOverwrite(view_channel=False),
            me: _allow(me, manage_channels=True, manage_messages=True, **MEMBER_PERMS),
            member: _allow(me, **MEMBER_PERMS),
        }
        for role in _support_targets(guild, cfg):
            overwrites[role] = _allow(me, **MEMBER_PERMS)

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("INSERT INTO ticket_settings (guild_id, counter) VALUES (?, 1) "
                             "ON CONFLICT(guild_id) DO UPDATE SET counter=counter+1", (guild.id,))
            async with db.execute("SELECT counter FROM ticket_settings WHERE guild_id=?", (guild.id,)) as c:
                number = (await c.fetchone())[0]
            await db.commit()

        nick = "".join(ch for ch in member.name.lower() if ch.isalnum() or ch in "-_")[:20] or "user"
        name = f"ticket-{number:04d}-{nick}"
        category = guild.get_channel(int(cfg["category_id"] or 0))
        if not isinstance(category, discord.CategoryChannel):
            category = None
        topic = f"Тикет #{number} · {member} ({member.id}) · {reason[:200]}"
        try:
            try:
                ch = await guild.create_text_channel(name, category=category, overwrites=overwrites,
                                                     topic=topic[:1024], reason=f"Тикет #{number} от {member}")
            except discord.HTTPException as ex:
                if category is None or isinstance(ex, discord.Forbidden):
                    raise
                # В категории уже 50 каналов (предел Discord) — создаём без неё
                ch = await guild.create_text_channel(name, overwrites=overwrites, topic=topic[:1024],
                                                     reason=f"Тикет #{number} от {member}")
        except discord.HTTPException as ex:
            await report_error("TICKETS", ex, f"Сервер {guild.id} ({guild.name}): не создался канал тикета")
            return None, "Не удалось создать канал тикета. Администрация уведомлена."

        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute(
                "INSERT INTO tickets (guild_id, user_id, channel_id, status, created_at, number, reason, answers) "
                "VALUES (?,?,?,?,?,?,?,?)", (guild.id, member.id, ch.id, "open", _now(), number, reason,
                                             json.dumps(answers, ensure_ascii=False)))
            tid = cur.lastrowid
            await db.commit()

        e = discord.Embed(title=f"🎫 Тикет #{number:04d}", description=cfg["welcome"] or DEFAULT_WELCOME,
                          color=int(cfg["panel_color"] or DEFAULT_COLOR), timestamp=datetime.datetime.utcnow())
        e.add_field(name="Участник", value=member.mention, inline=False)
        for x in answers:
            e.add_field(name=x["q"] or "Ответ", value=x["a"][:1024] or "—", inline=False)
        e.set_footer(text="Закрыть тикет — кнопкой ниже или /ticket close")
        ping = [member.mention]
        roles = []
        if cfg["ping_support"]:
            sr = guild.get_role(int(cfg["support_role"] or 0))
            if sr:
                ping.append(sr.mention)
                roles = [sr]
        try:
            await ch.send(" ".join(ping), embed=e, view=ticket_view(tid),
                          allowed_mentions=discord.AllowedMentions(users=[member], roles=roles,
                                                                   everyone=False))
        except discord.HTTPException as ex:
            await report_error("TICKETS", ex, f"Сервер {guild.id}: не отправилось приветствие в тикет #{number}")
        return ch, None
    finally:
        _opening.discard(key)


# ── Закрытие ──────────────────────────────────────────────────

async def build_transcript(channel: discord.TextChannel, answers: list[dict] = ()) -> tuple[bytes, int]:
    lines, count = [], 0
    async for m in channel.history(limit=TRANSCRIPT_MAX, oldest_first=True):
        count += 1
        ts = m.created_at.strftime("%Y-%m-%d %H:%M")
        text = m.clean_content or ""
        for e in m.embeds:
            parts = [p for p in (e.title, e.description) if p]
            parts += [f"{f.name}: {f.value}" for f in e.fields]
            if parts:
                text += ("\n" if text else "") + "[embed] " + " | ".join(parts)
        for a in m.attachments:
            text += ("\n" if text else "") + f"[файл] {a.filename} {a.url}"
        lines.append(f"[{ts} UTC] {m.author} ({m.author.id}): {text}")
    head = f"Транскрипт #{channel.name} · {channel.guild.name} · сообщений: {count}\n" + "=" * 60 + "\n"
    if answers:
        head += "Анкета:\n" + "".join(f"  {a['q']}: {a.get('a') or '—'}\n" for a in answers) + "=" * 60 + "\n"
    return (head + "\n".join(lines)).encode("utf-8"), count


async def close_ticket(guild: discord.Guild, ticket: dict, closer: discord.abc.User | None,
                       reason: str = "") -> str | None:
    """Закрывает тикет: транскрипт → лог-канал (и автору), удаление канала. Возвращает ошибку или None."""
    tid = ticket["id"]
    if tid in _closing:
        return "Тикет уже закрывается."
    _closing.add(tid)
    try:
        if not await _mark_closed(tid, closer.id if closer else 0, reason):
            return "Тикет уже закрыт."
        cfg = await get_settings(guild.id)
        ch = guild.get_channel(ticket["channel_id"] or 0)
        data, count = b"", 0
        if isinstance(ch, discord.TextChannel):
            try:
                await ch.send(f"🔒 Тикет закрыт{' — ' + closer.mention if closer else ''}. "
                              f"Канал удалится через {CLOSE_DELAY} сек.",
                              allowed_mentions=discord.AllowedMentions.none())
            except discord.HTTPException:
                pass
            try:
                data, count = await build_transcript(ch, parse_answers(ticket.get("answers")))
            except discord.HTTPException as ex:
                await report_error("TICKETS", ex, f"Сервер {guild.id}: не собрался транскрипт тикета #{ticket['number']}")

        number = ticket.get("number") or tid
        e = discord.Embed(title=f"🔒 Тикет #{number:04d} закрыт", color=0x36393F,
                          timestamp=datetime.datetime.utcnow())
        e.add_field(name="Автор", value=f"<@{ticket['user_id']}>", inline=True)
        e.add_field(name="Закрыл", value=closer.mention if closer else "—", inline=True)
        if ticket.get("claimed_by"):
            e.add_field(name="Взял в работу", value=f"<@{ticket['claimed_by']}>", inline=True)
        answers = parse_answers(ticket.get("answers"))
        if answers:
            for a in answers:
                e.add_field(name=a["q"] or "Ответ", value=(a.get("a") or "—")[:300], inline=False)
        else:
            e.add_field(name="Причина обращения", value=(ticket.get("reason") or "—")[:1024], inline=False)
        if reason:
            e.add_field(name="Причина закрытия", value=reason[:1024], inline=False)
        if ticket.get("created_at"):
            try:
                opened = datetime.datetime.fromisoformat(ticket["created_at"])
                e.add_field(name="Открыт", value=discord.utils.format_dt(opened.replace(
                    tzinfo=datetime.timezone.utc), "f"), inline=True)
            except ValueError:
                pass
        e.add_field(name="Сообщений", value=str(count), inline=True)
        fname = f"ticket-{number:04d}.txt"

        log_ch = guild.get_channel(int(cfg["log_channel"] or 0))
        if isinstance(log_ch, discord.TextChannel):
            try:
                msg = await log_ch.send(embed=e, file=discord.File(io.BytesIO(data), fname) if data else None,
                                        allowed_mentions=discord.AllowedMentions.none())
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("UPDATE tickets SET log_url=? WHERE id=?", (msg.jump_url, tid))
                    await db.commit()
            except discord.HTTPException as ex:
                await report_error("TICKETS", ex, f"Сервер {guild.id}: транскрипт не отправился в лог-канал")

        if cfg["dm_transcript"] and data:
            user = guild.get_member(ticket["user_id"])
            if user:
                try:
                    de = discord.Embed(title=f"Тикет #{number:04d} на {guild.name} закрыт",
                                       description=f"Причина: {reason}" if reason else None, color=0x36393F)
                    await user.send(embed=de, file=discord.File(io.BytesIO(data), fname))
                except discord.HTTPException:
                    pass   # ЛС закрыты — не страшно

        if isinstance(ch, discord.TextChannel):
            await asyncio.sleep(CLOSE_DELAY)
            try:
                await ch.delete(reason=f"Тикет #{number} закрыт")
            except discord.NotFound:
                pass
            except discord.HTTPException as ex:
                await report_error("TICKETS", ex, f"Сервер {guild.id}: не удалился канал тикета #{number}")
                return "Тикет закрыт, но канал удалить не получилось — удали его вручную."
        return None
    finally:
        _closing.discard(tid)


async def claim_ticket(ticket: dict, member: discord.Member) -> str | None:
    if ticket.get("claimed_by"):
        return f"Тикет уже взял <@{ticket['claimed_by']}>."
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("UPDATE tickets SET claimed_by=? WHERE id=? AND claimed_by=0 AND status='open'",
                               (member.id, ticket["id"]))
        await db.commit()
    return None if cur.rowcount else "Тикет уже взяли или закрыли."


# ── Компоненты ────────────────────────────────────────────────

class OpenModal(discord.ui.Modal):
    """Окно анкеты: вопросы сервера (до 5) или один общий «Что случилось?»."""
    def __init__(self, questions: list[dict]):
        super().__init__(title="Новый тикет", timeout=900)
        self.inputs = []
        for q in questions[:MAX_QUESTIONS]:
            long = q["style"] == "long"
            ti = discord.ui.TextInput(label=q["label"], placeholder=q["placeholder"] or None,
                                      style=discord.TextStyle.paragraph if long else discord.TextStyle.short,
                                      max_length=900 if long else 200, required=q["required"])
            self.add_item(ti)
            self.inputs.append((q["label"], ti))

    async def on_submit(self, interaction: discord.Interaction):
        await interaction.response.defer(ephemeral=True, thinking=True)
        answers = [{"q": label, "a": str(ti.value or "").strip()} for label, ti in self.inputs]
        ch, err = await open_ticket(interaction.user, answers)
        await interaction.followup.send(f"❌ {err}" if err else f"✅ Тикет открыт: {ch.mention}", ephemeral=True)


async def _ask_and_open(interaction: discord.Interaction):
    """Общая точка для кнопки и /ticket open: проверки → окно анкеты."""
    cfg = await get_settings(interaction.guild_id)
    if not await premium_ok(interaction.guild_id):
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    if not cfg["enabled"]:
        return await interaction.response.send_message("❌ Тикеты на этом сервере сейчас выключены.",
                                                       ephemeral=True)
    await interaction.response.send_modal(OpenModal(questions_for(cfg)))


class CloseModal(discord.ui.Modal, title="Закрыть тикет"):
    reason = discord.ui.TextInput(label="Причина закрытия (необязательно)", style=discord.TextStyle.short,
                                  max_length=300, required=False, placeholder="Вопрос решён")

    def __init__(self, tid: int):
        super().__init__()
        self.tid = tid

    async def on_submit(self, interaction: discord.Interaction):
        ticket = await get_ticket(self.tid)
        if not ticket or ticket["status"] != "open":
            return await interaction.response.send_message("Тикет уже закрыт.", ephemeral=True)
        await interaction.response.send_message("🔒 Закрываю…", ephemeral=True)
        err = await close_ticket(interaction.guild, ticket, interaction.user, str(self.reason.value or "").strip())
        if err:
            try:
                await interaction.followup.send(f"❌ {err}", ephemeral=True)
            except discord.HTTPException:
                pass


class OpenButton(discord.ui.DynamicItem[discord.ui.Button], template=r"tkt:open"):
    def __init__(self, label: str = DEFAULT_BUTTON):
        super().__init__(discord.ui.Button(label=label[:80], emoji="🎫", style=discord.ButtonStyle.primary,
                                           custom_id="tkt:open"))

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(item.label or DEFAULT_BUTTON)

    async def callback(self, interaction: discord.Interaction):
        await _ask_and_open(interaction)


class ClaimButton(discord.ui.DynamicItem[discord.ui.Button], template=r"tkt:claim:(?P<tid>\d+)"):
    def __init__(self, tid: int, claimed_name: str | None = None):
        self.tid = tid
        super().__init__(discord.ui.Button(
            label=f"В работе: {claimed_name}"[:80] if claimed_name else "Взять в работу",
            emoji="🙋", style=discord.ButtonStyle.secondary, disabled=bool(claimed_name),
            custom_id=f"tkt:claim:{tid}"))

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(int(match["tid"]))

    async def callback(self, interaction: discord.Interaction):
        ticket = await get_ticket(self.tid)
        if not ticket or ticket["status"] != "open":
            return await interaction.response.send_message("Тикет уже закрыт.", ephemeral=True)
        if not is_support(interaction.user, await get_settings(interaction.guild_id)):
            return await interaction.response.send_message("❌ Взять тикет может только команда поддержки.",
                                                           ephemeral=True)
        err = await claim_ticket(ticket, interaction.user)
        if err:
            return await interaction.response.send_message(f"❌ {err}", ephemeral=True)
        await interaction.response.edit_message(view=ticket_view(self.tid, interaction.user.display_name))
        await interaction.followup.send(f"🙋 {interaction.user.mention} взял тикет в работу.",
                                        allowed_mentions=discord.AllowedMentions.none())


class CloseButton(discord.ui.DynamicItem[discord.ui.Button], template=r"tkt:close:(?P<tid>\d+)"):
    def __init__(self, tid: int):
        self.tid = tid
        super().__init__(discord.ui.Button(label="Закрыть", emoji="🔒", style=discord.ButtonStyle.danger,
                                           custom_id=f"tkt:close:{tid}"))

    @classmethod
    async def from_custom_id(cls, interaction, item, match):
        return cls(int(match["tid"]))

    async def callback(self, interaction: discord.Interaction):
        ticket = await get_ticket(self.tid)
        if not ticket or ticket["status"] != "open":
            return await interaction.response.send_message("Тикет уже закрыт.", ephemeral=True)
        cfg = await get_settings(interaction.guild_id)
        if interaction.user.id != ticket["user_id"] and not is_support(interaction.user, cfg):
            return await interaction.response.send_message("❌ Закрыть тикет может автор или поддержка.",
                                                           ephemeral=True)
        await interaction.response.send_modal(CloseModal(self.tid))


DYNAMIC_ITEMS = (OpenButton, ClaimButton, CloseButton)


# ── Чистка при ручном удалении ────────────────────────────────

@bot.listen("on_guild_channel_delete")
async def _tk_channel_delete(channel):
    try:
        ticket = await ticket_by_channel(channel.id)
        if ticket and ticket["id"] not in _closing and await _mark_closed(ticket["id"], 0, "Канал удалён вручную"):
            cfg = await get_settings(channel.guild.id)
            log_ch = channel.guild.get_channel(int(cfg["log_channel"] or 0))
            if isinstance(log_ch, discord.TextChannel):
                e = discord.Embed(title=f"🗑️ Тикет #{(ticket['number'] or ticket['id']):04d} — канал удалён вручную",
                                  description=f"Автор: <@{ticket['user_id']}>. Транскрипт не сохранён.",
                                  color=0x36393F, timestamp=datetime.datetime.utcnow())
                try:
                    await log_ch.send(embed=e, allowed_mentions=discord.AllowedMentions.none())
                except discord.HTTPException:
                    pass
        cfg = await get_settings(channel.guild.id)
        changes = {}
        if cfg["panel_channel"] == channel.id:
            changes.update(panel_channel=0, panel_message=0)
        if cfg["category_id"] == channel.id:
            changes["category_id"] = 0
        if cfg["log_channel"] == channel.id:
            changes["log_channel"] = 0
        if changes:
            await save_settings(channel.guild.id, **changes)
    except Exception as ex:
        await report_error("TICKETS", ex, f"Сервер {channel.guild.id}: обработка удаления канала {channel.id}")


@bot.listen("on_raw_message_delete")
async def _tk_message_delete(payload):
    if not payload.guild_id:
        return
    cfg = await get_settings(payload.guild_id)
    if cfg["panel_message"] and cfg["panel_message"] == payload.message_id:
        await save_settings(payload.guild_id, panel_channel=0, panel_message=0)


# ── Команды ───────────────────────────────────────────────────

ticket_grp = app_commands.Group(name="ticket", description="Тикеты поддержки [Premium]", guild_only=True)


async def _ticket_here(interaction: discord.Interaction) -> dict | None:
    t = await ticket_by_channel(interaction.channel_id)
    if not t:
        await interaction.response.send_message("❌ Эта команда работает только в канале тикета.", ephemeral=True)
    return t


@ticket_grp.command(name="open", description="Открыть тикет (появится окно с вопросами)")
async def t_open(interaction: discord.Interaction):
    await _ask_and_open(interaction)


@ticket_grp.command(name="close", description="Закрыть этот тикет (транскрипт уйдёт в лог-канал)")
@app_commands.describe(reason="Причина закрытия")
async def t_close(interaction: discord.Interaction, reason: app_commands.Range[str, 0, 300] = ""):
    t = await _ticket_here(interaction)
    if not t:
        return
    if interaction.user.id != t["user_id"] and not is_support(interaction.user, await get_settings(interaction.guild_id)):
        return await interaction.response.send_message("❌ Закрыть тикет может автор или поддержка.", ephemeral=True)
    await interaction.response.send_message("🔒 Закрываю…", ephemeral=True)
    err = await close_ticket(interaction.guild, t, interaction.user, reason.strip())
    if err:
        try:
            await interaction.followup.send(f"❌ {err}", ephemeral=True)
        except discord.HTTPException:
            pass


@ticket_grp.command(name="claim", description="Взять этот тикет в работу")
async def t_claim(interaction: discord.Interaction):
    t = await _ticket_here(interaction)
    if not t:
        return
    if not is_support(interaction.user, await get_settings(interaction.guild_id)):
        return await interaction.response.send_message("❌ Только для команды поддержки.", ephemeral=True)
    err = await claim_ticket(t, interaction.user)
    await interaction.response.send_message(f"❌ {err}" if err else f"🙋 {interaction.user.mention} взял тикет в работу.",
                                            ephemeral=bool(err), allowed_mentions=discord.AllowedMentions.none())


async def _set_access(interaction: discord.Interaction, user: discord.Member, add: bool):
    t = await _ticket_here(interaction)
    if not t:
        return
    if not is_support(interaction.user, await get_settings(interaction.guild_id)):
        return await interaction.response.send_message("❌ Только для команды поддержки.", ephemeral=True)
    if not add and user.id == t["user_id"]:
        return await interaction.response.send_message("❌ Автора тикета убрать нельзя — закрой тикет.", ephemeral=True)
    try:
        await interaction.channel.set_permissions(
            user, overwrite=_allow(interaction.guild.me, **MEMBER_PERMS) if add else None,
            reason=f"Тикет: {'добавил' if add else 'убрал'} {interaction.user}")
    except discord.HTTPException:
        return await interaction.response.send_message("❌ У бота не хватает прав изменить доступ к каналу.",
                                                       ephemeral=True)
    await interaction.response.send_message(
        f"{'➕' if add else '➖'} {user.mention} {'добавлен в тикет' if add else 'убран из тикета'}.",
        allowed_mentions=discord.AllowedMentions(users=[user] if add else []))


@ticket_grp.command(name="add", description="Добавить участника в этот тикет")
async def t_add(interaction: discord.Interaction, user: discord.Member):
    await _set_access(interaction, user, True)


@ticket_grp.command(name="remove", description="Убрать участника из этого тикета")
async def t_remove(interaction: discord.Interaction, user: discord.Member):
    await _set_access(interaction, user, False)


@ticket_grp.command(name="setup", description="Включить тикеты и отправить панель с кнопкой в этот канал")
async def t_setup(interaction: discord.Interaction):
    if not interaction.user.guild_permissions.manage_guild:
        return await interaction.response.send_message("❌ Нужно право «Управлять сервером».", ephemeral=True)
    if not await premium_ok(interaction.guild_id):
        return await interaction.response.send_message(embed=upsell_embed("Premium"), ephemeral=True)
    problem = setup_problem(interaction.guild)
    if problem:
        return await interaction.response.send_message(f"❌ {problem}", ephemeral=True)
    if not isinstance(interaction.channel, discord.TextChannel):
        return await interaction.response.send_message("❌ Запусти команду в текстовом канале.", ephemeral=True)
    await interaction.response.defer(ephemeral=True, thinking=True)
    _, err = await ensure_category(interaction.guild)
    if err:
        return await interaction.followup.send(f"❌ {err}", ephemeral=True)
    await save_settings(interaction.guild_id, enabled=1)
    err = await send_panel(interaction.guild, interaction.channel)
    if err:
        return await interaction.followup.send(f"❌ {err}", ephemeral=True)
    await interaction.followup.send(
        "✅ Тикеты включены, панель отправлена. Роль поддержки, лог-канал для транскриптов и тексты "
        "настраиваются на сайте: дашборд → «Тикеты».", ephemeral=True)


bot.tree.add_command(ticket_grp)
