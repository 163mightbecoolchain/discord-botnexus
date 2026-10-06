"""
Ядро: объект бота, интенты, кулдауны, ИИ-запросы, очередь логов, анти-нюк, тарифы.
"""

import discord
from discord.ext import commands
import aiohttp
import aiosqlite
import os, asyncio, random, time, json, datetime
from functools import wraps
from .config import (
    ANTHROPIC_KEY,
    DB_PATH,
    GEMINI_KEY,
    GEMINI_MODEL,
    GROQ_KEY,
    GROQ_MODEL,
    TIER_FREE,
    TIER_NAMES,
    TIER_PREMIUM,
)
from .database import get_log_ch, is_enabled
from .ui import build_embed, C, make_embed

_cooldowns: dict = {}
_spam_tracker: dict = {}
# Подавляет автоматическое DM от on_member_update когда вызывающий код
# (например /warn) сам отправит объединённое сообщение
_suppress_next_timeout_dm: set = set()
_raid_tracker: dict = {}

# ── Анти-нюк трекер ──────────────────────────────────────────
# Ключ: (guild_id, mod_id, action_type) → [timestamps]
_antinuke_tracker: dict = {}
ANTINUKE_LIMITS = {
    "ban":     (5, 30),   # 5 банов за 30 сек
    "kick":    (5, 30),   # 5 киков за 30 сек
    "mute":    (8, 30),   # 8 мутов за 30 сек
    "channel": (3, 20),   # 3 удаления каналов за 20 сек
    "role":    (3, 20),   # 3 удаления ролей за 20 сек
}


async def antinuke_check(guild: discord.Guild, mod_id: int, action: str) -> bool:
    """Возвращает True если лимит превышён (нюк-атака)."""
    if not guild or not mod_id:
        return False
    # Владелец сервера исключён из проверки
    if guild.owner_id == mod_id:
        return False
    limit, window = ANTINUKE_LIMITS.get(action, (10, 60))
    key = (guild.id, mod_id, action)
    now = time.time()
    _antinuke_tracker.setdefault(key, [])
    _antinuke_tracker[key] = [t for t in _antinuke_tracker[key] if now - t < window]
    _antinuke_tracker[key].append(now)
    if len(_antinuke_tracker[key]) >= limit:
        await _antinuke_alert(guild, mod_id, action, len(_antinuke_tracker[key]), window)
        return True
    return False


async def _antinuke_alert(guild: discord.Guild, mod_id: int,
                           action: str, count: int, window: int):
    """Алерт владельцу и в лог-канал при обнаружении нюка."""
    ch  = await get_log_ch(guild)
    mod = guild.get_member(mod_id)
    labels = {"ban": "банов", "kick": "киков", "mute": "мутов",
              "channel": "удалений каналов", "role": "удалений ролей"}
    e = build_embed(C.DANGER)
    e.set_author(name="🚨 Анти-нюк — подозрительная активность")
    e.add_field(name="Модератор",
                value=f"{mod.mention if mod else mod_id} (`{mod.display_name if mod else mod_id}`)",
                inline=True)
    e.add_field(name="Действие",
                value=f"**{count} {labels.get(action, action)}** за {window} сек.",
                inline=True)
    e.add_field(name="Рекомендация",
                value="Немедленно проверь права этого модератора.",
                inline=False)
    if ch:
        try: await ch.send(embed=e)
        except Exception: pass
    if guild.owner:
        try: await guild.owner.send(embed=e)
        except Exception: pass

def cooldown(seconds: int):
    def decorator(func):
        @wraps(func)
        async def wrapper(interaction: discord.Interaction, *args, **kwargs):
            key = (interaction.user.id, func.__name__)
            now = time.time()
            remaining = seconds - (now - _cooldowns.get(key, 0))
            if remaining > 0:
                return await interaction.response.send_message(
                    f"⏳ Cooldown: **{remaining:.1f}s**", ephemeral=True)
            _cooldowns[key] = now
            return await func(interaction, *args, **kwargs)
        return wrapper
    return decorator

async def get_tier(gid):
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT tier,expires_at FROM subscriptions WHERE guild_id=?", (gid,)) as c:
            row = await c.fetchone()
            if not row: return TIER_FREE
            tier, exp = row
            # Пустой expires_at = бессрочная подписка
            if exp and datetime.datetime.utcnow() > datetime.datetime.fromisoformat(exp):
                await db.execute("UPDATE subscriptions SET tier=0 WHERE guild_id=?", (gid,))
                await db.commit()
                # Раньше подписка сгорала молча и владелец не понимал,
                # почему половина бота перестала работать
                asyncio.create_task(notify_tier_expired(gid, tier))
                return TIER_FREE
            return tier


async def notify_tier_expired(gid: int, old_tier: int):
    """Сообщает владельцу и в лог-канал, что подписка сервера закончилась"""
    try:
        guild = bot.get_guild(gid)
        if not guild:
            return
        e = build_embed(C.WARNING)
        e.set_author(name="⏳ Подписка закончилась")
        e.description = (
            f"Тариф **{TIER_NAMES.get(old_tier, '?')}** на сервере **{guild.name}** истёк.\n"
            f"Сервер переведён на **Free**."
        )
        e.add_field(
            name="Что перестало работать",
            value=("• Логирование событий безопасности\n"
                   "• Стилометрия и детект обхода бана\n"
                   "• Карантин новых аккаунтов\n"
                   "• Анти-рейд и анти-спам"),
            inline=False
        )
        e.set_footer(text="Продлить: /setpremium")
        ch = await get_log_ch(guild)
        if ch:
            try: await ch.send(embed=e)
            except Exception: pass
        if guild.owner:
            try: await guild.owner.send(embed=e)
            except Exception: pass
        print(f"[TIER] Подписка сервера {gid} истекла (был тир {old_tier})")
    except Exception as ex:
        print(f"[TIER] notify error: {ex}")

async def sec_check(guild, key):
    if await get_tier(guild.id) < TIER_PREMIUM: return None
    if not await is_enabled(guild.id, key): return None
    return await get_log_ch(guild)

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  BOT
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

intents = discord.Intents.default()
# Привилегированные интенты. Их можно временно отключить переменными окружения
# (INTENT_MEMBERS=0 / INTENT_MESSAGE_CONTENT=0), чтобы бот поднялся даже когда
# в Dev Portal они ещё не включены — часть функций при этом отвалится.
intents.members         = os.getenv("INTENT_MEMBERS", "1") != "0"
intents.message_content = os.getenv("INTENT_MESSAGE_CONTENT", "1") != "0"
# presences не запрашиваем — статусы и активности боту не нужны
# Префикс-команд больше нет: чтение чужих сообщений ради префикса требует
# Message Content Intent, а весь функционал переехал на слэш-команды.
# Префикс задан заведомо недостижимым — команды по тексту не сработают.
bot = commands.Bot(command_prefix=commands.when_mentioned, intents=intents)
# ID владельцев бота через запятую или пробел: OWNER_IDS=123,456
OWNER_IDS = {int(x) for x in os.getenv("OWNER_IDS", "").replace(",", " ").split() if x.isdigit()}

def upsell_embed(req):
    e = make_embed(
        title="🔒 Требуется апгрейд",
        description=(
            f"Эта функция требует **{req}**\n\n"
            f"⭐ **Premium** — €4.99/мес\n"
            f"💎 **Pro** — €9.99/мес\n\n"
            f"witnessbot.gg/premium"
        ),
        color=C.DANGER
    )
    return e

# ── AI: Groq (free) → Gemini (free) → Claude (paid) ──────────
async def ask_ai(prompt, system="You are Witness, a helpful Discord assistant. Be concise."):
    """
    Цепочка провайдеров: Groq → Gemini → Anthropic.
    Раньше любая ошибка глушилась и пользователь видел «добавь ключ в .env»,
    даже когда ключ был на месте, а запрос падал по другой причине.
    Теперь причины пишутся в лог, а пользователю возвращается понятный текст.
    """
    tried = []          # какие провайдеры пробовали и чем закончилось

    # ── Groq ──────────────────────────────────────────────────
    if GROQ_KEY:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post("https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {GROQ_KEY}",
                             "Content-Type": "application/json"},
                    json={"model": GROQ_MODEL,
                          "messages": [{"role": "system", "content": system},
                                       {"role": "user", "content": prompt}],
                          "max_tokens": 600},
                    timeout=aiohttp.ClientTimeout(total=20)) as r:
                    data = await r.json()
            if r.status == 200 and "choices" in data:
                return data["choices"][0]["message"]["content"]
            err = (data.get("error") or {}).get("message") or str(data)[:200]
            tried.append(f"Groq: HTTP {r.status} · {err}")
            print(f"[AI] Groq отказал: HTTP {r.status} · {err}")
        except Exception as ex:
            tried.append(f"Groq: {type(ex).__name__}")
            print(f"[AI] Groq недоступен: {ex}")
    else:
        tried.append("Groq: ключ не задан")

    # ── Gemini ────────────────────────────────────────────────
    if GEMINI_KEY:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/"
                    f"{GEMINI_MODEL}:generateContent?key={GEMINI_KEY}",
                    json={"contents": [{"parts": [{"text": f"{system}\n\n{prompt}"}]}]},
                    timeout=aiohttp.ClientTimeout(total=20)) as r:
                    data = await r.json()
            if r.status == 200 and data.get("candidates"):
                return data["candidates"][0]["content"]["parts"][0]["text"]
            err = (data.get("error") or {}).get("message") or str(data)[:200]
            tried.append(f"Gemini: HTTP {r.status} · {err}")
            print(f"[AI] Gemini отказал: HTTP {r.status} · {err}")
        except Exception as ex:
            tried.append(f"Gemini: {type(ex).__name__}")
            print(f"[AI] Gemini недоступен: {ex}")
    else:
        tried.append("Gemini: ключ не задан")

    # ── Anthropic ─────────────────────────────────────────────
    if ANTHROPIC_KEY:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post("https://api.anthropic.com/v1/messages",
                    headers={"x-api-key": ANTHROPIC_KEY,
                             "anthropic-version": "2023-06-01",
                             "content-type": "application/json"},
                    json={"model": "claude-haiku-4-5-20251001", "max_tokens": 600,
                          "system": system,
                          "messages": [{"role": "user", "content": prompt}]},
                    timeout=aiohttp.ClientTimeout(total=30)) as r:
                    data = await r.json()
            if r.status == 200 and data.get("content"):
                return data["content"][0]["text"]
            err = (data.get("error") or {}).get("message") or str(data)[:200]
            tried.append(f"Anthropic: HTTP {r.status} · {err}")
            print(f"[AI] Anthropic отказал: HTTP {r.status} · {err}")
        except Exception as ex:
            tried.append(f"Anthropic: {type(ex).__name__}")
            print(f"[AI] Anthropic недоступен: {ex}")

    # ── Ни один не ответил ────────────────────────────────────
    have_key = bool(GROQ_KEY or GEMINI_KEY or ANTHROPIC_KEY)
    if not have_key:
        return ("❌ AI не настроен: задай `GROQ_API_KEY` или `GEMINI_API_KEY` "
                "в переменных окружения (оба бесплатны).")
    detail = " · ".join(t for t in tried if "ключ не задан" not in t)
    return ("❌ Не удалось получить ответ от AI.\n"
            f"```{detail[:600]}```\n"
            "Ключ есть, но запрос не прошёл — проверь, что он действителен "
            "и не исчерпан лимит.")

# ── Invite cache — инициализируем сразу на уровне бота ───────
# Ключ: "guild_id:invite_code" → uses (int)
# Это гарантирует что cache существует до on_ready
_invite_cache: dict = {}

async def refresh_invite_cache(guild) -> bool:
    """Загружает инвайты гильдии в кэш. Возвращает True если успешно."""
    try:
        invites = await guild.invites()
        for inv in invites:
            _invite_cache[f"{guild.id}:{inv.code}"] = inv.uses or 0
        print(f"✅ Invite cache loaded for {guild.name}: {len(invites)} invites")
        return True
    except discord.Forbidden:
        print(f"⚠️ [{guild.name}] Нет прав MANAGE_GUILD — инвайт-трекинг отключён")
        return False
    except Exception as ex:
        print(f"⚠️ [{guild.name}] Invite cache error: {ex}")
        return False


async def notify_mute_over(guild_id: int, user_id: int, early: bool = False):
    """Сообщает участнику, что мут закончился или снят досрочно"""
    try:
        guild = bot.get_guild(guild_id)
        if not guild:
            return False
        user = bot.get_user(user_id) or await bot.fetch_user(user_id)
        if not user:
            return False
        e = build_embed(C.SUCCESS)
        e.set_author(name=f"Наказание снято · {guild.name}",
                     icon_url=guild.icon.url if guild.icon else None)
        e.description = (
            "### 🔊 Мут снят досрочно\n\nМодератор снял наказание раньше срока."
            if early else
            "### 🔊 Срок мута истёк\n\nТы снова можешь писать на сервере."
        )
        e.set_footer(text="Постарайся больше не нарушать правила сервера")
        await user.send(embed=e)
        return True
    except (discord.Forbidden, discord.HTTPException):
        return False
    except Exception as ex:
        print(f"[MUTE_OVER] {ex}")
        return False


_err_reported = {}          # ключ ошибки → время последней отправки

async def report_error(tag: str, error, context: str = ""):
    """
    Дублирует критические ошибки в приватный канал (ERROR_CHANNEL_ID).
    Логи Railway ротируются и теряются — важные сбои нужно видеть сразу.
    Одинаковые ошибки не чаще раза в 10 минут, чтобы не залить канал.
    """
    print(f"[{tag}] {context} {error}")
    ch_id = int(os.getenv("ERROR_CHANNEL_ID", "0") or 0)
    if not ch_id:
        return
    key = f"{tag}:{str(error)[:80]}"
    now = time.time()
    if now - _err_reported.get(key, 0) < 600:
        return
    _err_reported[key] = now
    if len(_err_reported) > 200:
        _err_reported.clear()
    try:
        ch = bot.get_channel(ch_id)
        if not ch:
            return
        e = build_embed(C.DANGER)
        e.set_author(name=f"⚠️ Ошибка · {tag}")
        if context:
            e.add_field(name="Контекст", value=context[:1000], inline=False)
        e.add_field(name="Ошибка",
                    value=f"```{type(error).__name__}: {str(error)[:900]}```",
                    inline=False)
        await ch.send(embed=e)
    except Exception:
        pass


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ОЧЕРЕДЬ ЛОГОВ
#  Массовые события (автоочистка инвайтов, рейд, чистка сообщений)
#  раньше слали по сообщению на каждое действие и упирались в
#  rate limit Discord (429). Теперь события копятся 3 секунды и
#  уходят пачками: в одном сообщении помещается до 10 эмбедов.
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
_log_queue: dict = {}          # channel_id → [(channel, embed), ...]
LOG_FLUSH_INTERVAL = 3         # секунд между отправками
LOG_QUEUE_MAX      = 200       # предохранитель от переполнения памяти


def queue_log(channel, embed):
    """
    Ставит эмбед в очередь на отправку. Для частых событий.
    Срочные вещи (анти-нюк, заявки на бан) шлём напрямую через ch.send.
    """
    if channel is None:
        return
    q = _log_queue.setdefault(channel.id, [])
    if len(q) >= LOG_QUEUE_MAX:
        return                  # очередь переполнена — молча отбрасываем
    q.append((channel, embed))


async def flush_log_queue():
    """Отправляет накопленное. Вызывается циклом и при остановке бота."""
    for ch_id in list(_log_queue.keys()):
        items = _log_queue.get(ch_id) or []
        if not items:
            _log_queue.pop(ch_id, None)
            continue
        channel = items[0][0]
        # Discord принимает до 10 эмбедов в одном сообщении
        batch  = items[:10]
        _log_queue[ch_id] = items[10:]
        try:
            await channel.send(embeds=[e for _, e in batch])
        except discord.HTTPException as ex:
            print(f"[LOG_QUEUE] Не отправлено в {ch_id}: {ex}")
        except Exception as ex:
            print(f"[LOG_QUEUE] {ex}")
        if not _log_queue.get(ch_id):
            _log_queue.pop(ch_id, None)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  AUDIT LOG HELPER
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def _audit_actor(guild, action, target_id, window: int = 12):
    """
    Возвращает (mod_id, reason) из audit log для действия над участником.
    Нужно, чтобы действия через интерфейс Discord тоже попадали в modlog.
    Требует у бота право View Audit Log.
    """
    try:
        now = datetime.datetime.now(datetime.timezone.utc)
        async for entry in guild.audit_logs(limit=6, action=action):
            if entry.target and entry.target.id != target_id:
                continue
            if (now - entry.created_at).total_seconds() > window:
                continue
            if entry.user and entry.user.id == bot.user.id:
                return None, None          # действие самого бота уже залогировано
            return (entry.user.id if entry.user else 0), (entry.reason or "")
    except discord.Forbidden:
        print("[AUDIT] Нет права View Audit Log — действия через Discord не логируются")
    except Exception as ex:
        print(f"[AUDIT] {ex}")
    return 0, ""
