"""
Правила AutoMod Discord вместо проверки текста сообщений самим ботом.

Без Message Content Intent бот получает сообщения с пустым текстом и не может
искать в них фишинг и скам. AutoMod проверяет сообщения на стороне Discord,
интент для этого не нужен: бот только создаёт правила и получает срабатывания.

Правила бот создаёт сам на каждом сервере, где у него есть право Manage Server:
  • «фишинг-ссылки» — поддельные домены Discord и Steam;
  • «подозрительные сообщения» — фейковые раздачи Nitro, крипто-скам;
  • «спам» — встроенный детектор спама Discord;
  • «свои слова» — список слов и доменов, который сервер задаёт сам.
Режим каждого правила (off / alert / block) сервер выбирает в дашборде, по
умолчанию фишинг блокируется, остальное — только алерт. Правило в режиме alert
с Message Content Intent не создаётся: то же самое проверяет security_module.
Алерты уходят в лог-канал сервера и пишутся в security_alerts, как раньше.
"""

import asyncio
import re
import time
import discord
from discord import AutoModRuleAction, AutoModRuleActionType as ActionType
from discord import AutoModRuleEventType, AutoModRuleTriggerType as TriggerType, AutoModTrigger

from .core import bot, intents
from .database import get_log_ch, get_threat_config

RULE_PREFIX = "Witness · "
RULE_PHISHING = RULE_PREFIX + "фишинг-ссылки"
RULE_SUSPICIOUS = RULE_PREFIX + "подозрительные сообщения"
RULE_SPAM = RULE_PREFIX + "спам"
RULE_CUSTOM = RULE_PREFIX + "свои слова"

RESYNC_INTERVAL = 30 * 60   # подхватываем смену лог-канала и новые серверы

# Регулярки AutoMod — диалект Rust: без lookahead/lookbehind, до 260 символов,
# до 10 штук на правило.
PHISHING_REGEX = [
    # steamcommunity.<что угодно, кроме .com>, а также steamcommunity.com.<домен>
    r"(?i)steamcommunity\.(?:[abd-z0-9]|c[a-np-z0-9]|co(?:$|[^a-z])|co[a-ln-z]|com[a-z0-9-]|com\.[a-z])",
    # опечатки в steamcommunity
    r"(?i)\b(?:steamcommun[l1|]ty|stearncommunity|steamcommnuity|steamcomminuty|steamcommunitty"
    r"|steamcomunity|steancommunity|stemcommunity|steamconmunity|steamcornmunity)\.[a-z]{2,}",
    # discord с заменой i на l/1 или o на 0
    r"(?i)\b(?:dl|d1)sc[o0]rd[a-z0-9-]*\.[a-z]{2,}",
    r"(?i)\bdisc0rd[a-z0-9-]*\.[a-z]{2,}",
    # перестановки букв в discord
    r"(?i)\b(?:dicsord|discrod|disocrd|dsicord|discorcl|diiscord|discordd|disscord|dyscord)"
    r"[a-z0-9-]*\.[a-z]{2,}",
    # discordnitro.*, discord-gift.*, discord-airdrop.* и т.п. (настоящие — discord.gift, discord.com)
    r"(?i)\bdiscord-?(?:nitro|gifts?|airdrop|promo|steam|drop)[a-z0-9-]*\.[a-z]{2,}",
    # discord.com.<домен>, discord.gg.<домен>
    r"(?i)\bdiscord(?:app)?\.(?:com|gg|gift)\.[a-z]{2,}",
]

SUSPICIOUS_REGEX = [
    r"(?i)(?:free|бесплатн)[^\n]{0,40}nitro|nitro[^\n]{0,40}(?:free|бесплатн)",
    r"(?i)claim[^\n]{0,30}nitro|nitro[^\n]{0,30}claim",
    r"(?i)free[^\n]{0,30}steam",
    r"(?i)send.{1,20}(?:btc|eth|usdt|crypto).{1,20}(?:back|return|double)",
    r"(?i)(?:double|2x|triple).{1,30}(?:bitcoin|eth|crypto)",
    r"(?i)investment.{1,50}(?:profit|return|guarantee)",
    r"(?i)(?:dm|message).{1,20}(?:profit|earn|make money)",
    # латиница вперемешку с похожими кириллическими/греческими буквами в адресе
    r"(?i)https?://[^\s/]*(?:[a-z][аеорсхуνωκρο]|[аеорсхуνωκρο][a-z])[^\s/]*",
]

BLOCK_MESSAGES = {
    "phishing":   "Witness: ссылка похожа на фишинг и заблокирована.",
    "suspicious": "Witness: сообщение похоже на скам и заблокировано.",
    "spam":       "Witness: сообщение похоже на спам и заблокировано.",
    "custom":     "Witness: сообщение содержит запрещённое на сервере слово.",
}

# ── Свои слова сервера ────────────────────────────────────────
# Главный риск — случайно заблокировать обычные ссылки. Поэтому:
#   1) слово, которое сработало бы на одну из SAFE_SAMPLES, не сохраняется;
#   2) эти же домены идут в allow_list правила — AutoMod их пропускает.
# Синтаксис как в AutoMod: «слово» — целиком, «слово*» — начало,
# «*слово» — конец, «*слово*» — где угодно.
SAFE_DOMAINS = [
    "discord.com", "discord.gg", "discord.gift", "discordapp.com", "discordapp.net",
    "cdn.discordapp.com", "media.discordapp.net", "steamcommunity.com", "steampowered.com",
    "youtube.com", "youtu.be", "twitch.tv", "github.com", "google.com", "wikipedia.org",
    "reddit.com", "x.com", "twitter.com", "tenor.com", "giphy.com", "imgur.com",
    "spotify.com", "t.me", "telegram.org", "vk.com", "albiononline.com", "tiktok.com",
    "instagram.com", "pinterest.com", "medium.com", "gitlab.com", "store.epicgames.com",
]
SAFE_SAMPLES = [f"https://{d}/some/path?x=1" for d in SAFE_DOMAINS] + [
    "https://www.youtube.com/watch?v=dQw4w9WgXcQ", "https://discord.gg/invite",
    "привет всем, как дела?", "hello everyone, good game", "gg wp",
]
MIN_WORD_LEN = 3
MAX_WORD_LEN = 60


def _word_regex(word: str):
    core = word.strip("*")
    body = re.escape(core.lower())
    left = "" if word.startswith("*") else r"(?<![\w])"
    right = "" if word.endswith("*") else r"(?![\w])"
    return re.compile(left + body + right)


def check_custom_words(words):
    """Проверяет список слов. Возвращает (годные слова, [(слово, причина)])."""
    good, bad = [], []
    for raw in words:
        w = str(raw).strip().lower()
        if not w:
            continue
        core = w.strip("*")
        if len(core) < MIN_WORD_LEN:
            bad.append((w, f"короче {MIN_WORD_LEN} символов"))
        elif len(w) > MAX_WORD_LEN:
            bad.append((w, f"длиннее {MAX_WORD_LEN} символов"))
        elif "*" in core:
            bad.append((w, "звёздочка только в начале или в конце"))
        else:
            rx = _word_regex(w)
            hit = next((x for x in SAFE_SAMPLES if rx.search(x.lower())), None)
            if hit:
                bad.append((w, f"заблокирует обычную ссылку или текст: {hit}"))
            else:
                good.append(w)
    return list(dict.fromkeys(good)), bad


# rule_id → (вид алерта, есть ли алерт-действие, блокирует ли)
_our_rules: dict = {}
_no_perms_reported: set = set()


def _desired_rules(log_channel_id, modes):
    """Какие правила должны быть на сервере: имя → (триггер, действия) или None (удалить).
    modes — конфиг из get_threat_config (режимы и custom_words)."""
    alert = [AutoModRuleAction(channel_id=log_channel_id)] if log_channel_id else []
    triggers = {
        "phishing":   (RULE_PHISHING, AutoModTrigger(type=TriggerType.keyword, regex_patterns=PHISHING_REGEX)),
        "suspicious": (RULE_SUSPICIOUS, AutoModTrigger(type=TriggerType.keyword, regex_patterns=SUSPICIOUS_REGEX)),
        "spam":       (RULE_SPAM, AutoModTrigger(type=TriggerType.spam)),
    }
    words = check_custom_words(modes.get("custom_words") or [])[0]
    if words:
        triggers["custom"] = (RULE_CUSTOM, AutoModTrigger(
            type=TriggerType.keyword, keyword_filter=words, allow_list=SAFE_DOMAINS))
    else:
        triggers["custom"] = (RULE_CUSTOM, None)
    desired = {}
    for kind, (name, trigger) in triggers.items():
        mode = modes.get(kind, "off")
        if trigger is None:
            desired[name] = None            # пустой список слов — правило не нужно
        elif mode == "block":
            desired[name] = (trigger, [AutoModRuleAction(custom_message=BLOCK_MESSAGES[kind])] + alert)
        elif mode == "alert" and alert and not (kind in ("suspicious", "spam") and intents.message_content):
            # Без лог-канала алерт-правило бесполезно. Скам и спам с Message
            # Content Intent проверяет сам бот — второе правило дало бы двойные алерты.
            desired[name] = (trigger, alert)
        else:
            desired[name] = None
    return desired


def _actions_key(actions):
    return sorted((a.type.value, a.channel_id or 0, a.custom_message or "") for a in actions)


def _is_same(rule, trigger, actions, exempt_roles, exempt_channels):
    return (rule.enabled
            and rule.trigger.type == trigger.type
            and list(rule.trigger.regex_patterns or []) == list(trigger.regex_patterns or [])
            and list(rule.trigger.keyword_filter or []) == list(trigger.keyword_filter or [])
            and list(rule.trigger.allow_list or []) == list(trigger.allow_list or [])
            and _actions_key(rule.actions) == _actions_key(actions)
            and set(rule.exempt_role_ids) == {o.id for o in exempt_roles}
            and set(rule.exempt_channel_ids) == {o.id for o in exempt_channels})


def _remember(rule):
    kind = {RULE_PHISHING: "phishing", RULE_SUSPICIOUS: "suspicious",
            RULE_SPAM: "spam", RULE_CUSTOM: "custom"}[rule.name]
    has_alert = any(a.type == ActionType.send_alert_message for a in rule.actions)
    blocks = any(a.type == ActionType.block_message for a in rule.actions)
    _our_rules[rule.id] = (kind, has_alert, blocks)


def _exemptions(guild, cfg):
    """Роли и каналы-исключения, которые ещё существуют на сервере."""
    roles = [discord.Object(id=int(r)) for r in cfg.get("exempt_roles") or []
             if str(r).isdigit() and guild.get_role(int(r))][:20]
    chans = [discord.Object(id=int(c)) for c in cfg.get("exempt_channels") or []
             if str(c).isdigit() and guild.get_channel(int(c))][:50]
    return roles, chans


async def sync_guild(guild: discord.Guild):
    """Создаёт, обновляет или удаляет правила Witness на сервере."""
    if not guild.me.guild_permissions.manage_guild:
        if guild.id not in _no_perms_reported:
            _no_perms_reported.add(guild.id)
            print(f"[AUTOMOD] {guild.name}: нет права Manage Server — правила не созданы")
        return
    _no_perms_reported.discard(guild.id)

    log_ch = await get_log_ch(guild)
    try:
        existing = await guild.fetch_automod_rules()
    except discord.HTTPException as ex:
        print(f"[AUTOMOD] {guild.name}: не удалось получить правила: {ex}")
        return
    ours = {r.name: r for r in existing
            if r.creator_id == bot.user.id and r.name.startswith(RULE_PREFIX)}
    # Правило спама на сервере может быть только одно. Если его уже завёл
    # админ или другой бот — сервер и так защищён, своё не создаём.
    foreign_spam = any(r.trigger.type == TriggerType.spam and r.id not in {o.id for o in ours.values()}
                       for r in existing)

    cfg = await get_threat_config(guild.id)
    desired = _desired_rules(log_ch.id if log_ch else None, cfg)
    ex_roles, ex_chans = _exemptions(guild, cfg)
    if foreign_spam:
        desired[RULE_SPAM] = None
    for name, spec in desired.items():
        rule = ours.get(name)
        try:
            if spec is None:
                if rule:
                    await rule.delete(reason="Witness: правило больше не нужно")
                    _our_rules.pop(rule.id, None)
                continue
            trigger, actions = spec
            if rule is None:
                rule = await guild.create_automod_rule(
                    name=name, event_type=AutoModRuleEventType.message_send,
                    trigger=trigger, actions=actions, enabled=True,
                    exempt_roles=ex_roles, exempt_channels=ex_chans,
                    reason="Witness: защита от фишинга, скама и спама")
                print(f"[AUTOMOD] {guild.name}: создано правило «{name}»")
            elif not _is_same(rule, trigger, actions, ex_roles, ex_chans):
                rule = await rule.edit(trigger=trigger, actions=actions, enabled=True,
                                       exempt_roles=ex_roles, exempt_channels=ex_chans,
                                       reason="Witness: обновление правила")
            _remember(rule)
        except discord.HTTPException as ex:
            # Например, правило спама на сервере может быть только одно —
            # если его уже завёл админ или другой бот, своё не создаём
            print(f"[AUTOMOD] {guild.name}: «{name}» — {ex}")


async def sync_all():
    for guild in bot.guilds:
        await sync_guild(guild)
        await asyncio.sleep(1)      # не упираемся в лимиты API


async def _resync_loop():
    while not bot.is_closed():
        await asyncio.sleep(RESYNC_INTERVAL)
        try:
            await sync_all()
        except Exception as ex:
            print(f"[AUTOMOD] resync: {ex}")


@bot.listen("on_ready")
async def _automod_on_ready():
    if getattr(bot, "_automod_started", False):
        return                      # on_ready повторяется при переподключении
    bot._automod_started = True
    await sync_all()
    print(f"✅ AutoMod: правил Witness — {len(_our_rules)}")
    bot.loop.create_task(_resync_loop())


@bot.listen("on_guild_join")
async def _automod_on_guild_join(guild):
    await sync_guild(guild)


_recent: dict = {}   # защита от двойной записи одного срабатывания


@bot.listen("on_automod_action")
async def _automod_on_action(execution: discord.AutoModAction):
    """Пишет срабатывание правил Witness в security_alerts — их видят /q и панель."""
    info = _our_rules.get(execution.rule_id)
    if not info:
        return
    kind, has_alert, blocks = info
    # У правила с блоком и алертом Discord присылает событие на каждое действие
    wanted = ActionType.send_alert_message if has_alert else ActionType.block_message
    if execution.action.type != wanted:
        return
    key = (execution.rule_id, execution.user_id, execution.channel_id)
    now = time.time()
    if now - _recent.get(key, 0) < 2:
        return
    _recent[key] = now
    if len(_recent) > 1000:
        _recent.clear()

    alert_type, severity, text = {
        "phishing":   ("phishing_detected", "HIGH", "AutoMod: фишинг-ссылка"),
        "suspicious": ("phishing_detected", "MEDIUM", "AutoMod: подозрение на фишинг/скам"),
        "spam":       ("duplicate_content", "MEDIUM", "AutoMod: подозрение на спам"),
        "custom":     ("custom_word", "MEDIUM", "AutoMod: запрещённое слово сервера"),
    }[kind]
    text += " — заблокировано" if blocks else " — без блокировки"
    try:
        from security_module import create_alert
        await create_alert(execution.guild_id, alert_type, severity, execution.user_id, text,
                           {"source": "automod", "rule_id": execution.rule_id,
                            "matched": execution.matched_keyword,
                            "channel_id": execution.channel_id})
    except Exception as ex:
        print(f"[AUTOMOD] alert: {ex}")
