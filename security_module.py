"""
Witness — Advanced Security Module
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Префиксные команды: -q <command>
Доступ: только администраторы (administrator permission)

Команды:
  -q scan @user       — полный анализ участника
  -q threat @user     — проверка через threat intelligence
  -q graph @user      — граф связей участника
  -q fp @user         — fingerprint профиль
  -q nlp [текст]      — NLP анализ текста
  -q forensics [id]   — криминалистика сообщения
  -q sig [action_id]  — проверить подпись действия
  -q alert            — последние алерты безопасности
  -q network          — сетевая статистика сервера
  -q whitelist @user  — добавить в whitelist
  -q blacklist @user  — добавить в blacklist
  -q status           — статус всех модулей
  -q help             — список команд

Реализованные системы:
  ✅ Социальная инженерия (impersonation, phishing, unicode spoofing, scam)
  ✅ Криминалистика контента (EXIF, hash, дублирование)
  ✅ Цифровые подписи модераторских действий (HMAC-SHA256)
  ✅ NLP фильтрация (через Groq/Gemini)
  ✅ Граф связей участников
  ✅ Fingerprinting аккаунтов (поведенческий профиль)
  ✅ Базовый threat intelligence (публичные списки)
  ✅ Распределённая защита (shared threat DB между серверами)
  ✅ Статистические аномалии (замена ML)
  ✅ Real-time мониторинг и алерты
"""

import discord
from discord.ext import commands
import aiohttp
import aiosqlite
import asyncio
import hashlib
import hmac
import json
import os
import re
import time
import datetime
import unicodedata
from collections import defaultdict, deque
from typing import Optional

# ── Константы ────────────────────────────────────────────────
PREFIX        = "-q"
DB_PATH       = os.getenv("DB_PATH", "witnessbot.db")
HMAC_SECRET   = os.getenv("HMAC_SECRET", hashlib.sha256(os.urandom(32)).hexdigest())

# Цвета для эмбедов модуля безопасности
class SC:
    CRITICAL = 0xFF0000   # Критическая угроза
    HIGH     = 0xFF6B35   # Высокий риск
    MEDIUM   = 0xFEE75C   # Средний риск
    LOW      = 0x57F287   # Низкий риск / OK
    INFO     = 0x5865F2   # Информация
    NEUTRAL  = 0x36393F   # Нейтральный


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DATABASE — дополнительные таблицы для advanced security
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def sec_db_init():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
            -- Fingerprint профили участников
            CREATE TABLE IF NOT EXISTS fingerprints (
                user_id     INTEGER NOT NULL,
                guild_id    INTEGER NOT NULL,
                first_seen  TEXT,
                last_seen   TEXT,
                msg_count   INTEGER DEFAULT 0,
                avg_msg_len REAL DEFAULT 0,
                active_hours TEXT DEFAULT '{}',
                join_pattern TEXT DEFAULT '{}',
                risk_score  REAL DEFAULT 0,
                PRIMARY KEY (user_id, guild_id)
            );

            -- Граф связей (кто с кем взаимодействует)
            CREATE TABLE IF NOT EXISTS social_graph (
                guild_id    INTEGER NOT NULL,
                user_a      INTEGER NOT NULL,
                user_b      INTEGER NOT NULL,
                interactions INTEGER DEFAULT 0,
                first_seen  TEXT,
                last_seen   TEXT,
                PRIMARY KEY (guild_id, user_a, user_b)
            );

            -- Хеши контента (для дублирования)
            CREATE TABLE IF NOT EXISTS content_hashes (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                content_hash TEXT NOT NULL,
                content_type TEXT DEFAULT 'text',
                created_at  TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_content_hash ON content_hashes(guild_id, content_hash);

            -- Цифровые подписи модераторских действий
            CREATE TABLE IF NOT EXISTS mod_signatures (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    INTEGER NOT NULL,
                mod_id      INTEGER NOT NULL,
                action_type TEXT NOT NULL,
                target_id   INTEGER,
                reason      TEXT,
                signature   TEXT NOT NULL,
                created_at  TEXT NOT NULL
            );

            -- Алерты безопасности
            CREATE TABLE IF NOT EXISTS security_alerts (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    INTEGER NOT NULL,
                alert_type  TEXT NOT NULL,
                severity    TEXT NOT NULL,
                user_id     INTEGER,
                description TEXT,
                metadata    TEXT DEFAULT '{}',
                resolved    INTEGER DEFAULT 0,
                created_at  TEXT NOT NULL
            );

            -- Whitelist / Blacklist
            CREATE TABLE IF NOT EXISTS sec_lists (
                guild_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                list_type   TEXT NOT NULL,
                reason      TEXT,
                added_by    INTEGER,
                created_at  TEXT NOT NULL,
                PRIMARY KEY (guild_id, user_id, list_type)
            );

            -- Распределённая база угроз (shared между серверами)
            CREATE TABLE IF NOT EXISTS global_threats (
                user_id     INTEGER PRIMARY KEY,
                threat_level TEXT DEFAULT 'low',
                reports     INTEGER DEFAULT 0,
                guilds_reported TEXT DEFAULT '[]',
                reason      TEXT,
                first_seen  TEXT,
                last_updated TEXT
            );

            -- Поведенческие аномалии
            CREATE TABLE IF NOT EXISTS behavior_anomalies (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                anomaly_type TEXT NOT NULL,
                score       REAL DEFAULT 0,
                details     TEXT DEFAULT '{}',
                created_at  TEXT NOT NULL
            );

            -- NLP результаты
            CREATE TABLE IF NOT EXISTS nlp_results (
                id          INTEGER PRIMARY KEY AUTOINCREMENT,
                guild_id    INTEGER NOT NULL,
                user_id     INTEGER NOT NULL,
                message_id  INTEGER,
                toxicity    REAL DEFAULT 0,
                threat      REAL DEFAULT 0,
                spam        REAL DEFAULT 0,
                result      TEXT DEFAULT '{}',
                created_at  TEXT NOT NULL
            );
        """)
        await db.commit()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  IN-MEMORY КЭШИ (оптимизация — не бьём БД на каждое событие)
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Кэш fingerprint профилей: {(guild_id, user_id): {...}}
_fp_cache: dict = {}
# Кэш хешей контента: {guild_id: deque([(hash, user_id, ts), ...])}
_content_cache: dict = defaultdict(lambda: deque(maxlen=1000))
# Кэш поведения: {(guild_id, user_id): deque([timestamps])}
_behavior_cache: dict = defaultdict(lambda: deque(maxlen=100))
# Кэш interaction graph: {(guild_id, user_a, user_b): count}
_graph_cache: dict = defaultdict(int)
# Алерты в памяти: {guild_id: deque([alert, ...])}
_alerts_cache: dict = defaultdict(lambda: deque(maxlen=50))
# Whitelist/Blacklist кэш: {guild_id: {user_id: 'white'|'black'}}
_lists_cache: dict = defaultdict(dict)


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ЦИФРОВЫЕ ПОДПИСИ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def sign_action(mod_id: int, action_type: str, target_id: int, reason: str, timestamp: str) -> str:
    """Создаёт HMAC-SHA256 подпись для модераторского действия"""
    payload = f"{mod_id}:{action_type}:{target_id}:{reason}:{timestamp}"
    sig = hmac.new(HMAC_SECRET.encode(), payload.encode(), hashlib.sha256).hexdigest()
    return sig


def verify_signature(mod_id: int, action_type: str, target_id: int,
                     reason: str, timestamp: str, signature: str) -> bool:
    """Верифицирует подпись модераторского действия"""
    expected = sign_action(mod_id, action_type, target_id, reason, timestamp)
    return hmac.compare_digest(expected, signature)


async def log_signed_action(guild_id: int, mod_id: int, action_type: str,
                             target_id: int, reason: str) -> int:
    """Логирует подписанное действие в БД, возвращает action_id"""
    ts = datetime.datetime.utcnow().isoformat()
    sig = sign_action(mod_id, action_type, target_id, reason, ts)
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "INSERT INTO mod_signatures (guild_id,mod_id,action_type,target_id,reason,signature,created_at) VALUES (?,?,?,?,?,?,?)",
            (guild_id, mod_id, action_type, target_id, reason, sig, ts)
        )
        await db.commit()
        return cursor.lastrowid


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  СОЦИАЛЬНАЯ ИНЖЕНЕРИЯ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Список подозрительных фишинговых паттернов
PHISHING_PATTERNS = [
    r"discord\.gift\b(?!\.com)",
    r"discordnitro\.(?!com)",
    r"steamcommunity\.(?!com)",
    r"free.*nitro",
    r"nitro.*free",
    r"claim.*nitro",
    r"nitro.*claim",
    r"@everyone.*http",
    r"free.*steam",
    r"discord\.com\.(?!)",
]

SCAM_PATTERNS = [
    r"send.{1,20}(?:btc|eth|usdt|crypto).{1,20}(?:back|return|double)",
    r"(?:double|2x|triple).{1,30}(?:bitcoin|eth|crypto)",
    r"investment.{1,50}(?:profit|return|guarantee)",
    r"(?:dm|message).{1,20}(?:profit|earn|make money)",
]

# Unicode символы которые выглядят как ASCII
CONFUSABLES = {
    'а': 'a', 'е': 'e', 'о': 'o', 'р': 'p', 'с': 'c',
    'х': 'x', 'у': 'y', 'В': 'B', 'М': 'M', 'Т': 'T',
    'ν': 'v', 'ω': 'w', 'κ': 'k', 'ρ': 'p', 'ο': 'o',
}


def normalize_text(text: str) -> str:
    """Нормализует unicode confusables к ASCII"""
    result = ""
    for char in text:
        result += CONFUSABLES.get(char, char)
    return unicodedata.normalize('NFKC', result)


def check_impersonation(member: discord.Member, guild: discord.Guild) -> list[dict]:
    """Проверяет не выдаёт ли участник себя за кого-то другого"""
    threats = []
    member_norm = normalize_text(member.display_name.lower())

    for other in guild.members:
        if other.id == member.id:
            continue
        # Проверяем только администраторов и модераторов
        if not (other.guild_permissions.administrator or other.guild_permissions.manage_messages):
            continue
        other_norm = normalize_text(other.display_name.lower())
        # Levenshtein distance упрощённый (через общие символы)
        similarity = _similarity(member_norm, other_norm)
        if similarity > 0.85 and member.id != other.id:
            threats.append({
                "type": "impersonation",
                "target": other,
                "similarity": round(similarity * 100),
                "severity": "HIGH"
            })

    # Проверяем имитацию ботов
    bot_keywords = ["bot", "бот", "assistant", "system", "admin", "mod", "nexus"]
    for kw in bot_keywords:
        if kw in member_norm and not member.bot:
            threats.append({
                "type": "bot_impersonation",
                "keyword": kw,
                "severity": "MEDIUM"
            })
    return threats


def _similarity(a: str, b: str) -> float:
    """Простая мера сходства двух строк (Jaccard на биграммах)"""
    if not a or not b: return 0.0
    def bigrams(s): return set(s[i:i+2] for i in range(len(s)-1))
    bg_a, bg_b = bigrams(a), bigrams(b)
    if not bg_a or not bg_b: return 1.0 if a == b else 0.0
    return len(bg_a & bg_b) / len(bg_a | bg_b)


def check_phishing(text: str) -> list[dict]:
    """Проверяет текст на фишинг/скам паттерны"""
    findings = []
    text_lower = text.lower()
    for pattern in PHISHING_PATTERNS:
        if re.search(pattern, text_lower):
            findings.append({"type": "phishing", "pattern": pattern, "severity": "HIGH"})
    for pattern in SCAM_PATTERNS:
        if re.search(pattern, text_lower):
            findings.append({"type": "scam", "pattern": pattern, "severity": "HIGH"})
    # Проверка unicode spoofing в URL
    urls = re.findall(r'https?://[^\s]+', text)
    for url in urls:
        norm_url = normalize_text(url)
        if norm_url != url:
            findings.append({"type": "unicode_spoofing", "url": url, "severity": "CRITICAL"})
    return findings


def check_unicode_spoofing(name: str) -> bool:
    """Проверяет содержит ли строка unicode spoofing"""
    return normalize_text(name) != name


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  КРИМИНАЛИСТИКА КОНТЕНТА
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

def hash_content(text: str) -> str:
    """SHA-256 хеш текстового контента"""
    return hashlib.sha256(text.strip().lower().encode()).hexdigest()


def hash_image(data: bytes) -> str:
    """SHA-256 хеш бинарных данных (изображение)"""
    return hashlib.sha256(data).hexdigest()


async def check_duplicate_content(guild_id: int, user_id: int,
                                   content: str, content_type: str = "text") -> dict:
    """Проверяет является ли контент дубликатом недавних сообщений"""
    h = hash_content(content)
    now = time.time()

    # Проверяем in-memory кэш (последние 1000 сообщений)
    cache = _content_cache[guild_id]
    duplicates = [(entry_hash, entry_uid, entry_ts)
                  for entry_hash, entry_uid, entry_ts in cache
                  if entry_hash == h and entry_uid != user_id and now - entry_ts < 3600]

    # Добавляем в кэш
    cache.append((h, user_id, now))

    if duplicates:
        # Сохраняем в БД для истории
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute(
                "INSERT INTO content_hashes (guild_id,user_id,content_hash,content_type,created_at) VALUES (?,?,?,?,?)",
                (guild_id, user_id, h, content_type, datetime.datetime.utcnow().isoformat())
            )
            await db.commit()
        return {
            "is_duplicate": True,
            "original_user": duplicates[0][1],
            "count": len(duplicates),
            "severity": "HIGH" if len(duplicates) > 3 else "MEDIUM"
        }
    return {"is_duplicate": False}


async def analyze_image_metadata(image_url: str) -> dict:
    """Анализирует метаданные изображения (EXIF и базовые свойства)"""
    result = {"url": image_url, "findings": []}
    try:
        async with aiohttp.ClientSession() as s:
            async with s.get(image_url, timeout=aiohttp.ClientTimeout(total=10)) as r:
                if r.status != 200:
                    return result
                data = await r.read()
                content_type = r.headers.get("content-type", "")

        result["size_bytes"] = len(data)
        result["hash"] = hash_image(data)
        result["content_type"] = content_type

        # Пытаемся читать EXIF через Pillow если доступен
        try:
            from PIL import Image, ExifTags
            import io
            img = Image.open(io.BytesIO(data))
            result["dimensions"] = f"{img.width}x{img.height}"
            result["format"] = img.format

            exif_data = img._getexif() if hasattr(img, '_getexif') else None
            if exif_data:
                for tag_id, value in exif_data.items():
                    tag = ExifTags.TAGS.get(tag_id, tag_id)
                    if tag in ("GPSInfo", "DateTime", "Make", "Model", "Software"):
                        result["findings"].append({"tag": str(tag), "value": str(value)[:100]})
                if "GPSInfo" in {ExifTags.TAGS.get(t) for t in exif_data}:
                    result["findings"].append({
                        "tag": "WARNING",
                        "value": "Изображение содержит GPS координаты!"
                    })
        except ImportError:
            result["findings"].append({"tag": "INFO", "value": "Pillow не установлен — EXIF недоступен"})
        except Exception:
            pass

    except Exception as ex:
        result["error"] = str(ex)
    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  NLP АНАЛИЗ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def nlp_analyze(text: str, groq_key: str = "", gemini_key: str = "") -> dict:
    """Анализирует текст через AI на токсичность, угрозы, спам.
    Groq (Llama 3.3 70B) → Gemini 2.0 Flash → Rule-based fallback
    """
    result = {"toxicity": 0.0, "threat": 0.0, "spam": 0.0, "summary": "", "source": ""}

    prompt = (
        "Analyze this text for toxicity, threats, and spam. "
        "Respond ONLY with valid JSON, no markdown, no explanation:\n"
        "{\"toxicity\": 0.0-1.0, \"threat\": 0.0-1.0, \"spam\": 0.0-1.0, \"summary\": \"brief reason in Russian\"}\n\n"
        f"Text: {text[:500]}"
    )

    # 1. Groq — Llama 3.3 70B (бесплатно, быстро)
    if groq_key:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={"Authorization": f"Bearer {groq_key}", "Content-Type": "application/json"},
                    json={
                        "model": "llama-3.3-70b-versatile",
                        "messages": [{"role": "user", "content": prompt}],
                        "max_tokens": 150,
                        "temperature": 0.1,
                    },
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        raw = data["choices"][0]["message"]["content"].strip()
                        raw = raw.replace("```json", "").replace("```", "").strip()
                        parsed = json.loads(raw)
                        result.update(parsed)
                        result["source"] = "Groq (Llama 3.3 70B)"
                        return result
        except Exception as ex:
            print(f"[NLP] Groq error: {ex}")

    # 2. Gemini — fallback (бесплатно, 1500 req/day)
    if gemini_key:
        try:
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={gemini_key}",
                    json={"contents": [{"parts": [{"text": prompt}]}]},
                    timeout=aiohttp.ClientTimeout(total=15)
                ) as r:
                    if r.status == 200:
                        data = await r.json()
                        raw = data["candidates"][0]["content"]["parts"][0]["text"].strip()
                        raw = raw.replace("```json", "").replace("```", "").strip()
                        parsed = json.loads(raw)
                        result.update(parsed)
                        result["source"] = "Gemini 2.0 Flash"
                        return result
        except Exception as ex:
            print(f"[NLP] Gemini error: {ex}")

    # 3. Rule-based fallback — если нет AI ключей
    text_lower = text.lower()
    toxic_words = [
        "убью", "умри", "сдохни", "убить", "kill", "die", "kys", "hate",
        "угрожаю", "расправлюсь", "уничтожу"
    ]
    scam_words = ["free nitro", "claim", "crypto", "bitcoin", "инвестиция", "заработок"]
    spam_signals = (
        len(re.findall(r"https?://", text)) > 2 or
        (len(text) > 100 and len(set(text.lower())) < 15) or
        text.count("@") > 5
    )
    threat_hit = any(w in text_lower for w in toxic_words)
    scam_hit   = any(w in text_lower for w in scam_words)

    result["toxicity"] = 0.85 if threat_hit else 0.1
    result["threat"]   = 0.90 if threat_hit else 0.0
    result["spam"]     = 0.85 if (spam_signals or scam_hit) else 0.1
    result["summary"]  = (
        "Обнаружены угрозы/токсичность" if threat_hit else
        "Обнаружен спам/скам" if spam_signals or scam_hit else
        "Контент выглядит безопасным"
    )
    result["source"] = "Rule-based (добавь GROQ_API_KEY для AI анализа)"
    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  FINGERPRINTING
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def update_fingerprint(guild_id: int, user_id: int, message: discord.Message):
    """Обновляет поведенческий профиль участника"""
    key = (guild_id, user_id)
    now = datetime.datetime.utcnow()
    hour = now.hour

    if key not in _fp_cache:
        _fp_cache[key] = {
            "msg_count": 0,
            "total_len": 0,
            "active_hours": defaultdict(int),
            "first_seen": now.isoformat(),
            "last_seen": now.isoformat(),
            "mention_count": 0,
            "link_count": 0,
            "emoji_count": 0,
            "caps_ratio_sum": 0,
            "unique_words": set(),
        }

    fp = _fp_cache[key]
    content = message.content or ""

    fp["msg_count"] += 1
    fp["total_len"] += len(content)
    fp["active_hours"][hour] += 1
    fp["last_seen"] = now.isoformat()
    fp["mention_count"] += len(message.mentions)
    fp["link_count"] += len(re.findall(r'https?://', content))
    fp["emoji_count"] += len(re.findall(r'<:[^:]+:\d+>', content))

    words = content.lower().split()
    fp["unique_words"].update(words[:50])

    if content and content.upper() == content and len(content) > 5:
        fp["caps_ratio_sum"] += 1

    # Периодически сохраняем в БД (каждые 50 сообщений)
    if fp["msg_count"] % 50 == 0:
        await _save_fingerprint(guild_id, user_id, fp)


async def _save_fingerprint(guild_id: int, user_id: int, fp: dict):
    """Сохраняет fingerprint в БД"""
    avg_len = fp["total_len"] / fp["msg_count"] if fp["msg_count"] else 0
    risk = _calculate_risk_score(fp)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            INSERT INTO fingerprints (user_id,guild_id,first_seen,last_seen,msg_count,avg_msg_len,active_hours,risk_score)
            VALUES (?,?,?,?,?,?,?,?)
            ON CONFLICT(user_id,guild_id) DO UPDATE SET
                last_seen=excluded.last_seen, msg_count=excluded.msg_count,
                avg_msg_len=excluded.avg_msg_len, active_hours=excluded.active_hours,
                risk_score=excluded.risk_score
        """, (user_id, guild_id, fp["first_seen"], fp["last_seen"],
              fp["msg_count"], round(avg_len, 1),
              json.dumps(dict(fp["active_hours"])), round(risk, 2)))
        await db.commit()


def _calculate_risk_score(fp: dict) -> float:
    """Рассчитывает risk score 0-100 на основе поведения"""
    score = 0.0

    if fp["msg_count"] > 0:
        # Высокий процент упоминаний — подозрительно
        mention_ratio = fp["mention_count"] / fp["msg_count"]
        if mention_ratio > 0.5: score += 20
        elif mention_ratio > 0.2: score += 10

        # Много ссылок
        link_ratio = fp["link_count"] / fp["msg_count"]
        if link_ratio > 0.5: score += 25
        elif link_ratio > 0.2: score += 10

        # Много caps
        caps_ratio = fp["caps_ratio_sum"] / fp["msg_count"]
        if caps_ratio > 0.3: score += 15

        # Очень короткие сообщения (спам-паттерн)
        avg_len = fp["total_len"] / fp["msg_count"]
        if avg_len < 5: score += 10

        # Активность только в ночное время (UTC 0-6)
        night_hours = sum(fp["active_hours"].get(str(h), 0) for h in range(0, 6))
        total_hours = sum(fp["active_hours"].values()) or 1
        if night_hours / total_hours > 0.8: score += 15

        # Бедный словарный запас
        vocab_size = len(fp.get("unique_words", set()))
        if fp["msg_count"] > 20 and vocab_size < 10: score += 15

    return min(score, 100.0)


async def get_fingerprint_report(guild_id: int, user_id: int) -> dict:
    """Возвращает полный fingerprint отчёт для участника"""
    key = (guild_id, user_id)
    fp = _fp_cache.get(key, {})

    # Также проверяем БД
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT * FROM fingerprints WHERE guild_id=? AND user_id=?",
            (guild_id, user_id)
        ) as c:
            row = await c.fetchone()

    if not fp and not row:
        return {"error": "Нет данных для этого участника. Нужно время для накопления профиля."}

    result = {}
    if row:
        cols = ["user_id", "guild_id", "first_seen", "last_seen", "msg_count",
                "avg_msg_len", "active_hours", "join_pattern", "risk_score"]
        result = dict(zip(cols, row))
        try:
            result["active_hours"] = json.loads(result.get("active_hours", "{}"))
        except Exception:
            result["active_hours"] = {}

    if fp:
        result["risk_score"] = round(_calculate_risk_score(fp), 1)
        result["msg_count"] = fp.get("msg_count", 0)
        result["mention_ratio"] = round(fp["mention_count"] / max(fp["msg_count"], 1), 2)
        result["link_ratio"] = round(fp["link_count"] / max(fp["msg_count"], 1), 2)
        result["vocab_size"] = len(fp.get("unique_words", set()))

    return result


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ГРАФ СВЯЗЕЙ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def update_social_graph(guild_id: int, user_id: int, message: discord.Message):
    """Обновляет граф социальных связей на основе упоминаний"""
    if not message.mentions:
        return
    now = datetime.datetime.utcnow().isoformat()
    for mentioned in message.mentions:
        if mentioned.id == user_id or mentioned.bot:
            continue
        key = (guild_id, min(user_id, mentioned.id), max(user_id, mentioned.id))
        _graph_cache[key] += 1

        # Сохраняем в БД каждые 10 взаимодействий
        if _graph_cache[key] % 10 == 0:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute("""
                    INSERT INTO social_graph (guild_id,user_a,user_b,interactions,first_seen,last_seen)
                    VALUES (?,?,?,?,?,?)
                    ON CONFLICT(guild_id,user_a,user_b) DO UPDATE SET
                        interactions=excluded.interactions, last_seen=excluded.last_seen
                """, (guild_id, min(user_id, mentioned.id), max(user_id, mentioned.id),
                      _graph_cache[key], now, now))
                await db.commit()


async def get_social_graph(guild_id: int, user_id: int, depth: int = 1) -> dict:
    """Возвращает граф связей участника"""
    connections = []

    # In-memory кэш
    for (g, a, b), count in _graph_cache.items():
        if g != guild_id: continue
        if a == user_id: connections.append({"user_id": b, "interactions": count})
        elif b == user_id: connections.append({"user_id": a, "interactions": count})

    # БД
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT user_a, user_b, interactions FROM social_graph WHERE guild_id=? AND (user_a=? OR user_b=?) ORDER BY interactions DESC LIMIT 20",
            (guild_id, user_id, user_id)
        ) as c:
            rows = await c.fetchall()

    for a, b, count in rows:
        other = b if a == user_id else a
        if not any(c["user_id"] == other for c in connections):
            connections.append({"user_id": other, "interactions": count})

    connections.sort(key=lambda x: x["interactions"], reverse=True)
    return {"user_id": user_id, "connections": connections[:15]}


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  THREAT INTELLIGENCE
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# Известные рейд-боты и подозрительные паттерны (публичные данные)
KNOWN_RAID_PATTERNS = {
    "username_patterns": [
        r"^[a-z]{8,12}\d{4}$",          # Рандомные буквы + 4 цифры
        r"^user\d{6,}$",                  # user + много цифр
        r"^[A-Z][a-z]+\d{4}$",           # Capitalized + 4 цифры (фабричные аккаунты)
    ],
    "avatar_none": True,                  # Без аватарки
}


async def check_threat_intelligence(user: discord.Member) -> dict:
    """Комплексная проверка участника через доступные threat sources"""
    threats = []
    risk_score = 0

    # 1. Проверка в локальной глобальной БД угроз
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT threat_level, reports, reason FROM global_threats WHERE user_id=?",
            (user.id,)
        ) as c:
            row = await c.fetchone()
    if row:
        level, reports, reason = row
        threats.append({
            "source": "Global Threat DB",
            "level": level,
            "reports": reports,
            "reason": reason
        })
        risk_score += {"low": 10, "medium": 30, "high": 60, "critical": 90}.get(level, 0)

    # 2. Анализ username паттернов
    for pattern in KNOWN_RAID_PATTERNS["username_patterns"]:
        if re.match(pattern, user.name):
            threats.append({
                "source": "Pattern Analysis",
                "level": "medium",
                "reason": f"Username соответствует паттерну рейд-ботов: `{pattern}`"
            })
            risk_score += 20
            break

    # 3. Возраст аккаунта
    age_days = (datetime.datetime.utcnow() - user.created_at.replace(tzinfo=None)).days
    if age_days < 1:
        threats.append({"source": "Account Age", "level": "critical", "reason": "Аккаунт создан сегодня"})
        risk_score += 40
    elif age_days < 7:
        threats.append({"source": "Account Age", "level": "high", "reason": f"Аккаунт создан {age_days} дней назад"})
        risk_score += 25
    elif age_days < 30:
        threats.append({"source": "Account Age", "level": "medium", "reason": f"Новый аккаунт ({age_days} дней)"})
        risk_score += 10

    # 4. Без аватарки + новый аккаунт — паттерн бота
    if user.avatar is None and age_days < 30:
        threats.append({"source": "Profile", "level": "medium", "reason": "Нет аватарки у нового аккаунта"})
        risk_score += 15

    # 5. Impersonation check
    guild = user.guild
    imp_threats = check_impersonation(user, guild)
    for t in imp_threats:
        threats.append({
            "source": "Impersonation Check",
            "level": t["severity"].lower(),
            "reason": f"Похож на {t.get('target', {}).display_name if hasattr(t.get('target', {}), 'display_name') else '?'} ({t.get('similarity', 0)}%)"
        })
        risk_score += 30

    # 6. Unicode spoofing в имени
    if check_unicode_spoofing(user.display_name):
        threats.append({
            "source": "Unicode Analysis",
            "level": "high",
            "reason": "Имя содержит unicode символы маскирующиеся под ASCII"
        })
        risk_score += 35

    return {
        "user_id": user.id,
        "risk_score": min(risk_score, 100),
        "threats": threats,
        "threat_level": (
            "CRITICAL" if risk_score >= 70 else
            "HIGH"     if risk_score >= 40 else
            "MEDIUM"   if risk_score >= 20 else
            "LOW"
        )
    }


async def report_to_global_db(user_id: int, guild_id: int, reason: str, threat_level: str = "medium"):
    """Добавляет участника в глобальную базу угроз"""
    now = datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT reports, guilds_reported FROM global_threats WHERE user_id=?", (user_id,)) as c:
            row = await c.fetchone()
        if row:
            reports, guilds_json = row
            guilds = json.loads(guilds_json)
            if guild_id not in guilds:
                guilds.append(guild_id)
            await db.execute(
                "UPDATE global_threats SET reports=?, guilds_reported=?, last_updated=?, threat_level=? WHERE user_id=?",
                (reports + 1, json.dumps(guilds), now, threat_level, user_id)
            )
        else:
            await db.execute(
                "INSERT INTO global_threats (user_id,threat_level,reports,guilds_reported,reason,first_seen,last_updated) VALUES (?,?,?,?,?,?,?)",
                (user_id, threat_level, 1, json.dumps([guild_id]), reason, now, now)
            )
        await db.commit()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  АЛЕРТЫ
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def create_alert(guild_id: int, alert_type: str, severity: str,
                       user_id: int = 0, description: str = "", metadata: dict = None):
    """Создаёт алерт безопасности"""
    now = datetime.datetime.utcnow().isoformat()
    alert = {
        "type": alert_type, "severity": severity,
        "user_id": user_id, "description": description,
        "metadata": metadata or {}, "created_at": now
    }
    _alerts_cache[guild_id].append(alert)

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO security_alerts (guild_id,alert_type,severity,user_id,description,metadata,created_at) VALUES (?,?,?,?,?,?,?)",
            (guild_id, alert_type, severity, user_id, description, json.dumps(metadata or {}), now)
        )
        await db.commit()


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  COG — основной класс модуля
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class AdvancedSecurityCog(commands.Cog, name="AdvancedSecurity"):

    def __init__(self, bot: commands.Bot):
        self.bot = bot
        self.groq_key = os.getenv("GROQ_API_KEY", "")
        self.gemini_key = os.getenv("GEMINI_API_KEY", "")

    # ── Проверка доступа ──────────────────────────────────────
    def _is_admin(self, ctx: commands.Context) -> bool:
        return ctx.author.guild_permissions.administrator

    async def _check_admin(self, ctx: commands.Context) -> bool:
        if not self._is_admin(ctx):
            await ctx.send(embed=discord.Embed(
                title="🔒 Нет доступа",
                description="Команды `-q` доступны только администраторам.",
                color=SC.CRITICAL
            ), delete_after=5)
            return False
        return True

    def _make_embed(self, title: str, description: str = "", color: int = SC.INFO) -> discord.Embed:
        e = discord.Embed(title=title, description=description, color=color,
                          timestamp=datetime.datetime.utcnow())
        e.set_footer(text="Witness Security · -q help для списка команд")
        return e

    def _risk_color(self, score: float) -> int:
        if score >= 70: return SC.CRITICAL
        if score >= 40: return SC.HIGH
        if score >= 20: return SC.MEDIUM
        return SC.LOW

    def _risk_emoji(self, level: str) -> str:
        return {"CRITICAL": "🔴", "HIGH": "🟠", "MEDIUM": "🟡", "LOW": "🟢"}.get(level.upper(), "⚪")

    # ── Слушатели событий ─────────────────────────────────────

    @commands.Cog.listener()
    async def on_message(self, message: discord.Message):
        if message.author.bot or not message.guild: return

        # Пропускаем команды — их обрабатывает main.py
        if message.content.startswith(("-q", "-", "!")):
            return

        gid, uid = message.guild.id, message.author.id

        # Проверяем whitelist
        if _lists_cache[gid].get(uid) == "white": return

        # Обновляем fingerprint и граф
        await update_fingerprint(gid, uid, message)
        await update_social_graph(gid, uid, message)

        # Проверка дублирования контента
        if message.content:
            dup = await check_duplicate_content(gid, uid, message.content)
            if dup["is_duplicate"]:
                await create_alert(gid, "duplicate_content", dup["severity"],
                                   uid, f"Дублирует контент {dup['count']} раз за час")

        # Проверка фишинга и скама
        if message.content:
            findings = check_phishing(message.content)
            if findings:
                await create_alert(gid, "phishing_detected", "HIGH", uid,
                                   f"Обнаружен фишинг/скам: {findings[0]['type']}", {"findings": findings})
                # Пробуем отправить алерт в лог-канал
                from main import get_log_ch  # import из основного файла
                ch = await get_log_ch(message.guild)
                if ch:
                    e = self._make_embed("🎣 Обнаружен фишинг/скам!", color=SC.CRITICAL)
                    e.add_field(name="Автор", value=message.author.mention, inline=True)
                    e.add_field(name="Канал", value=message.channel.mention, inline=True)
                    e.add_field(name="Тип", value=findings[0]["type"], inline=True)
                    e.add_field(name="Сообщение", value=message.content[:500], inline=False)
                    await ch.send(embed=e)

        # Анализ изображений
        for attachment in message.attachments:
            if any(attachment.filename.lower().endswith(ext) for ext in [".jpg", ".jpeg", ".png", ".gif", ".webp"]):
                meta = await analyze_image_metadata(attachment.url)
                if meta.get("findings"):
                    exif_warns = [f for f in meta["findings"] if "GPS" in f.get("tag", "")]
                    if exif_warns:
                        await create_alert(gid, "image_gps_metadata", "MEDIUM", uid,
                                           "Изображение содержит GPS координаты в EXIF")

    @commands.Cog.listener()
    async def on_member_join(self, member: discord.Member):
        if member.bot: return
        gid = member.guild.id

        # Threat intelligence при входе
        threat = await check_threat_intelligence(member)
        if threat["risk_score"] >= 40:
            await create_alert(gid, "high_risk_join", threat["threat_level"],
                               member.id, f"Risk score: {threat['risk_score']}/100",
                               {"threats": threat["threats"]})

            try:
                from main import get_log_ch
                ch = await get_log_ch(member.guild)
                if ch:
                    color = self._risk_color(threat["risk_score"])
                    e = self._make_embed(
                        f"{self._risk_emoji(threat['threat_level'])} Подозрительный вход",
                        color=color
                    )
                    e.set_thumbnail(url=member.display_avatar.url)
                    e.add_field(name="Участник", value=f"{member.mention} (`{member.name}`)", inline=False)
                    e.add_field(name="Risk Score", value=f"**{threat['risk_score']}/100**", inline=True)
                    e.add_field(name="Уровень", value=threat["threat_level"], inline=True)
                    for t in threat["threats"][:3]:
                        e.add_field(
                            name=f"{self._risk_emoji(t['level'])} {t['source']}",
                            value=t["reason"],
                            inline=False
                        )
                    await ch.send(embed=e)
            except Exception:
                pass

    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
    #  PREFIX КОМАНДЫ -q
    # ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

    @commands.command(name="q")
    async def q_dispatch(self, ctx: commands.Context, subcmd: str = "help", *args):
        """Диспетчер -q команд"""
        if not await self._check_admin(ctx): return

        handlers = {
            "scan":        self._cmd_scan,
            "threat":      self._cmd_threat,
            "graph":       self._cmd_graph,
            "fp":          self._cmd_fp,
            "nlp":         self._cmd_nlp,
            "forensics":   self._cmd_forensics,
            "sig":         self._cmd_sig,
            "alert":       self._cmd_alert,
            "network":     self._cmd_network,
            "whitelist":   self._cmd_whitelist,
            "blacklist":   self._cmd_blacklist,
            "status":      self._cmd_status,
            "report":      self._cmd_report,
            "help":        self._cmd_help,
        }

        handler = handlers.get(subcmd.lower())
        if handler:
            await handler(ctx, *args)
        else:
            await ctx.send(f"❌ Неизвестная команда `{subcmd}`. Используй `-q help`", delete_after=5)

    # ── -q help ───────────────────────────────────────────────
    async def _cmd_help(self, ctx, *args):
        e = self._make_embed("🔐 Witness Advanced Security", color=SC.INFO)
        cmds = [
            ("`-q scan @user`",      "Полный анализ участника (все проверки)"),
            ("`-q threat @user`",    "Threat Intelligence проверка"),
            ("`-q graph @user`",     "Граф социальных связей"),
            ("`-q fp @user`",        "Fingerprint поведенческий профиль"),
            ("`-q nlp [текст]`",     "NLP анализ текста (токсичность/угрозы/спам)"),
            ("`-q forensics [id]`",  "Криминалистика сообщения (EXIF, хеш, дубли)"),
            ("`-q sig [id]`",        "Проверить подпись модераторского действия"),
            ("`-q alert`",           "Последние алерты безопасности"),
            ("`-q network`",         "Сетевая статистика и аномалии сервера"),
            ("`-q whitelist @user`", "Добавить в whitelist"),
            ("`-q blacklist @user`", "Добавить в blacklist"),
            ("`-q report @user`",    "Отправить в глобальную базу угроз"),
            ("`-q status`",          "Статус всех систем безопасности"),
        ]
        for cmd, desc in cmds:
            e.add_field(name=cmd, value=desc, inline=False)
        e.set_footer(text="Только для администраторов · Witness Security")
        await ctx.send(embed=e)

    # ── -q scan @user ─────────────────────────────────────────
    async def _cmd_scan(self, ctx, *args):
        member = await self._resolve_member(ctx, args)
        if not member: return

        await ctx.send(f"🔍 Полное сканирование `{member.display_name}`...", delete_after=3)

        # Запускаем все проверки параллельно
        threat_task = asyncio.create_task(check_threat_intelligence(member))
        fp_task     = asyncio.create_task(get_fingerprint_report(ctx.guild.id, member.id))
        graph_task  = asyncio.create_task(get_social_graph(ctx.guild.id, member.id))

        threat, fp, graph = await asyncio.gather(threat_task, fp_task, graph_task)

        color = self._risk_color(threat["risk_score"])
        e = self._make_embed(
            f"🔐 Полное сканирование: {member.display_name}",
            color=color
        )
        e.set_thumbnail(url=member.display_avatar.url)

        # Основная информация
        age_days = (datetime.datetime.utcnow() - member.created_at.replace(tzinfo=None)).days
        join_days = (datetime.datetime.utcnow() - member.joined_at.replace(tzinfo=None)).days if member.joined_at else "?"
        e.add_field(name="👤 Аккаунт", value=(
            f"Возраст: **{age_days} дней**\n"
            f"На сервере: **{join_days} дней**\n"
            f"Роли: {len(member.roles) - 1}"
        ), inline=True)

        # Risk score с визуальным баром
        score = threat["risk_score"]
        bar = "█" * int(score / 10) + "░" * (10 - int(score / 10))
        e.add_field(name="⚠️ Risk Score", value=(
            f"`{bar}` **{score}/100**\n"
            f"Уровень: **{self._risk_emoji(threat['threat_level'])} {threat['threat_level']}**"
        ), inline=True)

        # Fingerprint
        if "error" not in fp:
            e.add_field(name="🧬 Fingerprint", value=(
                f"Сообщений: **{fp.get('msg_count', 0):,}**\n"
                f"Ср. длина: **{fp.get('avg_msg_len', 0):.0f} симв.**\n"
                f"Risk FP: **{fp.get('risk_score', 0):.0f}/100**"
            ), inline=True)

        # Граф
        conn_count = len(graph.get("connections", []))
        if conn_count > 0:
            top_conn = graph["connections"][:3]
            conn_lines = []
            for c in top_conn:
                u = ctx.guild.get_member(c["user_id"])
                name = u.display_name if u else f"ID:{c['user_id']}"
                conn_lines.append(f"**{name}** — {c['interactions']} взаим.")
            e.add_field(name=f"🕸️ Связи ({conn_count})", value="\n".join(conn_lines), inline=False)

        # Угрозы
        if threat["threats"]:
            threat_lines = [
                f"{self._risk_emoji(t['level'])} **{t['source']}**: {t['reason']}"
                for t in threat["threats"][:5]
            ]
            e.add_field(name="🚨 Обнаруженные угрозы", value="\n".join(threat_lines), inline=False)
        else:
            e.add_field(name="✅ Угрозы", value="Не обнаружено", inline=False)

        # Подписываем действие
        action_id = await log_signed_action(ctx.guild.id, ctx.author.id, "SCAN", member.id, "Полное сканирование")
        e.set_footer(text=f"Action ID: #{action_id} · Witness Security")

        await ctx.send(embed=e)

    # ── -q threat @user ───────────────────────────────────────
    async def _cmd_threat(self, ctx, *args):
        member = await self._resolve_member(ctx, args)
        if not member: return

        threat = await check_threat_intelligence(member)
        color = self._risk_color(threat["risk_score"])
        e = self._make_embed(
            f"{self._risk_emoji(threat['threat_level'])} Threat Intelligence: {member.display_name}",
            color=color
        )
        e.set_thumbnail(url=member.display_avatar.url)
        e.add_field(name="Risk Score", value=f"**{threat['risk_score']}/100** · {threat['threat_level']}", inline=True)
        e.add_field(name="ID", value=str(member.id), inline=True)

        if threat["threats"]:
            for t in threat["threats"]:
                e.add_field(
                    name=f"{self._risk_emoji(t['level'])} {t['source']}",
                    value=t["reason"],
                    inline=False
                )
        else:
            e.add_field(name="✅ Результат", value="Угроз не обнаружено", inline=False)

        await ctx.send(embed=e)

    # ── -q graph @user ────────────────────────────────────────
    async def _cmd_graph(self, ctx, *args):
        member = await self._resolve_member(ctx, args)
        if not member: return

        graph = await get_social_graph(ctx.guild.id, member.id)
        connections = graph.get("connections", [])

        e = self._make_embed(
            f"🕸️ Граф связей: {member.display_name}",
            color=SC.INFO
        )
        e.set_thumbnail(url=member.display_avatar.url)

        if not connections:
            e.description = "Нет данных о взаимодействиях. Нужно время для накопления."
        else:
            lines = []
            for c in connections[:10]:
                u = ctx.guild.get_member(c["user_id"])
                name = u.display_name if u else f"ID:{c['user_id']}"
                bar = "█" * min(c["interactions"] // 5 + 1, 10)
                lines.append(f"**{name}** `{bar}` {c['interactions']} взаим.")
            e.description = "\n".join(lines)
            e.add_field(name="Всего связей", value=str(len(connections)), inline=True)

            # Детектируем подозрительные кластеры
            if len(connections) > 10:
                new_accounts = []
                for c in connections[:10]:
                    u = ctx.guild.get_member(c["user_id"])
                    if u:
                        age = (datetime.datetime.utcnow() - u.created_at.replace(tzinfo=None)).days
                        if age < 30:
                            new_accounts.append(u.display_name)
                if len(new_accounts) > 3:
                    e.add_field(
                        name="⚠️ Подозрительный кластер",
                        value=f"{len(new_accounts)} новых аккаунтов среди связей: {', '.join(new_accounts[:3])}...",
                        inline=False
                    )

        await ctx.send(embed=e)

    # ── -q fp @user ───────────────────────────────────────────
    async def _cmd_fp(self, ctx, *args):
        member = await self._resolve_member(ctx, args)
        if not member: return

        fp = await get_fingerprint_report(ctx.guild.id, member.id)

        if "error" in fp:
            return await ctx.send(embed=self._make_embed("❓ Fingerprint", fp["error"], SC.NEUTRAL))

        risk = fp.get("risk_score", 0)
        color = self._risk_color(risk)
        e = self._make_embed(f"🧬 Fingerprint: {member.display_name}", color=color)
        e.set_thumbnail(url=member.display_avatar.url)

        bar = "█" * int(risk / 10) + "░" * (10 - int(risk / 10))
        e.add_field(name="Risk Score", value=f"`{bar}` **{risk:.0f}/100**", inline=True)
        e.add_field(name="Сообщений", value=f"**{fp.get('msg_count', 0):,}**", inline=True)
        e.add_field(name="Ср. длина", value=f"**{fp.get('avg_msg_len', 0):.0f}** симв.", inline=True)

        if fp.get("mention_ratio") is not None:
            e.add_field(name="Упоминания/сообщ.", value=f"**{fp['mention_ratio']:.0%}**", inline=True)
        if fp.get("link_ratio") is not None:
            e.add_field(name="Ссылки/сообщ.", value=f"**{fp['link_ratio']:.0%}**", inline=True)
        if fp.get("vocab_size"):
            e.add_field(name="Словарный запас", value=f"**{fp['vocab_size']}** уник. слов", inline=True)

        # Активность по часам
        hours = fp.get("active_hours", {})
        if hours:
            peak_hour = max(hours, key=hours.get, default="?")
            e.add_field(name="Пик активности", value=f"**{peak_hour}:00 UTC**", inline=True)
            # Ночная активность
            night = sum(hours.get(str(h), 0) for h in range(0, 6))
            total = sum(hours.values()) or 1
            if night / total > 0.7:
                e.add_field(name="⚠️ Аномалия", value="Преимущественно ночная активность (UTC 0-6)", inline=False)

        e.add_field(name="Первый раз", value=fp.get("first_seen", "?")[:10], inline=True)
        e.add_field(name="Последний раз", value=fp.get("last_seen", "?")[:10], inline=True)
        await ctx.send(embed=e)

    # ── -q nlp [текст] ────────────────────────────────────────
    async def _cmd_nlp(self, ctx, *args):
        if not args:
            return await ctx.send("❌ Использование: `-q nlp [текст для анализа]`", delete_after=5)

        text = " ".join(args)
        await ctx.send("🔍 Анализирую текст...", delete_after=2)

        result = await nlp_analyze(text, self.groq_key, self.gemini_key)

        # Определяем цвет по наихудшему показателю
        max_score = max(result.get("toxicity", 0), result.get("threat", 0), result.get("spam", 0))
        color = SC.CRITICAL if max_score > 0.7 else SC.HIGH if max_score > 0.4 else SC.LOW

        e = self._make_embed("🤖 NLP Анализ", color=color)
        e.add_field(name="📝 Текст", value=f"```{text[:300]}```", inline=False)

        scores = [
            ("☠️ Токсичность", result.get("toxicity", 0)),
            ("⚔️ Угрозы",      result.get("threat", 0)),
            ("📢 Спам",         result.get("spam", 0)),
        ]
        if "insult" in result:
            scores.append(("👊 Оскорбления", result["insult"]))

        for label, score in scores:
            bar = "█" * int(score * 10) + "░" * (10 - int(score * 10))
            verdict = "🔴 Высокий" if score > 0.7 else "🟡 Средний" if score > 0.3 else "🟢 Низкий"
            e.add_field(name=label, value=f"`{bar}` {score:.0%} · {verdict}", inline=True)

        if result.get("summary"):
            e.add_field(name="Вывод AI", value=result["summary"], inline=False)
        e.add_field(name="Источник", value=result.get("source", "?"), inline=True)
        await ctx.send(embed=e)

    # ── -q forensics [message_id] ─────────────────────────────
    async def _cmd_forensics(self, ctx, *args):
        if not args:
            return await ctx.send("❌ Использование: `-q forensics [ID сообщения]`", delete_after=5)

        try:
            msg_id = int(args[0])
        except ValueError:
            return await ctx.send("❌ Укажи числовой ID сообщения", delete_after=5)

        # Ищем сообщение во всех каналах
        message = None
        for channel in ctx.guild.text_channels:
            try:
                message = await channel.fetch_message(msg_id)
                break
            except (discord.NotFound, discord.Forbidden):
                continue

        if not message:
            return await ctx.send("❌ Сообщение не найдено", delete_after=5)

        e = self._make_embed(f"🔬 Криминалистика сообщения #{msg_id}", color=SC.INFO)
        e.add_field(name="Автор", value=f"{message.author.mention} (`{message.author.name}`)", inline=True)
        e.add_field(name="Канал", value=message.channel.mention, inline=True)
        e.add_field(name="Время", value=message.created_at.strftime("%d.%m.%Y %H:%M UTC"), inline=True)

        # Хеш контента
        if message.content:
            content_hash = hash_content(message.content)
            e.add_field(name="📋 Контент", value=f"```{message.content[:300]}```", inline=False)
            e.add_field(name="SHA-256 хеш", value=f"`{content_hash[:32]}...`", inline=False)

            # Проверка дублей
            dup = await check_duplicate_content(ctx.guild.id, message.author.id, message.content)
            e.add_field(
                name="🔄 Дублирование",
                value=f"{'⚠️ Дубликат!' if dup['is_duplicate'] else '✅ Уникальное'}",
                inline=True
            )

            # Фишинг
            phishing = check_phishing(message.content)
            e.add_field(
                name="🎣 Фишинг",
                value=f"{'⚠️ ' + phishing[0]['type'] if phishing else '✅ Чисто'}",
                inline=True
            )

        # Анализ изображений
        for att in message.attachments:
            if any(att.filename.lower().endswith(ext) for ext in [".jpg",".jpeg",".png",".gif",".webp"]):
                await ctx.send(f"🖼️ Анализирую изображение `{att.filename}`...")
                meta = await analyze_image_metadata(att.url)
                img_e = self._make_embed(f"🖼️ Изображение: {att.filename}", color=SC.INFO)
                img_e.add_field(name="Размер", value=f"{meta.get('size_bytes', 0) // 1024} KB", inline=True)
                img_e.add_field(name="Тип", value=meta.get("content_type", "?"), inline=True)
                img_e.add_field(name="Размеры", value=meta.get("dimensions", "?"), inline=True)
                img_e.add_field(name="SHA-256", value=f"`{meta.get('hash', '?')[:32]}...`", inline=False)
                if meta.get("findings"):
                    for f in meta["findings"]:
                        img_e.add_field(name=f"📌 {f['tag']}", value=str(f["value"])[:200], inline=False)
                await ctx.send(embed=img_e)

        await ctx.send(embed=e)

    # ── -q sig [action_id] ────────────────────────────────────
    async def _cmd_sig(self, ctx, *args):
        if not args:
            return await ctx.send("❌ Использование: `-q sig [ID действия]`", delete_after=5)

        try:
            action_id = int(args[0])
        except ValueError:
            return await ctx.send("❌ Укажи числовой ID действия", delete_after=5)

        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT mod_id,action_type,target_id,reason,signature,created_at FROM mod_signatures WHERE id=? AND guild_id=?",
                (action_id, ctx.guild.id)
            ) as c:
                row = await c.fetchone()

        if not row:
            return await ctx.send(embed=self._make_embed("❌ Действие не найдено", f"ID #{action_id} не существует на этом сервере", SC.DANGER))

        mod_id, action_type, target_id, reason, signature, created_at = row

        # Верифицируем подпись
        is_valid = verify_signature(mod_id, action_type, target_id, reason or "", created_at, signature)

        mod = ctx.guild.get_member(mod_id)
        target = ctx.guild.get_member(target_id)
        color = SC.LOW if is_valid else SC.CRITICAL

        e = self._make_embed(
            f"{'✅' if is_valid else '❌'} Цифровая подпись #{action_id}",
            color=color
        )
        e.add_field(name="Статус", value="✅ **Подпись верна** — действие не изменялось" if is_valid else "❌ **ПОДПИСЬ НЕДЕЙСТВИТЕЛЬНА** — возможна фальсификация!", inline=False)
        e.add_field(name="Модератор", value=mod.mention if mod else str(mod_id), inline=True)
        e.add_field(name="Действие", value=action_type, inline=True)
        e.add_field(name="Цель", value=target.mention if target else str(target_id), inline=True)
        e.add_field(name="Причина", value=reason or "не указана", inline=True)
        e.add_field(name="Время", value=created_at[:16], inline=True)
        e.add_field(name="Подпись (SHA-256)", value=f"`{signature[:32]}...`", inline=False)
        await ctx.send(embed=e)

    # ── -q alert ──────────────────────────────────────────────
    async def _cmd_alert(self, ctx, *args):
        # Получаем из БД последние 10 алертов
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT alert_type,severity,user_id,description,created_at FROM security_alerts WHERE guild_id=? ORDER BY id DESC LIMIT 10",
                (ctx.guild.id,)
            ) as c:
                rows = await c.fetchall()

        if not rows:
            return await ctx.send(embed=self._make_embed("✅ Нет алертов", "Последних угроз не обнаружено.", SC.LOW))

        e = self._make_embed("🚨 Последние алерты безопасности", color=SC.HIGH)
        for alert_type, severity, user_id, description, created_at in rows:
            user = ctx.guild.get_member(user_id) if user_id else None
            user_str = user.mention if user else (str(user_id) if user_id else "—")
            e.add_field(
                name=f"{self._risk_emoji(severity)} {alert_type} · {created_at[:16]}",
                value=f"Участник: {user_str}\n{description[:150]}",
                inline=False
            )
        await ctx.send(embed=e)

    # ── -q network ────────────────────────────────────────────
    async def _cmd_network(self, ctx, *args):
        guild = ctx.guild
        now = datetime.datetime.utcnow()
        e = self._make_embed("📡 Сетевая статистика сервера", color=SC.INFO)

        # Базовая статистика
        bots = sum(1 for m in guild.members if m.bot)
        new_accounts = sum(
            1 for m in guild.members
            if not m.bot and (now - m.created_at.replace(tzinfo=None)).days < 30
        )
        new_joins = sum(
            1 for m in guild.members
            if not m.bot and m.joined_at and (now - m.joined_at.replace(tzinfo=None)).days < 7
        )

        e.add_field(name="👥 Всего участников", value=f"**{guild.member_count}**", inline=True)
        e.add_field(name="🤖 Ботов", value=f"**{bots}**", inline=True)
        e.add_field(name="🆕 Новых аккаунтов (<30д)", value=f"**{new_accounts}**", inline=True)
        e.add_field(name="📥 Вошли за 7 дней", value=f"**{new_joins}**", inline=True)

        # Алерты за 24 часа
        day_ago = (now - datetime.timedelta(hours=24)).isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT severity, COUNT(*) FROM security_alerts WHERE guild_id=? AND created_at > ? GROUP BY severity",
                (guild.id, day_ago)
            ) as c:
                alert_rows = await c.fetchall()

        if alert_rows:
            alert_summary = " · ".join(f"{self._risk_emoji(sev)} {cnt}" for sev, cnt in alert_rows)
            e.add_field(name="🚨 Алертов за 24ч", value=alert_summary, inline=False)

        # Топ риск-скор участников из кэша
        risky = []
        for (gid, uid), fp in _fp_cache.items():
            if gid != guild.id: continue
            risk = _calculate_risk_score(fp)
            if risk > 40:
                risky.append((uid, risk))
        risky.sort(key=lambda x: x[1], reverse=True)

        if risky:
            lines = []
            for uid, risk in risky[:5]:
                m = guild.get_member(uid)
                name = m.display_name if m else str(uid)
                lines.append(f"{self._risk_emoji('HIGH' if risk>60 else 'MEDIUM')} **{name}** — {risk:.0f}/100")
            e.add_field(name="⚠️ Топ по риску (FP)", value="\n".join(lines), inline=False)

        # Активность систем
        e.add_field(name="🟢 Активные системы", value=(
            "Fingerprinting · Social Graph · Phishing Detection\n"
            "Duplicate Content · Threat Intelligence · Alerts"
        ), inline=False)

        await ctx.send(embed=e)

    # ── -q whitelist / blacklist @user ────────────────────────
    async def _cmd_whitelist(self, ctx, *args):
        await self._cmd_list(ctx, "white", *args)

    async def _cmd_blacklist(self, ctx, *args):
        await self._cmd_list(ctx, "black", *args)

    async def _cmd_list(self, ctx, list_type: str, *args):
        member = await self._resolve_member(ctx, args)
        if not member: return

        now = datetime.datetime.utcnow().isoformat()
        _lists_cache[ctx.guild.id][member.id] = list_type

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO sec_lists (guild_id,user_id,list_type,added_by,created_at)
                VALUES (?,?,?,?,?)
                ON CONFLICT(guild_id,user_id,list_type) DO UPDATE SET added_by=excluded.added_by, created_at=excluded.created_at
            """, (ctx.guild.id, member.id, list_type, ctx.author.id, now))
            await db.commit()

        emoji = "✅" if list_type == "white" else "🚫"
        label = "whitelist" if list_type == "white" else "blacklist"
        color = SC.LOW if list_type == "white" else SC.CRITICAL
        e = self._make_embed(f"{emoji} {label.capitalize()}", color=color)
        e.add_field(name="Участник", value=member.mention, inline=True)
        e.add_field(name="Список", value=label, inline=True)
        e.add_field(name="Добавил", value=ctx.author.mention, inline=True)
        await ctx.send(embed=e)

    # ── -q report @user ───────────────────────────────────────
    async def _cmd_report(self, ctx, *args):
        member = await self._resolve_member(ctx, args)
        if not member: return

        reason = " ".join(args[1:]) if len(args) > 1 else "Отчёт от администратора"
        await report_to_global_db(member.id, ctx.guild.id, reason, "medium")

        e = self._make_embed("📋 Добавлено в глобальную базу угроз", color=SC.WARNING)
        e.add_field(name="Участник", value=f"{member.mention} (`{member.name}`)", inline=True)
        e.add_field(name="Причина", value=reason, inline=True)
        e.description = "Этот участник будет отмечен при входе на другие серверы с Witness."
        await ctx.send(embed=e)

    # ── -q status ─────────────────────────────────────────────
    async def _cmd_status(self, ctx, *args):
        e = self._make_embed("🛡️ Статус Advanced Security", color=SC.LOW)

        systems = [
            ("🧬 Fingerprinting",       len(_fp_cache), "профилей в памяти"),
            ("🕸️ Social Graph",         len(_graph_cache), "связей в памяти"),
            ("📋 Content Hashes",       sum(len(v) for v in _content_cache.values()), "хешей в памяти"),
            ("🚨 Алерты",               sum(len(v) for v in _alerts_cache.values()), "в памяти"),
            ("⛔ Черный список",        sum(1 for v in _lists_cache.values() for t in v.values() if t=="black"), "записей"),
            ("✅ Белый список",         sum(1 for v in _lists_cache.values() for t in v.values() if t=="white"), "записей"),
        ]
        for name, count, label in systems:
            e.add_field(name=name, value=f"**{count}** {label}", inline=True)

        # Проверяем доступность AI
        ai_status = []
        if self.groq_key: ai_status.append("✅ Groq")
        if self.gemini_key: ai_status.append("✅ Gemini")
        if not ai_status: ai_status.append("⚠️ Нет AI ключей — используется rule-based")
        e.add_field(name="🤖 NLP Backend", value=" · ".join(ai_status), inline=False)
        e.add_field(name="🔑 HMAC Secret", value="✅ Настроен" if HMAC_SECRET else "❌ Не настроен", inline=True)
        await ctx.send(embed=e)

    # ── Вспомогательные методы ────────────────────────────────

    async def _resolve_member(self, ctx: commands.Context, args: tuple) -> Optional[discord.Member]:
        """Получает участника из упоминания или ID"""
        if ctx.message.mentions:
            return ctx.message.mentions[0]
        if args:
            try:
                member_id = int(args[0].strip("<@!>"))
                member = ctx.guild.get_member(member_id)
                if member: return member
            except ValueError:
                # Поиск по имени
                name = args[0].lower()
                for m in ctx.guild.members:
                    if m.display_name.lower().startswith(name) or m.name.lower().startswith(name):
                        return m
        await ctx.send("❌ Укажи участника: `-q scan @user` или `-q scan ID`", delete_after=5)
        return None


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ФОНОВАЯ ЗАДАЧА — периодический сброс кэшей
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def security_maintenance_loop(bot: commands.Bot):
    """Периодическое обслуживание: сохранение кэшей в БД, очистка старых данных"""
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            # Сохраняем fingerprints в БД каждые 10 минут
            for (guild_id, user_id), fp in list(_fp_cache.items()):
                if fp.get("msg_count", 0) > 0:
                    await _save_fingerprint(guild_id, user_id, fp)

            # Очищаем старые алерты из памяти (старше 24 часов)
            cutoff = (datetime.datetime.utcnow() - datetime.timedelta(hours=24)).isoformat()
            for guild_id in _alerts_cache:
                _alerts_cache[guild_id] = deque(
                    [a for a in _alerts_cache[guild_id] if a.get("created_at", "") > cutoff],
                    maxlen=50
                )

        except Exception as ex:
            print(f"[Security Maintenance] Error: {ex}")

        await asyncio.sleep(600)  # каждые 10 минут


# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  SETUP ФУНКЦИЯ — вызывается из main.py
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def setup(bot: commands.Bot):
    """Инициализирует и добавляет модуль безопасности в бота"""
    await sec_db_init()
    await bot.add_cog(AdvancedSecurityCog(bot))
    # Запускаем фоновое обслуживание
    bot.loop.create_task(security_maintenance_loop(bot))
    print("✅ Advanced Security Module loaded · prefix: -q")
