"""
Переводы интерфейса (RU / EN).
"""

import aiosqlite
from .config import DB_PATH

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  LOCALIZATION — поддержка RU / EN
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

_guild_lang: dict = {}  # {guild_id: "ru" | "en"}

def get_lang(guild_id: int) -> str:
    """Синхронная версия — из кэша (быстро, для частых вызовов)"""
    return _guild_lang.get(guild_id, "ru")

async def load_lang(guild_id: int) -> str:
    """Загружает язык из БД в кэш"""
    if guild_id in _guild_lang:
        return _guild_lang[guild_id]
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT lang FROM guild_settings WHERE guild_id=?", (guild_id,)
        ) as c:
            row = await c.fetchone()
    lang = row[0] if row and row[0] else "ru"
    _guild_lang[guild_id] = lang
    return lang

STRINGS = {
    "ru": {
        # General
        "level":           "Уровень",
        "xp":              "XP",
        "coins":           "Монеты",
        "members":         "Участники",
        "channels":        "Каналы",
        "roles":           "Роли",
        "boost":           "Буст",
        "created":         "Создан",
        "age":             "Аккаунт",
        "on_server":       "На сервере",
        "progress":        "До уровня {next}",
        "top_active":      "Топ активных",
        "source":          "Источник",
        "request":         "Запрос",
        # Albion
        "guild":           "Гильдия",
        "alliance":        "Альянс",
        "kill_fame":       "Kill Fame",
        "death_fame":      "Death Fame",
        "pve_fame":        "PvE Fame",
        "kd":              "K/D",
        "kills":           "Убийств",
        "deaths":          "Смертей",
        "activity":        "Активность",
        "fav_target":      "Любимая жертва",
        "last_kills":      "Последние убийства",
        "last_deaths":     "Последние смерти",
        "not_found":       "❌ **{name}** не найден.",
        "no_kills":        "Нет недавних убийств у **{name}**.",
        "no_deaths":       "Нет недавних смертей у **{name}**.",
        "advantage":       "Преимущество",
        # Games
        "rank":            "Ранг",
        "peak":            "Пик",
        "wr":              "Винрейт",
        "headshots":       "HS%",
        "wins":            "Побед",
        "temp":            "Температура",
        "feels":           "Ощущается",
        "humidity":        "Влажность",
        "wind":            "Ветер",
        "description":     "Описание",
        "original":        "Оригинал",
        "translation":     "Перевод",
        # Security logs
        "joined":          "Вход",
        "left":            "Выход",
        "banned":          "Бан",
        "unbanned":        "Разбан",
        "muted":           "Мьют",
        "unmuted":         "Мьют снят",
        "nick_changed":    "Смена ника",
        "roles_changed":   "Смена ролей",
        "msg_deleted":     "Удалено сообщение",
        "msg_edited":      "Редактирование",
        "invite_created":  "Инвайт создан",
        "invite_deleted":  "Инвайт удалён",
        "voice_update":    "Голос",
        "channel_created": "Канал создан",
        "channel_deleted": "Канал удалён",
        "role_created":    "Роль создана",
        "role_deleted":    "Роль удалена",
        "server_edited":   "Сервер изменён",
        "was":             "Было",
        "now":             "Стало",
        "member":          "Участник",
        "moderator":       "Модератор",
        "reason":          "Причина",
        "channel":         "Канал",
        "text":            "Текст",
        "added":           "Добавлены",
        "removed":         "Убраны",
        "until":           "До",
        "code":            "Код",
        "expires":         "Истекает",
        "uses":            "Использований",
        "invited_by":      "Пригласил",
        "invite":          "Инвайт",
        "action":          "Действие",
        "timeout_auto":    "Таймаут 30 сек",
        "antispam_title":  "Анти-спам",
        "raid_title":      "РЕЙД ЗАБЛОКИРОВАН",
        "raid_reason":     "8+ входов за 10 сек",
        "suspicious":      "Подозрительный аккаунт",
        "account_age":     "Возраст",
        "never":           "никогда",
        "unknown":         "неизвестно",
        "enter_action":    "вошёл в",
        "left_action":     "вышел из",
        "moved":           "→",
        "no_data":         "Нет данных.",
        "days":            "дней",
    },
    "en": {
        # General
        "level":           "Level",
        "xp":              "XP",
        "coins":           "Coins",
        "members":         "Members",
        "channels":        "Channels",
        "roles":           "Roles",
        "boost":           "Boost",
        "created":         "Created",
        "age":             "Account age",
        "on_server":       "On server",
        "progress":        "Progress to level {next}",
        "top_active":      "Most active",
        "source":          "Source",
        "request":         "Request",
        # Albion
        "guild":           "Guild",
        "alliance":        "Alliance",
        "kill_fame":       "Kill Fame",
        "death_fame":      "Death Fame",
        "pve_fame":        "PvE Fame",
        "kd":              "K/D",
        "kills":           "Kills",
        "deaths":          "Deaths",
        "activity":        "Activity",
        "fav_target":      "Favourite target",
        "last_kills":      "Recent kills",
        "last_deaths":     "Recent deaths",
        "not_found":       "❌ **{name}** not found.",
        "no_kills":        "No recent kills for **{name}**.",
        "no_deaths":       "No recent deaths for **{name}**.",
        "advantage":       "Advantage",
        # Games
        "rank":            "Rank",
        "peak":            "Peak",
        "wr":              "Win rate",
        "headshots":       "HS%",
        "wins":            "Wins",
        "temp":            "Temperature",
        "feels":           "Feels like",
        "humidity":        "Humidity",
        "wind":            "Wind",
        "description":     "Description",
        "original":        "Original",
        "translation":     "Translation",
        # Security logs
        "joined":          "Joined",
        "left":            "Left",
        "banned":          "Banned",
        "unbanned":        "Unbanned",
        "muted":           "Muted",
        "unmuted":         "Unmuted",
        "nick_changed":    "Nickname changed",
        "roles_changed":   "Roles changed",
        "msg_deleted":     "Message deleted",
        "msg_edited":      "Message edited",
        "invite_created":  "Invite created",
        "invite_deleted":  "Invite deleted",
        "voice_update":    "Voice",
        "channel_created": "Channel created",
        "channel_deleted": "Channel deleted",
        "role_created":    "Role created",
        "role_deleted":    "Role deleted",
        "server_edited":   "Server updated",
        "was":             "Before",
        "now":             "After",
        "member":          "Member",
        "moderator":       "Moderator",
        "reason":          "Reason",
        "channel":         "Channel",
        "text":            "Content",
        "added":           "Added",
        "removed":         "Removed",
        "until":           "Until",
        "code":            "Code",
        "expires":         "Expires",
        "uses":            "Uses",
        "invited_by":      "Invited by",
        "invite":          "Invite",
        "action":          "Action",
        "timeout_auto":    "Timeout 30s",
        "antispam_title":  "Anti-spam",
        "raid_title":      "RAID BLOCKED",
        "raid_reason":     "8+ joins in 10s",
        "suspicious":      "Suspicious account",
        "account_age":     "Age",
        "never":           "never",
        "unknown":         "unknown",
        "enter_action":    "joined",
        "left_action":     "left",
        "moved":           "→",
        "no_data":         "No data.",
        "days":            "days",
    }
}

def t(guild_id: int, key: str, **kwargs) -> str:
    """Translate key for guild language"""
    lang = get_lang(guild_id)
    text = STRINGS.get(lang, STRINGS["ru"]).get(key, STRINGS["ru"].get(key, key))
    if kwargs:
        try:
            text = text.format(**kwargs)
        except Exception:
            pass
    return text
