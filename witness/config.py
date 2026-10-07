"""
Настройки: переменные окружения, ключи API, тарифы.
"""

import os, asyncio, random, time, json, datetime
from dotenv import load_dotenv

load_dotenv()

TOKEN         = os.getenv("DISCORD_TOKEN")
GROQ_KEY      = os.getenv("GROQ_API_KEY")
GEMINI_KEY    = os.getenv("GEMINI_API_KEY")
# Названия моделей меняются у провайдеров — держим их настраиваемыми,
# чтобы при устаревании не нужно было править код
GROQ_MODEL    = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")
GEMINI_MODEL  = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY")
WEATHER_KEY   = os.getenv("WEATHER_API_KEY")
HENRIK_KEY    = os.getenv("HENRIK_API_KEY")
RIOT_KEY      = os.getenv("RIOT_API_KEY")
STEAM_KEY     = os.getenv("STEAM_API_KEY")
LOSTARK_KEY   = os.getenv("LOSTARK_API_KEY")
GOOGLE_CREDS  = os.getenv("GOOGLE_CREDENTIALS")
SHEET_ID      = os.getenv("SHEET_ID")
DB_PATH       = os.getenv("DB_PATH", "witnessbot.db")
# Security module
HMAC_SECRET     = os.getenv("HMAC_SECRET", "")           # любая случайная строка, фиксированная!
# EVENT_LOGS=0 глушит логи действий участников на всех серверах (например, когда
# на сервере уже логирует другой бот). Алерты анти-рейда и анти-спама остаются.
EVENT_LOGS      = os.getenv("EVENT_LOGS", "1") != "0"
ALBION_BASE   = "https://gameinfo.albiononline.com/api/gameinfo"
ALBION_DATA   = "https://west.albion-online-data.com/api/v2"

TIER_FREE, TIER_PREMIUM, TIER_PRO = 0, 1, 2
TIER_NAMES  = {0: "Free", 1: "⭐ Premium", 2: "💎 Pro"}
TIER_COLORS = {0: 0x6b7fa3, 1: 0x00E5FF, 2: 0xFFD700}



# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  PROMOTION COMMANDS
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

BOT_ID       = os.getenv("BOT_ID", "")        # ID бота из Developer Portal
TOPGG_TOKEN  = os.getenv("TOPGG_TOKEN", "")   # токен top.gg для автообновления статистики
SUPPORT_URL  = os.getenv("SUPPORT_URL", "https://discord.gg/witness")  # ссылка на support сервер


def get_invite_url() -> str:
    if not BOT_ID:
        return "Добавь BOT_ID в Railway Variables"
    perms = 8  # Administrator — всё в одном
    return f"https://discord.com/api/oauth2/authorize?client_id={BOT_ID}&permissions={perms}&scope=bot%20applications.commands"
