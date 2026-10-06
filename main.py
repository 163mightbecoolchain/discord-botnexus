"""
Witness v5.0 — Discord Bot for Gaming Communities
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
NEW IN v5:

  🛡️ SECURITY MODULE (Premium tier 1):
    - Full logging: 20+ event types, per-server toggle (хранится в DB)
    - Invite tracker: кто кого пригласил, история в SQLite (per guild)
    - Anti-raid: авто-кик при 8+ входах за 10 секунд
    - Anti-spam: авто-таймаут при 6+ сообщениях за 5 секунд
    - Suspicious detection: аккаунт младше 7 дней — alert
    - /security status/toggle/setlog
    - /invcheck /invuser /invdel
    - /warn /warnings /clearwarn
    - /purge

  💰 BLACKMARKET v2 (Pro):
    - Предметы T6.0–T8.4 (все уровни зачаровки)
    - % профит от городов И от Бреккилена отдельно
    - Сортировка по % профиту

  🤖 FREE AI (Groq → Gemini → Claude):
    - GROQ_API_KEY  — бесплатно (console.groq.com)
    - GEMINI_API_KEY — бесплатно (aistudio.google.com)
    - Claude как платный fallback

  📦 БЕЗОПАСНОСТЬ ХРАНЕНИЯ ДАННЫХ:
    - Все таблицы имеют guild_id как первичный/составной ключ
    - Данные серверов физически изолированы по guild_id
    - Сервер A НИКОГДА не видит данные сервера B
    - Варны, инвайты, настройки — всё per-guild
    - invite_cache ключи: "guild_id:invite_code" (нет пересечений)

  💳 КУДА ИДУТ ДЕНЬГИ ОТ ПОДПИСОК:
    - Stripe → твой банковский счёт (IBAN немецкий)
    - Настройка: Stripe Dashboard → Settings → Payouts
    - Вывод: автоматически каждые 2-7 рабочих дней
    - Минимум: €1

Requirements:
  pip install discord.py aiohttp python-dotenv aiosqlite
"""

import discord
import os, asyncio, random, time, json, datetime

from witness.config import TOKEN
from witness.core import bot, flush_log_queue, intents
from witness.tasks import graceful_shutdown
# Импорт модулей регистрирует их слэш-команды и обработчики событий на общем объекте bot.
from witness import moderation, admin, invites, general, ai_commands, games, albion, events  # noqa: F401

async def _graceful_shutdown():
    """
    Обработчик SIGTERM: Railway шлёт его при редеплое.
    Вызывает штатный graceful_shutdown (он сохраняет профили,
    языки и делает WAL checkpoint) и закрывает соединение.
    """
    print("\n[SHUTDOWN] Получен сигнал завершения, сохраняю данные...")
    try:
        await flush_log_queue()          # дописываем накопленные логи в каналы
    except Exception as ex:
        print(f"[SHUTDOWN] Очередь логов: {ex}")
    try:
        await graceful_shutdown(bot)
    except Exception as ex:
        print(f"[SHUTDOWN] Ошибка сохранения: {ex}")
    try:
        await bot.close()
    except Exception:
        pass
    print("[SHUTDOWN] Готово")


def _install_signal_handlers():
    """
    Railway при редеплое шлёт SIGTERM. Без обработчика процесс умирает
    мгновенно и несохранённые кэши (настройки, WAL) теряются.
    """
    import signal
    loop = asyncio.get_event_loop()

    def handler(signum, frame):
        try:
            loop.create_task(_graceful_shutdown())
        except Exception:
            pass

    for sig in (signal.SIGTERM, signal.SIGINT):
        try:
            signal.signal(sig, handler)
        except Exception:
            pass


if __name__ == "__main__":
    _install_signal_handlers()
    print("─" * 62)
    print(f"Запрошенные привилегированные интенты: "
          f"members={intents.members}, message_content={intents.message_content}")
    print("─" * 62)
    try:
        bot.run(TOKEN)
    except discord.errors.PrivilegedIntentsRequired:
        print("\n" + "=" * 62)
        print("❌ DISCORD ОТКЛОНИЛ ПРИВИЛЕГИРОВАННЫЕ ИНТЕНТЫ")
        print("=" * 62)
        print("Бот запросил:", 
              ", ".join(filter(None, [
                  "SERVER MEMBERS"  if intents.members else "",
                  "MESSAGE CONTENT" if intents.message_content else "",
              ])) or "ничего")
        print()
        print("Что проверить по порядку:")
        print()
        print("1. Токен и приложение — это одно и то же приложение?")
        print("   Dev Portal → приложение → Bot → Reset Token.")
        print("   Тумблеры включены у ТОГО приложения, чей токен в DISCORD_TOKEN?")
        print("   Частая причина: у аккаунта несколько приложений, тумблеры")
        print("   включены в одном, а токен взят из другого.")
        print()
        print("2. Dev Portal → Bot → Privileged Gateway Intents:")
        print("   Server Members Intent  — должен быть ВКЛЮЧЁН")
        print("   Message Content Intent — должен быть ВКЛЮЧЁН")
        print("   После переключения страница сохраняет сама, но убедись,")
        print("   что не осталось кнопки 'Save Changes' внизу экрана.")
        print()
        print("3. Временный запуск без части функций (переменные Railway):")
        print("   INTENT_MESSAGE_CONTENT=0  — отключит текст в логах, антифишинг, /summarize")
        print("   INTENT_MEMBERS=0          — отключит карантин и DM при наказаниях")
        print("=" * 62)
        raise SystemExit(1)
