"""
Фоновые задачи: напоминания, бэкапы, истечение мутов/банов/подписок, очистка памяти.
"""

import discord
import aiohttp
import aiosqlite
import os, asyncio, random, time, json, datetime
from .config import (
    BOT_ID,
    DB_PATH,
    TIER_NAMES,
    TOPGG_TOKEN,
)
from .database import (
    add_modlog,
    get_guild_settings,
    get_log_ch,
    _settings_cache,
    _SETTINGS_TTL,
)
from .i18n import _guild_lang
from .ui import build_embed, C
from .core import (
    bot,
    _cooldowns,
    flush_log_queue,
    LOG_FLUSH_INTERVAL,
    _log_queue,
    notify_mute_over,
    _raid_tracker,
    report_error,
    _spam_tracker,
)

# ── Очистка кэшей (запускается каждые 10 минут) ───────────────
async def reminder_check_loop(bot_instance):
    """Проверяет и отправляет напоминания каждую минуту"""
    await bot_instance.wait_until_ready()
    while not bot_instance.is_closed():
        try:
            now = datetime.datetime.utcnow().isoformat()
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT id,user_id,channel_id,message,repeat_mins,repeat_count,max_repeats "
                    "FROM reminders WHERE fire_at<=? AND done=0",
                    (now,)
                ) as c:
                    rows = await c.fetchall()
            for rid, uid, cid, message, repeat_mins, repeat_count, max_repeats in rows:
                try:
                    user = await bot_instance.fetch_user(uid)
                    if user:
                        e = discord.Embed(
                            description=f"⏰ {message}",
                            color=0x00B0F4,
                            timestamp=datetime.datetime.utcnow()
                        )
                        e.set_author(name="Reminder")
                        if repeat_mins > 0:
                            e.set_footer(text=f"Repeat {repeat_count+1}/{max_repeats} · every {repeat_mins} min")
                        await user.send(embed=e)
                except Exception:
                    try:
                        ch = bot_instance.get_channel(cid)
                        if ch:
                            u = await bot_instance.fetch_user(uid)
                            await ch.send(f"⏰ {u.mention if u else uid} {message}")
                    except Exception as ex:
                        print(f"[REMINDER] Не доставлено {uid}: {ex}")

                # Обновляем или помечаем как выполненное
                async with aiosqlite.connect(DB_PATH) as db:
                    new_count = repeat_count + 1
                    if repeat_mins > 0 and new_count < max_repeats:
                        next_fire = (datetime.datetime.utcnow() +
                                     datetime.timedelta(minutes=repeat_mins)).isoformat()
                        await db.execute(
                            "UPDATE reminders SET fire_at=?,repeat_count=? WHERE id=?",
                            (next_fire, new_count, rid)
                        )
                    else:
                        await db.execute("UPDATE reminders SET done=1 WHERE id=?", (rid,))
                    await db.commit()
        except Exception as ex:
            print(f"[REMINDER] Error: {ex}")
        await asyncio.sleep(60)


async def memory_cleanup_loop(bot_instance):
    """Чистит устаревшие записи из всех in-memory кэшей"""
    await bot_instance.wait_until_ready()
    while not bot_instance.is_closed():
        try:
            now = time.time()
            # Cooldowns старше 5 минут
            stale_cd = [k for k, v in _cooldowns.items() if now - v > 300]
            for k in stale_cd: _cooldowns.pop(k, None)
            # Spam tracker старше 60 секунд
            stale_sp = [k for k, v in _spam_tracker.items()
                        if v and now - v[-1] > 60]
            for k in stale_sp: _spam_tracker.pop(k, None)
            # Raid tracker старше 30 секунд
            stale_rd = [k for k, v in _raid_tracker.items()
                        if v and now - v[-1] > 30]
            for k in stale_rd: _raid_tracker.pop(k, None)
            # Settings cache старше TTL*2
            stale_sc = [k for k, v in _settings_cache.items()
                        if now - v.get("ts", 0) > _SETTINGS_TTL * 2]
            for k in stale_sc: _settings_cache.pop(k, None)
            # ── Очистка старых данных (раз в сутки) ─────────────
            if not hasattr(bot, '_last_db_cleanup') or                time.time() - bot._last_db_cleanup > 86400:
                bot._last_db_cleanup = time.time()
                try:
                    async with aiosqlite.connect(DB_PATH) as db:
                        # content_hashes: журнал найденных дублей сообщений.
                        # Детект работает на кэше в памяти (окно 1 час),
                        # эта таблица — только история, читателей у неё нет.
                        # Без очистки росла бесконтрольно.
                        await db.execute("""
                            DELETE FROM content_hashes
                            WHERE created_at < datetime('now', '-7 days')
                        """)
                        # security_alerts: разобранные > 30 дней,
                        # остальные > 90 дней
                        await db.execute("""
                            DELETE FROM security_alerts
                            WHERE resolved = 1
                              AND created_at < datetime('now', '-30 days')
                        """)
                        await db.execute("""
                            DELETE FROM security_alerts
                            WHERE created_at < datetime('now', '-90 days')
                        """)
                        # invite_log > 90 дней (кроме записей с заметками)
                        await db.execute("""
                            DELETE FROM invite_log
                            WHERE (note IS NULL OR note = '')
                              AND joined_at < datetime('now', '-90 days')
                        """)
                        # modlog: AUTO_ действия > 180 дней
                        await db.execute("""
                            DELETE FROM modlog
                            WHERE action LIKE 'AUTO_%'
                              AND created_at < datetime('now', '-180 days')
                        """)
                        # quarantine: завершённые > 30 дней
                        await db.execute("""
                            DELETE FROM quarantine
                            WHERE released = 1
                              AND quarantined_at < datetime('now', '-30 days')
                        """)
                        # temp_bans: разбаненные > 30 дней
                        await db.execute("""
                            DELETE FROM temp_bans
                            WHERE unbanned = 1
                              AND unban_at < datetime('now', '-30 days')
                        """)
                        # tickets: закрытые > 60 дней
                        await db.execute("""
                            DELETE FROM tickets
                            WHERE status = 'closed'
                              AND created_at < datetime('now', '-60 days')
                        """)
                        # reminders: выполненные > 7 дней
                        await db.execute("""
                            DELETE FROM reminders
                            WHERE done = 1
                              AND fire_at < datetime('now', '-7 days')
                        """)
                        await db.commit()
                        # VACUUM — освобождаем удалённые страницы
                        # Делаем раз в неделю (тяжёлая операция)
                        if not hasattr(bot, '_last_vacuum') or                            time.time() - bot._last_vacuum > 604800:
                            bot._last_vacuum = time.time()
                            try:
                                # aiosqlite оставляет курсор открытым после execute,
                                # а VACUUM требует, чтобы незакрытых запросов не было.
                                # Поэтому PRAGMA читаем через async with и закрываем.
                                async with aiosqlite.connect(DB_PATH) as vdb:
                                    async with vdb.execute(
                                            "PRAGMA wal_checkpoint(TRUNCATE)") as vc:
                                        await vc.fetchall()
                                    await vdb.execute("VACUUM")
                                print("[CLEANUP] VACUUM complete")
                            except Exception as vex:
                                print(f"[CLEANUP] VACUUM пропущен: {vex}")
                    print("[CLEANUP] DB cleanup complete")
                except Exception as ex:
                    print(f"[CLEANUP] Error: {ex}")
            if stale_cd or stale_sp or stale_rd:
                print(f"[CLEANUP] Cleared: {len(stale_cd)} cooldowns, "
                      f"{len(stale_sp)} spam, {len(stale_rd)} raid entries")
        except Exception as ex:
            print(f"[CLEANUP] Error: {ex}")
        await asyncio.sleep(600)  # каждые 10 минут


async def graceful_shutdown(bot_instance):
    """Сохраняет все кэши в БД перед остановкой"""
    print("⚙️ Graceful shutdown: saving caches...")
    # SQLite PRAGMA checkpoint — сбрасываем WAL на диск
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("PRAGMA wal_checkpoint(FULL)")
            await db.commit()
        print("✅ WAL checkpoint complete")
    except Exception as ex:
        print(f"⚠️ WAL checkpoint error: {ex}")
    try:
        # Сохраняем настройки
        async with aiosqlite.connect(DB_PATH) as db:
            for gid, lang in _guild_lang.items():
                await db.execute(
                    "INSERT INTO guild_settings (guild_id,lang) VALUES (?,?) "
                    "ON CONFLICT(guild_id) DO UPDATE SET lang=excluded.lang",
                    (gid, lang)
                )
            await db.commit()
        print("✅ Shutdown complete")
    except Exception as ex:
        print(f"⚠️ Shutdown error: {ex}")

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  DATABASE — per-guild isolation
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  HEALTH CHECK — Railway мониторит порт 8080
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

async def health_check_server():
    """Запускает полноценный веб-сервер (лендинг + дашборд + API + health check)"""
    try:
        from web_server import start_web_server
        await start_web_server(bot)
    except ImportError:
        # Fallback — простой health check если web_server.py не найден
        from aiohttp import web
        async def handle(request):
            return web.Response(
                text=f"OK|guilds={len(bot.guilds)}|latency={round(bot.latency*1000)}ms",
                status=200
            )
        app = web.Application()
        app.router.add_get("/", handle)
        app.router.add_get("/health", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "0.0.0.0", int(os.getenv("PORT", 8080)))
        await site.start()
        print(f"⚠️  web_server.py not found — using minimal health check")


async def tempban_loop(bot_instance):
    """Фоновая задача — проверяет истёкшие временные баны"""
    await bot_instance.wait_until_ready()
    while not bot_instance.is_closed():
        try:
            now = datetime.datetime.utcnow().isoformat()
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT guild_id,user_id,reason FROM temp_bans WHERE unban_at<=? AND unbanned=0",
                    (now,)
                ) as c:
                    rows = await c.fetchall()
            for guild_id, user_id, reason in rows:
                guild = bot_instance.get_guild(guild_id)
                if guild:
                    unbanned_now = False
                    try:
                        await guild.unban(discord.Object(id=user_id), reason=f"Tempban expired: {reason}")
                        unbanned_now = True
                        await add_modlog(guild_id, user_id, 0, "AUTO_UNBAN", "Tempban expired")
                        print(f"[TEMPBAN] Auto-unbanned {user_id} from {guild.name}")
                    except discord.NotFound:
                        # Бан уже снят вручную — запись закрываем, иначе цикл
                        # будет пытаться разбанить каждую минуту бесконечно
                        unbanned_now = True
                        print(f"[TEMPBAN] {user_id}: бан уже снят вручную, запись закрыта")
                    except discord.Forbidden:
                        print(f"[TEMPBAN] Нет прав на разбан {user_id} в {guild.name}")
                    except Exception as ex:
                        print(f"[TEMPBAN] Error unbanning {user_id}: {ex}")

                    if unbanned_now:
                        async with aiosqlite.connect(DB_PATH) as db:
                            await db.execute(
                                "UPDATE temp_bans SET unbanned=1 WHERE guild_id=? AND user_id=?",
                                (guild_id, user_id)
                            )
                            await db.commit()
                        # Разбанен — апелляция на этот бан больше не нужна
                        await expire_appeals(guild_id, user_id, ("BAN", "TEMPBAN"),
                                             "срок бана истёк")
        except Exception as ex:
            print(f"[TEMPBAN LOOP] Error: {ex}")
        await asyncio.sleep(60)  # проверяем каждую минуту


async def quarantine_loop(bot_instance):
    """Снимает карантинную роль по истечении времени"""
    await bot_instance.wait_until_ready()
    while not bot_instance.is_closed():
        try:
            now = datetime.datetime.utcnow().isoformat()
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT q.guild_id,q.user_id FROM quarantine q WHERE q.release_at<=? AND q.released=0",
                    (now,)
                ) as c:
                    rows = await c.fetchall()
            for guild_id, user_id in rows:
                guild = bot_instance.get_guild(guild_id)
                if not guild: continue
                member = guild.get_member(user_id)
                if not member: continue
                # Получаем настройки карантина
                async with aiosqlite.connect(DB_PATH) as db:
                    async with db.execute(
                        "SELECT role_id FROM quarantine_settings WHERE guild_id=?", (guild_id,)
                    ) as c:
                        qrow = await c.fetchone()
                if qrow and qrow[0]:
                    role = guild.get_role(qrow[0])
                    if role and role in member.roles:
                        try:
                            await member.remove_roles(role, reason="Quarantine expired")
                        except discord.Forbidden:
                            pass
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE quarantine SET released=1 WHERE guild_id=? AND user_id=?",
                        (guild_id, user_id)
                    )
                    await db.commit()
        except Exception as ex:
            print(f"[QUARANTINE LOOP] Error: {ex}")
        await asyncio.sleep(60)


async def log_flush_loop(bot_instance):
    """Раз в несколько секунд разгребает очередь логов"""
    await bot_instance.wait_until_ready()
    while not bot_instance.is_closed():
        try:
            if _log_queue:
                await flush_log_queue()
        except Exception as ex:
            print(f"[LOG_QUEUE] Loop error: {ex}")
        await asyncio.sleep(LOG_FLUSH_INTERVAL)


async def tier_expiry_loop(bot_instance):
    """
    Раз в 6 часов проверяет подписки и предупреждает заранее:
    за 7, 3 и 1 день до конца. Каждое предупреждение — один раз.
    """
    await bot_instance.wait_until_ready()
    await asyncio.sleep(120)
    while not bot_instance.is_closed():
        try:
            now = datetime.datetime.utcnow()
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT guild_id, tier, expires_at FROM subscriptions "
                    "WHERE tier > 0 AND expires_at != ''") as c:
                    rows = await c.fetchall()

            for gid, tier, exp in rows:
                try:
                    left = (datetime.datetime.fromisoformat(exp) - now).days
                except Exception:
                    continue
                if left < 0:
                    continue
                # Ближайший подходящий порог
                threshold = next((t for t in (1, 3, 7) if left <= t), None)
                if threshold is None:
                    continue

                async with aiosqlite.connect(DB_PATH) as db:
                    async with db.execute(
                        "SELECT 1 FROM tier_warnings WHERE guild_id=? AND days_left=?",
                        (gid, threshold)) as c:
                        if await c.fetchone():
                            continue
                    await db.execute(
                        "INSERT OR REPLACE INTO tier_warnings (guild_id, days_left, sent_at) "
                        "VALUES (?,?,?)", (gid, threshold, now.isoformat()))
                    await db.commit()

                guild = bot_instance.get_guild(gid)
                if not guild:
                    continue
                word = "день" if left == 1 else ("дня" if left in (2,3,4) else "дней")
                e = build_embed(C.WARNING if left > 1 else C.DANGER)
                e.set_author(name="⏳ Подписка скоро закончится")
                e.description = (
                    f"Тариф **{TIER_NAMES.get(tier, '?')}** на сервере **{guild.name}** "
                    f"истекает через **{left} {word}**."
                )
                e.add_field(
                    name="После окончания отключится",
                    value=("• Логирование событий безопасности\n"
                           "• Стилометрия и детект обхода бана\n"
                           "• Карантин и анти-рейд"),
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
                print(f"[TIER] Предупреждение серверу {gid}: осталось {left} дн.")
        except Exception as ex:
            print(f"[TIER] Loop error: {ex}")
        await asyncio.sleep(21600)      # 6 часов


async def backup_loop(bot_instance):
    """
    Раз в сутки делает копию базы, сжимает её и отправляет в приватный канал.
    Том Railway — единственное место, где живут данные; без копии
    любая его потеря означает потерю всей истории модерации.
    Канал задаётся переменной BACKUP_CHANNEL_ID.
    """
    await bot_instance.wait_until_ready()
    ch_id = int(os.getenv("BACKUP_CHANNEL_ID", "0") or 0)
    if not ch_id:
        print("[BACKUP] BACKUP_CHANNEL_ID не задан — резервные копии отключены")
        return
    await asyncio.sleep(300)          # даём боту прогреться после старта
    while not bot_instance.is_closed():
        tmp   = "/tmp/witness_backup.db"
        gz    = tmp + ".gz"
        parts = []
        try:
            # get_channel читает только кэш — если бот недавно добавлен
            # на сервер, канала там может не быть. Дозапрашиваем через API.
            ch = bot_instance.get_channel(ch_id)
            if not ch:
                try:
                    ch = await bot_instance.fetch_channel(ch_id)
                except Exception as cex:
                    print(f"[BACKUP] Канал {ch_id} недоступен: {cex}")
                    print("[BACKUP] Проверь: бот добавлен на сервер с этим каналом? "
                          "ID именно канала, а не сервера? Есть права "
                          "«Просмотр канала», «Отправка сообщений», «Прикрепление файлов»?")
                    await asyncio.sleep(3600); continue

            for f in (tmp, gz):
                if os.path.exists(f):
                    os.remove(f)

            # VACUUM INTO делает согласованную копию без остановки бота
            async with aiosqlite.connect(DB_PATH) as db:
                # Сбрасываем WAL, чтобы копия содержала все свежие записи.
                # Курсор PRAGMA обязательно закрываем — иначе VACUUM INTO
                # упадёт с "SQL statements in progress".
                async with db.execute("PRAGMA wal_checkpoint(TRUNCATE)") as c:
                    await c.fetchall()
                await db.execute("VACUUM INTO ?", (tmp,))

            raw_mb = os.path.getsize(tmp) / 1024 / 1024

            # Сжимаем: SQLite ужимается примерно в 5 раз.
            # Делаем в отдельном потоке, чтобы не блокировать бота.
            def _gzip():
                import gzip, shutil
                with open(tmp, "rb") as fi, gzip.open(gz, "wb", compresslevel=9) as fo:
                    shutil.copyfileobj(fi, fo, length=1024 * 1024)
            await asyncio.to_thread(_gzip)

            gz_mb = os.path.getsize(gz) / 1024 / 1024
            stamp = datetime.datetime.utcnow().strftime("%Y-%m-%d")
            LIMIT_MB = float(os.getenv("BACKUP_LIMIT_MB", "9"))   # запас под лимит Discord

            e = build_embed(C.PRIMARY)
            e.set_author(name="💾 Резервная копия базы")
            e.add_field(name="Дата",     value=stamp, inline=True)
            e.add_field(name="База",     value=f"{raw_mb:.1f} МБ", inline=True)
            e.add_field(name="В архиве", value=f"{gz_mb:.1f} МБ", inline=True)

            if gz_mb <= LIMIT_MB:
                await ch.send(embed=e, file=discord.File(gz, f"witness_{stamp}.db.gz"))
                print(f"[BACKUP] Копия отправлена ({gz_mb:.2f} МБ, база {raw_mb:.1f} МБ)")
            else:
                # Даже сжатая не влезает — режем на части по LIMIT_MB
                chunk = int(LIMIT_MB * 1024 * 1024)
                with open(gz, "rb") as f:
                    idx = 0
                    while True:
                        data = f.read(chunk)
                        if not data:
                            break
                        idx += 1
                        pth = f"/tmp/witness_{stamp}.db.gz.{idx:03d}"
                        with open(pth, "wb") as pf:
                            pf.write(data)
                        parts.append(pth)
                e.add_field(
                    name="Частей", value=str(len(parts)), inline=True)
                e.set_footer(text="Собрать: cat witness_*.db.gz.* > db.gz && gunzip db.gz")
                await ch.send(embed=e)
                for pth in parts:
                    await ch.send(file=discord.File(pth, os.path.basename(pth)))
                    await asyncio.sleep(1)      # не упираемся в rate limit
                print(f"[BACKUP] Копия отправлена {len(parts)} частями ({gz_mb:.1f} МБ)")
        except Exception as ex:
            await report_error("BACKUP", ex, "Не удалось создать резервную копию")
        finally:
            for f in [tmp, gz] + parts:
                try:
                    if os.path.exists(f): os.remove(f)
                except Exception:
                    pass
        await asyncio.sleep(86400)

async def mute_expiry_loop(bot_instance):
    """
    Раз в минуту проверяет истёкшие муты:
    уведомляет участника и закрывает висящие апелляции.
    """
    await bot_instance.wait_until_ready()
    while not bot_instance.is_closed():
        try:
            now = datetime.datetime.utcnow().isoformat()
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("""
                    SELECT guild_id, user_id FROM active_mutes
                    WHERE notified=0 AND until<=?
                """, (now,)) as c:
                    rows = await c.fetchall()
            for gid, uid in rows:
                await notify_mute_over(gid, uid, early=False)
                await expire_appeals(gid, uid, ("MUTE",), "срок мута истёк")
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE active_mutes SET notified=1 WHERE guild_id=? AND user_id=?",
                        (gid, uid))
                    await db.commit()
            # Чистим записи старше 30 дней
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "DELETE FROM active_mutes WHERE notified=1 AND until < datetime('now','-30 days')")
                await db.commit()
        except Exception as ex:
            await report_error("MUTE_EXPIRY", ex, "Сбой цикла проверки мутов")
        await asyncio.sleep(60)


async def expire_appeals(guild_id: int, user_id: int, action_types: tuple,
                          why: str = "срок наказания истёк"):
    """
    Закрывает висящие апелляции, когда наказание уже неактуально.
    Модераторам не нужно разбирать апелляцию на снятый мут.
    """
    try:
        placeholders = ",".join("?" * len(action_types))
        now = datetime.datetime.utcnow().isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(f"""
                SELECT id FROM appeals
                WHERE guild_id=? AND user_id=? AND status='pending'
                  AND action_type IN ({placeholders})
            """, (guild_id, user_id, *action_types)) as c:
                rows = await c.fetchall()
            if not rows:
                return 0
            await db.execute(f"""
                UPDATE appeals SET status='expired', reviewer_note=?, reviewed_at=?
                WHERE guild_id=? AND user_id=? AND status='pending'
                  AND action_type IN ({placeholders})
            """, (f"Закрыто автоматически: {why}", now, guild_id, user_id, *action_types))
            await db.commit()
        print(f"[APPEAL] Автозакрыто {len(rows)} апелляций у {user_id} ({why})")
        return len(rows)
    except Exception as ex:
        print(f"[APPEAL] expire_appeals error: {ex}")
        return 0

async def birthday_check_loop():
    """Фоновая задача — проверяет дни рождения каждый день в 09:00 UTC."""
    await bot.wait_until_ready()
    while not bot.is_closed():
        try:
            now = datetime.datetime.utcnow()
            if now.hour == 9 and now.minute < 5:
                today = f"{now.day:02d}.{now.month:02d}"
                async with aiosqlite.connect(DB_PATH) as db:
                    async with db.execute(
                        "SELECT guild_id, user_id FROM birthdays WHERE birthday=?", (today,)
                    ) as c:
                        rows = await c.fetchall()
                for gid, uid in rows:
                    settings = await get_guild_settings(gid)
                    ch_id = settings.get("birthday_channel", 0)
                    guild = bot.get_guild(gid)
                    if not guild: continue
                    ch = guild.get_channel(ch_id) or discord.utils.get(guild.text_channels, name="general")
                    if not ch: continue
                    member = guild.get_member(uid)
                    if not member: continue
                    e = discord.Embed(title="🎂 День рождения!", color=0xFF69B4)
                    e.description = f"Сегодня день рождения у {member.mention}! 🎉\nПоздравьте его/её!"
                    e.set_thumbnail(url=member.display_avatar.url)
                    try:
                        await ch.send(embed=e)
                    except Exception as ex:
                        print(f"[BIRTHDAY] Send error guild {gid}: {ex}")
        except Exception as ex:
            await report_error("BIRTHDAY", ex, "Сбой цикла дней рождения")
        await asyncio.sleep(300)  # проверяем каждые 5 минут


# ── Автообновление статистики на top.gg ──────────────────────
async def topgg_stats_loop(bot_instance):
    """Обновляет счётчик серверов на top.gg каждые 30 минут"""
    await bot_instance.wait_until_ready()
    if not TOPGG_TOKEN or not BOT_ID:
        print("⚠️ TOPGG_TOKEN или BOT_ID не заданы — автообновление top.gg отключено")
        return
    while not bot_instance.is_closed():
        try:
            guild_count = len(bot_instance.guilds)
            async with aiohttp.ClientSession() as s:
                async with s.post(
                    f"https://top.gg/api/bots/{BOT_ID}/stats",
                    headers={"Authorization": TOPGG_TOKEN,
                             "Content-Type": "application/json"},
                    json={"server_count": guild_count}
                ) as r:
                    if r.status == 200:
                        print(f"✅ top.gg stats updated: {guild_count} servers")
                    else:
                        print(f"⚠️ top.gg stats error: {r.status}")
        except Exception as ex:
            print(f"[TOPGG] Error: {ex}")
        await asyncio.sleep(1800)  # каждые 30 минут
