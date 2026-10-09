"""
Обработчики событий Discord (вход участников, сообщения, логи и т.д.).
"""

import discord
from discord import app_commands
import aiosqlite
import os, asyncio, random, time, json, datetime
from datetime import timedelta
from types import SimpleNamespace
from .config import (
    DB_PATH,
    EVENT_LOGS,
    SUPPORT_URL,
    TIER_FREE,
    TIER_PREMIUM,
)
from .database import (
    add_coins,
    add_modlog,
    add_xp,
    db_init,
    get_guild_settings,
    get_guild_settings_cached,
    get_log_ch,
    get_xp,
    is_enabled,
    log_invite_use,
)
from .i18n import _guild_lang, t
from .ui import build_embed, C
from .core import (
    antinuke_check,
    _audit_actor,
    bot,
    get_tier,
    intents,
    _invite_cache,
    notify_mute_over,
    queue_log,
    refresh_invite_cache,
    resolve_member,
    sec_check,
    _spam_tracker,
    _suppress_next_timeout_dm,
)
from .tasks import (
    backup_loop,
    birthday_check_loop,
    expire_appeals,
    graceful_shutdown,
    health_check_server,
    log_flush_loop,
    memory_cleanup_loop,
    mute_expiry_loop,
    quarantine_loop,
    reminder_check_loop,
    tempban_loop,
    tier_expiry_loop,
    topgg_stats_loop,
)
from .moderation import (
    AppealButton,
    BanApproveButton,
    BanRejectButton,
    send_appeal_dm,
)
from .invites import invite_autoclean_loop
from .ai_commands import count_message_activity
from .albion import albion_watch_loop, price_watch_loop

@bot.event
async def on_ready():
    await db_init()
    print(f"🗄️  DB path: {DB_PATH}")
    print(f"🔖 Witness build: 729b8616 | make_embed={True} | i18n={True}")

    # ── Загрузка Advanced Security Module ────────────────────
    try:
        from security_module import setup as security_setup
        await security_setup(bot)
    except ImportError:
        print("⚠️ security_module.py не найден — Advanced Security отключён")
    except Exception as ex:
        print(f"⚠️ Ошибка загрузки security модуля: {ex}")

    # Заполняем кэш инвайтов
    _invite_cache.clear()
    for guild in bot.guilds:
        await refresh_invite_cache(guild)

    # Без Server Members Intent входы распознаются по системному сообщению
    # «X присоединился» (см. on_message) — предупреждаем, где оно выключено
    if not intents.members:
        for guild in bot.guilds:
            if not (guild.system_channel and guild.system_channel_flags.join_notifications):
                print(f"⚠️ {guild.name}: системные сообщения о входе выключены — "
                      f"бот не увидит новых участников (антирейд, карантин, инвайты)")

    # Семафор Albion API создаётся сам при первом запросе (albion_fetch).

    # ── Синхронизация команд ──────────────────────────────────
    # НЕ очищаем tree — это удаляет все зарегистрированные команды!
    # Просто синхронизируем текущее состояние с Discord
    try:
        synced = await bot.tree.sync()
        print(f"✅ Синхронизировано {len(synced)} команд глобально")
        for cmd in sorted(synced, key=lambda c: c.name):
            print(f"   /{cmd.name}")
    except discord.HTTPException as ex:
        # 50240: у приложения включены Activities, и Discord создал команду
        # «точку входа» (Entry Point). Обычный sync её не содержит и Discord
        # отказывается затирать — из-за этого падала вся синхронизация.
        # Решение: подтягиваем существующую точку входа в дерево и повторяем.
        if getattr(ex, "code", None) == 50240:
            print("[SYNC] Обнаружена команда-точка входа Activity, сохраняю её...")
            try:
                existing = await bot.http.get_global_commands(bot.application_id)
                entry = [c for c in existing
                         if int(c.get("type", 1)) == 4]      # 4 = PRIMARY_ENTRY_POINT
                if entry:
                    # to_dict() в discord.py 2.x требует ссылку на дерево
                    payload = [c.to_dict(bot.tree) for c in bot.tree.get_commands()]
                    payload.extend(entry)
                    synced = await bot.http.bulk_upsert_global_commands(
                        bot.application_id, payload)
                    print(f"✅ Синхронизировано {len(synced)} команд "
                          f"(включая точку входа Activity)")
                else:
                    print("[SYNC] Точка входа не найдена, повтор обычной синхронизации")
                    synced = await bot.tree.sync()
                    print(f"✅ Синхронизировано {len(synced)} команд")
            except Exception as ex2:
                print(f"❌ Синхронизация не удалась: {ex2}")
                print("   Обходной путь: Dev Portal → Activities → Настройки → "
                      "отключить Activities, дождаться синхронизации, включить снова.")
        else:
            print(f"❌ Ошибка синхронизации: {ex}")
    except Exception as ex:
        print(f"❌ Ошибка синхронизации: {ex}")

    # ── Persistent views: восстанавливаем кнопки после рестарта ──
    if not hasattr(bot, "_dynamic_items_added"):
        bot._dynamic_items_added = True
        bot.add_dynamic_items(AppealButton, BanApproveButton, BanRejectButton)

    # ── Запуск фоновых задач ─────────────────────────────────
    if not hasattr(bot, "_tasks_started"):
        bot._tasks_started = True
        bot.loop.create_task(birthday_check_loop())
        bot.loop.create_task(price_watch_loop())
        bot.loop.create_task(tempban_loop(bot))
        bot.loop.create_task(quarantine_loop(bot))
        bot.loop.create_task(albion_watch_loop(bot))
        bot.loop.create_task(health_check_server())
        bot.loop.create_task(topgg_stats_loop(bot))
        bot.loop.create_task(memory_cleanup_loop(bot))
        bot.loop.create_task(reminder_check_loop(bot))
        bot.loop.create_task(invite_autoclean_loop())
        bot.loop.create_task(mute_expiry_loop(bot))
        bot.loop.create_task(backup_loop(bot))
        bot.loop.create_task(tier_expiry_loop(bot))
        bot.loop.create_task(log_flush_loop(bot))
        print("✅ Фоновые задачи запущены")

    # ── Загружаем языки серверов из БД ───────────────────────
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("SELECT guild_id, lang FROM guild_settings WHERE lang IS NOT NULL") as c:
                lang_rows = await c.fetchall()
        for gid, lang in lang_rows:
            if lang in ("ru", "en"):
                _guild_lang[gid] = lang
        print(f"✅ Языки загружены: {len(_guild_lang)} серверов")
    except Exception as ex:
        print(f"⚠️ Ошибка загрузки языков: {ex}")

    print(f"✅ Witness v5 | {bot.user} | {len(bot.guilds)} серверов | {len(_invite_cache)} инвайтов в кэше")
    await bot.change_presence(activity=discord.Activity(
        type=discord.ActivityType.watching, name="/help | witnessbot.gg"))




@bot.event
async def on_message(message):
    # Без Server Members Intent событие входа участника не приходит. Зато Discord
    # публикует в системный канал сообщение «X присоединился», и его автор —
    # сам новый участник. Передаём его в обычные обработчики входа.
    if message.type == discord.MessageType.new_member:
        if not intents.members and message.guild and isinstance(message.author, discord.Member):
            bot.dispatch("member_join", message.author)
        return
    if message.author.bot or not message.guild: return
    gid, uid = message.guild.id, message.author.id
    # Счётчик активности по часам для /serverstats. Раньше жил в отдельном
    # "on_message_for_stats", но такого события в Discord нет — он не вызывался.
    count_message_activity(message)
    await add_xp(gid, uid, 5); await add_coins(gid, uid, 1)

    xp = await get_xp(gid, uid)
    if xp > 0 and xp % 100 < 5:
        await message.channel.send(f"⚡ {message.author.mention} → **Уровень {xp//100}**! 🎉", delete_after=10)
    _tier_cached = (await get_guild_settings_cached(gid)).get("tier", TIER_FREE)
    if _tier_cached >= TIER_PREMIUM and await is_enabled(gid, "anti_spam"):
        key = (gid, uid); now = time.time()
        _spam_tracker.setdefault(key, [])
        _spam_tracker[key] = [t for t in _spam_tracker[key] if now-t<5]
        _spam_tracker[key].append(now)
        if len(_spam_tracker[key]) >= 6:
            try:
                await message.delete()
                await message.author.timeout(timedelta(seconds=30), reason="Anti-spam")
                # DM отправит on_member_update автоматически
                ch = await get_log_ch(message.guild)
                if ch:
                    e = build_embed(C.DANGER)
                    e.set_author(name=t(gid, "antispam_title"))
                    e.add_field(name=t(gid, "member"), value=message.author.mention)
                    e.add_field(name=t(gid, "action"), value=t(gid, "timeout_auto"))
                    await ch.send(embed=e)
            except Exception: pass
    # process_commands больше не нужен — префикс-команд нет

@bot.event
async def on_member_join(member):
    gid = member.guild.id
    # Локдаун и анти-рейд (пороги и действия — в дашборде, witness/protection.py)
    from .protection import handle_join
    if await handle_join(member):
        return

    # ── Invite tracking ────────────────────────────────────────
    # Снимок кэша ДО того как Discord обновит счётчики
    old_snapshot = {k: v for k, v in _invite_cache.items() if k.startswith(f"{gid}:")}

    used_code = None
    inviter_name = "неизвестно"
    inviter_id = 0

    # Ждём пока Discord обновит счётчик инвайта
    await asyncio.sleep(3)

    try:
        fresh_invites = await member.guild.invites()

        for inv in fresh_invites:
            cache_key = f"{gid}:{inv.code}"
            old_uses = old_snapshot.get(cache_key, 0)
            new_uses = inv.uses or 0
            if new_uses > old_uses:
                used_code = inv.code
                if inv.inviter:
                    inviter_name = inv.inviter.name
                    inviter_id = inv.inviter.id
                _invite_cache[cache_key] = new_uses
                break

        # Синхронизируем весь кэш
        for inv in fresh_invites:
            _invite_cache[f"{gid}:{inv.code}"] = inv.uses or 0

    except discord.Forbidden:
        print(f"[INVITE] {member.guild.name}: нет права Manage Server — инвайт не определён")
    except Exception as ex:
        print(f"[INVITE] {member.guild.name}: {ex}")

    # Разовые инвайты — исчезли из списка после использования
    if not used_code:
        try:
            fresh_codes = {f"{gid}:{inv.code}" for inv in await member.guild.invites()}
            for cache_key in list(old_snapshot.keys()):
                if cache_key not in fresh_codes:
                    used_code = cache_key.split(":", 1)[1]
                    inviter_name = "неизвестно (разовый инвайт)"
                    _invite_cache.pop(cache_key, None)
                    break
        except Exception:
            pass


    # Пишем в БД всегда (для /invcheck и /invuser)
    if used_code and inviter_id:
        await log_invite_use(gid, used_code, inviter_id, inviter_name, member.id, member.name)

    age = (datetime.datetime.utcnow() - member.created_at.replace(tzinfo=None)).days
    ch = await sec_check(member.guild, "joins")
    if ch:
        sus = "🔴 Подозрительный (<7д)" if age<7 else "🟡 Новый (<30д)" if age<30 else "🟢 Обычный"
        e = discord.Embed(title="📥 Вход", color=discord.Color.green(), timestamp=datetime.datetime.utcnow())
        e.set_thumbnail(url=member.display_avatar.url)
        e.add_field(name="Участник", value=f"{member.mention} (`{member.name}`)", inline=False)
        e.add_field(name="Аккаунт", value=f"{age} дней — {sus}", inline=True)
        e.add_field(name="Инвайт", value=f"`{used_code}`" if used_code else "неизвестно", inline=True)
        e.add_field(name="Пригласил", value=f"{inviter_name} (`{inviter_id}`)" if inviter_id else "неизвестно", inline=True)
        queue_log(ch, e)

    ch2 = await sec_check(member.guild, "suspicious")
    if ch2 and age < 7:
        e = discord.Embed(title="🚨 Подозрительный аккаунт", color=discord.Color.red(), timestamp=datetime.datetime.utcnow())
        e.set_thumbnail(url=member.display_avatar.url)
        e.add_field(name="Участник", value=f"{member.mention}", inline=False)
        e.add_field(name="Возраст", value=f"{age} дней", inline=True)
        e.add_field(name="Инвайт", value=f"`{used_code}`" if used_code else "неизвестно", inline=True)
        await ch2.send(embed=e)

    # Карантинная роль для новых аккаунтов (auto-quarantine)
    async with aiosqlite.connect(DB_PATH) as _qdb:
        async with _qdb.execute(
            "SELECT role_id,duration_hours,min_age_days,enabled FROM quarantine_settings WHERE guild_id=?",
            (gid,)
        ) as _qc:
            qsettings = await _qc.fetchone()
    if qsettings and qsettings[3] and age < qsettings[2]:
        qrole = member.guild.get_role(qsettings[0])
        if qrole:
            try:
                await member.add_roles(qrole, reason=f"Auto-quarantine: account {age}d old")
                release_at = (datetime.datetime.utcnow() + timedelta(hours=qsettings[1])).isoformat()
                async with aiosqlite.connect(DB_PATH) as _qdb2:
                    await _qdb2.execute(
                        "INSERT INTO quarantine (guild_id,user_id,quarantined_at,release_at) VALUES (?,?,?,?) ON CONFLICT(guild_id,user_id) DO UPDATE SET released=0,release_at=excluded.release_at",
                        (gid, member.id, datetime.datetime.utcnow().isoformat(), release_at)
                    )
                    await _qdb2.commit()
            except discord.Forbidden:
                pass

@bot.event
async def on_member_remove(member):
    # Участник мог не уйти сам, а быть кикнут через интерфейс Discord.
    # Кик отдельного события не имеет — распознаём по audit log.
    try:
        mod_id, reason = await _audit_actor(
            member.guild, discord.AuditLogAction.kick, member.id, window=8)
        if mod_id:
            await add_modlog(member.guild.id, member.id, mod_id, "KICK",
                             reason or "Кик через Discord", "")
            await antinuke_check(member.guild, mod_id, "kick")
    except Exception as ex:
        print(f"[KICK_LOG] {ex}")

    ch = await sec_check(member.guild, "leaves")
    if not ch: return
    roles = [r.mention for r in member.roles if r.name != "@everyone"]
    gid2 = member.guild.id
    e = build_embed(C.DANGER, thumbnail=member.display_avatar.url)
    e.set_author(name=t(gid2, "left"))
    e.add_field(name=t(gid2, "member"), value=f"{member.mention} · `{member.name}`", inline=False)
    e.add_field(name=t(gid2, "roles"),  value=", ".join(roles) if roles else "—",    inline=False)
    queue_log(ch, e)

@bot.event
async def on_member_ban(guild, user):
    # Бан мог быть выдан вручную через интерфейс Discord — тогда в modlog
    # записи нет, и на сайте действие не видно. Достаём автора из audit log.
    try:
        mod_id, reason = await _audit_actor(guild, discord.AuditLogAction.ban, user.id)
        if mod_id is not None:
            already = False
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("""
                    SELECT 1 FROM modlog
                    WHERE guild_id=? AND user_id=? AND action LIKE '%BAN%'
                      AND created_at > datetime('now','-15 seconds') LIMIT 1
                """, (guild.id, user.id)) as c:
                    already = bool(await c.fetchone())
            if not already:
                await add_modlog(guild.id, user.id, mod_id, "BAN",
                                 reason or "Выдан через Discord", "")
            if mod_id:
                await antinuke_check(guild, mod_id, "ban")
    except Exception as ex:
        print(f"[BAN_LOG] {ex}")

    ch = await sec_check(guild, "bans")
    if not ch: return
    gid3 = guild.id
    e = build_embed(C.DANGER)
    e.set_author(name=t(gid3, "banned"))
    e.add_field(name=t(gid3, "member"), value=f"{user.mention} · `{user.name}`", inline=False)
    await ch.send(embed=e)

@bot.event
async def on_member_unban(guild, user):
    # Ручной разбан тоже пишем в modlog и закрываем tempban-запись,
    # иначе цикл будет пытаться разбанить уже разбаненного
    try:
        mod_id, reason = await _audit_actor(guild, discord.AuditLogAction.unban, user.id)
        if mod_id is not None:
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "UPDATE temp_bans SET unbanned=1 WHERE guild_id=? AND user_id=?",
                    (guild.id, user.id))
                await db.commit()
            await add_modlog(guild.id, user.id, mod_id, "UNBAN",
                             reason or "Разбан через Discord", "")
            await expire_appeals(guild.id, user.id, ("BAN", "TEMPBAN"), "участник разбанен")
    except Exception as ex:
        print(f"[UNBAN_LOG] {ex}")

    ch = await sec_check(guild, "bans")
    if not ch: return
    gid4 = guild.id
    e = build_embed(C.SUCCESS)
    e.set_author(name=t(gid4, "unbanned"))
    e.add_field(name=t(gid4, "member"), value=user.mention, inline=False)
    await ch.send(embed=e)

@bot.event
async def on_member_update(before, after):
    if before.nick != after.nick:
        ch = await sec_check(after.guild, "nick_change")
        if ch:
            gid5 = after.guild.id
            e = build_embed(C.INFO)
            e.set_author(name=t(gid5, "nick_changed"))
            e.add_field(name=t(gid5, "member"), value=after.mention,              inline=False)
            e.add_field(name=t(gid5, "was"),    value=before.nick or before.name, inline=True)
            e.add_field(name=t(gid5, "now"),    value=after.nick or after.name,   inline=True)
            await ch.send(embed=e)
    added = set(after.roles)-set(before.roles); removed = set(before.roles)-set(after.roles)
    if added or removed:
        ch = await sec_check(after.guild, "role_change")
        if ch:
            gid6 = after.guild.id
            e = build_embed(C.PRIMARY)
            e.set_author(name=t(gid6, "roles_changed"))
            e.add_field(name=t(gid6, "member"),  value=after.mention, inline=False)
            if added:   e.add_field(name=t(gid6, "added"),   value=", ".join(r.mention for r in added),   inline=False)
            if removed: e.add_field(name=t(gid6, "removed"), value=", ".join(r.mention for r in removed), inline=False)
            await ch.send(embed=e)
    if before.timed_out_until != after.timed_out_until:
        ch = await sec_check(after.guild, "timeouts")
        if ch:
            gid7 = after.guild.id
            if after.timed_out_until:
                e = build_embed(C.WARNING)
                e.set_author(name=t(gid7, "muted"))
                e.add_field(name=t(gid7, "member"), value=after.mention, inline=False)
                e.add_field(name=t(gid7, "until"),  value=after.timed_out_until.strftime("%d.%m.%Y %H:%M"), inline=True)
            else:
                e = build_embed(C.SUCCESS)
                e.set_author(name=t(gid7, "unmuted"))
                e.add_field(name=t(gid7, "member"), value=after.mention, inline=False)
            await ch.send(embed=e)
        # ── active_mutes: отслеживаем срок, чтобы уведомить об окончании ──
        try:
            if after.timed_out_until:
                until_iso = after.timed_out_until.replace(tzinfo=None).isoformat()
                now_iso   = datetime.datetime.utcnow().isoformat()
                # причину подтянем из свежего modlog
                mute_reason = ""
                try:
                    async with aiosqlite.connect(DB_PATH) as db:
                        async with db.execute("""
                            SELECT reason FROM modlog
                            WHERE guild_id=? AND user_id=?
                              AND action IN ('MUTE','AUTO_MUTE','WARN')
                            ORDER BY id DESC LIMIT 1
                        """, (after.guild.id, after.id)) as c:
                            rr = await c.fetchone()
                    if rr and rr[0]:
                        mute_reason = rr[0]
                except Exception:
                    pass
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("""
                        INSERT INTO active_mutes (guild_id,user_id,until,reason,notified,created_at)
                        VALUES (?,?,?,?,0,?)
                        ON CONFLICT(guild_id,user_id) DO UPDATE SET
                            until=excluded.until, reason=excluded.reason,
                            notified=0, created_at=excluded.created_at
                    """, (after.guild.id, after.id, until_iso, mute_reason, now_iso))
                    await db.commit()

                # Мут мог быть выдан вручную через Discord — тогда записи в
                # modlog нет и на сайте действие не отображается
                if before.timed_out_until is None:
                    try:
                        mod_id, ar = await _audit_actor(
                            after.guild, discord.AuditLogAction.member_update, after.id)
                        if mod_id:
                            already = False
                            async with aiosqlite.connect(DB_PATH) as db:
                                async with db.execute("""
                                    SELECT 1 FROM modlog
                                    WHERE guild_id=? AND user_id=? AND action LIKE '%MUTE%'
                                      AND created_at > datetime('now','-15 seconds') LIMIT 1
                                """, (after.guild.id, after.id)) as c:
                                    already = bool(await c.fetchone())
                            if not already:
                                left = after.timed_out_until - datetime.datetime.now(
                                    datetime.timezone.utc)
                                mins = max(1, int(left.total_seconds() // 60))
                                dur = (f"{mins}m" if mins < 60 else
                                       f"{mins // 60}h" if mins < 1440 else
                                       f"{mins // 1440}d")
                                await add_modlog(after.guild.id, after.id, mod_id, "MUTE",
                                                 ar or mute_reason or "Мут через Discord", dur)
                            await antinuke_check(after.guild, mod_id, "mute")
                    except Exception as ex:
                        print(f"[MUTE_LOG] audit: {ex}")
            elif before.timed_out_until is not None:
                # Мут сняли досрочно — уведомляем и закрываем апелляции
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute(
                        "UPDATE active_mutes SET notified=1 WHERE guild_id=? AND user_id=?",
                        (after.guild.id, after.id))
                    await db.commit()
                await notify_mute_over(after.guild.id, after.id, early=True)
                await expire_appeals(after.guild.id, after.id, ("MUTE",),
                                     "мут снят досрочно")
        except Exception as ex:
            print(f"[MUTE_TRACK] {ex}")

        # mute_log — записываем для /muteboard
        if (before.timed_out_until is None and after.timed_out_until is not None):
            try:
                duration = (after.timed_out_until.replace(tzinfo=None) -
                            datetime.datetime.utcnow()).total_seconds()
                if duration > 0:
                    now = datetime.datetime.utcnow().isoformat()
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute("""
                            INSERT INTO mute_log (guild_id, user_id, total_seconds, mute_count, last_muted)
                            VALUES (?, ?, ?, 1, ?)
                            ON CONFLICT(guild_id, user_id) DO UPDATE SET
                                total_seconds = total_seconds + excluded.total_seconds,
                                mute_count    = mute_count + 1,
                                last_muted    = excluded.last_muted
                        """, (after.guild.id, after.id, int(duration), now))
                        await db.commit()
                    # DM с кнопкой апелляции на любой таймаут (даже от ручного действия модератора)
                    skey = (after.guild.id, after.id)
                    if not after.bot and skey not in _suppress_next_timeout_dm:
                        # форматируем длительность для DM
                        if duration < 60:
                            dur_str = f"{int(duration)} сек"
                        elif duration < 3600:
                            dur_str = f"{int(duration/60)} мин"
                        elif duration < 86400:
                            dur_str = f"{int(duration/3600)} ч"
                        else:
                            dur_str = f"{int(duration/86400)} д"

                        # Достаём настоящую причину из audit log (для ручных мутов через Discord UI)
                        mute_reason = f"Тайм-аут на {dur_str}"
                        try:
                            async for entry in after.guild.audit_logs(
                                limit=5, action=discord.AuditLogAction.member_update
                            ):
                                if (entry.target and entry.target.id == after.id and
                                        (datetime.datetime.now(datetime.timezone.utc) -
                                         entry.created_at).total_seconds() < 10):
                                    if entry.reason:
                                        mute_reason = f"Тайм-аут на {dur_str} · {entry.reason}"
                                    break
                        except Exception:
                            pass

                        try:
                            await send_appeal_dm(after, after.guild, "MUTE", mute_reason)
                        except Exception:
                            pass
                    # Снимаем флаг подавления после обработки
                    _suppress_next_timeout_dm.discard(skey)
            except Exception as ex:
                print(f"[MUTE_LOG] on_member_update error: {ex}")


@bot.event
async def on_audit_log_entry_create(entry):
    """
    Без Server Members Intent не приходят события изменения и ухода участника.
    Ник, роли, таймауты и кики видны в журнале аудита: по записи собираем
    состояние «до» и передаём в on_member_update, а кик пишем в журнал модерации.
    """
    if intents.members:
        return              # с интентом всё придёт обычными событиями
    A = discord.AuditLogAction
    guild = entry.guild
    target_id = getattr(entry.target, "id", None)
    if not target_id:
        return

    if entry.action == A.kick:
        try:
            await add_modlog(guild.id, target_id, entry.user_id or 0, "KICK",
                             entry.reason or "Кик через Discord", "")
            if entry.user_id:
                await antinuke_check(guild, entry.user_id, "kick")
        except Exception as ex:
            print(f"[KICK_LOG] audit: {ex}")
        return

    if entry.action not in (A.member_update, A.member_role_update):
        return
    try:
        # Нужно свежее состояние, а не из кэша resolve_member
        after = await guild.fetch_member(target_id)
    except discord.HTTPException:
        return

    before_roles = list(after.roles)
    if entry.action == A.member_role_update:
        added   = {r.id for r in getattr(entry.after, "roles", [])}
        removed = [guild.get_role(r.id) for r in getattr(entry.before, "roles", [])]
        before_roles = [r for r in after.roles if r.id not in added]
        before_roles += [r for r in removed if r is not None]

    before_timeout = after.timed_out_until          # таймаут в этой записи не менялся
    if hasattr(entry.before, "timed_out_until"):
        before_timeout = entry.before.timed_out_until
        # Истёкший таймаут для on_member_update — то же самое, что его отсутствие
        if before_timeout and before_timeout <= datetime.datetime.now(datetime.timezone.utc):
            before_timeout = None

    before = SimpleNamespace(
        nick=getattr(entry.before, "nick", after.nick),
        name=after.name,
        roles=before_roles,
        timed_out_until=before_timeout,
    )
    await on_member_update(before, after)


@bot.event
async def on_message_delete(message):
    if message.author.bot or not message.guild: return
    ch = await sec_check(message.guild, "msg_delete")
    if not ch: return
    gid8 = message.guild.id
    e = build_embed(C.DANGER)
    e.set_author(name=t(gid8, "msg_deleted"))
    e.add_field(name=t(gid8, "member"),  value=message.author.mention,                                    inline=True)
    e.add_field(name=t(gid8, "channel"), value=getattr(message.channel, "mention", str(message.channel)), inline=True)
    no_text = "*(attachment)*" if intents.message_content else "*(текст недоступен без Message Content Intent)*"
    e.add_field(name=t(gid8, "text"),    value=message.content[:1020] or no_text,                         inline=False)

    # Если к сообщению была прикреплена ветка — добавляем ссылку
    if message.guild:
        try:
            thread = message.guild.get_thread(message.id)
            if thread:
                e.add_field(
                    name="🧵 Thread",
                    value=f"{thread.mention} — [перейти]({thread.jump_url})\n"
                          f"Сообщений в ветке: **{thread.message_count}**",
                    inline=False
                )
        except Exception:
            pass
        # Discord также хранит thread в flags/channel если message.thread
        if hasattr(message, 'thread') and message.thread:
            thread = message.thread
            try:
                e.add_field(
                    name="🧵 Thread",
                    value=f"{thread.mention} — [перейти]({thread.jump_url})\n"
                          f"Сообщений в ветке: **{thread.message_count}**",
                    inline=False
                )
            except Exception:
                pass

    queue_log(ch, e)

@bot.event
async def on_message_edit(before, after):
    if before.author.bot or not before.guild or before.content == after.content: return
    ch = await sec_check(before.guild, "msg_edit")
    if not ch: return
    gid9 = before.guild.id
    e = build_embed(C.WARNING)
    e.set_author(name=t(gid9, "msg_edited"))
    e.add_field(name=t(gid9, "member"), value=before.author.mention,        inline=False)
    e.add_field(name=t(gid9, "was"),    value=before.content[:512] or "—",  inline=False)
    e.add_field(name=t(gid9, "now"),    value=after.content[:512] or "—",   inline=False)
    e.add_field(name="Link",            value=f"[Jump]({after.jump_url})",   inline=True)
    queue_log(ch, e)

@bot.event
async def on_invite_create(invite):
    _invite_cache[f"{invite.guild.id}:{invite.code}"] = invite.uses or 0
    print(f"[INVITE] Created: {invite.code} by {invite.inviter}")
    ch = await sec_check(invite.guild, "invites")
    if not ch: return
    gid10 = invite.guild.id
    e = build_embed(C.INFO)
    e.set_author(name=t(gid10, "invite_created"))
    e.add_field(name=t(gid10, "member"),  value=f"{invite.inviter.mention} · `{invite.inviter.name}`" if invite.inviter else "?", inline=True)
    e.add_field(name=t(gid10, "code"),    value=f"`{invite.code}`",                                                                inline=True)
    e.add_field(name=t(gid10, "uses"),    value=str(invite.max_uses) if invite.max_uses else "∞",                                  inline=True)
    e.add_field(name=t(gid10, "expires"), value=invite.expires_at.strftime("%d.%m.%Y %H:%M") if invite.expires_at else t(gid10, "never"), inline=True)
    queue_log(ch, e)

@bot.event
async def on_invite_delete(invite):
    _invite_cache.pop(f"{invite.guild.id}:{invite.code}", None)
    print(f"[INVITE] Deleted: {invite.code}")
    ch = await sec_check(invite.guild, "invites")
    if not ch: return
    gid11 = invite.guild.id
    e = build_embed(C.MUTED)
    e.set_author(name=t(gid11, "invite_deleted"))
    e.add_field(name=t(gid11, "code"), value=f"`{invite.code}`", inline=True)
    queue_log(ch, e)

@bot.event
async def on_close():
    """Вызывается при остановке бота"""
    await graceful_shutdown(bot)


@bot.event
async def on_guild_join(guild):
    """Когда бот добавляется на новый сервер"""
    await refresh_invite_cache(guild)
    # Онбординг — отправляем инструкцию владельцу
    try:
        owner = guild.owner
        if owner:
            e = discord.Embed(color=0x5865F2, timestamp=datetime.datetime.utcnow())
            e.set_author(name=f"Thanks for adding Witness to {guild.name}!",
                         icon_url=bot.user.display_avatar.url)
            e.description = (
                "Here's how to get started:\n\n"
                "**1.** Run `/setup` for guided configuration\n"
                "**2.** Run `/help` to see all commands\n"
                "**3.** Run `/security setlog #channel` to enable logging\n"
                "**4.** Run `/lang language:ru` if you prefer Russian\n\n"
                "**Free:** Albion stats · All games · Basic security\n"
                "**Premium €2.99:** AI · Black Market · Craft Calc\n"
                "**Security €4.99:** Advanced `/q` security tools\n\n"
                "Use `/setpremium` if you have a license key."
            )
            e.add_field(name="Support", value=SUPPORT_URL, inline=True)
            e.add_field(name="Docs", value="witnessbot.gg", inline=True)
            await owner.send(embed=e)
    except Exception:
        pass  # DM закрыты — не страшно


@bot.event
async def on_voice_state_update(member, before, after):
    ch = await sec_check(member.guild, "voice")
    if not ch or before.channel == after.channel: return
    if before.channel is None: desc, color = f"вошёл в **{after.channel.name}**", discord.Color.green()
    elif after.channel is None: desc, color = f"вышел из **{before.channel.name}**", discord.Color.red()
    else: desc, color = f"**{before.channel.name}** → **{after.channel.name}**", discord.Color.blue()
    gid12 = member.guild.id
    e = build_embed(color)
    e.set_author(name=t(gid12, "voice_update"))
    e.add_field(name=t(gid12, "member"), value=member.mention, inline=True)
    e.add_field(name=t(gid12, "action"), value=desc,           inline=True)
    queue_log(ch, e)

@bot.event
async def on_guild_channel_create(channel_created):
    ch = await sec_check(channel_created.guild, "channels")
    if not ch: return
    e = build_embed(C.SUCCESS)
    e.set_author(name="Channel created")
    e.add_field(name="Канал", value=channel_created.mention, inline=True)
    queue_log(ch, e)

@bot.event
async def on_guild_channel_delete(channel_deleted):
    ch = await sec_check(channel_deleted.guild, "channels")
    if not ch: return
    e = build_embed(C.DANGER)
    e.set_author(name="Channel deleted")
    e.add_field(name="Канал", value=channel_deleted.name, inline=True)
    await ch.send(embed=e)
    # Анти-нюк по удалению каналов — в witness/protection.py, по журналу аудита:
    # он работает и тогда, когда лог каналов выключен

@bot.event
async def on_guild_role_create(role):
    ch = await sec_check(role.guild, "roles")
    if not ch: return
    e = build_embed(C.SUCCESS)
    e.set_author(name="Role created")
    e.add_field(name="Роль", value=role.mention, inline=True)
    queue_log(ch, e)

@bot.event
async def on_guild_role_delete(role):
    ch = await sec_check(role.guild, "roles")
    if not ch: return
    e = build_embed(C.DANGER)
    e.set_author(name="Role deleted")
    e.add_field(name="Роль", value=role.name, inline=True)
    await ch.send(embed=e)
    # Анти-нюк по удалению ролей — в witness/protection.py, по журналу аудита

@bot.event
async def on_guild_update(before, after):
    ch = await sec_check(after, "server_edit")
    if not ch or before.name == after.name: return
    e = build_embed(C.INFO)
    e.set_author(name="Server updated")
    e.add_field(name="Было", value=before.name, inline=True)
    e.add_field(name="Стало", value=after.name, inline=True)
    await ch.send(embed=e)

@bot.event
async def on_user_update(before, after):
    if before.avatar == after.avatar: return
    for guild in bot.guilds:
        member = guild.get_member(after.id)
        if not member: continue
        ch = await sec_check(guild, "avatar_change")
        if not ch: continue
        e = build_embed(C.INFO)
        e.set_author(name="Avatar changed")
        e.add_field(name="Member", value=member.mention, inline=False)
        e.set_thumbnail(url=after.display_avatar.url)
        queue_log(ch, e)


@bot.event
async def on_thread_create(thread):
    ch = await sec_check(thread.guild, "threads")
    if not ch: return
    e = build_embed(C.SUCCESS)
    e.set_author(name="Thread created")
    e.add_field(name="Тред", value=thread.mention, inline=True)
    if thread.parent: e.add_field(name="Канал", value=thread.parent.mention, inline=True)
    queue_log(ch, e)

@bot.event
async def on_interaction(interaction):
    if not EVENT_LOGS: return
    if not await is_enabled(interaction.guild_id, "slash_commands"): return
    if interaction.type != discord.InteractionType.application_command: return
    ch = await get_log_ch(interaction.guild)
    if not ch: return
    e = build_embed(C.PRIMARY)
    e.set_author(name="Slash command")
    e.add_field(name="Пользователь", value=interaction.user.mention, inline=True)
    e.add_field(name="Команда", value=f"`/{interaction.data.get('name','?')}`", inline=True)
    await ch.send(embed=e)

@bot.event
async def on_reaction_add(reaction, user):
    if user.bot or not reaction.message.guild: return
    gid = reaction.message.guild.id

    # Starboard logic
    if str(reaction.emoji) == "⭐" and await get_tier(gid) >= TIER_PREMIUM:
        settings = await get_guild_settings(gid)
        sb_ch_id   = settings.get("starboard_channel", 0)
        threshold  = settings.get("starboard_threshold", 3)
        if sb_ch_id and reaction.count >= threshold:
            sb_ch = reaction.message.guild.get_channel(sb_ch_id)
            if not sb_ch: return
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT starboard_msg_id FROM starboard WHERE guild_id=? AND message_id=?",
                    (gid, reaction.message.id)
                ) as c:
                    existing = await c.fetchone()
            if existing: return  # уже добавлено
            msg = reaction.message
            e = discord.Embed(description=msg.content or "*(вложение)*", color=0xFFD700, timestamp=msg.created_at)
            e.set_author(name=msg.author.display_name, icon_url=msg.author.display_avatar.url)
            e.add_field(name="Источник", value=f"[Перейти]({msg.jump_url}) · {msg.channel.mention}", inline=False)
            if msg.attachments:
                e.set_image(url=msg.attachments[0].url)
            sb_msg = await sb_ch.send(content=f"⭐ **{reaction.count}** · {msg.channel.mention}", embed=e)
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT INTO starboard (guild_id, message_id, starboard_msg_id) VALUES (?,?,?)",
                    (gid, msg.id, sb_msg.id)
                )
                await db.commit()

    # Security reaction log (из существующего кода)
    if not reaction.message.guild: return
    ch = await sec_check(reaction.message.guild, "reactions")
    if not ch: return
    e = build_embed(C.SUCCESS)
    e.set_author(name="Reaction added")
    e.add_field(name="Пользователь", value=user.mention, inline=True)
    e.add_field(name="Реакция", value=str(reaction.emoji), inline=True)
    e.add_field(name="Сообщение", value=f"[Перейти]({reaction.message.jump_url})", inline=True)
    await ch.send(embed=e)



@bot.event
async def on_raw_reaction_add(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id: return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT role_id FROM reaction_roles WHERE guild_id=? AND message_id=? AND emoji=?",
            (payload.guild_id, payload.message_id, str(payload.emoji))
        ) as c:
            row = await c.fetchone()
    if not row: return
    guild = bot.get_guild(payload.guild_id)
    if not guild: return
    # При добавлении реакции Discord сам присылает участника — кэш не нужен
    member = payload.member or await resolve_member(guild, payload.user_id)
    if not member: return
    role = guild.get_role(row[0])
    if role:
        try:
            await member.add_roles(role, reason="Reaction role")
        except discord.Forbidden:
            pass


@bot.event
async def on_raw_reaction_remove(payload: discord.RawReactionActionEvent):
    if payload.user_id == bot.user.id: return
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute(
            "SELECT role_id FROM reaction_roles WHERE guild_id=? AND message_id=? AND emoji=?",
            (payload.guild_id, payload.message_id, str(payload.emoji))
        ) as c:
            row = await c.fetchone()
    if not row: return
    guild = bot.get_guild(payload.guild_id)
    if not guild: return
    member = await resolve_member(guild, payload.user_id)
    if not member: return
    role = guild.get_role(row[0])
    if role:
        try:
            await member.remove_roles(role, reason="Reaction role removed")
        except discord.Forbidden:
            pass


@bot.tree.error
async def on_app_command_error(interaction: discord.Interaction,
                                error: app_commands.AppCommandError):
    """Глобальный обработчик ошибок slash команд"""
    msg = None
    if isinstance(error, app_commands.CommandOnCooldown):
        msg = f"⏳ Cooldown: {error.retry_after:.1f}s"
    elif isinstance(error, app_commands.MissingPermissions):
        msg = "❌ Недостаточно прав для этой команды."
    elif isinstance(error, app_commands.BotMissingPermissions):
        msg = f"❌ У бота нет прав: {', '.join(error.missing_permissions)}"
    elif isinstance(error, app_commands.CommandInvokeError):
        inner = error.original
        if isinstance(inner, discord.Forbidden):
            msg = "❌ Нет прав выполнить это действие."
        elif isinstance(inner, discord.NotFound):
            msg = "❌ Объект не найден (удалён или недоступен)."
        else:
            msg = f"❌ Ошибка: {str(inner)[:200]}"
            print(f"[ERROR] Command error: {type(inner).__name__}: {inner}")
    else:
        msg = f"❌ {str(error)[:200]}"

    if msg:
        try:
            if interaction.response.is_done():
                await interaction.followup.send(msg, ephemeral=True)
            else:
                await interaction.response.send_message(msg, ephemeral=True)
        except Exception as ex:
            print(f"[CMD_ERROR] Не удалось отправить сообщение об ошибке: {ex} · исходная ошибка: {error}")
