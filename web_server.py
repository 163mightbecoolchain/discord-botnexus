"""
Witness Web Server
━━━━━━━━━━━━━━━━━
Routes:
  GET  /                         — Landing page
  GET  /dashboard                — Dashboard
  GET  /login                    — Discord OAuth2 redirect
  GET  /callback                 — OAuth2 callback
  GET  /logout                   — Clear session
  GET  /api/refresh              — Refresh token (auto-login)

  GET  /api/stats                — Bot stats (public)
  GET  /api/me                   — Current user
  GET  /api/guilds               — User guilds with bot
  GET  /api/guild/:id/settings   — All guild settings
  POST /api/guild/:id/settings   — Save guild settings (all fields)
  POST /api/guild/:id/security   — Save security toggles
  GET  /api/guild/:id/channels   — Channels + roles
  GET  /api/guild/:id/modlog     — Mod log
  GET  /api/guild/:id/invites    — Invite stats + search
  GET  /api/guild/:id/appeals    — Appeals list
"""

import os, hmac, json, time, hashlib, asyncio, aiohttp, aiofiles, aiosqlite, datetime, traceback
from pathlib import Path
from aiohttp import web

# ── Config ────────────────────────────────────────────────────
BOT_ID        = os.getenv("BOT_ID", "")
CLIENT_SECRET = os.getenv("DISCORD_CLIENT_SECRET", "")
HMAC_KEY      = os.getenv("HMAC_SECRET", "witness_secret").encode()
PORT          = int(os.getenv("PORT", 8080))
def _norm_url(v: str) -> str:
    """Приводит адрес к виду https://host без слеша на конце"""
    v = (v or "").strip().rstrip("/")
    if v and not v.startswith(("http://", "https://")):
        v = "https://" + v
    return v

# ВАЖНО: SITE_URL — это адрес ЭТОГО сервиса (бота), потому что именно здесь
# живёт /callback. Discord вернёт пользователя сюда после авторизации.
SITE_URL      = _norm_url(os.getenv("SITE_URL", f"http://localhost:{PORT}"))
# WEBSITE_URL — адрес отдельного сервиса с сайтом (лендинг + дашборд)
WEBSITE_URL   = _norm_url(os.getenv("WEBSITE_URL", ""))
# Права при приглашении: только то, чем бот пользуется (без Administrator).
# Кик, бан, таймаут, роли (карантин, реакции), каналы (тикеты, slowmode),
# сервер (AutoMod, инвайты), журнал аудита, сообщения, вложения (бэкапы).
# Тот же набор — в witness/config.py и website/app.py.
INVITE_PERMISSIONS = 1374658358518
INVITE_URL    = (f"https://discord.com/api/oauth2/authorize?client_id={BOT_ID}"
                 f"&permissions={INVITE_PERMISSIONS}&scope=bot%20applications.commands") if BOT_ID else "#"
SUPPORT_URL   = os.getenv("SUPPORT_URL", "https://discord.gg/witness")
DB_PATH       = os.getenv("DB_PATH", "witnessbot.db")
TEMPLATES_DIR = Path(__file__).parent / "witness_web" / "templates"

OAUTH_URL = (f"https://discord.com/api/oauth2/authorize?client_id={BOT_ID}"
             f"&redirect_uri={SITE_URL}/callback&response_type=code&scope=identify+guilds")

# ── Session ───────────────────────────────────────────────────

def sign_session(data: dict) -> str:
    import base64
    payload = json.dumps(data, separators=(',', ':'))
    sig     = hmac.new(HMAC_KEY, payload.encode(), hashlib.sha256).hexdigest()
    return base64.urlsafe_b64encode(payload.encode()).decode() + "." + sig

def verify_session(token: str) -> dict | None:
    import base64
    try:
        parts = token.split('.')
        if len(parts) != 2: return None
        encoded, sig = parts
        payload  = base64.urlsafe_b64decode(encoded.encode()).decode()
        expected = hmac.new(HMAC_KEY, payload.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected): return None
        data = json.loads(payload)
        # Токен живёт 30 дней
        if time.time() - data.get('ts', 0) > 30 * 86400: return None
        return data
    except Exception:
        return None

def get_session(request: web.Request) -> dict | None:
    auth = request.headers.get('Authorization', '')
    if auth.startswith('Bearer '):
        return verify_session(auth[7:])
    token = request.cookies.get('ws_session')
    return verify_session(token) if token else None

def require_auth(func):
    async def wrapper(request):
        session = get_session(request)
        if not session:
            if request.path.startswith('/api/'):
                return web.json_response({'error': 'unauthorized'}, status=401)
            raise web.HTTPFound('/login')
        request['session'] = session
        return await func(request)
    return wrapper

def _server_error(request, e: Exception):
    """500 без подробностей наружу: текст исключения (пути, SQL) — только в лог"""
    print(f"[API] {request.method} {request.path} · {type(e).__name__}: {e}")
    traceback.print_exception(e)
    return web.json_response({'error': 'internal_error'}, status=500)

# ── CORS ──────────────────────────────────────────────────────

def _add_cors(resp, origin: str):
    # discordsays.com — домен, с которого Discord отдаёт Activity в iframe.
    # Без него запросы из встроенного приложения блокируются браузером.
    allowed = (not origin or origin.endswith('.railway.app') or
               origin.endswith('.discordsays.com') or
               origin in ('https://discord.com', 'https://canary.discord.com',
                          'https://ptb.discord.com') or
               origin in [SITE_URL, WEBSITE_URL])
    if allowed and origin:
        try:
            resp.headers["Access-Control-Allow-Origin"]      = origin
            resp.headers["Access-Control-Allow-Credentials"] = "true"
            resp.headers["Access-Control-Allow-Methods"]     = "GET, POST, OPTIONS"
            resp.headers["Access-Control-Allow-Headers"]     = "Content-Type, Authorization"
            resp.headers["Access-Control-Max-Age"]           = "86400"
        except Exception:
            pass

@web.middleware
async def cors_middleware(request: web.Request, handler):
    origin = request.headers.get("Origin", "")
    if request.method == "OPTIONS":
        resp = web.Response(status=200)
        _add_cors(resp, origin)
        return resp
    try:
        resp = await handler(request)
    except web.HTTPException as e:
        _add_cors(e, origin)
        raise
    except Exception as e:
        resp = _server_error(request, e)
    _add_cors(resp, origin)
    return resp

# ── Templates ─────────────────────────────────────────────────

async def read_template(name: str) -> str:
    path = TEMPLATES_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"Template not found: {name}")
    async with aiofiles.open(path) as f:
        html = await f.read()
    html = html.replace('__INVITE_URL__',  INVITE_URL)
    html = html.replace('__SUPPORT_URL__', SUPPORT_URL)
    html = html.replace('__BOT_ID__',      BOT_ID)
    return html

# ── OAuth2 ────────────────────────────────────────────────────

async def exchange_code(code: str) -> dict | None:
    async with aiohttp.ClientSession() as s:
        async with s.post('https://discord.com/api/oauth2/token', data={
            'client_id':     BOT_ID,
            'client_secret': CLIENT_SECRET,
            'grant_type':    'authorization_code',
            'code':          code,
            'redirect_uri':  f"{SITE_URL}/callback",
        }) as r:
            return await r.json() if r.status == 200 else None

async def get_discord_user(access_token: str) -> dict | None:
    async with aiohttp.ClientSession() as s:
        async with s.get('https://discord.com/api/users/@me',
                         headers={'Authorization': f'Bearer {access_token}'}) as r:
            return await r.json() if r.status == 200 else None

async def get_discord_guilds(access_token: str) -> list:
    async with aiohttp.ClientSession() as s:
        async with s.get('https://discord.com/api/users/@me/guilds',
                         headers={'Authorization': f'Bearer {access_token}'}) as r:
            return await r.json() if r.status == 200 else []

async def refresh_discord_token(refresh_token: str) -> dict | None:
    async with aiohttp.ClientSession() as s:
        async with s.post('https://discord.com/api/oauth2/token', data={
            'client_id':     BOT_ID,
            'client_secret': CLIENT_SECRET,
            'grant_type':    'refresh_token',
            'refresh_token': refresh_token,
        }) as r:
            return await r.json() if r.status == 200 else None

# ── Page handlers ─────────────────────────────────────────────

async def handle_index(request):
    # Корень бот-API: отправляем людей на сайт
    if WEBSITE_URL:
        base = WEBSITE_URL if WEBSITE_URL.startswith('http') else f'https://{WEBSITE_URL}'
        raise web.HTTPFound(base)
    try:
        return web.Response(text=await read_template('index.html'), content_type='text/html')
    except FileNotFoundError:
        return web.Response(text='<h1>Witness Bot</h1>', content_type='text/html')

async def handle_dashboard(request):
    session = get_session(request)
    if not session:
        raise web.HTTPFound('/login')
    try:
        return web.Response(text=await read_template('dashboard.html'), content_type='text/html')
    except FileNotFoundError:
        return web.Response(text='<h1>Dashboard</h1>', content_type='text/html')

async def handle_login(request):
    if not BOT_ID or not CLIENT_SECRET:
        return web.Response(text='OAuth2 not configured.', status=500)
    raise web.HTTPFound(OAUTH_URL)

async def handle_callback(request):
    code  = request.rel_url.query.get('code')
    error = request.rel_url.query.get('error')
    if error or not code:
        raise web.HTTPFound('/?error=oauth_denied')

    token_data = await exchange_code(code)
    if not token_data or 'access_token' not in token_data:
        raise web.HTTPFound('/?error=oauth_failed')

    user = await get_discord_user(token_data['access_token'])
    if not user:
        raise web.HTTPFound('/?error=user_fetch_failed')

    session_data = {
        'user_id':       user['id'],
        'username':      user['username'],
        'discriminator': user.get('discriminator', '0'),
        'avatar':        user.get('avatar'),
        'access_token':  token_data['access_token'],
        'refresh_token': token_data.get('refresh_token', ''),
        'ts':            time.time(),
    }
    token = sign_session(session_data)

    if WEBSITE_URL:
        import urllib.parse
        base = WEBSITE_URL if WEBSITE_URL.startswith('http') else f'https://{WEBSITE_URL}'
        # Токен во фрагменте (#): браузер не отправляет его на сервер,
        # поэтому он не попадает в логи запросов и в Referer
        raise web.HTTPFound(f"{base}/dashboard#token={urllib.parse.quote(token)}")
    else:
        response = web.HTTPFound("/dashboard")
        response.set_cookie('ws_session', token, max_age=30*86400,
                            httponly=True, samesite='Lax', path='/')
        raise response

async def handle_logout(request):
    # После логаута возвращаем на сайт (если он на отдельном домене)
    if WEBSITE_URL:
        base = WEBSITE_URL if WEBSITE_URL.startswith('http') else f'https://{WEBSITE_URL}'
        response = web.HTTPFound(base)
    else:
        response = web.HTTPFound('/')
    response.del_cookie('ws_session', path='/')
    raise response

# ── API: refresh token ────────────────────────────────────────

@require_auth
async def api_refresh(request):
    """Обновляет Discord токен и возвращает новый session token"""
    s = request['session']
    refresh_token = s.get('refresh_token', '')
    if not refresh_token:
        return web.json_response({'error': 'no_refresh_token'}, status=400)

    try:
        new_tokens = await refresh_discord_token(refresh_token)
        if not new_tokens or 'access_token' not in new_tokens:
            return web.json_response({'error': 'refresh_failed'}, status=401)

        session_data = {
            **s,
            'access_token':  new_tokens['access_token'],
            'refresh_token': new_tokens.get('refresh_token', refresh_token),
            'ts':            time.time(),
        }
        new_token = sign_session(session_data)
        return web.json_response({'token': new_token, 'ok': True})
    except Exception as e:
        return _server_error(request, e)

# ── API: stats ────────────────────────────────────────────────

async def api_stats(request):
    try:
        bot    = request.app['bot']
        guilds = len(bot.guilds)
        members = sum(g.member_count or 0 for g in bot.guilds)
        return web.json_response({
            'guilds':  guilds,
            'members': members,
            'latency': round(bot.latency * 1000),
            'status':  'online' if bot.is_ready() else 'connecting',
        })
    except Exception as e:
        return _server_error(request, e)

# ── API: me ───────────────────────────────────────────────────

@require_auth
async def api_me(request):
    s = request['session']
    return web.json_response({
        'id':            s['user_id'],
        'username':      s['username'],
        'discriminator': s['discriminator'],
        'avatar':        s['avatar'],
    })

# ── API: guilds ───────────────────────────────────────────────

@require_auth
async def api_guilds(request):
    s   = request['session']
    bot = request.app['bot']
    try:
        user_guilds = await get_discord_guilds(s['access_token'])
    except Exception:
        return web.json_response([], status=200)

    bot_guild_ids = {g.id for g in bot.guilds}
    result = []
    for ug in user_guilds:
        gid  = int(ug['id'])
        perms = int(ug.get('permissions', 0))
        if gid not in bot_guild_ids: continue
        # 0x8 Admin · 0x20 Manage Guild · 0x2000 Manage Messages · 0x10000000000 Moderate Members
        # 0x4 Ban Members — им нужны апелляции и заявки на бан
        is_admin = bool(perms & 0x8 or perms & 0x20)
        is_mod   = bool(perms & 0x2000 or perms & 0x10000000000 or perms & 0x4)
        if not (is_admin or is_mod): continue
        tier = 0
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute("SELECT tier FROM subscriptions WHERE guild_id=?", (gid,)) as c:
                    row = await c.fetchone()
                    if row: tier = row[0]
        except Exception:
            pass
        bg = bot.get_guild(gid)
        result.append({
            'can_manage': is_admin,
            'perms':      str(perms),  # строкой — биты выше 2^53 не влезают в JS number
            'id':           ug['id'],
            'name':         ug['name'],
            'icon':         ug.get('icon'),
            'member_count': bg.member_count if bg else None,
            'tier':         tier,
        })
    return web.json_response(result)

async def _get_member(guild, user_id):
    """
    Участник сервера: из кэша, а если его там нет — запросом к Discord.
    Без Server Members Intent кэш почти пустой, и проверка прав в панели
    отказывала бы всем администраторам.
    """
    from witness.core import resolve_member
    return await resolve_member(guild, user_id)

_name_cache: dict = {}   # (guild_id, user_id) → (имя или None, время)
_name_jobs: dict = {}    # (guild_id, user_id) → задача, которая сейчас узнаёт имя
_user_sem = None         # лимит одновременных запросов профилей ушедших
NAME_FRESH = 600         # 10 мин: старше — показываем, но обновляем в фоне


async def _load_names(bot, bg, uids: list):
    """
    Ники пачками до 100 через gateway (query_members) — это не REST,
    и лимиты, на которых висит проверка прав модератора в каждом запросе
    панели, не тратятся. Раньше на каждого участника уходил отдельный
    GET /guilds/…/members/…: после перезапуска бота сотни таких запросов
    вставали в общую очередь, и панель зависала на минуты.
    Ушедшим (и забаненным) — глобальное имя через /users (другой лимит).
    """
    global _user_sem
    if _user_sem is None:
        _user_sem = asyncio.Semaphore(3)
    try:
        found = {}
        for i in range(0, len(uids), 100):
            chunk = uids[i:i + 100]
            try:
                for m in await bg.query_members(user_ids=chunk, limit=100, cache=False):
                    found[m.id] = m.display_name
            except Exception as ex:
                print(f"[NAMES] query_members: {ex}")
        now = time.time()
        for uid, name in found.items():
            _name_cache[(bg.id, uid)] = (name, now)

        async def from_profile(uid):
            async with _user_sem:
                try:
                    u = await bot.fetch_user(uid)
                    name = u.global_name or u.name
                except Exception:
                    name = None
            _name_cache[(bg.id, uid)] = (name, time.time())
        await asyncio.gather(*(from_profile(u) for u in uids if u not in found))
    finally:
        for uid in uids:
            _name_jobs.pop((bg.id, uid), None)


def _start_names(bot, bg, uids):
    """Одна фоновая задача на пачку; уже запрошенные ID не дублируем"""
    new = [u for u in uids if (bg.id, u) not in _name_jobs]
    if new:
        task = asyncio.create_task(_load_names(bot, bg, new))
        for u in new:
            _name_jobs[(bg.id, u)] = task
    return {_name_jobs[(bg.id, u)] for u in uids if (bg.id, u) in _name_jobs}


async def _user_names(bot, bg, ids) -> dict:
    """
    ID → имя для показа в панели. Без Server Members Intent в кэше бота
    почти никого нет, и вместо модераторов в Mod Log стояли ID/прочерки.

    - есть в кэше сервера — текущий ник, без запросов;
    - запомнено меньше 10 минут назад — берём из памяти;
    - запомнено давно — показываем сразу и обновляем в фоне,
      так смена ника доходит до панели за ~10 минут без ожидания;
    - не встречали — узнаём (см. _load_names), но ждём не дольше 2 секунд;
      кто не успел, появится при следующем открытии.
    """
    out, missing, stale, now = {}, [], [], time.time()
    for uid in {int(i) for i in ids if i and str(i).isdigit() and int(i)}:
        m = bg.get_member(uid)
        if m:
            out[uid] = m.display_name
            continue
        hit = _name_cache.get((bg.id, uid))
        if hit:
            out[uid] = hit[0]
            if now - hit[1] >= NAME_FRESH:
                stale.append(uid)                # обновим в фоне, не ждём
            continue
        missing.append(uid)

    if stale:
        _start_names(bot, bg, stale)
    if missing:
        jobs = _start_names(bot, bg, missing)
        if jobs:
            await asyncio.wait(jobs, timeout=2)
    for uid in missing:
        hit = _name_cache.get((bg.id, uid))
        out[uid] = hit[0] if hit else None
    return out

# ── API: guild settings GET ───────────────────────────────────

@require_auth
async def api_guild_settings_get(request):
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Guild not found'}, status=404)
    member = await _get_member(bg, s['user_id'])
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'Forbidden'}, status=403)

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            # guild_settings
            async with db.execute("SELECT * FROM guild_settings WHERE guild_id=?", (guild_id,)) as c:
                row = await c.fetchone()
                gs  = dict(zip([d[0] for d in c.description], row)) if row else {}

            # security: log_channel + toggles
            async with db.execute(
                "SELECT log_channel, settings FROM security_settings WHERE guild_id=?",
                (guild_id,)
            ) as c:
                sec_row = await c.fetchone()

            # punishment_settings (3-warn system)
            async with db.execute(
                "SELECT mute1_days, mute2_days, ban3_days, COALESCE(warn3_type,'ban') "
                "FROM punishment_settings WHERE guild_id=?",
                (guild_id,)
            ) as c:
                ps_row = await c.fetchone()

            # quarantine_settings
            async with db.execute(
                "SELECT role_id, duration_hours, min_age_days, enabled FROM quarantine_settings WHERE guild_id=?",
                (guild_id,)
            ) as c:
                qs_row = await c.fetchone()

        # С умолчаниями, как их видит бот (get_security) — иначе включённые
        # по умолчанию логи показывались в панели выключенными
        from witness.database import DEFAULT_SEC
        sec_settings = {**DEFAULT_SEC,
                        **(json.loads(sec_row[1] or '{}') if sec_row else {})}

        return web.json_response({
            # guild_settings
            'lang':               gs.get('lang', 'ru'),
            'tickets_enabled':    gs.get('tickets_enabled', 1),
            'lockdown':           gs.get('lockdown', 0),
            'starboard_threshold':gs.get('starboard_threshold', 3),
            'starboard_channel':  gs.get('starboard_channel', 0),
            'suggestion_channel': gs.get('suggestion_channel', 0),
            'ticket_category':    gs.get('ticket_category', 0),
            'birthday_channel':   gs.get('birthday_channel', 0),
            # security
            'log_channel':        sec_row[0] if sec_row else 0,
            'security':           sec_settings,
            # punishment (3-warn)
            'mute1_days':         ps_row[0] if ps_row else 7,
            'mute2_days':         ps_row[1] if ps_row else 7,
            'ban3_days':          ps_row[2] if ps_row else 30,
            'warn3_type':         ps_row[3] if ps_row else 'ban',
            # quarantine
            'quarantine_enabled':     qs_row[3] if qs_row else 0,
            'quarantine_role_id':     qs_row[0] if qs_row else 0,
            'quarantine_hours':       qs_row[1] if qs_row else 24,
            'quarantine_min_age':     qs_row[2] if qs_row else 7,
        })
    except Exception as e:
        return _server_error(request, e)

# ── API: guild settings POST ──────────────────────────────────

@require_auth
async def api_guild_settings_post(request):
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Not found'}, status=404)
    member = await _get_member(bg, s['user_id'])
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'Forbidden'}, status=403)

    try:
        data = await request.json()
        async with aiosqlite.connect(DB_PATH) as db:

            # ── guild_settings ─────────────────────────────────
            lang = data.get('lang')
            gs_fields = {
                'lang': lang if lang in ('ru', 'en', 'de') else None,
                'tickets_enabled': data.get('tickets_enabled'),
                'lockdown': data.get('lockdown'),
                'starboard_threshold': data.get('starboard_threshold'),
                'starboard_channel': data.get('starboard_channel'),
                'suggestion_channel': data.get('suggestion_channel'),
                'ticket_category': data.get('ticket_category'),
                'birthday_channel': data.get('birthday_channel'),
            }
            for key, val in gs_fields.items():
                if val is not None:
                    await db.execute(f"""
                        INSERT INTO guild_settings (guild_id, {key}) VALUES (?, ?)
                        ON CONFLICT(guild_id) DO UPDATE SET {key}=excluded.{key}
                    """, (guild_id, val))

            # ── security: log_channel ──────────────────────────
            if 'log_channel' in data:
                await db.execute("""
                    INSERT INTO security_settings (guild_id, log_channel)
                    VALUES (?, ?)
                    ON CONFLICT(guild_id) DO UPDATE SET log_channel=excluded.log_channel
                """, (guild_id, int(data['log_channel'] or 0)))

            # ── punishment_settings (3-warn) ───────────────────
            mute1 = data.get('mute1_days')
            mute2 = data.get('mute2_days')
            ban3  = data.get('ban3_days')
            w3t   = data.get('warn3_type')
            if any(v is not None for v in [mute1, mute2, ban3, w3t]):
                # Читаем текущие значения
                async with db.execute(
                    "SELECT mute1_days, mute2_days, ban3_days, COALESCE(warn3_type,'ban') "
                    "FROM punishment_settings WHERE guild_id=?",
                    (guild_id,)
                ) as c:
                    cur = await c.fetchone()
                cur = cur or (7, 7, 30, 'ban')
                new_mute1 = int(mute1) if mute1 is not None else cur[0]
                new_mute2 = int(mute2) if mute2 is not None else cur[1]
                new_ban3  = int(ban3)  if ban3  is not None else cur[2]
                new_w3t   = w3t        if w3t  in ('ban','mute','request') else cur[3]
                # Ограничения
                new_mute1 = max(1, min(27, new_mute1))
                new_mute2 = max(1, min(27, new_mute2))
                new_ban3  = max(1, new_ban3)
                now = datetime.datetime.utcnow().isoformat()
                await db.execute("""
                    INSERT INTO punishment_settings (guild_id, mute1_days, mute2_days, ban3_days, warn3_type, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(guild_id) DO UPDATE SET
                        mute1_days=excluded.mute1_days,
                        mute2_days=excluded.mute2_days,
                        ban3_days=excluded.ban3_days,
                        warn3_type=excluded.warn3_type,
                        updated_at=excluded.updated_at
                """, (guild_id, new_mute1, new_mute2, new_ban3, new_w3t, now))

            # ── quarantine_settings ────────────────────────────
            q_enabled = data.get('quarantine_enabled')
            q_role    = data.get('quarantine_role_id')
            q_hours   = data.get('quarantine_hours')
            q_age     = data.get('quarantine_min_age')
            if any(v is not None for v in [q_enabled, q_role, q_hours, q_age]):
                async with db.execute(
                    "SELECT role_id, duration_hours, min_age_days, enabled FROM quarantine_settings WHERE guild_id=?",
                    (guild_id,)
                ) as c:
                    cur_q = await c.fetchone()
                cur_q = cur_q or (0, 24, 7, 0)
                await db.execute("""
                    INSERT INTO quarantine_settings (guild_id, role_id, duration_hours, min_age_days, enabled)
                    VALUES (?, ?, ?, ?, ?)
                    ON CONFLICT(guild_id) DO UPDATE SET
                        role_id=excluded.role_id,
                        duration_hours=excluded.duration_hours,
                        min_age_days=excluded.min_age_days,
                        enabled=excluded.enabled
                """, (
                    guild_id,
                    int(q_role)    if q_role    is not None else cur_q[0],
                    int(q_hours)   if q_hours   is not None else cur_q[1],
                    int(q_age)     if q_age     is not None else cur_q[2],
                    int(q_enabled) if q_enabled is not None else cur_q[3],
                ))

            await db.commit()

        # Инвалидируем кэш настроек в боте (если используется)
        try:
            bot_ref = request.app.get('bot')
            if bot_ref and hasattr(bot_ref, '_settings_cache'):
                bot_ref._settings_cache.pop(guild_id, None)
        except Exception:
            pass

        return web.json_response({'ok': True})
    except Exception as e:
        return _server_error(request, e)

# ── API: security toggles POST ────────────────────────────────

@require_auth
async def api_security_save(request):
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Not found'}, status=404)
    member = await _get_member(bg, s['user_id'])
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'Forbidden'}, status=403)
    try:
        data = await request.json()
        if not isinstance(data, dict):
            return web.json_response({'error': 'Bad request'}, status=400)
        # Дописываем пришедшие переключатели к текущим настройкам, а не
        # заменяем JSON целиком: раньше устаревшая копия со страницы логов
        # затирала anti_raid / lockdown_min_age, сохранённые в «Защите»
        from witness.database import get_security, save_security, DEFAULT_SEC
        log_ch, sec = await get_security(guild_id)
        for k, v in data.items():
            if k in DEFAULT_SEC:
                sec[k] = bool(v)
        await save_security(guild_id, log_ch, sec)
        return web.json_response({'ok': True, 'security': sec})
    except Exception as e:
        return _server_error(request, e)

@require_auth
async def api_security_overview(request):
    """GET /api/guild/:id/security/overview — правила AutoMod Witness и последние угрозы"""
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Not found'}, status=404)
    member = await _get_member(bg, s['user_id'])
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'Forbidden'}, status=403)

    # Правила создаёт witness/automod.py; без Manage Server их нет и прочитать нельзя
    from witness.core import intents
    from witness.database import get_log_ch, get_threat_config
    log_ch  = await get_log_ch(bg)
    automod = {'can_manage': bg.me.guild_permissions.manage_guild, 'rules': [],
               'foreign_spam': False}
    if automod['can_manage']:
        try:
            for r in await bg.fetch_automod_rules():
                if r.creator_id != bot.user.id or not r.name.startswith("Witness · "):
                    # Правило спама может быть одно: чужое займёт место нашего
                    if r.trigger.type.name == 'spam':
                        automod['foreign_spam'] = True
                    continue
                kinds = {a.type.name for a in r.actions}
                automod['rules'].append({
                    'name':    r.name.removeprefix("Witness · "),
                    'enabled': r.enabled,
                    'blocks':  'block_message' in kinds,
                    'alerts':  'send_alert_message' in kinds,
                })
        except Exception as ex:
            print(f"[SECURITY] automod rules: {ex}")

    alerts, week, open_count = [], 0, 0
    try:
        since = (datetime.datetime.utcnow() - datetime.timedelta(days=7)).isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT COUNT(*), COALESCE(SUM(resolved=0),0) FROM security_alerts "
                "WHERE guild_id=? AND created_at>=?",
                (guild_id, since)) as c:
                week, open_count = await c.fetchone()
            async with db.execute("""
                SELECT id, alert_type, severity, user_id, description, metadata, resolved, created_at
                FROM security_alerts WHERE guild_id=?
                ORDER BY id DESC LIMIT 100
            """, (guild_id,)) as c:
                rows = await c.fetchall()
        names = await _user_names(bot, bg, [r[3] for r in rows])
        for aid, atype, sev, uid, desc, meta, resolved, ts in rows:
            try:
                meta = json.loads(meta or '{}')
            except ValueError:
                meta = {}
            alerts.append({
                'id':          aid,
                'resolved':    bool(resolved),
                'type':        atype,
                'severity':    sev,
                'user_id':     str(uid or ''),
                'user_name':   names.get(uid) if uid else None,
                'description': desc,
                'source':      meta.get('source', 'bot'),
                'matched':     meta.get('matched'),
                'created_at':  ts,
            })
    except Exception as ex:
        print(f"[SECURITY] alerts: {ex}")

    return web.json_response({
        'automod':   automod,
        'threats':   await get_threat_config(guild_id),
        'log_channel': str(log_ch.id) if log_ch else None,
        'message_content': intents.message_content,
        'alerts':    alerts,
        'alerts_7d': week,
        'alerts_open_7d': open_count,
    })

@require_auth
async def api_threats_save(request):
    """POST /api/guild/:id/threats — режимы {phishing|suspicious|spam|custom: off|alert|block},
    exempt_roles / exempt_channels (id) и custom_words (свои слова)."""
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Not found'}, status=404)
    member = await _get_member(bg, s['user_id'])
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'Forbidden'}, status=403)
    try:
        data = await request.json()
        if not isinstance(data, dict):
            return web.json_response({'error': 'Bad request'}, status=400)
        from witness.database import save_threats
        from witness.automod import sync_guild, check_custom_words
        # Роли и каналы — только существующие на этом сервере
        if isinstance(data.get('exempt_roles'), list):
            data['exempt_roles'] = [str(r) for r in data['exempt_roles']
                                    if str(r).isdigit() and bg.get_role(int(r))]
        if isinstance(data.get('exempt_channels'), list):
            data['exempt_channels'] = [str(c) for c in data['exempt_channels']
                                       if str(c).isdigit() and bg.get_channel(int(c))]
        # Слова, которые задели бы обычные ссылки, не сохраняем — возвращаем список
        if isinstance(data.get('custom_words'), list):
            good, bad = check_custom_words(data['custom_words'])
            if bad:
                return web.json_response({
                    'error': 'bad_words',
                    'bad_words': [{'word': w, 'reason': r} for w, r in bad],
                }, status=400)
            data['custom_words'] = good
        modes = await save_threats(guild_id, data)
        # Применяем сразу, не дожидаясь планового обновления правил раз в 30 минут
        await sync_guild(bg)
        return web.json_response({'ok': True, 'threats': modes})
    except Exception as e:
        return _server_error(request, e)

async def _protection_payload(bg):
    from witness.core import get_tier
    from witness.config import TIER_PREMIUM
    from witness.database import get_protection, get_security
    from witness.protection import lockdown_active
    cfg = await get_protection(bg.id)
    _, sec = await get_security(bg.id)
    return {
        **cfg,
        'raid_enabled': bool(sec.get('anti_raid')),
        'premium':      await get_tier(bg.id) >= TIER_PREMIUM,
        'lockdown':     await lockdown_active(bg),
        'lockdown_min_age': int(sec.get('lockdown_min_age', 7) or 7),
    }

@require_auth
async def api_protection(request):
    """GET/POST /api/guild/:id/protection — анти-нюк, анти-рейд, сгорание варнов."""
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Not found'}, status=404)
    member = await _get_member(bg, s['user_id'])
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'Forbidden'}, status=403)
    if request.method == 'GET':
        return web.json_response(await _protection_payload(bg))
    try:
        data = await request.json()
        if not isinstance(data, dict):
            return web.json_response({'error': 'Bad request'}, status=400)
        from witness.database import save_protection, get_security, save_security
        data.pop('lockdown_until', None)            # служебное поле, из дашборда не меняется
        if isinstance(data.get('trusted_roles'), list):
            data['trusted_roles'] = [str(r) for r in data['trusted_roles']
                                     if str(r).isdigit() and bg.get_role(int(r))]
        # Канал-ловушка должен быть текстовым каналом этого сервера
        if 'trap_channel' in data:
            import discord
            ch_id = str(data['trap_channel'] or '')
            if not (ch_id.isdigit() and isinstance(bg.get_channel(int(ch_id)), discord.TextChannel)):
                data['trap_channel'] = ''
        await save_protection(guild_id, data)
        # Включение анти-рейда и возраст для локдауна живут в security_settings —
        # там же их читают /lockdown и переключатели логов
        if 'raid_enabled' in data or 'lockdown_min_age' in data:
            log_ch, sec = await get_security(guild_id)
            if 'raid_enabled' in data:
                sec['anti_raid'] = bool(data['raid_enabled'])
            if 'lockdown_min_age' in data:
                try:
                    sec['lockdown_min_age'] = max(1, min(365, int(data['lockdown_min_age'])))
                except (TypeError, ValueError):
                    pass
            await save_security(guild_id, log_ch, sec)
        return web.json_response({'ok': True, **(await _protection_payload(bg))})
    except Exception as e:
        return _server_error(request, e)

@require_auth
async def api_alert_resolve(request):
    """POST /api/guild/:id/alert/:alert_id/resolve — {"resolved": true|false}"""
    guild_id = int(request.match_info['guild_id'])
    alert_id = int(request.match_info['alert_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Not found'}, status=404)
    member = await _get_member(bg, s['user_id'])
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'Forbidden'}, status=403)
    try:
        data = await request.json()
    except Exception:
        data = {}
    resolved = 1 if (data or {}).get('resolved', True) else 0
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("UPDATE security_alerts SET resolved=? WHERE id=? AND guild_id=?",
                               (resolved, alert_id, guild_id))
        await db.commit()
    if not cur.rowcount:
        return web.json_response({'error': 'Not found'}, status=404)
    return web.json_response({'ok': True, 'resolved': bool(resolved)})

# ── API: channels ─────────────────────────────────────────────

@require_auth
async def api_channels(request):
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response([], status=200)
    member = await _get_member(bg, s['user_id'])
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'Forbidden'}, status=403)

    channels = [{'id': str(c.id), 'name': c.name, 'type': str(c.type)}
                for c in sorted(bg.channels, key=lambda x: x.position)
                if str(c.type) in ('text', 'category')]
    roles    = [{'id': str(r.id), 'name': r.name}
                for r in bg.roles[1:]]
    return web.json_response({'channels': channels, 'roles': roles})

# ── API: modlog ───────────────────────────────────────────────

@require_auth
async def api_modlog(request):
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response([])
    member = await _get_member(bg, s['user_id'])
    if not member or not member.guild_permissions.manage_messages:
        return web.json_response({'error': 'Forbidden'}, status=403)

    # ?action=WARN,MUTE  ?uid=<ID участника или модератора>  ?before=<id записи>
    q = request.rel_url.query
    where, args = ["guild_id=?"], [guild_id]
    actions = [a for a in q.get('action', '').upper().split(',') if a.replace('_', '').isalpha()][:20]
    if actions:
        where.append(f"action IN ({','.join('?' * len(actions))})")
        args += actions
    if q.get('uid', '').isdigit():
        where.append("(user_id=? OR mod_id=?)")
        args += [int(q['uid'])] * 2
    if q.get('before', '').isdigit():
        where.append("id<?")
        args.append(int(q['before']))
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(f"""
                SELECT id, action, user_id, mod_id, reason, duration, created_at
                FROM modlog WHERE {' AND '.join(where)}
                ORDER BY id DESC LIMIT 100
            """, args) as c:
                rows = await c.fetchall()
        names = await _user_names(bot, bg, [r[2] for r in rows] + [r[3] for r in rows])
        result = []
        for rid, action, uid, mid, reason, dur, ts in rows:
            result.append({
                'id':        rid,
                'action':    action,
                'user_id':   str(uid),     # строкой: ID > 2^53, JS округлял бы
                'user_name': names.get(uid),
                'mod_id':    str(mid),
                'mod_name':  'Auto' if mid == 0 else names.get(mid),
                'reason':    reason,
                'duration':  dur,
                'created_at':ts,
            })
        return web.json_response(result)
    except Exception:
        return web.json_response([])

# ── API: invites + search ─────────────────────────────────────

@require_auth
async def api_invites(request):
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response([])
    member = await _get_member(bg, s['user_id'])
    from witness.core import is_moderator
    if not member or not is_moderator(member):
        return web.json_response({'error': 'Forbidden'}, status=403)
    try:
        days = max(1, min(90, int(request.rel_url.query.get('days') or 30)))
    except ValueError:
        days = 30
    try:
        from witness.invites import invite_overview
        data = await invite_overview(bg, days)
        data['can_manage'] = bool(member.guild_permissions.manage_guild)
        return web.json_response(data)
    except Exception as e:
        return _server_error(request, e)


async def _invites_guard(request, need_manage=False):
    """(guild, member, None) или (None, None, ответ). Смотреть — модераторам, менять — Manage Server."""
    bot = request.app['bot']
    bg  = bot.get_guild(int(request.match_info['guild_id']))
    if not bg:
        return None, None, web.json_response({'error': 'Guild not found'}, status=404)
    member = await _get_member(bg, request['session']['user_id'])
    from witness.core import is_moderator
    ok = member and (member.guild_permissions.manage_guild if need_manage else is_moderator(member))
    if not ok:
        return None, None, web.json_response({'error': 'Forbidden'}, status=403)
    return bg, member, None


def _invite_code(request) -> str:
    # «_unknown» в адресе — записи без определённого инвайта (код '')
    code = request.match_info['code']
    return '' if code == '_unknown' else code


@require_auth
async def api_invite_search(request):
    """GET /api/guild/:id/invites/search?q= — инвайты, создатели и зашедшие разом"""
    bg, _, err = await _invites_guard(request)
    if err:
        return err
    q = (request.rel_url.query.get('q') or '').strip()[:100]
    if len(q) < 2:
        return web.json_response({'invites': [], 'creators': [], 'joined': []})
    from witness.invites import invite_search
    return web.json_response(await invite_search(bg, q))


@require_auth
async def api_invite_details(request):
    """GET /api/guild/:id/invites/code/:code — карточка инвайта"""
    bg, member, err = await _invites_guard(request)
    if err:
        return err
    from witness.invites import invite_details
    data = await invite_details(bg, _invite_code(request))
    data['can_manage'] = bool(member.guild_permissions.manage_guild)
    return web.json_response(data)


@require_auth
async def api_invite_note(request):
    """POST /api/guild/:id/invites/code/:code/note {"note": "..."} — как /invnote"""
    bg, _, err = await _invites_guard(request, need_manage=True)
    if err:
        return err
    code = _invite_code(request)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({'error': 'bad_json'}, status=400)
    if not code:
        return web.json_response({'error': 'no_code'}, status=400)
    from witness.invites import set_invite_note
    await set_invite_note(bg.id, code, str((data or {}).get('note') or '').strip()[:200])
    return web.json_response({'ok': True})


@require_auth
async def api_invite_delete(request):
    """POST /api/guild/:id/invites/code/:code/delete — удалить действующий инвайт"""
    bg, member, err = await _invites_guard(request, need_manage=True)
    if err:
        return err
    from witness.invites import delete_invite
    res = await delete_invite(bg, _invite_code(request), member)
    if res:
        return web.json_response({'error': res}, status=404 if res == 'not_found' else 400)
    return web.json_response({'ok': True})


# ── API: Discord Activity ─────────────────────────────────────

async def api_activity_ping(request):
    """
    GET /activity/ping — проверка, что запрос из Activity вообще
    доходит до бота через прокси Discord. Без авторизации.
    """
    return web.json_response({
        'ok': True,
        'path': str(request.rel_url),
        'configured': bool(BOT_ID and CLIENT_SECRET),
        'bot_id_set': bool(BOT_ID),
        'secret_set': bool(CLIENT_SECRET),
    })


async def api_activity_token(request):
    """
    POST /api/activity/token  {"code": "..."}

    Обмен кода из Embedded App SDK на нашу сессию.
    Внутри Activity обычный OAuth-редирект недоступен: SDK сам получает
    код через discordSdk.commands.authorize(), а обменять его на токен
    может только сервер — client_secret на клиент отдавать нельзя.
    Возвращаем тот же подписанный токен, что и обычный вход на сайт,
    поэтому весь остальной API работает без изменений.
    """
    print(f"[ACTIVITY] Запрос обмена кода · путь {request.rel_url} · "
          f"origin {request.headers.get('Origin', '—')}")
    try:
        data = await request.json()
    except Exception:
        return web.json_response({'error': 'bad_json'}, status=400)

    code = (data.get('code') or '').strip()
    if not code:
        return web.json_response({'error': 'no_code'}, status=400)

    # Проверяем конфигурацию до запроса — иначе Discord вернёт туманную ошибку
    if not BOT_ID or not CLIENT_SECRET:
        print("[ACTIVITY] BOT_ID или DISCORD_CLIENT_SECRET не заданы")
        return web.json_response(
            {'error': 'not_configured',
             'detail': 'На сервере бота не заданы BOT_ID / DISCORD_CLIENT_SECRET'},
            status=500)

    # Для Activity redirect_uri не передаётся: код выдан через SDK, а не редиректом
    payload = {
        'client_id':     BOT_ID,
        'client_secret': CLIENT_SECRET,
        'grant_type':    'authorization_code',
        'code':          code,
    }
    try:
        async with aiohttp.ClientSession() as s:
            async with s.post('https://discord.com/api/oauth2/token',
                              data=payload) as r:
                status = r.status
                raw = await r.text()
        try:
            token_data = json.loads(raw)
        except Exception:
            token_data = {}
    except Exception as ex:
        print(f"[ACTIVITY] Сеть недоступна при обмене кода: {ex}")
        return web.json_response({'error': 'network'}, status=502)

    if 'access_token' not in token_data:
        # Пишем всё, что вернул Discord — без этого причину не найти
        print(f"[ACTIVITY] Discord отклонил обмен: HTTP {status} · {raw[:400]}")
        return web.json_response({
            'error':  'exchange_rejected',
            'status': status,
            'detail': token_data.get('error_description')
                      or token_data.get('error')
                      or raw[:200],
        }, status=401)

    user = await get_discord_user(token_data['access_token'])
    if not user:
        print("[ACTIVITY] Не удалось получить профиль пользователя")
        return web.json_response(
            {'error': 'user_fetch_failed',
             'detail': 'Токен получен, но профиль Discord не отдал'}, status=502)

    session_data = {
        'user_id':       user['id'],
        'username':      user['username'],
        'discriminator': user.get('discriminator', '0'),
        'avatar':        user.get('avatar'),
        'access_token':  token_data['access_token'],
        'refresh_token': token_data.get('refresh_token', ''),
        'ts':            time.time(),
    }
    return web.json_response({
        'token':        sign_session(session_data),
        'access_token': token_data['access_token'],
        'user': {'id': user['id'], 'username': user['username'],
                 'avatar': user.get('avatar')},
    })


# ── API: автоматизация и безопасность ─────────────────────────

@require_auth
async def api_automation_get(request):
    """GET /api/guild/:id/automation"""
    guild_id = int(request.match_info['guild_id'])
    s   = request['session']
    bot = request.app['bot']
    bg  = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({}, status=404)
    me = await _get_member(bg, s['user_id'])
    if not me or not (me.guild_permissions.manage_guild or
                      me.guild_permissions.administrator):
        return web.json_response({'error': 'Forbidden'}, status=403)

    out = {}
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT enabled, max_age_days, log_channel
                FROM invite_autoclean_settings WHERE guild_id=?
            """, (guild_id,)) as c:
                r = await c.fetchone()
            out['invclean_enabled']  = bool(r[0]) if r else False
            out['invclean_days']     = (r[1] if r else 14)
            out['invclean_channel']  = str(r[2]) if r and r[2] else ''

            async with db.execute("""
                SELECT starboard_channel, starboard_threshold, birthday_channel
                FROM guild_settings WHERE guild_id=?
            """, (guild_id,)) as c:
                g = await c.fetchone()
            out['starboard_channel']  = str(g[0]) if g and g[0] else ''
            out['starboard_threshold']= (g[1] if g and g[1] else 3)
            out['birthday_channel']   = str(g[2]) if g and g[2] else ''
    except Exception as e:
        return _server_error(request, e)
    return web.json_response(out)


@require_auth
async def api_automation_post(request):
    """POST /api/guild/:id/automation"""
    guild_id = int(request.match_info['guild_id'])
    s   = request['session']
    bot = request.app['bot']
    bg  = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Guild not found'}, status=404)
    me = await _get_member(bg, s['user_id'])
    if not me or not (me.guild_permissions.manage_guild or
                      me.guild_permissions.administrator):
        return web.json_response({'error': 'Forbidden'}, status=403)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({'error': 'bad_json'}, status=400)

    try:
        now = datetime.datetime.utcnow().isoformat()
        async with aiosqlite.connect(DB_PATH) as db:
            if any(k in data for k in ('invclean_enabled','invclean_days','invclean_channel')):
                async with db.execute("""
                    SELECT enabled, max_age_days, log_channel
                    FROM invite_autoclean_settings WHERE guild_id=?
                """, (guild_id,)) as c:
                    cur = await c.fetchone() or (0, 14, 0)
                en  = int(bool(data.get('invclean_enabled', cur[0])))
                dys = max(1, min(365, int(data.get('invclean_days', cur[1]) or 14)))
                ch  = int(data.get('invclean_channel') or cur[2] or 0)
                await db.execute("""
                    INSERT INTO invite_autoclean_settings (guild_id, enabled, max_age_days, log_channel)
                    VALUES (?,?,?,?)
                    ON CONFLICT(guild_id) DO UPDATE SET
                        enabled=excluded.enabled, max_age_days=excluded.max_age_days,
                        log_channel=excluded.log_channel
                """, (guild_id, en, dys, ch))

            gs_map = {
                'starboard_channel':   'starboard_channel',
                'starboard_threshold': 'starboard_threshold',
                'birthday_channel':    'birthday_channel',
            }
            for key, col in gs_map.items():
                if key in data:
                    v = data[key]
                    v = int(v or 0)
                    if key == 'starboard_threshold': v = max(1, min(100, v or 3))
                    await db.execute(
                        f"INSERT INTO guild_settings (guild_id, {col}) VALUES (?,?) "
                        f"ON CONFLICT(guild_id) DO UPDATE SET {col}=excluded.{col}",
                        (guild_id, v))
            await db.commit()

        cache = getattr(bot, '_settings_cache', None)
        if isinstance(cache, dict):
            cache.pop(guild_id, None)
        return web.json_response({'ok': True})
    except Exception as e:
        return _server_error(request, e)


# ── API: поиск по участнику ───────────────────────────────────

def _role_kind(role) -> str:
    """'admin' — управляет сервером, 'mod' — может наказывать или менять сервер"""
    p = role.permissions
    if p.administrator or p.manage_guild:
        return 'admin'
    if (p.ban_members or p.kick_members or p.moderate_members or
            p.manage_messages or p.manage_roles or p.manage_channels):
        return 'mod'
    return ''


@require_auth
async def api_member_lookup(request):
    """GET /api/guild/:id/member/:uid — досье на участника"""
    guild_id = int(request.match_info['guild_id'])
    uid      = int(request.match_info['uid'])
    s   = request['session']
    bot = request.app['bot']
    bg  = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Guild not found'}, status=404)
    me = await _get_member(bg, s['user_id'])
    p  = me.guild_permissions if me else None
    if not p or not (p.manage_messages or p.moderate_members or
                     p.ban_members or p.administrator or p.manage_guild):
        return web.json_response({'error': 'Forbidden'}, status=403)

    member = await _get_member(bg, uid)
    # Ушёл с сервера (например, забанен) — имя и аватар берём из профиля Discord
    user = member
    if not member:
        try:
            user = bot.get_user(uid) or await bot.fetch_user(uid)
        except Exception:
            user = None
    out = {
        'user_id': str(uid),
        'name':    (member.display_name if member else
                    (user.global_name or user.name) if user else ''),
        'tag':     str(user) if user else '',
        'avatar':  user.display_avatar.url if user else '',
        'joined':  member.joined_at.isoformat() if member and member.joined_at else '',
        'created': user.created_at.isoformat() if user else '',
        'on_server': bool(member),
        'timed_out': bool(member and member.is_timed_out()),
        'roles': [r.name for r in member.roles[1:]] if member else [],
        # Сверху вниз, как в Discord; kind — чем роль опасна (подсветка в панели)
        'roles_info': [{'name': r.name,
                        'color': f'#{r.color.value:06x}' if r.color.value else '',
                        'kind': _role_kind(r)}
                       for r in reversed(member.roles[1:])] if member else [],
    }
    # Быстрые действия в мини-профиле: показываем только разрешённые
    from witness.moderation import target_error, MUTE_DURATIONS
    blocked = await target_error(me, member)
    out['can'] = {
        'warn':   bool(member) and p.moderate_members and not blocked,
        'mute':   bool(member) and p.moderate_members and not blocked,
        'unmute': p.moderate_members,
        'unban':  p.ban_members,
    }
    out['blocked'] = blocked
    out['mute_durations'] = list(MUTE_DURATIONS)
    # Каждый блок изолирован: сбой одной выборки не должен ронять всё досье
    out.update({'warns': 0, 'history': [], 'appeals': [],
                'active_mute': None})
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            try:
                from witness.database import get_active_warnings
                out['warns'] = len(await get_active_warnings(guild_id, uid))
            except Exception as ex:
                print(f"[LOOKUP] warnings: {ex}")

            try:
                async with db.execute("""
                    SELECT action, reason, duration, created_at, id FROM modlog
                    WHERE guild_id=? AND user_id=? ORDER BY id DESC LIMIT 15
                """, (guild_id, uid)) as c:
                    out['history'] = [
                        {'action': a, 'reason': r, 'duration': d, 'created_at': ts, 'id': i}
                        for a, r, d, ts, i in await c.fetchall()
                    ]
            except Exception as ex:
                print(f"[LOOKUP] modlog: {ex}")

            try:
                async with db.execute("""
                    SELECT id, action_type, status, submitted_at FROM appeals
                    WHERE guild_id=? AND user_id=? ORDER BY id DESC LIMIT 10
                """, (guild_id, uid)) as c:
                    out['appeals'] = [
                        {'id': i, 'action_type': at, 'status': st, 'submitted_at': ts}
                        for i, at, st, ts in await c.fetchall()
                    ]
            except Exception as ex:
                print(f"[LOOKUP] appeals: {ex}")

            try:
                # В invite_log приглашённый хранится как member_id (не user_id)
                async with db.execute("""
                    SELECT invite_code, inviter_id, inviter_name FROM invite_log
                    WHERE guild_id=? AND member_id=? ORDER BY id DESC LIMIT 1
                """, (guild_id, uid)) as c:
                    row = await c.fetchone()
                if row:
                    inv = bg.get_member(row[1]) if row[1] else None
                    out['invited_by'] = {
                        'code': row[0] or '—',
                        'name': (inv.display_name if inv else (row[2] or str(row[1] or '—')))
                    }
            except Exception as ex:
                print(f"[LOOKUP] invite_log: {ex}")

            try:
                async with db.execute("""
                    SELECT until, reason FROM active_mutes
                    WHERE guild_id=? AND user_id=? AND notified=0
                """, (guild_id, uid)) as c:
                    mrow = await c.fetchone()
                out['active_mute'] = ({'until': mrow[0], 'reason': mrow[1]} if mrow else None)
            except Exception as ex:
                print(f"[LOOKUP] active_mutes: {ex}")
    except Exception as ex:
        print(f"[LOOKUP] DB error: {ex}")

    # Кейсы и заметки модераторов — только если включены в «Модерации»
    out.update({'cases': False, 'notes': []})
    try:
        from witness.database import get_protection
        if (await get_protection(guild_id))['cases_enabled']:
            from witness.guard import get_notes
            rows = await get_notes(guild_id, uid)
            names = await _user_names(bot, bg, [r[1] for r in rows])
            out['cases'] = True
            out['notes'] = [{'id': i, 'mod': names.get(m) or str(m), 'text': t, 'created_at': ts}
                            for i, m, t, ts in rows]
    except Exception as ex:
        print(f"[LOOKUP] notes: {ex}")

    # Сколько правок сообщений записано (ссылка на «Правки сообщений»)
    out['edits'] = None
    try:
        from witness.database import get_protection
        if (await get_protection(guild_id))['edits_enabled']:
            from witness.edits import counts
            out['edits'] = await counts(guild_id, uid)
    except Exception as ex:
        print(f"[LOOKUP] edits: {ex}")

    return web.json_response(out)


# ── API: история правок сообщений (witness/edits.py) ─────────

async def _edits_guard(request, need_manage=False):
    """(guild, None) или (None, ответ с ошибкой). Смотреть — модераторам, чистить — Manage Server."""
    bot = request.app['bot']
    bg  = bot.get_guild(int(request.match_info['guild_id']))
    if not bg:
        return None, web.json_response({'error': 'Guild not found'}, status=404)
    me = await _get_member(bg, request['session']['user_id'])
    p  = me.guild_permissions if me else None
    ok = p and ((p.manage_guild or p.administrator) if need_manage else
                (p.manage_messages or p.moderate_members or p.ban_members or p.administrator or p.manage_guild))
    if not ok:
        return None, web.json_response({'error': 'Forbidden'}, status=403)
    return bg, None


@require_auth
async def api_edit_authors(request):
    """GET /api/guild/:id/edits/authors?q=ник — участники с правками: и с сервера, и ушедшие"""
    bg, err = await _edits_guard(request)
    if err:
        return err
    q = (request.query.get('q') or '').strip()
    if len(q) < 2:
        return web.json_response([])
    from witness import edits
    out = {}
    for uid, name, _ in await edits.authors(bg.id, q):
        m = bg.get_member(uid)
        n = sum((await edits.counts(bg.id, uid)).values())     # вместе с архивом
        out[str(uid)] = {'id': str(uid), 'name': m.display_name if m else name, 'tag': str(m) if m else name,
                         'avatar': m.display_avatar.url if m else '', 'count': n, 'on_server': bool(m)}
    # Участники сервера, которые подходят по нику, но правок пока нет
    ql = q.lower()
    for m in bg.members:
        if len(out) >= 20:
            break
        if str(m.id) not in out and not m.bot and (ql in m.display_name.lower() or ql in m.name.lower()):
            out[str(m.id)] = {'id': str(m.id), 'name': m.display_name, 'tag': str(m),
                              'avatar': m.display_avatar.url, 'count': 0, 'on_server': True}
    return web.json_response(sorted(out.values(), key=lambda x: -x['count']))


@require_auth
async def api_edits(request):
    """GET /api/guild/:id/edits?user=ID&source=recent|archive&before_at=&before_id=&q=текст"""
    bg, err = await _edits_guard(request)
    if err:
        return err
    try:
        uid = int(request.query.get('user') or 0)
        before_id = int(request.query.get('before_id') or 0)
    except ValueError:
        return web.json_response({'error': 'bad_request'}, status=400)
    if not uid:
        return web.json_response({'error': 'user_required'}, status=400)
    q = (request.query.get('q') or '').strip()[:100]
    from witness import edits
    from witness.database import get_protection
    if request.query.get('source') == 'archive':
        before_at = (request.query.get('before_at') or '')[:40]
        items, more = await edits.archived(bg.id, uid, q, before_at, before_id)
    else:
        items, more = await edits.recent(bg.id, uid, q), False
    for it in items:
        ch = bg.get_channel(int(it['channel_id']))
        it['channel'] = ch.name if ch else ''
        it['url'] = f"https://discord.com/channels/{bg.id}/{it['channel_id']}/{it['message_id']}"
    cfg = await get_protection(bg.id)
    return web.json_response({'items': items, 'more': more, 'counts': await edits.counts(bg.id, uid),
                              'enabled': cfg['edits_enabled'], 'keep_days': cfg['edits_keep_days']})


@require_auth
async def api_edits_clear(request):
    """POST /api/guild/:id/edits/clear — {"user": ID} очистить участника, {} — весь сервер"""
    bg, err = await _edits_guard(request, need_manage=True)
    if err:
        return err
    try:
        data = await request.json()
        uid = int(data.get('user') or 0) if isinstance(data, dict) else 0
    except Exception:
        return web.json_response({'error': 'bad_json'}, status=400)
    from witness import edits
    return web.json_response({'ok': True, 'deleted': await edits.clear(bg.id, uid or None)})


@require_auth
async def api_edits_import(request):
    """GET — статус импорта из лог-канала, POST — запустить (Manage Server, запись правок включена)"""
    bg, err = await _edits_guard(request, need_manage=True)
    if err:
        return err
    from witness import edits
    from witness.database import get_protection
    if request.method == 'POST':
        if not (await get_protection(bg.id))['edits_enabled']:
            return web.json_response({'error': 'edits_disabled'}, status=400)
        edits.start_import(bg)
    return web.json_response(edits.import_status(bg.id))


@require_auth
async def api_member_notes(request):
    """POST /api/guild/:id/member/:uid/notes — {"text": "..."} добавить, {"delete": id} удалить"""
    guild_id = int(request.match_info['guild_id'])
    uid      = int(request.match_info['uid'])
    s   = request['session']
    bot = request.app['bot']
    bg  = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Guild not found'}, status=404)
    me = await _get_member(bg, s['user_id'])
    p  = me.guild_permissions if me else None
    if not p or not (p.manage_messages or p.moderate_members or p.administrator):
        return web.json_response({'error': 'Forbidden'}, status=403)
    from witness.database import get_protection
    if not (await get_protection(guild_id))['cases_enabled']:
        return web.json_response({'error': 'cases_disabled'}, status=400)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({'error': 'bad_json'}, status=400)
    if not isinstance(data, dict):
        return web.json_response({'error': 'bad_json'}, status=400)
    from witness.guard import add_note, delete_note
    if 'delete' in data:
        try:
            ok = await delete_note(guild_id, int(data['delete']))
        except (TypeError, ValueError):
            ok = False
        return web.json_response({'ok': ok}, status=200 if ok else 404)
    text = str(data.get('text') or '').strip()[:1000]
    if not text:
        return web.json_response({'error': 'text_required'}, status=400)
    nid = await add_note(guild_id, uid, int(s['user_id']), text)
    return web.json_response({'ok': True, 'id': nid})


@require_auth
async def api_member_search(request):
    """GET /api/guild/:id/members/search?q=ник — поиск участника по имени"""
    guild_id = int(request.match_info['guild_id'])
    q = (request.query.get('q') or '').strip().lower()
    s   = request['session']
    bot = request.app['bot']
    bg  = bot.get_guild(guild_id)
    if not bg:
        return web.json_response([])
    me = await _get_member(bg, s['user_id'])
    p  = me.guild_permissions if me else None
    if not p or not (p.manage_messages or p.moderate_members or
                     p.ban_members or p.administrator or p.manage_guild):
        return web.json_response({'error': 'Forbidden'}, status=403)
    if len(q) < 2:
        return web.json_response([])

    out = []
    if q.isdigit():
        m = await _get_member(bg, int(q))
        if m:
            out.append({'id': str(m.id), 'name': m.display_name,
                        'tag': str(m), 'avatar': m.display_avatar.url})
    for m in bg.members:
        if len(out) >= 20:
            break
        if q in m.display_name.lower() or q in m.name.lower():
            if not any(o['id'] == str(m.id) for o in out):
                out.append({'id': str(m.id), 'name': m.display_name,
                            'tag': str(m), 'avatar': m.display_avatar.url})
    if not out and not q.isdigit():
        # Без Server Members Intent кэш почти пустой — ищем по началу имени через Discord
        try:
            for m in await bg.query_members(query=q, limit=20):
                out.append({'id': str(m.id), 'name': m.display_name,
                            'tag': str(m), 'avatar': m.display_avatar.url})
        except Exception:
            pass
    return web.json_response(out)


# ── API: ban requests ─────────────────────────────────────────

@require_auth
async def api_ban_requests(request):
    """GET /api/guild/:id/banrequests?status=pending"""
    guild_id = int(request.match_info['guild_id'])
    s   = request['session']
    bot = request.app['bot']
    bg  = bot.get_guild(guild_id)
    if not bg:
        return web.json_response([], status=200)
    member = await _get_member(bg, s['user_id'])
    p = member.guild_permissions if member else None
    if not p or not (p.ban_members or p.administrator or p.manage_guild):
        return web.json_response({'error': 'Forbidden'}, status=403)

    status = request.query.get('status', '')
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            if status:
                q = """SELECT id,user_id,username,mod_id,reason,warn_count,status,
                              reviewer_id,created_at,reviewed_at
                       FROM ban_requests WHERE guild_id=? AND status=?
                       ORDER BY id DESC LIMIT 100"""
                args = (guild_id, status)
            else:
                q = """SELECT id,user_id,username,mod_id,reason,warn_count,status,
                              reviewer_id,created_at,reviewed_at
                       FROM ban_requests WHERE guild_id=?
                       ORDER BY id DESC LIMIT 100"""
                args = (guild_id,)
            async with db.execute(q, args) as c:
                rows = await c.fetchall()
    except Exception as e:
        return _server_error(request, e)

    names = await _user_names(bot, bg, [r[3] for r in rows] + [r[7] for r in rows])
    out = []
    for rid, uid, uname, mid, reason, wc, st, rev, created, reviewed in rows:
        out.append({
            'id': rid, 'user_id': str(uid), 'username': uname,
            'mod_id': str(mid), 'mod_name': 'Auto' if not mid else (names.get(mid) or str(mid)),
            'reason': reason, 'warn_count': wc, 'status': st,
            'reviewer_id': str(rev or ''),
            'reviewer_name': names.get(rev) or (str(rev) if rev else ''),
            'created_at': created, 'reviewed_at': reviewed,
        })
    return web.json_response(out)


@require_auth
async def api_ban_request_action(request):
    """POST /api/guild/:id/banrequest/:rid/[approve|reject]"""
    guild_id = int(request.match_info['guild_id'])
    rid      = int(request.match_info['rid'])
    action   = request.match_info['action']
    if action not in ('approve', 'reject'):
        return web.json_response({'error': 'invalid_action'}, status=400)

    s   = request['session']
    bot = request.app['bot']
    bg  = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Guild not found'}, status=404)
    member = await _get_member(bg, s['user_id'])
    p = member.guild_permissions if member else None
    if not p or not (p.ban_members or p.administrator):
        return web.json_response({'error': 'Forbidden'}, status=403)

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT user_id, reason, status FROM ban_requests WHERE id=? AND guild_id=?",
                (rid, guild_id)
            ) as c:
                row = await c.fetchone()
        if not row:
            return web.json_response({'error': 'not_found'}, status=404)
        uid, reason, status = row
        if status != 'pending':
            return web.json_response({'error': 'already_reviewed'}, status=400)

        now = datetime.datetime.utcnow().isoformat()
        ban_ok = False
        ban_days = 30
        new_status = 'approved' if action == 'approve' else 'rejected'

        # Сначала фиксируем решение: условие status='pending' не даёт
        # двум модераторам одновременно одобрить одну заявку (двойной бан)
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("""
                UPDATE ban_requests SET status=?, reviewer_id=?, reviewed_at=?
                WHERE id=? AND guild_id=? AND status='pending'
            """, (new_status, int(s['user_id']), now, rid, guild_id))
            await db.commit()
            if cur.rowcount == 0:
                return web.json_response({'error': 'already_reviewed'}, status=400)

        if action == 'approve':
            # Как кнопка «Одобрить» в Discord (_BanRequestActions.approve):
            # DM с кнопкой апелляции до бана, temp_bans и запись в modlog
            from witness.database import get_punishment_settings, add_modlog
            from witness.moderation import send_appeal_dm
            ps = await get_punishment_settings(guild_id)
            ban_days = ps.get('ban3_days', 30) or 30
            unban_at = (datetime.datetime.utcnow() +
                        datetime.timedelta(days=ban_days)).isoformat()
            target = await _get_member(bg, uid)
            if target:
                try:
                    await send_appeal_dm(target, bg, "TEMPBAN",
                                         f"Бан на {ban_days} дней · {reason}")
                except Exception:
                    pass
            try:
                await bg.ban(target or await bot.fetch_user(uid),
                             reason=f"[Заявка #{rid}] {reason}")
                ban_ok = True
                async with aiosqlite.connect(DB_PATH) as db:
                    await db.execute("""
                        INSERT INTO temp_bans (guild_id, user_id, mod_id, reason, unban_at)
                        VALUES (?, ?, ?, ?, ?)
                        ON CONFLICT(guild_id, user_id) DO UPDATE SET
                            unban_at=excluded.unban_at, unbanned=0, reason=excluded.reason
                    """, (guild_id, uid, int(s['user_id']), reason, unban_at))
                    await db.commit()
            except Exception as ex:
                print(f"[BANREQ] Бан не выдан #{rid}: {ex}")
            await add_modlog(guild_id, uid, int(s['user_id']),
                             "AUTO_TEMPBAN", reason, f"{ban_days}d")

        try:
            from witness.moderation import ban_request_result_embed, update_ban_request_message
            await update_ban_request_message(bg, rid, ban_request_result_embed(
                rid, action == 'approve', member.mention, ban_days, ban_ok))
        except Exception as ex:
            print(f"[BANREQ] message: {ex}")

        return web.json_response({'ok': True, 'status': new_status,
                                  'banned': ban_ok, 'ban_days': ban_days})
    except Exception as e:
        return _server_error(request, e)


# ── API: appeal accept/reject ─────────────────────────────────

@require_auth
async def api_appeal_action(request):
    """POST /api/guild/:id/appeal/:appeal_id/[accept|reject]"""
    guild_id  = int(request.match_info['guild_id'])
    appeal_id = int(request.match_info['appeal_id'])
    action    = request.match_info['action']
    if action not in ('accept', 'reject'):
        return web.json_response({'error': 'invalid_action'}, status=400)

    s   = request['session']
    bot = request.app['bot']
    bg  = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Guild not found'}, status=404)
    member = await _get_member(bg, s['user_id'])
    if not member:
        return web.json_response({'error': 'Forbidden'}, status=403)

    try:
        data = await request.json()
    except Exception:
        data = {}
    note = str(data.get('note') or '').strip()[:1000]

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT user_id, action_type, status FROM appeals WHERE id=? AND guild_id=?",
                (appeal_id, guild_id)
            ) as c:
                row = await c.fetchone()
        if not row:
            return web.json_response({'error': 'appeal_not_found'}, status=404)
        uid, atype, status = row
        if status != 'pending':
            return web.json_response({'error': 'already_reviewed'}, status=400)

        # Любой модератор, апелляцию на бан — только с правом бана (то же в /appeal accept|reject)
        from witness.moderation import can_decide_appeal
        if not can_decide_appeal(member.guild_permissions, atype):
            return web.json_response(
                {'error': 'missing_permission', 'need': atype}, status=403)

        now = datetime.datetime.utcnow().isoformat()
        new_status = 'accepted' if action == 'accept' else 'rejected'

        # Сначала фиксируем решение. Условие status='pending' не даёт двум
        # модераторам одновременно принять/отклонить одну апелляцию.
        async with aiosqlite.connect(DB_PATH) as db:
            cur = await db.execute("""
                UPDATE appeals SET status=?, reviewer_id=?,
                                    reviewer_note=?, reviewed_at=?
                WHERE id=? AND guild_id=? AND status='pending'
            """, (new_status, int(s['user_id']), note, now, appeal_id, guild_id))
            await db.commit()
            if cur.rowcount == 0:
                return web.json_response({'error': 'already_reviewed'}, status=400)

        # Если accept — пытаемся снять наказание
        if action == 'accept':
            try:
                if atype in ('BAN', 'TEMPBAN'):
                    try:
                        user = await bot.fetch_user(uid)
                        await bg.unban(user, reason=f"Апелляция #{appeal_id} принята")
                        async with aiosqlite.connect(DB_PATH) as db:
                            await db.execute(
                                "UPDATE temp_bans SET unbanned=1 WHERE guild_id=? AND user_id=? AND unbanned=0",
                                (guild_id, uid)
                            )
                            await db.commit()
                    except Exception:
                        pass
                elif atype == 'MUTE':
                    m = await _get_member(bg, uid)
                    if m and m.is_timed_out():
                        try:
                            await m.timeout(None, reason=f"Апелляция #{appeal_id}")
                        except Exception:
                            pass
                    # Мут снят по апелляции — уведомление «мут закончился» не нужно
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute(
                            "UPDATE active_mutes SET notified=1 WHERE guild_id=? AND user_id=?",
                            (guild_id, uid))
                        await db.commit()
                elif atype == 'WARN':
                    async with aiosqlite.connect(DB_PATH) as db:
                        await db.execute("""
                            DELETE FROM warnings WHERE id IN (
                                SELECT id FROM warnings
                                WHERE guild_id=? AND user_id=?
                                ORDER BY id DESC LIMIT 1
                            )
                        """, (guild_id, uid))
                        await db.commit()
            except Exception:
                pass

        # Как у /appeal accept|reject: запись в историю и DM тем же эмбедом
        try:
            from witness.database import add_modlog
            await add_modlog(guild_id, uid, int(s['user_id']),
                             'APPEAL_ACCEPTED' if action == 'accept' else 'APPEAL_REJECTED',
                             f"#{appeal_id}: {note}")
        except Exception as ex:
            print(f"[APPEAL] modlog: {ex}")
        try:
            from witness.moderation import notify_appeal_result
            await notify_appeal_result(uid, guild_id, appeal_id, action == 'accept', note)
        except Exception as ex:
            print(f"[APPEAL] notify: {ex}")

        return web.json_response({'ok': True, 'status': new_status})
    except Exception as e:
        return _server_error(request, e)


# ── API: appeals ──────────────────────────────────────────────

@require_auth
async def api_appeals(request):
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response([])
    member = await _get_member(bg, s['user_id'])
    # Апелляции видит любой модератор, решает — по can_decide_appeal (бан — только с правом бана).
    # Право решать сайт берёт отсюда (can_decide), по данным бота, а не из прав
    # OAuth на фронте: они расходились, и модератор видел замок вместо кнопок.
    from witness.moderation import can_decide_appeal, can_handle_appeal
    if not member or not can_handle_appeal(member.guild_permissions):
        return web.json_response({'error': 'Forbidden'}, status=403)

    status_filter = request.rel_url.query.get('status', '')
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            if status_filter:
                async with db.execute("""
                    SELECT id, user_id, username, action_type, status,
                           original_reason, appeal_text, why_change,
                           reviewer_id, reviewer_note, submitted_at, reviewed_at
                    FROM appeals WHERE guild_id=? AND status=?
                    ORDER BY id DESC LIMIT 50
                """, (guild_id, status_filter)) as c:
                    rows = await c.fetchall()
            else:
                async with db.execute("""
                    SELECT id, user_id, username, action_type, status,
                           original_reason, appeal_text, why_change,
                           reviewer_id, reviewer_note, submitted_at, reviewed_at
                    FROM appeals WHERE guild_id=?
                    ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END, id DESC
                    LIMIT 50
                """, (guild_id,)) as c:
                    rows = await c.fetchall()
        result = []
        names = await _user_names(bot, bg, [r[8] for r in rows])
        # Переписка: сколько сообщений и кто написал последним (бейдж «новый ответ»)
        thread = {}
        if rows:
            async with aiosqlite.connect(DB_PATH) as db:
                marks = ','.join('?' * len(rows))
                async with db.execute(f"""
                    SELECT appeal_id, COUNT(*),
                           (SELECT from_mod FROM appeal_messages m2 WHERE m2.appeal_id = m.appeal_id
                            ORDER BY id DESC LIMIT 1)
                    FROM appeal_messages m WHERE guild_id=? AND appeal_id IN ({marks})
                    GROUP BY appeal_id
                """, (guild_id, *[r[0] for r in rows])) as c:
                    thread = {a: (n, last) for a, n, last in await c.fetchall()}
        for (aid, uid, uname, atype, astatus, oreason, atext, wchange,
             rev_id, rnote, sub_at, rev_at) in rows:
            m = bg.get_member(uid)
            result.append({
                'id':            aid,
                'user_id':       str(uid),
                'username':      m.display_name if m else uname,
                'action_type':   atype,
                'status':        astatus,
                # Текст целиком: раньше резался до 300 символов,
                # и модератор не видел конец объяснения
                'original_reason': oreason or '',
                'appeal_text':   atext or '',
                'why_change':    wchange or '',
                'reviewer_id':   str(rev_id or ''),
                'reviewer_name': names.get(rev_id) if rev_id else None,
                'reviewer_note': rnote,
                'submitted_at':  sub_at,
                'reviewed_at':   rev_at,
                'messages':      thread.get(aid, (0, None))[0],
                'last_from':     {1: 'mod', 0: 'user'}.get(thread.get(aid, (0, None))[1]),
                'can_decide':    can_decide_appeal(member.guild_permissions, atype),
            })
        return web.json_response(result)
    except Exception:
        return web.json_response([])


@require_auth
async def api_appeal_messages(request):
    """GET/POST /api/guild/:id/appeal/:appeal_id/messages — переписка с участником до решения.
    Отвечать может любой модератор, который видит апелляции, даже без права решать её."""
    guild_id  = int(request.match_info['guild_id'])
    appeal_id = int(request.match_info['appeal_id'])
    s   = request['session']
    bot = request.app['bot']
    bg  = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Guild not found'}, status=404)
    member = await _get_member(bg, s['user_id'])
    from witness.moderation import can_handle_appeal, get_appeal_messages, send_appeal_reply
    if not member or not can_handle_appeal(member.guild_permissions):
        return web.json_response({'error': 'Forbidden'}, status=403)
    if request.method == 'POST':
        try:
            data = await request.json()
        except Exception:
            data = {}
        text = str((data or {}).get('text') or '').strip()
        if not text:
            return web.json_response({'error': 'text_required'}, status=400)
        res = await send_appeal_reply(bg, appeal_id, int(s['user_id']), text)
        if res in ('not_found', 'closed'):
            return web.json_response({'error': res}, status=404 if res == 'not_found' else 400)
        return web.json_response({'ok': True, 'delivered': res == 'sent'})
    rows = await get_appeal_messages(appeal_id, guild_id)
    names = await _user_names(bot, bg, [r[1] for r in rows if r[2]])
    return web.json_response([{'id': i, 'from_mod': bool(fm), 'author': names.get(a) or str(a) if fm else '',
                               'text': t, 'created_at': ts} for i, a, fm, t, ts in rows])

# ── API: активные наказания и действия из панели ──────────────

@require_auth
async def api_punishments(request):
    """GET /api/guild/:id/punishments — кто сейчас в муте и во временном бане"""
    guild_id = int(request.match_info['guild_id'])
    s   = request['session']
    bot = request.app['bot']
    bg  = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Guild not found'}, status=404)
    me = await _get_member(bg, s['user_id'])
    p  = me.guild_permissions if me else None
    if not p or not (p.manage_messages or p.moderate_members or
                     p.ban_members or p.manage_guild):
        return web.json_response({'error': 'Forbidden'}, status=403)

    now = datetime.datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("""
            SELECT user_id, until, reason, created_at FROM active_mutes
            WHERE guild_id=? AND notified=0 AND until>? ORDER BY until
        """, (guild_id, now)) as c:
            mutes = await c.fetchall()
        async with db.execute("""
            SELECT user_id, mod_id, reason, unban_at FROM temp_bans
            WHERE guild_id=? AND unbanned=0 ORDER BY unban_at
        """, (guild_id,)) as c:
            bans = await c.fetchall()
    names = await _user_names(bot, bg, [r[0] for r in mutes] + [r[0] for r in bans] +
                                       [r[1] for r in bans])
    return web.json_response({
        'mutes': [{'user_id': str(u), 'user_name': names.get(u), 'until': until,
                   'reason': reason, 'since': since} for u, until, reason, since in mutes],
        'tempbans': [{'user_id': str(u), 'user_name': names.get(u), 'mod_id': str(m),
                      'mod_name': 'Auto' if not m else names.get(m),
                      'reason': reason, 'until': until} for u, m, reason, until in bans],
        'can': {'unmute': p.moderate_members, 'unban': p.ban_members},
    })


@require_auth
async def api_member_action(request):
    """POST /api/guild/:id/member/:uid/action
    {"action": "warn"|"mute"|"unmute"|"unban", "reason": "...", "duration": "2 часа"}"""
    guild_id = int(request.match_info['guild_id'])
    uid      = int(request.match_info['uid'])
    s   = request['session']
    bot = request.app['bot']
    bg  = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Guild not found'}, status=404)
    me = await _get_member(bg, s['user_id'])
    if not me:
        return web.json_response({'error': 'Forbidden'}, status=403)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({'error': 'bad_json'}, status=400)
    if not isinstance(data, dict):
        return web.json_response({'error': 'bad_json'}, status=400)
    action = data.get('action')
    reason = str(data.get('reason') or '').strip()[:500]

    p = me.guild_permissions
    need = {'warn': p.moderate_members, 'mute': p.moderate_members,
            'unmute': p.moderate_members, 'unban': p.ban_members}
    if action not in need:
        return web.json_response({'error': 'invalid_action'}, status=400)
    if not need[action]:
        return web.json_response({'error': 'missing_permission'}, status=403)
    if action in ('warn', 'mute') and not reason:
        return web.json_response({'error': 'reason_required'}, status=400)

    from witness import moderation as mod
    target = await _get_member(bg, uid)
    if action in ('warn', 'mute'):
        if not target:
            return web.json_response({'error': 'not_on_server'}, status=400)
        blocked = await mod.target_error(me, target)
        if blocked:
            return web.json_response({'error': blocked}, status=403)

    mod_id = int(s['user_id'])
    if action == 'warn':
        res = await mod.panel_warn(bg, target, mod_id, reason)
    elif action == 'mute':
        res = await mod.panel_mute(bg, target, mod_id, str(data.get('duration') or ''), reason)
    elif action == 'unmute':
        res = await mod.panel_unmute(bg, target, uid, mod_id, reason or 'Снят с панели')
    else:
        res = await mod.panel_unban(bg, uid, mod_id, reason or 'Снят с панели')
    if res.get('error'):
        return web.json_response(res, status=400)
    return web.json_response({'ok': True, **res})


@require_auth
async def api_alerts_resolve(request):
    """POST /api/guild/:id/alerts/resolve  {"ids": [..], "resolved": true}"""
    guild_id = int(request.match_info['guild_id'])
    s   = request['session']
    bot = request.app['bot']
    bg  = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Not found'}, status=404)
    member = await _get_member(bg, s['user_id'])
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'Forbidden'}, status=403)
    try:
        data = await request.json()
    except Exception:
        return web.json_response({'error': 'bad_json'}, status=400)
    ids = [int(i) for i in (data or {}).get('ids', []) if str(i).isdigit()][:500]
    if not ids:
        return web.json_response({'ok': True, 'updated': 0})
    resolved = 1 if data.get('resolved', True) else 0
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute(
            f"UPDATE security_alerts SET resolved=? WHERE guild_id=? AND id IN ({','.join('?' * len(ids))})",
            (resolved, guild_id, *ids))
        await db.commit()
    return web.json_response({'ok': True, 'updated': cur.rowcount})

# ── App factory ───────────────────────────────────────────────

def create_app(bot) -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app['bot'] = bot

    # Pages
    app.router.add_get('/',          handle_index)
    app.router.add_get('/dashboard', handle_dashboard)
    app.router.add_get('/login',     handle_login)
    app.router.add_get('/callback',  handle_callback)
    app.router.add_get('/logout',    handle_logout)
    app.router.add_get('/health',    api_stats)

    # CORS preflight
    app.router.add_route('OPTIONS', '/api/{tail:.*}', lambda r: web.Response(status=200))
    app.router.add_route('OPTIONS', '/guild/{tail:.*}', lambda r: web.Response(status=200))
    app.router.add_route('OPTIONS', '/activity/{tail:.*}', lambda r: web.Response(status=200))

    # ── API ──────────────────────────────────────────────────
    # Каждый маршрут регистрируется дважды: с префиксом /api и без него.
    # Прокси Discord Activity ЗАМЕНЯЕТ сопоставленный префикс на домен,
    # поэтому запрос /.proxy/api/activity/token приходит сюда как
    # /activity/token. Дубли снимают эту неоднозначность.
    API_ROUTES = [
        ('GET',  '/stats',                                  api_stats),
        ('GET',  '/me',                                     api_me),
        ('GET',  '/refresh',                                api_refresh),
        ('GET',  '/guilds',                                 api_guilds),
        ('GET',  '/activity/ping',                          api_activity_ping),
        ('POST', '/activity/token',                         api_activity_token),
        ('GET',  '/guild/{guild_id}/settings',              api_guild_settings_get),
        ('POST', '/guild/{guild_id}/settings',              api_guild_settings_post),
        ('POST', '/guild/{guild_id}/security',              api_security_save),
        ('GET',  '/guild/{guild_id}/security/overview',     api_security_overview),
        ('POST', '/guild/{guild_id}/threats',               api_threats_save),
        ('POST', '/guild/{guild_id}/alert/{alert_id}/resolve', api_alert_resolve),
        ('GET',  '/guild/{guild_id}/protection',            api_protection),
        ('POST', '/guild/{guild_id}/protection',            api_protection),
        ('GET',  '/guild/{guild_id}/channels',              api_channels),
        ('GET',  '/guild/{guild_id}/modlog',                api_modlog),
        ('GET',  '/guild/{guild_id}/invites',               api_invites),
        ('GET',  '/guild/{guild_id}/invites/search',        api_invite_search),
        ('GET',  '/guild/{guild_id}/invites/code/{code}',   api_invite_details),
        ('POST', '/guild/{guild_id}/invites/code/{code}/note',   api_invite_note),
        ('POST', '/guild/{guild_id}/invites/code/{code}/delete', api_invite_delete),
        ('GET',  '/guild/{guild_id}/appeals',               api_appeals),
        ('GET',  '/guild/{guild_id}/appeal/{appeal_id}/messages', api_appeal_messages),
        ('POST', '/guild/{guild_id}/appeal/{appeal_id}/messages', api_appeal_messages),
        ('POST', '/guild/{guild_id}/appeal/{appeal_id}/{action}', api_appeal_action),
        ('GET',  '/guild/{guild_id}/automation',            api_automation_get),
        ('POST', '/guild/{guild_id}/automation',            api_automation_post),
        ('GET',  '/guild/{guild_id}/members/search',        api_member_search),
        ('GET',  '/guild/{guild_id}/member/{uid}',          api_member_lookup),
        ('POST', '/guild/{guild_id}/member/{uid}/action',   api_member_action),
        ('POST', '/guild/{guild_id}/member/{uid}/notes',    api_member_notes),
        ('GET',  '/guild/{guild_id}/edits/authors',         api_edit_authors),
        ('GET',  '/guild/{guild_id}/edits',                 api_edits),
        ('POST', '/guild/{guild_id}/edits/clear',           api_edits_clear),
        ('GET',  '/guild/{guild_id}/edits/import',          api_edits_import),
        ('POST', '/guild/{guild_id}/edits/import',          api_edits_import),
        ('GET',  '/guild/{guild_id}/punishments',           api_punishments),
        ('POST', '/guild/{guild_id}/alerts/resolve',        api_alerts_resolve),
        ('GET',  '/guild/{guild_id}/banrequests',           api_ban_requests),
        ('POST', '/guild/{guild_id}/banrequest/{rid}/{action}', api_ban_request_action),
    ]
    for method, path, handler in API_ROUTES:
        app.router.add_route(method, '/api' + path, handler)
        app.router.add_route(method, path,          handler)   # для прокси Activity

    # Перехватчик неизвестных путей — регистрируется последним, поэтому
    # существующие маршруты не затрагивает. Возвращает JSON с путём,
    # который реально дошёл до бота: без этого прокси Discord отдаёт
    # безликий 502 и понять причину невозможно.
    async def _unknown_route(request):
        print(f"[ROUTE404] {request.method} {request.rel_url} "
              f"· origin {request.headers.get('Origin', '—')}")
        return web.json_response({
            'error': 'unknown_route',
            'method': request.method,
            'path': str(request.rel_url.path),
            'hint': 'Проверь сопоставление URL в Dev Portal Activities',
        }, status=404)

    app.router.add_route('*', '/{tail:.*}', _unknown_route)

    return app


async def start_web_server(bot):
    app    = create_app(bot)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    print(f"✅ Web server started → {SITE_URL} (port {PORT})")
    print(f"   OAuth2 redirect_uri: {SITE_URL}/callback")
    print(f"   ↑ ЭТОТ адрес должен быть в Dev Portal → OAuth2 → Redirects")
    print(f"   Сайт (WEBSITE_URL):  {WEBSITE_URL or '— не задан, дашборд отдаётся ботом —'}")
    print(f"   OAuth2: {'✅ configured' if CLIENT_SECRET else '⚠️  DISCORD_CLIENT_SECRET not set'}")
    print(f"   Bot ID: {'✅ ' + BOT_ID if BOT_ID else '⚠️  BOT_ID not set'}")
