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
  GET  /api/guild/:id/twins      — Twin links
  GET  /api/guild/:id/appeals    — Appeals list
"""

import os, hmac, json, time, hashlib, asyncio, aiohttp, aiofiles, aiosqlite, datetime
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
INVITE_URL    = (f"https://discord.com/api/oauth2/authorize?client_id={BOT_ID}"
                 f"&permissions=8&scope=bot%20applications.commands") if BOT_ID else "#"
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

# ── CORS ──────────────────────────────────────────────────────

def _add_cors(resp, origin: str):
    allowed = (not origin or origin.endswith('.railway.app') or
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
        resp = web.json_response({'error': str(e)}, status=500)
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
        raise web.HTTPFound(f"{base}/dashboard?token={urllib.parse.quote(token)}")
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
        return web.json_response({'error': str(e)}, status=500)

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
        return web.json_response({'error': str(e)}, status=500)

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
        is_admin = bool(perms & 0x8 or perms & 0x20)
        is_mod   = bool(perms & 0x2000 or perms & 0x10000000000)
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

# ── API: guild settings GET ───────────────────────────────────

@require_auth
async def api_guild_settings_get(request):
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Guild not found'}, status=404)
    member = bg.get_member(int(s['user_id']))
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

        sec_settings = json.loads(sec_row[1] or '{}') if sec_row else {}

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
        return web.json_response({'error': str(e)}, status=500)

# ── API: guild settings POST ──────────────────────────────────

@require_auth
async def api_guild_settings_post(request):
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Not found'}, status=404)
    member = bg.get_member(int(s['user_id']))
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'Forbidden'}, status=403)

    try:
        data = await request.json()
        async with aiosqlite.connect(DB_PATH) as db:

            # ── guild_settings ─────────────────────────────────
            gs_fields = {
                'lang': data.get('lang'),
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
        return web.json_response({'error': str(e)}, status=500)

# ── API: security toggles POST ────────────────────────────────

@require_auth
async def api_security_save(request):
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response({'error': 'Not found'}, status=404)
    member = bg.get_member(int(s['user_id']))
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'Forbidden'}, status=403)
    try:
        data     = await request.json()
        settings = json.dumps(data)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO security_settings (guild_id, settings)
                VALUES (?, ?)
                ON CONFLICT(guild_id) DO UPDATE SET settings=excluded.settings
            """, (guild_id, settings))
            await db.commit()
        return web.json_response({'ok': True})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

# ── API: channels ─────────────────────────────────────────────

@require_auth
async def api_channels(request):
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response([], status=200)
    member = bg.get_member(int(s['user_id']))
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
    member = bg.get_member(int(s['user_id']))
    if not member or not member.guild_permissions.manage_messages:
        return web.json_response({'error': 'Forbidden'}, status=403)

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT action, user_id, mod_id, reason, duration, created_at
                FROM modlog WHERE guild_id=?
                ORDER BY id DESC LIMIT 100
            """, (guild_id,)) as c:
                rows = await c.fetchall()
        result = []
        for action, uid, mid, reason, dur, ts in rows:
            u = bg.get_member(uid)
            m = bg.get_member(mid)
            result.append({
                'action':    action,
                'user_id':   uid,
                'user_name': u.display_name if u else None,
                'mod_id':    mid,
                'mod_name':  m.display_name if m else ('Auto' if mid == 0 else None),
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
    member = bg.get_member(int(s['user_id']))
    if not member or not member.guild_permissions.manage_messages:
        return web.json_response({'error': 'Forbidden'}, status=403)

    q = request.rel_url.query.get('q', '').lower().strip()

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            if q:
                async with db.execute("""
                    SELECT invite_code, inviter_id, inviter_name,
                           COUNT(*) as uses, MAX(joined_at) as last_used, MAX(note) as note
                    FROM invite_log WHERE guild_id=?
                      AND (LOWER(invite_code) LIKE ? OR LOWER(inviter_name) LIKE ?
                           OR LOWER(note) LIKE ?)
                    GROUP BY invite_code
                    ORDER BY uses DESC LIMIT 50
                """, (guild_id, f'%{q}%', f'%{q}%', f'%{q}%')) as c:
                    rows = await c.fetchall()
            else:
                async with db.execute("""
                    SELECT invite_code, inviter_id, inviter_name,
                           COUNT(*) as uses, MAX(joined_at) as last_used, MAX(note) as note
                    FROM invite_log WHERE guild_id=?
                    GROUP BY invite_code
                    ORDER BY uses DESC LIMIT 100
                """, (guild_id,)) as c:
                    rows = await c.fetchall()
        result = []
        for code, iid, iname, uses, last, note in rows:
            result.append({
                'invite_code':  code,
                'inviter_id':   iid,
                'inviter_name': iname,
                'uses':         uses,
                'last_used':    last,
                'note':         note,
            })
        return web.json_response(result)
    except Exception:
        return web.json_response([])

# ── API: twins ────────────────────────────────────────────────

@require_auth
async def api_twins(request):
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response([])
    member = bg.get_member(int(s['user_id']))
    if not member or not member.guild_permissions.manage_messages:
        return web.json_response({'error': 'Forbidden'}, status=403)

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT id, user_a, user_b, similarity, reasons,
                       confirmed, false_positive, detected_at
                FROM twin_links WHERE guild_id=?
                ORDER BY similarity DESC LIMIT 50
            """, (guild_id,)) as c:
                rows = await c.fetchall()
        result = []
        for lid, ua, ub, sim, reasons, conf, fp, ts in rows:
            ma = bg.get_member(ua)
            mb = bg.get_member(ub)
            result.append({
                'id':           lid,
                'user_a':       ua,
                'user_a_name':  ma.display_name if ma else None,
                'user_b':       ub,
                'user_b_name':  mb.display_name if mb else None,
                'similarity':   sim,
                'reasons':      reasons,
                'confirmed':    conf,
                'false_positive': fp,
                'detected_at':  ts,
            })
        return web.json_response(result)
    except Exception:
        return web.json_response([])

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
    me = bg.get_member(int(s['user_id']))
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

            async with db.execute(
                "SELECT twin_threshold FROM guild_settings WHERE guild_id=?", (guild_id,)) as c:
                t = await c.fetchone()
            out['twin_threshold'] = (t[0] if t and t[0] else 80)
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)
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
    me = bg.get_member(int(s['user_id']))
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
                'twin_threshold':      'twin_threshold',
            }
            for key, col in gs_map.items():
                if key in data:
                    v = data[key]
                    v = int(v or 0)
                    if key == 'starboard_threshold': v = max(1, min(100, v or 3))
                    if key == 'twin_threshold':      v = max(50, min(99, v or 80))
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
        return web.json_response({'error': str(e)}, status=500)


# ── API: поиск по участнику ───────────────────────────────────

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
    me = bg.get_member(int(s['user_id']))
    p  = me.guild_permissions if me else None
    if not p or not (p.manage_messages or p.moderate_members or
                     p.ban_members or p.administrator or p.manage_guild):
        return web.json_response({'error': 'Forbidden'}, status=403)

    member = bg.get_member(uid)
    out = {
        'user_id': str(uid),
        'name':    member.display_name if member else '',
        'tag':     str(member) if member else '',
        'avatar':  member.display_avatar.url if member else '',
        'joined':  member.joined_at.isoformat() if member and member.joined_at else '',
        'created': member.created_at.isoformat() if member else '',
        'on_server': bool(member),
        'timed_out': bool(member and member.is_timed_out()),
        'roles': [r.name for r in member.roles[1:]] if member else [],
    }
    # Каждый блок изолирован: сбой одной выборки не должен ронять всё досье
    out.update({'warns': 0, 'history': [], 'appeals': [],
                'twins': [], 'active_mute': None})
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            try:
                async with db.execute(
                    "SELECT COUNT(*) FROM warnings WHERE guild_id=? AND user_id=?",
                    (guild_id, uid)) as c:
                    out['warns'] = (await c.fetchone())[0]
            except Exception as ex:
                print(f"[LOOKUP] warnings: {ex}")

            try:
                async with db.execute("""
                    SELECT action, reason, duration, created_at FROM modlog
                    WHERE guild_id=? AND user_id=? ORDER BY id DESC LIMIT 15
                """, (guild_id, uid)) as c:
                    out['history'] = [
                        {'action': a, 'reason': r, 'duration': d, 'created_at': ts}
                        for a, r, d, ts in await c.fetchall()
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
                    SELECT user_a, user_b, similarity, confirmed, false_positive
                    FROM twin_links
                    WHERE guild_id=? AND (user_a=? OR user_b=?)
                    ORDER BY similarity DESC LIMIT 10
                """, (guild_id, uid, uid)) as c:
                    twins = []
                    for a, b, sim, conf, fp in await c.fetchall():
                        other = b if a == uid else a
                        om = bg.get_member(other)
                        twins.append({'user_id': str(other),
                                      'name': om.display_name if om else str(other),
                                      'similarity': sim or 0,
                                      'confirmed': bool(conf),
                                      'false_positive': bool(fp)})
                    out['twins'] = twins
            except Exception as ex:
                print(f"[LOOKUP] twin_links: {ex}")

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

    return web.json_response(out)


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
    me = bg.get_member(int(s['user_id']))
    p  = me.guild_permissions if me else None
    if not p or not (p.manage_messages or p.moderate_members or
                     p.ban_members or p.administrator or p.manage_guild):
        return web.json_response({'error': 'Forbidden'}, status=403)
    if len(q) < 2:
        return web.json_response([])

    out = []
    if q.isdigit():
        m = bg.get_member(int(q))
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
    member = bg.get_member(int(s['user_id']))
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
        return web.json_response({'error': str(e)}, status=500)

    out = []
    for rid, uid, uname, mid, reason, wc, st, rev, created, reviewed in rows:
        mod = bg.get_member(mid)
        reviewer = bg.get_member(rev) if rev else None
        out.append({
            'id': rid, 'user_id': str(uid), 'username': uname,
            'mod_id': str(mid), 'mod_name': mod.display_name if mod else str(mid),
            'reason': reason, 'warn_count': wc, 'status': st,
            'reviewer_name': reviewer.display_name if reviewer else '',
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
    member = bg.get_member(int(s['user_id']))
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

        if action == 'approve':
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT ban3_days FROM punishment_settings WHERE guild_id=?", (guild_id,)
                ) as c:
                    pr = await c.fetchone()
            ban_days = (pr[0] if pr and pr[0] else 30)
            unban_at = (datetime.datetime.utcnow() +
                        datetime.timedelta(days=ban_days)).isoformat()
            try:
                target = bg.get_member(uid) or await bot.fetch_user(uid)
                await bg.ban(target, reason=f"[Заявка #{rid}] {reason}")
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

        new_status = 'approved' if action == 'approve' else 'rejected'
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                UPDATE ban_requests SET status=?, reviewer_id=?, reviewed_at=?
                WHERE id=? AND guild_id=?
            """, (new_status, int(s['user_id']), now, rid, guild_id))
            await db.commit()

        return web.json_response({'ok': True, 'status': new_status,
                                  'banned': ban_ok, 'ban_days': ban_days})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


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
    member = bg.get_member(int(s['user_id']))
    if not member:
        return web.json_response({'error': 'Forbidden'}, status=403)

    try:
        data = await request.json()
    except Exception:
        data = {}
    note = data.get('note', '')

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

        # Право на решение зависит от типа наказания
        p = member.guild_permissions
        if atype in ('BAN', 'TEMPBAN'):
            allowed = p.ban_members
        elif atype == 'MUTE':
            allowed = p.moderate_members or p.ban_members
        else:  # WARN, KICK и прочее
            allowed = p.manage_messages or p.moderate_members or p.ban_members
        if not (allowed or p.administrator or p.manage_guild):
            return web.json_response(
                {'error': 'missing_permission', 'need': atype}, status=403)

        now = datetime.datetime.utcnow().isoformat()
        new_status = 'accepted' if action == 'accept' else 'rejected'

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
                    m = bg.get_member(uid)
                    if m and m.is_timed_out():
                        try:
                            await m.timeout(None, reason=f"Апелляция #{appeal_id}")
                        except Exception:
                            pass
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

        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                UPDATE appeals SET status=?, reviewer_id=?,
                                    reviewer_note=?, reviewed_at=?
                WHERE id=? AND guild_id=?
            """, (new_status, int(s['user_id']), note, now, appeal_id, guild_id))
            await db.commit()

        # Уведомляем участника в DM
        try:
            user = bot.get_user(uid) or await bot.fetch_user(uid)
            if user:
                guild_name = bg.name
                if action == 'accept':
                    msg = f"✅ Твоя апелляция #{appeal_id} на сервере **{guild_name}** **принята**. Наказание снято."
                else:
                    msg = f"❌ Твоя апелляция #{appeal_id} на сервере **{guild_name}** **отклонена**."
                if note:
                    msg += f"\n\n**Комментарий:** {note}"
                await user.send(msg)
        except Exception:
            pass

        return web.json_response({'ok': True, 'status': new_status})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


# ── API: appeals ──────────────────────────────────────────────

@require_auth
async def api_appeals(request):
    guild_id = int(request.match_info['guild_id'])
    s        = request['session']
    bot      = request.app['bot']

    bg = bot.get_guild(guild_id)
    if not bg:
        return web.json_response([])
    member = bg.get_member(int(s['user_id']))
    if not member or not member.guild_permissions.manage_messages:
        return web.json_response({'error': 'Forbidden'}, status=403)

    status_filter = request.rel_url.query.get('status', '')
    try:
        async with aiosqlite.connect(DB_PATH) as db:
            if status_filter:
                async with db.execute("""
                    SELECT id, user_id, username, action_type, status,
                           appeal_text, reviewer_note, submitted_at, reviewed_at
                    FROM appeals WHERE guild_id=? AND status=?
                    ORDER BY id DESC LIMIT 50
                """, (guild_id, status_filter)) as c:
                    rows = await c.fetchall()
            else:
                async with db.execute("""
                    SELECT id, user_id, username, action_type, status,
                           appeal_text, reviewer_note, submitted_at, reviewed_at
                    FROM appeals WHERE guild_id=?
                    ORDER BY CASE status WHEN 'pending' THEN 0 ELSE 1 END, id DESC
                    LIMIT 50
                """, (guild_id,)) as c:
                    rows = await c.fetchall()
        result = []
        for aid, uid, uname, atype, astatus, atext, rnote, sub_at, rev_at in rows:
            m = bg.get_member(uid)
            result.append({
                'id':            aid,
                'user_id':       uid,
                'username':      m.display_name if m else uname,
                'action_type':   atype,
                'status':        astatus,
                'appeal_text':   atext[:300] if atext else '',
                'reviewer_note': rnote,
                'submitted_at':  sub_at,
                'reviewed_at':   rev_at,
            })
        return web.json_response(result)
    except Exception:
        return web.json_response([])

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

    # API
    app.router.add_get('/api/stats',                              api_stats)
    app.router.add_get('/api/me',                                 api_me)
    app.router.add_get('/api/refresh',                            api_refresh)
    app.router.add_get('/api/guilds',                             api_guilds)
    app.router.add_get('/api/guild/{guild_id}/settings',          api_guild_settings_get)
    app.router.add_post('/api/guild/{guild_id}/settings',         api_guild_settings_post)
    app.router.add_post('/api/guild/{guild_id}/security',         api_security_save)
    app.router.add_get('/api/guild/{guild_id}/channels',          api_channels)
    app.router.add_get('/api/guild/{guild_id}/modlog',            api_modlog)
    app.router.add_get('/api/guild/{guild_id}/invites',           api_invites)
    app.router.add_get('/api/guild/{guild_id}/twins',             api_twins)
    app.router.add_get('/api/guild/{guild_id}/appeals',           api_appeals)
    app.router.add_post('/api/guild/{guild_id}/appeal/{appeal_id}/{action}', api_appeal_action)
    app.router.add_get('/api/guild/{guild_id}/automation',                    api_automation_get)
    app.router.add_post('/api/guild/{guild_id}/automation',                   api_automation_post)
    app.router.add_get('/api/guild/{guild_id}/members/search',                api_member_search)
    app.router.add_get('/api/guild/{guild_id}/member/{uid}',                  api_member_lookup)
    app.router.add_get('/api/guild/{guild_id}/banrequests',                   api_ban_requests)
    app.router.add_post('/api/guild/{guild_id}/banrequest/{rid}/{action}',    api_ban_request_action)

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
