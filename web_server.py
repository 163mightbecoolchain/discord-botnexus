"""
Witness Web Server
━━━━━━━━━━━━━━━━━
Aiohttp web server встроенный в бота.
Запускается в on_ready параллельно с Discord ботом.

Routes:
  GET  /                    — Landing page
  GET  /dashboard           — Dashboard (требует auth)
  GET  /login               — Redirect → Discord OAuth2
  GET  /callback            — OAuth2 callback
  GET  /logout              — Clear session

  GET  /api/stats           — Bot stats (публичный)
  GET  /api/me              — Current user info (требует auth)
  GET  /api/guilds          — User's guilds with bot (требует auth)
  GET  /api/guild/:id/settings  — Guild settings
  POST /api/guild/:id/settings  — Update guild settings
  GET  /api/guild/:id/modlog    — Moderation log
  GET  /api/guild/:id/invites   — Invite stats
  GET  /api/guild/:id/twins     — Twin links

Environment variables needed:
  BOT_ID                  — Discord Application ID
  DISCORD_CLIENT_SECRET   — OAuth2 client secret
  HMAC_SECRET             — For session signing
  PORT                    — HTTP port (default 8080)
  SITE_URL                — Public URL (e.g. https://witness.up.railway.app)
"""

import os
import hmac
import json
import time
import hashlib
import asyncio
import aiohttp
import aiofiles
import aiosqlite
import datetime
from pathlib import Path

from aiohttp import web

# ── Config ────────────────────────────────────────────────────
BOT_ID         = os.getenv("BOT_ID", "")
CLIENT_SECRET  = os.getenv("DISCORD_CLIENT_SECRET", "")
HMAC_KEY       = os.getenv("HMAC_SECRET", "witness_secret").encode()
PORT           = int(os.getenv("PORT", 8080))
SITE_URL       = os.getenv("SITE_URL", f"http://localhost:{PORT}")
INVITE_URL     = f"https://discord.com/api/oauth2/authorize?client_id={BOT_ID}&permissions=8&scope=bot%20applications.commands" if BOT_ID else "#"
SUPPORT_URL    = os.getenv("SUPPORT_URL", "https://discord.gg/witness")

OAUTH_URL      = (
    f"https://discord.com/api/oauth2/authorize"
    f"?client_id={BOT_ID}"
    f"&redirect_uri={SITE_URL}/callback"
    f"&response_type=code"
    f"&scope=identify+guilds"
)

# Домен сайта для CORS (если сайт на отдельном сервисе)
WEBSITE_URL    = os.getenv("WEBSITE_URL", "")  # напр. https://witness-website.up.railway.app

TEMPLATES_DIR  = Path(__file__).parent / "witness_web" / "templates"
DB_PATH        = os.getenv("DB_PATH", "witnessbot.db")

# ── Session helpers ───────────────────────────────────────────

def sign_session(data: dict) -> str:
    """Создаёт подписанный session token"""
    payload = json.dumps(data, separators=(',', ':'))
    sig = hmac.new(HMAC_KEY, payload.encode(), hashlib.sha256).hexdigest()
    import base64
    encoded = base64.urlsafe_b64encode(payload.encode()).decode()
    return f"{encoded}.{sig}"


def verify_session(token: str) -> dict | None:
    """Проверяет и декодирует session token"""
    try:
        import base64
        parts = token.split('.')
        if len(parts) != 2:
            return None
        encoded, sig = parts
        payload = base64.urlsafe_b64decode(encoded.encode()).decode()
        expected = hmac.new(HMAC_KEY, payload.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return None
        data = json.loads(payload)
        # Token expires in 7 days
        if time.time() - data.get('ts', 0) > 7 * 86400:
            return None
        return data
    except Exception:
        return None


def get_session(request: web.Request) -> dict | None:
    # Сначала проверяем Bearer токен (для cross-domain сайта)
    auth_header = request.headers.get('Authorization', '')
    if auth_header.startswith('Bearer '):
        token = auth_header[7:]
        return verify_session(token)
    # Fallback — cookie (для same-domain)
    token = request.cookies.get('ws_session')
    if not token:
        return None
    return verify_session(token)


def require_auth(func):
    """Decorator — редирект на /login если не авторизован"""
    async def wrapper(request):
        session = get_session(request)
        if not session:
            if request.path.startswith('/api/'):
                return web.json_response({'error': 'unauthorized'}, status=401)
            raise web.HTTPFound('/login')
        request['session'] = session
        return await func(request)
    return wrapper


async def read_template(name: str) -> str:
    """Читает HTML шаблон и подставляет переменные"""
    path = TEMPLATES_DIR / name
    if not path.exists():
        raise FileNotFoundError(f"Template not found: {name}")
    async with aiofiles.open(path) as f:
        html = await f.read()
    # Подставляем переменные
    html = html.replace('__INVITE_URL__', INVITE_URL)
    html = html.replace('__SUPPORT_URL__', SUPPORT_URL)
    html = html.replace('__BOT_ID__', BOT_ID)
    return html


# ── OAuth2 ────────────────────────────────────────────────────

async def exchange_code(code: str) -> dict | None:
    """Обменивает OAuth2 code на access token"""
    async with aiohttp.ClientSession() as s:
        async with s.post('https://discord.com/api/oauth2/token', data={
            'client_id':     BOT_ID,
            'client_secret': CLIENT_SECRET,
            'grant_type':    'authorization_code',
            'code':          code,
            'redirect_uri':  f"{SITE_URL}/callback",
        }) as r:
            if r.status != 200:
                return None
            return await r.json()


async def get_discord_user(access_token: str) -> dict | None:
    async with aiohttp.ClientSession() as s:
        async with s.get('https://discord.com/api/users/@me',
                         headers={'Authorization': f'Bearer {access_token}'}) as r:
            if r.status != 200:
                return None
            return await r.json()


async def get_discord_guilds(access_token: str) -> list:
    async with aiohttp.ClientSession() as s:
        async with s.get('https://discord.com/api/users/@me/guilds',
                         headers={'Authorization': f'Bearer {access_token}'}) as r:
            if r.status != 200:
                return []
            return await r.json()


# ── Route handlers ────────────────────────────────────────────

async def handle_index(request):
    try:
        html = await read_template('index.html')
        return web.Response(text=html, content_type='text/html')
    except FileNotFoundError:
        return web.Response(text='<h1>Witness Bot</h1><p>Landing page not found.</p>',
                            content_type='text/html')


async def handle_dashboard(request):
    session = get_session(request)
    if not session:
        raise web.HTTPFound('/login')
    try:
        html = await read_template('dashboard.html')
        return web.Response(text=html, content_type='text/html')
    except FileNotFoundError:
        return web.Response(text='<h1>Dashboard</h1>', content_type='text/html')


async def handle_login(request):
    if not BOT_ID or not CLIENT_SECRET:
        return web.Response(
            text='<p style="color:red">OAuth2 not configured. Set BOT_ID and DISCORD_CLIENT_SECRET.</p>',
            content_type='text/html'
        )
    raise web.HTTPFound(OAUTH_URL)


async def handle_callback(request):
    code = request.rel_url.query.get('code')
    error = request.rel_url.query.get('error')

    if error or not code:
        raise web.HTTPFound('/?error=oauth_denied')

    token_data = await exchange_code(code)
    if not token_data or 'access_token' not in token_data:
        raise web.HTTPFound('/?error=oauth_failed')

    user = await get_discord_user(token_data['access_token'])
    if not user:
        raise web.HTTPFound('/?error=user_fetch_failed')

    # Создаём session
    session_data = {
        'user_id':      user['id'],
        'username':     user['username'],
        'discriminator':user.get('discriminator', '0'),
        'avatar':       user.get('avatar'),
        'access_token': token_data['access_token'],
        'ts':           time.time(),
    }
    token = sign_session(session_data)

    if WEBSITE_URL:
        # Cross-domain: передаём токен через URL параметр
        # Сайт сохранит его в localStorage
        import urllib.parse
        redirect_url = f"{WEBSITE_URL}/dashboard?token={urllib.parse.quote(token)}"
        raise web.HTTPFound(redirect_url)
    else:
        # Тот же домен: cookie работает нормально
        response = web.HTTPFound("/dashboard")
        response.set_cookie('ws_session', token, max_age=7*86400,
                           httponly=True, samesite='Lax', path='/')
        raise response


async def handle_logout(request):
    response = web.HTTPFound('/')
    response.del_cookie('ws_session', path='/')
    raise response


# ── API handlers ──────────────────────────────────────────────

async def api_stats(request):
    """Публичная статистика бота"""
    try:
        bot = request.app['bot']
        guilds = len(bot.guilds)
        members = sum(g.member_count or 0 for g in bot.guilds)
        latency = round(bot.latency * 1000)
        return web.json_response({
            'guilds':  guilds,
            'members': members,
            'latency': latency,
            'status':  'online' if bot.is_ready() else 'connecting',
        })
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


@require_auth
async def api_me(request):
    s = request['session']
    return web.json_response({
        'id':            s['user_id'],
        'username':      s['username'],
        'discriminator': s['discriminator'],
        'avatar':        s['avatar'],
    })


@require_auth
async def api_guilds(request):
    """Серверы пользователя где стоит бот"""
    s = request['session']
    bot = request.app['bot']

    try:
        user_guilds = await get_discord_guilds(s['access_token'])
    except Exception:
        return web.json_response([], status=200)

    # Фильтруем только те где есть бот и пользователь — admin
    bot_guild_ids = {g.id for g in bot.guilds}
    result = []
    for ug in user_guilds:
        guild_id = int(ug['id'])
        if guild_id not in bot_guild_ids:
            continue
        # Проверяем права (0x8 = Administrator, 0x20 = Manage Server)
        perms = int(ug.get('permissions', 0))
        if not (perms & 0x8 or perms & 0x20):
            continue

        # Достаём tier из БД
        tier = 0
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                async with db.execute(
                    "SELECT tier FROM subscriptions WHERE guild_id=?", (guild_id,)
                ) as c:
                    row = await c.fetchone()
                    if row:
                        tier = row[0]
        except Exception:
            pass

        bot_guild = bot.get_guild(guild_id)
        result.append({
            'id':           ug['id'],
            'name':         ug['name'],
            'icon':         ug.get('icon'),
            'member_count': bot_guild.member_count if bot_guild else None,
            'tier':         tier,
        })

    return web.json_response(result)


@require_auth
async def api_guild_settings_get(request):
    guild_id = int(request.match_info['guild_id'])
    s = request['session']

    # Проверяем права через Discord guilds
    bot = request.app['bot']
    bot_guild = bot.get_guild(guild_id)
    if not bot_guild:
        return web.json_response({'error': 'Guild not found'}, status=404)
    member = bot_guild.get_member(int(s['user_id']))
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'No permission'}, status=403)

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute(
                "SELECT * FROM guild_settings WHERE guild_id=?", (guild_id,)
            ) as c:
                row = await c.fetchone()
                if row:
                    cols = [d[0] for d in c.description]
                    return web.json_response(dict(zip(cols, row)))

            # Получаем security settings
            async with db.execute(
                "SELECT settings FROM security_settings WHERE guild_id=?", (guild_id,)
            ) as c:
                sec_row = await c.fetchone()

        result = {'guild_id': guild_id, 'lang': 'ru', 'tickets_enabled': 1}
        if sec_row:
            result['security'] = json.loads(sec_row[0] or '{}')
        return web.json_response(result)
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


@require_auth
async def api_guild_settings_post(request):
    guild_id = int(request.match_info['guild_id'])
    s = request['session']
    bot = request.app['bot']

    bot_guild = bot.get_guild(guild_id)
    if not bot_guild:
        return web.json_response({'error': 'Not found'}, status=404)
    member = bot_guild.get_member(int(s['user_id']))
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'Forbidden'}, status=403)

    try:
        data = await request.json()
        async with aiosqlite.connect(DB_PATH) as db:
            # Обновляем разрешённые поля
            allowed = ['lang', 'tickets_enabled', 'lockdown', 'starboard_threshold']
            for key in allowed:
                if key in data:
                    await db.execute(f"""
                        INSERT INTO guild_settings (guild_id, {key}) VALUES (?, ?)
                        ON CONFLICT(guild_id) DO UPDATE SET {key}=excluded.{key}
                    """, (guild_id, data[key]))
            await db.commit()
        return web.json_response({'ok': True})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)


@require_auth
async def api_modlog(request):
    guild_id = int(request.match_info['guild_id'])
    s = request['session']
    bot = request.app['bot']

    bot_guild = bot.get_guild(guild_id)
    if not bot_guild:
        return web.json_response([])
    member = bot_guild.get_member(int(s['user_id']))
    if not member or not member.guild_permissions.manage_messages:
        return web.json_response({'error': 'Forbidden'}, status=403)

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT action, user_id, mod_id, reason, duration, created_at
                FROM modlog WHERE guild_id=?
                ORDER BY id DESC LIMIT 50
            """, (guild_id,)) as c:
                rows = await c.fetchall()

        result = []
        for action, uid, mid, reason, dur, ts in rows:
            u = bot_guild.get_member(uid)
            m = bot_guild.get_member(mid)
            result.append({
                'action':     action,
                'user_id':    uid,
                'user_name':  u.display_name if u else None,
                'mod_id':     mid,
                'mod_name':   m.display_name if m else ('Auto' if mid == 0 else None),
                'reason':     reason,
                'duration':   dur,
                'created_at': ts,
            })
        return web.json_response(result)
    except Exception as e:
        return web.json_response([], status=200)


@require_auth
async def api_invites(request):
    guild_id = int(request.match_info['guild_id'])
    s = request['session']
    bot = request.app['bot']

    bot_guild = bot.get_guild(guild_id)
    if not bot_guild:
        return web.json_response([])
    member = bot_guild.get_member(int(s['user_id']))
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'Forbidden'}, status=403)

    try:
        async with aiosqlite.connect(DB_PATH) as db:
            async with db.execute("""
                SELECT invite_code, inviter_id, inviter_name,
                       COUNT(*) as uses, MAX(joined_at) as last_used,
                       MAX(note) as note
                FROM invite_log WHERE guild_id=?
                GROUP BY invite_code
                ORDER BY uses DESC LIMIT 50
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
    except Exception as e:
        return web.json_response([])


@require_auth
async def api_twins(request):
    guild_id = int(request.match_info['guild_id'])
    s = request['session']
    bot = request.app['bot']

    bot_guild = bot.get_guild(guild_id)
    if not bot_guild:
        return web.json_response([])
    member = bot_guild.get_member(int(s['user_id']))
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
            ma = bot_guild.get_member(ua)
            mb = bot_guild.get_member(ub)
            result.append({
                'id':          lid,
                'user_a':      ua,
                'user_a_name': ma.display_name if ma else None,
                'user_b':      ub,
                'user_b_name': mb.display_name if mb else None,
                'similarity':  sim,
                'reasons':     reasons,
                'confirmed':   conf,
                'false_positive': fp,
                'detected_at': ts,
            })
        return web.json_response(result)
    except Exception as e:
        return web.json_response([])



@require_auth
async def api_channels(request):
    """Список каналов сервера для dropdown"""
    guild_id = int(request.match_info['guild_id'])
    s = request['session']
    bot = request.app['bot']
    bot_guild = bot.get_guild(guild_id)
    if not bot_guild:
        return web.json_response([], status=200)
    member = bot_guild.get_member(int(s['user_id']))
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'Forbidden'}, status=403)
    channels = [
        {'id': str(c.id), 'name': c.name, 'type': str(c.type)}
        for c in sorted(bot_guild.channels, key=lambda x: x.position)
        if str(c.type) in ('text', 'category')
    ]
    roles = [
        {'id': str(r.id), 'name': r.name}
        for r in bot_guild.roles[1:]  # убираем @everyone
    ]
    return web.json_response({'channels': channels, 'roles': roles})


@require_auth
async def api_security_save(request):
    """Сохраняет настройки security toggles"""
    guild_id = int(request.match_info['guild_id'])
    s = request['session']
    bot = request.app['bot']
    bot_guild = bot.get_guild(guild_id)
    if not bot_guild:
        return web.json_response({'error': 'Not found'}, status=404)
    member = bot_guild.get_member(int(s['user_id']))
    if not member or not member.guild_permissions.manage_guild:
        return web.json_response({'error': 'Forbidden'}, status=403)
    try:
        data = await request.json()
        settings_json = json.dumps(data)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("""
                INSERT INTO security_settings (guild_id, settings)
                VALUES (?, ?)
                ON CONFLICT(guild_id) DO UPDATE SET settings=excluded.settings
            """, (guild_id, settings_json))
            await db.commit()
        return web.json_response({'ok': True})
    except Exception as e:
        return web.json_response({'error': str(e)}, status=500)

# ── App factory ───────────────────────────────────────────────

@web.middleware
async def cors_middleware(request: web.Request, handler):
    """CORS — разрешает запросы с сайта на отдельном домене"""
    # Preflight OPTIONS запрос
    if request.method == "OPTIONS":
        resp = web.Response()
    else:
        try:
            resp = await handler(request)
        except web.HTTPException as e:
            raise

    origin = request.headers.get("Origin", "")
    allowed = [SITE_URL, WEBSITE_URL] if WEBSITE_URL else [SITE_URL]

    if origin in allowed or not origin:
        resp.headers["Access-Control-Allow-Origin"]      = origin or "*"
        resp.headers["Access-Control-Allow-Credentials"] = "true"
        resp.headers["Access-Control-Allow-Methods"]     = "GET, POST, OPTIONS"
        resp.headers["Access-Control-Allow-Headers"]     = "Content-Type, Cookie"

    return resp


def create_app(bot) -> web.Application:
    app = web.Application(middlewares=[cors_middleware])
    app['bot'] = bot

    # Routes
    app.router.add_get('/',            handle_index)
    app.router.add_get('/dashboard',   handle_dashboard)
    app.router.add_get('/login',       handle_login)
    app.router.add_get('/callback',    handle_callback)
    app.router.add_get('/logout',      handle_logout)
    app.router.add_get('/health',      api_stats)

    # API
    app.router.add_get('/api/stats',                              api_stats)
    app.router.add_get('/api/me',                                 api_me)
    app.router.add_get('/api/guilds',                             api_guilds)
    app.router.add_get('/api/guild/{guild_id}/settings',          api_guild_settings_get)
    app.router.add_post('/api/guild/{guild_id}/settings',         api_guild_settings_post)
    app.router.add_get('/api/guild/{guild_id}/modlog',            api_modlog)
    app.router.add_get('/api/guild/{guild_id}/invites',           api_invites)
    app.router.add_get('/api/guild/{guild_id}/twins',             api_twins)
    app.router.add_get('/api/guild/{guild_id}/channels',          api_channels)
    app.router.add_post('/api/guild/{guild_id}/security',         api_security_save)

    return app


async def start_web_server(bot):
    """Запускается в on_ready как asyncio task"""
    app = create_app(bot)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    print(f"✅ Web server started → {SITE_URL} (port {PORT})")
    print(f"   OAuth2: {'✅ configured' if CLIENT_SECRET else '⚠️  DISCORD_CLIENT_SECRET not set'}")
    print(f"   Bot ID: {'✅ ' + BOT_ID if BOT_ID else '⚠️  BOT_ID not set'}")
