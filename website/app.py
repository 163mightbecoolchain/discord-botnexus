"""
Witness Website Server
"""
import os
from aiohttp import web, ClientSession, ClientTimeout

BOT_API_URL = os.getenv("BOT_API_URL", "").rstrip("/")
if BOT_API_URL and not BOT_API_URL.startswith("http"):
    BOT_API_URL = "https://" + BOT_API_URL
BOT_ID      = os.getenv("BOT_ID", "")
PORT        = int(os.getenv("PORT", 8080))
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))

# Права при приглашении: только то, чем бот пользуется (без Administrator).
# Кик, бан, таймаут, роли (карантин, реакции), каналы (тикеты, slowmode),
# сервер (AutoMod, инвайты), журнал аудита, сообщения, вложения (бэкапы).
# Тот же набор — в witness/config.py и website/app.py.
INVITE_PERMISSIONS = 1374658358518
INVITE_URL  = (
    f"https://discord.com/api/oauth2/authorize"
    f"?client_id={BOT_ID}&permissions={INVITE_PERMISSIONS}&scope=bot%20applications.commands"
    if BOT_ID else "#"
)
SUPPORT_URL = os.getenv("SUPPORT_URL", "https://discord.gg/witness")


def read_file(filename: str) -> str:
    with open(os.path.join(BASE_DIR, filename), encoding="utf-8") as f:
        html = f.read()
    html = html.replace("__BOT_API_URL__", BOT_API_URL)
    html = html.replace("__INVITE_URL__",  INVITE_URL)
    html = html.replace("__SUPPORT_URL__", SUPPORT_URL)
    html = html.replace("__BOT_ID__",      BOT_ID)
    return html


CSP = (
    "default-src 'self'; "
    "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://fonts.googleapis.com; "
    "style-src 'self' 'unsafe-inline' https://fonts.googleapis.com https://fonts.gstatic.com; "
    "font-src 'self' https://fonts.gstatic.com; "
    f"connect-src 'self' {BOT_API_URL} https://discord.com https://cdn.discordapp.com; "
    "img-src 'self' data: https://cdn.discordapp.com https://render.albiononline.com; "
    "frame-ancestors 'none';"
)


# Discord Activity открывается в iframe, поэтому для неё нужны
# отдельные заголовки. Обычные страницы сайта остаются закрытыми
# от встраивания — их политику не трогаем.
CSP_ACTIVITY = (
    "default-src 'self'; "
    "script-src 'self' 'unsafe-inline' 'unsafe-eval'; "
    "style-src 'self' 'unsafe-inline'; "
    "connect-src 'self' https://*.discordsays.com https://discord.com; "
    "img-src 'self' data: https://cdn.discordapp.com https://*.discordsays.com; "
    "frame-ancestors https://discord.com https://*.discord.com https://*.discordsays.com;"
)


async def html_response(filename: str, activity: bool = False) -> web.Response:
    resp = web.Response(text=read_file(filename), content_type="text/html")
    if activity:
        resp.headers["Content-Security-Policy"] = CSP_ACTIVITY
        # X-Frame-Options намеренно не ставим: он запретил бы iframe Discord
    else:
        resp.headers["Content-Security-Policy"] = CSP
        resp.headers["X-Frame-Options"]         = "DENY"
    resp.headers["X-Content-Type-Options"]  = "nosniff"
    return resp


async def handle_index(request):
    # Discord открывает Activity по КОРНЮ сопоставленного домена и передаёт
    # свои параметры (frame_id, instance_id, platform). Если они есть —
    # это запуск из клиента, отдаём activity.html вместо лендинга.
    # Лендинг в iframe всё равно не открылся бы: на нём стоит запрет встраивания.
    q = request.rel_url.query
    if "frame_id" in q or "instance_id" in q:
        return await html_response("activity.html", activity=True)
    return await html_response("index.html")

async def handle_privacy(request):
    return await html_response("privacy.html")

async def handle_activity(request):
    """Discord Activity — встраиваемая версия панели модерации"""
    return await html_response("activity.html", activity=True)


async def handle_static(request):
    """
    Статика для Activity (SDK). Discord блокирует загрузку скриптов
    со сторонних доменов, поэтому библиотека лежит у нас же.
    """
    name = request.match_info.get("name", "")
    if "/" in name or ".." in name or not name.endswith(".js"):
        return web.Response(status=404)
    path = os.path.join(BASE_DIR, "static", name)
    if not os.path.exists(path):
        return web.Response(status=404)
    with open(path, "r", encoding="utf-8") as f:
        body = f.read()
    resp = web.Response(text=body, content_type="application/javascript")
    resp.headers["Cache-Control"] = "public, max-age=86400"
    return resp

async def handle_callback(request):
    """
    Страховка от путаницы в настройках.
    /callback живёт на сервисе бота, но если в Dev Portal (и в SITE_URL)
    указан адрес САЙТА — Discord приведёт пользователя сюда.
    Пробрасываем code боту, чтобы авторизация всё равно завершилась.
    """
    if not BOT_API_URL:
        return web.Response(text="BOT_API_URL not set", status=500)
    qs = request.rel_url.query_string
    raise web.HTTPFound(f"{BOT_API_URL}/callback" + (f"?{qs}" if qs else ""))

async def handle_dashboard(request):
    return await html_response("dashboard.html")

RESTARTING_HTML = """<!DOCTYPE html>
<html lang="ru"><head><meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<meta http-equiv="refresh" content="10">
<title>Witness перезапускается</title>
<style>
body{margin:0;min-height:100vh;display:grid;place-items:center;background:#0a0e14;color:#e0e6ed;
font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;padding:16px;box-sizing:border-box}
.c{max-width:420px;text-align:center}
.s{width:28px;height:28px;margin:0 auto 20px;border-radius:50%;border:3px solid #2a3441;border-top-color:#00d9a3;
animation:r .8s linear infinite}@keyframes r{to{transform:rotate(360deg)}}
h1{font-size:1.3rem;margin:0 0 8px}p{color:#a8b3c1;margin:0 0 20px;line-height:1.6}a{color:#00d9a3}
</style></head><body><div class="c"><div class="s"></div>
<h1>Бот перезапускается</h1>
<p>Обычно это занимает 1–3 минуты после обновления. Страница обновится сама и продолжит вход.</p>
<a href="/">← На главную</a></div></body></html>"""


async def bot_is_up() -> bool:
    try:
        timeout = ClientTimeout(total=4)
        async with ClientSession(timeout=timeout) as s:
            async with s.get(f"{BOT_API_URL}/health") as r:
                return r.status < 500
    except Exception:
        return False


async def handle_login(request):
    if not BOT_API_URL:
        return web.Response(text="BOT_API_URL not set", status=500)
    # Пока бот перезапускается, Railway отдаёт на /login голый 502 —
    # показываем понятную страницу, которая сама повторит вход
    if not await bot_is_up():
        resp = web.Response(text=RESTARTING_HTML, content_type="text/html", status=503)
        resp.headers["Retry-After"] = "10"
        resp.headers["Cache-Control"] = "no-store"
        return resp
    raise web.HTTPFound(f"{BOT_API_URL}/login")

async def handle_logout(request):
    # Чистим cookie на боте и возвращаемся на главную сайта
    if BOT_API_URL:
        raise web.HTTPFound(f"{BOT_API_URL}/logout")
    raise web.HTTPFound("/")

async def handle_health(request):
    return web.Response(text="OK", status=200)

app = web.Application()
app.router.add_get("/",          handle_index)
app.router.add_get("/dashboard", handle_dashboard)
app.router.add_get("/privacy",   handle_privacy)
app.router.add_get("/activity",  handle_activity)
app.router.add_get("/static/{name}", handle_static)
app.router.add_get("/callback",  handle_callback)
app.router.add_get("/login",     handle_login)
app.router.add_get("/logout",    handle_logout)
app.router.add_get("/health",    handle_health)

if __name__ == "__main__":
    print(f"✅ Witness Website → port {PORT}")
    print(f"   BOT_API_URL: {BOT_API_URL or '⚠️  not set'}")
    print(f"   BOT_ID:      {BOT_ID or '⚠️  not set'}")
    web.run_app(app, host="0.0.0.0", port=PORT)
