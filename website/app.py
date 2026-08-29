"""
Witness Website Server
"""
import os
from aiohttp import web

BOT_API_URL = os.getenv("BOT_API_URL", "").rstrip("/")
if BOT_API_URL and not BOT_API_URL.startswith("http"):
    BOT_API_URL = "https://" + BOT_API_URL
BOT_ID      = os.getenv("BOT_ID", "")
PORT        = int(os.getenv("PORT", 8080))
BASE_DIR    = os.path.dirname(os.path.abspath(__file__))

INVITE_URL  = (
    f"https://discord.com/api/oauth2/authorize"
    f"?client_id={BOT_ID}&permissions=8&scope=bot%20applications.commands"
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


async def html_response(filename: str) -> web.Response:
    resp = web.Response(text=read_file(filename), content_type="text/html")
    resp.headers["Content-Security-Policy"] = CSP
    resp.headers["X-Frame-Options"]         = "DENY"
    resp.headers["X-Content-Type-Options"]  = "nosniff"
    return resp


async def handle_index(request):
    return await html_response("index.html")

async def handle_privacy(request):
    return await html_response("privacy.html")

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

async def handle_login(request):
    if not BOT_API_URL:
        return web.Response(text="BOT_API_URL not set", status=500)
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
app.router.add_get("/callback",  handle_callback)
app.router.add_get("/login",     handle_login)
app.router.add_get("/logout",    handle_logout)
app.router.add_get("/health",    handle_health)

if __name__ == "__main__":
    print(f"✅ Witness Website → port {PORT}")
    print(f"   BOT_API_URL: {BOT_API_URL or '⚠️  not set'}")
    print(f"   BOT_ID:      {BOT_ID or '⚠️  not set'}")
    web.run_app(app, host="0.0.0.0", port=PORT)
