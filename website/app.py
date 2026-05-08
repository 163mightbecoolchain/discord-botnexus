"""
Witness Website Server
Простой сервер для отдачи HTML страниц.
API запросы идут напрямую на Railway бот (BOT_API_URL).
"""
import os
import aiofiles
from aiohttp import web

BOT_API_URL = os.getenv("BOT_API_URL", "")
PORT        = int(os.getenv("PORT", 8080))


def inject_bot_url(html: str) -> str:
    """Подставляет BOT_API_URL в HTML перед отдачей"""
    return html.replace("__BOT_API_URL__", BOT_API_URL)


async def serve_html(filename: str) -> web.Response:
    path = os.path.join(os.path.dirname(__file__), filename)
    if not os.path.exists(path):
        raise web.HTTPNotFound()
    async with aiofiles.open(path) as f:
        html = await f.read()
    return web.Response(text=inject_bot_url(html), content_type="text/html")


async def handle_index(request):
    return await serve_html("index.html")


async def handle_dashboard(request):
    return await serve_html("dashboard.html")


async def handle_login(request):
    """Редиректит на OAuth2 на боте"""
    if not BOT_API_URL:
        return web.Response(text="BOT_API_URL not configured", status=500)
    raise web.HTTPFound(f"{BOT_API_URL}/login")


async def handle_health(request):
    return web.Response(text="OK", status=200)


app = web.Application()
app.router.add_get("/",          handle_index)
app.router.add_get("/dashboard", handle_dashboard)
app.router.add_get("/login",     handle_login)
app.router.add_get("/health",    handle_health)

if __name__ == "__main__":
    print(f"✅ Witness Website → port {PORT}")
    print(f"   BOT_API_URL: {BOT_API_URL or '⚠️  not set!'}")
    web.run_app(app, host="0.0.0.0", port=PORT)
