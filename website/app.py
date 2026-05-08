"""
Witness Website Server
"""
import os
from aiohttp import web

BOT_API_URL = os.getenv("BOT_API_URL", "")
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
    # Подставляем все переменные
    html = html.replace("__BOT_API_URL__", BOT_API_URL)
    html = html.replace("__INVITE_URL__",  INVITE_URL)
    html = html.replace("__SUPPORT_URL__", SUPPORT_URL)
    html = html.replace("__BOT_ID__",      BOT_ID)
    return html


async def handle_index(request):
    return web.Response(text=read_file("index.html"), content_type="text/html")

async def handle_dashboard(request):
    return web.Response(text=read_file("dashboard.html"), content_type="text/html")

async def handle_login(request):
    if not BOT_API_URL:
        return web.Response(text="BOT_API_URL not set", status=500)
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
    print(f"   BOT_API_URL: {BOT_API_URL or '⚠️  not set'}")
    print(f"   BOT_ID:      {BOT_ID or '⚠️  not set'}")
    print(f"   INVITE_URL:  {INVITE_URL}")
    web.run_app(app, host="0.0.0.0", port=PORT)
