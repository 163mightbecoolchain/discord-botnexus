"""
Дизайн-система: цвета, эмбеды, общие кнопки и пагинация.
"""

import discord
import os, asyncio, random, time, json, datetime

# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
#  ДИЗАЙН-СИСТЕМА
# ━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

class C:
    """Цветовая палитра Witness"""
    PRIMARY   = 0x5865F2   # Discord blurple — основной цвет бота
    SUCCESS   = 0x57F287   # Зелёный — успех, прибыль
    DANGER    = 0xED4245   # Красный — ошибка, убыток
    WARNING   = 0xFEE75C   # Жёлтый — предупреждение, устаревшие данные
    INFO      = 0x00B0F4   # Голубой — информация, Albion
    GOLD      = 0xF0B232   # Золотой — Pro, награды, топ
    MUTED     = 0x36393F   # Тёмный — нейтральный
    PREMIUM   = 0x00E5FF   # Циан — Premium
    PRO       = 0xFFD700   # Золото — Pro tier
    FREE      = 0x6b7fa3   # Серый — Free tier


def bar(value: float, max_val: float = 100, width: int = 10, filled: str = "█", empty: str = "░") -> str:
    """Прогресс-бар: bar(75) → ████████░░ 75%"""
    if max_val <= 0: return empty * width
    pct = min(value / max_val, 1.0)
    filled_n = round(pct * width)
    return filled * filled_n + empty * (width - filled_n)


def profit_color(pct: float) -> int:
    """Цвет по % профита"""
    if pct >= 30:  return C.SUCCESS
    if pct >= 10:  return C.GOLD
    if pct >= 0:   return C.INFO
    return C.DANGER


def make_embed(title: str = "", description: str = "", color: int = C.PRIMARY,
               footer: str = "", thumbnail: str = "") -> discord.Embed:
    """Создаёт эмбед в едином стиле Witness"""
    e = discord.Embed(title=title or None, description=description or None, color=color,
                      timestamp=datetime.datetime.utcnow())
    ts = datetime.datetime.utcnow().strftime("%d.%m.%Y %H:%M UTC")
    e.set_footer(text=f"Witness · {footer + ' · ' if footer else ''}{ts}")
    if thumbnail:
        e.set_thumbnail(url=thumbnail)
    return e


def build_embed(color: int = C.PRIMARY, description: str = "",
                title: str = "", footer: str = "", thumbnail: str = "") -> discord.Embed:
    """build_embed — современный алиас make_embed. set_author() вызывается отдельно."""
    return make_embed(title=title, description=description,
                      color=color, footer=footer, thumbnail=thumbnail)


def tier_badge(tier: int) -> str:
    return {0: "🔓 Free", 1: "⭐ Premium", 2: "💎 Pro"}.get(tier, "?")


class PaginatedView(discord.ui.View):
    """Кнопки пагинации для больших результатов"""

    def __init__(self, pages: list[discord.Embed], current: int = 0):
        super().__init__(timeout=180)
        self.pages = pages
        self.current = current
        self._update_buttons()

    def _update_buttons(self):
        self.prev_btn.disabled = self.current == 0
        self.next_btn.disabled = self.current >= len(self.pages) - 1
        self.counter.label = f"{self.current + 1} / {len(self.pages)}"

    @discord.ui.button(label="◀", style=discord.ButtonStyle.secondary)
    async def prev_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current -= 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.current], view=self)

    @discord.ui.button(label="1 / 1", style=discord.ButtonStyle.secondary, disabled=True)
    async def counter(self, interaction: discord.Interaction, button: discord.ui.Button):
        pass

    @discord.ui.button(label="▶", style=discord.ButtonStyle.secondary)
    async def next_btn(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.current += 1
        self._update_buttons()
        await interaction.response.edit_message(embed=self.pages[self.current], view=self)

    async def on_timeout(self):
        for item in self.children:
            item.disabled = True


class ConfirmView(discord.ui.View):
    """Кнопки подтверждения Да/Нет"""

    def __init__(self):
        super().__init__(timeout=30)
        self.confirmed = None

    @discord.ui.button(label="✅ Да", style=discord.ButtonStyle.success)
    async def confirm(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = True
        self.stop()
        await interaction.response.defer()

    @discord.ui.button(label="❌ Нет", style=discord.ButtonStyle.danger)
    async def cancel(self, interaction: discord.Interaction, button: discord.ui.Button):
        self.confirmed = False
        self.stop()
        await interaction.response.defer()
