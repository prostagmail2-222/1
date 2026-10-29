from __future__ import annotations

"""Single-file modular-in-spirit Anime Telegram bot for aiogram 3.x.

Install: pip install aiogram sqlalchemy aiosqlite asyncpg python-dotenv redis aiohttp openai
Run:     python anime_bot.py

Set BOT_TOKEN and OWNER_IDS as environment variables or fill the defaults below.
For production, set a strong DATABASE_URL and consider REDIS_URL.
"""

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.enums import ChatMemberStatus
from aiogram.exceptions import TelegramAPIError, TelegramRetryAfter
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)
from dotenv import load_dotenv
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
    select,
)
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship

# ------------------------------- CONFIG ------------------------------------

load_dotenv()

# You may put your values here, but environment variables / .env take priority.
BOT_TOKEN = os.getenv("BOT_TOKEN", "PUT_BOT_TOKEN_HERE").strip()
OWNER_IDS_RAW = os.getenv("OWNER_IDS", "8318241400").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite+aiosqlite:///./anime_bot.db").strip()
REDIS_URL = os.getenv("REDIS_URL", "").strip()
BOT_NAME = os.getenv("BOT_NAME", "Anime Bot").strip()
PAYMENT_INSTRUCTIONS = os.getenv(
    "PAYMENT_INSTRUCTIONS", "VIP to'lovi bo'yicha bot egasi bilan bog'laning."
).strip()
AI_PROVIDER = os.getenv("AI_PROVIDER", "off").strip().lower()  # off | openai | gemini
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "").strip()
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-4o-mini").strip()
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "").strip()
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash").strip()

try:
    OWNER_IDS = {int(value.strip()) for value in OWNER_IDS_RAW.split(",") if value.strip()}
except ValueError as exc:
    raise RuntimeError("OWNER_IDS vergul bilan ajratilgan Telegram ID raqamlari bo'lishi kerak.") from exc

if not BOT_TOKEN or BOT_TOKEN == "PUT_BOT_TOKEN_HERE":
    raise RuntimeError("BOT_TOKEN belgilanmagan. .env yoki muhit o'zgaruvchisiga haqiqiy token kiriting.")
if not OWNER_IDS:
    raise RuntimeError("OWNER_IDS ichida kamida bitta Owner Telegram ID bo'lishi kerak.")
if AI_PROVIDER not in {"off", "openai", "gemini"}:
    raise RuntimeError("AI_PROVIDER: off, openai yoki gemini bo'lishi kerak.")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)
log = logging.getLogger("anime_bot")

# --------------------------- DATABASE MODELS -------------------------------

class Base(DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"
    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[str | None] = mapped_column(String(64), nullable=True)
    full_name: Mapped[str] = mapped_column(String(256), default="")
    role: Mapped[str] = mapped_column(String(16), default="user", index=True)
    vip_until: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    last_seen_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())

    purchases: Mapped[list["VIPPurchase"]] = relationship(back_populates="user")


class Anime(Base):
    __tablename__ = "animes"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(64), unique=True, index=True)
    title: Mapped[str] = mapped_column(String(256), index=True)
    description: Mapped[str] = mapped_column(Text, default="")
    genres: Mapped[str] = mapped_column(String(512), default="")
    year: Mapped[int | None] = mapped_column(Integer, nullable=True)
    cover_file_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    views: Mapped[int] = mapped_column(Integer, default=0)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    episodes: Mapped[list["Episode"]] = relationship(
        back_populates="anime", cascade="all, delete-orphan", order_by="Episode.number"
    )


class Episode(Base):
    __tablename__ = "episodes"
    __table_args__ = (UniqueConstraint("anime_id", "number", name="uq_episode_anime_number"),)
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    anime_id: Mapped[int] = mapped_column(ForeignKey("animes.id", ondelete="CASCADE"), index=True)
    number: Mapped[int] = mapped_column(Integer)
    title: Mapped[str] = mapped_column(String(256), default="")
    file_id: Mapped[str] = mapped_column(String(256))
    file_type: Mapped[str] = mapped_column(String(16), default="video")
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    anime: Mapped[Anime] = relationship(back_populates="episodes")


class Channel(Base):
    __tablename__ = "channels"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    title: Mapped[str] = mapped_column(String(256))
    chat_id: Mapped[str] = mapped_column(String(128), unique=True, index=True)
    invite_url: Mapped[str] = mapped_column(String(512), default="")
    is_required: Mapped[bool] = mapped_column(Boolean, default=True)
    is_post_target: Mapped[bool] = mapped_column(Boolean, default=False)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class VIPPlan(Base):
    __tablename__ = "vip_plans"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    code: Mapped[str] = mapped_column(String(32), unique=True)
    title: Mapped[str] = mapped_column(String(128))
    days: Mapped[int] = mapped_column(Integer)
    price: Mapped[int] = mapped_column(Integer, default=0)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)


class VIPPurchase(Base):
    __tablename__ = "vip_purchases"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, autoincrement=True)
    user_id: Mapped[int] = mapped_column(ForeignKey("users.id", ondelete="CASCADE"), index=True)
    plan_id: Mapped[int] = mapped_column(ForeignKey("vip_plans.id"), index=True)
    status: Mapped[str] = mapped_column(String(24), default="awaiting_proof", index=True)
    proof_type: Mapped[str | None] = mapped_column(String(16), nullable=True)
    proof_file_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    proof_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(DateTime(timezone=True), server_default=func.now())
    reviewed_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    user: Mapped[User] = relationship(back_populates="purchases")
    plan: Mapped[VIPPlan] = relationship()


engine_kwargs: dict[str, Any] = {"pool_pre_ping": True}
if not DATABASE_URL.startswith("sqlite"):
    engine_kwargs.update(pool_size=10, max_overflow=20, pool_recycle=1800)
engine = create_async_engine(DATABASE_URL, **engine_kwargs)
Session = async_sessionmaker(engine, expire_on_commit=False, class_=AsyncSession)

# ------------------------------- FSM ---------------------------------------

class SearchStates(StatesGroup):
    title = State()
    code = State()
    genre = State()
    image = State()


class AIStates(StatesGroup):
    question = State()


class ChannelStates(StatesGroup):
    add_data = State()


class AddAnimeStates(StatesGroup):
    title = State()
    code = State()
    description = State()
    genres = State()
    year = State()
    cover = State()


class AddEpisodeStates(StatesGroup):
    anime_code = State()
    number = State()
    title = State()
    file = State()


class EditAnimeStates(StatesGroup):
    code = State()
    title = State()


class PostStates(StatesGroup):
    content = State()
    buttons = State()
    ready = State()


class PurchaseStates(StatesGroup):
    proof = State()


class OwnerStates(StatesGroup):
    add_admin = State()
    remove_admin = State()
    vip_user = State()
    vip_days = State()
    plan_price = State()


# ----------------------------- KEYBOARDS -----------------------------------

router = Router(name="anime_bot")
MENU_LABELS = {
    "🔎 Anime izlash", "💎 VIP", "🤖 AI", "📘 Qo'llanma", "⚙️ Sozlamalar",
    "ℹ️ Bot haqida", "📢 Kanallarimiz", "🔤 Nom bo'yicha", "🔢 Kod bo'yicha",
    "🏷 Janr bo'yicha", "🖼 Rasm orqali", "🎲 Tasodifiy anime", "🏆 Top anime",
    "🆕 Eng yangi", "🔙 Asosiy menyu",
}


def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔎 Anime izlash"), KeyboardButton(text="💎 VIP")],
            [KeyboardButton(text="🤖 AI"), KeyboardButton(text="📘 Qo'llanma")],
            [KeyboardButton(text="⚙️ Sozlamalar"), KeyboardButton(text="ℹ️ Bot haqida")],
            [KeyboardButton(text="📢 Kanallarimiz")],
        ],
        resize_keyboard=True,
        input_field_placeholder="Bo'limni tanlang",
    )


def search_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🔤 Nom bo'yicha"), KeyboardButton(text="🔢 Kod bo'yicha")],
            [KeyboardButton(text="🏷 Janr bo'yicha"), KeyboardButton(text="🖼 Rasm orqali")],
            [KeyboardButton(text="🎲 Tasodifiy anime"), KeyboardButton(text="🏆 Top anime")],
            [KeyboardButton(text="🆕 Eng yangi"), KeyboardButton(text="🔙 Asosiy menyu")],
        ], resize_keyboard=True
    )


def admin_keyboard(is_owner: bool) -> InlineKeyboardMarkup:
    rows = [
        [InlineKeyboardButton(text="📢 Kanallarni sozlash", callback_data="admin:channels")],
        [InlineKeyboardButton(text="🎞 Anime boshqaruvi", callback_data="admin:anime")],
        [InlineKeyboardButton(text="🧱 Post tayyorlash", callback_data="admin:post")],
        [InlineKeyboardButton(text="📨 Broadcast", callback_data="admin:broadcast")],
    ]
    if is_owner:
        rows += [
            [InlineKeyboardButton(text="💎 VIP boshqaruvi", callback_data="owner:vip")],
            [InlineKeyboardButton(text="🛡 Adminlar boshqaruvi", callback_data="owner:admins")],
        ]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def anime_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Anime qo'shish", callback_data="anime:add")],
        [InlineKeyboardButton(text="🎬 Qism qo'shish", callback_data="anime:episode_add")],
        [InlineKeyboardButton(text="✏️ Anime nomini tahrirlash", callback_data="anime:edit")],
        [InlineKeyboardButton(text="⬅️ Admin panel", callback_data="admin:home")],
    ])


def subscription_keyboard(channels: list[Channel]) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    for channel in channels:
        url = channel.invite_url
        if not url and channel.chat_id.startswith("@"):
            url = f"https://t.me/{channel.chat_id[1:]}"
        if url.startswith(("https://", "http://")):
            rows.append([InlineKeyboardButton(text=f"📢 {channel.title}", url=url)])
    rows.append([InlineKeyboardButton(text="✅ Tekshirish", callback_data="sub:check")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def channel_keyboard(channels: list[Channel]) -> InlineKeyboardMarkup:
    rows = []
    for channel in channels:
        flags = ("✅" if channel.is_required else "➖") + ("📤" if channel.is_post_target else "➖")
        rows.append([InlineKeyboardButton(
            text=f"{flags} {channel.title}", callback_data=f"channel:del:{channel.id}"
        )])
    rows.append([InlineKeyboardButton(text="➕ Kanal qo'shish", callback_data="channel:add")])
    rows.append([InlineKeyboardButton(text="⬅️ Admin panel", callback_data="admin:home")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def anime_results_keyboard(items: list[Anime]) -> InlineKeyboardMarkup | None:
    if not items:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"🎞 {a.title[:50]} [{a.code}]", callback_data=f"anime:view:{a.id}")
    ] for a in items])


def episodes_keyboard(episodes: list[Episode]) -> InlineKeyboardMarkup | None:
    if not episodes:
        return None
    rows = []
    for index in range(0, len(episodes), 2):
        rows.append([
            InlineKeyboardButton(text=f"🎬 {ep.number}-qism", callback_data=f"ep:{ep.id}")
            for ep in episodes[index:index + 2]
        ])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def plans_keyboard(plans: list[VIPPlan]) -> InlineKeyboardMarkup:
    rows = [[InlineKeyboardButton(
        text=f"{p.title} — {p.price:,} so'm".replace(",", " "), callback_data=f"buy:{p.id}"
    )] for p in plans]
    if not rows:
        rows = [[InlineKeyboardButton(text="Tariflar hozircha yo'q", callback_data="vip:none")]]
    return InlineKeyboardMarkup(inline_keyboard=rows)


def owner_vip_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ VIP berish", callback_data="owner:vip_add")],
        [InlineKeyboardButton(text="💰 Tarif narxini o'zgartirish", callback_data="owner:price")],
        [InlineKeyboardButton(text="📋 Tariflar", callback_data="owner:plans")],
        [InlineKeyboardButton(text="⬅️ Admin panel", callback_data="admin:home")],
    ])


def owner_admin_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="➕ Admin qo'shish", callback_data="owner:admin_add")],
        [InlineKeyboardButton(text="➖ Adminni olib tashlash", callback_data="owner:admin_remove")],
        [InlineKeyboardButton(text="⬅️ Admin panel", callback_data="admin:home")],
    ])


def post_action_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👁 Preview", callback_data="post:preview")],
        [InlineKeyboardButton(text="📢 Kanallarga yuborish", callback_data="post:send_channels")],
        [InlineKeyboardButton(text="👥 Foydalanuvchilarga yuborish", callback_data="post:send_users")],
        [InlineKeyboardButton(text="❌ Bekor qilish", callback_data="post:cancel")],
    ])


def purchase_review_keyboard(purchase_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Tasdiqlash", callback_data=f"purchase:approve:{purchase_id}"),
        InlineKeyboardButton(text="❌ Rad etish", callback_data=f"purchase:reject:{purchase_id}"),
    ]])

# ------------------------------- HELPERS -----------------------------------

ROLE_CACHE: dict[int, tuple[float, str]] = {}


def invalidate_role(user_id: int) -> None:
    ROLE_CACHE.pop(user_id, None)


async def get_role(user_id: int) -> str:
    if user_id in OWNER_IDS:
        return "owner"
    now = time.monotonic()
    cached = ROLE_CACHE.get(user_id)
    if cached and now - cached[0] < 3:
        return cached[1]
    async with Session() as session:
        role = await session.scalar(select(User.role).where(User.id == user_id))
    value = role if role in {"admin", "user"} else "user"
    ROLE_CACHE[user_id] = (now, value)
    if len(ROLE_CACHE) > 20_000:
        ROLE_CACHE.clear()
    return value


async def is_staff(user_id: int) -> bool:
    return await get_role(user_id) in {"owner", "admin"}


async def owner_only(event: Message | CallbackQuery) -> bool:
    user = event.from_user
    if user and user.id in OWNER_IDS:
        return True
    if isinstance(event, Message):
        await event.answer("⛔ Bu amal faqat bot egasi uchun.")
    else:
        await event.answer("Bu amal faqat bot egasi uchun.", show_alert=True)
    return False


async def staff_only(event: Message | CallbackQuery) -> bool:
    user = event.from_user
    if user and await is_staff(user.id):
        return True
    if isinstance(event, Message):
        await event.answer("⛔ Bu bo'lim faqat adminlar uchun.")
    else:
        await event.answer("Admin huquqi talab qilinadi.", show_alert=True)
    return False


async def upsert_user(message: Message) -> None:
    user_data = message.from_user
    if not user_data:
        return
    async with Session() as session:
        user = await session.get(User, user_data.id)
        if user is None:
            session.add(User(
                id=user_data.id,
                username=user_data.username,
                full_name=(user_data.full_name or "")[:256],
                role="user",
                last_seen_at=datetime.now(timezone.utc),
            ))
        else:
            user.username = user_data.username
            user.full_name = (user_data.full_name or "")[:256]
            user.last_seen_at = datetime.now(timezone.utc)
        await session.commit()


def parse_buttons(raw: str) -> list[list[dict[str, str]]]:
    """Expected JSON: [[{"text":"Website","url":"https://example.com"}]]."""
    if raw.strip() in {"", "-"}:
        return []
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError("JSON formati noto'g'ri.") from exc
    if not isinstance(data, list) or len(data) > 8:
        raise ValueError("Tugmalar ro'yxati 8 qatordan oshmasin.")
    result: list[list[dict[str, str]]] = []
    total = 0
    for row in data:
        if not isinstance(row, list) or not row or len(row) > 8:
            raise ValueError("Har bir qatorda 1–8 ta tugma bo'lishi kerak.")
        parsed_row = []
        for item in row:
            if not isinstance(item, dict):
                raise ValueError("Har bir tugma text va url maydonlariga ega JSON obyekt bo'lishi kerak.")
            text = str(item.get("text", "")).strip()
            url = str(item.get("url", "")).strip()
            if not text or len(text) > 64:
                raise ValueError("Tugma matni 1–64 belgi bo'lishi kerak.")
            if not url.startswith(("https://", "http://")):
                raise ValueError("Tugma URL manzili http:// yoki https:// bilan boshlansin.")
            parsed_row.append({"text": text, "url": url})
            total += 1
            if total > 50:
                raise ValueError("Ko'pi bilan 50 ta tugma qo'shish mumkin.")
        result.append(parsed_row)
    return result


def build_url_keyboard(button_rows: list[list[dict[str, str]]]) -> InlineKeyboardMarkup | None:
    if not button_rows:
        return None
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=item["text"], url=item["url"]) for item in row
    ] for row in button_rows])


async def send_post(
    bot: Bot,
    chat_id: int | str,
    content_type: str,
    text: str,
    file_id: str | None,
    buttons: list[list[dict[str, str]]],
) -> None:
    markup = build_url_keyboard(buttons)
    if content_type == "text":
        await bot.send_message(chat_id, text or " ", reply_markup=markup)
    elif content_type == "photo" and file_id:
        await bot.send_photo(chat_id, file_id, caption=text or None, reply_markup=markup)
    elif content_type == "video" and file_id:
        await bot.send_video(chat_id, file_id, caption=text or None, reply_markup=markup)
    elif content_type == "document" and file_id:
        await bot.send_document(chat_id, file_id, caption=text or None, reply_markup=markup)
    else:
        raise ValueError("Post turi yoki fayl ID noto'g'ri.")


async def check_subscriptions(bot: Bot, user_id: int) -> tuple[bool, list[Channel]]:
    async with Session() as session:
        channels = list((await session.scalars(select(Channel).where(
            Channel.is_active.is_(True), Channel.is_required.is_(True)
        ))).all())
    missing: list[Channel] = []
    valid = {ChatMemberStatus.MEMBER, ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR}
    for channel in channels:
        try:
            member = await bot.get_chat_member(channel.chat_id, user_id)
            restricted_member = member.status == ChatMemberStatus.RESTRICTED and bool(getattr(member, "is_member", False))
            if member.status not in valid and not restricted_member:
                missing.append(channel)
        except TelegramAPIError:
            log.exception("Obuna holatini tekshirib bo'lmadi: %s", channel.chat_id)
            # Fail closed: API xatosida obunani bor deb hisoblamaymiz.
            missing.append(channel)
    return not missing, missing


class SubscriptionMiddleware(BaseMiddleware):
    def __init__(self) -> None:
        self.valid_cache: dict[int, float] = {}

    async def __call__(self, handler, event, data):
        user = getattr(event, "from_user", None)
        if user is None:
            return await handler(event, data)
        if user.id in OWNER_IDS or await is_staff(user.id):
            return await handler(event, data)
        if isinstance(event, Message) and event.text and event.text.split(maxsplit=1)[0] == "/start":
            return await handler(event, data)
        if isinstance(event, CallbackQuery) and event.data == "sub:check":
            return await handler(event, data)
        now = time.monotonic()
        if now - self.valid_cache.get(user.id, 0) < 20:
            return await handler(event, data)
        bot: Bot = data["bot"]
        subscribed, missing = await check_subscriptions(bot, user.id)
        if subscribed:
            self.valid_cache[user.id] = now
            if len(self.valid_cache) > 20_000:
                self.valid_cache.clear()
            return await handler(event, data)
        markup = subscription_keyboard(missing)
        text = "Botdan foydalanish uchun avval quyidagi kanallarga obuna bo'ling, so'ng Tekshirish tugmasini bosing."
        if isinstance(event, Message):
            await event.answer(text, reply_markup=markup)
        elif isinstance(event, CallbackQuery):
            await event.answer("Avval majburiy kanallarga obuna bo'ling.", show_alert=True)
            if event.message:
                await event.message.answer(text, reply_markup=markup)
        return None


async def search_title(query: str) -> list[Anime]:
    async with Session() as session:
        return list((await session.scalars(select(Anime).where(
            Anime.title.ilike(f"%{query.strip()}%")
        ).order_by(Anime.title.asc()).limit(10))).all())


async def anime_by_code(code: str) -> Anime | None:
    async with Session() as session:
        return await session.scalar(select(Anime).where(func.lower(Anime.code) == code.strip().lower()))


async def search_genre(genre: str) -> list[Anime]:
    async with Session() as session:
        return list((await session.scalars(select(Anime).where(
            Anime.genres.ilike(f"%{genre.strip()}%")
        ).order_by(Anime.title.asc()).limit(10))).all())


async def get_random_anime() -> Anime | None:
    async with Session() as session:
        # RANDOM() is supported by SQLite and PostgreSQL.
        from sqlalchemy import func as sqlfunc
        return await session.scalar(select(Anime).order_by(sqlfunc.random()).limit(1))


async def get_anime_list(mode: str) -> list[Anime]:
    async with Session() as session:
        stmt = select(Anime)
        if mode == "top":
            stmt = stmt.order_by(Anime.views.desc(), Anime.title.asc())
        else:
            stmt = stmt.order_by(Anime.created_at.desc())
        return list((await session.scalars(stmt.limit(10))).all())


async def show_anime(message: Message, anime_id: int) -> None:
    async with Session() as session:
        anime = await session.get(Anime, anime_id)
        if not anime:
            await message.answer("Anime topilmadi.")
            return
        anime.views += 1
        title = anime.title
        code = anime.code
        description = anime.description
        genres = anime.genres
        year = anime.year
        cover = anime.cover_file_id
        episodes = list((await session.scalars(select(Episode).where(
            Episode.anime_id == anime_id
        ).order_by(Episode.number.asc()))).all())
        await session.commit()
    caption = f"🎞 {title}\nKod: {code}\nJanr: {genres or '—'}\nYil: {year or '—'}\n\n{description or 'Tavsif kiritilmagan.'}"
    markup = episodes_keyboard(episodes)
    if cover:
        try:
            await message.answer_photo(cover, caption=caption[:1024], reply_markup=markup)
        except TelegramAPIError:
            await message.answer(caption, reply_markup=markup)
    else:
        await message.answer(caption, reply_markup=markup)


async def notify_owners(bot: Bot, text: str, reply_markup: InlineKeyboardMarkup | None = None) -> None:
    for owner_id in OWNER_IDS:
        try:
            await bot.send_message(owner_id, text, reply_markup=reply_markup)
        except TelegramAPIError:
            log.warning("Owner'ga xabar yuborilmadi: %s", owner_id)


async def ai_text(prompt: str) -> str:
    if AI_PROVIDER == "off":
        return "AI hozircha o'chirilgan. .env faylida AI_PROVIDER=openai yoki gemini qiling."
    if AI_PROVIDER == "openai":
        if not OPENAI_API_KEY:
            return "OPENAI_API_KEY sozlanmagan."
        from openai import AsyncOpenAI
        async with AsyncOpenAI(api_key=OPENAI_API_KEY) as client:
            result = await client.responses.create(model=OPENAI_MODEL, input=prompt)
            return result.output_text.strip() or "AI javob qaytarmadi."
    if not GEMINI_API_KEY:
        return "GEMINI_API_KEY sozlanmagan."
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    timeout = aiohttp.ClientTimeout(total=45)
    async with aiohttp.ClientSession(timeout=timeout) as client:
        async with client.post(url, params={"key": GEMINI_API_KEY}, json={
            "contents": [{"parts": [{"text": prompt}]}]
        }) as response:
            body = await response.text()
            if response.status >= 400:
                raise RuntimeError(f"Gemini API ({response.status}): {body[:300]}")
            data = json.loads(body)
    parts = data.get("candidates", [{}])[0].get("content", {}).get("parts", [])
    return "\n".join(p.get("text", "") for p in parts if p.get("text")) or "AI javob qaytarmadi."


async def identify_anime_from_image(bot: Bot, photo_file_id: str) -> str:
    if AI_PROVIDER == "off":
        raise RuntimeError("Rasm orqali izlash uchun AI_PROVIDER sozlanmagan.")
    file = await bot.get_file(photo_file_id)
    image_stream = await bot.download_file(file.file_path)
    image_bytes = image_stream.read() if image_stream else b""
    if not image_bytes:
        raise RuntimeError("Rasm yuklab olinmadi.")
    prompt = "Rasmdagi anime nomini aniqlashga harakat qil. Eng ehtimoliy nomni va 1 ta muqobil nomni qisqa yoz. Ishonching komil bo'lmasa, noaniqligini ayt."
    import base64
    encoded = base64.b64encode(image_bytes).decode("ascii")
    if AI_PROVIDER == "openai":
        if not OPENAI_API_KEY:
            raise RuntimeError("OPENAI_API_KEY sozlanmagan.")
        from openai import AsyncOpenAI
        async with AsyncOpenAI(api_key=OPENAI_API_KEY) as client:
            response = await client.responses.create(
                model=OPENAI_MODEL,
                input=[{"role": "user", "content": [
                    {"type": "input_text", "text": prompt},
                    {"type": "input_image", "image_url": f"data:image/jpeg;base64,{encoded}"},
                ]}],
            )
            return response.output_text.strip()
    if not GEMINI_API_KEY:
        raise RuntimeError("GEMINI_API_KEY sozlanmagan.")
    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent"
    timeout = aiohttp.ClientTimeout(total=45)
    async with aiohttp.ClientSession(timeout=timeout) as client:
        async with client.post(url, params={"key": GEMINI_API_KEY}, json={
            "contents": [{"parts": [
                {"text": prompt},
                {"inlineData": {"mimeType": "image/jpeg", "data": encoded}},
            ]}]
        }) as response:
            body = await response.text()
            if response.status >= 400:
                raise RuntimeError(f"Gemini API ({response.status}): {body[:300]}")
            data = json.loads(body)
    parts = data.get("candidates", [{}])[0].get("content", {}).get("parts", [])
    return "\n".join(p.get("text", "") for p in parts if p.get("text")) or "Anime aniqlanmadi."

# ----------------------------- START / MENUS -------------------------------

@router.message(CommandStart())
async def start_command(message: Message, state: FSMContext) -> None:
    await state.clear()
    await upsert_user(message)
    if not message.from_user:
        return
    ok, missing = await check_subscriptions(message.bot, message.from_user.id)
    if not ok:
        await message.answer(
            "Assalomu alaykum! Botdan foydalanish uchun avval kanallarga obuna bo'ling:",
            reply_markup=subscription_keyboard(missing),
        )
        return
    role = await get_role(message.from_user.id)
    vip_text = ""
    async with Session() as session:
        user = await session.get(User, message.from_user.id)
        if user and user.vip_until and _vip_is_active(user.vip_until):
            vip_text = "\n💎 Sizda VIP obuna faol."
    greeting = f"Salom, {message.from_user.first_name}! Anime olamiga xush kelibsiz.{vip_text}"
    if role in {"owner", "admin"}:
        greeting += "\nAdmin panel: /admin"
    await message.answer(greeting, reply_markup=main_menu())


@router.callback_query(F.data == "sub:check")
async def check_subscription_callback(callback: CallbackQuery, state: FSMContext) -> None:
    if not callback.message:
        await callback.answer()
        return
    ok, missing = await check_subscriptions(callback.bot, callback.from_user.id)
    if not ok:
        await callback.answer("Hali barcha kanallarga obuna bo'lmadingiz.", show_alert=True)
        await callback.message.answer("Kanallarga obuna bo'lib, qayta tekshiring.", reply_markup=subscription_keyboard(missing))
        return
    await state.clear()
    await callback.answer("Obuna tasdiqlandi!")
    await callback.message.answer("✅ Obuna tasdiqlandi. Endi botdan foydalanishingiz mumkin.", reply_markup=main_menu())


@router.message(Command("cancel"))
async def cancel_state(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Amal bekor qilindi.", reply_markup=main_menu())


@router.message(Command("admin"))
async def admin_command(message: Message) -> None:
    if not await staff_only(message):
        return
    role = await get_role(message.from_user.id) if message.from_user else "user"
    await message.answer("🛡 Admin panel", reply_markup=admin_keyboard(role == "owner"))


@router.message(F.text == "🔎 Anime izlash")
async def search_menu_handler(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Anime izlash usulini tanlang:", reply_markup=search_menu())


@router.message(F.text == "🔙 Asosiy menyu")
async def back_main(message: Message, state: FSMContext) -> None:
    await state.clear()
    await message.answer("Asosiy menyu:", reply_markup=main_menu())


@router.message(F.text == "🔤 Nom bo'yicha")
async def search_title_start(message: Message, state: FSMContext) -> None:
    await state.set_state(SearchStates.title)
    await message.answer("Anime nomini yuboring:")


@router.message(SearchStates.title, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def search_title_result(message: Message, state: FSMContext) -> None:
    results = await search_title(message.text or "")
    await state.clear()
    await message.answer("Topilgan anime:" if results else "Bu nom bo'yicha anime topilmadi.", reply_markup=anime_results_keyboard(results))


@router.message(F.text == "🔢 Kod bo'yicha")
async def search_code_start(message: Message, state: FSMContext) -> None:
    await state.set_state(SearchStates.code)
    await message.answer("Anime kodini yuboring:")


@router.message(SearchStates.code, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def search_code_result(message: Message, state: FSMContext) -> None:
    anime = await anime_by_code(message.text or "")
    await state.clear()
    if anime:
        await show_anime(message, anime.id)
    else:
        await message.answer("Bu kod bo'yicha anime topilmadi.")


@router.message(F.text == "🏷 Janr bo'yicha")
async def search_genre_start(message: Message, state: FSMContext) -> None:
    await state.set_state(SearchStates.genre)
    await message.answer("Janr nomini yuboring (masalan: Action, Comedy):")


@router.message(SearchStates.genre, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def search_genre_result(message: Message, state: FSMContext) -> None:
    results = await search_genre(message.text or "")
    await state.clear()
    await message.answer("Topilgan anime:" if results else "Bu janr bo'yicha anime topilmadi.", reply_markup=anime_results_keyboard(results))


@router.message(F.text == "🎲 Tasodifiy anime")
async def random_anime(message: Message) -> None:
    anime = await get_random_anime()
    if anime:
        await show_anime(message, anime.id)
    else:
        await message.answer("Katalogda hali anime yo'q.")


@router.message(F.text == "🏆 Top anime")
async def top_anime(message: Message) -> None:
    items = await get_anime_list("top")
    await message.answer("🏆 Top anime:" if items else "Katalog bo'sh.", reply_markup=anime_results_keyboard(items))


@router.message(F.text == "🆕 Eng yangi")
async def latest_anime(message: Message) -> None:
    items = await get_anime_list("latest")
    await message.answer("🆕 Eng yangi anime:" if items else "Katalog bo'sh.", reply_markup=anime_results_keyboard(items))


@router.callback_query(F.data.startswith("anime:view:"))
async def anime_view_callback(callback: CallbackQuery) -> None:
    try:
        anime_id = int((callback.data or "").split(":")[-1])
    except ValueError:
        await callback.answer("Anime ID noto'g'ri.", show_alert=True)
        return
    await callback.answer()
    if callback.message:
        await show_anime(callback.message, anime_id)


@router.callback_query(F.data.startswith("ep:"))
async def episode_callback(callback: CallbackQuery) -> None:
    try:
        episode_id = int((callback.data or "").split(":")[-1])
    except ValueError:
        await callback.answer("Qism ID noto'g'ri.", show_alert=True)
        return
    async with Session() as session:
        episode = await session.get(Episode, episode_id)
        anime = await session.get(Anime, episode.anime_id) if episode else None
    if not episode:
        await callback.answer("Qism topilmadi.", show_alert=True)
        return
    await callback.answer()
    caption = f"🎞 {anime.title if anime else 'Anime'} — {episode.number}-qism"
    if episode.title:
        caption += f"\n{episode.title}"
    try:
        if episode.file_type == "document":
            await callback.message.answer_document(episode.file_id, caption=caption if len(caption) < 1024 else caption[:1020])
        else:
            await callback.message.answer_video(episode.file_id, caption=caption if len(caption) < 1024 else caption[:1020])
    except (TelegramAPIError, AttributeError):
        log.exception("Epizod yuborilmadi: %s", episode.id)
        if callback.message:
            await callback.message.answer("Qismni yuborishda xatolik yuz berdi.")


@router.message(F.text == "🖼 Rasm orqali")
async def image_search_start(message: Message, state: FSMContext) -> None:
    if AI_PROVIDER == "off":
        await message.answer("Rasm orqali qidiruv uchun AI_PROVIDER sozlamasini yoqing.")
        return
    await state.set_state(SearchStates.image)
    await message.answer("Anime rasmini yuboring. AI rasmga qarab nomini taxmin qiladi.")


@router.message(SearchStates.image, F.photo)
async def image_search_result(message: Message, state: FSMContext) -> None:
    try:
        guess = await identify_anime_from_image(message.bot, message.photo[-1].file_id)
        matches = await search_title(guess.splitlines()[0][:100])
        await message.answer(f"🧠 AI taxmini:\n{guess[:1500]}")
        if matches:
            await message.answer("Mahalliy katalogdan mos natijalar:", reply_markup=anime_results_keyboard(matches))
        else:
            await message.answer("Mahalliy katalogda mos anime topilmadi.")
    except Exception as exc:
        log.exception("Rasm orqali qidiruvda xato")
        await message.answer(f"Rasmni tahlil qilib bo'lmadi: {str(exc)[:300]}")
    finally:
        await state.clear()

# -------------------------------- AI ---------------------------------------

@router.message(F.text == "🤖 AI")
async def ai_start(message: Message, state: FSMContext) -> None:
    await state.set_state(AIStates.question)
    await message.answer("Savolingizni yuboring. Chiqish uchun /cancel yozing.")


@router.message(AIStates.question, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def ai_answer(message: Message) -> None:
    try:
        answer = await ai_text(message.text or "")
        await message.answer(answer[:4000])
    except Exception as exc:
        log.exception("AI so'rovi muvaffaqiyatsiz")
        await message.answer(f"AI xizmatida xatolik: {str(exc)[:250]}")

# ------------------------------- VIP ---------------------------------------

def _vip_is_active(vip_until: datetime) -> bool:
    if vip_until.tzinfo is None:
        vip_until = vip_until.replace(tzinfo=timezone.utc)
    return vip_until > datetime.now(timezone.utc)


@router.message(F.text == "💎 VIP")
async def vip_menu(message: Message) -> None:
    async with Session() as session:
        plans = list((await session.scalars(select(VIPPlan).where(VIPPlan.is_active.is_(True)).order_by(VIPPlan.days.asc()))).all())
    await message.answer(
        f"💎 VIP tariflari\n\nTo'lov yo'riqnomasi: {PAYMENT_INSTRUCTIONS}\n\nTarifni tanlang:",
        reply_markup=plans_keyboard(plans),
    )


@router.callback_query(F.data == "vip:none")
async def vip_none(callback: CallbackQuery) -> None:
    await callback.answer("Hozircha faol tarif yo'q.", show_alert=True)


@router.callback_query(F.data.startswith("buy:"))
async def buy_vip(callback: CallbackQuery, state: FSMContext) -> None:
    try:
        plan_id = int((callback.data or "").split(":")[-1])
    except ValueError:
        await callback.answer("Tarif ID noto'g'ri.", show_alert=True)
        return
    async with Session() as session:
        plan = await session.get(VIPPlan, plan_id)
        user = await session.get(User, callback.from_user.id)
        if not user:
            user = User(id=callback.from_user.id, username=callback.from_user.username, full_name=callback.from_user.full_name[:256])
            session.add(user)
            await session.flush()
        if not plan or not plan.is_active:
            await callback.answer("Bu tarif mavjud emas.", show_alert=True)
            return
        purchase = VIPPurchase(user_id=callback.from_user.id, plan_id=plan.id)
        session.add(purchase)
        await session.commit()
        purchase_id = purchase.id
        title = plan.title
        price = plan.price
    await state.set_state(PurchaseStates.proof)
    await state.update_data(purchase_id=purchase_id)
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            f"Siz tanladingiz: {title}\nNarx: {price:,} so'm\n\n{PAYMENT_INSTRUCTIONS}\n\nTo'lov cheki rasmini, faylini yoki to'lov ma'lumotini yuboring. Tasdiqlashni Owner bajaradi.".replace(",", " ")
        )


@router.message(PurchaseStates.proof)
async def vip_proof(message: Message, state: FSMContext) -> None:
    data = await state.get_data()
    purchase_id = data.get("purchase_id")
    proof_type = None
    proof_file_id = None
    proof_text = None
    if message.photo:
        proof_type, proof_file_id = "photo", message.photo[-1].file_id
    elif message.document:
        proof_type, proof_file_id = "document", message.document.file_id
    elif message.text:
        proof_type, proof_text = "text", message.text[:2000]
    else:
        await message.answer("Chekni rasm, fayl yoki matn ko'rinishida yuboring.")
        return
    async with Session() as session:
        purchase = await session.get(VIPPurchase, int(purchase_id)) if purchase_id else None
        if not purchase or purchase.status not in {"awaiting_proof", "pending"}:
            await state.clear()
            await message.answer("Xarid so'rovi topilmadi. VIP menyusidan qaytadan boshlang.")
            return
        purchase.proof_type = proof_type
        purchase.proof_file_id = proof_file_id
        purchase.proof_text = proof_text
        purchase.status = "pending"
        user = await session.get(User, purchase.user_id)
        plan = await session.get(VIPPlan, purchase.plan_id)
        await session.commit()
    await state.clear()
    await message.answer("✅ To'lov ma'lumoti yuborildi. Owner tekshirib, natijani xabar qiladi.")
    user_label = f"{user.full_name if user else message.from_user.full_name} (ID: {message.from_user.id})"
    text = f"💎 VIP to'lov so'rovi #{purchase_id}\nFoydalanuvchi: {user_label}\nTarif: {plan.title if plan else '—'}\nNarx: {plan.price if plan else '—'} so'm"
    for owner_id in OWNER_IDS:
        try:
            if proof_type == "photo":
                await message.bot.send_photo(owner_id, proof_file_id, caption=text, reply_markup=purchase_review_keyboard(int(purchase_id)))
            elif proof_type == "document":
                await message.bot.send_document(owner_id, proof_file_id, caption=text, reply_markup=purchase_review_keyboard(int(purchase_id)))
            else:
                await message.bot.send_message(owner_id, text + f"\nMa'lumot: {proof_text}", reply_markup=purchase_review_keyboard(int(purchase_id)))
        except TelegramAPIError:
            log.exception("VIP so'rovi Owner'ga yuborilmadi")


@router.callback_query(F.data.startswith("purchase:approve:"))
async def approve_purchase(callback: CallbackQuery) -> None:
    if not await owner_only(callback):
        return
    purchase_id = int((callback.data or "").split(":")[-1])
    async with Session() as session:
        purchase = await session.get(VIPPurchase, purchase_id)
        if not purchase or purchase.status != "pending":
            await callback.answer("So'rov topilmadi yoki oldin ko'rib chiqilgan.", show_alert=True)
            return
        plan = await session.get(VIPPlan, purchase.plan_id)
        user = await session.get(User, purchase.user_id)
        if not plan or not user:
            await callback.answer("Foydalanuvchi yoki tarif topilmadi.", show_alert=True)
            return
        now = datetime.now(timezone.utc)
        until = user.vip_until
        if until is not None and until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)
        start_at = until if until and until > now else now
        user.vip_until = start_at + timedelta(days=plan.days)
        purchase.status = "approved"
        purchase.reviewed_at = now
        target_user = user.id
        new_until = user.vip_until
        await session.commit()
    await callback.answer("VIP tasdiqlandi.")
    if callback.message:
        await callback.message.answer(f"✅ VIP so'rov #{purchase_id} tasdiqlandi.")
    try:
        await callback.bot.send_message(target_user, f"🎉 VIP obunangiz faollashtirildi! Tugash vaqti: {new_until:%Y-%m-%d %H:%M} UTC")
    except TelegramAPIError:
        log.warning("VIP tasdig'i foydalanuvchiga yetkazilmadi: %s", target_user)


@router.callback_query(F.data.startswith("purchase:reject:"))
async def reject_purchase(callback: CallbackQuery) -> None:
    if not await owner_only(callback):
        return
    purchase_id = int((callback.data or "").split(":")[-1])
    async with Session() as session:
        purchase = await session.get(VIPPurchase, purchase_id)
        if not purchase or purchase.status != "pending":
            await callback.answer("So'rov topilmadi yoki oldin ko'rib chiqilgan.", show_alert=True)
            return
        purchase.status = "rejected"
        purchase.reviewed_at = datetime.now(timezone.utc)
        target_user = purchase.user_id
        await session.commit()
    await callback.answer("So'rov rad etildi.")
    if callback.message:
        await callback.message.answer(f"VIP so'rov #{purchase_id} rad etildi.")
    try:
        await callback.bot.send_message(target_user, "VIP to'lov so'rovingiz rad etildi. Batafsil ma'lumot uchun Owner bilan bog'laning.")
    except TelegramAPIError:
        pass

# ----------------------------- INFORMATION ---------------------------------

@router.message(F.text == "📘 Qo'llanma")
async def help_page(message: Message) -> None:
    await message.answer(
        "📘 Qo'llanma\n\n"
        "1. Anime izlash bo'limidan nom, kod yoki janr bilan qidiring.\n"
        "2. Anime kartasidagi qism tugmasini bosing.\n"
        "3. VIP bo'limida tarif tanlab, to'lov ma'lumotini yuboring.\n"
        "4. AI bo'limida savol berishingiz mumkin.\n\n"
        "Muammo bo'lsa, bot egasi bilan bog'laning."
    )


@router.message(F.text == "⚙️ Sozlamalar")
async def settings_page(message: Message) -> None:
    await message.answer("⚙️ Sozlamalar\nHozircha til yoki xabarnoma sozlamalari yoqilmagan. Asosiy funksiyalar menyuda mavjud.")


@router.message(F.text == "ℹ️ Bot haqida")
async def about_page(message: Message) -> None:
    await message.answer(f"ℹ️ {BOT_NAME}\nAnime qidirish, qismlarni ko'rish, VIP va AI yordamchi funksiyalari mavjud.")


@router.message(F.text == "📢 Kanallarimiz")
async def public_channels(message: Message) -> None:
    async with Session() as session:
        channels = list((await session.scalars(select(Channel).where(
            Channel.is_active.is_(True), Channel.is_post_target.is_(True)
        ).order_by(Channel.title.asc()))).all())
    if not channels:
        await message.answer("Hozircha kanallar ro'yxati kiritilmagan.")
        return
    rows = []
    for channel in channels:
        url = channel.invite_url or (f"https://t.me/{channel.chat_id[1:]}" if channel.chat_id.startswith("@") else "")
        if url.startswith(("https://", "http://")):
            rows.append([InlineKeyboardButton(text=channel.title, url=url)])
    if rows:
        await message.answer("📢 Bizning kanallar:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    else:
        await message.answer("Kanal havolalari hali kiritilmagan.")

# ------------------------------ ADMIN PANEL --------------------------------

@router.callback_query(F.data.startswith("admin:"))
async def admin_callbacks(callback: CallbackQuery, state: FSMContext) -> None:
    if not await staff_only(callback):
        return
    data = callback.data or ""
    if data == "admin:home":
        role = await get_role(callback.from_user.id)
        await callback.answer()
        if callback.message:
            await callback.message.answer("🛡 Admin panel", reply_markup=admin_keyboard(role == "owner"))
        return
    if data == "admin:channels":
        async with Session() as session:
            channels = list((await session.scalars(select(Channel).order_by(Channel.title.asc()))).all())
        await callback.answer()
        if callback.message:
            await callback.message.answer("📢 Kanallar. Qatorni bosib o'chirish mumkin. Botni kanalda admin qilib qo'ying.", reply_markup=channel_keyboard(channels))
        return
    if data == "admin:anime":
        await callback.answer()
        if callback.message:
            await callback.message.answer("🎞 Anime boshqaruvi", reply_markup=anime_admin_keyboard())
        return
    if data == "admin:post" or data == "admin:broadcast":
        await state.clear()
        await state.update_data(default_target="channels" if data == "admin:post" else "users")
        await state.set_state(PostStates.content)
        await callback.answer()
        if callback.message:
            await callback.message.answer(
                "Post matnini yoki rasm/video bilan caption yuboring.\n"
                "Keyingi bosqichda URL tugmalarni JSON shaklida kiritasiz yoki - yuborasiz."
            )
        return
    await callback.answer("Noma'lum admin buyrug'i.", show_alert=True)


@router.callback_query(F.data == "channel:add")
async def channel_add_begin(callback: CallbackQuery, state: FSMContext) -> None:
    if not await staff_only(callback):
        return
    await state.set_state(ChannelStates.add_data)
    await callback.answer()
    if callback.message:
        await callback.message.answer(
            "Kanal ma'lumotlarini bitta qatorda yuboring:\n"
            "Nomi | @username yoki -100... | https://t.me/... yoki - | required(true/false) | post(true/false)\n"
            "Misol: Anime News | @anime_news | https://t.me/anime_news | true | true\n"
            "Bot kanalda administrator bo'lishi kerak. Bekor qilish: /cancel"
        )


@router.message(ChannelStates.add_data, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def channel_add_save(message: Message, state: FSMContext) -> None:
    if not await staff_only(message):
        return
    parts = [p.strip() for p in (message.text or "").split("|")]
    if len(parts) != 5:
        await message.answer("5 ta maydon kerak; maydonlarni | bilan ajrating.")
        return
    title, chat_id, url, required, post_target = parts
    if not title or not (chat_id.startswith("@") or chat_id.lstrip("-").isdigit()):
        await message.answer("Kanal nomi yoki chat ID noto'g'ri.")
        return
    if url == "-":
        url = ""
    elif url and not url.startswith(("https://", "http://")):
        await message.answer("URL https://t.me/... yoki - bo'lishi kerak.")
        return
    if required.lower() not in {"true", "false"} or post_target.lower() not in {"true", "false"}:
        await message.answer("required va post qiymatlari true yoki false bo'lishi kerak.")
        return
    async with Session() as session:
        session.add(Channel(title=title[:256], chat_id=chat_id, invite_url=url,
                            is_required=required.lower() == "true",
                            is_post_target=post_target.lower() == "true", is_active=True))
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            await message.answer("Bu chat ID oldin qo'shilgan.")
            return
    await state.clear()
    await message.answer("✅ Kanal saqlandi. Botga kanal admin huquqi berilganini tekshiring.")


@router.callback_query(F.data.startswith("channel:del:"))
async def channel_delete(callback: CallbackQuery) -> None:
    if not await staff_only(callback):
        return
    try:
        channel_id = int((callback.data or "").split(":")[-1])
    except ValueError:
        await callback.answer("Kanal ID noto'g'ri.", show_alert=True)
        return
    async with Session() as session:
        channel = await session.get(Channel, channel_id)
        if channel:
            await session.delete(channel)
            await session.commit()
    await callback.answer("Kanal o'chirildi." if channel else "Kanal topilmadi.")
    if callback.message:
        async with Session() as session:
            channels = list((await session.scalars(select(Channel).order_by(Channel.title.asc()))).all())
        await callback.message.answer("📢 Kanallar:", reply_markup=channel_keyboard(channels))


@router.callback_query(F.data == "anime:add")
async def anime_add_begin(callback: CallbackQuery, state: FSMContext) -> None:
    if not await staff_only(callback):
        return
    await state.clear()
    await state.set_state(AddAnimeStates.title)
    await callback.answer()
    if callback.message:
        await callback.message.answer("Anime nomini yuboring. Bekor qilish: /cancel")


@router.message(AddAnimeStates.title, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def anime_add_title(message: Message, state: FSMContext) -> None:
    if not await staff_only(message):
        return
    await state.update_data(title=(message.text or "").strip()[:256])
    await state.set_state(AddAnimeStates.code)
    await message.answer("Takrorlanmaydigan anime kodini yuboring:")


@router.message(AddAnimeStates.code, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def anime_add_code(message: Message, state: FSMContext) -> None:
    if not await staff_only(message):
        return
    code = (message.text or "").strip()
    if not code or len(code) > 64:
        await message.answer("Kod 1–64 belgi bo'lishi kerak.")
        return
    await state.update_data(code=code)
    await state.set_state(AddAnimeStates.description)
    await message.answer("Tavsifni yuboring yoki bo'sh qoldirish uchun - yozing:")


@router.message(AddAnimeStates.description, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def anime_add_description(message: Message, state: FSMContext) -> None:
    if not await staff_only(message):
        return
    value = (message.text or "").strip()
    await state.update_data(description="" if value == "-" else value[:8000])
    await state.set_state(AddAnimeStates.genres)
    await message.answer("Janrlarni vergul bilan yuboring, masalan: Action, Fantasy:")


@router.message(AddAnimeStates.genres, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def anime_add_genres(message: Message, state: FSMContext) -> None:
    if not await staff_only(message):
        return
    await state.update_data(genres=(message.text or "").strip()[:512])
    await state.set_state(AddAnimeStates.year)
    await message.answer("Chiqqan yilini yuboring yoki noma'lum bo'lsa - yuboring:")


@router.message(AddAnimeStates.year, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def anime_add_year(message: Message, state: FSMContext) -> None:
    if not await staff_only(message):
        return
    raw = (message.text or "").strip()
    year = None
    if raw != "-":
        try:
            year = int(raw)
            if year < 1900 or year > 2100:
                raise ValueError
        except ValueError:
            await message.answer("Yil 1900–2100 oralig'ida bo'lishi yoki - bo'lishi kerak.")
            return
    await state.update_data(year=year)
    await state.set_state(AddAnimeStates.cover)
    await message.answer("Muqova rasmini yuboring yoki rasmsiz saqlash uchun /skip yozing:")


@router.message(AddAnimeStates.cover)
async def anime_add_cover(message: Message, state: FSMContext) -> None:
    if not await staff_only(message):
        return
    if message.photo:
        cover_id = message.photo[-1].file_id
    elif message.text and message.text.strip() == "/skip":
        cover_id = None
    else:
        await message.answer("Rasm yuboring yoki /skip yozing.")
        return
    data = await state.get_data()
    async with Session() as session:
        session.add(Anime(title=data["title"], code=data["code"], description=data.get("description", ""),
                          genres=data.get("genres", ""), year=data.get("year"), cover_file_id=cover_id))
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            await state.clear()
            await message.answer("Bu anime kodi band. Yangi kod bilan qayta urinib ko'ring.")
            return
    await state.clear()
    await message.answer(f"✅ Anime saqlandi: {data['title']} [{data['code']}]. Qismlarni 'Qism qo'shish' orqali kiriting.")


@router.callback_query(F.data == "anime:episode_add")
async def episode_add_begin(callback: CallbackQuery, state: FSMContext) -> None:
    if not await staff_only(callback):
        return
    await state.clear()
    await state.set_state(AddEpisodeStates.anime_code)
    await callback.answer()
    if callback.message:
        await callback.message.answer("Qism qo'shiladigan anime kodini yuboring:")


@router.message(AddEpisodeStates.anime_code, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def episode_anime_code(message: Message, state: FSMContext) -> None:
    if not await staff_only(message):
        return
    anime = await anime_by_code(message.text or "")
    if not anime:
        await message.answer("Anime kodi topilmadi. Qayta yuboring yoki /cancel.")
        return
    await state.update_data(anime_id=anime.id, anime_title=anime.title)
    await state.set_state(AddEpisodeStates.number)
    await message.answer("Qism raqamini kiriting:")


@router.message(AddEpisodeStates.number, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def episode_number(message: Message, state: FSMContext) -> None:
    if not await staff_only(message):
        return
    try:
        number = int((message.text or "").strip())
        if number < 1:
            raise ValueError
    except ValueError:
        await message.answer("Qism raqami musbat butun son bo'lishi kerak.")
        return
    await state.update_data(number=number)
    await state.set_state(AddEpisodeStates.title)
    await message.answer("Qism nomini kiriting yoki nom bo'lmasa - yuboring:")


@router.message(AddEpisodeStates.title, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def episode_title(message: Message, state: FSMContext) -> None:
    if not await staff_only(message):
        return
    value = (message.text or "").strip()
    await state.update_data(episode_title="" if value == "-" else value[:256])
    await state.set_state(AddEpisodeStates.file)
    await message.answer("Video yoki document ko'rinishida qismni yuboring:")


@router.message(AddEpisodeStates.file)
async def episode_save(message: Message, state: FSMContext) -> None:
    if not await staff_only(message):
        return
    if message.video:
        file_id, file_type = message.video.file_id, "video"
    elif message.document:
        file_id, file_type = message.document.file_id, "document"
    else:
        await message.answer("Video yoki document yuboring.")
        return
    data = await state.get_data()
    async with Session() as session:
        session.add(Episode(anime_id=data["anime_id"], number=data["number"], title=data.get("episode_title", ""), file_id=file_id, file_type=file_type))
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            await state.clear()
            await message.answer("Bu anime uchun bunday qism raqami mavjud.")
            return
    await state.clear()
    await message.answer(f"✅ {data['anime_title']} — {data['number']}-qism saqlandi.")


@router.callback_query(F.data == "anime:edit")
async def anime_edit_begin(callback: CallbackQuery, state: FSMContext) -> None:
    if not await staff_only(callback):
        return
    await state.clear()
    await state.set_state(EditAnimeStates.code)
    await callback.answer()
    if callback.message:
        await callback.message.answer("Tahrirlanadigan anime kodini yuboring:")


@router.message(EditAnimeStates.code, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def anime_edit_code(message: Message, state: FSMContext) -> None:
    if not await staff_only(message):
        return
    anime = await anime_by_code(message.text or "")
    if not anime:
        await message.answer("Anime topilmadi. Kodni qayta yuboring yoki /cancel.")
        return
    await state.update_data(anime_id=anime.id, old_title=anime.title)
    await state.set_state(EditAnimeStates.title)
    await message.answer(f"Hozirgi nom: {anime.title}\nYangi nomni yuboring:")


@router.message(EditAnimeStates.title, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def anime_edit_title(message: Message, state: FSMContext) -> None:
    if not await staff_only(message):
        return
    data = await state.get_data()
    title = (message.text or "").strip()
    if not title:
        await message.answer("Nom bo'sh bo'lmasligi kerak.")
        return
    async with Session() as session:
        anime = await session.get(Anime, int(data["anime_id"]))
        if anime:
            anime.title = title[:256]
            await session.commit()
    await state.clear()
    await message.answer(f"✅ Anime nomi yangilandi: {title[:256]}")

# ------------------------------ POST BUILDER -------------------------------

@router.message(PostStates.content)
async def post_receive_content(message: Message, state: FSMContext) -> None:
    if not await staff_only(message):
        return
    if message.photo:
        content_type, file_id = "photo", message.photo[-1].file_id
        text = message.caption or ""
    elif message.video:
        content_type, file_id = "video", message.video.file_id
        text = message.caption or ""
    elif message.document:
        content_type, file_id = "document", message.document.file_id
        text = message.caption or ""
    elif message.text:
        content_type, file_id = "text", None
        text = message.text
    else:
        await message.answer("Matn, rasm, video yoki document yuboring.")
        return
    await state.update_data(content_type=content_type, file_id=file_id, text=text[:4000])
    await state.set_state(PostStates.buttons)
    await message.answer(
        "URL tugmalarni JSON formatida yuboring yoki - yozing. Misol:\n"
        '[[{"text":"🌐 Sayt","url":"https://example.com"},{"text":"📢 Kanal","url":"https://t.me/example"}]]'
    )


@router.message(PostStates.buttons, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def post_receive_buttons(message: Message, state: FSMContext) -> None:
    if not await staff_only(message):
        return
    try:
        buttons = parse_buttons(message.text or "-")
    except ValueError as exc:
        await message.answer(f"Tugmalar formati xato: {exc}\nQaytadan yuboring yoki - yozing.")
        return
    await state.update_data(buttons=buttons)
    await state.set_state(PostStates.ready)
    await message.answer("✅ Post tayyor. Avval preview ko'ring yoki maqsadni tanlang:", reply_markup=post_action_keyboard())


@router.callback_query(F.data.startswith("post:"))
async def post_actions(callback: CallbackQuery, state: FSMContext) -> None:
    if not await staff_only(callback):
        return
    action = (callback.data or "").split(":", 1)[1]
    data = await state.get_data()
    if action == "cancel":
        await state.clear()
        await callback.answer("Post bekor qilindi.")
        if callback.message:
            await callback.message.answer("Post bekor qilindi.")
        return
    if not data.get("content_type"):
        await callback.answer("Post ma'lumoti topilmadi. Qaytadan boshlang.", show_alert=True)
        return
    if action == "preview":
        await callback.answer()
        try:
            await send_post(callback.bot, callback.from_user.id, data["content_type"], data.get("text", ""), data.get("file_id"), data.get("buttons", []))
            if callback.message:
                await callback.message.answer("Bu preview. Yuborish turini tanlang:", reply_markup=post_action_keyboard())
        except Exception as exc:
            await callback.message.answer(f"Preview xatosi: {str(exc)[:200]}")
        return
    if action in {"send_channels", "send_users"}:
        targets: list[int | str] = []
        if action == "send_channels":
            async with Session() as session:
                targets = list((await session.scalars(select(Channel.chat_id).where(
                    Channel.is_active.is_(True), Channel.is_post_target.is_(True)
                ))).all())
        else:
            async with Session() as session:
                targets = list((await session.scalars(select(User.id))).all())
        await callback.answer("Yuborish boshlandi.")
        if callback.message:
            await callback.message.answer(f"📤 Yuborish boshlandi. Manzillar soni: {len(targets)}")
        success = 0
        failed = 0
        for target in targets:
            try:
                await send_post(callback.bot, target, data["content_type"], data.get("text", ""), data.get("file_id"), data.get("buttons", []))
                success += 1
            except TelegramRetryAfter as exc:
                await asyncio.sleep(float(exc.retry_after) + 1)
                try:
                    await send_post(callback.bot, target, data["content_type"], data.get("text", ""), data.get("file_id"), data.get("buttons", []))
                    success += 1
                except Exception:
                    failed += 1
            except Exception as exc:
                failed += 1
                log.info("Post yuborilmadi %s: %s", target, exc)
            await asyncio.sleep(0.04)
        await state.clear()
        if callback.message:
            await callback.message.answer(f"✅ Yuborish tugadi. Muvaffaqiyatli: {success}; xato: {failed}.")
        return
    await callback.answer("Noma'lum post amali.", show_alert=True)

# ----------------------------- OWNER CONTROLS ------------------------------

@router.callback_query(F.data.startswith("owner:"))
async def owner_callbacks(callback: CallbackQuery, state: FSMContext) -> None:
    if not await owner_only(callback):
        return
    data = callback.data or ""
    await callback.answer()
    if data == "owner:admins":
        if callback.message:
            await callback.message.answer("🛡 Adminlar boshqaruvi", reply_markup=owner_admin_keyboard())
    elif data == "owner:vip":
        if callback.message:
            await callback.message.answer("💎 VIP boshqaruvi", reply_markup=owner_vip_keyboard())
    elif data == "owner:admin_add":
        await state.set_state(OwnerStates.add_admin)
        if callback.message:
            await callback.message.answer("Admin qilinadigan foydalanuvchining Telegram ID raqamini yuboring:")
    elif data == "owner:admin_remove":
        await state.set_state(OwnerStates.remove_admin)
        if callback.message:
            await callback.message.answer("Admin huquqi olib tashlanadigan foydalanuvchining ID raqamini yuboring:")
    elif data == "owner:vip_add":
        await state.set_state(OwnerStates.vip_user)
        if callback.message:
            await callback.message.answer("VIP beriladigan foydalanuvchining Telegram ID raqamini yuboring:")
    elif data == "owner:price":
        async with Session() as session:
            plans = list((await session.scalars(select(VIPPlan).order_by(VIPPlan.days.asc()))).all())
        if callback.message:
            rows = [[InlineKeyboardButton(text=f"{p.title} — {p.price} so'm", callback_data=f"owner:price_plan:{p.id}")] for p in plans]
            await callback.message.answer("Narxini o'zgartirish uchun tarifni tanlang:", reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    elif data.startswith("owner:price_plan:"):
        try:
            plan_id = int(data.split(":")[-1])
        except ValueError:
            if callback.message:
                await callback.message.answer("Tarif ID xato.")
            return
        await state.set_state(OwnerStates.plan_price)
        await state.update_data(plan_id=plan_id)
        if callback.message:
            await callback.message.answer("Yangi narxni so'mda butun son qilib yuboring:")
    elif data == "owner:plans":
        async with Session() as session:
            plans = list((await session.scalars(select(VIPPlan).order_by(VIPPlan.days.asc()))).all())
        lines = [f"{p.id}. {p.title} — {p.days} kun — {p.price:,} so'm — {'faol' if p.is_active else 'o‘chiq'}".replace(",", " ") for p in plans]
        if callback.message:
            await callback.message.answer("\n".join(lines) or "Tarif yo'q.", reply_markup=owner_vip_keyboard())


@router.message(OwnerStates.add_admin, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def owner_add_admin(message: Message, state: FSMContext) -> None:
    if not await owner_only(message):
        return
    try:
        target_id = int((message.text or "").strip())
        if target_id <= 0:
            raise ValueError
    except ValueError:
        await message.answer("Telegram ID musbat butun son bo'lishi kerak.")
        return
    if target_id in OWNER_IDS:
        await state.clear()
        await message.answer("Bu foydalanuvchi allaqachon Owner.")
        return
    async with Session() as session:
        user = await session.get(User, target_id)
        if user is None:
            user = User(id=target_id, full_name="Admin", role="admin")
            session.add(user)
        else:
            user.role = "admin"
        await session.commit()
    invalidate_role(target_id)
    await state.clear()
    await message.answer(f"✅ {target_id} admin qilindi.")


@router.message(OwnerStates.remove_admin, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def owner_remove_admin(message: Message, state: FSMContext) -> None:
    if not await owner_only(message):
        return
    try:
        target_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("Telegram ID raqam bo'lishi kerak.")
        return
    if target_id in OWNER_IDS:
        await message.answer("Owner huquqini bu bo'limdan olib tashlab bo'lmaydi.")
        await state.clear()
        return
    async with Session() as session:
        user = await session.get(User, target_id)
        if user:
            user.role = "user"
            await session.commit()
    invalidate_role(target_id)
    await state.clear()
    await message.answer("✅ Admin huquqi olib tashlandi." if user else "Foydalanuvchi bazada topilmadi.")


@router.message(OwnerStates.vip_user, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def owner_vip_user(message: Message, state: FSMContext) -> None:
    if not await owner_only(message):
        return
    try:
        target_id = int((message.text or "").strip())
    except ValueError:
        await message.answer("Telegram ID raqam bo'lishi kerak.")
        return
    await state.update_data(target_id=target_id)
    await state.set_state(OwnerStates.vip_days)
    await message.answer("Necha kunlik VIP berilsin? Butun son kiriting:")


@router.message(OwnerStates.vip_days, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def owner_vip_days(message: Message, state: FSMContext) -> None:
    if not await owner_only(message):
        return
    try:
        days = int((message.text or "").strip())
        if days < 1 or days > 3650:
            raise ValueError
    except ValueError:
        await message.answer("VIP muddati 1–3650 kun oralig'ida bo'lishi kerak.")
        return
    data = await state.get_data()
    target_id = int(data["target_id"])
    async with Session() as session:
        user = await session.get(User, target_id)
        if not user:
            user = User(id=target_id, full_name="Foydalanuvchi", role="user")
            session.add(user)
            await session.flush()
        until = user.vip_until
        if until is not None and until.tzinfo is None:
            until = until.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        user.vip_until = (until if until and until > now else now) + timedelta(days=days)
        vip_until = user.vip_until
        await session.commit()
    await state.clear()
    await message.answer(f"✅ {target_id} uchun VIP {days} kunga berildi. Tugaydi: {vip_until:%Y-%m-%d %H:%M} UTC")
    try:
        await message.bot.send_message(target_id, f"🎉 Owner sizga {days} kunlik VIP berdi. Tugash vaqti: {vip_until:%Y-%m-%d %H:%M} UTC")
    except TelegramAPIError:
        pass


@router.message(OwnerStates.plan_price, F.text, ~F.text.in_(MENU_LABELS), ~F.text.startswith("/"))
async def owner_plan_price(message: Message, state: FSMContext) -> None:
    if not await owner_only(message):
        return
    try:
        price = int((message.text or "").strip())
        if price < 0 or price > 10**12:
            raise ValueError
    except ValueError:
        await message.answer("Narx 0 yoki undan katta butun son bo'lishi kerak.")
        return
    data = await state.get_data()
    async with Session() as session:
        plan = await session.get(VIPPlan, int(data["plan_id"]))
        if not plan:
            await state.clear()
            await message.answer("Tarif topilmadi.")
            return
        plan.price = price
        await session.commit()
        plan_title = plan.title
    await state.clear()
    await message.answer(f"✅ {plan_title} narxi {price:,} so'm qilib saqlandi.".replace(",", " "))

# ------------------------------ DB / START ---------------------------------

async def init_db() -> None:
    async with engine.begin() as connection:
        await connection.run_sync(Base.metadata.create_all)
    async with Session() as session:
        existing = set((await session.scalars(select(VIPPlan.code))).all())
        defaults = [
            VIPPlan(code="month", title="1 oylik VIP", days=30, price=15000, is_active=True),
            VIPPlan(code="quarter", title="3 oylik VIP", days=90, price=40000, is_active=True),
            VIPPlan(code="year", title="1 yillik VIP", days=365, price=120000, is_active=True),
        ]
        session.add_all([plan for plan in defaults if plan.code not in existing])
        await session.commit()


async def main() -> None:
    await init_db()
    bot = Bot(token=BOT_TOKEN)
    storage = MemoryStorage()
    if REDIS_URL:
        try:
            from aiogram.fsm.storage.redis import RedisStorage
            storage = RedisStorage.from_url(REDIS_URL)
        except Exception as exc:
            log.exception("Redis ishlamadi; MemoryStorage qo'llanadi: %s", exc)
            storage = MemoryStorage()
    dp = Dispatcher(storage=storage)
    dp.include_router(router)
    middleware = SubscriptionMiddleware()
    dp.message.outer_middleware(middleware)
    dp.callback_query.outer_middleware(middleware)
    try:
        await bot.delete_webhook(drop_pending_updates=False)
        log.info("%s ishga tushmoqda", BOT_NAME)
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()
        await dp.storage.close()
        await engine.dispose()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        log.info("Bot to'xtatildi")
