"""
Floor board. Submitted and issued are separate.
Source of truth is GoHighLevel. Discord only displays.

Run:  python bot.py
Needs: .env filled from .env.example
"""

from __future__ import annotations

import asyncio
import email
import imaplib
import json
import os
import re
from datetime import datetime, timedelta
from email.header import decode_header
from zoneinfo import ZoneInfo

import aiohttp
import aiosqlite
import discord
from aiohttp import web
from discord import app_commands
from dotenv import load_dotenv

load_dotenv()

TZ = ZoneInfo(os.getenv("TZ", "America/Chicago"))
DB = os.getenv("DB_PATH", "floor.db")
TOKEN = os.getenv("DISCORD_TOKEN", "")
CHANNEL_ID = int(os.getenv("DISCORD_CHANNEL_ID") or 0)
WINS_CHANNEL_ID = int(os.getenv("WINS_CHANNEL_ID") or CHANNEL_ID or 0)
GUILD_ID = int(os.getenv("DISCORD_GUILD_ID") or 0)
WEBHOOK_SECRET = os.getenv("WEBHOOK_SECRET", "")
GHL_TOKEN = os.getenv("GHL_TOKEN", "")
GHL_LOCATION = os.getenv("GHL_LOCATION_ID", "")
GHL_PIPELINE = os.getenv("GHL_PIPELINE_ID", "")
GHL_VERSION = os.getenv("GHL_API_VERSION", "2021-07-28")
POLL_SECONDS = int(os.getenv("POLL_SECONDS") or 300)
IMAP_HOST = os.getenv("IMAP_HOST", "")
IMAP_USER = os.getenv("IMAP_USER", "")
IMAP_PASSWORD = os.getenv("IMAP_PASSWORD", "")
IMAP_FOLDER = os.getenv("IMAP_FOLDER", "INBOX")

GOLD = 0xC6A15B
MEDALS = {1: "🥇", 2: "🥈", 3: "🥉"}
ISSUED_HINTS = ("issued", "policy issued", "placed in force", "in force", "application approved", "policy delivered")
NOT_TAKEN_HINTS = ("not taken", "declined", "incomplete", "withdrawn")

intents = discord.Intents.default()
bot = discord.Client(intents=intents)
tree = app_commands.CommandTree(bot)
board_lock = asyncio.Lock()


def now() -> datetime:
    return datetime.now(TZ)


def week_start(dt: datetime) -> datetime:
    start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return start - timedelta(days=start.weekday())


def money(n: float) -> str:
    return f"${n:,.0f}"


def stage_of(name: str) -> str | None:
    n = (name or "").strip().lower()
    if not n:
        return None
    if "chargeback" in n or n in {"lapsed", "cancelled", "canceled"}:
        return "chargeback"
    if "not taken" in n or "not-taken" in n or n in {"declined", "withdrawn", "incomplete"}:
        return "not_taken"
    if "issued" in n or n in {"placed", "in force", "paid"}:
        return "issued"
    if "submitted" in n or "pending" in n:
        return "submitted"
    return None


async def db() -> aiosqlite.Connection:
    conn = await aiosqlite.connect(DB)
    conn.row_factory = aiosqlite.Row
    return conn


async def init_db() -> None:
    conn = await db()
    await conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS agents (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT UNIQUE,
            discord_id TEXT,
            ghl_user_id TEXT,
            rookie INTEGER DEFAULT 1,
            team TEXT DEFAULT 'Unassigned'
        );
        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ghl_id TEXT UNIQUE,
            agent TEXT,
            client TEXT,
            premium REAL DEFAULT 0,
            product TEXT,
            carrier TEXT,
            policy_number TEXT,
            status TEXT,
            submitted_at TEXT,
            issued_at TEXT,
            updated_at TEXT
        );
        CREATE TABLE IF NOT EXISTS meta (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """
    )
    try:
        await conn.execute("ALTER TABLE agents ADD COLUMN team TEXT DEFAULT 'Unassigned'")
    except Exception:
        pass
    await conn.commit()
    await conn.close()


async def upsert_agent(name: str, ghl_user_id: str | None = None, discord_id: str | None = None) -> None:
    if not name:
        return
    conn = await db()
    await conn.execute(
        """
        INSERT INTO agents (name, ghl_user_id, discord_id)
        VALUES (?, ?, ?)
        ON CONFLICT(name) DO UPDATE SET
            ghl_user_id = COALESCE(excluded.ghl_user_id, agents.ghl_user_id),
            discord_id = COALESCE(excluded.discord_id, agents.discord_id)
        """,
        (name, ghl_user_id, discord_id),
    )
    await conn.commit()
    await conn.close()


async def upsert_deal(payload: dict) -> str:
    """Insert or update a deal. Returns a short event label for the win channel."""
    ghl_id = payload.get("ghl_id") or f"manual-{payload['agent']}-{payload['client']}-{int(now().timestamp())}"
    status = payload["status"]
    stamp = now().isoformat()
    conn = await db()
    cur = await conn.execute("SELECT status, submitted_at, issued_at FROM deals WHERE ghl_id = ?", (ghl_id,))
    row = await cur.fetchone()
    submitted_at = stamp
    issued_at = stamp if status == "issued" else None
    event = "submitted"
    if row:
        submitted_at = row["submitted_at"] or stamp
        issued_at = row["issued_at"]
        if status == "issued" and row["status"] != "issued":
            issued_at = stamp
            event = "issued"
        elif status == row["status"]:
            event = "updated"
        else:
            event = status
        if status != "issued":
            issued_at = row["issued_at"]
    await conn.execute(
        """
        INSERT INTO deals (ghl_id, agent, client, premium, product, carrier, policy_number, status, submitted_at, issued_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(ghl_id) DO UPDATE SET
            agent = excluded.agent,
            client = excluded.client,
            premium = excluded.premium,
            product = excluded.product,
            carrier = excluded.carrier,
            policy_number = COALESCE(excluded.policy_number, deals.policy_number),
            status = excluded.status,
            submitted_at = excluded.submitted_at,
            issued_at = excluded.issued_at,
            updated_at = excluded.updated_at
        """,
        (
            ghl_id,
            payload["agent"],
            payload.get("client") or "Client",
            float(payload.get("premium") or 0),
            payload.get("product") or "",
            payload.get("carrier") or "",
            payload.get("policy_number"),
            status,
            submitted_at,
            issued_at,
            stamp,
        ),
    )
    await conn.commit()
    await conn.close()
    await upsert_agent(payload["agent"], payload.get("ghl_user_id"))
    return event


async def ranks(status: str, since: datetime) -> list[dict]:
    col = "issued_at" if status == "issued" else "submitted_at"
    status_sql = "status = 'issued'" if status == "issued" else "status = 'submitted'"
    conn = await db()
    cur = await conn.execute(
        f"""
        SELECT agent, COUNT(*) AS apps, COALESCE(SUM(premium), 0) AS premium
        FROM deals
        WHERE {status_sql} AND {col} >= ?
        GROUP BY agent
        ORDER BY premium DESC, apps DESC
        """,
        (since.isoformat(),),
    )
    rows = [dict(r) for r in await cur.fetchall()]
    await conn.close()
    return rows


async def producers(since_day: datetime, since_week: datetime, since_month: datetime) -> list[dict]:
    conn = await db()
    cur = await conn.execute(
        """
        SELECT agent,
          COALESCE(SUM(CASE WHEN status IN ('submitted', 'issued') AND submitted_at >= ? THEN premium ELSE 0 END), 0) AS d_sub,
          COALESCE(SUM(CASE WHEN status = 'issued' AND issued_at >= ? THEN premium ELSE 0 END), 0) AS d_iss,
          COALESCE(SUM(CASE WHEN status IN ('submitted', 'issued') AND submitted_at >= ? THEN premium ELSE 0 END), 0) AS w_sub,
          COALESCE(SUM(CASE WHEN status = 'issued' AND issued_at >= ? THEN premium ELSE 0 END), 0) AS w_iss,
          COALESCE(SUM(CASE WHEN status IN ('submitted', 'issued') AND submitted_at >= ? THEN premium ELSE 0 END), 0) AS m_sub,
          COALESCE(SUM(CASE WHEN status = 'issued' AND issued_at >= ? THEN premium ELSE 0 END), 0) AS m_iss
        FROM deals
        WHERE status IN ('submitted', 'issued')
        GROUP BY agent
        ORDER BY m_iss DESC, m_sub DESC
        LIMIT 10
        """,
        (
            since_day.isoformat(), since_day.isoformat(),
            since_week.isoformat(), since_week.isoformat(),
            since_month.isoformat(), since_month.isoformat(),
        ),
    )
    rows = [dict(r) for r in await cur.fetchall()]
    await conn.close()
    return rows


async def teams(since_day: datetime, since_week: datetime, since_month: datetime) -> list[dict]:
    conn = await db()
    cur = await conn.execute(
        """
        SELECT COALESCE(a.team, 'Unassigned') AS team,
          COALESCE(SUM(CASE WHEN d.status IN ('submitted', 'issued') AND d.submitted_at >= ? THEN d.premium ELSE 0 END), 0) AS d_sub,
          COALESCE(SUM(CASE WHEN d.status = 'issued' AND d.issued_at >= ? THEN d.premium ELSE 0 END), 0) AS d_iss,
          COALESCE(SUM(CASE WHEN d.status IN ('submitted', 'issued') AND d.submitted_at >= ? THEN d.premium ELSE 0 END), 0) AS w_sub,
          COALESCE(SUM(CASE WHEN d.status = 'issued' AND d.issued_at >= ? THEN d.premium ELSE 0 END), 0) AS w_iss,
          COALESCE(SUM(CASE WHEN d.status IN ('submitted', 'issued') AND d.submitted_at >= ? THEN d.premium ELSE 0 END), 0) AS m_sub,
          COALESCE(SUM(CASE WHEN d.status = 'issued' AND d.issued_at >= ? THEN d.premium ELSE 0 END), 0) AS m_iss
        FROM deals d
        LEFT JOIN agents a ON lower(a.name) = lower(d.agent)
        WHERE d.status IN ('submitted', 'issued')
        GROUP BY COALESCE(a.team, 'Unassigned')
        ORDER BY m_iss DESC, m_sub DESC
        LIMIT 10
        """,
        (
            since_day.isoformat(), since_day.isoformat(),
            since_week.isoformat(), since_week.isoformat(),
            since_month.isoformat(), since_month.isoformat(),
        ),
    )
    rows = [dict(r) for r in await cur.fetchall()]
    await conn.close()
    return rows


async def personal(since_day: datetime, since_week: datetime, since_month: datetime, since_year: datetime) -> list[dict]:
    conn = await db()
    cur = await conn.execute(
        """
        SELECT agent,
          SUM(CASE WHEN status IN ('submitted', 'issued') AND submitted_at >= ? THEN premium ELSE 0 END) AS d_sub,
          SUM(CASE WHEN status = 'issued' AND issued_at >= ? THEN premium ELSE 0 END) AS d_iss,
          SUM(CASE WHEN status IN ('submitted', 'issued') AND submitted_at >= ? THEN premium ELSE 0 END) AS w_sub,
          SUM(CASE WHEN status = 'issued' AND issued_at >= ? THEN premium ELSE 0 END) AS w_iss,
          SUM(CASE WHEN status IN ('submitted', 'issued') AND submitted_at >= ? THEN premium ELSE 0 END) AS m_sub,
          SUM(CASE WHEN status = 'issued' AND issued_at >= ? THEN premium ELSE 0 END) AS m_iss,
          SUM(CASE WHEN status IN ('submitted', 'issued') AND submitted_at >= ? THEN premium ELSE 0 END) AS y_sub,
          SUM(CASE WHEN status = 'issued' AND issued_at >= ? THEN premium ELSE 0 END) AS y_iss
        FROM deals
        WHERE status IN ('submitted', 'issued')
        GROUP BY agent
        ORDER BY y_iss DESC, y_sub DESC
        """,
        (
            since_day.isoformat(), since_day.isoformat(),
            since_week.isoformat(), since_week.isoformat(),
            since_month.isoformat(), since_month.isoformat(),
            since_year.isoformat(), since_year.isoformat(),
        ),
    )
    rows = [dict(r) for r in await cur.fetchall()]
    await conn.close()
    return rows


async def totals(since: datetime) -> dict:
    conn = await db()
    cur = await conn.execute(
        """
        SELECT
          SUM(CASE WHEN status IN ('submitted', 'issued') AND submitted_at >= ? THEN 1 ELSE 0 END) AS submitted_apps,
          SUM(CASE WHEN status IN ('submitted', 'issued') AND submitted_at >= ? THEN premium ELSE 0 END) AS submitted_prem,
          SUM(CASE WHEN status = 'issued' AND issued_at >= ? THEN 1 ELSE 0 END) AS issued_apps,
          SUM(CASE WHEN status = 'issued' AND issued_at >= ? THEN premium ELSE 0 END) AS issued_prem
        FROM deals
        WHERE status IN ('submitted', 'issued')
        """,
        (since.isoformat(), since.isoformat(), since.isoformat(), since.isoformat()),
    )
    row = dict(await cur.fetchone())
    await conn.close()
    return row


async def period_people(since: datetime) -> list[dict]:
    conn = await db()
    cur = await conn.execute(
        """
        SELECT agent,
          COALESCE(SUM(CASE WHEN status IN ('submitted', 'issued') AND submitted_at >= ? THEN premium ELSE 0 END), 0) AS sub,
          COALESCE(SUM(CASE WHEN status = 'issued' AND issued_at >= ? THEN premium ELSE 0 END), 0) AS iss,
          COALESCE(SUM(CASE WHEN status IN ('submitted', 'issued') AND submitted_at >= ? THEN 1 ELSE 0 END), 0) AS sub_apps,
          COALESCE(SUM(CASE WHEN status = 'issued' AND issued_at >= ? THEN 1 ELSE 0 END), 0) AS iss_apps
        FROM deals
        WHERE status IN ('submitted', 'issued')
        GROUP BY agent
        HAVING sub > 0 OR iss > 0
        ORDER BY iss DESC, sub DESC
        LIMIT 10
        """,
        (since.isoformat(), since.isoformat(), since.isoformat(), since.isoformat()),
    )
    rows = [dict(r) for r in await cur.fetchall()]
    await conn.close()
    return rows


async def period_teams(since: datetime) -> list[dict]:
    conn = await db()
    cur = await conn.execute(
        """
        SELECT COALESCE(a.team, 'Unassigned') AS team,
          COALESCE(SUM(CASE WHEN d.status IN ('submitted', 'issued') AND d.submitted_at >= ? THEN d.premium ELSE 0 END), 0) AS sub,
          COALESCE(SUM(CASE WHEN d.status = 'issued' AND d.issued_at >= ? THEN d.premium ELSE 0 END), 0) AS iss
        FROM deals d
        LEFT JOIN agents a ON lower(a.name) = lower(d.agent)
        WHERE d.status IN ('submitted', 'issued')
        GROUP BY COALESCE(a.team, 'Unassigned')
        HAVING sub > 0 OR iss > 0
        ORDER BY iss DESC, sub DESC
        LIMIT 10
        """,
        (since.isoformat(), since.isoformat()),
    )
    rows = [dict(r) for r in await cur.fetchall()]
    await conn.close()
    return rows


async def lifetime_people() -> list[dict]:
    conn = await db()
    cur = await conn.execute(
        """
        SELECT agent,
          COALESCE(SUM(CASE WHEN status IN ('submitted', 'issued') THEN premium ELSE 0 END), 0) AS sub,
          COALESCE(SUM(CASE WHEN status = 'issued' THEN premium ELSE 0 END), 0) AS iss
        FROM deals
        WHERE status IN ('submitted', 'issued')
        GROUP BY agent
        ORDER BY iss DESC, sub DESC
        """
    )
    rows = [dict(r) for r in await cur.fetchall()]
    await conn.close()
    return rows


async def lifetime_teams() -> list[dict]:
    conn = await db()
    cur = await conn.execute(
        """
        SELECT COALESCE(a.team, 'Unassigned') AS team,
          COALESCE(SUM(CASE WHEN d.status IN ('submitted', 'issued') THEN d.premium ELSE 0 END), 0) AS sub,
          COALESCE(SUM(CASE WHEN d.status = 'issued' THEN d.premium ELSE 0 END), 0) AS iss
        FROM deals d
        LEFT JOIN agents a ON lower(a.name) = lower(d.agent)
        WHERE d.status IN ('submitted', 'issued')
        GROUP BY COALESCE(a.team, 'Unassigned')
        ORDER BY iss DESC, sub DESC
        """
    )
    rows = [dict(r) for r in await cur.fetchall()]
    await conn.close()
    return rows


def chunk_rank(rows: list[dict], key: str, size: int = 10) -> list[str]:
    if not rows:
        return ["No production yet"]
    blocks = []
    for start in range(0, len(rows), size):
        chunk = rows[start:start + size]
        lines = []
        for i, r in enumerate(chunk, start=start + 1):
            lines.append(f"`{i}`  **{r[key]}**\n{money(r['sub'])} sub  ·  {money(r['iss'])} iss")
        blocks.append("\n".join(lines))
    return blocks


def company_block(row: dict) -> str:
    return f"Submitted  {money(row['submitted_prem'] or 0)}\nIssued  {money(row['issued_prem'] or 0)}"


async def build_boards() -> dict[str, discord.Embed]:
    current = now()
    day = current.replace(hour=0, minute=0, second=0, microsecond=0)
    week = week_start(current)
    month = current.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
    year = current.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
    windows = {
        "day": (day, "DAILY LEADERBOARD", day.strftime("%b %-d")),
        "week": (week, "WEEKLY LEADERBOARD", f"week of {week.strftime('%b %-d')}"),
        "month": (month, "MONTHLY LEADERBOARD", month.strftime("%B %Y")),
        "year": (year, "YEAR LEADERBOARD", str(year.year)),
    }
    totals_by = {}
    people_by = {}
    teams_by = {}
    boards = {}
    for key, (since, title, label) in windows.items():
        totals_by[key] = await totals(since)
        people_by[key] = await period_people(since)
        teams_by[key] = await period_teams(since)
        embed = discord.Embed(title=title, description=label, color=GOLD, timestamp=current)
        embed.add_field(name="SILVERTHORNE", value=company_block(totals_by[key]), inline=False)
        embed.add_field(name="TOP 10 PRODUCERS", value=chunk_rank(people_by[key], "agent")[0], inline=False)
        embed.add_field(name="TOP 10 TEAMS", value=chunk_rank(teams_by[key], "team")[0], inline=False)
        embed.set_footer(text="Updates with every sale  ·  source GoHighLevel")
        boards[key] = embed

    people_life = await lifetime_people()
    teams_life = await lifetime_teams()
    master = discord.Embed(
        title="MASTER LEADERBOARD",
        description="Silverthorne Financial Group  ·  lifetime  ·  updates with every sale",
        color=GOLD,
        timestamp=current,
    )
    master.add_field(name="DAY", value=company_block(totals_by["day"]), inline=True)
    master.add_field(name="WEEK", value=company_block(totals_by["week"]), inline=True)
    master.add_field(name="MONTH", value=company_block(totals_by["month"]), inline=True)
    master.add_field(name="YEAR", value=company_block(totals_by["year"]), inline=True)
    for i, block in enumerate(chunk_rank(people_life, "agent"), start=1):
        master.add_field(name="EVERY ACTIVE AGENT  ·  lifetime" if i == 1 else "AGENTS  ·  continued", value=block, inline=False)
    for i, block in enumerate(chunk_rank(teams_life, "team"), start=1):
        master.add_field(name="EVERY TEAM  ·  lifetime" if i == 1 else "TEAMS  ·  continued", value=block, inline=False)
    master.set_footer(text="Master stays pinned  ·  every agent and team, lifetime")
    master.set_image(url="attachment://banner.jpg")
    boards["master"] = master
    return boards


BOARD_ORDER = ("day", "week", "month", "year", "master")


async def push_board() -> None:
    if not CHANNEL_ID:
        return
    channel = bot.get_channel(CHANNEL_ID) or await bot.fetch_channel(CHANNEL_ID)
    async with board_lock:
        boards = await build_boards()
        conn = await db()
        for key in BOARD_ORDER:
            meta_key = f"board_{key}"
            cur = await conn.execute("SELECT value FROM meta WHERE key = ?", (meta_key,))
            row = await cur.fetchone()
            embed = boards[key]
            file = discord.File("banner.jpg", filename="banner.jpg") if key == "master" and os.path.exists("banner.jpg") else None
            msg = None
            if row:
                try:
                    msg = await channel.fetch_message(int(row["value"]))
                    if file:
                        await msg.edit(embed=embed, attachments=[file])
                    else:
                        await msg.edit(embed=embed)
                except discord.NotFound:
                    msg = None
                except Exception as exc:
                    print("board edit failed", key, exc)
                    msg = None
            if msg is None:
                msg = await channel.send(embed=embed, file=file) if file else await channel.send(embed=embed)
                await conn.execute(
                    "INSERT INTO meta (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (meta_key, str(msg.id)),
                )
                await conn.commit()
            if key == "master":
                try:
                    await msg.pin()
                except discord.HTTPException:
                    pass
        await conn.close()


async def agent_mention(name: str) -> str:
    conn = await db()
    cur = await conn.execute("SELECT discord_id FROM agents WHERE lower(name) = lower(?)", (name,))
    row = await cur.fetchone()
    await conn.close()
    if row and row["discord_id"]:
        return f"<@{row['discord_id']}>"
    return name or "Unassigned"


def short_client(name: str) -> str:
    parts = [p for p in (name or "").replace(",", " ").split() if p]
    if not parts:
        return "Client"
    if len(parts) == 1:
        return parts[0]
    return f"{parts[0]} {parts[-1][0].upper()}."


async def announce(event: str, payload: dict) -> None:
    """Ping the floor on a new submitted or issued app. Ignore updates."""
    if event not in {"submitted", "issued"} or not WINS_CHANNEL_ID:
        return
    try:
        channel = bot.get_channel(WINS_CHANNEL_ID) or await bot.fetch_channel(WINS_CHANNEL_ID)
    except discord.HTTPException:
        return
    who = await agent_mention(payload.get("agent") or "Unassigned")
    client = short_client(payload.get("client") or "Client")
    premium = money(float(payload.get("premium") or 0))
    if event == "issued":
        text = f"✅ **ISSUED**  ·  {who}  ·  {client}  ·  {premium}"
    else:
        text = f"🔥 **APP IN**  ·  {who}  ·  {client}  ·  {premium}"
    await channel.send(
        f"@here {text}",
        allowed_mentions=discord.AllowedMentions(everyone=True, users=True, roles=False),
    )


def dig(obj: dict, *path: str):
    cur = obj
    for key in path:
        if not isinstance(cur, dict):
            return None
        cur = cur.get(key)
    return cur


def deal_from_ghl(body: dict) -> dict | None:
    opp = body.get("opportunity") or dig(body, "customData", "opportunity") or body
    custom = body.get("customData") or {}
    custom_stage = custom.get("stage") or custom.get("Stage") or custom.get("STAGE")
    stage = (
        dig(opp, "pipelineStageName")
        or dig(body, "pipeline_stage")
        or custom_stage
        or body.get("stage")
        or ""
    )
    status = stage_of(str(stage))
    if not status:
        return None
    agent = (
        dig(opp, "assignedToName")
        or dig(body, "user", "name")
        or dig(body, "customData", "agent")
        or body.get("agent")
        or "Unassigned"
    )
    return {
        "ghl_id": opp.get("id") or body.get("id") or dig(body, "customData", "opportunity_id"),
        "agent": agent,
        "ghl_user_id": opp.get("assignedTo") or dig(body, "user", "id"),
        "client": opp.get("name") or dig(opp, "contact", "name") or dig(body, "full_name") or "Client",
        "premium": opp.get("monetaryValue") or opp.get("monetary_value") or dig(body, "customData", "premium") or 0,
        "product": dig(body, "customData", "product") or "",
        "carrier": dig(body, "customData", "carrier") or "",
        "policy_number": dig(body, "customData", "policy_number"),
        "status": status,
    }


async def ghl_get(session: aiohttp.ClientSession, path: str, params: dict) -> dict:
    headers = {
        "Authorization": f"Bearer {GHL_TOKEN}",
        "Version": GHL_VERSION,
        "Accept": "application/json",
    }
    async with session.get(f"https://services.leadconnectorhq.com{path}", headers=headers, params=params) as resp:
        text = await resp.text()
        if resp.status >= 300:
            print(f"GHL {resp.status} {text[:300]}")
            return {}
        return json.loads(text)


async def poll_ghl() -> int:
    if not (GHL_TOKEN and GHL_LOCATION):
        return 0
    changed = 0
    async with aiohttp.ClientSession() as session:
        params = {"location_id": GHL_LOCATION, "limit": 100}
        if GHL_PIPELINE:
            params["pipeline_id"] = GHL_PIPELINE
        data = await ghl_get(session, "/opportunities/search", params)
        for opp in data.get("opportunities") or []:
            stage_id = opp.get("pipelineStageId")
            stage_name = opp.get("pipelineStageName") or ""
            if not stage_name and stage_id:
                stage_name = STAGE_CACHE.get(stage_id, "")
            parsed = deal_from_ghl({"opportunity": {**opp, "pipelineStageName": stage_name}})
            if not parsed:
                continue
            event = await upsert_deal(parsed)
            if event in {"submitted", "issued"}:
                changed += 1
                await announce(event, parsed)
    return changed


STAGE_CACHE: dict[str, str] = {}


async def refresh_stages() -> None:
    if not (GHL_TOKEN and GHL_PIPELINE):
        return
    async with aiohttp.ClientSession() as session:
        data = await ghl_get(session, f"/opportunities/pipelines/{GHL_PIPELINE}", {})
        # Response shapes vary. Accept either stages[] or pipeline.stages[].
        stages = data.get("stages") or dig(data, "pipeline", "stages") or []
        for stage in stages:
            if stage.get("id") and stage.get("name"):
                STAGE_CACHE[stage["id"]] = stage["name"]


def imap_scan() -> list[dict]:
    """Match carrier issued mail to open submitted deals. No agent typing."""
    if not (IMAP_HOST and IMAP_USER and IMAP_PASSWORD):
        return []
    hits = []
    mail = imaplib.IMAP4_SSL(IMAP_HOST)
    mail.login(IMAP_USER, IMAP_PASSWORD)
    mail.select(IMAP_FOLDER)
    _, ids = mail.search(None, "UNSEEN")
    for num in (ids[1] or b"").split()[-30:]:
        _, msg_data = mail.fetch(num, "(RFC822)")
        raw = msg_data[0][1]
        msg = email.message_from_bytes(raw)
        subject = ""
        decoded = decode_header(msg.get("Subject") or "")
        for part, enc in decoded:
            subject += part.decode(enc or "utf-8", errors="ignore") if isinstance(part, bytes) else part
        body = ""
        if msg.is_multipart():
            for part in msg.walk():
                if part.get_content_type() == "text/plain":
                    body += part.get_payload(decode=True).decode(errors="ignore")
        else:
            payload = msg.get_payload(decode=True)
            body = payload.decode(errors="ignore") if payload else ""
        blob = f"{subject}\n{body}".lower()
        if any(h in blob for h in ISSUED_HINTS):
            hits.append({"kind": "issued", "blob": blob, "subject": subject})
        elif any(h in blob for h in NOT_TAKEN_HINTS):
            hits.append({"kind": "not_taken", "blob": blob, "subject": subject})
    mail.logout()
    return hits


async def mark_status(ghl_id: str, status: str) -> None:
    conn = await db()
    issued_at = now().isoformat() if status == "issued" else None
    await conn.execute(
        """
        UPDATE deals
        SET status = ?,
            issued_at = COALESCE(?, issued_at),
            updated_at = ?
        WHERE ghl_id = ?
        """,
        (status, issued_at, now().isoformat(), ghl_id),
    )
    await conn.commit()
    await conn.close()


async def apply_mail() -> int:
    hits = await asyncio.to_thread(imap_scan)
    if not hits:
        return 0
    conn = await db()
    cur = await conn.execute(
        "SELECT ghl_id, agent, client, premium, policy_number FROM deals WHERE status = 'submitted'"
    )
    open_deals = await cur.fetchall()
    await conn.close()
    flipped = 0
    for hit in hits:
        for deal in open_deals:
            client = (deal["client"] or "").lower()
            policy = (deal["policy_number"] or "").lower()
            last = client.split()[-1] if client else ""
            if (policy and policy in hit["blob"]) or (last and len(last) > 2 and last in hit["blob"]):
                new_status = "issued" if hit["kind"] == "issued" else hit["kind"]
                await mark_status(deal["ghl_id"], new_status)
                if new_status == "issued":
                    await announce("issued", {
                        "agent": deal["agent"],
                        "client": deal["client"],
                        "premium": deal["premium"],
                    })
                flipped += 1
                break
    return flipped


async def loop_sync() -> None:
    await bot.wait_until_ready()
    await refresh_stages()
    while True:
        try:
            await poll_ghl()
            await apply_mail()
            await push_board()
        except Exception as exc:  # noqa: BLE001
            print("sync error", exc)
        await asyncio.sleep(POLL_SECONDS)


async def handle_ghl(request: web.Request) -> web.Response:
    if WEBHOOK_SECRET and request.headers.get("X-Webhook-Secret") != WEBHOOK_SECRET:
        print("webhook rejected: bad secret")
        return web.Response(status=401, text="bad secret")
    body = await request.json()
    print("webhook", json.dumps(body)[:500])
    parsed = deal_from_ghl(body)
    if not parsed or not parsed.get("agent"):
        print("webhook ignored", body.get("customData"))
        return web.json_response({"ok": False, "reason": "stage not tracked"})
    event = await upsert_deal(parsed)
    await announce(event, parsed)
    await push_board()
    return web.json_response({"ok": True, "event": event})


async def start_web() -> None:
    app = web.Application()
    app.router.add_post("/ghl", handle_ghl)
    app.router.add_get("/health", lambda _: web.Response(text="ok"))
    app.router.add_get("/ghl", lambda _: web.Response(text="ok"))
    runner_port = int(os.getenv("PORT") or 8080)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", runner_port)
    await site.start()
    print(f"webhook listening on {runner_port}/ghl")


@tree.command(name="board", description="Refresh the floor board")
async def cmd_board(interaction: discord.Interaction) -> None:
    await interaction.response.defer(ephemeral=True)
    await push_board()
    await interaction.followup.send("Board updated.", ephemeral=True)


@tree.command(name="submit", description="Manual fallback. Prefer the GHL stage move.")
@app_commands.describe(agent="Agent name", client="Client last name", premium="Annual premium", product="Term, SI, IUL")
async def cmd_submit(interaction: discord.Interaction, agent: str, client: str, premium: float, product: str = "") -> None:
    payload = {"agent": agent, "client": client, "premium": premium, "product": product, "status": "submitted"}
    await upsert_deal(payload)
    await announce("submitted", payload)
    await push_board()
    await interaction.response.send_message(f"Submitted {client} · {money(premium)} · {agent}", ephemeral=True)


@tree.command(name="issued", description="Manual fallback. Flip the newest submitted match to issued.")
@app_commands.describe(client="Client last name", agent="Agent name")
async def cmd_issued(interaction: discord.Interaction, client: str, agent: str) -> None:
    conn = await db()
    cur = await conn.execute(
        "SELECT ghl_id, premium FROM deals WHERE lower(client) = lower(?) AND lower(agent) = lower(?) ORDER BY submitted_at DESC LIMIT 1",
        (client, agent),
    )
    row = await cur.fetchone()
    await conn.close()
    if not row:
        await interaction.response.send_message("No matching submitted app.", ephemeral=True)
        return
    await mark_status(row["ghl_id"], "issued")
    await announce("issued", {"agent": agent, "client": client, "premium": row["premium"]})
    await push_board()
    await interaction.response.send_message(f"Issued {client} · {agent}", ephemeral=True)


@tree.command(name="reset", description="Clear all production and start the boards at zero")
async def cmd_reset(interaction: discord.Interaction) -> None:
    conn = await db()
    await conn.execute("DELETE FROM deals")
    await conn.commit()
    await conn.close()
    await push_board()
    await interaction.response.send_message("Boards reset to zero.", ephemeral=True)


@tree.command(name="team", description="Assign a producer to a team")
@app_commands.describe(name="Producer name on the board", team="Team name")
async def cmd_team(interaction: discord.Interaction, name: str, team: str) -> None:
    await upsert_agent(name)
    conn = await db()
    await conn.execute("UPDATE agents SET team = ? WHERE lower(name) = lower(?)", (team.strip(), name))
    await conn.commit()
    await conn.close()
    await push_board()
    await interaction.response.send_message(f"{name} is on {team}.", ephemeral=True)
@app_commands.describe(
    name="Name exactly as it shows on the board",
    user="The agent. Leave blank to link yourself.",
)
async def cmd_link(
    interaction: discord.Interaction,
    name: str,
    user: discord.Member | None = None,
) -> None:
    target = user or interaction.user
    await upsert_agent(name, discord_id=str(target.id))
    await interaction.response.send_message(
        f"Linked {name} to {target.mention}. Sales will ping them.",
        ephemeral=True,
    )


@bot.event
async def on_ready() -> None:
    await init_db()
    if GUILD_ID:
        tree.copy_global_to(guild=discord.Object(id=GUILD_ID))
        await tree.sync(guild=discord.Object(id=GUILD_ID))
    else:
        await tree.sync()
    print(f"online as {bot.user}")
    await start_web()
    await push_board()
    bot.loop.create_task(loop_sync())


if __name__ == "__main__":
    if not TOKEN:
        raise SystemExit("Set DISCORD_TOKEN in .env")
    bot.run(TOKEN)
