import os
import re
import sqlite3
import asyncio
from datetime import datetime, timezone, timedelta
from typing import Optional

import discord
from discord.ext import commands
from dotenv import load_dotenv
from aiohttp import web

load_dotenv()

TOKEN = os.getenv("DISCORD_TOKEN")
if not TOKEN:
    raise RuntimeError("DISCORD_TOKEN 없음")

guild_id_raw = os.getenv("GUILD_ID")
if not guild_id_raw:
    raise RuntimeError("GUILD_ID 없음")

GUILD_ID = int(guild_id_raw)
TOP_ROLE_NAME = os.getenv("TOP_ROLE_NAME", "꼬들 킹")
PORT = int(os.getenv("PORT", 10000))

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "kordle_rank.db")

KST = timezone(timedelta(hours=9))

intents = discord.Intents.default()
intents.message_content = True
intents.members = True

bot = commands.Bot(command_prefix="!", intents=intents, help_command=None)


async def handle_root(request: web.Request):
    return web.Response(text="Bot is running!")


async def handle_healthz(request: web.Request):
    return web.Response(text="ok")


async def start_web_server():
    web_app = web.Application()
    web_app.router.add_get("/", handle_root)
    web_app.router.add_get("/healthz", handle_healthz)

    runner = web.AppRunner(web_app)
    await runner.setup()

    site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
    await site.start()

    print(f"Web server running on 0.0.0.0:{PORT}", flush=True)
    return runner


def now_kst() -> datetime:
    return datetime.now(KST)


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_kst_date_str() -> str:
    return now_kst().strftime("%Y-%m-%d")


def get_current_period_key() -> str:
    # 2026-01-01부터 3일 단위 시즌
    base = datetime(2026, 1, 1, tzinfo=KST).date()
    today = now_kst().date()
    diff_days = (today - base).days
    period_index = diff_days // 3
    return f"{base.isoformat()}_p{period_index}"


def get_period_bounds(period_key: str):
    # period_key 예: 2026-01-01_p12
    base_str, p_str = period_key.split("_p")
    base_date = datetime.strptime(base_str, "%Y-%m-%d").date()
    period_index = int(p_str)

    start_date = base_date + timedelta(days=period_index * 3)
    end_date = start_date + timedelta(days=2)
    return start_date, end_date


def get_period_reset_datetime(period_key: str) -> datetime:
    # 해당 시즌 종료 다음날 00:00 KST
    _, end_date = get_period_bounds(period_key)
    reset_date = end_date + timedelta(days=1)
    return datetime.combine(reset_date, datetime.min.time(), tzinfo=KST)


def format_remaining(delta: timedelta) -> str:
    total_seconds = int(delta.total_seconds())
    if total_seconds <= 0:
        return "곧 초기화됨"

    days, rem = divmod(total_seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, seconds = divmod(rem, 60)

    parts = []
    if days > 0:
        parts.append(f"{days}일")
    if hours > 0 or days > 0:
        parts.append(f"{hours}시간")
    if minutes > 0 or hours > 0 or days > 0:
        parts.append(f"{minutes}분")
    parts.append(f"{seconds}초")
    return " ".join(parts)


def get_wipe_status_text() -> str:
    period_key = get_current_period_key()
    start_date, end_date = get_period_bounds(period_key)
    now = now_kst()
    reset_at = get_period_reset_datetime(period_key)
    remaining = reset_at - now

    return "\n".join(
        [
            "```",
            "[시즌 초기화 정보]",
            f"현재 시즌: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}",
            f"초기화 시각: {reset_at.strftime('%Y-%m-%d %H:%M:%S KST')}",
            f"현재 시각: {now.strftime('%Y-%m-%d %H:%M:%S KST')}",
            f"남은 시간: {format_remaining(remaining)}",
            "```",
        ]
    )


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS submissions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            guild_id TEXT NOT NULL,
            user_id TEXT NOT NULL,
            username TEXT NOT NULL,
            kordle_date TEXT NOT NULL,
            kordle_round INTEGER,
            period_key TEXT NOT NULL,
            yellow_count INTEGER NOT NULL DEFAULT 0,
            green_count INTEGER NOT NULL DEFAULT 0,
            white_count INTEGER NOT NULL DEFAULT 0,
            score INTEGER NOT NULL DEFAULT 0,
            success_attempt INTEGER,
            is_success INTEGER NOT NULL DEFAULT 0,
            channel_id TEXT NOT NULL,
            message_id TEXT NOT NULL,
            submitted_at TEXT NOT NULL,
            UNIQUE(guild_id, user_id, kordle_date)
        )
        """
    )

    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS meta (
            guild_id TEXT NOT NULL,
            meta_key TEXT NOT NULL,
            meta_value TEXT NOT NULL,
            PRIMARY KEY (guild_id, meta_key)
        )
        """
    )

    conn.commit()
    conn.close()


def count_tokens_in_line(line: str) -> dict:
    yellow = len(re.findall(r"🟨|:yellow_square:", line))
    green = len(re.findall(r"🟩|:green_square:", line))
    white = len(re.findall(r"⬜️|⬜|:white_large_square:", line))

    return {
        "yellow": yellow,
        "green": green,
        "white": white,
    }


def parse_kordle_message(content: str):
    if not content or not isinstance(content, str):
        return None

    lines = [line.strip() for line in content.splitlines() if line.strip()]
    if len(lines) < 2:
        return None

    header_line = None
    board_lines = []
    round_number = None
    declared_attempt = None

    header_pattern = re.compile(r"꼬들\s+(\d+)\s+([1-6Xx])/6", re.IGNORECASE)

    for line in lines:
        header_match = header_pattern.search(line)
        if header_match:
            header_line = line
            round_number = int(header_match.group(1))
            declared_attempt = header_match.group(2).upper()
            continue

        counts = count_tokens_in_line(line)
        total = counts["yellow"] + counts["green"] + counts["white"]

        if total >= 4:
            board_lines.append(
                {
                    "raw": line,
                    "yellow": counts["yellow"],
                    "green": counts["green"],
                    "white": counts["white"],
                    "total": total,
                }
            )

    if header_line is None:
        return None

    if len(board_lines) < 2:
        return None

    total_yellow = sum(line["yellow"] for line in board_lines)
    total_green = sum(line["green"] for line in board_lines)
    total_white = sum(line["white"] for line in board_lines)

    score = total_yellow + (total_green * 2)

    success_attempt = None
    for idx, line in enumerate(board_lines, start=1):
        if line["total"] > 0 and line["green"] == line["total"]:
            success_attempt = idx
            break

    is_success = success_attempt is not None

    return {
        "header_line": header_line,
        "round_number": round_number,
        "declared_attempt": declared_attempt,
        "board_lines": board_lines,
        "yellow_count": total_yellow,
        "green_count": total_green,
        "white_count": total_white,
        "score": score,
        "success_attempt": success_attempt,
        "is_success": is_success,
    }


def has_submitted_today(guild_id: int, user_id: int, kordle_date: str) -> bool:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT id
        FROM submissions
        WHERE guild_id = ? AND user_id = ? AND kordle_date = ?
        """,
        (str(guild_id), str(user_id), kordle_date),
    )
    row = cur.fetchone()
    conn.close()
    return row is not None


def create_submission(
    guild_id: int,
    user_id: int,
    username: str,
    kordle_date: str,
    period_key: str,
    parsed: dict,
    channel_id: int,
    message_id: int,
) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO submissions (
            guild_id, user_id, username, kordle_date, kordle_round, period_key,
            yellow_count, green_count, white_count,
            score, success_attempt, is_success,
            channel_id, message_id, submitted_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            str(guild_id),
            str(user_id),
            username,
            kordle_date,
            parsed["round_number"],
            period_key,
            parsed["yellow_count"],
            parsed["green_count"],
            parsed["white_count"],
            parsed["score"],
            parsed["success_attempt"],
            1 if parsed["is_success"] else 0,
            str(channel_id),
            str(message_id),
            now_iso(),
        ),
    )
    conn.commit()
    conn.close()


def get_meta(guild_id: int, key: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT meta_value
        FROM meta
        WHERE guild_id = ? AND meta_key = ?
        """,
        (str(guild_id), key),
    )
    row = cur.fetchone()
    conn.close()
    return row["meta_value"] if row else None


def set_meta(guild_id: int, key: str, value: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO meta (guild_id, meta_key, meta_value)
        VALUES (?, ?, ?)
        ON CONFLICT(guild_id, meta_key)
        DO UPDATE SET meta_value = excluded.meta_value
        """,
        (str(guild_id), key, value),
    )
    conn.commit()
    conn.close()


def get_period_winner(guild_id: int, period_key: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            user_id,
            username,
            SUM(score) AS total_score,
            MIN(submitted_at) AS first_submit_at
        FROM submissions
        WHERE guild_id = ? AND period_key = ?
        GROUP BY user_id, username
        ORDER BY total_score DESC, first_submit_at ASC
        LIMIT 1
        """,
        (str(guild_id), period_key),
    )
    row = cur.fetchone()
    conn.close()
    return row


def get_period_leaderboard(guild_id: int, period_key: str, limit: int = 10):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            user_id,
            username,
            SUM(score) AS total_score,
            SUM(is_success) AS success_count,
            COUNT(*) - SUM(is_success) AS fail_count,
            COUNT(*) AS total_submissions,
            CASE
                WHEN SUM(is_success) = 0 THEN NULL
                ELSE ROUND(
                    CAST(SUM(CASE WHEN is_success = 1 THEN success_attempt ELSE 0 END) AS REAL)
                    / SUM(is_success),
                    2
                )
            END AS avg_success_attempt,
            MIN(submitted_at) AS first_submit_at
        FROM submissions
        WHERE guild_id = ? AND period_key = ?
        GROUP BY user_id, username
        ORDER BY
            total_score DESC,
            avg_success_attempt ASC,
            first_submit_at ASC
        LIMIT ?
        """,
        (str(guild_id), period_key, limit),
    )
    rows = cur.fetchall()
    conn.close()
    return rows


def get_period_user_rank(guild_id: int, user_id: int, period_key: str):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT
            user_id,
            username,
            SUM(score) AS total_score,
            SUM(is_success) AS success_count,
            COUNT(*) - SUM(is_success) AS fail_count,
            COUNT(*) AS total_submissions,
            CASE
                WHEN SUM(is_success) = 0 THEN NULL
                ELSE ROUND(
                    CAST(SUM(CASE WHEN is_success = 1 THEN success_attempt ELSE 0 END) AS REAL)
                    / SUM(is_success),
                    2
                )
            END AS avg_success_attempt,
            MIN(submitted_at) AS first_submit_at
        FROM submissions
        WHERE guild_id = ? AND period_key = ?
        GROUP BY user_id, username
        ORDER BY
            total_score DESC,
            avg_success_attempt ASC,
            first_submit_at ASC
        """,
        (str(guild_id), period_key),
    )
    rows = cur.fetchall()
    conn.close()

    for idx, row in enumerate(rows, start=1):
        if row["user_id"] == str(user_id):
            return idx, row

    return None, None


async def apply_king_role(guild: discord.Guild, winner_user_id: Optional[int]):
    role = discord.utils.get(guild.roles, name=TOP_ROLE_NAME)
    if role is None:
        return False, f"'{TOP_ROLE_NAME}' 역할 없음"

    me = guild.me
    if me is None:
        return False, "봇 멤버 정보 없음"

    if not me.guild_permissions.manage_roles:
        return False, "봇에 역할 관리 권한 없음"

    if me.top_role.position <= role.position:
        return False, f"봇 역할이 '{TOP_ROLE_NAME}'보다 위에 있어야 함"

    for member in list(role.members):
        if winner_user_id is None or member.id != winner_user_id:
            try:
                await member.remove_roles(role, reason="사흘 꼬들 킹 갱신")
            except (discord.Forbidden, discord.HTTPException):
                pass

    if winner_user_id is None:
        return True, "우승자 없음"

    winner = guild.get_member(winner_user_id)
    if winner is None:
        try:
            winner = await guild.fetch_member(winner_user_id)
        except discord.HTTPException:
            return False, "우승자 유저를 찾지 못함"

    if role not in winner.roles:
        try:
            await winner.add_roles(role, reason="사흘 꼬들 킹 부여")
        except (discord.Forbidden, discord.HTTPException):
            return False, "역할 부여 실패"

    return True, "역할 갱신 완료"


async def finalize_previous_period_if_needed(
    guild: discord.Guild,
    announce_channel: Optional[discord.abc.Messageable] = None
):
    current_period = get_current_period_key()
    last_seen_period = get_meta(guild.id, "current_period")
    last_finalized_period = get_meta(guild.id, "last_finalized_period")

    if last_seen_period is None:
        set_meta(guild.id, "current_period", current_period)
        return

    if last_seen_period == current_period:
        return

    previous_period = last_seen_period

    if last_finalized_period == previous_period:
        set_meta(guild.id, "current_period", current_period)
        return

    winner = get_period_winner(guild.id, previous_period)
    winner_id = int(winner["user_id"]) if winner else None
    winner_name = winner["username"] if winner else "없음"

    await apply_king_role(guild, winner_id)

    start_date, end_date = get_period_bounds(previous_period)
    text = (
        "```"
        f"\n[시즌 종료]"
        f"\n기간: {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}"
        f"\n이번 사흘 간 꼬들 킹: {winner_name}"
        "\n```"
    )

    if announce_channel is not None:
        try:
            await announce_channel.send(text)
        except discord.HTTPException:
            pass

    set_meta(guild.id, "last_finalized_period", previous_period)
    set_meta(guild.id, "current_period", current_period)


async def handle_auto_collect(message: discord.Message) -> None:
    guild = message.guild
    if guild is None:
        return

    await finalize_previous_period_if_needed(guild, message.channel)

    parsed = parse_kordle_message(message.content)
    if parsed is None:
        return

    kordle_date = get_kst_date_str()

    if has_submitted_today(guild.id, message.author.id, kordle_date):
        return

    period_key = get_current_period_key()

    create_submission(
        guild_id=guild.id,
        user_id=message.author.id,
        username=message.author.display_name,
        kordle_date=kordle_date,
        period_key=period_key,
        parsed=parsed,
        channel_id=message.channel.id,
        message_id=message.id,
    )

    try:
        await message.reply("점수가 반영되었사옵니다.", mention_author=False)
    except discord.HTTPException:
        pass


@bot.command(name="순위")
async def rank_command(ctx: commands.Context):
    guild = ctx.guild
    if guild is None:
        await ctx.reply("해당 명령어는 서버에서만 사용 가능하옵니다.", mention_author=False)
        return

    await finalize_previous_period_if_needed(guild, ctx.channel)

    period_key = get_current_period_key()
    start_date, end_date = get_period_bounds(period_key)
    rows = get_period_leaderboard(guild.id, period_key, 10)

    if not rows:
        await ctx.reply("현재 3일 시즌에 반영된 기록이 존재하지 않사옵니다.", mention_author=False)
        return

    medal = {1: "🥇", 2: "🥈", 3: "🥉"}

    lines = [
        "```",
        f"[현재 3일 시즌 순위] {start_date.strftime('%Y-%m-%d')} ~ {end_date.strftime('%Y-%m-%d')}",
    ]

    for i, row in enumerate(rows, start=1):
        icon = medal.get(i, f"{i}위")
        avg = row["avg_success_attempt"] if row["avg_success_attempt"] is not None else "-"
        lines.append(
            f"{icon} | {row['username']} | {row['total_score']}점 | 성공 {row['success_count']}회 | 실패 {row['fail_count']}회 | 평균 {avg}"
        )

    lines.append("```")
    await ctx.reply("\n".join(lines), mention_author=False)


@bot.command(name="내점수")
async def my_score_command(ctx: commands.Context):
    guild = ctx.guild
    if guild is None:
        await ctx.reply("해당 명령어는 서버에서만 사용 가능하옵니다.", mention_author=False)
        return

    await finalize_previous_period_if_needed(guild, ctx.channel)

    period_key = get_current_period_key()
    rank, row = get_period_user_rank(guild.id, ctx.author.id, period_key)

    if row is None:
        await ctx.reply("현재 3일 시즌에 반영된 기록이 존재하지 않사옵니다.", mention_author=False)
        return

    avg = row["avg_success_attempt"] if row["avg_success_attempt"] is not None else "-"

    await ctx.reply(
        "\n".join(
            [
                "```",
                f"[{ctx.author.display_name} 현재 시즌 기록]",
                f"순위: {rank}위",
                f"시즌 점수: {row['total_score']}점",
                f"성공: {row['success_count']}회",
                f"실패: {row['fail_count']}회",
                f"총 제출: {row['total_submissions']}회",
                f"평균 성공 시도: {avg}",
                "```",
            ]
        ),
        mention_author=False,
    )


@bot.command(name="wipe")
async def wipe_command(ctx: commands.Context):
    guild = ctx.guild
    if guild is None:
        await ctx.reply("서버에서만 사용할 수 있어.", mention_author=False)
        return

    await finalize_previous_period_if_needed(guild, ctx.channel)
    await ctx.reply(get_wipe_status_text(), mention_author=False)


@bot.command(name="도움말")
async def help_command(ctx: commands.Context):
    await ctx.reply(
        "\n".join(
            [
                "```",
                "[명령어 목록]",
                "!순위   - 현재 3일 시즌 랭킹",
                "!내점수 - 내 현재 시즌 기록",
                "!wipe   - 시즌 초기화까지 남은 시간",
                "```",
            ]
        ),
        mention_author=False,
    )
    
@bot.command(name="테스트킹")
async def test_king(ctx):
    guild = ctx.guild
    if guild is None:
        return

    success, msg = await apply_king_role(guild, ctx.author.id)
    await ctx.reply(f"테스트 결과: {msg}")

@bot.event
async def on_message(message: discord.Message):
    if message.author.bot:
        return

    await handle_auto_collect(message)
    await bot.process_commands(message)

@bot.event
async def on_ready():
    print("DB 위치:", DB_PATH, flush=True)
    print(f"{bot.user} 로그인 완료", flush=True)

    guild = bot.get_guild(GUILD_ID)
    if guild:
        print(f"대상 서버: {guild.name}", flush=True)
        announce_channel = guild.system_channel
        await finalize_previous_period_if_needed(guild, announce_channel)
    else:
        print("GUILD_ID에 해당하는 서버를 찾지 못함", flush=True)

async def main():
    print("main() 시작", flush=True)
    init_db()
    print("DB 초기화 완료", flush=True)

    runner = await start_web_server()
    print("웹서버 시작 완료", flush=True)

    try:
        print("디스코드 로그인 시도", flush=True)
        await bot.start(TOKEN)
    except Exception as e:
        print(f"디스코드 로그인 에러: {e}", flush=True)
        raise
    finally:
        print("runner cleanup", flush=True)
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())