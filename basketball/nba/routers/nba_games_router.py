from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, List, Optional

import pytz
from flask import Blueprint, jsonify, request

from basketball.nba.nba_db import nba_fetch_all


nba_games_bp = Blueprint("nba_games", __name__)


def _safe_int(v: Any) -> Optional[int]:
    try:
        return int(v)
    except Exception:
        return None


@nba_games_bp.get("/api/nba/games")
def nba_list_games():
    """
    NBA games list (date + timezone)
    GET /api/nba/games?date=YYYY-MM-DD&timezone=Asia/Seoul&league=standard&season=2025&live=0|1&limit=300

    반환 형태는 hockey_games_router와 최대한 비슷하게:
    - game_id(id)
    - league/season/stage/status/status_long/date_utc
    - home/away(=visitor) 팀 메타 + 점수
    - arena
    """
    date_str = (request.args.get("date") or "").strip()
    tz_str = (request.args.get("timezone") or "UTC").strip() or "UTC"

    league = (request.args.get("league") or "standard").strip() or "standard"
    season = request.args.get("season", type=int)
    live = request.args.get("live", type=int) or 0
    limit = request.args.get("limit", type=int) or 300
    limit = max(1, min(limit, 500))

    if not date_str:
        return jsonify({"ok": False, "error": "date is required (YYYY-MM-DD)"}), 400

    try:
        user_tz = pytz.timezone(tz_str)
    except Exception:
        return jsonify({"ok": False, "error": f"Invalid timezone: {tz_str}"}), 400

    try:
        local_date = datetime.strptime(date_str, "%Y-%m-%d")
    except ValueError:
        return jsonify({"ok": False, "error": "Invalid date format YYYY-MM-DD"}), 400

    # [local_start, next_day_start)
    local_start = user_tz.localize(datetime(local_date.year, local_date.month, local_date.day, 0, 0, 0))
    local_next = local_start + timedelta(days=1)
    utc_start = local_start.astimezone(timezone.utc)
    utc_end = local_next.astimezone(timezone.utc)

    where: List[str] = ["g.league = %s", "g.date_start_utc >= %s", "g.date_start_utc < %s"]
    params: List[Any] = [league, utc_start, utc_end]

    if season is not None:
        where.append("g.season = %s")
        params.append(int(season))

    if live == 1:
        # 네가 이미 쓰던 기준 그대로
        where.append("g.status_long = ANY(%s)")
        params.append(["In Play", "Live", "Halftime"])

    where_sql = "WHERE " + " AND ".join(where)

    # scores: raw_json->scores->home/visitors->points
    sql = f"""
        SELECT
            g.id AS game_id,
            g.league,
            g.season,
            g.stage,
            g.status_long,
            g.status_short,
            g.date_start_utc,
            g.home_team_id,
            g.visitor_team_id,
            g.arena_name,
            g.arena_city,
            g.arena_state,

            th.name AS home_name,
            th.nickname AS home_nickname,
            th.code AS home_code,
            th.logo AS home_logo,

            tv.name AS away_name,
            tv.nickname AS away_nickname,
            tv.code AS away_code,
            tv.logo AS away_logo,

            NULLIF(g.raw_json #>> '{{scores,home,points}}', '')::int AS home_score,
            NULLIF(g.raw_json #>> '{{scores,visitors,points}}', '')::int AS away_score,

            g.raw_json #>> '{{status,clock}}' AS live_clock
        FROM nba_games g
        LEFT JOIN nba_teams th ON th.id = g.home_team_id
        LEFT JOIN nba_teams tv ON tv.id = g.visitor_team_id
        {where_sql}
        ORDER BY g.date_start_utc ASC
        LIMIT %s
    """.strip()

    rows = nba_fetch_all(sql, tuple(params + [limit]))

    # ✅ league logo (server-hosted static)
    # - 프록시/Cloudflare 환경에서 request.host_url 이 http 로 잡힐 수 있어서 https 로 고정
    league_logo_url = f"https://{request.host}/static/nba/Basketball_Nba_League_logo.svg"

    # hockey처럼 live=1이면 status_long을 "Live ... clock"로 가공(있을 때만)
    if rows:
        for r in rows:
            # ✅ games 응답에도 league 로고 포함
            r["league_logo"] = league_logo_url

            # (선택) league_name도 필요하면 같이 내려줌
            # - 앱에서 "standard" 대신 "NBA" 같은 표시가 필요할 때 유용
            if "league_name" not in r or r.get("league_name") in (None, ""):
                r["league_name"] = "NBA"

            if live == 1:
                st = (r.get("status_long") or "").strip()
                clock = (r.get("live_clock") or "").strip()
                if clock:
                    r["status_long"] = f"{st} {clock}"

    return jsonify({"ok": True, "count": len(rows), "rows": rows})
