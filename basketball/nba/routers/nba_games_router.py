# basketball/nba/routers/nba_games_router.py
from __future__ import annotations

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

import pytz
from flask import Blueprint, jsonify, request

from basketball.nba.nba_db import nba_fetch_all


nba_games_bp = Blueprint("nba_games_bp", __name__)


def _to_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


@nba_games_bp.get("/api/nba/games")
def nba_list_games():
    """
    NBA games list (MatchList 용)
    - date(YYYY-MM-DD) + timezone 기준으로 로컬 하루 범위의 경기 반환
    - league 기본: standard (nba_games.league)
    - live=1이면 라이브 상태만 반환 (status_long 기반)
    """
    date_str = request.args.get("date", type=str)
    tz_str = request.args.get("timezone", "UTC")
    league = (request.args.get("league", "standard") or "standard").strip()

    # hockey와 동일한 파라미터 형태 유지(호환)
    live = int(request.args.get("live", "0") or "0")
    limit = int(request.args.get("limit", "300") or "300")
    limit = max(1, min(limit, 1000))

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

    where: List[str] = []
    params: List[Any] = []

    where.append("g.date_start_utc >= %s AND g.date_start_utc < %s")
    params.extend([utc_start, utc_end])

    # league 필터 (기본 standard)
    if league:
        where.append("g.league = %s")
        params.append(league)

    # live=1 필터:
    # 네가 이전에 쓰던 LIVE_STATUSES와 동일한 개념으로 고정(추측 X, 너 코드에 이미 존재)
    # (DB에 현재 값이 없더라도, 조건은 “정확히” 네가 사용하던 기준)
    if live == 1:
        where.append("g.status_long = ANY(%s)")
        params.append(["In Play", "Live", "Halftime"])

    where_sql = "WHERE " + " AND ".join(where)

    sql = f"""
        SELECT
            g.id AS game_id,
            g.league,
            g.season,
            g.stage,
            g.status_long,
            g.status_short,
            g.date_start_utc,
            g.arena_name,
            g.arena_city,
            g.arena_state,

            ht.id AS home_team_id,
            ht.name AS home_name,
            ht.nickname AS home_nickname,
            ht.code AS home_code,
            ht.logo AS home_logo,

            vt.id AS visitor_team_id,
            vt.name AS visitor_name,
            vt.nickname AS visitor_nickname,
            vt.code AS visitor_code,
            vt.logo AS visitor_logo,

            -- 점수: raw_json.scores.home.points / scores.visitors.points
            CASE
                WHEN (g.raw_json #>> '{{scores,home,points}}') ~ '^[0-9]+$'
                    THEN (g.raw_json #>> '{{scores,home,points}}')::int
                ELSE NULL
            END AS home_points,
            CASE
                WHEN (g.raw_json #>> '{{scores,visitors,points}}') ~ '^[0-9]+$'
                    THEN (g.raw_json #>> '{{scores,visitors,points}}')::int
                ELSE NULL
            END AS visitor_points,

            -- 라인스코어(쿼터별): 그대로 jsonb 내려줌
            (g.raw_json #> '{{scores,home,linescore}}')     AS home_linescore,
            (g.raw_json #> '{{scores,visitors,linescore}}') AS visitor_linescore,

            -- 시계(라이브 타이머): raw_json.status.clock (예: "PT02M31.00S" 같은 값이 올 수 있음)
            (g.raw_json #>> '{{status,clock}}') AS live_clock
        FROM nba_games g
        JOIN nba_teams ht ON ht.id = g.home_team_id
        JOIN nba_teams vt ON vt.id = g.visitor_team_id
        {where_sql}
        ORDER BY g.date_start_utc ASC
        LIMIT %s
    """.strip()

    params.append(limit)

    rows = nba_fetch_all(sql, tuple(params))

    # 결과 키는 앱에서 쓰기 쉽게 hockey 스타일과 최대한 유사하게 맞춰서 내려줌
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "game_id": _to_int(r.get("game_id")),
                "league": r.get("league"),
                "season": _to_int(r.get("season")),
                "stage": _to_int(r.get("stage")),
                "status_long": r.get("status_long") or "",
                "status_short": _to_int(r.get("status_short")),
                "date_start_utc": r.get("date_start_utc"),
                "arena": {
                    "name": r.get("arena_name") or "",
                    "city": r.get("arena_city") or "",
                    "state": r.get("arena_state") or "",
                },
                "home": {
                    "id": _to_int(r.get("home_team_id")),
                    "name": r.get("home_name") or "",
                    "nickname": r.get("home_nickname") or "",
                    "code": r.get("home_code") or "",
                    "logo": r.get("home_logo"),
                    "points": _to_int(r.get("home_points")),
                    "linescore": r.get("home_linescore"),
                },
                "visitors": {
                    "id": _to_int(r.get("visitor_team_id")),
                    "name": r.get("visitor_name") or "",
                    "nickname": r.get("visitor_nickname") or "",
                    "code": r.get("visitor_code") or "",
                    "logo": r.get("visitor_logo"),
                    "points": _to_int(r.get("visitor_points")),
                    "linescore": r.get("visitor_linescore"),
                },
                "live_clock": r.get("live_clock"),
            }
        )

    return jsonify({"ok": True, "count": len(out), "rows": out})
