from typing import Optional, List, Any

from flask import Blueprint, request, jsonify

from hockey.hockey_db import hockey_fetch_all


hockey_games_bp = Blueprint("hockey_games", __name__, url_prefix="/api/hockey")


@hockey_games_bp.route("/games")
def route_hockey_games():
    """
    하키 경기 목록 (DB 연결/수집 상태 확인용 - 경량)
    - SELECT * 제거 (raw_json/score_json 등 대형 컬럼으로 응답 비대해지는 것 방지)

    Query:
      - season: int (선택)
      - league_id: int (선택)
      - limit: int (선택, 기본 50, 최대 500)
      - live: int (선택, 1이면 진행중(P1/P2/P3/OT/SO)만 반환)
    """
    season: Optional[int] = request.args.get("season", type=int)
    league_id: Optional[int] = request.args.get("league_id", type=int)
    limit: int = request.args.get("limit", type=int) or 50
    live: int = request.args.get("live", type=int) or 0

    if limit < 1:
        limit = 1
    if limit > 500:
        limit = 500

    where: List[str] = []
    params: List[Any] = []

    if season is not None:
        where.append("season = %s")
        params.append(season)

    if league_id is not None:
        where.append("league_id = %s")
        params.append(league_id)

    # ✅ 진행중만 보고 싶을 때
    if live == 1:
        where.append("status IN ('P1','P2','P3','OT','SO')")

    where_sql = ""
    if where:
        where_sql = "WHERE " + " AND ".join(where)

    # ✅ live=1이면 경기시간 기준으로 정렬, 아니면 기존대로 id DESC
    order_sql = "ORDER BY game_date DESC" if live == 1 else "ORDER BY id DESC"

    # ✅ 디버그/확인용으로 필요한 최소 컬럼만 반환 (live_timer 포함)
    sql = f"""
        SELECT
            id,
            league_id,
            season,
            stage,
            group_name,
            game_date,
            status,
            status_long,
            live_timer,
            home_team_id,
            away_team_id,
            timezone
        FROM hockey_games
        {where_sql}
        {order_sql}
        LIMIT %s
    """
    params.append(limit)

    rows = hockey_fetch_all(sql, tuple(params))
    return jsonify({"ok": True, "count": len(rows), "rows": rows})
