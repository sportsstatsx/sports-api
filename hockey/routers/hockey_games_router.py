# hockey/routers/hockey_games_router.py
from typing import Optional, List, Any

from flask import Blueprint, request, jsonify

from hockey.hockey_db import hockey_fetch_all


hockey_games_bp = Blueprint("hockey_games", __name__, url_prefix="/api/hockey")


@hockey_games_bp.route("/games")
def route_hockey_games():
    """
    하키 경기 목록 (DB 연결/수집 상태 확인 + 추후 앱 매치리스트 기반)

    Query:
      - season: int (선택)
      - league_id: int (선택)
      - limit: int (선택, 기본 50, 최대 500)
    """
    season: Optional[int] = request.args.get("season", type=int)
    league_id: Optional[int] = request.args.get("league_id", type=int)
    limit: int = request.args.get("limit", type=int) or 50

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

    where_sql = ""
    if where:
        where_sql = "WHERE " + " AND ".join(where)

    sql = f"""
        SELECT *
        FROM hockey_games
        {where_sql}
        ORDER BY id DESC
        LIMIT %s
    """
    params.append(limit)

    rows = hockey_fetch_all(sql, tuple(params))
    return jsonify({"ok": True, "count": len(rows), "rows": rows})
