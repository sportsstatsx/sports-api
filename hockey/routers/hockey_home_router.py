# hockey/routers/hockey_home_router.py
from typing import Optional, List, Any

from flask import Blueprint, request, jsonify

from hockey.hockey_db import hockey_fetch_all


# /api/hockey 로 시작
hockey_home_bp = Blueprint("hockey_home", __name__, url_prefix="/api/hockey")


@hockey_home_bp.route("/games")
def route_hockey_games():
    """
    하키 DB 연결/수집 상태 확인용 (임시/디버그 성격)

    Query:
      - season: int (선택)
      - league_id: int (선택)
      - limit: int (선택, 기본 50)
    """
    season: Optional[int] = request.args.get("season", type=int)
    league_id: Optional[int] = request.args.get("league_id", type=int)
    limit: int = request.args.get("limit", type=int) or 50

    # 안전장치
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

    # ⚠️ 스키마를 100% 확신 못 하니까 일단 SELECT * 로 연결 확인부터
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
