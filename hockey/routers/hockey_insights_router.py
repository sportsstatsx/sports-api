# hockey/routers/hockey_insights_router.py
from __future__ import annotations

from flask import Blueprint, jsonify, request

from hockey.services.hockey_insights_service import hockey_get_team_insights


hockey_insights_bp = Blueprint("hockey_insights", __name__, url_prefix="/api/hockey")


@hockey_insights_bp.route("/insights", methods=["GET"])
def hockey_team_insights():
    """
    하키 팀 인사이트 (서버 계산형)

    Query:
      - team_id: int (필수)  -> 화면의 팀 선택 버튼으로 전달
      - last_n: int (선택, 기본 10, 1~50)
      - season: int (선택)   -> 원하면 시즌 필터(없으면 전체)
      - league_id: int (선택)-> 원하면 리그 필터(없으면 전체)
    """
    team_id = request.args.get("team_id", type=int)
    last_n = request.args.get("last_n", type=int) or 10
    season = request.args.get("season", type=int)
    league_id = request.args.get("league_id", type=int)

    if not team_id:
        return jsonify({"ok": False, "error": "team_id is required"}), 400

    if last_n < 1:
        last_n = 1
    if last_n > 50:
        last_n = 50

    try:
        return jsonify(
            hockey_get_team_insights(
                team_id=team_id,
                last_n=last_n,
                season=season,
                league_id=league_id,
            )
        )
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
