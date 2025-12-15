# hockey/routers/hockey_insights_router.py
from __future__ import annotations

from flask import Blueprint, jsonify, request

from hockey.services.hockey_insights_service import hockey_get_team_insights


hockey_insights_bp = Blueprint("hockey_insights", __name__, url_prefix="/api/hockey")


@hockey_insights_bp.route("/insights", methods=["GET"])
def hockey_team_insights():
    """
    하키 Insights (팀 기반)

    Query:
      - team_id: int (필수)
      - last_n: int (선택, 기본 20, 최대 50)
      - last_minutes: int (선택, 기본 3)  -> last 3 minutes: minute >= (20-last_minutes)
    """
    team_id = request.args.get("team_id", type=int)
    last_n = request.args.get("last_n", type=int) or 20
    last_minutes = request.args.get("last_minutes", type=int) or 3

    if not team_id:
        return jsonify({"ok": False, "error": "team_id is required"}), 400

    try:
        data = hockey_get_team_insights(
            team_id=team_id,
            last_n=last_n,
            last_minutes=last_minutes,
        )
        return jsonify(data)
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
