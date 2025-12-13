# hockey/routers/hockey_standings_router.py
from __future__ import annotations

from typing import Optional

from flask import Blueprint, jsonify, request

from hockey.services.hockey_standings_service import hockey_get_standings

hockey_standings_bp = Blueprint("hockey_standings_bp", __name__)

# 권장: main.py에서 app.register_blueprint(hockey_standings_bp, url_prefix="/api/hockey")
@hockey_standings_bp.route("/standings", methods=["GET"])
def hockey_standings():
    league_id = request.args.get("league_id", type=int)
    season = request.args.get("season", type=int)

    # optional filters
    stage: Optional[str] = request.args.get("stage", type=str)
    # group_name을 정식 키로 쓰되, 혹시 group으로 들어오는 것도 허용(하위호환)
    group_name: Optional[str] = request.args.get("group_name", type=str)
    if group_name is None:
        group_name = request.args.get("group", type=str)

    if not league_id or league_id <= 0:
        return jsonify({"ok": False, "error": "league_id is required"}), 400
    if not season or season <= 0:
        return jsonify({"ok": False, "error": "season is required"}), 400

    try:
        data = hockey_get_standings(
            league_id=league_id,
            season=season,
            stage=stage,
            group_name=group_name,
        )
        return jsonify(data)
    except ValueError as e:
        # 서비스에서 명시적으로 던지는 케이스만 404/400로 나눌 수도 있지만,
        # 지금은 정식으로 404로 통일(리그 없음 등)
        msg = str(e)
        if msg in ("LEAGUE_NOT_FOUND",):
            return jsonify({"ok": False, "error": msg}), 404
        return jsonify({"ok": False, "error": msg}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
