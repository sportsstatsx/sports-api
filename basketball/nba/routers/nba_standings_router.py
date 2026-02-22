# basketball/nba/routers/nba_standings_router.py
from __future__ import annotations

from flask import Blueprint, jsonify, request

from basketball.nba.services.nba_standings_service import nba_get_standings

nba_standings_bp = Blueprint("nba_standings_bp", __name__, url_prefix="/api/nba")


@nba_standings_bp.route("/standings", methods=["GET"])
def standings():
    # NBA는 league_id가 아니라 league(text)
    league = (request.args.get("league", type=str) or "standard").strip()
    season = request.args.get("season", type=int)

    # 하키 스탠딩과 동일하게 stage/group 필터 지원 (옵션)
    stage = request.args.get("stage", type=str)
    group_name = request.args.get("group_name", type=str)

    if not season:
        return jsonify({"ok": False, "error": "season is required"}), 400

    try:
        return jsonify(
            nba_get_standings(
                league=league,
                season=season,
                stage=stage,
                group_name=group_name,
            )
        )
    except ValueError as e:
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
