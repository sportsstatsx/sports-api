# hockey/routers/hockey_standings_router.py
from __future__ import annotations

from flask import Blueprint, jsonify, request

from hockey.services.hockey_standings_service import hockey_get_standings

hockey_standings_bp = Blueprint("hockey_standings_bp", __name__, url_prefix="/api/hockey")


@hockey_standings_bp.route("/standings", methods=["GET"])
def standings():
    league_id = request.args.get("league_id", type=int)
    season = request.args.get("season", type=int)

    if not league_id or not season:
        return jsonify({"ok": False, "error": "league_id and season are required"}), 400

    try:
        return jsonify(hockey_get_standings(league_id=league_id, season=season))
    except ValueError as e:
        if str(e) == "LEAGUE_NOT_FOUND":
            return jsonify({"ok": False, "error": "LEAGUE_NOT_FOUND"}), 404
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
