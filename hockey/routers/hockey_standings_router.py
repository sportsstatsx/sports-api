# hockey/routers/hockey_standings_router.py
from __future__ import annotations

from flask import Blueprint, jsonify, request

from hockey.services.hockey_standings_service import hockey_get_standings

hockey_standings_bp = Blueprint("hockey_standings", __name__, url_prefix="/api/hockey")


@hockey_standings_bp.route("/standings", methods=["GET"])
def standings():
    league_id = request.args.get("league_id", type=int)
    season = request.args.get("season", type=int)

    if not league_id or not season:
        return jsonify({"ok": False, "error": "league_id and season are required"}), 400

    try:
        data = hockey_get_standings(league_id=league_id, season=season)
        return jsonify(data)
    except ValueError as e:
        msg = str(e)
        code = 404 if msg in ("LEAGUE_NOT_FOUND",) else 400
        return jsonify({"ok": False, "error": msg}), code
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
