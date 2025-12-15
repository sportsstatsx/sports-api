# hockey/routers/hockey_insights_router.py
from __future__ import annotations

from flask import Blueprint, jsonify, request

from hockey.services.hockey_insights_service import hockey_get_game_insights


hockey_insights_bp = Blueprint("hockey_insights", __name__, url_prefix="/api/hockey")


@hockey_insights_bp.route("/games/<int:game_id>/insights")
def hockey_game_insights(game_id: int):
    try:
        team_id = request.args.get("team_id", type=int)
        last_n = request.args.get("last_n", type=int) or 10
        last_minutes = request.args.get("last_minutes", type=int) or 3

        return jsonify(
            hockey_get_game_insights(
                game_id=game_id,
                team_id=team_id,
                last_n=last_n,
                last_minutes=last_minutes,
            )
        )

    except ValueError as e:
        if str(e) == "GAME_NOT_FOUND":
            return jsonify({"ok": False, "error": "Game not found"}), 404
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
