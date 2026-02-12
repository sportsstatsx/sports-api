# basketball/nba/routers/nba_insights_router.py
from __future__ import annotations

from flask import Blueprint, jsonify, request

from basketball.nba.services.nba_insights_service import nba_get_game_insights


nba_insights_bp = Blueprint("nba_insights", __name__, url_prefix="/api/nba")


@nba_insights_bp.route("/games/<int:game_id>/insights")
def nba_game_insights(game_id: int):
    try:
        team_id = request.args.get("team_id", type=int)
        last_n = request.args.get("last_n", type=int) or 10
        season = request.args.get("season", type=int)

        return jsonify(
            nba_get_game_insights(
                game_id=game_id,
                team_id=team_id,
                last_n=last_n,
                season=season,
            )
        )

    except ValueError as e:
        if str(e) == "GAME_NOT_FOUND":
            return jsonify({"ok": False, "error": "Game not found"}), 404
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
