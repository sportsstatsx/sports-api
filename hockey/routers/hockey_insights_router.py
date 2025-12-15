# hockey/routers/hockey_insights_router.py
from __future__ import annotations

from flask import Blueprint, jsonify, request

from hockey.services.hockey_insights_service import hockey_get_game_insights


hockey_insights_bp = Blueprint("hockey_insights", __name__, url_prefix="/api/hockey")


@hockey_insights_bp.route("/games/<int:game_id>/insights", methods=["GET"])
def hockey_game_insights(game_id: int):
    # 선택: 샘플 사이즈 (기본 200)
    sample_size = request.args.get("sample_size", type=int) or 200
    if sample_size < 20:
        sample_size = 20
    if sample_size > 1000:
        sample_size = 1000

    try:
        return jsonify(hockey_get_game_insights(game_id=game_id, sample_size=sample_size))
    except ValueError as e:
        if str(e) == "GAME_NOT_FOUND":
            return jsonify({"ok": False, "error": "Game not found"}), 404
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
