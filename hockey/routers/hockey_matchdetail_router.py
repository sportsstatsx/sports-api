# hockey/routers/hockey_matchdetail_router.py
from __future__ import annotations

from flask import Blueprint, jsonify

from hockey.services.hockey_matchdetail_service import hockey_get_game_detail


hockey_matchdetail_bp = Blueprint("hockey_matchdetail", __name__, url_prefix="/api/hockey")


@hockey_matchdetail_bp.route("/games/<int:game_id>")
@hockey_matchdetail_bp.route("/matchdetail/<int:game_id>")  # ✅ 구버전/앱 호환 alias
def hockey_game_detail(game_id: int):
    try:
        return jsonify(hockey_get_game_detail(game_id))
    except ValueError as e:
        if str(e) == "GAME_NOT_FOUND":
            return jsonify({"ok": False, "error": "Game not found"}), 404
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
