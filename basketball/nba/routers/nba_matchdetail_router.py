# basketball/nba/routers/nba_matchdetail_router.py
from __future__ import annotations

from flask import Blueprint, jsonify, request

from basketball.nba.services.nba_matchdetail_service import nba_get_game_detail

nba_matchdetail_bp = Blueprint("nba_matchdetail", __name__, url_prefix="/api/nba")


@nba_matchdetail_bp.route("/games/<int:game_id>")
@nba_matchdetail_bp.route("/matchdetail/<int:game_id>")  # 앱 호환 alias
def nba_game_detail(game_id: int):
    """
    NBA 상세
    - override/admin 로직 없음
    """
    try:
        return jsonify(nba_get_game_detail(game_id))
    except ValueError as e:
        if str(e) == "GAME_NOT_FOUND":
            return jsonify({"ok": False, "error": "Game not found"}), 404
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
