# basketball/nba/routers/nba_matchdetail_router.py
from __future__ import annotations

from flask import Blueprint, jsonify, request

from basketball.nba.services.nba_matchdetail_service import nba_get_game_detail

nba_matchdetail_bp = Blueprint("nba_matchdetail", __name__, url_prefix="/api/nba")


@nba_matchdetail_bp.route("/games/<int:game_id>")
@nba_matchdetail_bp.route("/matchdetail/<int:game_id>")  # 앱 호환 alias
def nba_game_detail(game_id: int):
    """
    NBA match detail
    - H2H: 기본 5개, 더보기는 h2h_limit으로 재호출
      예) /api/nba/matchdetail/16728?h2h_limit=20
    """
    h2h_limit = request.args.get("h2h_limit", default=5, type=int)
    try:
        return jsonify(nba_get_game_detail(game_id, h2h_limit=h2h_limit))
    except ValueError as e:
        if str(e) == "GAME_NOT_FOUND":
            return jsonify({"ok": False, "error": "Game not found"}), 404
        return jsonify({"ok": False, "error": str(e)}), 400
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
