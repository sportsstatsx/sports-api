# hockey/leaguedetail/hockey_leaguedetail_routes.py
from __future__ import annotations

from flask import Blueprint, request, jsonify

from hockey.leaguedetail.hockey_bundle_service import get_hockey_league_detail_bundle

hockey_leaguedetail_bp = Blueprint("hockey_leaguedetail", __name__, url_prefix="/api/hockey")


@hockey_leaguedetail_bp.route("/league_detail_bundle", methods=["GET"])
def hockey_league_detail_bundle():
    """
    Hockey League Detail 번들 엔드포인트.

    Query:
      - league_id (int, 필수)
      - season    (int, 선택)  → 없으면 서버에서 기본 시즌 선택
    """
    try:
        league_id = request.args.get("league_id", type=int)
        season = request.args.get("season", type=int)  # optional

        if not league_id:
            return jsonify({"ok": False, "error": "league_id is required"}), 400

        bundle = get_hockey_league_detail_bundle(league_id=league_id, season=season)
        return jsonify({"ok": True, "data": bundle})

    except Exception as e:
        print(f"[hockey_league_detail_bundle] ERROR: {e}")
        return jsonify({"ok": False, "error": "internal server error"}), 500
