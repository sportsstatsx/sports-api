# ==============================================================
# matchdetail_router.py (Flask 버전 + comp/last_n 지원)
# ==============================================================

from flask import Blueprint, request, jsonify
from services.bundle_service import build_match_detail_bundle

matchdetail_bp = Blueprint("matchdetail", __name__)


@matchdetail_bp.route("/match_detail_bundle", methods=["GET"])
def match_detail_bundle():
    """
    Flask A방식:
      /api/match_detail_bundle?fixture_id=xxx&league_id=xxx&season=2025
                              &comp=League&last_n=Last%205
    """
    fixture_id = request.args.get("fixture_id")
    league_id = request.args.get("league_id")
    season = request.args.get("season")

    # 🔥 신규 필터
    comp = request.args.get("comp", "All")
    last_n = request.args.get("last_n", "Last 10")

    if not fixture_id or not league_id or not season:
        return jsonify({"ok": False, "error": "fixture_id / league_id / season required"})

    try:
        fixture_id_int = int(fixture_id)
        league_id_int = int(league_id)
        season_int = int(season)
    except:
        return jsonify({"ok": False, "error": "Invalid fixture_id/league_id/season"})

    data = build_match_detail_bundle(
        fixture_id=fixture_id_int,
        league_id=league_id_int,
        season_int=season_int,
        comp=comp,
        last_n=last_n
    )

    return jsonify({"ok": True, "data": data})
