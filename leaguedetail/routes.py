# leaguedetail/routes.py
from flask import Blueprint, request, jsonify

from leaguedetail.bundle_service import get_league_detail_bundle

leaguedetail_bp = Blueprint("leaguedetail", __name__)


@leaguedetail_bp.route("/api/league_detail_bundle", methods=["GET"])
def league_detail_bundle():
    """
    League Detail 화면 번들 엔드포인트.

    Query:
      - league_id (int, 필수)
      - season    (int, 선택)  → 없으면 서버에서 기본 시즌 선택
    """
    try:
        league_id = request.args.get("league_id", type=int)
        season = request.args.get("season", type=int)  # optional

        if not league_id:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "league_id is required",
                    }
                ),
                400,
            )

        bundle = get_league_detail_bundle(league_id=league_id, season=season)

        return jsonify({"ok": True, "data": bundle})

    except Exception as e:
        # 필요하면 logger로 바꿔도 됨
        print(f"[league_detail_bundle] ERROR: {e}")
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "internal server error",
                }
            ),
            500,
        )
