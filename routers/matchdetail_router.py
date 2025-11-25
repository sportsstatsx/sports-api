from flask import Blueprint, request, jsonify
from matchdetail.bundle_service import get_match_detail_bundle  # ← 여기!

matchdetail_bp = Blueprint("matchdetail", __name__)


@matchdetail_bp.route("/api/match_detail_bundle", methods=["GET"])
def match_detail_bundle():
    """
    매치디테일 화면에서 한 번만 호출하는 번들 엔드포인트.
    Query:
      - fixture_id (int, 필수)
      - league_id  (int, 필수)
      - season     (int, 필수)
    """
    try:
        fixture_id = request.args.get("fixture_id", type=int)
        league_id = request.args.get("league_id", type=int)
        season = request.args.get("season", type=int)

        if fixture_id is None or league_id is None or season is None:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "fixture_id, league_id, season are required",
                    }
                ),
                400,
            )

        bundle = get_match_detail_bundle(
            fixture_id=fixture_id,
            league_id=league_id,
            season=season,
        )

        if not bundle:
            return jsonify({"ok": False, "error": "Match not found"}), 404

        return jsonify({"ok": True, "data": bundle})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
