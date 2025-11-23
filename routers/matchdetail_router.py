from flask import Blueprint, request, jsonify
from service.matchdetail.bundle_service import get_match_detail_bundle

matchdetail_bp = Blueprint("matchdetail", __name__)


@matchdetail_bp.route("/api/match_detail_bundle", methods=["GET"])
def match_detail_bundle():
    try:
        fixture_id = request.args.get("fixture_id", type=int)
        league_id = request.args.get("league_id", type=int)
        season = request.args.get("season", type=int)

        if not fixture_id or not league_id or not season:
            return jsonify({"ok": False, "error": "Missing required params"}), 400

        bundle = get_match_detail_bundle(
            fixture_id=fixture_id,
            league_id=league_id,
            season=season,
        )

        # async 함수면 await 필요 → 너 db 헬퍼는 sync니까 sync 버전 사용해야 함
        if hasattr(bundle, "__await__"):
            bundle = asyncio.run(bundle)

        if not bundle:
            return jsonify({"ok": False, "error": "Match not found"}), 404

        return jsonify({"ok": True, "data": bundle})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
