from flask import Blueprint, request, jsonify
from matchdetail.bundle_service import get_match_detail_bundle  # ← 원래 구조 그대로

matchdetail_bp = Blueprint("matchdetail", __name__)


@matchdetail_bp.route("/api/match_detail_bundle", methods=["GET"])
def match_detail_bundle():
    """
    매치디테일 화면에서 한 번만 호출하는 번들 엔드포인트.
    Query:
      - fixture_id (int, 필수)
      - league_id  (int, 필수)
      - season     (int, 필수)
      - comp       (str, 선택)   ← 지금은 읽기만 하고, 서버 계산에는 아직 안씀
      - last_n     (str, 선택)   ← 마찬가지
    """
    try:
        fixture_id_raw = request.args.get("fixture_id")
        league_id_raw = request.args.get("league_id")
        season_raw = request.args.get("season")

        if not fixture_id_raw or not league_id_raw or not season_raw:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "fixture_id, league_id, season are required",
                    }
                ),
                400,
            )

        try:
            fixture_id = int(fixture_id_raw)
            league_id = int(league_id_raw)
            season = int(season_raw)
        except ValueError:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "Invalid fixture_id/league_id/season",
                    }
                ),
                400,
            )

        # 🔥 나중에 쓸 comp / last_n (지금은 읽기만 하고 무시)
        comp = request.args.get("comp")     # e.g. "All", "League", "UCL" ...
        last_n = request.args.get("last_n") # e.g. "Last 5", "2024" ...

        # 현재 get_match_detail_bundle 시그니처는
        # fixture_id / league_id / season 만 받으니까 일단 그대로 유지
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
