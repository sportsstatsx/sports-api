from flask import Blueprint, request, jsonify
from matchdetail.bundle_service import get_match_detail_bundle

matchdetail_bp = Blueprint("matchdetail", __name__)


@matchdetail_bp.route("/api/match_detail_bundle", methods=["GET"])
def match_detail_bundle():
    """
    매치디테일 화면에서 한 번만 호출하는 번들 엔드포인트.
    Query:
      - fixture_id (int, 필수)
      - league_id  (int, 필수)
      - season     (int, 필수)
      - comp       (string, 선택)   ← 추가됨
      - last_n     (string, 선택)   ← 추가됨
      - apply_override (0/1, 선택) ← 관리자 raw/merged 비교용
    """
    try:
        fixture_id = request.args.get("fixture_id", type=int)
        league_id = request.args.get("league_id", type=int)
        season = request.args.get("season", type=int)

        # 🔥 새로 추가된 필터
        comp = request.args.get("comp")     # e.g. "League", "Cup", "All"
        last_n = request.args.get("last_n") # e.g. "Last 5", "Last 10"

        # ✅ override 적용 여부 (기본 True)
        ao_raw = request.args.get("apply_override")
        if ao_raw is None:
            apply_override = True
        else:
            v = str(ao_raw).strip().lower()
            apply_override = not (v in ("0", "false", "no", "off"))

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
            comp=comp,
            last_n=last_n,
            apply_override=apply_override,
        )

        if not bundle:
            return jsonify({"ok": False, "error": "Match not found"}), 404

        return jsonify({"ok": True, "data": bundle})

    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500
