# src/teamdetail/routes.py

from __future__ import annotations

from flask import Blueprint, request, jsonify, current_app

from teamdetail.bundle_service import get_team_detail_bundle

teamdetail_bp = Blueprint("teamdetail", __name__)


@teamdetail_bp.route("/api/team_detail_bundle", methods=["GET"])
def team_detail_bundle():
    """
    Team Detail 화면에서 한 번만 호출하는 번들 엔드포인트.

    Query:
      - team_id  (int, 필수)
      - league_id (int, 필수)
      - season   (int, 필수)
    """
    try:
        team_id = request.args.get("team_id", type=int)
        league_id = request.args.get("league_id", type=int)
        season = request.args.get("season", type=int)

        if team_id is None or league_id is None or season is None:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "team_id, league_id, season 은 모두 필수입니다.",
                    }
                ),
                400,
            )

        bundle = get_team_detail_bundle(
            team_id=team_id,
            league_id=league_id,
            season=season,
        )

        return jsonify({"ok": True, "data": bundle})

    except Exception as e:  # noqa: BLE001
        current_app.logger.exception("team_detail_bundle error")
        return jsonify({"ok": False, "error": str(e)}), 500
