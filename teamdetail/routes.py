# src/teamdetail/routes.py

from __future__ import annotations

from flask import Blueprint, request, jsonify, current_app

from teamdetail.bundle_service import get_team_detail_bundle

teamdetail_bp = Blueprint("teamdetail", __name__)


@teamdetail_bp.route("/api/team_detail_bundle", methods=["GET"])
def team_detail_bundle():
    """
    ✅ 완전무결 팀디테일 번들:
    - team_id, league_id 는 필수
    - season 은 optional (없거나/틀려도 서버가 DB 기준으로 보정)
    """
    try:
        team_id = request.args.get("team_id", type=int)
        league_id = request.args.get("league_id", type=int)
        season = request.args.get("season", type=int)  # optional

        if team_id is None or league_id is None:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "team_id, league_id 는 필수입니다.",
                    }
                ),
                400,
            )

        # ✅ season 보정(없어도 되고, 잘못돼도 DB 기준으로 고정)
        from leaguedetail.seasons_block import resolve_season_for_league

        resolved_season = resolve_season_for_league(league_id=league_id, season=season)
        if resolved_season is None:
            return jsonify({"ok": False, "error": "season_not_resolvable"}), 400

        bundle = get_team_detail_bundle(
            team_id=team_id,
            league_id=league_id,
            season=resolved_season,
        )

        return jsonify({"ok": True, "data": bundle})

    except Exception as e:  # noqa: BLE001
        current_app.logger.exception("team_detail_bundle error")
        return jsonify({"ok": False, "error": str(e)}), 500

