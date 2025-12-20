# hockey/teamdetail/hockey_team_detail_routes.py

from flask import Blueprint, request, jsonify

from hockey.teamdetail.hockey_team_detail_bundle_service import (
    build_hockey_team_detail_bundle,
)

hockey_teamdetail_bp = Blueprint(
    "hockey_teamdetail",
    __name__,
    url_prefix="/api/hockey",
)


@hockey_teamdetail_bp.route("/team_detail_bundle")
def hockey_team_detail_bundle():
    team_id = request.args.get("team_id", type=int)
    league_id = request.args.get("league_id", type=int)
    season = request.args.get("season", type=int)

    if not team_id or not league_id or not season:
        return jsonify({
            "ok": False,
            "error": "team_id, league_id, season are required"
        }), 400

    bundle = build_hockey_team_detail_bundle(
        team_id=team_id,
        league_id=league_id,
        season=season,
    )

    return jsonify(bundle)
