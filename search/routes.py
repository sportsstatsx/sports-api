from __future__ import annotations

from flask import Blueprint, jsonify, request

from search.service import search_resolve, search_suggest

search_bp = Blueprint("search", __name__)


@search_bp.route("/api/search/suggest", methods=["GET"])
def search_suggest_route():
    q = request.args.get("q", type=str) or ""
    sport = request.args.get("sport", type=str) or "all"

    data = search_suggest(q=q, sport=sport)
    return jsonify({"ok": True, "data": data})


@search_bp.route("/api/search/resolve", methods=["GET"])
def search_resolve_route():
    kind = request.args.get("kind", type=str) or ""
    sport = request.args.get("sport", type=str) or ""
    league_id = request.args.get("league_id", type=int)
    team_id = request.args.get("team_id", type=int)
    season = request.args.get("season", type=int)

    if not kind:
        return jsonify({"ok": False, "error": "kind is required"}), 400

    if sport not in ("football", "hockey"):
        return jsonify({"ok": False, "error": "sport must be football or hockey"}), 400

    if kind == "league" and not league_id:
        return jsonify({"ok": False, "error": "league_id is required for league resolve"}), 400

    if kind == "team" and not team_id:
        return jsonify({"ok": False, "error": "team_id is required for team resolve"}), 400

    data = search_resolve(
        kind=kind,
        sport=sport,
        league_id=league_id,
        team_id=team_id,
        season=season,
    )
    return jsonify({"ok": True, "data": data})
