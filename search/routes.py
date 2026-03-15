from __future__ import annotations

from flask import Blueprint, request, jsonify, current_app

from search.service import (
    search_suggestions,
    search_selection_result,
)

search_bp = Blueprint("search", __name__)


@search_bp.route("/api/search/suggest", methods=["GET"])
def search_suggest():
    """
    자동완성 후보 목록 API

    Query:
      - q     (str, 선택)   : 검색어
      - sport (str, 선택)   : all | football | hockey
    """
    try:
        q = (request.args.get("q") or "").strip()
        sport = (request.args.get("sport") or "all").strip().lower()

        data = search_suggestions(q=q, sport=sport)
        return jsonify({"ok": True, "data": data})

    except Exception as e:  # noqa: BLE001
        current_app.logger.exception("search_suggest error")
        return jsonify({"ok": False, "error": str(e)}), 500


@search_bp.route("/api/search/select", methods=["GET"])
def search_select():
    """
    후보 선택 후 카드 결과 API

    Query:
      - kind      (str, 필수) : league | team
      - sport     (str, 필수) : football | hockey
      - league_id (int, 선택) : kind=league 일 때 필수
      - team_id   (int, 선택) : kind=team 일 때 필수
    """
    try:
        kind = (request.args.get("kind") or "").strip().lower()
        sport = (request.args.get("sport") or "").strip().lower()
        league_id = request.args.get("league_id", type=int)
        team_id = request.args.get("team_id", type=int)

        if kind not in ("league", "team"):
            return jsonify({"ok": False, "error": "kind must be 'league' or 'team'"}), 400

        if sport not in ("football", "hockey"):
            return jsonify({"ok": False, "error": "sport must be 'football' or 'hockey'"}), 400

        if kind == "league" and not league_id:
            return jsonify({"ok": False, "error": "league_id is required for league selection"}), 400

        if kind == "team" and not team_id:
            return jsonify({"ok": False, "error": "team_id is required for team selection"}), 400

        data = search_selection_result(
            kind=kind,
            sport=sport,
            league_id=league_id,
            team_id=team_id,
        )

        return jsonify({"ok": True, "data": data})

    except Exception as e:  # noqa: BLE001
        current_app.logger.exception("search_select error")
        return jsonify({"ok": False, "error": str(e)}), 500
