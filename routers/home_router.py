from typing import Optional

from flask import Blueprint, request, jsonify

from services.home_service import (
    get_home_leagues,
    get_home_league_directory,
    get_next_matchday,
    get_prev_matchday,
    get_team_info,
)

home_bp = Blueprint("home", __name__, url_prefix="/api/home")


# ─────────────────────────────────────────
# 홈: 상단 리그 탭용 API
# ─────────────────────────────────────────

@home_bp.get("/leagues")
def home_leagues():
    date_str: Optional[str] = request.args.get("date")
    rows = get_home_leagues(date_str)
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})


# ─────────────────────────────────────────
# 홈: 리그 디렉터리
# ─────────────────────────────────────────

@home_bp.get("/league_directory")
def home_league_directory():
    date_str: Optional[str] = request.args.get("date")
    rows = get_home_league_directory(date_str)
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})


# ─────────────────────────────────────────
# 홈: 다음 / 이전 매치데이
# ─────────────────────────────────────────

@home_bp.get("/matchday/next")
@home_bp.get("/next_matchday")
def next_matchday():
    date_str: Optional[str] = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "date_required"}), 400

    league_id: Optional[int] = request.args.get("league_id", type=int)
    next_date = get_next_matchday(date_str, league_id)
    return jsonify({"ok": True, "date": next_date})


@home_bp.get("/matchday/prev")
@home_bp.get("/prev_matchday")
def prev_matchday():
    date_str: Optional[str] = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "date_required"}), 400

    league_id: Optional[int] = request.args.get("league_id", type=int)
    prev_date = get_prev_matchday(date_str, league_id)
    return jsonify({"ok": True, "date": prev_date})


# ─────────────────────────────────────────
# 홈: 팀 정보
# ─────────────────────────────────────────

@home_bp.get("/team_info")
def home_team_info():
    team_id: Optional[int] = request.args.get("team_id", type=int)
    if not team_id:
        return jsonify({"ok": False, "error": "team_id_required"}), 400

    team = get_team_info(team_id)
    if not team:
        return jsonify({"ok": False, "error": "not_found"}), 404

    return jsonify({"ok": True, "team": team})
