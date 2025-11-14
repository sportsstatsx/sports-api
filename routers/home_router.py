from typing import Optional

from flask import Blueprint, request, jsonify

from services.home_service import (
    get_home_leagues,
    get_home_league_directory,
    get_next_matchday,
    get_prev_matchday,
)

home_bp = Blueprint("home", __name__, url_prefix="/api/home")


# ─────────────────────────────────────────
# 홈: 상단 리그 탭용 API
# ─────────────────────────────────────────

@home_bp.get("/leagues")
def home_leagues():
    """
    상단 탭용: 지정 날짜에 경기 있는 리그 목록.

    query:
      - date: yyyy-MM-dd (없으면 오늘 UTC 기준)
    """
    date_str: Optional[str] = request.args.get("date")
    rows = get_home_leagues(date_str)
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})


# ─────────────────────────────────────────
# 홈: 리그 디렉터리 (전체 리그 + 해당 날짜 경기 수)
# ─────────────────────────────────────────

@home_bp.get("/league_directory")
def home_league_directory():
    """
    리그 선택 바텀시트용: 지원 리그 + 해당 날짜 경기 수.

    query:
      - date: yyyy-MM-dd (없으면 오늘)
    """
    date_str: Optional[str] = request.args.get("date")
    rows = get_home_league_directory(date_str)
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})


# ─────────────────────────────────────────
# 홈: 다음 / 이전 매치데이 API
#   - 옛 경로(/matchday/next, /matchday/prev)도 같이 지원
# ─────────────────────────────────────────

@home_bp.get("/next_matchday")
@home_bp.get("/matchday/next")
def next_matchday():
    """
    지정 날짜 이후(포함) 첫 번째 매치데이.

    query:
      - date: yyyy-MM-dd (필수)
      - league_id: >0 이면 그 리그만, 0/없음이면 전체
    """
    date_str: Optional[str] = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "date_required"}), 400

    league_id: Optional[int] = request.args.get("league_id", type=int)
    next_date = get_next_matchday(date_str, league_id)
    return jsonify({"ok": True, "date": next_date})


@home_bp.get("/prev_matchday")
@home_bp.get("/matchday/prev")
def prev_matchday():
    """
    지정 날짜 이전(포함) 마지막 매치데이.

    query:
      - date: yyyy-MM-dd (필수)
      - league_id: >0 이면 그 리그만, 0/없음이면 전체
    """
    date_str: Optional[str] = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "date_required"}), 400

    league_id: Optional[int] = request.args.get("league_id", type=int)
    prev_date = get_prev_matchday(date_str, league_id)
    return jsonify({"ok": True, "date": prev_date})
