from typing import Optional, List

from flask import Blueprint, request, jsonify

from services.home_service import (
    get_home_leagues,
    get_home_league_directory,
    get_next_matchday,
    get_prev_matchday,
    get_team_info,
    get_team_insights_overall_with_filters,
    get_team_seasons,  # ⭐ 팀 시즌 목록용
)

# /api/home 로 시작하는 모든 엔드포인트
home_bp = Blueprint("home", __name__, url_prefix="/api/home")


# ─────────────────────────────────────────
# 1) 홈: 상단 리그 탭용 API
# ─────────────────────────────────────────


@home_bp.get("/leagues")
def home_leagues():
    """
    상단 탭용: 해당 날짜에 '경기가 있는 리그'만 반환.

    query:
      - date: yyyy-MM-dd (필수, "사용자 로컬 날짜")
      - timezone: IANA timezone string (선택, 없으면 UTC)
    """
    date_str: Optional[str] = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "missing_date"}), 400

    timezone_str: str = request.args.get("timezone", "UTC")

    # league_ids 필터는 아직 사용 안 함 (필요하면 나중에 확장)
    rows = get_home_leagues(date_str=date_str, timezone_str=timezone_str, league_ids=None)
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})


# ─────────────────────────────────────────
# 2) 홈: 리그 선택 바텀시트용 디렉터리
# ─────────────────────────────────────────


@home_bp.get("/league_directory")
def home_league_directory():
    """
    리그 선택 바텀시트용 디렉터리.

    - 전체 지원 리그 목록과
    - 해당 날짜(date)에 편성된 경기 수(today_count)를 함께 돌려준다.

    query:
      - date: yyyy-MM-dd (필수, "사용자 로컬 날짜")
      - timezone: IANA timezone string (선택, 없으면 UTC)
    """
    date_str: Optional[str] = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "missing_date"}), 400

    timezone_str: str = request.args.get("timezone", "UTC")

    rows = get_home_league_directory(date_str=date_str, timezone_str=timezone_str)
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})


# ─────────────────────────────────────────
# 3) 홈: 다음 / 이전 매치데이 API
# ─────────────────────────────────────────


@home_bp.get("/next_matchday")
def next_matchday():
    """
    지정 날짜 이후(포함) 첫 번째 매치데이.

    query:
      - date: yyyy-MM-dd (필수)
      - league_id: >0 이면 그 리그만, 0/없음이면 전체
    """
    date_str: Optional[str] = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "missing_date"}), 400

    league_id: Optional[int] = request.args.get("league_id", type=int)
    next_date = get_next_matchday(date_str, league_id)
    return jsonify({"ok": True, "date": next_date})


@home_bp.get("/prev_matchday")
def prev_matchday():
    """
    지정 날짜 이전 마지막 매치데이.

    query:
      - date: yyyy-MM-dd (필수)
      - league_id: >0 이면 그 리그만, 0/없음이면 전체
    """
    date_str: Optional[str] = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "missing_date"}), 400

    league_id: Optional[int] = request.args.get("league_id", type=int)
    prev_date = get_prev_matchday(date_str, league_id)
    return jsonify({"ok": True, "date": prev_date})


# 이하 team_info / team_insights_overall / team_seasons 는 변경 없음
# ...
