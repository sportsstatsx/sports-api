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
      - date: yyyy-MM-dd (필수)
    """
    date_str: Optional[str] = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "missing_date"}), 400

    # league_ids 필터는 아직 사용 안 함 (필요하면 나중에 확장)
    rows = get_home_leagues(date_str=date_str, league_ids=None)
    return jsonify({"ok": True, "rows": rows, "count": len(rows)})


# ─────────────────────────────────────────
# 2) 홈: 특정 리그 매치 디렉터리 (홈 매치리스트용)
# ─────────────────────────────────────────

@home_bp.get("/league_directory")
def home_league_directory():
    """
    홈 매치리스트용: 특정 리그의 해당 날짜 매치 리스트.

    query:
      - league_id: 리그 ID (필수)
      - date: yyyy-MM-dd (필수)
    """
    league_id: Optional[int] = request.args.get("league_id", type=int)
    date_str: Optional[str] = request.args.get("date")

    if not league_id:
        return jsonify({"ok": False, "error": "missing_league_id"}), 400
    if not date_str:
        return jsonify({"ok": False, "error": "missing_date"}), 400

    # home_service 시그니처: (league_id, date_str)
    row = get_home_league_directory(league_id=league_id, date_str=date_str)
    return jsonify({"ok": True, "row": row})


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


# ─────────────────────────────────────────
# 4) 홈: 팀 정보 (이름/국가/로고)
# ─────────────────────────────────────────

@home_bp.get("/team_info")
def home_team_info():
    """
    팀 이름/국가/로고 조회용.

    query:
      - team_id: 팀 ID (필수)
    """
    team_id: Optional[int] = request.args.get("team_id", type=int)
    if not team_id:
        return jsonify({"ok": False, "error": "team_id_required"}), 400

    team = get_team_info(team_id)
    if not team:
        return jsonify({"ok": False, "error": "not_found"}), 404

    return jsonify({"ok": True, "team": team})


# ─────────────────────────────────────────
# 5) 홈: Insights Overall (Competition / Last N / Season 필터 메타 포함)
#     → 인사이트 탭이 사용할 새 API
# ─────────────────────────────────────────

@home_bp.get("/team_insights_overall")
def home_team_insights_overall():
    """
    Insights Overall 탭 전용 API.

    query:
      - league_id: 리그 ID (필수)
      - team_id  : 팀 ID (필수)
      - season   : 시즌(연도) (선택, 없으면 서버에서 최신 시즌 사용)
      - comp     : Competition 필터 (선택, 없으면 'All')
      - last_n   : Last N 필터 (선택, 없으면 0 = 시즌 전체)
    """
    league_id: Optional[int] = request.args.get("league_id", type=int)
    team_id: Optional[int] = request.args.get("team_id", type=int)

    if not league_id:
        return jsonify({"ok": False, "error": "missing_league_id"}), 400
    if not team_id:
        return jsonify({"ok": False, "error": "missing_team_id"}), 400

    # 시즌은 선택값
    season: Optional[int] = request.args.get("season", type=int)

    # 클라이언트에서 보낸 comp / last_n 라벨 그대로 받기
    comp: Optional[str] = request.args.get("comp")
    last_n_raw: Optional[str] = request.args.get("last_n")

    row = get_team_insights_overall_with_filters(
        team_id=team_id,
        league_id=league_id,
        season=season,
        comp=comp,
        last_n=last_n_raw,
    )
    if row is None:
        return jsonify({"ok": False, "error": "not_found"}), 404

    return jsonify({"ok": True, "row": row})


# ─────────────────────────────────────────
# 6) 홈: 팀별 사용 가능한 시즌 목록
#     → 인사이트 시즌 필터용
# ─────────────────────────────────────────

@home_bp.get("/team_seasons")
def home_team_seasons():
    """
    Insights 필터용: 해당 리그/팀이 가진 시즌 목록만 돌려줌.

    query:
      - league_id: 리그 ID (필수)
      - team_id  : 팀 ID (필수)

    response 예:
      {
        "ok": true,
        "seasons": [2025, 2024]
      }
    """
    league_id: Optional[int] = request.args.get("league_id", type=int)
    team_id: Optional[int] = request.args.get("team_id", type=int)

    if not league_id:
        return jsonify({"ok": False, "error": "missing_league_id"}), 400
    if not team_id:
        return jsonify({"ok": False, "error": "missing_team_id"}), 400

    seasons = get_team_seasons(league_id=league_id, team_id=team_id)
    return jsonify({"ok": True, "seasons": seasons})
