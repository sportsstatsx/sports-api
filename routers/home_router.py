from typing import Optional, List

from flask import Blueprint, request, jsonify

from services.home_service import (
    get_home_leagues,
    get_home_league_directory,
    get_next_matchday,
    get_prev_matchday,
    get_team_info,
    get_team_insights_overall_with_filters,
    get_team_seasons,
)

# /api/home 로 시작하는 모든 엔드포인트
home_bp = Blueprint("home", __name__, url_prefix="/api/home")


# ─────────────────────────────────────
#  1) 상단 리그 탭용: /api/home/leagues
# ─────────────────────────────────────
@home_bp.route("/leagues")
def route_home_leagues():
    """
    상단 리그 탭용 엔드포인트.

    Query:
      - date: yyyy-MM-dd (필수, 사용자 로컬 날짜)
      - timezone: IANA timezone (예: Asia/Seoul) – 없으면 UTC
      - league_ids: "39,140,141" 형식 (선택)
    """
    date_str: Optional[str] = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "missing_date"}), 400

    timezone_str: str = request.args.get("timezone", "UTC")

    # 선택: league_ids=39,78,61
    league_ids_raw: Optional[str] = request.args.get("league_ids")
    league_ids: Optional[List[int]] = None
    if league_ids_raw:
        parsed: List[int] = []
        for part in league_ids_raw.split(","):
            part = part.strip()
            if not part:
                continue
            try:
                parsed.append(int(part))
            except ValueError:
                # 잘못된 값은 무시
                continue
        if parsed:
            league_ids = parsed

    rows = get_home_leagues(
        date_str=date_str,
        timezone_str=timezone_str,
        league_ids=league_ids,
    )
    return jsonify({"ok": True, "rows": rows})


# ─────────────────────────────────────
#  2) 리그 선택 바텀시트: /api/home/league_directory
# ─────────────────────────────────────
@home_bp.route("/league_directory")
def route_home_league_directory():
    """
    리그 선택 바텀시트용 디렉터리.

    Query:
      - date: yyyy-MM-dd (필수, 사용자 로컬 날짜)
      - timezone: IANA timezone (예: Asia/Seoul) – 없으면 UTC
    """
    date_str: Optional[str] = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "missing_date"}), 400

    timezone_str: str = request.args.get("timezone", "UTC")

    rows = get_home_league_directory(
        date_str=date_str,
        timezone_str=timezone_str,
    )
    return jsonify({"ok": True, "rows": rows})


# ─────────────────────────────────────
#  3) 다음 / 이전 매치데이
#    /api/home/next_matchday
#    /api/home/prev_matchday
# ─────────────────────────────────────
@home_bp.route("/next_matchday")
def route_next_matchday():
    """
    다음 매치데이 날짜 조회.

    Query:
      - date: yyyy-MM-dd (필수)
      - league_id: >0 이면 그 리그만, 0/없음이면 전체
    """
    date_str: Optional[str] = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "missing_date"}), 400

    league_id: Optional[int] = request.args.get("league_id", type=int)
    next_date = get_next_matchday(date_str, league_id)
    return jsonify({"ok": True, "date": next_date})


@home_bp.route("/prev_matchday")
def route_prev_matchday():
    """
    이전 매치데이 날짜 조회.

    Query:
      - date: yyyy-MM-dd (필수)
      - league_id: >0 이면 그 리그만, 0/없음이면 전체
    """
    date_str: Optional[str] = request.args.get("date")
    if not date_str:
        return jsonify({"ok": False, "error": "missing_date"}), 400

    league_id: Optional[int] = request.args.get("league_id", type=int)
    prev_date = get_prev_matchday(date_str, league_id)
    return jsonify({"ok": True, "date": prev_date})


# ─────────────────────────────────────
#  4) 팀 기본 정보: /api/home/team_info
# ─────────────────────────────────────
@home_bp.route("/team_info")
def route_team_info():
    """
    팀 기본 정보 조회.

    Query:
      - team_id: 팀 ID (필수)
    """
    team_id: Optional[int] = request.args.get("team_id", type=int)
    if not team_id:
        return jsonify({"ok": False, "error": "team_id_required"}), 400

    info = get_team_info(team_id)
    if not info:
        return jsonify({"ok": False, "error": "not_found"}), 404

    return jsonify({"ok": True, "team": info})


# ─────────────────────────────────────
#  5) 인사이트 Overall:
#     /api/home/team_insights_overall
# ─────────────────────────────────────
@home_bp.route("/team_insights_overall")
def route_team_insights_overall():
    """
    ✅ 완전무결:
    - season이 오염돼도(2027/20262027 등) DB 기준으로 보정 후 조회
    """
    league_id: Optional[int] = request.args.get("league_id", type=int)
    team_id: Optional[int] = request.args.get("team_id", type=int)

    if not league_id or not team_id:
        return jsonify({"ok": False, "error": "league_id and team_id are required"}), 400

    season: Optional[int] = request.args.get("season", type=int)
    comp: Optional[str] = request.args.get("comp", type=str)
    last_n: Optional[str] = request.args.get("last_n", type=str)

    # ✅ season 보정(없어도 됨 / 틀려도 DB 기준으로 고정)
    from leaguedetail.seasons_block import resolve_season_for_league
    season = resolve_season_for_league(league_id=int(league_id), season=season)

    row = get_team_insights_overall_with_filters(
        team_id=team_id,
        league_id=league_id,
        season=season,
        comp=comp,
        last_n=last_n,
    )
    if row is None:
        return jsonify({"ok": False, "error": "not_found"}), 404

    return jsonify({"ok": True, "row": row})



# ─────────────────────────────────────
#  6) 팀 시즌 목록: /api/home/team_seasons
# ─────────────────────────────────────
@home_bp.route("/team_seasons")
def route_team_seasons():
    """
    해당 리그/팀의 사용 가능한 시즌 목록.

    Query:
      - league_id: 리그 ID (필수)
      - team_id  : 팀 ID (필수)
    """
    league_id: Optional[int] = request.args.get("league_id", type=int)
    team_id: Optional[int] = request.args.get("team_id", type=int)

    if not league_id or not team_id:
        return jsonify({"ok": False, "error": "league_id and team_id are required"}), 400

    seasons = get_team_seasons(league_id=league_id, team_id=team_id)
    return jsonify({"ok": True, "seasons": seasons})
