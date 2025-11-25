# ============================================================
#  insights_block.py (A방식 완전 구현본)
# ============================================================

from typing import List, Dict, Any, Optional
from datetime import datetime

from services.insights.insights_overall_outcome_totals import enrich_outcome_totals
from services.insights.insights_overall_resultscombos_draw import enrich_resultscombos_draw
from services.insights.insights_overall_timing import enrich_timing
from services.insights.insights_overall_firstgoal_momentum import enrich_firstgoal_momentum
from services.insights.insights_overall_shooting_efficiency import enrich_shooting_efficiency
from services.insights.insights_overall_discipline_setpieces import enrich_discipline_setpieces
from services.insights.insights_overall_goalsbytime import enrich_goals_by_time

from database.db import db

# ============================================================
#   필터 파싱 함수들
# ============================================================

LAST_N_LIST = ["Last 3", "Last 5", "Last 7", "Last 10"]


def parse_last_n(raw: Optional[str]) -> Optional[int]:
    """기존 last_n 파싱 (Last 3 → 3)"""
    if not raw:
        return None
    if raw.startswith("Last"):
        try:
            n = int(raw.split(" ")[1])
            return n
        except:
            return None
    return None


def parse_last_n_or_season(raw: Optional[str], season_list: List[int]) -> Dict[str, Any]:
    """
    lastN 또는 시즌 필터 파싱.
    return:
      {
        "mode": "last_n" or "season",
        "value": int
      }
    """
    if not raw:
        # 서버 기본값: Last 10
        return {"mode": "last_n", "value": 10}

    if raw.startswith("Last"):
        n = parse_last_n(raw)
        return {"mode": "last_n", "value": n}

    # 시즌이면?
    try:
        yr = int(raw)
        if yr in season_list:
            return {"mode": "season", "value": yr}
        # 시즌 리스트에 없어도 그냥 시즌으로 취급
        return {"mode": "season", "value": yr}
    except:
        pass

    # default fallback
    return {"mode": "last_n", "value": 10}


# ============================================================
#   COMP 옵션 만들기
# ============================================================

def fetch_team_competitions(team_id: int) -> List[str]:
    """
    팀이 참여한 모든 competition 명칭을 가져온다.
    League, Cup, Europe 등.
    """
    q = """
        SELECT DISTINCT competition_name
        FROM match_team_stats
        WHERE team_id = %s
    """
    rows = db.fetch_all(q, (team_id,))
    comps = [r["competition_name"] for r in rows if r.get("competition_name")]
    return comps


def normalize_comp_options(home_id: int, away_id: int) -> List[str]:
    """
    홈/어웨이 두 팀의 competition 을 합쳐서 옵션 생성.
    All 은 항상 포함.
    League 은 항상 포함.
    나머지는 DB에 있는 competition_name 들을 그대로 포함.
    """
    home_comps = fetch_team_competitions(home_id)
    away_comps = fetch_team_competitions(away_id)

    combined = set(home_comps + away_comps)

    # League 는 무조건 추가
    base = ["All", "League"]

    # 나머지 컵 / 대륙컵 합치기
    others = [c for c in combined if c not in ("League", "")]

    return base + sorted(others)


# ============================================================
#   LAST_N 옵션 만들기 (Last 3/5/7/10 + 시즌 목록)
# ============================================================

def fetch_team_seasons(team_id: int) -> List[int]:
    q = """
        SELECT DISTINCT season
        FROM match_team_stats
        WHERE team_id = %s
        ORDER BY season ASC
    """
    rows = db.fetch_all(q, (team_id,))
    lst = []
    for r in rows:
        try:
            lst.append(int(r["season"]))
        except:
            pass
    return sorted(list(set(lst)))


def build_last_n_options(team_id: int) -> List[str]:
    """
    Last 3/5/7/10 + 시즌 목록 (2024, 2025 등)
    """
    seasons = fetch_team_seasons(team_id)
    season_opts = [str(s) for s in seasons]
    return LAST_N_LIST + season_opts


# ============================================================
#   Matches 필터링
# ============================================================

def load_team_matches(team_id: int) -> List[Dict[str, Any]]:
    """
    team_id 의 모든 matches 를 가져온다.
    """
    q = """
        SELECT *
        FROM matches
        WHERE home_id = %s OR away_id = %s
        ORDER BY date DESC
    """
    return db.fetch_all(q, (team_id, team_id))


def filter_matches_by_comp(matches: List[Dict[str, Any]], comp: str) -> List[Dict[str, Any]]:
    if not comp or comp == "All":
        return matches
    # comp = "League" or specific cup name
    if comp == "League":
        return [m for m in matches if m.get("competition_name") == "League"]
    else:
        return [m for m in matches if m.get("competition_name") == comp]


def filter_matches_by_last_n(matches: List[Dict[str, Any]], n: int) -> List[Dict[str, Any]]:
    return matches[:n]


def filter_matches_by_season(matches: List[Dict[str, Any]], season: int) -> List[Dict[str, Any]]:
    return [m for m in matches if int(m.get("season", 0)) == season]


# ============================================================
#   Team Insights 계산 (home/away 각각)
# ============================================================

def compute_team_insights(team_id: int, comp: str, last_n_raw: str) -> Dict[str, Any]:
    """
    team_id 기준으로 comp / last_n / season 기반 필터 적용해서
    전체 insights 계산.
    """
    # 1) 모든 경기 로드
    matches = load_team_matches(team_id)
    if not matches:
        return {}

    # 2) comp 옵션 적용
    matches = filter_matches_by_comp(matches, comp)

    # 3) last_n or season 파싱
    seasons_owned = fetch_team_seasons(team_id)
    parsed = parse_last_n_or_season(last_n_raw, seasons_owned)

    if parsed["mode"] == "last_n":
        n = parsed["value"]
        matches = filter_matches_by_last_n(matches, n)
    else:
        yr = parsed["value"]
        matches = filter_matches_by_season(matches, yr)

    if not matches:
        # 경기 없으면 빈 구조
        return {
            "events_sample": 0,
            "first_goal_sample": 0,
            "goals_by_time_for": [0]*10,
            "goals_by_time_against": [0]*10,
        }

    # 4) team match list 로 enrich_* 함수들 호출
    #    각 함수 내부에서 team_id 기반 match filtering/aggregating 수행
    
    outcome = enrich_outcome_totals(team_id, matches)
    combos = enrich_resultscombos_draw(team_id, matches)
    timing = enrich_timing(team_id, matches)
    firstgoal = enrich_firstgoal_momentum(team_id, matches)
    shooting = enrich_shooting_efficiency(team_id, matches)
    discipline = enrich_discipline_setpieces(team_id, matches)
    goals = enrich_goals_by_time(team_id, matches)

    # sample count
    events_sample = len(matches)

    # first_goal_sample 은 firstgoal 모듈에서 반환하도록 했을 것으로 가정
    first_goal_sample = firstgoal.get("first_goal_sample", events_sample)

    return {
        **outcome,
        **combos,
        **timing,
        **firstgoal,
        **shooting,
        **discipline,
        **goals,
        "events_sample": events_sample,
        "first_goal_sample": first_goal_sample
    }


# ============================================================
#   PUBLIC: Insights 전체 생성 (MatchDetailBundle 에서 호출)
# ============================================================

def build_insights_overall_block(
    league_id: int,
    season_int: int,
    home_team_id: int,
    away_team_id: int,
    comp: str,
    last_n_raw: str
) -> Dict[str, Any]:
    """
    서버가 최종적으로 match_detail_bundle 에 넣을 insights_overall 블록 생성.
    """

    # ------------------------------------------------------------
    # 1) comp 옵션 생성
    # ------------------------------------------------------------
    comp_options = normalize_comp_options(home_team_id, away_team_id)

    # selected comp 기본값 처리
    selected_comp = comp
    if selected_comp not in comp_options:
        selected_comp = "All"

    # ------------------------------------------------------------
    # 2) last_n 옵션 생성
    # ------------------------------------------------------------
    lastn_options = build_last_n_options(home_team_id)  # away 기준으로도 가능함 (둘이 시즌 거의 동일)
    if last_n_raw not in lastn_options:
        # 기본값
        selected_last_n = "Last 10"
    else:
        selected_last_n = last_n_raw

    # ------------------------------------------------------------
    # 3) home / away insights 계산
    # ------------------------------------------------------------
    home_block = compute_team_insights(home_team_id, selected_comp, selected_last_n)
    away_block = compute_team_insights(away_team_id, selected_comp, selected_last_n)

    # ------------------------------------------------------------
    # 4) 반환 JSON
    # ------------------------------------------------------------
    return {
        "league_id": league_id,
        "season": season_int,
        "comp": selected_comp,
        "last_n": selected_last_n,

        "filters": {
            "comp": {
                "options": comp_options,
                "selected": selected_comp
            },
            "last_n": {
                "options": lastn_options,
                "selected": selected_last_n
            }
        },

        "home_team_id": home_team_id,
        "away_team_id": away_team_id,

        "home": home_block,
        "away": away_block
    }
