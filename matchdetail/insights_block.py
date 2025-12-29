# matchdetail/insights_block.py

from __future__ import annotations
from typing import Any, Dict, Optional, List

from db import fetch_all

# ─────────────────────────────────────
#  (통합) 기존 services/insights/utils.py
# ─────────────────────────────────────

# ─────────────────────────────────────
#  공통 유틸
# ─────────────────────────────────────

def safe_div(num: Any, den: Any) -> float:
    """
    0 나누기, 타입 오류 등을 모두 0.0 으로 처리하는 안전한 나눗셈.
    """
    try:
        num_f = float(num)
        den_f = float(den)
    except (TypeError, ValueError):
        return 0.0

    if den_f == 0.0:
        return 0.0

    return num_f / den_f


def fmt_pct(num: Any, den: Any) -> int:
    """
    분자/분모에서 퍼센트(int, 0~100) 를 만들어 준다.
    분모가 0 이면 0 리턴.
    """
    v = safe_div(num, den) * 100.0
    return int(round(v)) if v > 0.0 else 0


def fmt_avg(total: Any, matches: Any, decimals: int = 1) -> float:
    """
    total / matches 의 평균을 소수점 n자리까지 반올림해서 리턴.
    matches <= 0 이면 0.0
    """
    try:
        total_f = float(total)
        matches_i = int(matches)
    except (TypeError, ValueError):
        return 0.0

    if matches_i <= 0:
        return 0.0

    v = total_f / matches_i
    factor = 10 ** decimals
    return round(v * factor) / factor


# ─────────────────────────────────────
#  Competition(대회) 필터 정규화
# ─────────────────────────────────────

def normalize_comp(raw: Any) -> str:
    """
    UI에서 내려오는 competition 필터 값을
    서버 내부에서 사용하는 표준 문자열로 정규화.

    새 규칙:
      - None, ""          → "All"
      - "All", "전체"     → "All"
      - "League", "리그"  → "League"
      - "UEFA", "Europe (UEFA)" 등 → "UEFA"
      - "ACL", "AFC Champions League" 등 → "ACL"
      - "Cup", "Domestic Cup", "국내컵" → "Cup"
      - 그 외 문자열(예: "UEFA Champions League", "FA Cup") → 그대로 반환
        → 나중에 competition_detail.competitions 의 name 과 1:1 매칭해서
          특정 대회만 필터링할 때 사용
    """
    if raw is None:
        return "All"

    s = str(raw).strip()
    if not s:
        return "All"

    # 이미 우리가 쓰는 표준 값이면 그대로
    if s in ("All", "League", "Cup", "UEFA", "ACL"):
        return s

    lower = s.lower()

    # 흔한 표현들 정규화
    if lower in ("all", "전체", "full", "season", "full season"):
        return "All"

    if lower in ("league", "리그"):
        return "League"

    if "uefa" in lower or "europe" in lower:
        return "UEFA"

    if "afc champions league" in lower or lower == "acl":
        return "ACL"

    if lower in ("cup", "domestic cup", "국내컵") or "cup" in lower:
        return "Cup"

    # 그 외는 그대로
    return s


# ─────────────────────────────────────
#  Last N 파싱
# ─────────────────────────────────────

def parse_last_n(raw: Any) -> int:
    """
    UI에서 last_n 값이
      - None / "" / "Season" / "All" → 0
      - "Last 5" / "Last10" / 10     → 10
    이런 식으로 올 수 있으니 정리해서 int로 반환.
    0이면 "시즌 전체" 의미.
    """
    if raw is None:
        return 0

    s = str(raw).strip()
    if not s:
        return 0

    lower = s.lower()
    if lower in ("season", "all", "full season"):
        return 0

    # "Last 5", "Last 10" 등에서 숫자만 추출
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        try:
            n = int(digits)
            return n if n > 0 else 0
        except ValueError:
            return 0

    # 마지막 fallback: 전체 문자열이 숫자일 때
    if s.isdigit():
        n = int(s)
        return n if n > 0 else 0

    return 0


# ─────────────────────────────────────
#  공통 league_ids_for_query 헬퍼
#   - insights_filters.target_league_ids_last_n 를 우선 사용
#   - 비어있으면 fallback_league_id 한 개 사용
# ─────────────────────────────────────

def build_league_ids_for_query(
    *,
    insights_filters: Optional[Dict[str, Any]],
    fallback_league_id: Optional[int],
) -> List[int]:
    league_ids: List[int] = []

    # 1) 우선: 필터에서 내려온 target_league_ids_last_n (있으면 그걸 사용)
    if insights_filters and isinstance(insights_filters, dict):
        raw_ids = insights_filters.get("target_league_ids_last_n")
        if isinstance(raw_ids, list):
            for x in raw_ids:
                try:
                    league_ids.append(int(x))
                except (TypeError, ValueError):
                    continue

        # 중복 제거 (순서 유지)
        if league_ids:
            seen = set()
            deduped: List[int] = []
            for lid in league_ids:
                if lid in seen:
                    continue
                seen.add(lid)
                deduped.append(lid)
            league_ids = deduped

    # 2) 폴백: 기본 league_id 한 개
    if not league_ids and fallback_league_id is not None:
        try:
            league_ids = [int(fallback_league_id)]
        except (TypeError, ValueError):
            league_ids = []

    return league_ids


# ─────────────────────────────────────
#  (통합) 기존 services/insights/insights_overall_outcome_totals.py
# ─────────────────────────────────────

def enrich_overall_outcome_totals(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    matches_total_api: int = 0,
    last_n: int = 0,
) -> None:
    """
    Insights Overall - Outcome & Totals / Goal Diff / Clean Sheet / No Goals / Result Combos.

    생성/보정하는 키들:
      - win_pct
      - btts_pct
      - over05_pct, over15_pct, over25_pct, over35_pct, over45_pct
      - total_over15_pct, total_over25_pct, total_over35_pct, total_over45_pct, total_over55_pct
      - win_and_total15_pct, win_and_total25_pct, win_and_total35_pct, win_and_total45_pct, win_and_total55_pct
      - win_and_btts1_pct, win_and_btts2_pct, win_and_btts3_pct
      - clean_sheet_pct, no_goals_pct
      - goal_diff_avg
      - pp_occ_avg, penalty_avg
      - ppg_per_pp, shga_per_pp, shg_per_pk, ppga_per_pk
      - (결과 콤보) win_and_over25_pct, draw_and_under25_pct 등
    """
    # 입력 안전장치
    stats = stats or {}
    insights = insights or {}

    # Last N이면 league_id 필터를 target_league_ids_last_n로 대체할 수 있음
    insights_filters = insights.get("insights_filters") if isinstance(insights, dict) else None
    league_ids_for_query = build_league_ids_for_query(
        insights_filters=insights_filters if isinstance(insights_filters, dict) else None,
        fallback_league_id=league_id,
    )

    # last_n 조건
    last_clause = ""
    if last_n and last_n > 0:
        last_clause = "ORDER BY m.date DESC LIMIT %(last_n)s"

    # 시즌 조건
    season_clause = ""
    if season_int is not None:
        season_clause = "AND m.season = %(season)s"

    # 리그 조건 (IN)
    league_clause = ""
    if league_ids_for_query:
        league_clause = "AND m.league_id = ANY(%(league_ids)s)"

    # 경기 집합(팀 기준: 홈/원정 포함)
    sql = f"""
    WITH base AS (
      SELECT
        m.id,
        m.date,
        m.home_team_id,
        m.away_team_id,
        m.home_goals,
        m.away_goals,
        CASE
          WHEN %(team_id)s = m.home_team_id THEN m.home_goals
          WHEN %(team_id)s = m.away_team_id THEN m.away_goals
          ELSE NULL
        END AS tg,
        CASE
          WHEN %(team_id)s = m.home_team_id THEN m.away_goals
          WHEN %(team_id)s = m.away_team_id THEN m.home_goals
          ELSE NULL
        END AS ag
      FROM matches m
      WHERE (m.home_team_id = %(team_id)s OR m.away_team_id = %(team_id)s)
        {season_clause}
        {league_clause}
      {last_clause}
    )
    SELECT
      COUNT(*) AS matches,
      SUM(CASE WHEN tg > ag THEN 1 ELSE 0 END) AS wins,
      SUM(CASE WHEN tg = ag THEN 1 ELSE 0 END) AS draws,
      SUM(CASE WHEN tg < ag THEN 1 ELSE 0 END) AS losses,

      SUM(CASE WHEN tg >= 1 THEN 1 ELSE 0 END) AS tg_05p,
      SUM(CASE WHEN tg >= 2 THEN 1 ELSE 0 END) AS tg_15p,
      SUM(CASE WHEN tg >= 3 THEN 1 ELSE 0 END) AS tg_25p,
      SUM(CASE WHEN tg >= 4 THEN 1 ELSE 0 END) AS tg_35p,
      SUM(CASE WHEN tg >= 5 THEN 1 ELSE 0 END) AS tg_45p,

      SUM(CASE WHEN (tg + ag) >= 2 THEN 1 ELSE 0 END) AS total_15p,
      SUM(CASE WHEN (tg + ag) >= 3 THEN 1 ELSE 0 END) AS total_25p,
      SUM(CASE WHEN (tg + ag) >= 4 THEN 1 ELSE 0 END) AS total_35p,
      SUM(CASE WHEN (tg + ag) >= 5 THEN 1 ELSE 0 END) AS total_45p,
      SUM(CASE WHEN (tg + ag) >= 6 THEN 1 ELSE 0 END) AS total_55p,

      SUM(CASE WHEN tg >= 1 AND ag >= 1 THEN 1 ELSE 0 END) AS btts1,
      SUM(CASE WHEN tg >= 2 AND ag >= 2 THEN 1 ELSE 0 END) AS btts2,
      SUM(CASE WHEN tg >= 3 AND ag >= 3 THEN 1 ELSE 0 END) AS btts3,

      SUM(CASE WHEN tg > ag AND (tg + ag) >= 2 THEN 1 ELSE 0 END) AS w_total15,
      SUM(CASE WHEN tg > ag AND (tg + ag) >= 3 THEN 1 ELSE 0 END) AS w_total25,
      SUM(CASE WHEN tg > ag AND (tg + ag) >= 4 THEN 1 ELSE 0 END) AS w_total35,
      SUM(CASE WHEN tg > ag AND (tg + ag) >= 5 THEN 1 ELSE 0 END) AS w_total45,
      SUM(CASE WHEN tg > ag AND (tg + ag) >= 6 THEN 1 ELSE 0 END) AS w_total55,

      SUM(CASE WHEN tg > ag AND tg >= 1 AND ag >= 1 THEN 1 ELSE 0 END) AS w_btts1,
      SUM(CASE WHEN tg > ag AND tg >= 2 AND ag >= 2 THEN 1 ELSE 0 END) AS w_btts2,
      SUM(CASE WHEN tg > ag AND tg >= 3 AND ag >= 3 THEN 1 ELSE 0 END) AS w_btts3,

      SUM(CASE WHEN ag = 0 THEN 1 ELSE 0 END) AS clean_sheet,
      SUM(CASE WHEN tg = 0 THEN 1 ELSE 0 END) AS no_goals,

      SUM(tg - ag) AS goal_diff_sum
    FROM base
    """

    rows = fetch_all(
        sql,
        {
            "team_id": team_id,
            "season": season_int,
            "league_ids": league_ids_for_query,
            "last_n": last_n,
        },
    )
    r = rows[0] if rows else {}

    matches = int(r.get("matches") or 0)

    # API에서 내려온 matches_total을 우선 적용(있으면)
    matches_den = matches_total_api if matches_total_api else matches

    # Outcome
    wins = int(r.get("wins") or 0)
    draws = int(r.get("draws") or 0)
    losses = int(r.get("losses") or 0)

    insights["win_pct"] = fmt_pct(wins, matches_den)
    insights["draw_pct"] = fmt_pct(draws, matches_den)
    insights["loss_pct"] = fmt_pct(losses, matches_den)

    # Team goals 0.5+ ~ 4.5+
    insights["over05_pct"] = fmt_pct(r.get("tg_05p"), matches_den)
    insights["over15_pct"] = fmt_pct(r.get("tg_15p"), matches_den)
    insights["over25_pct"] = fmt_pct(r.get("tg_25p"), matches_den)
    insights["over35_pct"] = fmt_pct(r.get("tg_35p"), matches_den)
    insights["over45_pct"] = fmt_pct(r.get("tg_45p"), matches_den)

    # Total 1.5+ ~ 5.5+
    insights["total_over15_pct"] = fmt_pct(r.get("total_15p"), matches_den)
    insights["total_over25_pct"] = fmt_pct(r.get("total_25p"), matches_den)
    insights["total_over35_pct"] = fmt_pct(r.get("total_35p"), matches_den)
    insights["total_over45_pct"] = fmt_pct(r.get("total_45p"), matches_den)
    insights["total_over55_pct"] = fmt_pct(r.get("total_55p"), matches_den)

    # BTTS 1+/2+/3+
    insights["btts1_pct"] = fmt_pct(r.get("btts1"), matches_den)
    insights["btts2_pct"] = fmt_pct(r.get("btts2"), matches_den)
    insights["btts3_pct"] = fmt_pct(r.get("btts3"), matches_den)

    # Win & Total
    insights["win_and_total15_pct"] = fmt_pct(r.get("w_total15"), matches_den)
    insights["win_and_total25_pct"] = fmt_pct(r.get("w_total25"), matches_den)
    insights["win_and_total35_pct"] = fmt_pct(r.get("w_total35"), matches_den)
    insights["win_and_total45_pct"] = fmt_pct(r.get("w_total45"), matches_den)
    insights["win_and_total55_pct"] = fmt_pct(r.get("w_total55"), matches_den)

    # Win & BTTS
    insights["win_and_btts1_pct"] = fmt_pct(r.get("w_btts1"), matches_den)
    insights["win_and_btts2_pct"] = fmt_pct(r.get("w_btts2"), matches_den)
    insights["win_and_btts3_pct"] = fmt_pct(r.get("w_btts3"), matches_den)

    # Clean Sheet / No Goals
    insights["clean_sheet_pct"] = fmt_pct(r.get("clean_sheet"), matches_den)
    insights["no_goals_pct"] = fmt_pct(r.get("no_goals"), matches_den)

    # Goal diff avg
    insights["goal_diff_avg"] = fmt_avg(r.get("goal_diff_sum"), matches_den, decimals=1)


# ─────────────────────────────────────
#  (통합) 기존 services/insights/insights_overall_goalsbytime.py
# ─────────────────────────────────────

def enrich_overall_goals_by_time(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    last_n: Optional[int] = None,  # Last N (없으면 시즌 전체)
) -> None:
    """
    Goals by Time 섹션.

    🔹 기본 아이디어
      1) matches 테이블에서 Competition + Last N 기준으로
         이 팀이 참여한 최근 N경기(혹은 시즌 전체)를 가져온다.
      2) match_events 테이블에서 goal 이벤트를 가져와서
         minute를 기준으로 구간별로 카운트한다.
      3) 팀 득점(for) / 실점(against)을 각각 계산한다.
    """
    stats = stats or {}
    insights = insights or {}

    last_n_int = int(last_n or 0)

    season_clause = ""
    if season_int is not None:
        season_clause = "AND m.season = %(season)s"

    last_clause = ""
    if last_n_int > 0:
        last_clause = "ORDER BY m.date DESC LIMIT %(last_n)s"

    # 1) 경기 집합
    sql_matches = f"""
    SELECT m.id
    FROM matches m
    WHERE (m.home_team_id = %(team_id)s OR m.away_team_id = %(team_id)s)
      AND m.league_id = %(league_id)s
      {season_clause}
    {last_clause}
    """
    match_rows = fetch_all(
        sql_matches,
        {"team_id": team_id, "league_id": league_id, "season": season_int, "last_n": last_n_int},
    )
    match_ids = [int(r["id"]) for r in match_rows if r.get("id") is not None]
    if not match_ids:
        insights["goals_by_time_for"] = []
        insights["goals_by_time_against"] = []
        return

    # 2) 이벤트 조회 (goal)
    sql_events = """
    SELECT
      e.match_id,
      e.team_id,
      e.minute
    FROM match_events e
    WHERE e.match_id = ANY(%(match_ids)s)
      AND e.type = 'Goal'
      AND e.minute IS NOT NULL
    """
    ev_rows = fetch_all(sql_events, {"match_ids": match_ids})

    # 구간 정의 (0-15, 16-30, 31-45, 46-60, 61-75, 76-90, 90+)
    buckets = [
        ("0-15", 0, 15),
        ("16-30", 16, 30),
        ("31-45", 31, 45),
        ("46-60", 46, 60),
        ("61-75", 61, 75),
        ("76-90", 76, 90),
        ("90+", 91, 9999),
    ]

    def _init_counts() -> List[Dict[str, Any]]:
        return [{"bucket": name, "count": 0} for name, _, _ in buckets]

    goals_for = _init_counts()
    goals_against = _init_counts()

    for r in ev_rows:
        try:
            minute = int(r.get("minute") or 0)
        except (TypeError, ValueError):
            continue

        ev_team_id = r.get("team_id")
        is_for = (str(ev_team_id) == str(team_id))

        # 버킷 찾기
        idx = None
        for i, (_, lo, hi) in enumerate(buckets):
            if lo <= minute <= hi:
                idx = i
                break
        if idx is None:
            continue

        if is_for:
            goals_for[idx]["count"] += 1
        else:
            goals_against[idx]["count"] += 1

    insights["goals_by_time_for"] = goals_for
    insights["goals_by_time_against"] = goals_against


# ─────────────────────────────────────
#  안전한 int 변환
# ─────────────────────────────────────
def _extract_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


# ─────────────────────────────────────
#  API 입력값 파싱 (league_id, season, last_n, comp 등)
# ─────────────────────────────────────
def _parse_season(raw_season: Any) -> Optional[int]:
    season_int = _extract_int(raw_season)
    return season_int


def _parse_last_n(raw_last_n: Any) -> int:
    return parse_last_n(raw_last_n)


def _normalize_comp(raw_comp: Any) -> str:
    return normalize_comp(raw_comp)


# ─────────────────────────────────────
#  Competition 필터용 league_id 집합 만들기
# ─────────────────────────────────────
def _build_comp_league_ids(
    *,
    comp_std: str,
    competition_detail: Optional[Dict[str, Any]],
    league_id: int,
) -> List[int]:
    """
    comp_std:
      - "All"    → [league_id]
      - "League" → competition_detail.competitions 중 type='league' 의 league_id들
      - "Cup"    → competition_detail.competitions 중 type='cup' 의 league_id들
      - "UEFA"   → competition_detail.competitions 중 name에 'UEFA'/'Champions League' 등 포함하는 것
      - "ACL"    → competition_detail.competitions 중 name에 'AFC'/'Champions League' 등 포함하는 것
      - 기타 문자열 → name 정확히 매칭되는 것
    """
    if comp_std == "All":
        return [league_id]

    comp = competition_detail or {}
    comps = comp.get("competitions") or []
    if not isinstance(comps, list):
        return [league_id]

    league_ids: List[int] = []
    uefa_ids: List[int] = []
    acl_ids: List[int] = []

    for c in comps:
        if not isinstance(c, dict):
            continue

        lid = c.get("league_id")
        lid_int = _extract_int(lid)
        if lid_int is None:
            continue

        ctype = str(c.get("type") or "").lower()
        cname = str(c.get("name") or "").strip()

        if comp_std == "League":
            if ctype == "league":
                league_ids.append(lid_int)
            continue

        if comp_std == "Cup":
            if ctype == "cup":
                league_ids.append(lid_int)
            continue

        lower_name = cname.lower()
        if ("uefa" in lower_name) or ("champions league" in lower_name and "afc" not in lower_name):
            uefa_ids.append(lid_int)
        if ("afc" in lower_name) and ("champions league" in lower_name):
            acl_ids.append(lid_int)

        # 기타 문자열: name 정확 매칭
        if comp_std not in ("UEFA", "ACL") and cname == comp_std:
            league_ids.append(lid_int)

    def _dedupe(seq: List[int]) -> List[int]:
        seen = set()
        out: List[int] = []
        for v in seq:
            if v in seen:
                continue
            seen.add(v)
            out.append(v)
        return out

    if comp_std == "UEFA":
        if uefa_ids:
            return _dedupe(uefa_ids)
        return [league_id]

    if comp_std == "ACL":
        if acl_ids:
            return _dedupe(acl_ids)
        return [league_id]

    if league_ids:
        return _dedupe(league_ids)

    return [league_id]


# ─────────────────────────────────────
#  팀 인사이트 전체 블록 구성
# ─────────────────────────────────────
def build_team_insights_overall_block(
    *,
    league_id: int,
    season: Any,
    team_id: int,
    comp: Any = "All",
    last_n: Any = 0,
) -> Dict[str, Any]:
    """
    기존 matchdetail 인사이트 overall 블록 빌더.
    - 내부에서 comp/last_n 필터를 해석해서
      stats/insights를 구성 후 반환
    """
    season_int = _parse_season(season)
    last_n_int = _parse_last_n(last_n)
    comp_std = _normalize_comp(comp)

    # competition_detail 로 league_id 집합 구성
    competition_detail = None
    try:
        competition_detail = fetch_all(
            """
            SELECT competition_detail
            FROM leagues
            WHERE id = %(league_id)s
            """,
            {"league_id": league_id},
        )
        if competition_detail:
            competition_detail = competition_detail[0].get("competition_detail")
        if not isinstance(competition_detail, dict):
            competition_detail = None
    except Exception:
        competition_detail = None

    target_league_ids = _build_comp_league_ids(
        comp_std=comp_std,
        competition_detail=competition_detail,
        league_id=league_id,
    )

    insights_filters = {
        "comp": comp_std,
        "target_league_ids_last_n": target_league_ids,
        "last_n": last_n_int,
    }

    # 기존 로직대로 stats/insights 만들고 enrich 함수들 호출
    stats: Dict[str, Any] = {}
    insights: Dict[str, Any] = {
        "insights_filters": insights_filters
    }

    # matches_total_api는 기존 stats에 따라 다를 수 있으니, 없으면 0
    matches_total_api = int(stats.get("matches_total_api") or 0)

    enrich_overall_outcome_totals(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=matches_total_api,
        last_n=last_n_int,
    )

    enrich_overall_goals_by_time(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n_int,
    )

    return insights


# ─────────────────────────────────────
#  (기존) matchdetail response wrapper
# ─────────────────────────────────────
def build_team_insights_overall_response(
    *,
    league_id: int,
    season: Any,
    team_id: int,
    comp: Any = "All",
    last_n: Any = 0,
) -> Dict[str, Any]:
    header = build_team_insights_overall_block(
        league_id=league_id,
        season=season,
        team_id=team_id,
        comp=comp,
        last_n=last_n,
    )

    # 기존 출력 포맷 유지
    return {
        "ok": True,
        "league_id": league_id,
        "season": season,
        "team_id": team_id,
        "comp": comp,
        "last_n": last_n,
        "header": header,
    }


def build_team_insights_overall_header_only(
    *,
    league_id: int,
    season: Any,
    team_id: int,
    comp: Any = "All",
    last_n: Any = 0,
) -> Dict[str, Any]:
    header = build_team_insights_overall_block(
        league_id=league_id,
        season=season,
        team_id=team_id,
        comp=comp,
        last_n=last_n,
    )

    # 기존 matchdetail에서 header만 쓰는 경우
    home_block = header.get("home") or {}
    away_block = header.get("away") or {}

    return {
        "ok": True,
        "league_id": league_id,
        "season": season,
        "team_id": team_id,
        "comp": comp,
        "last_n": last_n,
        "home": home_block,
        "away": away_block,
    }
