# services/insights/football_insights_overall.py
#
# ✅ Outcome + GoalsByTime 통합본
#   - enrich_overall_outcome_totals
#   - enrich_overall_goals_by_time
#
from __future__ import annotations

from typing import Any, Dict, Optional, List

from db import fetch_all
from services.insights.utils import fmt_avg, fmt_pct, build_league_ids_for_query

__all__ = [
    "enrich_overall_outcome_totals",
    "enrich_overall_goals_by_time",
]


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
      - team_over05_pct
      - team_over15_pct
      - over15_pct
      - over25_pct
      - goal_diff_avg
      - clean_sheet_pct
      - no_goals_pct
      - win_and_over25_pct
      - lose_and_btts_pct

    matches_total_api:
        API-Football 의 fixtures.played.total 같은 값.
        0 이면 실제 경기 수(mt_tot)를 그대로 사용.

    last_n:
        >0 이면 최근 last_n 경기만 집계 (date_utc 기준 DESC).
        0 이면 시즌 전체 경기 사용.
    """
    if season_int is None:
        return

    # Competition 필터 + Last N 에서 사용할 league_id 집합 결정
    #   - insights_block 쪽에서 이미 comp + last_n 조합에 맞춰
    #     stats["insights_filters"]["target_league_ids_last_n"] 를 세팅해주고 있음
    #   - 여기서는 last_n 이 0(Season 모드) 이더라도
    #     항상 그 target_league_ids_last_n 를 우선 사용
    league_ids_for_query = build_league_ids_for_query(
        stats,
        fallback_league_id=league_id,
    )

    if not league_ids_for_query:
        # 리그 ID 를 전혀 못 구하면 아무 것도 하지 않음
        return

    # ─────────────────────────────────────
    # 1) 샘플 매치 로딩 (시즌 전체 or 최근 N경기)
    # ─────────────────────────────────────
    placeholders = ",".join(["%s"] * len(league_ids_for_query))

    base_sql = f"""
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            m.home_ft,
            m.away_ft,
            m.status_group,
            m.date_utc
        FROM matches m
        WHERE m.league_id IN ({placeholders})
          AND m.season    = %s
          AND (m.home_id = %s OR m.away_id = %s)
          AND lower(m.status_group) IN ('finished','ft','fulltime')
        ORDER BY m.date_utc DESC
    """

    params: list[Any] = [*league_ids_for_query, season_int, team_id, team_id]
    if last_n and last_n > 0:
        base_sql += " LIMIT %s"
        params.append(last_n)

    match_rows = fetch_all(base_sql, tuple(params))

    if not match_rows:
        return

    # 샘플 크기(= 실제 사용한 경기 수)
    mt_tot = len(match_rows)

    # total matches(분모) 보정: API 제공값이 있으면 그걸 우선
    denom_total = matches_total_api if matches_total_api and matches_total_api > 0 else mt_tot
    if denom_total <= 0:
        return

    # ─────────────────────────────────────
    # 2) 승/무/패 + 득점/실점 + BTTS/오버
    # ─────────────────────────────────────
    w = d = l = 0

    team_goals = 0
    opp_goals = 0

    btts = 0
    team_over05 = 0
    team_over15 = 0
    over15 = 0
    over25 = 0

    # 홈/원정 분리
    home_w = home_d = home_l = 0
    away_w = away_d = away_l = 0

    home_team_goals = home_opp_goals = 0
    away_team_goals = away_opp_goals = 0

    home_btts = away_btts = 0
    home_team_over05 = away_team_over05 = 0
    home_team_over15 = away_team_over15 = 0
    home_over15 = away_over15 = 0
    home_over25 = away_over25 = 0

    # clean sheet / no goals
    cs = 0
    ng = 0
    home_cs = away_cs = 0
    home_ng = away_ng = 0

    # win & over 2.5 / lose & btts
    win_and_over25 = 0
    lose_and_btts = 0
    home_win_and_over25 = away_win_and_over25 = 0
    home_lose_and_btts = away_lose_and_btts = 0

    # first goal
    first_goal_for = 0
    first_goal_against = 0
    home_first_goal_for = home_first_goal_against = 0
    away_first_goal_for = away_first_goal_against = 0

    # events sample (이 섹션에서 사용한 경기수)
    insights["events_sample"] = mt_tot

    for r in match_rows:
        hid = r.get("home_id")
        aid = r.get("away_id")
        hft = r.get("home_ft") or 0
        aft = r.get("away_ft") or 0

        is_home = hid == team_id
        is_away = aid == team_id
        if not (is_home or is_away):
            continue

        tg = int(hft) if is_home else int(aft)
        og = int(aft) if is_home else int(hft)

        team_goals += tg
        opp_goals += og

        if is_home:
            home_team_goals += tg
            home_opp_goals += og
        else:
            away_team_goals += tg
            away_opp_goals += og

        # W/D/L
        if tg > og:
            w += 1
            if is_home:
                home_w += 1
            else:
                away_w += 1
        elif tg == og:
            d += 1
            if is_home:
                home_d += 1
            else:
                away_d += 1
        else:
            l += 1
            if is_home:
                home_l += 1
            else:
                away_l += 1

        # BTTS
        if tg > 0 and og > 0:
            btts += 1
            if is_home:
                home_btts += 1
            else:
                away_btts += 1

        # Team over
        if tg >= 1:
            team_over05 += 1
            if is_home:
                home_team_over05 += 1
            else:
                away_team_over05 += 1
        if tg >= 2:
            team_over15 += 1
            if is_home:
                home_team_over15 += 1
            else:
                away_team_over15 += 1

        # Totals over
        tot = tg + og
        if tot >= 2:
            over15 += 1
            if is_home:
                home_over15 += 1
            else:
                away_over15 += 1
        if tot >= 3:
            over25 += 1
            if is_home:
                home_over25 += 1
            else:
                away_over25 += 1

        # Clean sheet / No goals
        if og == 0:
            cs += 1
            if is_home:
                home_cs += 1
            else:
                away_cs += 1
        if tg == 0:
            ng += 1
            if is_home:
                home_ng += 1
            else:
                away_ng += 1

        # win & over 2.5
        if tg > og and (tg + og) >= 3:
            win_and_over25 += 1
            if is_home:
                home_win_and_over25 += 1
            else:
                away_win_and_over25 += 1

        # lose & btts
        if tg < og and (tg > 0 and og > 0):
            lose_and_btts += 1
            if is_home:
                home_lose_and_btts += 1
            else:
                away_lose_and_btts += 1

        # first goal (간단 판정: 0-0 아닌 경기면, 득점한 팀이 first goal로 간주)
        if (hft + aft) > 0:
            if hft > 0 and aft == 0:
                # 홈만 득점
                if is_home:
                    first_goal_for += 1
                    home_first_goal_for += 1
                else:
                    first_goal_against += 1
                    away_first_goal_against += 1
            elif aft > 0 and hft == 0:
                # 원정만 득점
                if is_home:
                    first_goal_against += 1
                    home_first_goal_against += 1
                else:
                    first_goal_for += 1
                    away_first_goal_for += 1
            else:
                # 둘 다 득점: first goal은 match_events 기반이 더 정확하지만,
                # 여기서는 outcome/total 모듈의 기존 정책 유지(집계 제외)
                pass

    # 분모(전체/홈/원정)
    # ※ outcome 모듈은 "실제 샘플(mt_tot)" 기준으로 퍼센트 계산이 자연스럽지만
    #    기존 로직과의 호환을 위해 denom_total도 함께 유지
    home_games = sum(1 for r in match_rows if r.get("home_id") == team_id)
    away_games = sum(1 for r in match_rows if r.get("away_id") == team_id)

    insights["matches_total"] = mt_tot
    insights["matches_total_api"] = matches_total_api

    # 핵심 퍼센트들
    insights["win_pct"] = fmt_pct(w, mt_tot)
    insights["draw_pct"] = fmt_pct(d, mt_tot)
    insights["lose_pct"] = fmt_pct(l, mt_tot)

    insights["home_win_pct"] = fmt_pct(home_w, home_games)
    insights["home_draw_pct"] = fmt_pct(home_d, home_games)
    insights["home_lose_pct"] = fmt_pct(home_l, home_games)

    insights["away_win_pct"] = fmt_pct(away_w, away_games)
    insights["away_draw_pct"] = fmt_pct(away_d, away_games)
    insights["away_lose_pct"] = fmt_pct(away_l, away_games)

    insights["btts_pct"] = fmt_pct(btts, mt_tot)
    insights["home_btts_pct"] = fmt_pct(home_btts, home_games)
    insights["away_btts_pct"] = fmt_pct(away_btts, away_games)

    insights["team_over05_pct"] = fmt_pct(team_over05, mt_tot)
    insights["team_over15_pct"] = fmt_pct(team_over15, mt_tot)
    insights["home_team_over05_pct"] = fmt_pct(home_team_over05, home_games)
    insights["home_team_over15_pct"] = fmt_pct(home_team_over15, home_games)
    insights["away_team_over05_pct"] = fmt_pct(away_team_over05, away_games)
    insights["away_team_over15_pct"] = fmt_pct(away_team_over15, away_games)

    insights["over15_pct"] = fmt_pct(over15, mt_tot)
    insights["over25_pct"] = fmt_pct(over25, mt_tot)
    insights["home_over15_pct"] = fmt_pct(home_over15, home_games)
    insights["home_over25_pct"] = fmt_pct(home_over25, home_games)
    insights["away_over15_pct"] = fmt_pct(away_over15, away_games)
    insights["away_over25_pct"] = fmt_pct(away_over25, away_games)

    # goal diff avg
    goal_diff = team_goals - opp_goals
    home_goal_diff = home_team_goals - home_opp_goals
    away_goal_diff = away_team_goals - away_opp_goals

    insights["goal_diff_avg"] = fmt_avg(goal_diff, mt_tot)
    insights["home_goal_diff_avg"] = fmt_avg(home_goal_diff, home_games)
    insights["away_goal_diff_avg"] = fmt_avg(away_goal_diff, away_games)

    # clean sheet / no goals
    insights["clean_sheet_pct"] = fmt_pct(cs, mt_tot)
    insights["home_clean_sheet_pct"] = fmt_pct(home_cs, home_games)
    insights["away_clean_sheet_pct"] = fmt_pct(away_cs, away_games)

    insights["no_goals_pct"] = fmt_pct(ng, mt_tot)
    insights["home_no_goals_pct"] = fmt_pct(home_ng, home_games)
    insights["away_no_goals_pct"] = fmt_pct(away_ng, away_games)

    # combos
    insights["win_and_over25_pct"] = fmt_pct(win_and_over25, mt_tot)
    insights["home_win_and_over25_pct"] = fmt_pct(home_win_and_over25, home_games)
    insights["away_win_and_over25_pct"] = fmt_pct(away_win_and_over25, away_games)

    insights["lose_and_btts_pct"] = fmt_pct(lose_and_btts, mt_tot)
    insights["home_lose_and_btts_pct"] = fmt_pct(home_lose_and_btts, home_games)
    insights["away_lose_and_btts_pct"] = fmt_pct(away_lose_and_btts, away_games)

    # first goal (간이)
    insights["first_goal_for_pct"] = fmt_pct(first_goal_for, mt_tot)
    insights["first_goal_against_pct"] = fmt_pct(first_goal_against, mt_tot)
    insights["home_first_goal_for_pct"] = fmt_pct(home_first_goal_for, home_games)
    insights["home_first_goal_against_pct"] = fmt_pct(home_first_goal_against, home_games)
    insights["away_first_goal_for_pct"] = fmt_pct(away_first_goal_for, away_games)
    insights["away_first_goal_against_pct"] = fmt_pct(away_first_goal_against, away_games)


# ─────────────────────────────────────
#  Goals by Time
# ─────────────────────────────────────

def enrich_overall_goals_by_time(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    last_n: int = 0,
) -> None:
    if season_int is None:
        return

    # competition / last_n 필터에서 만들어둔 league ids 사용 (없으면 league_id로 폴백)
    filters = stats.get("insights_filters") if isinstance(stats, dict) else None
    target_ids = None
    if isinstance(filters, dict):
        target_ids = filters.get("target_league_ids_last_n")

    league_ids_for_query: List[int] = []
    if isinstance(target_ids, list):
        for v in target_ids:
            try:
                league_ids_for_query.append(int(v))
            except (TypeError, ValueError):
                continue

    if not league_ids_for_query:
        league_ids_for_query = [league_id]

    placeholders = ",".join(["%s"] * len(league_ids_for_query))

    # goals by time (우리팀 득점/실점: FT 스코어 + match_events의 minute 기반 집계)
    # 여기서는 match_events에서 goal/minute를 사용.
    # finished 경기만, last_n 있으면 최근 N경기만.
    base_match_sql = f"""
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            m.date_utc
        FROM matches m
        WHERE m.league_id IN ({placeholders})
          AND m.season    = %s
          AND (m.home_id = %s OR m.away_id = %s)
          AND lower(m.status_group) IN ('finished','ft','fulltime')
        ORDER BY m.date_utc DESC
    """
    params: List[Any] = [*league_ids_for_query, season_int, team_id, team_id]
    if last_n and last_n > 0:
        base_match_sql += " LIMIT %s"
        params.append(last_n)

    match_rows = fetch_all(base_match_sql, tuple(params))
    if not match_rows:
        return

    fixture_ids: List[int] = []
    for r in match_rows:
        fid = r.get("fixture_id")
        if fid is None:
            continue
        try:
            fixture_ids.append(int(fid))
        except (TypeError, ValueError):
            continue

    if not fixture_ids:
        return

    # bucket 정의 (0-15,16-30,31-45,46-60,61-75,76-90,90+)
    buckets = [
        ("0_15", 0, 15),
        ("16_30", 16, 30),
        ("31_45", 31, 45),
        ("46_60", 46, 60),
        ("61_75", 61, 75),
        ("76_90", 76, 90),
        ("90_plus", 91, 10**9),
    ]

    # init
    for_name: Dict[str, int] = {k: 0 for (k, _, _) in buckets}
    ag_name: Dict[str, int] = {k: 0 for (k, _, _) in buckets}

    # events 쿼리
    ev_placeholders = ",".join(["%s"] * len(fixture_ids))
    ev_sql = f"""
        SELECT
            me.fixture_id,
            me.team_id,
            me.type,
            me.minute
        FROM match_events me
        WHERE me.fixture_id IN ({ev_placeholders})
          AND lower(me.type) = 'goal'
    """
    ev_rows = fetch_all(ev_sql, tuple(fixture_ids))
    if not ev_rows:
        # events가 없으면 0으로 내려주되 sample은 matches 기준으로 남겨둠
        insights["goals_by_time_sample"] = len(match_rows)
        for k, _, _ in buckets:
            insights[f"gbytime_for_{k}"] = 0
            insights[f"gbytime_against_{k}"] = 0
        return

    def _bucket_key(minute_val: Any) -> Optional[str]:
        try:
            m = int(minute_val)
        except Exception:
            return None
        for k, lo, hi in buckets:
            if lo <= m <= hi:
                return k
        return None

    for ev in ev_rows:
        tid = ev.get("team_id")
        key = _bucket_key(ev.get("minute"))
        if key is None:
            continue

        if tid == team_id:
            for_name[key] += 1
        else:
            ag_name[key] += 1

    insights["goals_by_time_sample"] = len(match_rows)
    for k, _, _ in buckets:
        insights[f"gbytime_for_{k}"] = int(for_name.get(k, 0))
        insights[f"gbytime_against_{k}"] = int(ag_name.get(k, 0))
