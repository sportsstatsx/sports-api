# services/insights/insights_overall_outcome_totals.py
from __future__ import annotations

from typing import Any, Dict, Optional

from db import fetch_all
from .utils import fmt_pct, fmt_avg


def enrich_overall_outcome_totals(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    matches_total_api: int = 0,
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
    """
    if season_int is None:
        return

    match_rows = fetch_all(
        """
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            m.home_ft,
            m.away_ft,
            m.status_group
        FROM matches m
        WHERE m.league_id = %s
          AND m.season    = %s
          AND (m.home_id = %s OR m.away_id = %s)
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
        """,
        (league_id, season_int, team_id, team_id),
    )

    if not match_rows:
        return

    mt_tot = 0
    mh_tot = 0
    ma_tot = 0

    win_t = win_h = win_a = 0
    draw_t = draw_h = draw_a = 0
    lose_t = lose_h = lose_a = 0

    btts_t = btts_h = btts_a = 0
    team_o05_t = team_o05_h = team_o05_a = 0
    team_o15_t = team_o15_h = team_o15_a = 0
    o15_t = o15_h = o15_a = 0
    o25_t = o25_h = o25_a = 0

    win_o25_t = win_o25_h = win_o25_a = 0
    lose_btts_t = lose_btts_h = lose_btts_a = 0

    cs_t = cs_h = cs_a = 0
    ng_t = ng_h = ng_a = 0

    gf_sum_t = gf_sum_h = gf_sum_a = 0
    ga_sum_t = ga_sum_h = ga_sum_a = 0

    for mr in match_rows:
        home_id = mr["home_id"]
        away_id = mr["away_id"]
        home_ft = mr["home_ft"]
        away_ft = mr["away_ft"]

        if home_ft is None or away_ft is None:
            continue

        is_home = (team_id == home_id)
        gf = home_ft if is_home else away_ft
        ga = away_ft if is_home else home_ft
        total_goals = (gf or 0) + (ga or 0)

        mt_tot += 1
        gf_sum_t += gf
        ga_sum_t += ga

        if is_home:
            mh_tot += 1
            gf_sum_h += gf
            ga_sum_h += ga
        else:
            ma_tot += 1
            gf_sum_a += gf
            ga_sum_a += ga

        # W/D/L
        if gf > ga:
            win_t += 1
            if is_home:
                win_h += 1
            else:
                win_a += 1
        elif gf == ga:
            draw_t += 1
            if is_home:
                draw_h += 1
            else:
                draw_a += 1
        else:
            lose_t += 1
            if is_home:
                lose_h += 1
            else:
                lose_a += 1

        # BTTS / Team Over / Totals
        is_btts = (gf > 0 and ga > 0)
        if is_btts:
            btts_t += 1
            if is_home:
                btts_h += 1
            else:
                btts_a += 1

        if gf >= 1:
            team_o05_t += 1
            if is_home:
                team_o05_h += 1
            else:
                team_o05_a += 1
        if gf >= 2:
            team_o15_t += 1
            if is_home:
                team_o15_h += 1
            else:
                team_o15_a += 1

        if total_goals >= 2:
            o15_t += 1
            if is_home:
                o15_h += 1
            else:
                o15_a += 1
        if total_goals >= 3:
            o25_t += 1
            if is_home:
                o25_h += 1
            else:
                o25_a += 1

        # Clean sheet / No goals
        if ga == 0:
            cs_t += 1
            if is_home:
                cs_h += 1
            else:
                cs_a += 1
        if gf == 0:
            ng_t += 1
            if is_home:
                ng_h += 1
            else:
                ng_a += 1

        # Combos
        if gf > ga and total_goals >= 3:
            win_o25_t += 1
            if is_home:
                win_o25_h += 1
            else:
                win_o25_a += 1

        if gf < ga and is_btts:
            lose_btts_t += 1
            if is_home:
                lose_btts_h += 1
            else:
                lose_btts_a += 1

    if mt_tot == 0:
        return

    # 경기 수: API값이 있으면 우선, 없으면 실제 경기 수
    eff_tot = matches_total_api or mt_tot
    eff_home = mh_tot or eff_tot
    eff_away = ma_tot or eff_tot

    # 승률 등
    insights["win_pct"] = {
        "total": fmt_pct(win_t, eff_tot),
        "home": fmt_pct(win_h, eff_home),
        "away": fmt_pct(win_a, eff_away),
    }
    insights["btts_pct"] = {
        "total": fmt_pct(btts_t, eff_tot),
        "home": fmt_pct(btts_h, eff_home),
        "away": fmt_pct(btts_a, eff_away),
    }
    insights["team_over05_pct"] = {
        "total": fmt_pct(team_o05_t, eff_tot),
        "home": fmt_pct(team_o05_h, eff_home),
        "away": fmt_pct(team_o05_a, eff_away),
    }
    insights["team_over15_pct"] = {
        "total": fmt_pct(team_o15_t, eff_tot),
        "home": fmt_pct(team_o15_h, eff_home),
        "away": fmt_pct(team_o15_a, eff_away),
    }
    insights["over15_pct"] = {
        "total": fmt_pct(o15_t, eff_tot),
        "home": fmt_pct(o15_h, eff_home),
        "away": fmt_pct(o15_a, eff_away),
    }
    insights["over25_pct"] = {
        "total": fmt_pct(o25_t, eff_tot),
        "home": fmt_pct(o25_h, eff_home),
        "away": fmt_pct(o25_a, eff_away),
    }

    # 골 득실 평균
    insights["goal_diff_avg"] = {
        "total": fmt_avg(gf_sum_t - ga_sum_t, eff_tot),
        "home": fmt_avg(gf_sum_h - ga_sum_h, eff_home),
        "away": fmt_avg(gf_sum_a - ga_sum_a, eff_away),
    }

    # 클린시트 / 무득점
    insights["clean_sheet_pct"] = {
        "total": fmt_pct(cs_t, eff_tot),
        "home": fmt_pct(cs_h, eff_home),
        "away": fmt_pct(cs_a, eff_away),
    }
    insights["no_goals_pct"] = {
        "total": fmt_pct(ng_t, eff_tot),
        "home": fmt_pct(ng_h, eff_home),
        "away": fmt_pct(ng_a, eff_away),
    }

    # 콤보
    insights["win_and_over25_pct"] = {
        "total": fmt_pct(win_o25_t, eff_tot),
        "home": fmt_pct(win_o25_h, eff_home),
        "away": fmt_pct(win_o25_a, eff_away),
    }
    insights["lose_and_btts_pct"] = {
        "total": fmt_pct(lose_btts_t, eff_tot),
        "home": fmt_pct(lose_btts_h, eff_home),
        "away": fmt_pct(lose_btts_a, eff_away),
    }
