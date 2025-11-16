# services/insights/insights_overall_outcome_totals.py
from __future__ import annotations

from typing import Any, Dict, Optional

from db import fetch_all
from .utils import fmt_pct, fmt_avg


def enrich_overall_outcome_and_combos(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
) -> None:
    """
    Outcome & Totals + Result Combos & Draw 에 필요한 지표들을
    stats["insights_overall"] 에 채워 넣는다.

    - win_pct / draw_pct / lose_and_btts_pct ...
    - over15_pct / over25_pct / clean_sheet_pct / no_goals_pct
    - goal_diff_avg 등
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
          AND (%s = m.home_id OR %s = m.away_id)
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
        """,
        (league_id, season_int, team_id, team_id),
    )

    mt_tot = mh_tot = ma_tot = 0

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

    gf_sum_t = gf_sum_h = gf_sum_a = 0.0
    ga_sum_t = ga_sum_h = ga_sum_a = 0.0

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

        if gf is None or ga is None:
            continue

        mt_tot += 1
        if is_home:
            mh_tot += 1
        else:
            ma_tot += 1

        # 승/무/패
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

        # 득점/실점 누적
        gf_sum_t += gf
        ga_sum_t += ga
        if is_home:
            gf_sum_h += gf
            ga_sum_h += ga
        else:
            gf_sum_a += gf
            ga_sum_a += ga

        # BTTS
        if gf > 0 and ga > 0:
            btts_t += 1
            if is_home:
                btts_h += 1
            else:
                btts_a += 1

        # 팀 오버 0.5 / 1.5
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

        total_goals = gf + ga

        # 경기 오버 1.5 / 2.5
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

        # Win & Over 2.5
        if gf > ga and total_goals >= 3:
            win_o25_t += 1
            if is_home:
                win_o25_h += 1
            else:
                win_o25_a += 1

        # Lose & BTTS
        if gf < ga and gf > 0 and ga > 0:
            lose_btts_t += 1
            if is_home:
                lose_btts_h += 1
            else:
                lose_btts_a += 1

        # 클린시트 / 노골
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

    if mt_tot <= 0:
        return

    # 일부 팀에서 home/away 경기가 0일 수 있어서, 0 나누기 방지용
    home_base = mh_tot or mt_tot
    away_base = ma_tot or mt_tot

    # ─ Outcome & Totals ─
    insights.setdefault(
        "win_pct",
        {
            "total": fmt_pct(win_t, mt_tot),
            "home": fmt_pct(win_h, home_base),
            "away": fmt_pct(win_a, away_base),
        },
    )
    insights.setdefault(
        "btts_pct",
        {
            "total": fmt_pct(btts_t, mt_tot),
            "home": fmt_pct(btts_h, home_base),
            "away": fmt_pct(btts_a, away_base),
        },
    )
    insights.setdefault(
        "team_over05_pct",
        {
            "total": fmt_pct(team_o05_t, mt_tot),
            "home": fmt_pct(team_o05_h, home_base),
            "away": fmt_pct(team_o05_a, away_base),
        },
    )
    insights.setdefault(
        "team_over15_pct",
        {
            "total": fmt_pct(team_o15_t, mt_tot),
            "home": fmt_pct(team_o15_h, home_base),
            "away": fmt_pct(team_o15_a, away_base),
        },
    )
    insights.setdefault(
        "over15_pct",
        {
            "total": fmt_pct(o15_t, mt_tot),
            "home": fmt_pct(o15_h, home_base),
            "away": fmt_pct(o15_a, away_base),
        },
    )
    insights.setdefault(
        "over25_pct",
        {
            "total": fmt_pct(o25_t, mt_tot),
            "home": fmt_pct(o25_h, home_base),
            "away": fmt_pct(o25_a, away_base),
        },
    )
    insights.setdefault(
        "clean_sheet_pct",
        {
            "total": fmt_pct(cs_t, mt_tot),
            "home": fmt_pct(cs_h, home_base),
            "away": fmt_pct(cs_a, away_base),
        },
    )
    insights.setdefault(
        "no_goals_pct",
        {
            "total": fmt_pct(ng_t, mt_tot),
            "home": fmt_pct(ng_h, home_base),
            "away": fmt_pct(ng_a, away_base),
        },
    )

    # ─ Result Combos & Draw ─
    insights.setdefault(
        "win_and_over25_pct",
        {
            "total": fmt_pct(win_o25_t, mt_tot),
            "home": fmt_pct(win_o25_h, home_base),
            "away": fmt_pct(win_o25_a, away_base),
        },
    )
    insights.setdefault(
        "lose_and_btts_pct",
        {
            "total": fmt_pct(lose_btts_t, mt_tot),
            "home": fmt_pct(lose_btts_h, home_base),
            "away": fmt_pct(lose_btts_a, away_base),
        },
    )
    insights.setdefault(
        "draw_pct",
        {
            "total": fmt_pct(draw_t, mt_tot),
            "home": fmt_pct(draw_h, home_base),
            "away": fmt_pct(draw_a, away_base),
        },
    )
    insights.setdefault(
        "goal_diff_avg",
        {
            "total": fmt_avg(gf_sum_t - ga_sum_t, mt_tot),
            "home": fmt_avg(gf_sum_h - ga_sum_h, home_base),
            "away": fmt_avg(gf_sum_a - ga_sum_a, away_base),
        },
    )
