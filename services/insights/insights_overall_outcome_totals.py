from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all


def _safe_div(num, den) -> float:
    try:
        num_f = float(num)
    except (TypeError, ValueError):
        return 0.0
    try:
        den_f = float(den)
    except (TypeError, ValueError):
        return 0.0
    if den_f == 0:
        return 0.0
    return num_f / den_f


def _fmt_pct(n, d) -> int:
    v = _safe_div(n, d)
    return int(round(v * 100)) if v > 0 else 0


def _fmt_avg(n, d) -> float:
    v = _safe_div(n, d)
    return round(v, 2) if v > 0 else 0.0


def insights_overall_outcome_totals(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    team_id: int,
    league_id: int,
    season_int: Optional[int],
) -> None:
    """
    기존 home_service.get_team_season_stats 안의
    'Outcome & Totals / Result Combos' 블록 전체를 옮긴 함수.

    여기서 win_pct, btts_pct, over15_pct, over25_pct,
    clean_sheet_pct, no_goals_pct, draw_pct,
    win_and_over25_pct, lose_and_btts_pct, goal_diff_avg 등을
    insights_overall 에 채운다.
    """

    match_rows: List[Dict[str, Any]] = []
    if season_int is not None:
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

    if not match_rows:
        return

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

        gf_sum_t += gf
        ga_sum_t += ga
        if is_home:
            gf_sum_h += gf
            ga_sum_h += ga
        else:
            gf_sum_a += gf
            ga_sum_a += ga

        if gf > 0 and ga > 0:
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

        total_goals = gf + ga
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

        if gf > ga and total_goals >= 3:
            win_o25_t += 1
            if is_home:
                win_o25_h += 1
            else:
                win_o25_a += 1

        if gf < ga and gf > 0 and ga > 0:
            lose_btts_t += 1
            if is_home:
                lose_btts_h += 1
            else:
                lose_btts_a += 1

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

    insights.setdefault(
        "win_pct",
        {
            "total": _fmt_pct(win_t, mt_tot),
            "home": _fmt_pct(win_h, mh_tot or mt_tot),
            "away": _fmt_pct(win_a, ma_tot or mt_tot),
        },
    )
    insights.setdefault(
        "btts_pct",
        {
            "total": _fmt_pct(btts_t, mt_tot),
            "home": _fmt_pct(btts_h, mh_tot or mt_tot),
            "away": _fmt_pct(btts_a, ma_tot or mt_tot),
        },
    )
    insights.setdefault(
        "team_over05_pct",
        {
            "total": _fmt_pct(team_o05_t, mt_tot),
            "home": _fmt_pct(team_o05_h, mh_tot or mt_tot),
            "away": _fmt_pct(team_o05_a, ma_tot or mt_tot),
        },
    )
    insights.setdefault(
        "team_over15_pct",
        {
            "total": _fmt_pct(team_o15_t, mt_tot),
            "home": _fmt_pct(team_o15_h, mh_tot or mt_tot),
            "away": _fmt_pct(team_o15_a, ma_tot or mt_tot),
        },
    )
    insights.setdefault(
        "over15_pct",
        {
            "total": _fmt_pct(o15_t, mt_tot),
            "home": _fmt_pct(o15_h, mh_tot or mt_tot),
            "away": _fmt_pct(o15_a, ma_tot or mt_tot),
        },
    )
    insights.setdefault(
        "over25_pct",
        {
            "total": _fmt_pct(o25_t, mt_tot),
            "home": _fmt_pct(o25_h, mh_tot or mt_tot),
            "away": _fmt_pct(o25_a, ma_tot or mt_tot),
        },
    )
    insights.setdefault(
        "clean_sheet_pct",
        {
            "total": _fmt_pct(cs_t, mt_tot),
            "home": _fmt_pct(cs_h, mh_tot or mt_tot),
            "away": _fmt_pct(cs_a, ma_tot or mt_tot),
        },
    )
    insights.setdefault(
        "no_goals_pct",
        {
            "total": _fmt_pct(ng_t, mt_tot),
            "home": _fmt_pct(ng_h, mh_tot or mt_tot),
            "away": _fmt_pct(ng_a, ma_tot or mt_tot),
        },
    )
    insights.setdefault(
        "win_and_over25_pct",
        {
            "total": _fmt_pct(win_o25_t, mt_tot),
            "home": _fmt_pct(win_o25_h, mh_tot or mt_tot),
            "away": _fmt_pct(win_o25_a, ma_tot or mt_tot),
        },
    )
    insights.setdefault(
        "lose_and_btts_pct",
        {
            "total": _fmt_pct(lose_btts_t, mt_tot),
            "home": _fmt_pct(lose_btts_h, mh_tot or mt_tot),
            "away": _fmt_pct(lose_btts_a, ma_tot or mt_tot),
        },
    )
    insights.setdefault(
        "draw_pct",
        {
            "total": _fmt_pct(draw_t, mt_tot),
            "home": _fmt_pct(draw_h, mh_tot or mt_tot),
            "away": _fmt_pct(draw_a, ma_tot or mt_tot),
        },
    )
    insights.setdefault(
        "goal_diff_avg",
        {
            "total": _fmt_avg(gf_sum_t - ga_sum_t, mt_tot),
            "home": _fmt_avg(gf_sum_h - ga_sum_h, mh_tot or mt_tot),
            "away": _fmt_avg(gf_sum_a - ga_sum_a, ma_tot or mt_tot),
        },
    )
