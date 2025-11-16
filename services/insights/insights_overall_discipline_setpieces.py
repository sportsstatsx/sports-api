# services/insights/insights_overall_discipline_setpieces.py
from __future__ import annotations

from typing import Any, Dict, Optional

from db import fetch_all
from .utils import fmt_avg


def enrich_overall_discipline_setpieces(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    matches_total_api: int = 0,
) -> None:
    """
    Insights Overall - Discipline & Set Pieces.

    - corners_per_match : 경기당 코너킥 (total/home/away)
    - yellow_per_match  : 경기당 옐로카드 (total/home/away)
    - red_per_match     : 경기당 레드카드 (total/home/away)

    (Red 카드 이후 영향 지표는 추후 확장 포인트로 남겨둠)
    """
    if season_int is None:
        return

    disc_rows = fetch_all(
        """
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            SUM(
                CASE
                    WHEN lower(mts.name) LIKE 'corner%%'
                         AND mts.value ~ '^[0-9]+$'
                    THEN mts.value::int
                    ELSE 0
                END
            ) AS corners,
            SUM(
                CASE
                    WHEN lower(mts.name) LIKE 'yellow%%'
                         AND mts.value ~ '^[0-9]+$'
                    THEN mts.value::int
                    ELSE 0
                END
            ) AS yellows,
            SUM(
                CASE
                    WHEN lower(mts.name) LIKE 'red%%'
                         AND mts.value ~ '^[0-9]+$'
                    THEN mts.value::int
                    ELSE 0
                END
            ) AS reds
        FROM matches m
        JOIN match_team_stats mts
          ON m.fixture_id = mts.fixture_id
         AND mts.team_id IN (m.home_id, m.away_id)
        WHERE m.league_id = %s
          AND m.season    = %s
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
        GROUP BY m.fixture_id, m.home_id, m.away_id
        """,
        (league_id, season_int),
    )

    if not disc_rows:
        return

    tot_matches = 0
    home_matches = 0
    away_matches = 0

    sum_corners_t = sum_corners_h = sum_corners_a = 0
    sum_yellows_t = sum_yellows_h = sum_yellows_a = 0
    sum_reds_t = sum_reds_h = sum_reds_a = 0

    for dr in disc_rows:
        home_id = dr["home_id"]
        away_id = dr["away_id"]
        is_home = (home_id == team_id)
        is_away = (away_id == team_id)
        if not (is_home or is_away):
            continue

        corners = dr["corners"] or 0
        yellows = dr["yellows"] or 0
        reds = dr["reds"] or 0

        tot_matches += 1
        sum_corners_t += corners
        sum_yellows_t += yellows
        sum_reds_t += reds

        if is_home:
            home_matches += 1
            sum_corners_h += corners
            sum_yellows_h += yellows
            sum_reds_h += reds
        else:
            away_matches += 1
            sum_corners_a += corners
            sum_yellows_a += yellows
            sum_reds_a += reds

    if tot_matches == 0:
        return

    eff_tot = matches_total_api or tot_matches
    eff_home = home_matches or eff_tot
    eff_away = away_matches or eff_tot

    def avg_for(v_t, v_h, v_a, d_t, d_h, d_a):
        return (
            fmt_avg(v_t, d_t) if d_t > 0 else 0.0,
            fmt_avg(v_h, d_h) if d_h > 0 else 0.0,
            fmt_avg(v_a, d_a) if d_a > 0 else 0.0,
        )

    c_tot, c_h, c_a = avg_for(
        sum_corners_t, sum_corners_h, sum_corners_a, eff_tot, eff_home, eff_away
    )
    y_tot, y_h, y_a = avg_for(
        sum_yellows_t, sum_yellows_h, sum_yellows_a, eff_tot, eff_home, eff_away
    )
    r_tot, r_h, r_a = avg_for(
        sum_reds_t, sum_reds_h, sum_reds_a, eff_tot, eff_home, eff_away
    )

    insights["corners_per_match"] = {
        "total": c_tot,
        "home": c_h,
        "away": c_a,
    }
    insights["yellow_per_match"] = {
        "total": y_tot,
        "home": y_h,
        "away": y_a,
    }
    insights["red_per_match"] = {
        "total": r_tot,
        "home": r_h,
        "away": r_a,
    }
