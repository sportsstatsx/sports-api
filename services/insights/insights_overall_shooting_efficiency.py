# services/insights/insights_overall_shooting_efficiency.py
from __future__ import annotations

from typing import Any, Dict, Optional

from db import fetch_all
from .utils import fmt_pct, fmt_avg


def enrich_overall_shooting_efficiency(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    matches_total_api: int = 0,
) -> None:
    """
    Insights Overall - Shooting & Efficiency 섹션.

    - shots_per_match : 경기당 슈팅 수 (total/home/away)
    - shots_on_target_pct : 유효슈팅 비율 (total/home/away)
    """
    if season_int is None:
        return

    shot_rows = fetch_all(
        """
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            SUM(
                CASE
                    WHEN lower(mts.name) IN ('total shots','shots total','shots')
                         AND mts.value ~ '^[0-9]+$'
                    THEN mts.value::int
                    ELSE 0
                END
            ) AS total_shots,
            SUM(
                CASE
                    WHEN lower(mts.name) IN (
                        'shots on goal',
                        'shots on target',
                        'shots on target (inc woodwork)',
                        'shots on target (inc. woodwork)'
                    )
                    AND mts.value ~ '^[0-9]+$'
                    THEN mts.value::int
                    ELSE 0
                END
            ) AS shots_on_target
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

    if not shot_rows:
        return

    total_matches = 0
    home_matches = 0
    away_matches = 0

    total_shots_total = 0
    total_shots_home = 0
    total_shots_away = 0

    sog_total = 0
    sog_home = 0
    sog_away = 0

    for r in shot_rows:
        home_id = r["home_id"]
        away_id = r["away_id"]
        is_home = (home_id == team_id)
        is_away = (away_id == team_id)
        if not (is_home or is_away):
            continue

        total_shots = r["total_shots"] or 0
        sog = r["shots_on_target"] or 0

        if total_shots <= 0 and sog <= 0:
            continue

        total_matches += 1
        total_shots_total += total_shots
        sog_total += sog

        if is_home:
            home_matches += 1
            total_shots_home += total_shots
            sog_home += sog
        else:
            away_matches += 1
            total_shots_away += total_shots
            sog_away += sog

    if total_matches == 0:
        return

    eff_total = matches_total_api or total_matches or 0
    eff_home = home_matches or 0
    eff_away = away_matches or 0

    if eff_total == 0:
        return

    # shots 블록도 같이 기록 (다른 곳에서 재사용 가능)
    stats["shots"] = {
        "total": {
            "total": int(total_shots_total),
            "home": int(total_shots_home),
            "away": int(total_shots_away),
        },
        "on": {
            "total": int(sog_total),
            "home": int(sog_home),
            "away": int(sog_away),
        },
    }

    avg_total = fmt_avg(total_shots_total, eff_total)
    avg_home = fmt_avg(total_shots_home, eff_home) if eff_home > 0 else 0.0
    avg_away = fmt_avg(total_shots_away, eff_away) if eff_away > 0 else 0.0

    insights["shots_per_match"] = {
        "total": avg_total,
        "home": avg_home,
        "away": avg_away,
    }
    insights["shots_on_target_pct"] = {
        "total": fmt_pct(sog_total, total_shots_total),
        "home": fmt_pct(sog_home, total_shots_home) if total_shots_home > 0 else 0,
        "away": fmt_pct(sog_away, total_shots_away) if total_shots_away > 0 else 0,
    }
