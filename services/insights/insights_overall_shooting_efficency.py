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


def insights_overall_shooting_efficency(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    team_id: int,
    league_id: int,
    season_int: Optional[int],
    matches_total_api: int,
) -> None:
    """
    기존 home_service.get_team_season_stats 안에 있던
    'Shooting & Efficiency (Shots)' 블록을 그대로 분리한 함수.

    - stats: team_season_stats.value 전체 JSON(dict)
    - insights: stats["insights_overall"]
    - matches_total_api: fixtures.played.total (없으면 0)
    """

    if season_int is None:
        return

    shot_rows: List[Dict[str, Any]] = fetch_all(
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
                        'shotsongoal',
                        'shots on target'
                    )
                    AND mts.value ~ '^[0-9]+$'
                    THEN mts.value::int
                    ELSE 0
                END
            ) AS shots_on_goal
        FROM matches m
        LEFT JOIN match_team_stats mts
          ON mts.fixture_id = m.fixture_id
         AND mts.team_id   = %s
        WHERE m.league_id = %s
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
        GROUP BY m.fixture_id, m.home_id, m.away_id
        """,
        (team_id, league_id, season_int, team_id, team_id),
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

    for r2 in shot_rows:
        ts = r2["total_shots"] or 0
        sog = r2["shots_on_goal"] or 0

        is_home = (r2["home_id"] == team_id)
        is_away = (r2["away_id"] == team_id)
        if not (is_home or is_away):
            continue

        total_matches += 1
        total_shots_total += ts
        sog_total += sog

        if is_home:
            home_matches += 1
            total_shots_home += ts
            sog_home += sog
        else:
            away_matches += 1
            total_shots_away += ts
            sog_away += sog

    eff_total = matches_total_api or total_matches or 0
    eff_home = home_matches or 0
    eff_away = away_matches or 0

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

    avg_total = _fmt_avg(total_shots_total, eff_total) if eff_total > 0 else 0.0
    avg_home = _fmt_avg(total_shots_home, eff_home) if eff_home > 0 else 0.0
    avg_away = _fmt_avg(total_shots_away, eff_away) if eff_away > 0 else 0.0

    insights["shots_per_match"] = {
        "total": avg_total,
        "home": avg_home,
        "away": avg_away,
    }
    insights["shots_on_target_pct"] = {
        "total": _fmt_pct(sog_total, total_shots_total),
        "home": _fmt_pct(sog_home, total_shots_home),
        "away": _fmt_pct(sog_away, total_shots_away),
    }
