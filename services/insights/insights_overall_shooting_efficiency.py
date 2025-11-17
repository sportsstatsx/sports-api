from typing import Any, Dict, Optional, List

from db import fetch_all
from .utils import fmt_avg
from .filters_lastn import build_fixture_filter_clause


def enrich_overall_shooting_efficiency(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: int,
    team_id: int,
    matches_total_api: int,
    fixture_ids: Optional[List[int]] = None,
) -> None:
    """
    슈팅 / 유효슈팅 기반의 효율 지표.
    - fixture_ids 가 주어지면 해당 경기들만 대상으로 계산 (Last N 필터)
    """

    extra_where, id_params = build_fixture_filter_clause(fixture_ids)

    shot_rows = fetch_all(
        f"""
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
         AND mts.team_id  = %s          -- 우리 팀만
        WHERE m.league_id = %s
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
          {extra_where}
        GROUP BY m.fixture_id, m.home_id, m.away_id
        """,
        (team_id, league_id, season_int, team_id, team_id, *id_params),
    )

    if not shot_rows:
        return

    # 2) 전체 / 홈 / 원정 집계
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

    eff_total = total_matches
    eff_home = home_matches or eff_total
    eff_away = away_matches or eff_total

    # shots 블록
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
    avg_home = fmt_avg(total_shots_home, eff_home)
    avg_away = fmt_avg(total_shots_away, eff_away)

    insights["shots_per_match"] = {
        "total": avg_total,
        "home": avg_home,
        "away": avg_away,
    }
