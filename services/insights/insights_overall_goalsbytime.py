# services/insights/insights_overall_goalsbytime.py
from __future__ import annotations

from typing import Any, Dict, Optional

from db import fetch_all


def _bucket_index(minute: int) -> int:
    """
    0-9, 10-19, 20-29, 30-39, 40-44, 45-49, 50-59, 60-69, 70-79, 80+ (총 10버킷)
    기존 Goals by Time UI에 맞춰 10개 구간으로 나눈다고 가정.
    """
    if minute < 0:
        minute = 0
    if minute < 10:
        return 0
    if minute < 20:
        return 1
    if minute < 30:
        return 2
    if minute < 40:
        return 3
    if minute < 45:
        return 4
    if minute < 50:
        return 5
    if minute < 60:
        return 6
    if minute < 70:
        return 7
    if minute < 80:
        return 8
    return 9


def enrich_overall_goals_by_time(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
) -> None:
    """
    Insights Overall - Goals by Time (For / Against).
    - goals_by_time_for: [10개 버킷]
    - goals_by_time_against: [10개 버킷]
    """
    if season_int is None:
        return

    goal_rows = fetch_all(
        """
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            e.minute,
            e.team_id
        FROM matches m
        JOIN match_events e
          ON e.fixture_id = m.fixture_id
        WHERE m.league_id = %s
          AND m.season    = %s
          AND (m.home_id = %s OR m.away_id = %s)
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
          AND e.type = 'Goal'
          AND e.minute IS NOT NULL
        """,
        (league_id, season_int, team_id, team_id),
    )

    if not goal_rows:
        return

    for_buckets = [0] * 10
    against_buckets = [0] * 10

    for gr in goal_rows:
        minute = gr["minute"]
        if minute is None:
            continue

        try:
            m_val = int(minute)
        except (TypeError, ValueError):
            continue

        idx = _bucket_index(m_val)
        is_for = (gr["team_id"] == team_id)
        if is_for:
            for_buckets[idx] += 1
        else:
            against_buckets[idx] += 1

    insights["goals_by_time_for"] = for_buckets
    insights["goals_by_time_against"] = against_buckets
