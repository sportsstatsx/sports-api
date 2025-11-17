# services/insights/insights_overall_goalsbytime.py
from __future__ import annotations

from typing import Any, Dict, Optional, List

from db import fetch_all
from .filters_lastn import build_fixture_filter_clause


def enrich_overall_goals_by_time(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: int,
    team_id: int,
    fixture_ids: Optional[List[int]] = None,
) -> None:
    """
    Goals by Time (0–15, 16–30, ..., 80–90) 득점/실점 분포.
    fixture_ids 가 주어지면 해당 경기들만 사용.
    """

    extra_where, id_params = build_fixture_filter_clause(fixture_ids)

    goal_rows = fetch_all(
        f"""
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            e.team_id,
            e.minute,
            e.extra
        FROM matches m
        JOIN match_events e
          ON e.fixture_id = m.fixture_id
        WHERE m.league_id = %s
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND lower(e.type) = 'goal'
          AND e.minute IS NOT NULL
          {extra_where}
        """,
        (league_id, season_int, team_id, team_id, *id_params),
    )

    if not goal_rows:
        return

    # 10 구간 버킷 (0~9, 10~19, ..., 80~90+)
    for_buckets = [0] * 10
    against_buckets = [0] * 10

    def bucket_index(minute: int) -> int:
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

    for gr in goal_rows:
        minute = gr.get("minute")
        try:
            m_val = int(minute)
        except (TypeError, ValueError):
            continue

        if m_val < 0:
            continue

        idx = bucket_index(m_val)
        is_for = (gr.get("team_id") == team_id)
        if is_for:
            for_buckets[idx] += 1
        else:
            against_buckets[idx] += 1

    insights["goals_by_time_for"] = for_buckets
    insights["goals_by_time_against"] = against_buckets
