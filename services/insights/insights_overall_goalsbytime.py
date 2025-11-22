# services/insights/insights_overall_goalsbytime.py
from __future__ import annotations

from typing import Any, Dict, Optional

from db import fetch_all


def enrich_overall_goals_by_time(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    last_n: Optional[int] = None,  # 🔹 추가: Last N (없으면 시즌 전체)
) -> None:
    """
    Goals by Time 섹션.

    기존 home_service.py 에서 잘 동작하던
    - goals_by_time_for
    - goals_by_time_against
    계산 로직을 그대로 모듈로 분리한 버전.
    """
    if season_int is None:
        return

    # ─────────────────────────────────────
    # 1) 골 이벤트 로딩 (시즌 전체 or 최근 N경기)
    # ─────────────────────────────────────
    base_sql = """
        SELECT
            e.fixture_id,
            e.minute,
            e.team_id,
            m.home_id,
            m.away_id
        FROM matches m
        JOIN match_events e
          ON e.fixture_id = m.fixture_id
        WHERE m.league_id = %s
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND lower(e.type) = 'goal'
          AND e.minute IS NOT NULL
    """

    params = [league_id, season_int, team_id, team_id]

    # 🔹 last_n > 0 이면, 이 팀의 "최근 N경기"에 해당하는 fixture_id 들만 사용
    if last_n is not None and last_n > 0:
        base_sql += """
          AND m.fixture_id IN (
              SELECT m2.fixture_id
              FROM matches m2
              WHERE m2.league_id = %s
                AND m2.season    = %s
                AND (%s = m2.home_id OR %s = m2.away_id)
              ORDER BY m2.date_utc DESC
              LIMIT %s
          )
        """
        params.extend([league_id, season_int, team_id, team_id, last_n])

    goal_rows = fetch_all(base_sql, tuple(params))


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
