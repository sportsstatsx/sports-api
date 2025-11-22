# services/insights/insights_overall_goalsbytime.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all


def enrich_overall_goals_by_time(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    last_n: Optional[int] = None,  # 🔹 Last N (없으면 시즌 전체)
) -> None:
    """
    Goals by Time 섹션.

    기존 home_service.py 에서 잘 동작하던
    - goals_by_time_for
    - goals_by_time_against
    계산 로직을 그대로 모듈로 분리한 버전.

    🔹 Competition + Last N 필터 규칙
        - 시즌 전체(last_n 가 None/0)일 때는 항상 league_id 한 개만 사용
        - last_n > 0 이고 stats.insights_filters.target_league_ids_last_n 가 존재하면,
          해당 ID 리스트를 IN (...) 으로 사용해서
          리그 / 컵 / 대륙컵을 함께 집계한다.
    """
    if season_int is None:
        return

    # ─────────────────────────────────────
    # 0) Competition / Last N 에 따른 league_id 집합 결정
    # ─────────────────────────────────────
    league_ids_for_query: List[int]
    filters = stats.get("insights_filters") if isinstance(stats, dict) else None
    target_ids = None
    if filters and isinstance(filters, dict):
        target_ids = filters.get("target_league_ids_last_n")

    if last_n and last_n > 0 and isinstance(target_ids, list):
        league_ids_for_query = []
        for v in target_ids:
            try:
                league_ids_for_query.append(int(v))
            except (TypeError, ValueError):
                # 잘못된 값은 건너뛴다.
                continue
        # 비정상적으로 비어 있으면 안전하게 기본 리그만 사용
        if not league_ids_for_query:
            league_ids_for_query = [league_id]
    else:
        # 시즌 전체 모드 또는 필터 정보 없음 → 기본 리그만
        league_ids_for_query = [league_id]

    # ─────────────────────────────────────
    # 1) 골 이벤트 로딩 (시즌 전체 or 최근 N경기)
    # ─────────────────────────────────────
    placeholders = ",".join(["%s"] * len(league_ids_for_query))

    base_sql = f"""
        SELECT
            e.fixture_id,
            e.minute,
            e.team_id,
            m.home_id,
            m.away_id
        FROM matches m
        JOIN match_events e
          ON e.fixture_id = m.fixture_id
        WHERE m.league_id IN ({placeholders})
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND lower(e.type) = 'goal'
          AND e.minute IS NOT NULL
    """

    # m.league_id IN (...), m.season, home/away 조건
    params: List[Any] = [*league_ids_for_query, season_int, team_id, team_id]

    # 🔹 last_n > 0 이면, 이 팀의 "최근 N경기"에 해당하는 fixture_id 들만 사용
    if last_n is not None and last_n > 0:
        placeholders_sub = ",".join(["%s"] * len(league_ids_for_query))
        base_sql += f"""
          AND m.fixture_id IN (
              SELECT m2.fixture_id
              FROM matches m2
              WHERE m2.league_id IN ({placeholders_sub})
                AND m2.season    = %s
                AND (%s = m2.home_id OR %s = m2.away_id)
              ORDER BY m2.date_utc DESC
              LIMIT %s
          )
        """
        # 서브쿼리용: league_ids_for_query + season_int + home/away + last_n
        params.extend([*league_ids_for_query, season_int, team_id, team_id, last_n])

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
