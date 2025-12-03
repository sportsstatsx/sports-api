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
    last_n: Optional[int] = None,  # Last N (없으면 시즌 전체)
) -> None:
    """
    Goals by Time 섹션.

    🔹 기본 아이디어
      1) matches 테이블에서 Competition + Last N 기준으로
         이 팀이 뛴 경기들의 fixture_id 목록을 먼저 뽑는다.
      2) 그 fixture_id 들에 속한 goal 이벤트만 모아서
         10개 버킷(0~9,10~19,...,80~90+)에 득점/실점을 카운트한다.

    🔹 Competition + Last N 규칙
      - 시즌 전체(last_n == None 또는 0)일 때는 항상 league_id 한 개만 사용.
      - last_n > 0 이고 stats.insights_filters.target_league_ids_last_n 가 있으면
        그 ID 리스트를 IN (...) 으로 사용해서
        리그 / 국내컵 / 대륙컵을 함께 집계한다.
    """
    if season_int is None:
        return

        # Competition / Last N에 따른 league_id 집합 결정
    filters = stats.get("insights_filters") if isinstance(stats, dict) else None
    target_ids = None
    if isinstance(filters, dict):
        target_ids = filters.get("target_league_ids_last_n")

    league_ids_for_query: List[int] = []
    if isinstance(target_ids, list):
        for v in target_ids:
            try:
                league_ids_for_query.append(int(v))
            except (TypeError, ValueError):
                continue

    # target이 비어있으면 현재 리그 한 개로 폴백
    if not league_ids_for_query:
        league_ids_for_query = [league_id]


    # ─────────────────────────────────────
    # 1) Competition + Last N 기준으로 이 팀의 경기 목록(fixture_id) 뽑기
    # ─────────────────────────────────────
    placeholders = ",".join(["%s"] * len(league_ids_for_query))

    matches_sql = f"""
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            m.date_utc
        FROM matches m
        WHERE m.league_id IN ({placeholders})
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
        ORDER BY m.date_utc DESC
    """

    match_params: List[Any] = [*league_ids_for_query, season_int, team_id, team_id]

    # last_n > 0 이면 최근 N경기만 사용
    if last_n is not None and last_n > 0:
        matches_sql += "\n        LIMIT %s"
        match_params.append(last_n)

    match_rows = fetch_all(matches_sql, tuple(match_params))
    if not match_rows:
        return

    fixture_ids: List[int] = []
    for mr in match_rows:
        fid = mr.get("fixture_id")
        if fid is None:
            continue
        try:
            fixture_ids.append(int(fid))
        except (TypeError, ValueError):
            continue

    if not fixture_ids:
        return

    # ─────────────────────────────────────
    # 2) 위에서 뽑은 fixture_id 들에 대해 골 이벤트만 로드
    # ─────────────────────────────────────
    fi_placeholders = ",".join(["%s"] * len(fixture_ids))

    goals_sql = f"""
        SELECT
            e.fixture_id,
            e.minute,
            e.team_id,
            e.detail
        FROM match_events e
        WHERE e.fixture_id IN ({fi_placeholders})
          AND lower(e.type) = 'goal'
          AND e.minute IS NOT NULL
    """


    goal_rows = fetch_all(goals_sql, tuple(fixture_ids))
    if not goal_rows:
        # 경기 자체는 있지만 골이 하나도 없는 경우
        insights["goals_by_time_for"] = [0] * 10
        insights["goals_by_time_against"] = [0] * 10
        return

    # ─────────────────────────────────────
    # 3) 10 구간 버킷 (0~9, 10~19, ..., 80~90+)
    # ─────────────────────────────────────
    for_buckets = [0] * 10
    against_buckets = [0] * 10

    def bucket_index(minute: int) -> int:
        if minute < 10:
            return 0          # 0–9
        elif minute < 20:
            return 1          # 10–19
        elif minute < 30:
            return 2          # 20–29
        elif minute < 40:
            return 3          # 30–39
        elif minute <= 45:
            # ✅ 40–45+ (전반 종료 + 추가시간까지 전부 전반으로)
            return 4
        elif minute < 50:
            # 46–49 (실제 후반 시작 이후)
            return 5
        elif minute < 60:
            return 6
        elif minute < 70:
            return 7
        elif minute < 80:
            return 8
        else:
            # 80–90+
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

        raw_team_id = gr.get("team_id")
        try:
            ev_team_id = int(raw_team_id) if raw_team_id is not None else None
        except (TypeError, ValueError):
            continue

        # 자책골 여부: detail 에 "own" + "goal" 이 들어있는지 체크
        detail_str = (gr.get("detail") or "").lower()
        is_own = ("own" in detail_str and "goal" in detail_str)

        if ev_team_id is None:
            continue

        if is_own:
            # 자책골
            if ev_team_id == team_id:
                # 우리가 자책골 → 실점
                is_for = False
                is_against = True
            else:
                # 상대가 자책골 → 득점
                is_for = True
                is_against = False
        else:
            # 일반 골 / PK 골
            is_for = (ev_team_id == team_id)
            is_against = not is_for

        if is_for:
            for_buckets[idx] += 1
        elif is_against:
            against_buckets[idx] += 1


    insights["goals_by_time_for"] = for_buckets
    insights["goals_by_time_against"] = against_buckets
