# services/insights/filters_lastn.py
from typing import List, Optional

from db import fetch_all


def get_last_n_fixture_ids(
    *,
    league_id: int,
    season_int: int,
    team_id: int,
    last_n: Optional[int],
) -> Optional[List[int]]:
    """
    시즌 전체 경기 중에서,
    해당 팀 기준으로 가장 최근(last_n) 경기들의 fixture_id 리스트를 돌려준다.
    - last_n 이 None 이거나 0/음수면 None 리턴 → 필터 없이 전체 시즌 사용
    """
    if not last_n or last_n <= 0:
        return None

    rows = fetch_all(
        """
        SELECT
            m.fixture_id
        FROM matches m
        WHERE m.league_id = %s
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
        ORDER BY m.date_utc DESC, m.fixture_id DESC
        LIMIT %s
        """,
        (league_id, season_int, team_id, team_id, last_n),
    )

    if not rows:
        return None

    seen = set()
    fixture_ids: List[int] = []
    for r in rows:
        fid = r.get("fixture_id")
        if isinstance(fid, int) and fid not in seen:
            seen.add(fid)
            fixture_ids.append(fid)

    return fixture_ids or None


def build_fixture_filter_clause(
    fixture_ids: Optional[List[int]],
) -> tuple[str, list]:
    """
    각 모듈에서 공통으로 쓰는 WHERE 절 + 파라미터 생성 헬퍼.
    - fixture_ids 가 None/빈 리스트면 "" 와 [] 반환 → 추가 필터 없음
    - 있으면 " AND m.fixture_id IN (%s, %s, ...)" 와 id 리스트 반환
    """
    if not fixture_ids:
        return "", []

    ids = [int(x) for x in fixture_ids if isinstance(x, int)]
    if not ids:
        return "", []

    placeholders = ", ".join(["%s"] * len(ids))
    clause = f" AND m.fixture_id IN ({placeholders})"
    return clause, ids
