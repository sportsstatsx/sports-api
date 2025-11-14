# services/home_service.py

import os
from typing import Any, Dict, List, Optional, Sequence, Mapping

from db import fetch_all, fetch_one

# ─────────────────────────────────────────
# 홈 탭에서 사용할 리그 목록 설정
# ─────────────────────────────────────────
#
# HOME_LEAGUES 가 있으면 우선 사용하고,
# 없으면 LIVE_LEAGUES 를 fallback 으로 사용.
#
# 예:
#   HOME_LEAGUES="39,40,140,141"
#   LIVE_LEAGUES="39,40,140,141,78,79,..."
# ─────────────────────────────────────────

_RAW_HOME_LEAGUES = (
    os.getenv("HOME_LEAGUES")
    or os.getenv("home-leagues")
    or os.getenv("LIVE_LEAGUES")
    or os.getenv("live-league")
    or ""
)


def _parse_ids(env_val: str) -> List[int]:
    ids: List[int] = []
    for part in env_val.replace(" ", "").split(","):
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            continue
    return ids


HOME_LEAGUE_IDS: List[int] = _parse_ids(_RAW_HOME_LEAGUES)


def _has_home_filter() -> bool:
    return len(HOME_LEAGUE_IDS) > 0


# ─────────────────────────────────────────
# 1) 홈 상단: 날짜별 리그별 경기 수 목록
#    /api/home/leagues  에서 사용
# ─────────────────────────────────────────

def get_home_leagues_for_date(date_str: str) -> List[Dict[str, Any]]:
    """
    주어진 날짜(YYYY-MM-DD)에 대해,
    홈 탭 상단에 보여줄 "리그별 매치 수" 리스트를 반환.

    반환 예시:
    [
      {
        "league_id": 39,
        "league_name": "Premier League",
        "country": "England",
        "match_count": 8
      },
      ...
    ]
    """
    params: List[Any] = [date_str]
    where_extra = ""

    if _has_home_filter():
        # 특정 리그만 보여주고 싶을 때 필터
        where_extra = " AND m.league_id = ANY(%s)"
        params.append(HOME_LEAGUE_IDS)

    rows = fetch_all(
        f"""
        SELECT
            m.league_id,
            l.name   AS league_name,
            COALESCE(l.country, '') AS country,
            COUNT(*) AS match_count
        FROM matches m
        JOIN leagues l
          ON l.id = m.league_id
        WHERE SUBSTRING(m.date_utc FROM 1 FOR 10) = %s
        {where_extra}
        GROUP BY m.league_id, l.name, l.country
        ORDER BY country, league_name
        """,
        params,
    )

    return rows


# ─────────────────────────────────────────
# 2) 홈 하단: 국가별 리그 디렉터리
#    /api/home/league_directory  에서 사용
# ─────────────────────────────────────────

def get_home_league_directory() -> List[Dict[str, Any]]:
    """
    홈 탭 하단에 보여줄 '국가별 리그 디렉터리' 용 데이터.

    반환 예시:
    [
      {
        "country": "England",
        "league_id": 39,
        "league_name": "Premier League"
      },
      ...
    ]
    """
    params: List[Any] = []
    where_extra = ""

    if _has_home_filter():
        where_extra = "WHERE l.id = ANY(%s)"
        params.append(HOME_LEAGUE_IDS)

    rows = fetch_all(
        f"""
        SELECT
            COALESCE(l.country, '') AS country,
            l.id   AS league_id,
            l.name AS league_name
        FROM leagues l
        {where_extra}
        ORDER BY country, league_name
        """,
        params,
    )

    return rows


# ─────────────────────────────────────────
# 3) 다음 매치데이 찾기
#    /api/home/next_matchday  에서 사용
# ─────────────────────────────────────────

def get_next_matchday(date_str: str) -> Optional[Dict[str, Any]]:
    """
    주어진 날짜 이후에 '경기가 있는 가장 가까운 날짜'를 찾는다.

    반환 예시:
      {"next_date": "2025-01-17"}

    없으면 None.
    """
    params: List[Any] = [date_str]
    where_extra = ""

    if _has_home_filter():
        where_extra = " AND m.league_id = ANY(%s)"
        params.append(HOME_LEAGUE_IDS)

    row = fetch_one(
        f"""
        SELECT
            SUBSTRING(m.date_utc FROM 1 FOR 10) AS next_date
        FROM matches m
        WHERE SUBSTRING(m.date_utc FROM 1 FOR 10) > %s
        {where_extra}
        GROUP BY SUBSTRING(m.date_utc FROM 1 FOR 10)
        ORDER BY next_date ASC
        LIMIT 1
        """,
        params,
    )

    return row


# ─────────────────────────────────────────
# 4) 이전 매치데이 찾기
#    /api/home/prev_matchday  에서 사용
# ─────────────────────────────────────────

def get_prev_matchday(date_str: str) -> Optional[Dict[str, Any]]:
    """
    주어진 날짜 이전에 '경기가 있는 가장 가까운 날짜'를 찾는다.

    반환 예시:
      {"prev_date": "2024-12-30"}

    없으면 None.
    """
    params: List[Any] = [date_str]
    where_extra = ""

    if _has_home_filter():
        where_extra = " AND m.league_id = ANY(%s)"
        params.append(HOME_LEAGUE_IDS)

    row = fetch_one(
        f"""
        SELECT
            SUBSTRING(m.date_utc FROM 1 FOR 10) AS prev_date
        FROM matches m
        WHERE SUBSTRING(m.date_utc FROM 1 FOR 10) < %s
        {where_extra}
        GROUP BY SUBSTRING(m.date_utc FROM 1 FOR 10)
        ORDER BY prev_date DESC
        LIMIT 1
        """,
        params,
    )

    return row
