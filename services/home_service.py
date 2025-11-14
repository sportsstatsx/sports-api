import os
from datetime import datetime
from typing import List, Optional

from db import fetch_all, fetch_one


# ─────────────────────────────────────────
# 허용 리그 설정 (API-Football 대상 리그)
# main.py 와 동일한 규칙으로 환경변수 파싱
# ─────────────────────────────────────────

_RAW_LIVE_LEAGUES = (
    os.getenv("live-league")
    or os.getenv("LIVE_LEAGUES")
    or os.getenv("LIVE_LEAGUE")
    or ""
)


def _parse_allowed_league_ids(raw: str) -> List[int]:
    """
    "39, 140,141" 같은 문자열을 [39, 140, 141] 로 파싱.
    잘못된 값은 조용히 무시.
    """
    ids: List[int] = []
    for part in raw.replace(" ", "").split(","):
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            continue
    return ids


ALLOWED_LEAGUE_IDS: List[int] = _parse_allowed_league_ids(_RAW_LIVE_LEAGUES)


# ─────────────────────────────────────────
# 날짜 유틸
# ─────────────────────────────────────────

def _normalize_date(date_str: Optional[str]) -> str:
    """
    - date_str 가 None/빈 문자열이면 오늘(UTC) 날짜를 "YYYY-MM-DD" 로 반환
    - 값이 있으면 그대로 사용 (이미 YYYY-MM-DD 라고 가정)
    """
    if not date_str:
        return datetime.utcnow().strftime("%Y-%m-%d")
    return date_str


# ─────────────────────────────────────────
# 홈: 상단 리그 리스트 (오늘 경기 있는 리그들)
# ─────────────────────────────────────────

def get_home_leagues(date_str: Optional[str]) -> List[dict]:
    """
    상단 탭용: 오늘(또는 지정된 날짜)에 경기 있는 리그 목록.

    반환:
    [
      {
        "league_id": 39,
        "league_name": "Premier League",
        "country": "England",
        "logo": "https://...",
        "match_count": 8
      },
      ...
    ]
    """
    date_str = _normalize_date(date_str)

    where_parts = ["SUBSTRING(m.date_utc FROM 1 FOR 10) = %s"]
    params: List[object] = [date_str]

    if ALLOWED_LEAGUE_IDS:
        placeholders = ",".join(["%s"] * len(ALLOWED_LEAGUE_IDS))
        where_parts.append(f"m.league_id IN ({placeholders})")
        params.extend(ALLOWED_LEAGUE_IDS)

    where_sql = "WHERE " + " AND ".join(where_parts)

    sql = f"""
        SELECT
            l.id   AS league_id,
            l.name AS league_name,
            l.country,
            l.logo,
            COUNT(*) AS match_count
        FROM matches m
        JOIN leagues l
          ON m.league_id = l.id
        {where_sql}
        GROUP BY l.id, l.name, l.country, l.logo
        ORDER BY match_count DESC, l.id
    """

    return fetch_all(sql, tuple(params))


# ─────────────────────────────────────────
# 홈: 리그 디렉터리 (전체 리그 + 오늘 경기 수)
# ─────────────────────────────────────────

def get_home_league_directory(date_str: Optional[str]) -> List[dict]:
    """
    리그 선택 바텀시트용: 전체 지원 리그 + 오늘 경기 수.

    반환:
    [
      {
        "league_id": 39,
        "league_name": "Premier League",
        "country": "England",
        "logo": "https://...",
        "match_count": 8   # 오늘 경기 수 (없으면 0)
      },
      ...
    ]
    """
    date_str = _normalize_date(date_str)

    where_league_parts: List[str] = []
    params: List[object] = [date_str]

    if ALLOWED_LEAGUE_IDS:
        placeholders = ",".join(["%s"] * len(ALLOWED_LEAGUE_IDS))
        where_league_parts.append(f"l.id IN ({placeholders})")
        params.extend(ALLOWED_LEAGUE_IDS)

    where_league_sql = ""
    if where_league_parts:
        where_league_sql = "WHERE " + " AND ".join(where_league_parts)

    sql = f"""
        WITH match_counts AS (
            SELECT
                league_id,
                COUNT(*) AS match_count
            FROM matches
            WHERE SUBSTRING(date_utc FROM 1 FOR 10) = %s
            GROUP BY league_id
        )
        SELECT
            l.id   AS league_id,
            l.name AS league_name,
            l.country,
            l.logo,
            COALESCE(mc.match_count, 0) AS match_count
        FROM leagues l
        LEFT JOIN match_counts mc
          ON l.id = mc.league_id
        {where_league_sql}
        ORDER BY l.id
    """

    return fetch_all(sql, tuple(params))


# ─────────────────────────────────────────
# 홈: 다음 / 이전 매치데이
# ─────────────────────────────────────────

def get_next_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    """
    지정 날짜 이후(포함) 첫 번째 매치데이 날짜 문자열 반환 (없으면 None).

    - date_str: "YYYY-MM-DD"
    - league_id: 특정 리그만 보고 싶으면 ID, 전체면 None 또는 0
    """
    date_str = _normalize_date(date_str)

    where_parts = ["SUBSTRING(m.date_utc FROM 1 FOR 10) >= %s"]
    params: List[object] = [date_str]

    if league_id is not None and league_id > 0:
        where_parts.append("m.league_id = %s")
        params.append(league_id)

    if ALLOWED_LEAGUE_IDS:
        placeholders = ",".join(["%s"] * len(ALLOWED_LEAGUE_IDS))
        where_parts.append(f"m.league_id IN ({placeholders})")
        params.extend(ALLOWED_LEAGUE_IDS)

    where_sql = "WHERE " + " AND ".join(where_parts)

    sql = f"""
        SELECT MIN(SUBSTRING(m.date_utc FROM 1 FOR 10)) AS next_date
        FROM matches m
        {where_sql}
    """

    row = fetch_one(sql, tuple(params))
    if not row:
        return None
    return row.get("next_date")


def get_prev_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    """
    지정 날짜 이전(포함) 마지막 매치데이 날짜 문자열 반환 (없으면 None).

    - date_str: "YYYY-MM-DD"
    - league_id: 특정 리그만 보고 싶으면 ID, 전체면 None 또는 0
    """
    date_str = _normalize_date(date_str)

    where_parts = ["SUBSTRING(m.date_utc FROM 1 FOR 10) <= %s"]
    params: List[object] = [date_str]

    if league_id is not None and league_id > 0:
        where_parts.append("m.league_id = %s")
        params.append(league_id)

    if ALLOWED_LEAGUE_IDS:
        placeholders = ",".join(["%s"] * len(ALLOWED_LEAGUE_IDS))
        where_parts.append(f"m.league_id IN ({placeholders})")
        params.extend(ALLOWED_LEAGUE_IDS)

    where_sql = "WHERE " + " AND ".join(where_parts)

    sql = f"""
        SELECT MAX(SUBSTRING(m.date_utc FROM 1 FOR 10)) AS prev_date
        FROM matches m
        {where_sql}
    """

    row = fetch_one(sql, tuple(params))
    if not row:
        return None
    return row.get("prev_date")
