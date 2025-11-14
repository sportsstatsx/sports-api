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
    ids: List[int] = []
    for part in raw.replace(";", ",").split(","):
        part = part.strip()
        if not part:
            continue
        try:
            ids.append(int(part))
        except ValueError:
            continue
    # 중복 제거 + 정렬
    return sorted(set(ids))


ALLOWED_LEAGUE_IDS: List[int] = _parse_allowed_league_ids(_RAW_LIVE_LEAGUES)


def _normalize_date(date_str: Optional[str]) -> str:
    """
    date_str 가 None/빈 문자열이면 UTC 오늘 날짜(YYYY-MM-DD)로 대체.
    """
    if not date_str:
        return datetime.utcnow().strftime("%Y-%m-%d")
    return date_str


# ─────────────────────────────────────────
# 홈: 상단 리그 탭용 서비스
# ─────────────────────────────────────────

def get_home_leagues(date_str: Optional[str]) -> List[dict]:
    """
    상단 탭용: 오늘(또는 지정된 날짜)에 경기 있는 리그 목록.
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
            m.league_id,
            l.name AS league_name,
            COUNT(*) AS match_count
        FROM matches m
        JOIN leagues l ON l.id = m.league_id
        {where_sql}
        GROUP BY m.league_id, l.name
        ORDER BY l.name ASC
    """

    return fetch_all(sql, tuple(params))


# ─────────────────────────────────────────
# 홈: 리그 디렉터리 (전체 리그 + 오늘 경기 수)
# ─────────────────────────────────────────

def get_home_league_directory(date_str: Optional[str]) -> List[dict]:
    """
    리그 선택 바텀시트용: 전체 지원 리그 + 오늘 경기 수.
    """
    date_str = _normalize_date(date_str)

    where_parts: List[str] = []
    params: List[object] = []

    # 허용된 리그만 대상으로
    if ALLOWED_LEAGUE_IDS:
        placeholders = ",".join(["%s"] * len(ALLOWED_LEAGUE_IDS))
        where_parts.append(f"l.id IN ({placeholders})")
        params.extend(ALLOWED_LEAGUE_IDS)

    where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    sql = f"""
        SELECT
            l.id AS league_id,
            l.name AS league_name,
            l.country AS country,
            COALESCE(
                SUM(
                    CASE
                        WHEN SUBSTRING(m.date_utc FROM 1 FOR 10) = %s THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS today_count
        FROM leagues l
        LEFT JOIN matches m ON m.league_id = l.id
        {where_sql}
        GROUP BY l.id, l.name, l.country
        ORDER BY l.name ASC
    """

    # today_count 계산용 날짜 파라미터를 맨 앞에 추가
    params_with_date: List[object] = list(params)
    params_with_date.insert(0, date_str)

    return fetch_all(sql, tuple(params_with_date))


# ─────────────────────────────────────────
# 홈: 다음 / 이전 매치데이
# ─────────────────────────────────────────

def get_next_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    """
    지정 날짜 이후(포함) 첫 번째 매치데이 날짜 문자열 반환 (없으면 None).
    """
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
    지정 날짜 이전 마지막 매치데이 날짜 문자열 반환 (없으면 None).
    """
    where_parts = ["SUBSTRING(m.date_utc FROM 1 FOR 10) < %s"]
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
