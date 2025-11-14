# services/home_service.py
"""
홈 화면 관련 조회 로직 모듈.

- /api/fixtures
- /api/home/leagues
- /api/home/league_directory
- /api/home/next_matchday
- /api/home/prev_matchday

Flask, 라우팅, rate limit 같은 것은 main.py 에 두고
여기서는 DB 조회/비즈니스 로직만 담당한다.
"""

import os
from datetime import datetime

from db import fetch_all, fetch_one


# ─────────────────────────────────────────
# 허용 리그 목록 (LIVE_LEAGUES / live-league 환경변수)
# ─────────────────────────────────────────

_RAW_LIVE_LEAGUES = (
    os.getenv("live-league")
    or os.getenv("LIVE_LEAGUES")
    or os.getenv("LIVE_LEAGUE")
    or ""
)


def _parse_allowed_league_ids(raw: str):
    ids: list[int] = []
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


ALLOWED_LEAGUE_IDS: list[int] = _parse_allowed_league_ids(_RAW_LIVE_LEAGUES)


# ─────────────────────────────────────────
# /api/fixtures  (홈 매치 리스트)
# ─────────────────────────────────────────

def get_fixtures_for_home(
    *,
    league_id: int | None,
    date_str: str | None,
    page: int,
    page_size: int,
):
    """
    홈 화면 매치 리스트용 데이터.

    매개변수:
      - league_id: >0 이면 해당 리그만, None/0 이면 허용 리그 전체
      - date_str: "YYYY-MM-DD" (없으면 오늘 UTC 기준)
      - page, page_size: 페이지네이션
    """
    if not date_str:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")

    where_parts: list[str] = ["m.date_utc::date = %s::date"]
    params: list[object] = [date_str]

    # 리그 필터
    if league_id and league_id > 0:
        where_parts.append("m.league_id = %s")
        params.append(league_id)
    elif ALLOWED_LEAGUE_IDS:
        placeholders = ",".join(["%s"] * len(ALLOWED_LEAGUE_IDS))
        where_parts.append(f"m.league_id IN ({placeholders})")
        params.extend(ALLOWED_LEAGUE_IDS)

    where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

    sql = f"""
        SELECT
            m.fixture_id,
            m.league_id,
            l.name AS league_name,
            m.season,
            m.date_utc,
            TO_CHAR(m.date_utc AT TIME ZONE 'UTC', 'YYYY-MM-DD') AS match_date,
            TO_CHAR(m.date_utc AT TIME ZONE 'UTC', 'HH24:MI:SS') AS match_time_utc,
            m.status,
            m.status_group,
            m.home_id,
            th.name AS home_name,
            m.away_id,
            ta.name AS away_name,
            m.home_ft,
            m.away_ft
        FROM matches m
        JOIN leagues l ON l.id = m.league_id
        JOIN teams   th ON th.id = m.home_id
        JOIN teams   ta ON ta.id = m.away_id
        {where_sql}
        ORDER BY m.date_utc ASC
        LIMIT %s OFFSET %s
    """

    params.extend([page_size, (page - 1) * page_size])

    rows = fetch_all(sql, tuple(params))
    return rows


# ─────────────────────────────────────────
# /api/home/leagues  (상단 탭용)
# ─────────────────────────────────────────

def get_home_leagues(*, date_str: str | None):
    """
    상단 탭용: 해당 날짜에 경기 있는 리그 목록.
    """
    if not date_str:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")

    where_parts: list[str] = ["m.date_utc::date = %s::date"]
    params: list[object] = [date_str]

    if ALLOWED_LEAGUE_IDS:
        placeholders = ",".join(["%s"] * len(ALLOWED_LEAGUE_IDS))
        where_parts.append(f"m.league_id IN ({placeholders})")
        params.extend(ALLOWED_LEAGUE_IDS)

    where_sql = "WHERE " + " AND ".join(where_parts) if where_parts else ""

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

    rows = fetch_all(sql, tuple(params))
    return rows


# ─────────────────────────────────────────
# /api/home/league_directory  (대륙/리그 바텀시트용)
# ─────────────────────────────────────────

def get_league_directory(*, date_str: str | None):
    """
    대륙/리그 선택용 디렉토리.

    - 오늘 경기 수(today_count)를 함께 내려줌
    - 허용 리그(ALLOWED_LEAGUE_IDS)만 대상
    """
    if not date_str:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")

    params: list[object] = [date_str]

    league_filter_sql = ""
    if ALLOWED_LEAGUE_IDS:
        placeholders = ",".join(["%s"] * len(ALLOWED_LEAGUE_IDS))
        league_filter_sql = f"WHERE l.id IN ({placeholders})"
        params.extend(ALLOWED_LEAGUE_IDS)

    sql = f"""
        SELECT
            l.country,
            l.id   AS league_id,
            l.name AS league_name,
            COALESCE(
                SUM(
                    CASE
                        WHEN m.date_utc::date = %s::date THEN 1
                        ELSE 0
                    END
                ),
                0
            ) AS today_count
        FROM leagues l
        LEFT JOIN matches m ON m.league_id = l.id
        {league_filter_sql}
        GROUP BY l.country, l.id, l.name
        ORDER BY l.country, l.name
    """

    rows = fetch_all(sql, tuple(params))
    return rows


# ─────────────────────────────────────────
# /api/home/next_matchday  /  /api/home/prev_matchday
# ─────────────────────────────────────────

def get_next_matchday(*, date_str: str | None, league_id: int | None):
    """
    기준 날짜 이후(포함)로 가장 가까운 매치데이 날짜.
    """
    if not date_str:
        date_str = datetime.utcnow().strftime("%Y-%m-%d")

    where_parts: list[str] = ["m.date_utc::date >= %s::date"]
    params: list[object] = [date_str]

    if league_id and league_id > 0:
        where_parts.append("m.league_id = %s")
        params.append(league_id)
    elif ALLOWED_LEAGUE_IDS:
        placeholders = ",".join(["%s"] * len(ALLOWED_LEAGUE_IDS))
        where_parts.append(f"m.league_id IN ({placeholders})")
        params.extend(ALLOWED_LEAGUE_IDS)

    where_sql = " AND ".join(where_parts)

    sql = f"""
        SELECT
            MIN(m.date_utc::date) AS next_date
        FROM matches m
        WHERE {where_sql}
    """

    row = fetch_one(sql, tuple(params))
    return row["next_date"] if row and row.get("next_date") is not None else None


def get_prev_matchday(*, date_str: str | None, league_id: int | None):
    """
    기준 날짜 이전(포함)으로 가장
