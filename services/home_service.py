from typing import Any, Dict, List, Optional

from db import fetch_all
from live_fixtures_common import (
    LIVE_LEAGUES_ENV,
    parse_live_leagues,
    now_utc,
)


# ─────────────────────────────────────────
# 공통 유틸
# ─────────────────────────────────────────

def _resolve_date(date_str: Optional[str]) -> str:
    """
    date_str 가 None 이면 오늘(UTC) yyyy-MM-dd 로 반환.
    """
    if date_str:
        return date_str
    return now_utc().strftime("%Y-%m-%d")


def _load_match_counts_by_league(date_str: str) -> Dict[int, int]:
    """
    matches 테이블에서 해당 날짜의 리그별 경기 수 집계.
    """
    rows = fetch_all(
        """
        SELECT league_id, COUNT(*) AS match_count
        FROM matches
        WHERE SUBSTRING(date_utc FROM 1 FOR 10) = %s
        GROUP BY league_id
        """,
        (date_str,),
    )
    counts: Dict[int, int] = {}
    for r in rows:
        lid = r.get("league_id")
        if lid is None:
            continue
        counts[int(lid)] = int(r.get("match_count") or 0)
    return counts


def _get_supported_league_ids() -> Optional[List[int]]:
    """
    LIVE_LEAGUES_ENV 를 사용해서 '지원 리그' 리스트 반환.
    값이 없으면 None (전체 리그 허용).
    """
    if not LIVE_LEAGUES_ENV:
        return None
    ids = parse_live_leagues(LIVE_LEAGUES_ENV)
    return ids or None


# ─────────────────────────────────────────
# 1) 홈 상단 탭: get_home_leagues
# ─────────────────────────────────────────

def get_home_leagues(date_str: Optional[str]) -> List[Dict[str, Any]]:
    """
    상단 탭용: 해당 날짜에 실제로 경기가 있는 리그만 반환.

    returns: [
      {
        "country": "...",
        "league_id": 39,
        "league_name": "Premier League",
        "logo": "https://...",
        "match_count": 5,
      }, ...
    ]
    """
    target_date = _resolve_date(date_str)

    # 리그별 경기 수
    match_counts = _load_match_counts_by_league(target_date)
    if not match_counts:
        return []

    supported_ids = _get_supported_league_ids()

    # matches 에서 날짜 조건 + 필요한 리그만 골라 leagues 테이블 조인
    rows = fetch_all(
        """
        SELECT
            l.country,
            m.league_id,
            l.name  AS league_name,
            l.logo  AS logo
        FROM matches m
        JOIN leagues l ON l.id = m.league_id
        WHERE SUBSTRING(m.date_utc FROM 1 FOR 10) = %s
        GROUP BY l.country, m.league_id, l.name, l.logo
        """,
        (target_date,),
    )

    result: List[Dict[str, Any]] = []
    for r in rows:
        league_id = int(r["league_id"])
        if supported_ids and league_id not in supported_ids:
            continue

        cnt = match_counts.get(league_id, 0)
        if cnt <= 0:
            continue

        result.append(
            {
                "country": r.get("country"),
                "league_id": league_id,
                "league_name": r.get("league_name"),
                "logo": r.get("logo"),
                "match_count": cnt,
            }
        )

    # 나라 / 리그 이름 기준 정렬 (원하면 match_count 기준으로 바꿀 수 있음)
    result.sort(key=lambda x: (x["country"] or "", x["league_name"] or ""))
    return result


# ─────────────────────────────────────────
# 2) 리그 디렉터리: get_home_league_directory
# ─────────────────────────────────────────

def get_home_league_directory(date_str: Optional[str]) -> List[Dict[str, Any]]:
    """
    리그 선택 바텀시트용: '지원 리그' 전체 + 해당 날짜의 match_count.

    - 지원 리그: LIVE_LEAGUES_ENV 에 정의된 리그 (없으면 leagues 전체)
    """
    target_date = _resolve_date(date_str)
    match_counts = _load_match_counts_by_league(target_date)
    supported_ids = _get_supported_league_ids()

    # leagues 테이블에서 리그 목록 가져오기
    rows = fetch_all(
        """
        SELECT
            id      AS league_id,
            name    AS league_name,
            country,
            logo
        FROM leagues
        ORDER BY country, name
        """,
        (),
    )

    result: List[Dict[str, Any]] = []
    for r in rows:
        league_id = int(r["league_id"])
        if supported_ids and league_id not in supported_ids:
            continue

        result.append(
            {
                "country": r.get("country"),
                "league_id": league_id,
                "league_name": r.get("league_name"),
                "logo": r.get("logo"),
                "match_count": match_counts.get(league_id, 0),
            }
        )

    return result


# ─────────────────────────────────────────
# 3) 다음 / 이전 매치데이
# ─────────────────────────────────────────

def get_next_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    """
    date_str(yyyy-MM-dd) 이후(포함) 첫 번째 매치데이 날짜 문자열을 반환.
    league_id 가 주어지면 그 리그만.
    """
    target_date = date_str  # 라우터에서 필수로 받으므로 여기서는 그대로 사용

    params: List[Any] = [target_date]
    where_clauses = ["SUBSTRING(date_utc FROM 1 FOR 10) >= %s"]

    if league_id and league_id > 0:
        where_clauses.append("league_id = %s")
        params.append(league_id)

    sql = f"""
        SELECT MIN(SUBSTRING(date_utc FROM 1 FOR 10)) AS match_date
        FROM matches
        WHERE {' AND '.join(where_clauses)}
    """

    rows = fetch_all(sql, tuple(params))
    if not rows:
        return None

    match_date = rows[0].get("match_date")
    return match_date


def get_prev_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    """
    date_str(yyyy-MM-dd) 이전(포함) 마지막 매치데이 날짜 문자열을 반환.
    league_id 가 주어지면 그 리그만.
    """
    target_date = date_str

    params: List[Any] = [target_date]
    where_clauses = ["SUBSTRING(date_utc FROM 1 FOR 10) <= %s"]

    if league_id and league_id > 0:
        where_clauses.append("league_id = %s")
        params.append(league_id)

    sql = f"""
        SELECT MAX(SUBSTRING(date_utc FROM 1 FOR 10)) AS match_date
        FROM matches
        WHERE {' AND '.join(where_clauses)}
    """

    rows = fetch_all(sql, tuple(params))
    if not rows:
        return None

    match_date = rows[0].get("match_date")
    return match_date
