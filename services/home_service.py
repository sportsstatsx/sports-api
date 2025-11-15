# services/home_service.py

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from db import fetch_all


# ─────────────────────────────────────
#  공통: 날짜 파싱
# ─────────────────────────────────────

def _normalize_date(date_str: Optional[str]) -> str:
    """
    yyyy-MM-dd 형태의 문자열을 받고, 없으면 오늘(UTC 기준)으로 채움.
    항상 'YYYY-MM-DD' 문자열을 리턴.
    """
    if date_str:
        # 이미 yyyy-MM-dd 로 들어온다고 가정하지만, 혹시 몰라서 파싱 한 번 함
        dt = datetime.strptime(date_str, "%Y-%m-%d").date()
        return dt.isoformat()

    today_utc = datetime.now(timezone.utc).date()
    return today_utc.isoformat()


# ─────────────────────────────────────
#  1) 홈 상단 리그 탭용 API
#     /api/home/leagues
# ─────────────────────────────────────

def get_home_leagues(date_str: Optional[str]) -> List[Dict[str, Any]]:
    """
    상단 탭용: 해당 날짜에 '경기가 있는 리그' 만 반환.

    반환 컬럼:
      - country
      - league_id
      - league_name
      - logo
      - match_count
    """
    d = _normalize_date(date_str)

    rows = fetch_all(
        """
        SELECT
            l.country                AS country,
            m.league_id              AS league_id,
            l.name                   AS league_name,
            COALESCE(l.logo, '')     AS logo,
            COUNT(*)                 AS match_count
        FROM matches m
        JOIN leagues l
          ON l.id = m.league_id
        WHERE m.date_utc::date = %s
        GROUP BY l.country, m.league_id, l.name, l.logo
        ORDER BY l.country, l.name
        """,
        (d,),
    )

    # fetch_all 이 dict 리스트를 반환한다고 가정
    return rows


# ─────────────────────────────────────
#  2) 홈 리그 디렉터리
#     /api/home/league_directory
# ─────────────────────────────────────

def get_home_league_directory(date_str: Optional[str]) -> List[Dict[str, Any]]:
    """
    리그 선택 바텀시트용: "전체 지원 리그" + 해당 날짜 경기 수.

    반환 컬럼:
      - country
      - league_id
      - league_name
      - logo
      - match_count (없으면 0)
    """
    d = _normalize_date(date_str)

    # leagues 전체를 기준으로 LEFT JOIN 해서
    # DB에 존재하는 리그는 모두 나오도록 구성
    rows = fetch_all(
        """
        WITH match_counts AS (
            SELECT
                league_id,
                COUNT(*) AS match_count
            FROM matches
            WHERE date_utc::date = %s
            GROUP BY league_id
        )
        SELECT
            l.country                    AS country,
            l.id                         AS league_id,
            l.name                       AS league_name,
            COALESCE(l.logo, '')         AS logo,
            COALESCE(mc.match_count, 0)  AS match_count
        FROM leagues l
        LEFT JOIN match_counts mc
          ON mc.league_id = l.id
        -- 실제로 한 번이라도 matches 에 등장한 리그만 보고 싶으면 아래 WHERE 사용
        -- WHERE l.id IN (SELECT DISTINCT league_id FROM matches)
        ORDER BY l.country, l.name
        """,
        (d,),
    )

    return rows


# ─────────────────────────────────────
#  3) 다음 / 이전 매치데이
#     /api/home/next_matchday
#     /api/home/prev_matchday
# ─────────────────────────────────────

def get_next_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    """
    지정 날짜 이후(포함) 첫 번째 매치데이 날짜를 yyyy-MM-dd 로 반환.
    league_id 가 None 또는 0 이면 전체 리그 기준.
    """
    d = _normalize_date(date_str)

    where_clauses = ["m.date_utc::date >= %s"]
    params: List[Any] = [d]

    if league_id and league_id > 0:
        where_clauses.append("m.league_id = %s")
        params.append(league_id)

    sql = f"""
        SELECT
            m.date_utc::date AS match_date
        FROM matches m
        WHERE {' AND '.join(where_clauses)}
        GROUP BY match_date
        ORDER BY match_date ASC
        LIMIT 1
    """

    rows = fetch_all(sql, tuple(params))
    if not rows:
        return None

    match_date = rows[0]["match_date"]
    # match_date 가 date 객체이든 문자열이든 str() 하면 YYYY-MM-DD 형태가 나옴
    return str(match_date)


def get_prev_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    """
    지정 날짜 이전 마지막 매치데이 날짜를 yyyy-MM-dd 로 반환.
    league_id 가 None 또는 0 이면 전체 리그 기준.
    """
    d = _normalize_date(date_str)

    where_clauses = ["m.date_utc::date <= %s"]
    params: List[Any] = [d]

    if league_id and league_id > 0:
        where_clauses.append("m.league_id = %s")
        params.append(league_id)

    sql = f"""
        SELECT
            m.date_utc::date AS match_date
        FROM matches m
        WHERE {' AND '.join(where_clauses)}
        GROUP BY match_date
        ORDER BY match_date DESC
        LIMIT 1
    """

    rows = fetch_all(sql, tuple(params))
    if not rows:
        return None

    # ─────────────────────────────────────
#  4) 팀 시즌 스탯 (team_season_stats)
#     /api/team_season_stats 에서 사용
# ─────────────────────────────────────

def get_team_season_stats(team_id: int, league_id: int):
    """
    team_season_stats 테이블에서
    (league_id, team_id) 에 해당하는 가장 최신 season 한 줄을 가져온다.

    반환 예시:
      {
        "league_id": 39,
        "season": 2025,
        "team_id": 42,
        "name": "full_json",
        "value": { ... 원본 JSON ... }
      }
    """

    rows = fetch_all(
        """
        SELECT
            league_id,
            season,
            team_id,
            name,
            value
        FROM team_season_stats
        WHERE league_id = %s
          AND team_id   = %s
        ORDER BY season DESC
        LIMIT 1
        """,
        (league_id, team_id),
    )

    if not rows:
        return None

    row = rows[0]

    return {
        "league_id": row["league_id"],
        "season": row["season"],
        "team_id": row["team_id"],
        "name": row.get("name"),
        "value": row["value"],  # JSONB → 파이썬 dict 로 나옴
    }


    match_date = rows[0]["match_date"]
    return str(match_date)
