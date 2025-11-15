# services/home_service.py

from __future__ import annotations

import json
from datetime import datetime, date as date_cls
from typing import Any, Dict, List, Optional

from db import fetch_all


# ─────────────────────────────────────
#  공통: 날짜 파싱/정규화
# ─────────────────────────────────────

def _normalize_date(date_str: Optional[str]) -> str:
    """
    다양한 형태(YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS 등)의 문자열을
    안전하게 'YYYY-MM-DD' 형태로 정규화한다.
    """
    if not date_str:
        # 호출 측에서 None 체크를 해야 하지만, 혹시 몰라 방어
        return datetime.now().date().isoformat()

    s = date_str.strip()

    # 이미 yyyy-mm-dd 형태면 그대로 사용
    if len(s) >= 10:
        only_date = s[:10]
        try:
            dt = datetime.fromisoformat(only_date)
            return dt.date().isoformat()
        except Exception:
            # fromisoformat 실패 시에도 그대로 잘라서 쓴다
            return only_date
    return s


# ─────────────────────────────────────
#  1) 홈 상단 리그 탭용
# ─────────────────────────────────────

def get_home_leagues(date_str: str) -> List[Dict[str, Any]]:
    """
    주어진 날짜(date_str)에 실제 경기가 편성된 리그 목록을 돌려준다.

    반환 예시:
    [
      {
        "league_id": 39,
        "league_name": "Premier League",
        "country": "England",
        "logo": "https://...",
        "season": 2025,
      },
      ...
    ]
    """
    norm_date = _normalize_date(date_str)

    rows = fetch_all(
        """
        SELECT
            m.league_id,
            l.name  AS league_name,
            l.country,
            l.logo,
            m.season
        FROM matches m
        JOIN leagues l ON l.id = m.league_id
        WHERE m.date_utc::date = %s
        GROUP BY m.league_id, l.name, l.country, l.logo, m.season
        ORDER BY l.country NULLS LAST, l.name
        """,
        (norm_date,),
    )

    result: List[Dict[str, Any]] = []
    for r in rows:
        result.append(
            {
                "league_id": r["league_id"],
                "league_name": r["league_name"],
                "country": r.get("country"),
                "logo": r.get("logo"),
                "season": r["season"],
            }
        )
    return result


# ─────────────────────────────────────
#  2) 홈: 리그별 매치데이 디렉터리
# ─────────────────────────────────────

def get_home_league_directory(date_str: str, league_id: Optional[int]) -> Dict[str, Any]:
    """
    특정 리그(또는 전체)에 대해 사용 가능한 매치데이(날짜 목록)를 돌려준다.

    - date_str 에 가장 가까운 매치데이를 current_date 로 잡고
    - 그 주변 모든 매치데이를 items 리스트에 담아준다.

    반환 예시:
    {
      "current_date": "2025-11-15",
      "items": [
        {"date": "2025-11-10", "matches": 8},
        {"date": "2025-11-15", "matches": 6},
        {"date": "2025-11-20", "matches": 7},
        ...
      ]
    }
    """
    norm_date = _normalize_date(date_str)

    params: List[Any] = []
    where_clause = "1=1"
    if league_id and league_id > 0:
        where_clause += " AND m.league_id = %s"
        params.append(league_id)

    rows = fetch_all(
        f"""
        SELECT
            m.date_utc::date AS match_date,
            COUNT(*)          AS matches
        FROM matches m
        WHERE {where_clause}
        GROUP BY match_date
        ORDER BY match_date ASC
        """,
        tuple(params),
    )

    items: List[Dict[str, Any]] = []
    target = datetime.fromisoformat(norm_date).date()
    nearest: Optional[date_cls] = None

    for r in rows:
        md: date_cls = r["match_date"]
        items.append(
            {
                "date": md.isoformat(),
                "matches": r["matches"],
            }
        )
        if nearest is None:
            nearest = md
        else:
            # target 과의 차이가 더 작은 날짜를 current 로 선택
            if abs(md - target) < abs(nearest - target):
                nearest = md

    current_date = nearest.isoformat() if nearest is not None else norm_date

    return {
        "current_date": current_date,
        "items": items,
    }


# ─────────────────────────────────────
#  3) 다음/이전 매치데이
# ─────────────────────────────────────

def _find_matchday(date_str: str, league_id: Optional[int], *, direction: str) -> Optional[str]:
    """
    direction:
      - "next" : date_str 이후(포함) 첫 매치데이
      - "prev" : date_str 이전(포함) 마지막 매치데이
    """
    norm_date = _normalize_date(date_str)

    params: List[Any] = [norm_date]
    where_parts: List[str] = ["m.date_utc::date >= %s" if direction == "next" else "m.date_utc::date <= %s"]

    if league_id and league_id > 0:
        where_parts.append("m.league_id = %s")
        params.append(league_id)

    order = "ASC" if direction == "next" else "DESC"

    sql = f"""
        SELECT
            m.date_utc::date AS match_date
        FROM matches m
        WHERE {' AND '.join(where_parts)}
        GROUP BY match_date
        ORDER BY match_date {order}
        LIMIT 1
    """

    rows = fetch_all(sql, tuple(params))
    if not rows:
        return None

    match_date = rows[0]["match_date"]
    # match_date 가 date 객체이든 문자열이든 str() 하면 YYYY-MM-DD 형태가 나옴
    return str(match_date)


def get_next_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    """지정 날짜 이후(포함) 첫 매치데이."""
    return _find_matchday(date_str, league_id, direction="next")


def get_prev_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    """지정 날짜 이전(포함) 마지막 매치데이."""
    return _find_matchday(date_str, league_id, direction="prev")


# ─────────────────────────────────────
#  4) 팀 시즌 스탯 (team_season_stats)
#     + match_team_stats 기반 슈팅 집계
# ─────────────────────────────────────

def get_team_season_stats(team_id: int, league_id: int) -> Optional[Dict[str, Any]]:
    """
    team_season_stats 테이블에서
    (league_id, team_id) 에 해당하는 가장 최신 season 한 줄을 가져온다.

    value 컬럼은 API-Football /teams/statistics 의 JSON 이거나,
    그와 동등한 구조의 full_json 이라고 가정한다.

    여기서 일부 고급 지표(insights_overall.*)가 추가/보정된다.
    (예: shots_per_match, shots_on_target_pct)

    shots 값이 full_json 안에 없거나 0이면,
    match_team_stats + matches 를 합산해서 다시 계산한다.
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
    season = row["season"]

    # value 컬럼(JSONB 혹은 TEXT)을 파이썬 dict 로 정규화
    raw_value = row["value"]
    if isinstance(raw_value, dict):
        stats = raw_value
    else:
        try:
            stats = json.loads(raw_value)
        except Exception:
            stats = {}

    if not isinstance(stats, dict):
        stats = {}

    # insights_overall 보장
    insights = stats.get("insights_overall")
    if not isinstance(insights, dict):
        insights = {}
        stats["insights_overall"] = insights

    # ─────────────────────────────────────────
    # 기본 경기 수 (fixtures.played.*)
    # ─────────────────────────────────────────
    fixtures = stats.get("fixtures") or {}
    played = fixtures.get("played") or {}

    matches_total = played.get("total") or 0
    matches_home = played.get("home") or 0
    matches_away = played.get("away") or 0

    def safe_div(num, den) -> float:
        try:
            num_f = float(num)
        except (TypeError, ValueError):
            return 0.0
        if not den:
            return 0.0
        return num_f / float(den)

    # ─────────────────────────────────────────
    # 1단계: full_json 안에 shots 가 있으면 우선 사용
    # ─────────────────────────────────────────
    def _extract_shots_from_stats(root: Dict[str, Any]) -> Dict[str, int]:
        shots_block = root.get("shots")
        if not isinstance(shots_block, dict):
            return {
                "st_total": 0,
                "st_home": 0,
                "st_away": 0,
                "so_total": 0,
                "so_home": 0,
                "so_away": 0,
            }

        total_block = shots_block.get("total") or {}
        on_block = shots_block.get("on") or {}

        def to_int(v: Any) -> int:
            try:
                return int(v)
            except (TypeError, ValueError):
                return 0

        st_total = to_int(total_block.get("total"))
        st_home = to_int(total_block.get("home"))
        st_away = to_int(total_block.get("away"))

        so_total = to_int(on_block.get("total"))
        so_home = to_int(on_block.get("home"))
        so_away = to_int(on_block.get("away"))

        return {
            "st_total": st_total,
            "st_home": st_home,
            "st_away": st_away,
            "so_total": so_total,
            "so_home": so_home,
            "so_away": so_away,
        }

    base_shots = _extract_shots_from_stats(stats)

    st_total = base_shots["st_total"]
    st_home = base_shots["st_home"]
    st_away = base_shots["st_away"]
    so_total = base_shots["so_total"]
    so_home = base_shots["so_home"]
    so_away = base_shots["so_away"]

    has_any_shots = (st_total > 0) or (so_total > 0)

    # ─────────────────────────────────────────
    # 2단계: shots 가 없거나 전부 0이면,
    #        match_team_stats + matches 에서 재계산
    # ─────────────────────────────────────────
    if not has_any_shots:
        agg_rows = fetch_all(
            """
            SELECT
                COUNT(DISTINCT m.fixture_id) AS matches_total,
                COUNT(DISTINCT m.fixture_id) FILTER (WHERE m.home_id = %s) AS matches_home,
                COUNT(DISTINCT m.fixture_id) FILTER (WHERE m.away_id = %s) AS matches_away,

                SUM(
                    CASE
                        WHEN mts.name IN ('Total Shots', 'Shots Total', 'Total shots', 'Shots')
                             AND mts.value ~ '^[0-9]+$'
                        THEN mts.value::int
                        ELSE 0
                    END
                ) AS shots_total,

                SUM(
                    CASE
                        WHEN mts.name IN ('Total Shots', 'Shots Total', 'Total shots', 'Shots')
                             AND m.home_id = %s
                             AND mts.value ~ '^[0-9]+$'
                        THEN mts.value::int
                        ELSE 0
                    END
                ) AS shots_home,

                SUM(
                    CASE
                        WHEN mts.name IN ('Total Shots', 'Shots Total', 'Total shots', 'Shots')
                             AND m.away_id = %s
                             AND mts.value ~ '^[0-9]+$'
                        THEN mts.value::int
                        ELSE 0
                    END
                ) AS shots_away,

                SUM(
                    CASE
                        WHEN mts.name IN ('Shots on Goal', 'ShotsOnGoal', 'Shots on target', 'Shots on Target')
                             AND mts.value ~ '^[0-9]+$'
                        THEN mts.value::int
                        ELSE 0
                    END
                ) AS sog_total,

                SUM(
                    CASE
                        WHEN mts.name IN ('Shots on Goal', 'ShotsOnGoal', 'Shots on target', 'Shots on Target')
                             AND m.home_id = %s
                             AND mts.value ~ '^[0-9]+$'
                        THEN mts.value::int
                        ELSE 0
                    END
                ) AS sog_home,

                SUM(
                    CASE
                        WHEN mts.name IN ('Shots on Goal', 'ShotsOnGoal', 'Shots on target', 'Shots on Target')
                             AND m.away_id = %s
                             AND mts.value ~ '^[0-9]+$'
                        THEN mts.value::int
                        ELSE 0
                    END
                ) AS sog_away
            FROM match_team_stats mts
            JOIN matches m ON m.fixture_id = mts.fixture_id
            WHERE m.league_id = %s
              AND m.season    = %s
              AND mts.team_id = %s
            """,
            (
                team_id,  # matches_home 필터용
                team_id,  # matches_away 필터용
                team_id,  # shots_home 필터용
                team_id,  # shots_away 필터용
                team_id,  # sog_home 필터용
                team_id,  # sog_away 필터용
                league_id,
                season,
                team_id,
            ),
        )

        if agg_rows:
            agg = agg_rows[0]

            # 매치 수는 team_season_stats 안의 fixtures.played 와 다를 수 있지만,
            # 슈팅 관련 지표에서는 match_team_stats 기준으로 사용하는 것이 더 정확하다.
            mt_total = agg.get("matches_total") or matches_total
            mt_home = agg.get("matches_home") or matches_home
            mt_away = agg.get("matches_away") or matches_away

            if mt_total:
                matches_total = mt_total
            if mt_home:
                matches_home = mt_home
            if mt_away:
                matches_away = mt_away

            st_total = (agg.get("shots_total") or 0) or st_total
            st_home = (agg.get("shots_home") or 0) or st_home
            st_away = (agg.get("shots_away") or 0) or st_away

            so_total = (agg.get("sog_total") or 0) or so_total
            so_home = (agg.get("sog_home") or 0) or so_home
            so_away = (agg.get("sog_away") or 0) or so_away

            # 원본 stats 안에도 shots 블록을 채워 넣어두면,
            # 나중에 다른 곳에서 써도 일관성이 생김
            stats["shots"] = {
                "total": {
                    "total": int(st_total),
                    "home": int(st_home),
                    "away": int(st_away),
                },
                "on": {
                    "total": int(so_total),
                    "home": int(so_home),
                    "away": int(so_away),
                },
            }

    # ─────────────────────────────────────────
    # 최종: insights_overall 에 shots_per_match / shots_on_target_pct 채우기
    # ─────────────────────────────────────────
    def fmt_avg(n, m):
        v = safe_div(n, m)
        return round(v, 2) if v > 0 else 0.0

    def fmt_pct(n, d):
        v = safe_div(n, d)
        return int(round(v * 100)) if v > 0 else 0

    if matches_total > 0 and (st_total > 0 or so_total > 0):
        insights["shots_per_match"] = {
            "total": fmt_avg(st_total, matches_total),
            "home": fmt_avg(st_home, matches_home or matches_total),
            "away": fmt_avg(st_away, matches_away or matches_total),
        }

        insights["shots_on_target_pct"] = {
            "total": fmt_pct(so_total, st_total),
            "home": fmt_pct(so_home, st_home),
            "away": fmt_pct(so_away, st_away),
        }

    # ─────────────────────────────────────────
    # 최종 반환 – value 에 stats(dict)를 넣어서 반환
    # (DB에 다시 저장하진 않고, API 응답에서만 계산된 필드 사용)
    # ─────────────────────────────────────────
    return {
        "league_id": row["league_id"],
        "season": row["season"],
        "team_id": row["team_id"],
        "name": row.get("name"),
        "value": stats,
    }


# ─────────────────────────────────────
#  5) 팀 정보 (teams 테이블)
# ─────────────────────────────────────

def get_team_info(team_id: int) -> Optional[Dict[str, Any]]:
    """
    teams 테이블에서 단일 팀 정보 조회.
    """
    rows = fetch_all(
        """
        SELECT
            id,
            name,
            country,
            logo
        FROM teams
        WHERE id = %s
        LIMIT 1
        """,
        (team_id,),
    )
    if not rows:
        return None
    return rows[0]
