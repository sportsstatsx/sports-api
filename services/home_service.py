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
    where_parts: List[str] = [
        "m.date_utc::date >= %s" if direction == "next" else "m.date_utc::date <= %s"
    ]

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
# ─────────────────────────────────────

def get_team_season_stats(team_id: int, league_id: int) -> Optional[Dict[str, Any]]:
    """
    team_season_stats 테이블에서
    (league_id, team_id) 에 해당하는 가장 최신 season 한 줄을 가져온다.

    value 컬럼은 API-Football /teams/statistics 의 JSON 이거나,
    그와 동등한 구조의 full_json 이라고 가정한다.

    여기서 일부 고급 지표(insights_overall.*)가 추가/보정된다.
    (예: shots_per_match, shots_on_target_pct)

    ⚠️ shots 필드가 full_json 안에 없거나 null 인 경우,
       match_team_stats 테이블을 이용해서 경기별 통계를 합산해서 계산한다.
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
    # ① full_json 안에 shots 필드가 있을 때 → 그대로 사용
    # ─────────────────────────────────────────
    shots = stats.get("shots") or {}
    if isinstance(shots, dict) and matches_total > 0:
        total_block = shots.get("total") or {}
        on_block = shots.get("on") or {}

        st_total = total_block.get("total") or 0
        st_home = total_block.get("home") or 0
        st_away = total_block.get("away") or 0

        so_total = on_block.get("total") or 0
        so_home = on_block.get("home") or 0
        so_away = on_block.get("away") or 0

        def fmt_avg(n, m):
            v = safe_div(n, m)
            # 숫자로 내려주고, 클라이언트에서 포맷팅 (소수 자리수 등) 처리
            return round(v, 2) if v > 0 else 0.0

        def fmt_pct(n, d):
            v = safe_div(n, d)
            return int(round(v * 100)) if v > 0 else 0

        # 이미 값이 있다면 덮어쓰지 않고, 없으면 새로 채움
        insights.setdefault(
            "shots_per_match",
            {
                "total": fmt_avg(st_total, matches_total),
                "home": fmt_avg(st_home, matches_home or matches_total),
                "away": fmt_avg(st_away, matches_away or matches_total),
            },
        )

        insights.setdefault(
            "shots_on_target_pct",
            {
                "total": fmt_pct(so_total, st_total),
                "home": fmt_pct(so_home, st_home),
                "away": fmt_pct(so_away, st_away),
            },
        )

    else:
        # ─────────────────────────────────────────
        # ② full_json 안에 shots 가 없거나 null 인 경우
        #    → match_team_stats + matches 를 이용해서 직접 계산
        # ─────────────────────────────────────────
        agg_rows = fetch_all(
            """
            SELECT
                m.fixture_id,
                m.home_id,
                m.away_id,
                MAX(
                    CASE
                        WHEN mts.name = 'Total Shots'
                        THEN COALESCE(NULLIF(mts.value, '')::int, 0)
                        ELSE 0
                    END
                ) AS total_shots,
                MAX(
                    CASE
                        WHEN mts.name = 'Shots on Goal'
                        THEN COALESCE(NULLIF(mts.value, '')::int, 0)
                        ELSE 0
                    END
                ) AS shots_on_goal
            FROM matches m
            JOIN match_team_stats mts
              ON mts.fixture_id = m.fixture_id
             AND mts.team_id   = %s
            WHERE m.league_id    = %s
              AND m.season       = %s
              AND m.status_group = 'FINISHED'
            GROUP BY m.fixture_id, m.home_id, m.away_id
            """,
            (team_id, league_id, season),
        )

        if agg_rows:
            home_matches = 0
            away_matches = 0

            total_shots_total = 0
            total_shots_home = 0
            total_shots_away = 0

            on_goal_total = 0
            on_goal_home = 0
            on_goal_away = 0

            for r in agg_rows:
                fixture_total = r["total_shots"] or 0
                fixture_on = r["shots_on_goal"] or 0

                is_home = r["home_id"] == team_id
                is_away = r["away_id"] == team_id

                if is_home:
                    home_matches += 1
                    total_shots_home += fixture_total
                    on_goal_home += fixture_on
                elif is_away:
                    away_matches += 1
                    total_shots_away += fixture_total
                    on_goal_away += fixture_on
                else:
                    # 이 팀이 아닌 행이면 스킵
                    continue

                total_shots_total += fixture_total
                on_goal_total += fixture_on

            # fixtures.played 가 0 이면, match_team_stats 기준으로 보정
            eff_total = matches_total or (home_matches + away_matches)
            eff_home = matches_home or (home_matches or eff_total)
            eff_away = matches_away or (away_matches or eff_total)

            def fmt_avg2(n, m):
                v = safe_div(n, m)
                return round(v, 2) if v > 0 else 0.0

            def fmt_pct2(n, d):
                v = safe_div(n, d)
                return int(round(v * 100)) if v > 0 else 0

            insights["shots_per_match"] = {
                "total": fmt_avg2(total_shots_total, eff_total),
                "home": fmt_avg2(total_shots_home, eff_home),
                "away": fmt_avg2(total_shots_away, eff_away),
            }

            insights["shots_on_target_pct"] = {
                "total": fmt_pct2(on_goal_total, total_shots_total),
                "home": fmt_pct2(on_goal_home, total_shots_home),
                "away": fmt_pct2(on_goal_away, total_shots_away),
            }

    # ─────────────────────────────────────────
    # 필요시 다른 고급 지표도 여기에서 추가/보정 가능
    # (예: xG 기반 효율, 득점 분포 기반 지표 등)
    # ─────────────────────────────────────────

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
