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
        return datetime.now().date().isoformat()

    s = date_str.strip()
    if len(s) >= 10:
        only_date = s[:10]
        try:
            dt = datetime.fromisoformat(only_date)
            return dt.date().isoformat()
        except Exception:
            return only_date
    return s


# ─────────────────────────────────────
#  1) 홈 상단 리그 탭
# ─────────────────────────────────────

def get_home_leagues(date_str: str) -> List[Dict[str, Any]]:
    """
    주어진 날짜(date_str)에 실제 경기가 편성된 리그 목록을 돌려준다.
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
    return str(match_date)


def get_next_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    return _find_matchday(date_str, league_id, direction="next")


def get_prev_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    return _find_matchday(date_str, league_id, direction="prev")


# ─────────────────────────────────────
#  4) 팀 시즌 스탯 + Insights Overall
# ─────────────────────────────────────

def get_team_season_stats(team_id: int, league_id: int) -> Optional[Dict[str, Any]]:
    """
    team_season_stats 테이블에서 (league_id, team_id)에 해당하는
    가장 최신 season 한 줄을 가져온 뒤, 서버에서만 계산 가능한
    insights_overall.* 값을 채워 넣는다.

    - shots_per_match
    - shots_on_target_pct
    - btts_pct
    - team_over05_pct
    - team_over15_pct
    - over15_pct
    - over25_pct
    - win_and_over25_pct
    - lose_and_btts_pct
    - goal_diff_avg
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

    # value(JSONB/TEXT) → dict
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

    def fmt_pct(n, d) -> int:
        v = safe_div(n, d)
        return int(round(v * 100)) if v > 0 else 0

    def fmt_avg(n, d) -> float:
        v = safe_div(n, d)
        return round(v, 2) if v > 0 else 0.0

    # ─────────────────────────────────────────
    # A. 슈팅 지표 (match_team_stats 기반)
    # ─────────────────────────────────────────
    shot_rows = fetch_all(
        """
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            MAX(
                CASE
                    WHEN mts.name IN ('Total Shots', 'Shots Total', 'Total shots', 'Shots')
                         AND mts.value ~ '^[0-9]+$'
                    THEN mts.value::int
                    ELSE 0
                END
            ) AS total_shots,
            MAX(
                CASE
                    WHEN mts.name IN (
                        'Shots on Goal', 'ShotsOnGoal',
                        'Shots on target', 'Shots on Target'
                    )
                    AND mts.value ~ '^[0-9]+$'
                    THEN mts.value::int
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

    if shot_rows:
        total_matches = 0
        home_matches = 0
        away_matches = 0

        total_shots_total = 0
        total_shots_home = 0
        total_shots_away = 0

        sog_total = 0
        sog_home = 0
        sog_away = 0

        for r2 in shot_rows:
            ts = r2["total_shots"] or 0
            sog = r2["shots_on_goal"] or 0

            is_home = (r2["home_id"] == team_id)
            is_away = (r2["away_id"] == team_id)
            if not (is_home or is_away):
                continue

            total_matches += 1
            total_shots_total += ts
            sog_total += sog

            if is_home:
                home_matches += 1
                total_shots_home += ts
                sog_home += sog
            else:
                away_matches += 1
                total_shots_away += ts
                sog_away += sog

        eff_total = matches_total or total_matches
        eff_home = matches_home or (home_matches or eff_total)
        eff_away = matches_away or (away_matches or eff_total)

        insights["shots_per_match"] = {
            "total": fmt_avg(total_shots_total, eff_total),
            "home": fmt_avg(total_shots_home, eff_home),
            "away": fmt_avg(total_shots_away, eff_away),
        }
        insights["shots_on_target_pct"] = {
            "total": fmt_pct(sog_total, total_shots_total),
            "home": fmt_pct(sog_home, total_shots_home),
            "away": fmt_pct(sog_away, total_shots_away),
        }

    # ─────────────────────────────────────────
    # B. Outcome & Totals 지표 (matches 기반)
    # ─────────────────────────────────────────
    outcome_rows = fetch_all(
        """
        SELECT
            COUNT(*)                           AS matches_total,
            SUM(CASE WHEN is_home THEN 1 ELSE 0 END) AS matches_home,
            SUM(CASE WHEN NOT is_home THEN 1 ELSE 0 END) AS matches_away,

            -- BTTS
            SUM(CASE WHEN gf > 0 AND ga > 0 THEN 1 ELSE 0 END) AS btts_total,
            SUM(CASE WHEN is_home AND gf > 0 AND ga > 0 THEN 1 ELSE 0 END) AS btts_home,
            SUM(CASE WHEN NOT is_home AND gf > 0 AND ga > 0 THEN 1 ELSE 0 END) AS btts_away,

            -- 팀 득점 기준
            SUM(CASE WHEN gf >= 1 THEN 1 ELSE 0 END) AS team_over05_total,
            SUM(CASE WHEN is_home AND gf >= 1 THEN 1 ELSE 0 END) AS team_over05_home,
            SUM(CASE WHEN NOT is_home AND gf >= 1 THEN 1 ELSE 0 END) AS team_over05_away,

            SUM(CASE WHEN gf >= 2 THEN 1 ELSE 0 END) AS team_over15_total,
            SUM(CASE WHEN is_home AND gf >= 2 THEN 1 ELSE 0 END) AS team_over15_home,
            SUM(CASE WHEN NOT is_home AND gf >= 2 THEN 1 ELSE 0 END) AS team_over15_away,

            -- 전체 득점 기준
            SUM(CASE WHEN (gf + ga) >= 2 THEN 1 ELSE 0 END) AS over15_total,
            SUM(CASE WHEN is_home AND (gf + ga) >= 2 THEN 1 ELSE 0 END) AS over15_home,
            SUM(CASE WHEN NOT is_home AND (gf + ga) >= 2 THEN 1 ELSE 0 END) AS over15_away,

            SUM(CASE WHEN (gf + ga) >= 3 THEN 1 ELSE 0 END) AS over25_total,
            SUM(CASE WHEN is_home AND (gf + ga) >= 3 THEN 1 ELSE 0 END) AS over25_home,
            SUM(CASE WHEN NOT is_home AND (gf + ga) >= 3 THEN 1 ELSE 0 END) AS over25_away,

            -- Win & Over 2.5
            SUM(
                CASE
                    WHEN gf > ga AND (gf + ga) >= 3
                    THEN 1 ELSE 0
                END
            ) AS win_over25_total,
            SUM(
                CASE
                    WHEN is_home AND gf > ga AND (gf + ga) >= 3
                    THEN 1 ELSE 0
                END
            ) AS win_over25_home,
            SUM(
                CASE
                    WHEN NOT is_home AND gf > ga AND (gf + ga) >= 3
                    THEN 1 ELSE 0
                END
            ) AS win_over25_away,

            -- Lose & BTTS
            SUM(
                CASE
                    WHEN gf < ga AND gf > 0 AND ga > 0
                    THEN 1 ELSE 0
                END
            ) AS lose_btts_total,
            SUM(
                CASE
                    WHEN is_home AND gf < ga AND gf > 0 AND ga > 0
                    THEN 1 ELSE 0
                END
            ) AS lose_btts_home,
            SUM(
                CASE
                    WHEN NOT is_home AND gf < ga AND gf > 0 AND ga > 0
                    THEN 1 ELSE 0
                END
            ) AS lose_btts_away,

            -- 골득실 합
            SUM(gf - ga) AS gd_total,
            SUM(CASE WHEN is_home THEN (gf - ga) ELSE 0 END) AS gd_home,
            SUM(CASE WHEN NOT is_home THEN (gf - ga) ELSE 0 END) AS gd_away
        FROM (
            SELECT
                m.fixture_id,
                CASE
                    WHEN m.home_id = %s THEN TRUE
                    WHEN m.away_id = %s THEN FALSE
                    ELSE NULL
                END AS is_home,
                CASE
                    WHEN m.home_id = %s THEN m.home_ft
                    WHEN m.away_id = %s THEN m.away_ft
                    ELSE NULL
                END AS gf,
                CASE
                    WHEN m.home_id = %s THEN m.away_ft
                    WHEN m.away_id = %s THEN m.home_ft
                    ELSE NULL
                END AS ga
            FROM matches m
            WHERE m.league_id    = %s
              AND m.season       = %s
              AND (m.home_id = %s OR m.away_id = %s)
              AND m.status_group = 'FINISHED'
        ) t
        WHERE gf IS NOT NULL AND ga IS NOT NULL
        """,
        (
            team_id, team_id,
            team_id, team_id,
            team_id, team_id,
            league_id, season,
            team_id, team_id,
        ),
    )

    if outcome_rows:
        o = outcome_rows[0]

        m_total = o.get("matches_total") or matches_total
        m_home = o.get("matches_home") or matches_home
        m_away = o.get("matches_away") or matches_away

        def row_pct(total_key: str, home_key: str, away_key: str) -> Dict[str, int]:
            t = o.get(total_key) or 0
            h = o.get(home_key) or 0
            a = o.get(away_key) or 0
            return {
                "total": fmt_pct(t, m_total),
                "home": fmt_pct(h, m_home or m_total),
                "away": fmt_pct(a, m_away or m_total),
            }

        # BTTS
        insights["btts_pct"] = row_pct(
            "btts_total", "btts_home", "btts_away"
        )

        # Team Over 0.5 / 1.5
        insights["team_over05_pct"] = row_pct(
            "team_over05_total", "team_over05_home", "team_over05_away"
        )
        insights["team_over15_pct"] = row_pct(
            "team_over15_total", "team_over15_home", "team_over15_away"
        )

        # Over 1.5 / 2.5
        insights["over15_pct"] = row_pct(
            "over15_total", "over15_home", "over15_away"
        )
        insights["over25_pct"] = row_pct(
            "over25_total", "over25_home", "over25_away"
        )

        # Win & Over 2.5
        insights["win_and_over25_pct"] = row_pct(
            "win_over25_total", "win_over25_home", "win_over25_away"
        )

        # Lose & BTTS
        insights["lose_and_btts_pct"] = row_pct(
            "lose_btts_total", "lose_btts_home", "lose_btts_away"
        )

        # 골득실 평균
        gd_total = o.get("gd_total") or 0
        gd_home = o.get("gd_home") or 0
        gd_away = o.get("gd_away") or 0
        insights["goal_diff_avg"] = {
            "total": fmt_avg(gd_total, m_total),
            "home": fmt_avg(gd_home, m_home or m_total),
            "away": fmt_avg(gd_away, m_away or m_total),
        }

    # 최종 반환
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
