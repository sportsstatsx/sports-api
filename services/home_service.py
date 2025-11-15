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

    match_date = rows[0]["match_date"]
    return str(match_date)


# ─────────────────────────────────────
#  4) Insights Overall용 고급 지표 계산
#     (서버에서 matches 테이블을 보고 계산)
# ─────────────────────────────────────

def _pct(count: int, base: int) -> int:
    if base <= 0 or count <= 0:
        return 0
    return int(round(count * 100.0 / base))


def _safe_div(n: float, d: int) -> float:
    if d <= 0:
        return 0.0
    return n / d


def _compute_overall_insights(league_id: int, season: int, team_id: int) -> Dict[str, Any]:
    """
    matches 테이블을 기준으로 해당 팀의 리그/시즌 단위 고급 지표를 계산한다.

    - BTTS
    - Team Over 0.5 / 1.5
    - Over 1.5 / 2.5 (전체 득점)
    - Win & Over 2.5
    - Lose & BTTS
    - Draw %
    - Goal Difference Avg (총 득실차 평균)
    """

    rows = fetch_all(
        """
        SELECT
            home_id,
            away_id,
            home_ft,
            away_ft,
            status_group
        FROM matches
        WHERE league_id = %s
          AND season    = %s
          AND (home_id = %s OR away_id = %s)
        """,
        (league_id, season, team_id, team_id),
    )

    played_total = 0
    played_home = 0
    played_away = 0

    # Outcome
    win_total = win_home = win_away = 0
    draw_total = draw_home = draw_away = 0
    lose_total = lose_home = lose_away = 0

    # BTTS
    btts_total = btts_home = btts_away = 0

    # Team Over
    team_o05_total = team_o05_home = team_o05_away = 0
    team_o15_total = team_o15_home = team_o15_away = 0

    # Match Over
    o15_total = o15_home = o15_away = 0
    o25_total = o25_home = o25_away = 0

    # Result combos
    win_o25_total = win_o25_home = win_o25_away = 0
    lose_btts_total = lose_btts_home = lose_btts_away = 0

    # Goal diff
    gd_sum_total = 0.0
    gd_sum_home = 0.0
    gd_sum_away = 0.0

    for r in rows:
        home_id = r["home_id"]
        away_id = r["away_id"]
        hf = r["home_ft"]
        af = r["away_ft"]

        # 스코어가 없는 경기(미종료 등)는 제외
        if hf is None or af is None:
            continue

        is_home = (home_id == team_id)
        gf = hf if is_home else af
        ga = af if is_home else hf
        total_goals = (hf or 0) + (af or 0)

        played_total += 1
        if is_home:
            played_home += 1
        else:
            played_away += 1

        # Outcome
        if gf > ga:
            win_total += 1
            if is_home:
                win_home += 1
            else:
                win_away += 1
        elif gf == ga:
            draw_total += 1
            if is_home:
                draw_home += 1
            else:
                draw_away += 1
        else:
            lose_total += 1
            if is_home:
                lose_home += 1
            else:
                lose_away += 1

        # BTTS
        if hf > 0 and af > 0:
            btts_total += 1
            if is_home:
                btts_home += 1
            else:
                btts_away += 1

        # Team Over 0.5 / 1.5
        if gf >= 1:
            team_o05_total += 1
            if is_home:
                team_o05_home += 1
            else:
                team_o05_away += 1
        if gf >= 2:
            team_o15_total += 1
            if is_home:
                team_o15_home += 1
            else:
                team_o15_away += 1

        # Match Over 1.5 / 2.5
        if total_goals >= 2:
            o15_total += 1
            if is_home:
                o15_home += 1
            else:
                o15_away += 1
        if total_goals >= 3:
            o25_total += 1
            if is_home:
                o25_home += 1
            else:
                o25_away += 1

        # Win & Over 2.5
        if gf > ga and total_goals >= 3:
            win_o25_total += 1
            if is_home:
                win_o25_home += 1
            else:
                win_o25_away += 1

        # Lose & BTTS
        if gf < ga and hf > 0 and af > 0:
            lose_btts_total += 1
            if is_home:
                lose_btts_home += 1
            else:
                lose_btts_away += 1

        # Goal diff
        gd = float(gf - ga)
        gd_sum_total += gd
        if is_home:
            gd_sum_home += gd
        else:
            gd_sum_away += gd

    if played_total == 0:
        return {}

    insights = {
        "samples": {
            "matches_total": played_total,
            "matches_home": played_home,
            "matches_away": played_away,
        },
        # BTTS %
        "btts_pct": {
            "total": _pct(btts_total, played_total),
            "home": _pct(btts_home, played_home),
            "away": _pct(btts_away, played_away),
        },
        # Team Over 0.5 / 1.5
        "team_over05_pct": {
            "total": _pct(team_o05_total, played_total),
            "home": _pct(team_o05_home, played_home),
            "away": _pct(team_o05_away, played_away),
        },
        "team_over15_pct": {
            "total": _pct(team_o15_total, played_total),
            "home": _pct(team_o15_home, played_home),
            "away": _pct(team_o15_away, played_away),
        },
        # Over 1.5 / 2.5 (전체 득점 기준)
        "over15_pct": {
            "total": _pct(o15_total, played_total),
            "home": _pct(o15_home, played_home),
            "away": _pct(o15_away, played_away),
        },
        "over25_pct": {
            "total": _pct(o25_total, played_total),
            "home": _pct(o25_home, played_home),
            "away": _pct(o25_away, played_away),
        },
        # Win & Over 2.5
        "win_and_over25_pct": {
            "total": _pct(win_o25_total, played_total),
            "home": _pct(win_o25_home, played_home),
            "away": _pct(win_o25_away, played_away),
        },
        # Lose & BTTS
        "lose_and_btts_pct": {
            "total": _pct(lose_btts_total, played_total),
            "home": _pct(lose_btts_home, played_home),
            "away": _pct(lose_btts_away, played_away),
        },
        # Draw %
        "draw_pct": {
            "total": _pct(draw_total, played_total),
            "home": _pct(draw_home, played_home),
            "away": _pct(draw_away, played_away),
        },
        # Goal Difference Avg
        "goal_diff_avg": {
            "total": round(_safe_div(gd_sum_total, played_total), 2),
            "home": round(_safe_div(gd_sum_home, played_home), 2),
            "away": round(_safe_div(gd_sum_away, played_away), 2),
        },
    }

    return insights


# ─────────────────────────────────────
#  5) 팀 시즌 스탯 (team_season_stats)
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
        "value": { ... 원본 JSON ..., "insights_overall": { ...계산된 지표... } }
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

    league_id_db = row["league_id"]
    season_db = row["season"]
    team_id_db = row["team_id"]
    value = row["value"]  # JSONB → 파이썬 dict 로 나옴 (psycopg 기준)

    if not isinstance(value, dict):
        value = {}

    # 서버에서 matches 테이블을 보고 고급 지표 계산
    insights = _compute_overall_insights(
        league_id=league_id_db,
        season=season_db,
        team_id=team_id_db,
    )

    if insights:
        # value 안에 insights_overall 블록으로 주입
        existing = value.get("insights_overall") or {}
        if isinstance(existing, dict):
            merged = {**existing, **insights}
        else:
            merged = insights
        value["insights_overall"] = merged

    return {
        "league_id": league_id_db,
        "season": season_db,
        "team_id": team_id_db,
        "name": row.get("name"),
        "value": value,
    }
