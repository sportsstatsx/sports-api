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
    가장 최신 season 한 줄을 가져오고, 거기에 insights_overall.* 지표를
    추가/보정해서 반환한다.
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

    # value(JSON)를 파싱
    raw_value = row.get("value")
    if isinstance(raw_value, str):
        try:
            stats = json.loads(raw_value)
        except Exception:
            stats = {}
    elif isinstance(raw_value, dict):
        stats = raw_value
    else:
        stats = {}

    if not isinstance(stats, dict):
        stats = {}

    # ------------------------------------------------------------------
    # insights_overall 보장
    # ------------------------------------------------------------------
    insights = stats.get("insights_overall")
    if not isinstance(insights, dict):
        insights = {}
        stats["insights_overall"] = insights

    # ------------------------------------------------------------------
    # 공통 유틸
    # ------------------------------------------------------------------
    fixtures = stats.get("fixtures") or {}
    played = fixtures.get("played") or {}

    matches_total_api = played.get("total") or 0
    matches_home_api = played.get("home") or 0
    matches_away_api = played.get("away") or 0

    def safe_div(num, den) -> float:
        try:
            num_f = float(num)
        except (TypeError, ValueError):
            return 0.0
        try:
            den_f = float(den)
        except (TypeError, ValueError):
            return 0.0
        if den_f == 0:
            return 0.0
        return num_f / den_f

    # ------------------------------------------------------------------
    # 1) Shots / Shots on Target (기존 로직 유지)
    # ------------------------------------------------------------------
    shots = stats.get("shots") or {}
    if isinstance(shots, dict) and matches_total_api:
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
            # 경기당 슛 수 → 소수 2자리 정도
            return round(v, 2) if v > 0 else 0.0

        def fmt_pct(n, d):
            v = safe_div(n, d)
            # % 값은 정수로
            return int(round(v * 100)) if v > 0 else 0

        # 이미 값이 있으면 유지, 없으면 채움
        insights.setdefault(
            "shots_per_match",
            {
                "total": fmt_avg(st_total, matches_total_api),
                "home": fmt_avg(st_home, matches_home_api or matches_total_api),
                "away": fmt_avg(st_away, matches_away_api or matches_total_api),
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

    # ------------------------------------------------------------------
    # 2) Outcome & Totals / Result Combos – 경기 테이블 기반 계산
    # ------------------------------------------------------------------
    season = row.get("season")
    try:
        season_int = int(season)
    except (TypeError, ValueError):
        season_int = None

    if season_int is not None:
        match_rows = fetch_all(
            """
            SELECT
                m.fixture_id,
                m.home_id,
                m.away_id,
                m.home_ft,
                m.away_ft,
                m.status_group
            FROM matches m
            WHERE m.league_id = %s
              AND m.season    = %s
              AND (%s = m.home_id OR %s = m.away_id)
              AND (
                    lower(m.status_group) IN ('finished', 'ft', 'fulltime')
                 OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
              )
            """,
            (league_id, season_int, team_id, team_id),
        )

        # 카운터들
        mt_tot = mh_tot = ma_tot = 0  # 전체/홈/원정 경기 수

        win_t = win_h = win_a = 0
        draw_t = draw_h = draw_a = 0
        lose_t = lose_h = lose_a = 0

        btts_t = btts_h = btts_a = 0
        team_o05_t = team_o05_h = team_o05_a = 0
        team_o15_t = team_o15_h = team_o15_a = 0
        o15_t = o15_h = o15_a = 0
        o25_t = o25_h = o25_a = 0
        win_o25_t = win_o25_h = win_o25_a = 0
        lose_btts_t = lose_btts_h = lose_btts_a = 0

        cs_t = cs_h = cs_a = 0
        ng_t = ng_h = ng_a = 0

        gf_sum_t = gf_sum_h = gf_sum_a = 0.0
        ga_sum_t = ga_sum_h = ga_sum_a = 0.0

        for mr in match_rows:
            home_id = mr["home_id"]
            away_id = mr["away_id"]
            home_ft = mr["home_ft"]
            away_ft = mr["away_ft"]

            if home_ft is None or away_ft is None:
                continue

            is_home = (team_id == home_id)
            gf = home_ft if is_home else away_ft
            ga = away_ft if is_home else home_ft

            if gf is None or ga is None:
                continue

            # 경기 수
            mt_tot += 1
            if is_home:
                mh_tot += 1
            else:
                ma_tot += 1

            # 승/무/패
            if gf > ga:
                win_t += 1
                if is_home:
                    win_h += 1
                else:
                    win_a += 1
            elif gf == ga:
                draw_t += 1
                if is_home:
                    draw_h += 1
                else:
                    draw_a += 1
            else:
                lose_t += 1
                if is_home:
                    lose_h += 1
                else:
                    lose_a += 1

            # 득점/실점 합계 (Goal Diff 평균용)
            gf_sum_t += gf
            ga_sum_t += ga
            if is_home:
                gf_sum_h += gf
                ga_sum_h += ga
            else:
                gf_sum_a += gf
                ga_sum_a += ga

            # BTTS
            if gf > 0 and ga > 0:
                btts_t += 1
                if is_home:
                    btts_h += 1
                else:
                    btts_a += 1

            # 팀 득점 기준
            if gf >= 1:
                team_o05_t += 1
                if is_home:
                    team_o05_h += 1
                else:
                    team_o05_a += 1
            if gf >= 2:
                team_o15_t += 1
                if is_home:
                    team_o15_h += 1
                else:
                    team_o15_a += 1

            # 전체 득점 기준
            total_goals = gf + ga
            if total_goals >= 2:
                o15_t += 1
                if is_home:
                    o15_h += 1
                else:
                    o15_a += 1
            if total_goals >= 3:
                o25_t += 1
                if is_home:
                    o25_h += 1
                else:
                    o25_a += 1

            # Win & Over 2.5
            if gf > ga and total_goals >= 3:
                win_o25_t += 1
                if is_home:
                    win_o25_h += 1
                else:
                    win_o25_a += 1

            # Lose & BTTS
            if gf < ga and gf > 0 and ga > 0:
                lose_btts_t += 1
                if is_home:
                    lose_btts_h += 1
                else:
                    lose_btts_a += 1

            # 클린시트 / 노골
            if ga == 0:
                cs_t += 1
                if is_home:
                    cs_h += 1
                else:
                    cs_a += 1
            if gf == 0:
                ng_t += 1
                if is_home:
                    ng_h += 1
                else:
                    ng_a += 1

        # 실제로 집계된 경기가 있을 때만 채운다
        if mt_tot > 0:
            def pct(n, d):
                v = safe_div(n, d)
                return int(round(v * 100)) if v > 0 else 0

            def avg(n, d):
                v = safe_div(n, d)
                return round(v, 2) if v > 0 else 0.0

            # Outcome & Totals
            insights.setdefault(
                "win_pct",
                {
                    "total": pct(win_t, mt_tot),
                    "home": pct(win_h, mh_tot or mt_tot),
                    "away": pct(win_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "btts_pct",
                {
                    "total": pct(btts_t, mt_tot),
                    "home": pct(btts_h, mh_tot or mt_tot),
                    "away": pct(btts_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "team_over05_pct",
                {
                    "total": pct(team_o05_t, mt_tot),
                    "home": pct(team_o05_h, mh_tot or mt_tot),
                    "away": pct(team_o05_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "team_over15_pct",
                {
                    "total": pct(team_o15_t, mt_tot),
                    "home": pct(team_o15_h, mh_tot or mt_tot),
                    "away": pct(team_o15_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "over15_pct",
                {
                    "total": pct(o15_t, mt_tot),
                    "home": pct(o15_h, mh_tot or mt_tot),
                    "away": pct(o15_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "over25_pct",
                {
                    "total": pct(o25_t, mt_tot),
                    "home": pct(o25_h, mh_tot or mt_tot),
                    "away": pct(o25_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "clean_sheet_pct",
                {
                    "total": pct(cs_t, mt_tot),
                    "home": pct(cs_h, mh_tot or mt_tot),
                    "away": pct(cs_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "no_goals_pct",
                {
                    "total": pct(ng_t, mt_tot),
                    "home": pct(ng_h, mh_tot or mt_tot),
                    "away": pct(ng_a, ma_tot or mt_tot),
                },
            )

            # Result Combos & Draw / Goal Diff
            insights.setdefault(
                "win_and_over25_pct",
                {
                    "total": pct(win_o25_t, mt_tot),
                    "home": pct(win_o25_h, mh_tot or mt_tot),
                    "away": pct(win_o25_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "lose_and_btts_pct",
                {
                    "total": pct(lose_btts_t, mt_tot),
                    "home": pct(lose_btts_h, mh_tot or mt_tot),
                    "away": pct(lose_btts_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "draw_pct",
                {
                    "total": pct(draw_t, mt_tot),
                    "home": pct(draw_h, mh_tot or mt_tot),
                    "away": pct(draw_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "goal_diff_avg",
                {
                    "total": avg(gf_sum_t - ga_sum_t, mt_tot),
                    "home": avg(gf_sum_h - ga_sum_h, mh_tot or mt_tot),
                    "away": avg(gf_sum_a - ga_sum_a, ma_tot or mt_tot),
                },
            )

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
