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

    # insights_overall 보장
    insights = stats.get("insights_overall")
    if not isinstance(insights, dict):
        insights = {}
        stats["insights_overall"] = insights

    fixtures = stats.get("fixtures") or {}
    played = fixtures.get("played") or {}
    matches_total_api = played.get("total") or 0

    # 공통 유틸
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

    def fmt_pct(n, d) -> int:
        v = safe_div(n, d)
        return int(round(v * 100)) if v > 0 else 0

    def fmt_avg(n, d) -> float:
        v = safe_div(n, d)
        return round(v, 2) if v > 0 else 0.0

    # 시즌
    season = row.get("season")
    try:
        season_int = int(season)
    except (TypeError, ValueError):
        season_int = None

    # ─────────────────────────────
    # Shooting & Efficiency (Shots)
    # ─────────────────────────────
    if season_int is not None:
        shot_rows = fetch_all(
            """
            SELECT
                m.fixture_id,
                m.home_id,
                m.away_id,
                SUM(
                    CASE
                        WHEN lower(mts.name) IN ('total shots','shots total','shots')
                             AND mts.value ~ '^[0-9]+$'
                        THEN mts.value::int
                        ELSE 0
                    END
                ) AS total_shots,
                SUM(
                    CASE
                        WHEN lower(mts.name) IN (
                            'shots on goal',
                            'shotsongoal',
                            'shots on target'
                        )
                        AND mts.value ~ '^[0-9]+$'
                        THEN mts.value::int
                        ELSE 0
                    END
                ) AS shots_on_goal
            FROM matches m
            LEFT JOIN match_team_stats mts
              ON mts.fixture_id = m.fixture_id
             AND mts.team_id   = %s
            WHERE m.league_id = %s
              AND m.season    = %s
              AND (%s = m.home_id OR %s = m.away_id)
              AND (
                    lower(m.status_group) IN ('finished','ft','fulltime')
                 OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
              )
            GROUP BY m.fixture_id, m.home_id, m.away_id
            """,
            (team_id, league_id, season_int, team_id, team_id),
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

            # API 쪽 fixtures.played 값이 없으면 실제 경기 수 사용
            eff_total = matches_total_api or total_matches or 0
            eff_home = home_matches or 0
            eff_away = away_matches or 0

            # shots 블록도 같이 기록 (나중에 재사용 가능)
            stats["shots"] = {
                "total": {
                    "total": int(total_shots_total),
                    "home": int(total_shots_home),
                    "away": int(total_shots_away),
                },
                "on": {
                    "total": int(sog_total),
                    "home": int(sog_home),
                    "away": int(sog_away),
                },
            }

            avg_total = fmt_avg(total_shots_total, eff_total) if eff_total > 0 else 0.0
            avg_home = fmt_avg(total_shots_home, eff_home) if eff_home > 0 else 0.0
            avg_away = fmt_avg(total_shots_away, eff_away) if eff_away > 0 else 0.0

            insights["shots_per_match"] = {
                "total": avg_total,
                "home": avg_home,
                "away": avg_away,
            }
            insights["shots_on_target_pct"] = {
                "total": fmt_pct(sog_total, total_shots_total),
                "home": fmt_pct(sog_home, total_shots_home),
                "away": fmt_pct(sog_away, total_shots_away),
            }

    # ─────────────────────────────
    # Outcome & Totals / Result Combos
    # + Goals by Time / Discipline & Set Pieces (서버 DB 버전)
    # ─────────────────────────────
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
                    lower(m.status_group) IN ('finished','ft','fulltime')
                 OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
              )
            """,
            (league_id, season_int, team_id, team_id),
        )

        mt_tot = mh_tot = ma_tot = 0

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

            mt_tot += 1
            if is_home:
                mh_tot += 1
            else:
                ma_tot += 1

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

            gf_sum_t += gf
            ga_sum_t += ga
            if is_home:
                gf_sum_h += gf
                ga_sum_h += ga
            else:
                gf_sum_a += gf
                ga_sum_a += ga

            if gf > 0 and ga > 0:
                btts_t += 1
                if is_home:
                    btts_h += 1
                else:
                    btts_a += 1

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

            if gf > ga and total_goals >= 3:
                win_o25_t += 1
                if is_home:
                    win_o25_h += 1
                else:
                    win_o25_a += 1

            if gf < ga and gf > 0 and ga > 0:
                lose_btts_t += 1
                if is_home:
                    lose_btts_h += 1
                else:
                    lose_btts_a += 1

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

        if mt_tot > 0:
            # Outcome & Totals / Result Combos
            insights.setdefault(
                "win_pct",
                {
                    "total": fmt_pct(win_t, mt_tot),
                    "home": fmt_pct(win_h, mh_tot or mt_tot),
                    "away": fmt_pct(win_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "btts_pct",
                {
                    "total": fmt_pct(btts_t, mt_tot),
                    "home": fmt_pct(btts_h, mh_tot or mt_tot),
                    "away": fmt_pct(btts_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "team_over05_pct",
                {
                    "total": fmt_pct(team_o05_t, mt_tot),
                    "home": fmt_pct(team_o05_h, mh_tot or mt_tot),
                    "away": fmt_pct(team_o05_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "team_over15_pct",
                {
                    "total": fmt_pct(team_o15_t, mt_tot),
                    "home": fmt_pct(team_o15_h, mh_tot or mt_tot),
                    "away": fmt_pct(team_o15_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "over15_pct",
                {
                    "total": fmt_pct(o15_t, mt_tot),
                    "home": fmt_pct(o15_h, mh_tot or mt_tot),
                    "away": fmt_pct(o15_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "over25_pct",
                {
                    "total": fmt_pct(o25_t, mt_tot),
                    "home": fmt_pct(o25_h, mh_tot or mt_tot),
                    "away": fmt_pct(o25_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "clean_sheet_pct",
                {
                    "total": fmt_pct(cs_t, mt_tot),
                    "home": fmt_pct(cs_h, mh_tot or mt_tot),
                    "away": fmt_pct(cs_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "no_goals_pct",
                {
                    "total": fmt_pct(ng_t, mt_tot),
                    "home": fmt_pct(ng_h, mh_tot or mt_tot),
                    "away": fmt_pct(ng_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "win_and_over25_pct",
                {
                    "total": fmt_pct(win_o25_t, mt_tot),
                    "home": fmt_pct(win_o25_h, mh_tot or mt_tot),
                    "away": fmt_pct(win_o25_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "lose_and_btts_pct",
                {
                    "total": fmt_pct(lose_btts_t, mt_tot),
                    "home": fmt_pct(lose_btts_h, mh_tot or mt_tot),
                    "away": fmt_pct(lose_btts_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "draw_pct",
                {
                    "total": fmt_pct(draw_t, mt_tot),
                    "home": fmt_pct(draw_h, mh_tot or mt_tot),
                    "away": fmt_pct(draw_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "goal_diff_avg",
                {
                    "total": fmt_avg(gf_sum_t - ga_sum_t, mt_tot),
                    "home": fmt_avg(gf_sum_h - ga_sum_h, mh_tot or mt_tot),
                    "away": fmt_avg(gf_sum_a - ga_sum_a, ma_tot or mt_tot),
                },
            )

            # ─────────────────────────────
            # Goals by Time (서버 DB 기반)
            # ─────────────────────────────
            goal_rows = fetch_all(
                """
                SELECT
                    e.fixture_id,
                    e.minute,
                    e.team_id,
                    m.home_id,
                    m.away_id
                FROM matches m
                JOIN match_events e
                  ON e.fixture_id = m.fixture_id
                WHERE m.league_id = %s
                  AND m.season    = %s
                  AND (%s = m.home_id OR %s = m.away_id)
                  AND lower(e.type) = 'goal'
                  AND e.minute IS NOT NULL
                """,
                (league_id, season_int, team_id, team_id),
            )

            if goal_rows:
                for_buckets = [0] * 10
                against_buckets = [0] * 10

                def bucket_index(minute: int) -> int:
                    if minute < 10:
                        return 0
                    if minute < 20:
                        return 1
                    if minute < 30:
                        return 2
                    if minute < 40:
                        return 3
                    if minute < 45:
                        return 4
                    if minute < 50:
                        return 5
                    if minute < 60:
                        return 6
                    if minute < 70:
                        return 7
                    if minute < 80:
                        return 8
                    return 9

                for gr in goal_rows:
                    minute = gr["minute"]
                    if minute is None:
                        continue
                    try:
                        m_val = int(minute)
                    except (TypeError, ValueError):
                        continue

                    idx = bucket_index(m_val)
                    is_for = (gr["team_id"] == team_id)
                    if is_for:
                        for_buckets[idx] += 1
                    else:
                        against_buckets[idx] += 1

                insights["goals_by_time_for"] = for_buckets
                insights["goals_by_time_against"] = against_buckets

            # ─────────────────────────────
            # Discipline & Set Pieces
            #  (코너/옐로/레드 per match – 서버 DB 기반)
            # ─────────────────────────────
            disc_rows = fetch_all(
                """
                SELECT
                    m.fixture_id,
                    m.home_id,
                    m.away_id,
                    SUM(
                        CASE
                            WHEN lower(mts.name) LIKE 'corner%%'
                                 AND mts.value ~ '^[0-9]+$'
                            THEN mts.value::int
                            ELSE 0
                        END
                    ) AS corners,
                    SUM(
                        CASE
                            WHEN lower(mts.name) LIKE 'yellow%%'
                                 AND mts.value ~ '^[0-9]+$'
                            THEN mts.value::int
                            ELSE 0
                        END
                    ) AS yellows,
                    SUM(
                        CASE
                            WHEN lower(mts.name) LIKE 'red%%'
                                 AND mts.value ~ '^[0-9]+$'
                            THEN mts.value::int
                            ELSE 0
                        END
                    ) AS reds
                FROM matches m
                LEFT JOIN match_team_stats mts
                  ON mts.fixture_id = m.fixture_id
                 AND mts.team_id   = %s
                WHERE m.league_id = %s
                  AND m.season    = %s
                  AND (%s = m.home_id OR %s = m.away_id)
                  AND (
                        lower(m.status_group) IN ('finished','ft','fulltime')
                     OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
                  )
                GROUP BY m.fixture_id, m.home_id, m.away_id
                """,
                (team_id, league_id, season_int, team_id, team_id),
            )

            if disc_rows:
                tot_matches = 0
                home_matches = 0
                away_matches = 0

                sum_corners_t = sum_corners_h = sum_corners_a = 0
                sum_yellows_t = sum_yellows_h = sum_yellows_a = 0
                sum_reds_t = sum_reds_h = sum_reds_a = 0

                for dr in disc_rows:
                    home_id = dr["home_id"]
                    away_id = dr["away_id"]
                    is_home = (home_id == team_id)
                    is_away = (away_id == team_id)
                    if not (is_home or is_away):
                        continue

                    corners = dr["corners"] or 0
                    yellows = dr["yellows"] or 0
                    reds = dr["reds"] or 0

                    tot_matches += 1
                    sum_corners_t += corners
                    sum_yellows_t += yellows
                    sum_reds_t += reds

                    if is_home:
                        home_matches += 1
                        sum_corners_h += corners
                        sum_yellows_h += yellows
                        sum_reds_h += reds
                    else:
                        away_matches += 1
                        sum_corners_a += corners
                        sum_yellows_a += yellows
                        sum_reds_a += reds

                eff_tot = tot_matches or mt_tot
                eff_home = home_matches or mh_tot
                eff_away = away_matches or ma_tot

                def avg_for(v_t, v_h, v_a, d_t, d_h, d_a):
                    return (
                        fmt_avg(v_t, d_t) if d_t > 0 else 0.0,
                        fmt_avg(v_h, d_h) if d_h > 0 else 0.0,
                        fmt_avg(v_a, d_a) if d_a > 0 else 0.0,
                    )

                c_tot, c_h, c_a = avg_for(
                    sum_corners_t,
                    sum_corners_h,
                    sum_corners_a,
                    eff_tot,
                    eff_home,
                    eff_away,
                )
                y_tot, y_h, y_a = avg_for(
                    sum_yellows_t,
                    sum_yellows_h,
                    sum_yellows_a,
                    eff_tot,
                    eff_home,
                    eff_away,
                )
                r_tot, r_h, r_a = avg_for(
                    sum_reds_t,
                    sum_reds_h,
                    sum_reds_a,
                    eff_tot,
                    eff_home,
                    eff_away,
                )

                insights["corners_per_match"] = {
                    "total": c_tot,
                    "home": c_h,
                    "away": c_a,
                }
                insights["yellow_per_match"] = {
                    "total": y_tot,
                    "home": y_h,
                    "away": y_a,
                }
                insights["red_per_match"] = {
                    "total": r_tot,
                    "home": r_h,
                    "away": r_a,
                }

            # 🔴 Opp/Own red 이후 영향(샘플, 퍼센트, 평균 골)은
            #    일단 기존처럼 앱에서 0 / 빈 값으로 두고,
            #    나중에 필요하면 여기에서 match_events 기반으로 추가 계산 넣으면 됨.

    # 최종 반환
    return {
        "league_id": row["league_id"],
        "season": row["season"],
        "team_id": row["team_id"],
        "name": row.get("name"),
        "value": stats,
    }


# ─────────────────────────────────────
#  5) 팀 정보
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
