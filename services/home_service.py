from __future__ import annotations

import json
from datetime import datetime, date as date_cls
from typing import Any, Dict, List, Optional

from db import fetch_all

from .insights.insights_overall_timing import enrich_overall_timing
from .insights.insights_overall_firstgoal_momentum import enrich_overall_firstgoal_momentum


# ─────────────────────────────────────
#  공통: 날짜 파싱/정규화
# ─────────────────────────────────────

def _normalize_date(date_str: Optional[str]) -> str:
    """
    다양한 형태(YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS 등)의 문자열을
    안전하게 'YYYY-MM-DD' 로 정규화한다.
    """
    if not date_str:
        # 오늘 날짜
        return datetime.utcnow().date().isoformat()

    if isinstance(date_str, date_cls):
        return date_str.isoformat()

    try:
        dt = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        return datetime.utcnow().date().isoformat()


# ─────────────────────────────────────
#  1) 홈 화면: 리그 목록
# ─────────────────────────────────────

def get_home_leagues(date_str: Optional[str], league_ids: Optional[List[int]] = None) -> List[Dict[str, Any]]:
    """
    주어진 날짜(date_str)에 실제 경기가 편성된 리그 목록을 돌려준다.
    league_ids 가 주어지면 해당 리그들만 필터링.
    """
    norm_date = _normalize_date(date_str)

    params: List[Any] = [norm_date]
    where_clause = "m.date_utc::date = %s"

    if league_ids:
        placeholders = ", ".join(["%s"] * len(league_ids))
        where_clause += f" AND m.league_id IN ({placeholders})"
        params.extend(league_ids)

    rows = fetch_all(
        f"""
        SELECT
            m.league_id,
            l.name       AS league_name,
            l.country    AS country,
            l.logo       AS league_logo,
            m.season
        FROM matches m
        JOIN leagues l
          ON m.league_id = l.league_id
        WHERE {where_clause}
        GROUP BY
            m.league_id,
            l.name,
            l.country,
            l.logo,
            m.season
        ORDER BY
            l.country,
            l.name
        """,
        tuple(params),
    )

    result: List[Dict[str, Any]] = []
    for r in rows:
        result.append(
            {
                "league_id": r["league_id"],
                "name": r["league_name"],
                "country": r["country"],
                "logo": r["league_logo"],
                "season": r["season"],
            }
        )
    return result


# ─────────────────────────────────────
#  2) 홈 화면: 특정 리그의 매치 디렉터리
# ─────────────────────────────────────

def get_home_league_directory(league_id: int, date_str: Optional[str]) -> Dict[str, Any]:
    """
    특정 리그의 주어진 날짜(date_str)에 대한 매치 디렉터리 정보.
    """
    norm_date = _normalize_date(date_str)

    rows = fetch_all(
        """
        SELECT
            m.fixture_id,
            m.league_id,
            m.season,
            m.round,
            m.date_utc,
            m.status_short,
            m.status_group,
            m.home_id,
            th.name   AS home_name,
            th.logo   AS home_logo,
            m.away_id,
            ta.name   AS away_name,
            ta.logo   AS away_logo,
            m.home_ft,
            m.away_ft
        FROM matches m
        JOIN teams th ON th.id = m.home_id
        JOIN teams ta ON ta.id = m.away_id
        WHERE m.league_id = %s
          AND m.date_utc::date = %s
        ORDER BY m.date_utc ASC, m.fixture_id ASC
        """,
        (league_id, norm_date),
    )

    fixtures: List[Dict[str, Any]] = []
    season: Optional[int] = None
    round_name: Optional[str] = None

    for r in rows:
        season = season or r["season"]
        round_name = round_name or r["round"]

        fixtures.append(
            {
                "fixture_id": r["fixture_id"],
                "league_id": r["league_id"],
                "season": r["season"],
                "round": r["round"],
                "date_utc": r["date_utc"].isoformat() if r["date_utc"] else None,
                "status_short": r["status_short"],
                "status_group": r["status_group"],
                "home": {
                    "id": r["home_id"],
                    "name": r["home_name"],
                    "logo": r["home_logo"],
                    "goals": r["home_ft"],
                },
                "away": {
                    "id": r["away_id"],
                    "name": r["away_name"],
                    "logo": r["away_logo"],
                    "goals": r["away_ft"],
                },
            }
        )

    return {
        "league_id": league_id,
        "date": norm_date,
        "season": season,
        "round": round_name,
        "fixtures": fixtures,
    }


# ─────────────────────────────────────
#  3) 다음/이전 매치데이
# ─────────────────────────────────────

def _find_matchday(date_str: str, league_id: Optional[int], direction: str) -> Optional[str]:
    """
    direction: 'next' or 'prev'
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
        if direction == "next":
            if md > target and (nearest is None or md < nearest):
                nearest = md
        else:
            if md < target and (nearest is None or md > nearest):
                nearest = md

    if not nearest:
        return None
    return nearest.isoformat()


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

    insights = stats.get("insights_overall")
    if not isinstance(insights, dict):
        insights = {}
        stats["insights_overall"] = insights

    fixtures = stats.get("fixtures") or {}
    played = fixtures.get("played") or {}
    matches_total_api = played.get("total") or 0

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

            for r in shot_rows:
                home_id = r["home_id"]
                away_id = r["away_id"]
                is_home = (home_id == team_id)
                is_away = (away_id == team_id)
                if not (is_home or is_away):
                    continue

                total_shots = r["total_shots"] or 0
                sog = r["shots_on_goal"] or 0

                if total_shots <= 0 and sog <= 0:
                    continue

                total_matches += 1
                total_shots_total += total_shots
                sog_total += sog

                if is_home:
                    home_matches += 1
                    total_shots_home += total_shots
                    sog_home += sog
                else:
                    away_matches += 1
                    total_shots_away += total_shots
                    sog_away += sog

            if total_matches > 0:
                eff_total = matches_total_api or total_matches or 0
                eff_home = home_matches or 0
                eff_away = away_matches or 0

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
                    "total": fmt_pct(sog_total, total_shots_total) if total_shots_total > 0 else 0,
                    "home": fmt_pct(sog_home, total_shots_home) if total_shots_home > 0 else 0,
                    "away": fmt_pct(sog_away, total_shots_away) if total_shots_away > 0 else 0,
                }

    # ─────────────────────────────
    # Outcome & Totals / Result Combos / Goal Diff / Clean sheet / No goals
    # ─────────────────────────────
    standings_rows = fetch_all(
        """
        SELECT
            s.rank,
            s.team_id,
            s.group_name,
            s.played,
            s.win,
            s.draw,
            s.lose,
            s.goals_for,
            s.goals_against
        FROM standings s
        WHERE s.league_id = %s
          AND s.season    = %s
        """,
        (league_id, season_int),
    ) if season_int is not None else []

    if standings_rows:
        mt_tot = 0
        mh_tot = 0
        ma_tot = 0

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

        gf_sum_t = gf_sum_h = gf_sum_a = 0
        ga_sum_t = ga_sum_h = ga_sum_a = 0

        for r in standings_rows:
            if r["team_id"] != team_id:
                continue

            played_total = r["played"] or 0
            win = r["win"] or 0
            draw = r["draw"] or 0
            lose = r["lose"] or 0
            gf = r["goals_for"] or 0
            ga = r["goals_against"] or 0

            mt_tot += played_total
            win_t += win
            draw_t += draw
            lose_t += lose
            gf_sum_t += gf
            ga_sum_t += ga

        if mt_tot > 0:
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

            goal_diff_avg = safe_div(gf_sum_t - ga_sum_t, mt_tot)
            insights.setdefault(
                "goal_diff_avg",
                {
                    "total": round(goal_diff_avg, 2),
                    "home": 0.0,
                    "away": 0.0,
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

    # ─────────────────────────────
    # Goals by Time (For / Against)
    # ─────────────────────────────
    if season_int is not None:
        goal_rows = fetch_all(
            """
            SELECT
                m.fixture_id,
                m.home_id,
                m.away_id,
                e.minute,
                e.team_id
            FROM matches m
            JOIN match_events e
              ON e.fixture_id = m.fixture_id
            WHERE m.league_id = %s
              AND m.season    = %s
              AND (
                    lower(m.status_group) IN ('finished','ft','fulltime')
                 OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
              )
              AND e.type = 'Goal'
              AND e.minute IS NOT NULL
            """,
            (league_id, season_int),
        )

        if goal_rows:
            for_buckets = [0] * 10
            against_buckets = [0] * 10

            for gr in goal_rows:
                minute = gr["minute"] or 0
                try:
                    minute_int = int(minute)
                except (TypeError, ValueError):
                    continue
                if minute_int < 0:
                    continue

                if minute_int >= 90:
                    idx = 9
                else:
                    idx = minute_int // 10
                if idx < 0:
                    idx = 0
                if idx > 9:
                    idx = 9

                is_for = gr["team_id"] == team_id
                if is_for:
                    for_buckets[idx] += 1
                else:
                    against_buckets[idx] += 1

            insights["goals_by_time_for"] = for_buckets
            insights["goals_by_time_against"] = against_buckets

    # ─────────────────────────────
    # Discipline & Set Pieces (per match 평균)
    # ─────────────────────────────
    if season_int is not None:
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

            eff_tot = tot_matches or matches_total_api or 0
            eff_home = home_matches or 0
            eff_away = away_matches or 0

            def avg_for(v_t, v_h, v_a, d_t, d_h, d_a):
                return (
                    fmt_avg(v_t, d_t) if d_t > 0 else 0.0,
                    fmt_avg(v_h, d_h) if d_h > 0 else 0.0,
                    fmt_avg(v_a, d_a) if d_a > 0 else 0.0,
                )

            if eff_tot > 0:
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


    # ─────────────────────────────
    # 추가 Insights (Timing / First Goal & Momentum)
    # ─────────────────────────────
    if season_int is not None:
        try:
            enrich_overall_timing(
                stats=stats,
                insights=insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
            )
            enrich_overall_firstgoal_momentum(
                stats=stats,
                insights=insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
            )
        except Exception:
            # 일부 인사이트 계산이 실패해도 기본 값은 그대로 둔다.
            pass

    # 최종 반환
    return {
        "league_id": row["league_id"],
        "season": row["season"],
        "team_id": row["team_id"],
        "name": row.get("name"),
        "value": stats,
    }


# ─────────────────────────────────────
#  5) 팀 기본 정보
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
