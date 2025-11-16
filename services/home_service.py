from __future__ import annotations

import json
from datetime import datetime, date as date_cls
from typing import Any, Dict, List, Optional

from db import fetch_all

from .insights.insights_overall_timing import enrich_overall_timing
from .insights.insights_overall_firstgoal_momentum import (
    enrich_overall_firstgoal_momentum,
)
from .insights.insights_overall_discipline_setpieces import (
    enrich_overall_discipline_setpieces,
)


# ─────────────────────────────────────
#  공통: 날짜 파싱/정규화
# ─────────────────────────────────────


def _normalize_date(date_str: Optional[str]) -> str:
    """
    다양한 형태(YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS 등)의 문자열을
    YYYY-MM-DD 로 정규화한다.
    """
    if not date_str:
        # 오늘 날짜
        return datetime.utcnow().date().isoformat()

    try:
        # 이미 date 객체이면 그대로 사용
        if isinstance(date_str, date_cls):
            return date_str.isoformat()

        # 문자열 파싱
        # "2025-11-10" 또는 "2025-11-10T08:00:00Z" 등
        dt = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        # 이상하면 그냥 오늘 날짜
        return datetime.utcnow().date().isoformat()


# ─────────────────────────────────────
#  1) 홈 화면: 리그 목록
# ─────────────────────────────────────


def get_home_leagues_for_date(
    date_str: Optional[str] = None,
    league_filter: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
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
        JOIN leagues l ON m.league_id = l.league_id
        WHERE m.date_utc::date = %s
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
        (norm_date,),
    )

    results: List[Dict[str, Any]] = []
    for r in rows:
        lid = r["league_id"]
        if league_filter and lid not in league_filter:
            continue

        results.append(
            {
                "league_id": lid,
                "name": r["league_name"],
                "country": r["country"],
                "logo": r["logo"],
                "season": r["season"],
            }
        )
    return results


def get_home_leagues(
    date_str: Optional[str] = None,
    league_filter: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """
    과거 호환 함수. (기본적으로는 오늘 날짜를 기준으로 리그를 조회)
    """
    return get_home_leagues_for_date(date_str=date_str, league_filter=league_filter)


# ─────────────────────────────────────
#  2) 홈 화면: 리그별 매치 디렉터리
# ─────────────────────────────────────


def get_home_league_directory(
    league_id: int,
    date_str: Optional[str] = None,
) -> Dict[str, Any]:
    """
    특정 리그(league_id)의 주어진 날짜(date_str)에 대한
    홈 화면 디렉터리 정보(날짜, 라운드, 경기 목록 등)를 반환한다.
    """
    norm_date = _normalize_date(date_str)

    # 날짜 기준으로 이 리그의 경기들을 모두 가져온다.
    rows = fetch_all(
        """
        SELECT
            m.fixture_id,
            m.league_id,
            m.season,
            m.round,
            m.date_utc,
            m.status_short,
            m.status_long,
            m.home_id,
            t_home.name  AS home_name,
            t_home.logo  AS home_logo,
            m.away_id,
            t_away.name  AS away_name,
            t_away.logo  AS away_logo,
            m.goals_home,
            m.goals_away
        FROM matches m
        JOIN teams t_home ON m.home_id = t_home.id
        JOIN teams t_away ON m.away_id = t_away.id
        WHERE m.league_id = %s
          AND m.date_utc::date = %s
        ORDER BY m.date_utc ASC
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
                "status_long": r["status_long"],
                "home": {
                    "id": r["home_id"],
                    "name": r["home_name"],
                    "logo": r["home_logo"],
                    "goals": r["goals_home"],
                },
                "away": {
                    "id": r["away_id"],
                    "name": r["away_name"],
                    "logo": r["away_logo"],
                    "goals": r["goals_away"],
                },
            }
        )

    return {
        "league_id": league_id,
        "season": season,
        "round": round_name,
        "date": norm_date,
        "fixtures": fixtures,
    }


# ─────────────────────────────────────
#  3) 다음/이전 매치데이
# ─────────────────────────────────────


def _find_matchday_boundary(
    league_id: Optional[int],
    base_date_str: Optional[str],
    direction: str,
) -> Optional[str]:
    """
    direction 이 "next" 이면 base_date 이후의 가장 가까운 match_date,
    "prev" 이면 base_date 이전의 가장 가까운 match_date 를 반환한다.
    """
    base_date = _normalize_date(base_date_str)

    params: List[Any] = [base_date]
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
        GROUP BY m.date_utc::date
        ORDER BY m.date_utc::date {order}
        LIMIT 1
    """

    rows = fetch_all(sql, tuple(params))
    if not rows:
        return None
    return rows[0]["match_date"].isoformat()


def get_next_matchday(
    league_id: Optional[int],
    base_date_str: Optional[str],
) -> Optional[str]:
    return _find_matchday_boundary(league_id, base_date_str, direction="next")


def get_prev_matchday(
    league_id: Optional[int],
    base_date_str: Optional[str],
) -> Optional[str]:
    return _find_matchday_boundary(league_id, base_date_str, direction="prev")


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
                            'shots on target',
                            'shots on target (inc woodwork)',
                            'shots on target (inc. woodwork)'
                        )
                        AND mts.value ~ '^[0-9]+$'
                        THEN mts.value::int
                        ELSE 0
                    END
                ) AS shots_on_target
            FROM matches m
            JOIN match_team_stats mts
              ON m.fixture_id = mts.fixture_id
             AND mts.team_id IN (m.home_id, m.away_id)
            WHERE m.league_id = %s
              AND m.season    = %s
              AND m.status_short IN ('FT','AET','PEN')
            GROUP BY m.fixture_id, m.home_id, m.away_id
            """,
            (league_id, season_int),
        )

        total_shots_total = 0
        total_shots_home = 0
        total_shots_away = 0

        sog_total = 0
        sog_home = 0
        sog_away = 0

        total_matches = 0
        home_matches = 0
        away_matches = 0

        for r in shot_rows:
            fid = r["fixture_id"]
            home_id = r["home_id"]
            away_id = r["away_id"]
            total_shots = r["total_shots"] or 0
            sog = r["shots_on_target"] or 0

            if total_shots <= 0 and sog <= 0:
                continue

            total_matches += 1
            total_shots_total += total_shots
            sog_total += sog

            if home_id == team_id:
                home_matches += 1
                total_shots_home += total_shots
                sog_home += sog
            elif away_id == team_id:
                away_matches += 1
                total_shots_away += total_shots
                sog_away += sog

        if total_matches > 0:
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
                "total": fmt_pct(sog_total, total_shots_total) if total_shots_total > 0 else 0,
                "home": fmt_pct(sog_home, total_shots_home) if total_shots_home > 0 else 0,
                "away": fmt_pct(sog_away, total_shots_away) if total_shots_away > 0 else 0,
            }

    # ─────────────────────────────
    # Outcome & Totals / Result Combos / Goal Diff / Clean sheet / No goals
    #   (기존 team_season_stats.value 기반, 없으면 계산)
    # ─────────────────────────────
    standings_rows = fetch_all(
        """
        SELECT
            s.rank,
            s.team_id,
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
        # standings 기반 보정/기본값 계산
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

        cs_t = cs_h = cs_a = 0
        ng_t = ng_h = ng_a = 0

        win_o25_t = win_o25_h = win_o25_a = 0
        lose_btts_t = lose_btts_h = lose_btts_a = 0

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

            # 홈/원정 분리는 standings로는 알 수 없어서
            # 일단 total 기준으로만 계산하고,
            # home/away는 추후 match 테이블 기반으로 보정 가능.

            # BTTS, 팀 득점 오버, 총 득점 오버 등은
            # 원래는 개별 경기 단위로 계산해야 하지만,
            # 여기서는 일단 placeholder 개념으로 두고,
            # team_season_stats.value에 이미 채워져 있지 않으면
            # 0 값으로 남겨둔다.

        if mt_tot > 0:
            # 이미 값이 있으면 유지(.setdefault)
            insights.setdefault(
                "win_pct",
                {
                    "total": fmt_pct(win_t, mt_tot),
                    "home": 0,
                    "away": 0,
                },
            )
            insights.setdefault(
                "btts_pct",
                {
                    "total": fmt_pct(btts_t, mt_tot),
                    "home": 0,
                    "away": 0,
                },
            )
            insights.setdefault(
                "team_over05_pct",
                {
                    "total": fmt_pct(team_o05_t, mt_tot),
                    "home": 0,
                    "away": 0,
                },
            )
            insights.setdefault(
                "team_over15_pct",
                {
                    "total": fmt_pct(team_o15_t, mt_tot),
                    "home": 0,
                    "away": 0,
                },
            )
            insights.setdefault(
                "over15_pct",
                {
                    "total": fmt_pct(o15_t, mt_tot),
                    "home": 0,
                    "away": 0,
                },
            )
            insights.setdefault(
                "over25_pct",
                {
                    "total": fmt_pct(o25_t, mt_tot),
                    "home": 0,
                    "away": 0,
                },
            )

            # Goal diff avg (GF - GA)
            goal_diff_avg = safe_div(gf_sum_t - ga_sum_t, mt_tot)
            insights.setdefault(
                "goal_diff_avg",
                {
                    "total": round(goal_diff_avg, 2),
                    "home": 0.0,
                    "away": 0.0,
                },
            )

            # Clean sheet / No goals (placeholder)
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
                e.time,
                e.team_id
            FROM matches m
            JOIN match_events e
              ON m.fixture_id = e.fixture_id
            WHERE m.league_id = %s
              AND m.season    = %s
              AND m.status_short IN ('FT','AET','PEN')
              AND e.type = 'Goal'
              AND e.time IS NOT NULL
            """,
            (league_id, season_int),
        )

        if goal_rows:
            # 0–9, 10–19, ..., 80–89, 90+ 형태로 10개 버킷
            for_buckets = [0] * 10
            against_buckets = [0] * 10

            for gr in goal_rows:
                minute = gr["time"] or 0
                if minute < 0:
                    minute = 0
                if minute >= 90:
                    idx = 9
                else:
                    idx = minute // 10
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
    # Discipline & Set Pieces (per match 평균) – 서버 DB 기반
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
            JOIN match_team_stats mts
              ON m.fixture_id = mts.fixture_id
             AND mts.team_id IN (m.home_id, m.away_id)
            WHERE m.league_id = %s
              AND m.season    = %s
              AND m.status_short IN ('FT','AET','PEN')
            GROUP BY m.fixture_id, m.home_id, m.away_id
            """,
            (league_id, season_int),
        )

        if disc_rows:
            sum_corners_t = sum_corners_h = sum_corners_a = 0
            sum_yellows_t = sum_yellows_h = sum_yellows_a = 0
            sum_reds_t = sum_reds_h = sum_reds_a = 0

            eff_tot = eff_home = eff_away = 0

            for r in disc_rows:
                fid = r["fixture_id"]
                home_id = r["home_id"]
                away_id = r["away_id"]
                c = r["corners"] or 0
                y = r["yellows"] or 0
                red = r["reds"] or 0

                if c <= 0 and y <= 0 and red <= 0:
                    continue

                eff_tot += 1
                sum_corners_t += c
                sum_yellows_t += y
                sum_reds_t += red

                if home_id == team_id:
                    eff_home += 1
                    sum_corners_h += c
                    sum_yellows_h += y
                    sum_reds_h += red
                elif away_id == team_id:
                    eff_away += 1
                    sum_corners_a += c
                    sum_yellows_a += y
                    sum_reds_a += red

            if eff_tot > 0:
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

    # ─────────────────────────────
    #  추가 Insights 모듈 연동
    #   - Timing
    #   - First Goal / Momentum
    #   - Discipline & Set Pieces (red card impact 등)
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
            enrich_overall_discipline_setpieces(
                stats=stats,
                insights=insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
            )
        except Exception:
            # 일부 인사이트 계산에 실패해도 기본 값은 그대로 사용
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
