# ai_predictions_block.py

from __future__ import annotations
from typing import Any, Dict, Optional, Tuple

from db import fetch_all
from ai_predictions_engine import compute_ai_predictions_v2

GOAL_DETAILS_SCORED = ("Normal Goal", "Penalty", "Own Goal")


def _fetch_one(sql: str, params: Tuple[Any, ...]) -> Optional[Dict[str, Any]]:
    rows = fetch_all(sql, params)
    return rows[0] if rows else None


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        if x is None:
            return default
        return float(x)
    except Exception:
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return lo if x < lo else hi if x > hi else x


def _mix_weighted(
    season_val: Optional[float],
    l10_val: Optional[float],
    l5_val: Optional[float],
    *,
    w_season: float = 0.55,
    w_l10: float = 0.35,
    w_l5: float = 0.10,
) -> float:
    parts = []
    if season_val is not None:
        parts.append((w_season, season_val))
    if l10_val is not None:
        parts.append((w_l10, l10_val))
    if l5_val is not None:
        parts.append((w_l5, l5_val))
    if not parts:
        return 0.0
    wsum = sum(w for w, _ in parts)
    return sum((w / wsum) * v for w, v in parts)


def _league_avgs(league_id: int, season: int) -> Tuple[float, float]:
    row = _fetch_one(
        """
        select
          avg(home_ft)::float as mu_home,
          avg(away_ft)::float as mu_away
        from matches
        where league_id=%s and season=%s
          and status_group='FINISHED'
          and home_ft is not null and away_ft is not null
        """,
        (league_id, season),
    )
    mu_home = _safe_float(row["mu_home"] if row else None, 1.35)
    mu_away = _safe_float(row["mu_away"] if row else None, 1.15)
    return (_clamp(mu_home, 0.6, 2.4), _clamp(mu_away, 0.6, 2.4))


def _team_home_context_stats(team_id: int, league_id: int, season: int):
    row_s = _fetch_one(
        """
        select
          avg(home_ft)::float as gf,
          avg(away_ft)::float as ga,
          count(*)::int as n
        from matches
        where league_id=%s and season=%s and status_group='FINISHED'
          and home_id=%s
          and home_ft is not null and away_ft is not null
        """,
        (league_id, season, team_id),
    )
    n_s = int(row_s["n"]) if row_s and row_s.get("n") is not None else 0
    gf_s = _safe_float(row_s["gf"] if row_s else None, 0.0) if n_s > 0 else None
    ga_s = _safe_float(row_s["ga"] if row_s else None, 0.0) if n_s > 0 else None

    row_10 = _fetch_one(
        """
        with last as (
          select home_ft, away_ft
          from matches
          where league_id=%s and season=%s and status_group='FINISHED'
            and home_id=%s
            and home_ft is not null and away_ft is not null
          order by (date_utc::timestamptz) desc
          limit 10
        )
        select
          avg(home_ft)::float as gf,
          avg(away_ft)::float as ga,
          count(*)::int as n
        from last
        """,
        (league_id, season, team_id),
    )
    n_10 = int(row_10["n"]) if row_10 and row_10.get("n") is not None else 0
    gf_10 = _safe_float(row_10["gf"] if row_10 else None, 0.0) if n_10 > 0 else None
    ga_10 = _safe_float(row_10["ga"] if row_10 else None, 0.0) if n_10 > 0 else None

    row_5 = _fetch_one(
        """
        with last as (
          select home_ft, away_ft
          from matches
          where league_id=%s and season=%s and status_group='FINISHED'
            and home_id=%s
            and home_ft is not null and away_ft is not null
          order by (date_utc::timestamptz) desc
          limit 5
        )
        select
          avg(home_ft)::float as gf,
          avg(away_ft)::float as ga,
          count(*)::int as n
        from last
        """,
        (league_id, season, team_id),
    )
    n_5 = int(row_5["n"]) if row_5 and row_5.get("n") is not None else 0
    gf_5 = _safe_float(row_5["gf"] if row_5 else None, 0.0) if n_5 > 0 else None
    ga_5 = _safe_float(row_5["ga"] if row_5 else None, 0.0) if n_5 > 0 else None

    return gf_s, ga_s, gf_10, ga_10, gf_5, ga_5


def _team_away_context_stats(team_id: int, league_id: int, season: int):
    row_s = _fetch_one(
        """
        select
          avg(away_ft)::float as gf,
          avg(home_ft)::float as ga,
          count(*)::int as n
        from matches
        where league_id=%s and season=%s and status_group='FINISHED'
          and away_id=%s
          and home_ft is not null and away_ft is not null
        """,
        (league_id, season, team_id),
    )
    n_s = int(row_s["n"]) if row_s and row_s.get("n") is not None else 0
    gf_s = _safe_float(row_s["gf"] if row_s else None, 0.0) if n_s > 0 else None
    ga_s = _safe_float(row_s["ga"] if row_s else None, 0.0) if n_s > 0 else None

    row_10 = _fetch_one(
        """
        with last as (
          select home_ft, away_ft
          from matches
          where league_id=%s and season=%s and status_group='FINISHED'
            and away_id=%s
            and home_ft is not null and away_ft is not null
          order by (date_utc::timestamptz) desc
          limit 10
        )
        select
          avg(away_ft)::float as gf,
          avg(home_ft)::float as ga,
          count(*)::int as n
        from last
        """,
        (league_id, season, team_id),
    )
    n_10 = int(row_10["n"]) if row_10 and row_10.get("n") is not None else 0
    gf_10 = _safe_float(row_10["gf"] if row_10 else None, 0.0) if n_10 > 0 else None
    ga_10 = _safe_float(row_10["ga"] if row_10 else None, 0.0) if n_10 > 0 else None

    row_5 = _fetch_one(
        """
        with last as (
          select home_ft, away_ft
          from matches
          where league_id=%s and season=%s and status_group='FINISHED'
            and away_id=%s
            and home_ft is not null and away_ft is not null
          order by (date_utc::timestamptz) desc
          limit 5
        )
        select
          avg(away_ft)::float as gf,
          avg(home_ft)::float as ga,
          count(*)::int as n
        from last
        """,
        (league_id, season, team_id),
    )
    n_5 = int(row_5["n"]) if row_5 and row_5.get("n") is not None else 0
    gf_5 = _safe_float(row_5["gf"] if row_5 else None, 0.0) if n_5 > 0 else None
    ga_5 = _safe_float(row_5["ga"] if row_5 else None, 0.0) if n_5 > 0 else None

    return gf_s, ga_s, gf_10, ga_10, gf_5, ga_5


def _league_goal_shares(league_id: int, season: int) -> Tuple[float, float, float]:
    row = _fetch_one(
        """
        with finished as (
          select fixture_id, (home_ft + away_ft) as ft_total
          from matches
          where league_id=%s and season=%s and status_group='FINISHED'
            and home_ft is not null and away_ft is not null
        )
        select
          sum(case when e.type='Goal' and e.detail in %s and e.minute <= 45 then 1 else 0 end)::float as goals_1h,
          sum(case when e.type='Goal' and e.detail in %s and e.minute > 45 then 1 else 0 end)::float as goals_2h,
          sum(case when e.type='Goal' and e.detail in %s and e.minute between 35 and 45 then 1 else 0 end)::float as goals_w35_45,
          sum(case when e.type='Goal' and e.detail in %s and e.minute >= 80 then 1 else 0 end)::float as goals_w80_90,
          sum(f.ft_total)::float as ft_total_sum
        from finished f
        left join match_events e on e.fixture_id=f.fixture_id
        """,
        (league_id, season, GOAL_DETAILS_SCORED, GOAL_DETAILS_SCORED, GOAL_DETAILS_SCORED, GOAL_DETAILS_SCORED),
    )

    goals_1h = _safe_float(row["goals_1h"] if row else None, 0.0)
    goals_2h = _safe_float(row["goals_2h"] if row else None, 0.0)
    goals_w35 = _safe_float(row["goals_w35_45"] if row else None, 0.0)
    goals_w80 = _safe_float(row["goals_w80_90"] if row else None, 0.0)
    ft_total_sum = _safe_float(row["ft_total_sum"] if row else None, 0.0)

    if ft_total_sum <= 0.0:
        share_1h = 0.45
    else:
        share_1h = goals_1h / ft_total_sum

    share_1h = _clamp(share_1h, 0.20, 0.65)

    if goals_1h <= 0.0:
        w35 = 0.22
    else:
        w35 = goals_w35 / goals_1h

    if goals_2h <= 0.0:
        w80 = 0.22
    else:
        w80 = goals_w80 / goals_2h

    w35 = _clamp(w35, 0.05, 0.55)
    w80 = _clamp(w80, 0.05, 0.55)

    return share_1h, w35, w80


def build_ai_predictions_block(match_id: int, insights_overall: Dict[str, Any]) -> Dict[str, Any]:
    match_row = _fetch_one(
        """
        select league_id, season, home_id, away_id
        from matches
        where fixture_id=%s
        """,
        (match_id,),
    )
    if not match_row:
        return {"version": 2, "sections": []}

    league_id = int(match_row["league_id"])
    season = int(match_row["season"])
    home_id = int(match_row["home_id"])
    away_id = int(match_row["away_id"])

    mu_home, mu_away = _league_avgs(league_id, season)

    h_gf_s, h_ga_s, h_gf_10, h_ga_10, h_gf_5, h_ga_5 = _team_home_context_stats(home_id, league_id, season)
    a_gf_s, a_ga_s, a_gf_10, a_ga_10, a_gf_5, a_ga_5 = _team_away_context_stats(away_id, league_id, season)

    h_gf = _mix_weighted(h_gf_s, h_gf_10, h_gf_5)
    h_ga = _mix_weighted(h_ga_s, h_ga_10, h_ga_5)
    a_gf = _mix_weighted(a_gf_s, a_gf_10, a_gf_5)
    a_ga = _mix_weighted(a_ga_s, a_ga_10, a_ga_5)

    if h_gf <= 0.0:
        h_gf = mu_home
    if h_ga <= 0.0:
        h_ga = mu_away
    if a_gf <= 0.0:
        a_gf = mu_away
    if a_ga <= 0.0:
        a_ga = mu_home

    att_home = _clamp(h_gf / mu_home, 0.55, 1.70)
    def_away = _clamp(a_ga / mu_home, 0.55, 1.70)
    att_away = _clamp(a_gf / mu_away, 0.55, 1.70)
    def_home = _clamp(h_ga / mu_away, 0.55, 1.70)

    lam_h_ft = _clamp(mu_home * att_home * def_away, 0.05, 4.50)
    lam_a_ft = _clamp(mu_away * att_away * def_home, 0.05, 4.50)

    share_1h, winshare_35, winshare_80 = _league_goal_shares(league_id, season)

    lam_h_1h = _clamp(lam_h_ft * share_1h, 0.01, 3.50)
    lam_a_1h = _clamp(lam_a_ft * share_1h, 0.01, 3.50)
    lam_h_2h = _clamp(lam_h_ft * (1.0 - share_1h), 0.01, 3.50)
    lam_a_2h = _clamp(lam_a_ft * (1.0 - share_1h), 0.01, 3.50)

    lam_w35_45 = _clamp((lam_h_1h + lam_a_1h) * winshare_35, 0.0, 3.0)
    lam_w80_90 = _clamp((lam_h_2h + lam_a_2h) * winshare_80, 0.0, 3.0)

    return compute_ai_predictions_v2(
        lam_h_ft=lam_h_ft,
        lam_a_ft=lam_a_ft,
        lam_h_1h=lam_h_1h,
        lam_a_1h=lam_a_1h,
        lam_h_2h=lam_h_2h,
        lam_a_2h=lam_a_2h,
        lam_w35_45=lam_w35_45,
        lam_w80_90=lam_w80_90,
        gmax=10,
    )
