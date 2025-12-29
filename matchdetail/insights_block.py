# matchdetail/insights_block.py

from __future__ import annotations

from typing import Any, Dict, Optional, List

from db import fetch_all


# ─────────────────────────────────────
#  (통합) services/insights/utils.py
# ─────────────────────────────────────

def safe_div(num: Any, den: Any) -> float:
    """
    0 나누기, 타입 오류 등을 모두 0.0 으로 처리하는 안전한 나눗셈.
    """
    try:
        num_f = float(num)
    except (TypeError, ValueError):
        return 0.0

    try:
        den_f = float(den)
    except (TypeError, ValueError):
        return 0.0

    if den_f == 0.0:
        return 0.0

    return num_f / den_f


def fmt_pct(num: Any, den: Any) -> int:
    """
    분자/분모에서 퍼센트(int, 0~100) 를 만들어 준다.
    분모가 0 이면 0 리턴.
    """
    v = safe_div(num, den) * 100.0
    return int(round(v)) if v > 0.0 else 0


def fmt_avg(total: Any, matches: Any, decimals: int = 1) -> float:
    """
    total / matches 의 평균을 소수점 n자리까지 반올림해서 리턴.
    matches <= 0 이면 0.0
    """
    try:
        total_f = float(total)
        matches_i = int(matches)
    except (TypeError, ValueError):
        return 0.0

    if matches_i <= 0:
        return 0.0

    v = total_f / matches_i
    factor = 10 ** decimals
    return round(v * factor) / factor


def normalize_comp(raw: Any) -> str:
    if raw is None:
        return "All"

    s = str(raw).strip()
    if not s:
        return "All"

    if s in ("All", "League", "Cup", "UEFA", "ACL"):
        return s

    lower = s.lower()

    if lower in ("all", "전체", "full", "season", "full season"):
        return "All"

    if lower in ("league", "리그"):
        return "League"

    if "uefa" in lower or "europe" in lower:
        return "UEFA"

    if "afc champions league" in lower or lower == "acl":
        return "ACL"

    if lower in ("cup", "domestic cup", "국내컵") or "cup" in lower:
        return "Cup"

    return s


def parse_last_n(raw: Any) -> int:
    if raw is None:
        return 0

    s = str(raw).strip()
    if not s:
        return 0

    lower = s.lower()
    if lower in ("season", "all", "full season"):
        return 0

    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        try:
            n = int(digits)
            return n if n > 0 else 0
        except ValueError:
            return 0

    if s.isdigit():
        n = int(s)
        return n if n > 0 else 0

    return 0


def build_league_ids_for_query(
    *,
    insights_filters: Optional[Dict[str, Any]],
    fallback_league_id: Optional[int],
) -> List[int]:
    league_ids: List[int] = []

    if insights_filters and isinstance(insights_filters, dict):
        raw_ids = insights_filters.get("target_league_ids_last_n")
        if isinstance(raw_ids, list):
            for x in raw_ids:
                try:
                    league_ids.append(int(x))
                except (TypeError, ValueError):
                    continue

        if league_ids:
            seen = set()
            deduped: List[int] = []
            for lid in league_ids:
                if lid in seen:
                    continue
                seen.add(lid)
                deduped.append(lid)
            league_ids = deduped

    if not league_ids and fallback_league_id is not None:
        try:
            league_ids = [int(fallback_league_id)]
        except (TypeError, ValueError):
            league_ids = []

    return league_ids


# ─────────────────────────────────────
#  (통합) services/insights/insights_overall_outcome_totals.py
# ─────────────────────────────────────

def enrich_overall_outcome_totals(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    matches_total_api: int = 0,
    last_n: int = 0,
) -> None:
    """
    Outcome + Totals / BTTS / CleanSheet / NoGoals / GoalDiff / ResultCombos 등.
    (기존 services/insights 구현 그대로)
    """
    if season_int is None:
        return

    insights_filters = insights.get("insights_filters") if isinstance(insights, dict) else None
    league_ids_for_query = build_league_ids_for_query(
        insights_filters=insights_filters if isinstance(insights_filters, dict) else None,
        fallback_league_id=league_id,
    )
    if not league_ids_for_query:
        return

    placeholders = ",".join(["%s"] * len(league_ids_for_query))

    base_sql = f"""
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            m.home_ft,
            m.away_ft,
            m.status_group,
            m.date_utc
        FROM matches m
        WHERE m.league_id IN ({placeholders})
          AND m.season    = %s
          AND (m.home_id = %s OR m.away_id = %s)
          AND lower(m.status_group) IN ('finished','ft','fulltime')
        ORDER BY m.date_utc DESC
    """

    params: list[Any] = [*league_ids_for_query, season_int, team_id, team_id]
    if last_n and last_n > 0:
        base_sql += " LIMIT %s"
        params.append(last_n)

    match_rows = fetch_all(base_sql, tuple(params))
    if not match_rows:
        return

    mt_tot = mh_tot = ma_tot = 0

    win_t = win_h = win_a = 0
    draw_t = draw_h = draw_a = 0
    lose_t = lose_h = lose_a = 0

    btts_t = btts_h = btts_a = 0
    team_o05_t = team_o05_h = team_o05_a = 0
    team_o15_t = team_o15_h = team_o15_a = 0
    team_o25_t = team_o25_h = team_o25_a = 0
    team_o35_t = team_o35_h = team_o35_a = 0
    team_o45_t = team_o45_h = team_o45_a = 0

    total_o15_t = total_o15_h = total_o15_a = 0
    total_o25_t = total_o25_h = total_o25_a = 0
    total_o35_t = total_o35_h = total_o35_a = 0
    total_o45_t = total_o45_h = total_o45_a = 0
    total_o55_t = total_o55_h = total_o55_a = 0

    cs_t = cs_h = cs_a = 0
    ng_t = ng_h = ng_a = 0

    gd_sum_t = gd_sum_h = gd_sum_a = 0

    win_and_btts_t = win_and_btts_h = win_and_btts_a = 0
    draw_and_btts_t = draw_and_btts_h = draw_and_btts_a = 0
    lose_and_btts_t = lose_and_btts_h = lose_and_btts_a = 0

    for r in match_rows:
        try:
            home_id = int(r.get("home_id"))
            away_id = int(r.get("away_id"))
        except (TypeError, ValueError):
            continue

        is_home = (home_id == team_id)
        is_away = (away_id == team_id)
        if not (is_home or is_away):
            continue

        try:
            hg = int(r.get("home_ft") or 0)
            ag = int(r.get("away_ft") or 0)
        except (TypeError, ValueError):
            hg = int(r.get("home_ft") or 0) if str(r.get("home_ft") or "").isdigit() else 0
            ag = int(r.get("away_ft") or 0) if str(r.get("away_ft") or "").isdigit() else 0

        tg = hg if is_home else ag
        og = ag if is_home else hg

        mt_tot += 1
        if is_home:
            mh_tot += 1
        else:
            ma_tot += 1

        if tg > og:
            win_t += 1
            if is_home:
                win_h += 1
            else:
                win_a += 1
        elif tg == og:
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

        if tg >= 1:
            team_o05_t += 1
            (team_o05_h if is_home else team_o05_a).__iadd__(0)  # no-op (keep style parity)

        if tg >= 1:
            if is_home:
                team_o05_h += 1
            else:
                team_o05_a += 1
        if tg >= 2:
            team_o15_t += 1
            if is_home:
                team_o15_h += 1
            else:
                team_o15_a += 1
        if tg >= 3:
            team_o25_t += 1
            if is_home:
                team_o25_h += 1
            else:
                team_o25_a += 1
        if tg >= 4:
            team_o35_t += 1
            if is_home:
                team_o35_h += 1
            else:
                team_o35_a += 1
        if tg >= 5:
            team_o45_t += 1
            if is_home:
                team_o45_h += 1
            else:
                team_o45_a += 1

        tot = tg + og
        if tot >= 2:
            total_o15_t += 1
            if is_home:
                total_o15_h += 1
            else:
                total_o15_a += 1
        if tot >= 3:
            total_o25_t += 1
            if is_home:
                total_o25_h += 1
            else:
                total_o25_a += 1
        if tot >= 4:
            total_o35_t += 1
            if is_home:
                total_o35_h += 1
            else:
                total_o35_a += 1
        if tot >= 5:
            total_o45_t += 1
            if is_home:
                total_o45_h += 1
            else:
                total_o45_a += 1
        if tot >= 6:
            total_o55_t += 1
            if is_home:
                total_o55_h += 1
            else:
                total_o55_a += 1

        both_score = (tg >= 1 and og >= 1)
        if both_score:
            btts_t += 1
            if is_home:
                btts_h += 1
            else:
                btts_a += 1

        if og == 0:
            cs_t += 1
            if is_home:
                cs_h += 1
            else:
                cs_a += 1

        if tg == 0:
            ng_t += 1
            if is_home:
                ng_h += 1
            else:
                ng_a += 1

        gd = tg - og
        gd_sum_t += gd
        if is_home:
            gd_sum_h += gd
        else:
            gd_sum_a += gd

        if both_score and tg > og:
            win_and_btts_t += 1
            if is_home:
                win_and_btts_h += 1
            else:
                win_and_btts_a += 1
        if both_score and tg == og:
            draw_and_btts_t += 1
            if is_home:
                draw_and_btts_h += 1
            else:
                draw_and_btts_a += 1
        if both_score and tg < og:
            lose_and_btts_t += 1
            if is_home:
                lose_and_btts_h += 1
            else:
                lose_and_btts_a += 1

    eff_tot = matches_total_api if matches_total_api else mt_tot
    eff_home = mh_tot
    eff_away = ma_tot

    insights["events_sample"] = {"total": eff_tot, "home": eff_home, "away": eff_away}

    insights["win_pct"] = {
        "total": fmt_pct(win_t, eff_tot),
        "home": fmt_pct(win_h, eff_home),
        "away": fmt_pct(win_a, eff_away),
    }
    insights["draw_pct"] = {
        "total": fmt_pct(draw_t, eff_tot),
        "home": fmt_pct(draw_h, eff_home),
        "away": fmt_pct(draw_a, eff_away),
    }
    insights["btts_pct"] = {
        "total": fmt_pct(btts_t, eff_tot),
        "home": fmt_pct(btts_h, eff_home),
        "away": fmt_pct(btts_a, eff_away),
    }

    insights["team_over05_pct"] = {
        "total": fmt_pct(team_o05_t, eff_tot),
        "home": fmt_pct(team_o05_h, eff_home),
        "away": fmt_pct(team_o05_a, eff_away),
    }
    insights["team_over15_pct"] = {
        "total": fmt_pct(team_o15_t, eff_tot),
        "home": fmt_pct(team_o15_h, eff_home),
        "away": fmt_pct(team_o15_a, eff_away),
    }
    insights["team_over25_pct"] = {
        "total": fmt_pct(team_o25_t, eff_tot),
        "home": fmt_pct(team_o25_h, eff_home),
        "away": fmt_pct(team_o25_a, eff_away),
    }
    insights["team_over35_pct"] = {
        "total": fmt_pct(team_o35_t, eff_tot),
        "home": fmt_pct(team_o35_h, eff_home),
        "away": fmt_pct(team_o35_a, eff_away),
    }
    insights["team_over45_pct"] = {
        "total": fmt_pct(team_o45_t, eff_tot),
        "home": fmt_pct(team_o45_h, eff_home),
        "away": fmt_pct(team_o45_a, eff_away),
    }

    insights["total_over15_pct"] = {
        "total": fmt_pct(total_o15_t, eff_tot),
        "home": fmt_pct(total_o15_h, eff_home),
        "away": fmt_pct(total_o15_a, eff_away),
    }
    insights["total_over25_pct"] = {
        "total": fmt_pct(total_o25_t, eff_tot),
        "home": fmt_pct(total_o25_h, eff_home),
        "away": fmt_pct(total_o25_a, eff_away),
    }
    insights["total_over35_pct"] = {
        "total": fmt_pct(total_o35_t, eff_tot),
        "home": fmt_pct(total_o35_h, eff_home),
        "away": fmt_pct(total_o35_a, eff_away),
    }
    insights["total_over45_pct"] = {
        "total": fmt_pct(total_o45_t, eff_tot),
        "home": fmt_pct(total_o45_h, eff_home),
        "away": fmt_pct(total_o45_a, eff_away),
    }
    insights["total_over55_pct"] = {
        "total": fmt_pct(total_o55_t, eff_tot),
        "home": fmt_pct(total_o55_h, eff_home),
        "away": fmt_pct(total_o55_a, eff_away),
    }

    insights["clean_sheet_pct"] = {
        "total": fmt_pct(cs_t, eff_tot),
        "home": fmt_pct(cs_h, eff_home),
        "away": fmt_pct(cs_a, eff_away),
    }
    insights["no_goals_pct"] = {
        "total": fmt_pct(ng_t, eff_tot),
        "home": fmt_pct(ng_h, eff_home),
        "away": fmt_pct(ng_a, eff_away),
    }

    insights["goal_diff_avg"] = {
        "total": fmt_avg(gd_sum_t, eff_tot, decimals=1),
        "home": fmt_avg(gd_sum_h, eff_home, decimals=1),
        "away": fmt_avg(gd_sum_a, eff_away, decimals=1),
    }

    insights["lose_and_btts_pct"] = {
        "total": fmt_pct(lose_and_btts_t, eff_tot),
        "home": fmt_pct(lose_and_btts_h, eff_home),
        "away": fmt_pct(lose_and_btts_a, eff_away),
    }
    insights["win_and_btts_pct"] = {
        "total": fmt_pct(win_and_btts_t, eff_tot),
        "home": fmt_pct(win_and_btts_h, eff_home),
        "away": fmt_pct(win_and_btts_a, eff_away),
    }
    insights["draw_and_btts_pct"] = {
        "total": fmt_pct(draw_and_btts_t, eff_tot),
        "home": fmt_pct(draw_and_btts_h, eff_home),
        "away": fmt_pct(draw_and_btts_a, eff_away),
    }


# ─────────────────────────────────────
#  (통합) services/insights/insights_overall_goalsbytime.py
# ─────────────────────────────────────

def enrich_overall_goals_by_time(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    last_n: Optional[int] = None,
) -> None:
    if season_int is None:
        return

    last_n_int = int(last_n or 0)

    insights_filters = insights.get("insights_filters") if isinstance(insights, dict) else None
    league_ids_for_query = build_league_ids_for_query(
        insights_filters=insights_filters if isinstance(insights_filters, dict) else None,
        fallback_league_id=league_id,
    )
    if not league_ids_for_query:
        return

    placeholders = ",".join(["%s"] * len(league_ids_for_query))

    base_sql = f"""
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            m.date_utc
        FROM matches m
        WHERE m.league_id IN ({placeholders})
          AND m.season    = %s
          AND (m.home_id = %s OR m.away_id = %s)
          AND lower(m.status_group) IN ('finished','ft','fulltime')
        ORDER BY m.date_utc DESC
    """

    params: list[Any] = [*league_ids_for_query, season_int, team_id, team_id]
    if last_n_int and last_n_int > 0:
        base_sql += " LIMIT %s"
        params.append(last_n_int)

    match_rows = fetch_all(base_sql, tuple(params))
    if not match_rows:
        insights["goals_by_time_for"] = []
        insights["goals_by_time_against"] = []
        return

    fixture_ids: List[int] = []
    for r in match_rows:
        try:
            fid = int(r.get("fixture_id"))
            fixture_ids.append(fid)
        except (TypeError, ValueError):
            continue

    if not fixture_ids:
        insights["goals_by_time_for"] = []
        insights["goals_by_time_against"] = []
        return

    fi_placeholders = ",".join(["%s"] * len(fixture_ids))

    sql_goals = f"""
        SELECT
            e.fixture_id,
            e.team_id,
            e.minute
        FROM match_events e
        WHERE e.fixture_id IN ({fi_placeholders})
          AND lower(e.type) IN ('goal','own goal','penalty','penalty goal')
          AND e.minute IS NOT NULL
    """

    goal_rows = fetch_all(sql_goals, tuple(fixture_ids)) or []

    buckets = [
        ("0-15", 0, 15),
        ("16-30", 16, 30),
        ("31-45", 31, 45),
        ("46-60", 46, 60),
        ("61-75", 61, 75),
        ("76-90", 76, 90),
        ("90+", 91, 9999),
    ]

    def _init_counts() -> List[Dict[str, Any]]:
        return [{"bucket": name, "count": 0} for name, _, _ in buckets]

    goals_for = _init_counts()
    goals_against = _init_counts()

    for r in goal_rows:
        try:
            minute = int(r.get("minute") or 0)
        except (TypeError, ValueError):
            continue

        ev_team_id = r.get("team_id")
        is_for = (str(ev_team_id) == str(team_id))

        idx = None
        for i, (_, lo, hi) in enumerate(buckets):
            if lo <= minute <= hi:
                idx = i
                break
        if idx is None:
            continue

        if is_for:
            goals_for[idx]["count"] += 1
        else:
            goals_against[idx]["count"] += 1

    insights["goals_by_time_for"] = goals_for
    insights["goals_by_time_against"] = goals_against


# ─────────────────────────────────────
#  (기존) matchdetail/insights_block.py (header 기반 빌더)
# ─────────────────────────────────────

def _extract_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except (TypeError, ValueError):
        return None


def _get_meta_from_header(header: Dict[str, Any]) -> Dict[str, Any]:
    league_id = _extract_int(header.get("league_id") or header.get("leagueId"))
    season_int = _extract_int(header.get("season") or header.get("season_int"))
    home_team_id = _extract_int(header.get("home_team_id") or header.get("homeTeamId"))
    away_team_id = _extract_int(header.get("away_team_id") or header.get("awayTeamId"))

    # header 구조가 다른 버전도 커버
    if home_team_id is None:
        ht = header.get("home") or {}
        if isinstance(ht, dict):
            home_team_id = _extract_int(ht.get("team_id") or ht.get("id"))
    if away_team_id is None:
        at = header.get("away") or {}
        if isinstance(at, dict):
            away_team_id = _extract_int(at.get("team_id") or at.get("id"))

    # filters
    header_filters = header.get("filters") or {}
    if not isinstance(header_filters, dict):
        header_filters = {}

    return {
        "league_id": league_id,
        "season_int": season_int,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "header_filters": header_filters,
    }


def _get_last_n_from_header(header: Dict[str, Any]) -> int:
    hf = header.get("filters") or {}
    if not isinstance(hf, dict):
        hf = {}
    raw = hf.get("last_n")
    return parse_last_n(raw)


def _build_comp_league_ids(
    *,
    comp_std: str,
    competition_detail: Optional[Dict[str, Any]],
    league_id: int,
) -> List[int]:
    if comp_std == "All":
        return [league_id]

    comp = competition_detail or {}
    comps = comp.get("competitions") or []
    if not isinstance(comps, list):
        return [league_id]

    league_ids: List[int] = []
    uefa_ids: List[int] = []
    acl_ids: List[int] = []

    for c in comps:
        if not isinstance(c, dict):
            continue

        lid_int = _extract_int(c.get("league_id"))
        if lid_int is None:
            continue

        ctype = str(c.get("type") or "").lower()
        cname = str(c.get("name") or "").strip()

        if comp_std == "League":
            if ctype == "league":
                league_ids.append(lid_int)
            continue

        if comp_std == "Cup":
            if ctype == "cup":
                league_ids.append(lid_int)
            continue

        lower_name = cname.lower()
        if ("uefa" in lower_name) or ("champions league" in lower_name and "afc" not in lower_name):
            uefa_ids.append(lid_int)
        if ("afc" in lower_name) and ("champions league" in lower_name):
            acl_ids.append(lid_int)

        if comp_std not in ("UEFA", "ACL") and cname == comp_std:
            league_ids.append(lid_int)

    def _dedupe(seq: List[int]) -> List[int]:
        seen = set()
        out: List[int] = []
        for v in seq:
            if v in seen:
                continue
            seen.add(v)
            out.append(v)
        return out

    if comp_std == "UEFA":
        return _dedupe(uefa_ids) if uefa_ids else [league_id]
    if comp_std == "ACL":
        return _dedupe(acl_ids) if acl_ids else [league_id]
    return _dedupe(league_ids) if league_ids else [league_id]


def _build_insights_filters_for_team(
    *,
    league_id: int,
    season_int: int,
    team_id: int,
    last_n: int,
    comp_raw: Any,
) -> Dict[str, Any]:
    comp_std = normalize_comp(comp_raw)

    competition_detail = None
    try:
        rows = fetch_all(
            """
            SELECT competition_detail
            FROM leagues
            WHERE id = %s
            """,
            (league_id,),
        )
        if rows:
            competition_detail = rows[0].get("competition_detail")
        if not isinstance(competition_detail, dict):
            competition_detail = None
    except Exception:
        competition_detail = None

    target_league_ids = _build_comp_league_ids(
        comp_std=comp_std,
        competition_detail=competition_detail,
        league_id=league_id,
    )

    return {
        "comp": comp_std,
        "target_league_ids_last_n": target_league_ids,
        "last_n": last_n,
        "season": season_int,
        "team_id": team_id,
    }


def _build_side_insights(
    *,
    league_id: int,
    season_int: int,
    team_id: int,
    last_n: int,
    comp_raw: Any,
    header_filters: Dict[str, Any],
):
    stats: Dict[str, Any] = {}
    insights: Dict[str, Any] = {}

    side_filters = _build_insights_filters_for_team(
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
        comp_raw=comp_raw,
    )
    insights["insights_filters"] = side_filters

    matches_total_api = 0
    try:
        matches_total_api = int(stats.get("matches_total_api") or 0)
    except Exception:
        matches_total_api = 0

    enrich_overall_outcome_totals(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=matches_total_api,
        last_n=last_n,
    )

    enrich_overall_goals_by_time(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
    )

    return insights


def build_insights_overall_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not header:
        return None

    meta = _get_meta_from_header(header)

    league_id = meta["league_id"]
    season_int = meta["season_int"]
    home_team_id = meta["home_team_id"]
    away_team_id = meta["away_team_id"]
    header_filters = meta["header_filters"]

    if None in (league_id, season_int, home_team_id, away_team_id):
        return None

    last_n_for_calc = _get_last_n_from_header(header)
    season_for_calc = season_int

    comp_label = (header_filters.get("comp") if isinstance(header_filters, dict) else None) or "All"
    comp_label_home = comp_label
    comp_label_away = comp_label

    raw_last_n_label = header_filters.get("last_n") if isinstance(header_filters, dict) else None
    season_override = None
    if raw_last_n_label:
        try:
            s = str(raw_last_n_label).strip()
            lower = s.lower()
            if lower.startswith("season"):
                digits = "".join(ch for ch in s if ch.isdigit())
                if digits:
                    season_override = int(digits)
        except Exception:
            season_override = None

    if season_override:
        season_for_calc = season_override
        last_n_for_calc = 0

    home_ins = _build_side_insights(
        league_id=league_id,
        season_int=season_for_calc,
        team_id=home_team_id,
        last_n=last_n_for_calc,
        comp_raw=comp_label_home,
        header_filters=header_filters,
    )

    away_ins = _build_side_insights(
        league_id=league_id,
        season_int=season_for_calc,
        team_id=away_team_id,
        last_n=last_n_for_calc,
        comp_raw=comp_label_away,
        header_filters=header_filters,
    )

    filters_for_client = {
        "comp": comp_label,
        "last_n": raw_last_n_label if raw_last_n_label is not None else last_n_for_calc,
    }

    return {
        "league_id": league_id,
        "season": season_for_calc,
        "last_n": last_n_for_calc,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "filters": filters_for_client,
        "home": home_ins,
        "away": away_ins,
    }
