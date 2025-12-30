# matchdetail/insights_block.py

from __future__ import annotations
from typing import Any, Dict, Optional, List

from db import fetch_all


# ─────────────────────────────────────
#  ✅ 통합: services/insights/utils.py
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


def fmt_avg(num: Any, den: Any, decimals: int = 2) -> float:
    """
    분자/분모에서 평균(float) 을 만들어 준다.
    분모가 0 이면 0.0 리턴.
    """
    v = safe_div(num, den)
    try:
        return round(float(v), decimals)
    except (TypeError, ValueError):
        return 0.0


def normalize_comp(raw: Any) -> str:
    """
    UI에서 내려오는 competition 필터 값을
    서버 내부에서 사용하는 표준 문자열로 정규화.
    """
    if raw is None:
        return "All"

    s = str(raw).strip()
    if not s:
        return "All"

    lower = s.lower()

    # All
    if lower in ("all", "전체"):
        return "All"

    # League
    if lower in ("league", "리그"):
        return "League"

    # Cup
    if lower in ("cup", "domestic cup", "국내컵"):
        return "Cup"

    # UEFA
    if "uefa" in lower or "europe" in lower:
        return "UEFA"

    # ACL
    if "acl" in lower or "afc champions" in lower:
        return "ACL"

    return s


def parse_last_n(raw: Any) -> int:
    """
    UI에서 내려오는 lastN 값을 안전하게 정수 N 으로 변환.
    """
    if raw is None:
        return 0

    # 이미 숫자면 그대로
    if isinstance(raw, int):
        return raw if raw > 0 else 0
    if isinstance(raw, float):
        try:
            n = int(raw)
            return n if n > 0 else 0
        except (TypeError, ValueError):
            return 0

    s = str(raw).strip()
    if not s:
        return 0

    lower = s.lower()
    if lower in ("season", "all", "full season"):
        return 0

    # "Last 5", "Last 10" 등에서 숫자만 추출
    digits = "".join(ch for ch in s if ch.isdigit())
    if digits:
        try:
            n = int(digits)
            return n if n > 0 else 0
        except ValueError:
            return 0

    # 마지막 fallback: 전체 문자열이 숫자일 때
    if s.isdigit():
        n = int(s)
        return n if n > 0 else 0

    return 0


def build_league_ids_for_query(
    stats: Any,
    fallback_league_id: Optional[int],
) -> List[int]:
    """
    stats["insights_filters"]["target_league_ids_last_n"] 가 있으면 그걸 사용.
    없거나 비어있으면 fallback_league_id 한 개로 폴백.
    """
    league_ids: List[int] = []
    filters = {}

    try:
        filters = (stats or {}).get("insights_filters", {}) or {}
    except Exception:
        filters = {}

    raw_list = filters.get("target_league_ids_last_n")

    # 1) 우선: target_league_ids_last_n 사용
    if isinstance(raw_list, list):
        for v in raw_list:
            try:
                league_ids.append(int(v))
            except (TypeError, ValueError):
                continue

        # 중복 제거
        if league_ids:
            seen = set()
            deduped = []
            for lid in league_ids:
                if lid in seen:
                    continue
                seen.add(lid)
                deduped.append(lid)
            league_ids = deduped

    # 2) 폴백: 기본 league_id 한 개
    if not league_ids and fallback_league_id is not None:
        try:
            league_ids = [int(fallback_league_id)]
        except (TypeError, ValueError):
            league_ids = []

    return league_ids


# ─────────────────────────────────────
#  ✅ 통합: services/insights/insights_overall_outcome_totals.py
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
    FT Results(기존 Outcome + Totals) 섹션 생성.

    ✅ 확정 전제(너가 준 DB 출력 기반)
    - matches.status_group: FINISHED / UPCOMING
    - FT 스코어: matches.home_ft / matches.away_ft
      (FINISHED 중 FT NULL 1건 존재 → FT 지표 계산 불가이므로 제외)
    - 코너/카드: match_team_stats.name = 'Corner Kicks' / 'Yellow Cards' / 'Red Cards'
    - PK:
      * 시도 = Goal(detail='Penalty' 또는 'Missed Penalty')  (FT 시간대만)
      * 성공 = Goal(detail='Penalty')
      * Pen Won(AVG) = 시도/경기수
      * Pen Conv%   = 성공/시도 * 100
    - Own Goal:
      * match_events의 Goal(detail='Own Goal')에서 team_id는 '득점으로 기록된 팀(beneficiary)'으로 처리
        (예: fixture 1469785는 away_ft=2인데 own goal event team_id=303(away)로 기록되어 있어
             team_id를 '자책한 팀'으로 보면 스코어와 모순이 발생)
    - FT 범위: minute <= 90 (90+추가시간은 minute=90, extra로 들어오므로 포함)
              연장(105/120 등)은 minute>90으로 제외
    """
    if not season_int:
        return

    # ─────────────────────────────────────
    # 1) Finished(FT 계산 가능) 경기만 기준으로 matches 가져오기
    # ─────────────────────────────────────
    league_ids_for_query = [league_id]
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
          AND lower(m.status_group) = 'finished'
          AND m.home_ft IS NOT NULL
          AND m.away_ft IS NOT NULL
        ORDER BY m.date_utc DESC
    """

    params: List[Any] = []
    params.extend(league_ids_for_query)
    params.extend([season_int, team_id, team_id])

    rows = fetch_all(base_sql, tuple(params))
    if not rows:
        return

    if last_n and last_n > 0:
        rows = rows[:last_n]

    # 경기 집합(분모)
    eff_tot = len(rows)
    eff_home = 0
    eff_away = 0

    # 카운터(총/홈/원정)
    w_t = w_h = w_a = 0
    d_t = d_h = d_a = 0
    l_t = l_h = l_a = 0

    cs_t = cs_h = cs_a = 0
    btts_t = btts_h = btts_a = 0

    tg05_t = tg05_h = tg05_a = 0
    tg15_t = tg15_h = tg15_a = 0
    tg25_t = tg25_h = tg25_a = 0

    tot15_t = tot15_h = tot15_a = 0
    tot25_t = tot25_h = tot25_a = 0

    w_btts_t = w_btts_h = w_btts_a = 0
    w_tot15_t = w_tot15_h = w_tot15_a = 0
    w_tot25_t = w_tot25_h = w_tot25_a = 0

    # First Goal / First Concede (분모=전체 경기, 0:0 포함)
    fg_t = fg_h = fg_a = 0
    fc_t = fc_h = fc_a = 0

    # Corners / Cards (AVG)
    corners_sum_t = corners_sum_h = corners_sum_a = 0
    yellow_sum_t = yellow_sum_h = yellow_sum_a = 0
    red_sum_t = red_sum_h = red_sum_a = 0

    # Pen Won (AVG) / Pen Conv%
    pen_att_t = pen_att_h = pen_att_a = 0
    pen_sc_t = pen_sc_h = pen_sc_a = 0

    # Red Card → Score/Concede
    opp_red_den_t = opp_red_den_h = opp_red_den_a = 0
    opp_red_num_t = opp_red_num_h = opp_red_num_a = 0

    own_red_den_t = own_red_den_h = own_red_den_a = 0
    own_red_num_t = own_red_num_h = own_red_num_a = 0

    fixture_ids: List[int] = [int(r["fixture_id"]) for r in rows if r.get("fixture_id") is not None]

    # ─────────────────────────────────────
    # 2) match_team_stats(코너/카드) bulk fetch
    # ─────────────────────────────────────
    stats_map: Dict[tuple, int] = {}
    if fixture_ids:
        in_fix = ",".join(["%s"] * len(fixture_ids))
        stats_sql = f"""
            SELECT fixture_id, team_id, name, value
            FROM match_team_stats
            WHERE fixture_id IN ({in_fix})
              AND name IN ('Corner Kicks','Yellow Cards','Red Cards')
        """
        s_rows = fetch_all(stats_sql, tuple(fixture_ids))
        for sr in s_rows or []:
            try:
                fx = int(sr.get("fixture_id"))
                tid = int(sr.get("team_id"))
                name = str(sr.get("name") or "").strip()
                raw = sr.get("value")
                # value는 보통 숫자 문자열이지만, 예외는 0으로 처리
                val = int(str(raw).strip()) if raw is not None and str(raw).strip().isdigit() else 0
                stats_map[(fx, tid, name)] = val
            except Exception:
                continue

    # ─────────────────────────────────────
    # 3) match_events(goal/card) bulk fetch (FT 범위만)
    # ─────────────────────────────────────
    events_by_fixture: Dict[int, List[Dict[str, Any]]] = {}
    if fixture_ids:
        in_fix = ",".join(["%s"] * len(fixture_ids))
        ev_sql = f"""
            SELECT
                e.id,
                e.fixture_id,
                e.team_id,
                e.type,
                e.detail,
                e.minute,
                COALESCE(e.extra, 0) AS extra
            FROM match_events e
            WHERE e.fixture_id IN ({in_fix})
              AND lower(e.type) IN ('goal','card')
              AND e.minute <= 90
            ORDER BY e.fixture_id, e.minute, COALESCE(e.extra,0), e.id
        """
        ev_rows = fetch_all(ev_sql, tuple(fixture_ids))
        for ev in ev_rows or []:
            try:
                fx = int(ev.get("fixture_id"))
            except Exception:
                continue
            events_by_fixture.setdefault(fx, []).append(ev)

    def _is_after(ev: Dict[str, Any], ref: tuple) -> bool:
        try:
            return (int(ev.get("minute") or 0), int(ev.get("extra") or 0), int(ev.get("id") or 0)) > ref
        except Exception:
            return False

    # ─────────────────────────────────────
    # 4) 경기별 집계
    # ─────────────────────────────────────
    for r in rows:
        try:
            fx = int(r.get("fixture_id"))
            home_id = int(r.get("home_id"))
            away_id = int(r.get("away_id"))
            home_ft = int(r.get("home_ft"))
            away_ft = int(r.get("away_ft"))
        except Exception:
            continue

        is_home = (home_id == team_id)
        is_away = (away_id == team_id)
        if not (is_home or is_away):
            continue

        if is_home:
            eff_home += 1
            gf = home_ft
            ga = away_ft
            opp_team_id = away_id
        else:
            eff_away += 1
            gf = away_ft
            ga = home_ft
            opp_team_id = home_id

        tg = gf + ga

        # ── W/D/L
        if gf > ga:
            w_t += 1
            if is_home:
                w_h += 1
            else:
                w_a += 1
        elif gf == ga:
            d_t += 1
            if is_home:
                d_h += 1
            else:
                d_a += 1
        else:
            l_t += 1
            if is_home:
                l_h += 1
            else:
                l_a += 1

        # ── Clean Sheet / BTTS
        if ga == 0:
            cs_t += 1
            if is_home:
                cs_h += 1
            else:
                cs_a += 1

        if gf >= 1 and ga >= 1:
            btts_t += 1
            if is_home:
                btts_h += 1
            else:
                btts_a += 1

        # ── TG x+ (우리 득점)
        if gf >= 1:
            tg05_t += 1
            if is_home:
                tg05_h += 1
            else:
                tg05_a += 1
        if gf >= 2:
            tg15_t += 1
            if is_home:
                tg15_h += 1
            else:
                tg15_a += 1
        if gf >= 3:
            tg25_t += 1
            if is_home:
                tg25_h += 1
            else:
                tg25_a += 1

        # ── Total x+ (총득점)
        if tg >= 2:
            tot15_t += 1
            if is_home:
                tot15_h += 1
            else:
                tot15_a += 1
        if tg >= 3:
            tot25_t += 1
            if is_home:
                tot25_h += 1
            else:
                tot25_a += 1

        # ── 콤보
        if (gf > ga) and (gf >= 1 and ga >= 1):
            w_btts_t += 1
            if is_home:
                w_btts_h += 1
            else:
                w_btts_a += 1

        if (gf > ga) and (tg >= 2):
            w_tot15_t += 1
            if is_home:
                w_tot15_h += 1
            else:
                w_tot15_a += 1

        if (gf > ga) and (tg >= 3):
            w_tot25_t += 1
            if is_home:
                w_tot25_h += 1
            else:
                w_tot25_a += 1

        # ── Corners / Cards (우리 팀)
        corners = stats_map.get((fx, team_id, "Corner Kicks"), 0)
        yellow = stats_map.get((fx, team_id, "Yellow Cards"), 0)
        red = stats_map.get((fx, team_id, "Red Cards"), 0)

        corners_sum_t += corners
        yellow_sum_t += yellow
        red_sum_t += red
        if is_home:
            corners_sum_h += corners
            yellow_sum_h += yellow
            red_sum_h += red
        else:
            corners_sum_a += corners
            yellow_sum_a += yellow
            red_sum_a += red

        # ── Events 기반(First Goal/Concede, Pen, Red 이후 득/실점)
        evs = events_by_fixture.get(fx, [])
        goals = [e for e in evs if str(e.get("type") or "").lower() == "goal"]
        cards = [
            e for e in evs
            if str(e.get("type") or "").lower() == "card" and str(e.get("detail") or "") == "Red Card"
        ]

        # First Goal / First Concede (분모는 전체 경기)
        if goals:
            first = goals[0]
            try:
                first_team = int(first.get("team_id")) if first.get("team_id") is not None else None
            except Exception:
                first_team = None

            if first_team == team_id:
                fg_t += 1
                if is_home:
                    fg_h += 1
                else:
                    fg_a += 1
            elif first_team in (home_id, away_id):
                fc_t += 1
                if is_home:
                    fc_h += 1
                else:
                    fc_a += 1
            # team_id가 비정상(None 등)이면 그냥 무시(분모는 유지)

        # Pen Won / Pen Conv% (PK 시도/성공: goal detail 기반)
        pen_attempts = 0
        pen_scored = 0
        for g in goals:
            if g.get("team_id") is None:
                continue
            try:
                g_tid = int(g.get("team_id"))
            except Exception:
                continue
            if g_tid != team_id:
                continue
            det = str(g.get("detail") or "")
            if det == "Penalty":
                pen_attempts += 1
                pen_scored += 1
            elif det == "Missed Penalty":
                pen_attempts += 1

        pen_att_t += pen_attempts
        pen_sc_t += pen_scored
        if is_home:
            pen_att_h += pen_attempts
            pen_sc_h += pen_scored
        else:
            pen_att_a += pen_attempts
            pen_sc_a += pen_scored

        # Opp Red Card → Score
        opp_reds = [c for c in cards if c.get("team_id") is not None and int(c.get("team_id")) == opp_team_id]
        if opp_reds:
            # 가장 이른 상대 레드(정렬되어 있으므로 0번)
            first_red = opp_reds[0]
            ref = (int(first_red.get("minute") or 0), int(first_red.get("extra") or 0), int(first_red.get("id") or 0))

            opp_red_den_t += 1
            if is_home:
                opp_red_den_h += 1
            else:
                opp_red_den_a += 1

            scored_after = any(
                (g.get("team_id") is not None and int(g.get("team_id")) == team_id and _is_after(g, ref))
                for g in goals
            )
            if scored_after:
                opp_red_num_t += 1
                if is_home:
                    opp_red_num_h += 1
                else:
                    opp_red_num_a += 1

        # Own Red Card → Concede
        own_reds = [c for c in cards if c.get("team_id") is not None and int(c.get("team_id")) == team_id]
        if own_reds:
            first_red = own_reds[0]
            ref = (int(first_red.get("minute") or 0), int(first_red.get("extra") or 0), int(first_red.get("id") or 0))

            own_red_den_t += 1
            if is_home:
                own_red_den_h += 1
            else:
                own_red_den_a += 1

            conceded_after = any(
                (g.get("team_id") is not None and int(g.get("team_id")) == opp_team_id and _is_after(g, ref))
                for g in goals
            )
            if conceded_after:
                own_red_num_t += 1
                if is_home:
                    own_red_num_h += 1
                else:
                    own_red_num_a += 1

    # ─────────────────────────────────────
    # 5) insights 키(앱이 쓰는 구조)로 세팅
    # ─────────────────────────────────────
    insights["win_pct"] = {"total": fmt_pct(w_t, eff_tot), "home": fmt_pct(w_h, eff_home), "away": fmt_pct(w_a, eff_away)}
    insights["draw_pct"] = {"total": fmt_pct(d_t, eff_tot), "home": fmt_pct(d_h, eff_home), "away": fmt_pct(d_a, eff_away)}
    insights["loss_pct"] = {"total": fmt_pct(l_t, eff_tot), "home": fmt_pct(l_h, eff_home), "away": fmt_pct(l_a, eff_away)}

    insights["clean_sheet_pct"] = {"total": fmt_pct(cs_t, eff_tot), "home": fmt_pct(cs_h, eff_home), "away": fmt_pct(cs_a, eff_away)}
    insights["btts_pct"] = {"total": fmt_pct(btts_t, eff_tot), "home": fmt_pct(btts_h, eff_home), "away": fmt_pct(btts_a, eff_away)}

    insights["win_and_btts_pct"] = {"total": fmt_pct(w_btts_t, eff_tot), "home": fmt_pct(w_btts_h, eff_home), "away": fmt_pct(w_btts_a, eff_away)}

    insights["team_over05_pct"] = {"total": fmt_pct(tg05_t, eff_tot), "home": fmt_pct(tg05_h, eff_home), "away": fmt_pct(tg05_a, eff_away)}
    insights["team_over15_pct"] = {"total": fmt_pct(tg15_t, eff_tot), "home": fmt_pct(tg15_h, eff_home), "away": fmt_pct(tg15_a, eff_away)}
    insights["team_over25_pct"] = {"total": fmt_pct(tg25_t, eff_tot), "home": fmt_pct(tg25_h, eff_home), "away": fmt_pct(tg25_a, eff_away)}

    insights["over15_pct"] = {"total": fmt_pct(tot15_t, eff_tot), "home": fmt_pct(tot15_h, eff_home), "away": fmt_pct(tot15_a, eff_away)}
    insights["over25_pct"] = {"total": fmt_pct(tot25_t, eff_tot), "home": fmt_pct(tot25_h, eff_home), "away": fmt_pct(tot25_a, eff_away)}

    insights["win_and_over15_pct"] = {"total": fmt_pct(w_tot15_t, eff_tot), "home": fmt_pct(w_tot15_h, eff_home), "away": fmt_pct(w_tot15_a, eff_away)}
    insights["win_and_over25_pct"] = {"total": fmt_pct(w_tot25_t, eff_tot), "home": fmt_pct(w_tot25_h, eff_home), "away": fmt_pct(w_tot25_a, eff_away)}

    insights["first_goal_pct"] = {"total": fmt_pct(fg_t, eff_tot), "home": fmt_pct(fg_h, eff_home), "away": fmt_pct(fg_a, eff_away)}
    insights["first_concede_pct"] = {"total": fmt_pct(fc_t, eff_tot), "home": fmt_pct(fc_h, eff_home), "away": fmt_pct(fc_a, eff_away)}

    insights["corners_avg"] = {"total": fmt_avg(corners_sum_t, eff_tot, 2), "home": fmt_avg(corners_sum_h, eff_home, 2), "away": fmt_avg(corners_sum_a, eff_away, 2)}
    insights["yellow_avg"] = {"total": fmt_avg(yellow_sum_t, eff_tot, 2), "home": fmt_avg(yellow_sum_h, eff_home, 2), "away": fmt_avg(yellow_sum_a, eff_away, 2)}
    insights["red_avg"] = {"total": fmt_avg(red_sum_t, eff_tot, 2), "home": fmt_avg(red_sum_h, eff_home, 2), "away": fmt_avg(red_sum_a, eff_away, 2)}

    insights["pen_won_avg"] = {"total": fmt_avg(pen_att_t, eff_tot, 2), "home": fmt_avg(pen_att_h, eff_home, 2), "away": fmt_avg(pen_att_a, eff_away, 2)}
    insights["pen_conv_pct"] = {"total": fmt_pct(pen_sc_t, pen_att_t), "home": fmt_pct(pen_sc_h, pen_att_h), "away": fmt_pct(pen_sc_a, pen_att_a)}

    insights["opp_red_to_score_pct"] = {"total": fmt_pct(opp_red_num_t, opp_red_den_t), "home": fmt_pct(opp_red_num_h, opp_red_den_h), "away": fmt_pct(opp_red_num_a, opp_red_den_a)}
    insights["own_red_to_concede_pct"] = {"total": fmt_pct(own_red_num_t, own_red_den_t), "home": fmt_pct(own_red_num_h, own_red_den_h), "away": fmt_pct(own_red_num_a, own_red_den_a)}

    # 하위 호환용 nested
    insights["outcome_totals"] = {
        "win_pct": fmt_pct(w_t, eff_tot),
        "draw_pct": fmt_pct(d_t, eff_tot),
        "loss_pct": fmt_pct(l_t, eff_tot),
        "clean_sheet_pct": fmt_pct(cs_t, eff_tot),
        "btts_pct": fmt_pct(btts_t, eff_tot),
        "win_and_btts_pct": fmt_pct(w_btts_t, eff_tot),
        "team_over05_pct": fmt_pct(tg05_t, eff_tot),
        "team_over15_pct": fmt_pct(tg15_t, eff_tot),
        "team_over25_pct": fmt_pct(tg25_t, eff_tot),
        "over15_pct": fmt_pct(tot15_t, eff_tot),
        "over25_pct": fmt_pct(tot25_t, eff_tot),
        "win_and_over15_pct": fmt_pct(w_tot15_t, eff_tot),
        "win_and_over25_pct": fmt_pct(w_tot25_t, eff_tot),
        "first_goal_pct": fmt_pct(fg_t, eff_tot),
        "first_concede_pct": fmt_pct(fc_t, eff_tot),
        "corners_avg": fmt_avg(corners_sum_t, eff_tot, 2),
        "yellow_avg": fmt_avg(yellow_sum_t, eff_tot, 2),
        "red_avg": fmt_avg(red_sum_t, eff_tot, 2),
        "pen_won_avg": fmt_avg(pen_att_t, eff_tot, 2),
        "pen_conv_pct": fmt_pct(pen_sc_t, pen_att_t),
        "opp_red_to_score_pct": fmt_pct(opp_red_num_t, opp_red_den_t),
        "own_red_to_concede_pct": fmt_pct(own_red_num_t, own_red_den_t),
    }






# ─────────────────────────────────────
#  ✅ 통합: services/insights/insights_overall_goalsbytime.py
# ─────────────────────────────────────

def enrich_overall_goals_by_time(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    last_n: Optional[int] = None,  # Last N (없으면 시즌 전체)
) -> None:
    """
    Goals by Time 섹션.
    """
    if not season_int:
        return

    # ─────────────────────────────────────
    # 0) Competition + Last N 기준 league_id 집합 생성
    # ─────────────────────────────────────
    league_ids_for_query: List[int] = []
    try:
        filters = (stats or {}).get("insights_filters", {}) or {}
        target = filters.get("target_league_ids_last_n") or []
    except Exception:
        target = []

    if isinstance(target, list):
        for v in target:
            try:
                league_ids_for_query.append(int(v))
            except (TypeError, ValueError):
                continue

    if not league_ids_for_query:
        league_ids_for_query = [league_id]

    # ─────────────────────────────────────
    # 1) fixture_id 뽑기
    # ─────────────────────────────────────
    placeholders = ",".join(["%s"] * len(league_ids_for_query))

    matches_sql = f"""
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            m.date_utc
        FROM matches m
        WHERE m.league_id IN ({placeholders})
          AND m.season = %s
          AND (m.home_id = %s OR m.away_id = %s)
          AND lower(m.status_group) IN ('finished','ft','fulltime')
        ORDER BY m.date_utc DESC
    """

    params: List[Any] = []
    params.extend(league_ids_for_query)
    params.extend([season_int, team_id, team_id])

    rows = fetch_all(matches_sql, tuple(params))
    if not rows:
        return

    if last_n and last_n > 0:
        rows = rows[:last_n]

    fixture_ids: List[int] = []
    for r in rows:
        try:
            fixture_ids.append(int(r.get("fixture_id")))
        except Exception:
            continue

    if not fixture_ids:
        return

    # ─────────────────────────────────────
    # 2) goal 이벤트 뽑기  ✅ elapsed → minute
    # ─────────────────────────────────────
    placeholders2 = ",".join(["%s"] * len(fixture_ids))

    events_sql = f"""
        SELECT
            e.fixture_id,
            e.team_id,
            e.type,
            e.detail,
            e.minute
        FROM match_events e
        WHERE e.fixture_id IN ({placeholders2})
          AND lower(e.type) = 'goal'
    """

    ev_rows = fetch_all(events_sql, tuple(fixture_ids))
    if not ev_rows:
        return

    # ─────────────────────────────────────
    # 3) 버킷 집계
    # ─────────────────────────────────────
    for_buckets = [0, 0, 0, 0, 0, 0]
    against_buckets = [0, 0, 0, 0, 0, 0]

    def bucket_idx(minute: int) -> int:
        if minute <= 15:
            return 0
        if minute <= 30:
            return 1
        if minute <= 45:
            return 2
        if minute <= 60:
            return 3
        if minute <= 75:
            return 4
        return 5

    for ev in ev_rows:
        try:
            m = ev.get("minute")
            if m is None:
                continue
            minute = int(m)
        except Exception:
            continue

        idx = bucket_idx(minute)

        try:
            ev_team_id = ev.get("team_id")
            if ev_team_id is None:
                continue
            ev_team_id = int(ev_team_id)
        except Exception:
            continue

        is_for = (ev_team_id == team_id)

        if is_for:
            for_buckets[idx] += 1
        else:
            against_buckets[idx] += 1

    insights["goals_by_time_for"] = for_buckets
    insights["goals_by_time_against"] = against_buckets




# ─────────────────────────────────────
#  안전한 int 변환
# ─────────────────────────────────────
def _extract_int(v: Any) -> Optional[int]:
    if v is None:
        return None
    try:
        return int(v)
    except Exception:
        return None


# ─────────────────────────────────────
#  header 구조 그대로 파싱
# ─────────────────────────────────────
def _get_meta_from_header(header: Dict[str, Any]) -> Dict[str, Optional[int]]:
    """
    header 스키마에 100% 맞게 파싱:
      - league_id → header["league_id"]
      - season → header["season"]
      - home_team_id → header["home"]["id"]
      - away_team_id → header["away"]["id"]
    """
    league_id = _extract_int(header.get("league_id"))
    season = _extract_int(header.get("season"))

    home_block = header.get("home") or {}
    away_block = header.get("away") or {}

    home_team_id = _extract_int(home_block.get("id"))
    away_team_id = _extract_int(away_block.get("id"))

    return {
        "league_id": league_id,
        "season_int": season,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
    }


def _get_last_n_from_header(header: Dict[str, Any]) -> int:
    filters = header.get("filters") or {}
    raw_last_n = filters.get("last_n") or header.get("last_n")
    return parse_last_n(raw_last_n)


def _get_filters_from_header(header: Dict[str, Any]) -> Dict[str, Any]:
    """
    헤더에 이미 들어있는 filters 블록을 그대로 옮겨오되,
    last_n 값은 항상 존재하도록 정리해서 insights_overall.filters 로 내려준다.
    (여기서는 "선택된 값"만 다루고, 실제 league_id 집합은 아래 헬퍼에서 만든다)
    """
    header_filters = header.get("filters") or {}

    # 방어적으로 복사
    filters: Dict[str, Any] = dict(header_filters)

    # 선택된 last_n 라벨을 헤더에서 확보
    raw_last_n = header_filters.get("last_n") or header.get("last_n")
    if raw_last_n is not None:
        filters["last_n"] = raw_last_n

    # comp 같은 다른 필터 값이 header.filters 안에 있으면 그대로 유지
    return filters


# ─────────────────────────────────────
#  Competition + Last N 에 따른 league_id 집합 만들기
#   → stats["insights_filters"]["target_league_ids_last_n"] 로 사용
# ─────────────────────────────────────
def _build_insights_filters_for_team(
    *,
    league_id: int,
    season_int: int,
    team_id: int,
    comp_raw: Any,
    last_n: int,
) -> Dict[str, Any]:
    filters: Dict[str, Any] = {}

    # 시즌이나 팀이 없으면 아무것도 하지 않는다.
    if season_int is None or team_id is None:
        return filters

    # 🔥 중요:
    #   last_n == 0 (Season 2025 같은 시즌 모드) 여도 여기서는
    #   "이 팀이 그 시즌에 뛴 league_id 집합"을 반드시 만든다.
    #   - last_n 은 나중에 경기 수 자를 때만 쓰고
    #   - 어떤 대회들을 포함할지는 comp_std / target_league_ids_last_n 로 제어한다.
    comp_std = normalize_comp(comp_raw)

    # 이 팀이 해당 시즌에 실제로 뛴 경기들의 league_id 목록 + league 이름 로딩
    rows = fetch_all(
        """
        SELECT DISTINCT
            m.league_id,
            l.name      AS league_name,
            l.country   AS league_country
        FROM matches m
        JOIN leagues l ON l.id = m.league_id
        WHERE m.season = %s
          AND (m.home_id = %s OR m.away_id = %s)
        """,
        (season_int, team_id, team_id),
    )

    if not rows:
        # 그래도 comp / last_n 정보는 채워서 내려주자
        filters["comp_std"] = comp_std
        filters["last_n_int"] = int(last_n)
        return filters

    all_ids: List[int] = []
    cup_ids: List[int] = []
    uefa_ids: List[int] = []
    acl_ids: List[int] = []
    name_pairs: List[tuple[int, str]] = []

    for r in rows:
        lid = r.get("league_id")
        name = (r.get("league_name") or "").strip()
        if lid is None:
            continue
        try:
            lid_int = int(lid)
        except (TypeError, ValueError):
            continue

        all_ids.append(lid_int)
        name_pairs.append((lid_int, name))

        lower = name.lower()

        # 대략적인 Cup 판별 (FA Cup, League Cup, Copa, 컵, 杯 등)
        if (
            "cup" in lower
            or "copa" in lower
            or "컵" in lower
            or "taça" in lower
            or "杯" in lower
        ):
            cup_ids.append(lid_int)

        # UEFA 계열 (UCL, UEL, UECL 등)
        if (
            "uefa" in lower
            or "champions league" in lower
            or "europa league" in lower
            or "conference league" in lower
        ):
            uefa_ids.append(lid_int)

        # ACL / AFC 챔피언스리그 계열
        if "afc" in lower or "acl" in lower or "afc champions league" in lower:
            acl_ids.append(lid_int)

    # 중복 제거용 헬퍼
    def _dedupe(seq: List[int]) -> List[int]:
        seen = set()
        out: List[int] = []
        for v in seq:
            if v in seen:
                continue
            seen.add(v)
            out.append(v)
        return out

    target_ids: List[int]

    if comp_std == "All":
        # 팀이 이 시즌에 뛴 모든 대회
        target_ids = all_ids
    elif comp_std == "League":
        # 현재 경기의 리그만
        try:
            target_ids = [int(league_id)]
        except (TypeError, ValueError):
            target_ids = all_ids
    elif comp_std == "Cup":
        target_ids = cup_ids
    elif comp_std == "UEFA":
        target_ids = uefa_ids
    elif comp_std == "ACL":
        target_ids = acl_ids
    else:
        # 개별 대회 이름: 먼저 완전 일치, 없으면 부분 일치로 검색
        target_ids = []
        comp_lower = (comp_raw or "").strip().lower()

        for lid_int, name in name_pairs:
            if name.lower() == comp_lower:
                target_ids.append(lid_int)

        # 완전 일치가 없으면 부분 일치
        if not target_ids and comp_lower:
            for lid_int, name in name_pairs:
                if comp_lower in name.lower():
                    target_ids.append(lid_int)

    # 아무 것도 못 찾았으면 안전하게 폴백
    if not target_ids:
        if comp_std in ("League",):
            # League 에서는 현재 리그만이라도 보장
            try:
                target_ids = [int(league_id)]
            except (TypeError, ValueError):
                target_ids = all_ids
        else:
            # 그 외에는 All 과 동일하게
            target_ids = all_ids

    target_ids = _dedupe(target_ids)

    filters["target_league_ids_last_n"] = target_ids
    filters["comp_std"] = comp_std
    filters["last_n_int"] = int(last_n)

    return filters



# ─────────────────────────────────────
#  Game Sample 홈/원정 분포 계산
# ─────────────────────────────────────
def _compute_events_sample_home_away(
    *,
    season_int: Optional[int],
    team_id: Optional[int],
    league_id: Optional[int],
    filters: Dict[str, Any],
    events_sample: Optional[int],
) -> Dict[str, Optional[int]]:
    """
    stats["insights_filters"]["target_league_ids_last_n"] 기준으로
    해당 팀의 시즌 경기들 중 홈/원정 개수를 세고,
    그 비율을 events_sample 에 맞게 스케일링해서
    events_sample_home / events_sample_away 로 내려준다.
    """
    out: Dict[str, Optional[int]] = {
        "events_sample_home": None,
        "events_sample_away": None,
    }

    if not season_int or not team_id or not events_sample or events_sample <= 0:
        return out

    # comp 필터에서 사용하는 리그 집합
    target_league_ids = filters.get("target_league_ids_last_n")

    # 비어 있으면 현재 리그만이라도 사용
    if not target_league_ids:
        if league_id is not None:
            try:
                target_league_ids = [int(league_id)]
            except (TypeError, ValueError):
                target_league_ids = []
        else:
            target_league_ids = []

    if not target_league_ids:
        return out

    placeholders = ", ".join(["%s"] * len(target_league_ids))
    sql = f"""
        SELECT home_id, away_id
        FROM matches
        WHERE season = %s
          AND league_id IN ({placeholders})
          AND (home_id = %s OR away_id = %s)
    """

    params: List[Any] = [season_int]
    params.extend(target_league_ids)
    params.extend([team_id, team_id])

    rows = fetch_all(sql, tuple(params))

    raw_home = 0
    raw_away = 0
    for r in rows:
        hid = r.get("home_id")
        aid = r.get("away_id")
        if hid == team_id:
            raw_home += 1
        elif aid == team_id:
            raw_away += 1

    raw_total = raw_home + raw_away
    if raw_total <= 0:
        return out

    total = int(events_sample)
    # 비율 유지하면서 total 에 맞게 스케일링
    factor = float(total) / float(raw_total)

    est_home = int(round(raw_home * factor))
    # 라운딩으로 인해 합이 안맞는 것 보정
    est_home = max(0, min(est_home, total))
    est_away = max(0, total - est_home)

    out["events_sample_home"] = est_home
    out["events_sample_away"] = est_away
    return out


# ─────────────────────────────────────
#  한 팀(홈/원정) 계산
# ─────────────────────────────────────
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

    # Competition + Last N 기준 league_id 집합 생성
    side_filters = _build_insights_filters_for_team(
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        comp_raw=comp_raw,
        last_n=last_n,
    )

    merged_filters: Dict[str, Any] = dict(header_filters)
    merged_filters.update(side_filters)

    # 섹션들에서 공통으로 사용할 필터 정보
    stats["insights_filters"] = merged_filters

    # ✅ 유지: Outcome + Totals
    enrich_overall_outcome_totals(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=0,
        last_n=last_n,
    )

    # ✅ 유지: Goals by Time
    enrich_overall_goals_by_time(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
    )


    # ───────── Game Sample 홈/원정 분포 계산 ─────────
    events_sample = insights.get("events_sample")
    if isinstance(events_sample, (int, float)) and events_sample > 0:
        sample_split = _compute_events_sample_home_away(
            season_int=season_int,
            team_id=team_id,
            league_id=league_id,
            filters=stats.get("insights_filters", {}),
            events_sample=int(events_sample),
        )
        if sample_split.get("events_sample_home") is not None:
            insights["events_sample_home"] = sample_split["events_sample_home"]
        if sample_split.get("events_sample_away") is not None:
            insights["events_sample_away"] = sample_split["events_sample_away"]

    return insights


# ─────────────────────────────────────
#  필터 옵션용 헬퍼
# ─────────────────────────────────────
def _build_comp_options_for_team(
    *, league_id: int, season_int: int, team_id: int
) -> List[str]:
    """
    이 팀이 해당 시즌에 실제로 뛴 Competition 옵션 생성.

    - 리그: 현재 경기 league_id 에 해당하는 리그 이름 1개만 추가
    - 컵 / UEFA / ACL: 개별 대회명 + 조건부 그룹 라벨(Cup / Europe (UEFA) / Continental)
    """
    if season_int is None or team_id is None:
        return []

    rows = fetch_all(
        """
        SELECT DISTINCT
            m.league_id,
            l.name      AS league_name
        FROM matches m
        JOIN leagues l ON l.id = m.league_id
        WHERE m.season = %s
          AND (m.home_id = %s OR m.away_id = %s)
        """,
        (season_int, team_id, team_id),
    )

    if not rows:
        return []

    comp_options: List[str] = ["All"]

    # 리그 / 컵 / UEFA / ACL 를 분리해서 모아두기
    league_names: List[str] = []
    league_name_by_id: Dict[int, str] = {}

    cup_names: List[str] = []
    uefa_names: List[str] = []
    acl_names: List[str] = []

    for r in rows:
        lid = r.get("league_id")
        name = (r.get("league_name") or "").strip()
        if not name or lid is None:
            continue
        try:
            lid_int = int(lid)
        except (TypeError, ValueError):
            continue

        lower = name.lower()

        is_cup = (
            "cup" in lower
            or "copa" in lower
            or "컵" in lower
            or "taça" in lower
            or "杯" in lower
        )
        is_uefa = (
            "uefa" in lower
            or "champions league" in lower
            or "europa league" in lower
            or "conference league" in lower
        )
        is_acl = (
            "afc" in lower
            or "acl" in lower
            or "afc champions league" in lower
        )

        # 리그(국내 대회) 후보
        if not (is_cup or is_uefa or is_acl):
            league_names.append(name)
            league_name_by_id[lid_int] = name

        # 컵 / UEFA / ACL 후보 목록
        if is_cup:
            cup_names.append(name)
        if is_uefa:
            uefa_names.append(name)
        if is_acl:
            acl_names.append(name)

    # ── 리그 이름 선택: 현재 match 의 league_id 를 최우선 ──
    league_name_for_team: Optional[str] = None
    try:
        match_league_id = int(league_id)
    except (TypeError, ValueError):
        match_league_id = None

    if match_league_id is not None and match_league_id in league_name_by_id:
        league_name_for_team = league_name_by_id[match_league_id]
    elif league_names:
        league_name_for_team = league_names[0]

    if league_name_for_team and league_name_for_team not in comp_options:
        comp_options.append(league_name_for_team)

    # 중복 없이 추가하는 헬퍼
    def _append_unique(names: List[str]) -> None:
        for n in names:
            if n not in comp_options:
                comp_options.append(n)

    # 컵: "Cup" + 개별 컵 이름들
    if cup_names:
        if "Cup" not in comp_options:
            comp_options.append("Cup")
        _append_unique(sorted(set(cup_names)))

    # UEFA: Europe (UEFA) + UCL/UEL/Conference 개별 이름
    if uefa_names:
        if len(set(uefa_names)) >= 2 and "Europe (UEFA)" not in comp_options:
            comp_options.append("Europe (UEFA)")
        _append_unique(sorted(set(uefa_names)))

    # ACL: Continental + ACL 관련 대회명들
    if acl_names:
        if "Continental" not in comp_options:
            comp_options.append("Continental")
        _append_unique(sorted(set(acl_names)))

    return comp_options


def _build_last_n_options_for_match(
    *, home_team_id: int, away_team_id: int
) -> List[str]:
    """
    두 팀이 가진 시즌 목록을 기반으로 Last N 옵션 뒤에
    Season YYYY 옵션들을 붙여서 내려준다.
    (교집합이 비면 합집합을 사용)
    """
    base_options: List[str] = ["Last 3", "Last 5", "Last 7", "Last 10"]

    if home_team_id is None or away_team_id is None:
        return base_options

    def _load_seasons(team_id: int) -> List[int]:
        rows = fetch_all(
            """
            SELECT DISTINCT season
            FROM matches
            WHERE home_id = %s OR away_id = %s
            ORDER BY season DESC
            """,
            (team_id, team_id),
        )
        seasons: List[int] = []
        for r in rows:
            s = r.get("season")
            if s is None:
                continue
            try:
                seasons.append(int(s))
            except (TypeError, ValueError):
                continue
        return seasons

    home_seasons = set(_load_seasons(home_team_id))
    away_seasons = set(_load_seasons(away_team_id))

    inter = home_seasons & away_seasons
    if inter:
        seasons_sorted = sorted(inter, reverse=True)
    else:
        seasons_sorted = sorted(home_seasons | away_seasons, reverse=True)

    for s in seasons_sorted:
        label = f"Season {s}"
        if label not in base_options:
            base_options.append(label)

    return base_options


def _merge_options(*lists: List[str]) -> List[str]:
    seen = set()
    merged: List[str] = []
    for lst in lists:
        for v in lst:
            if v in seen:
                continue
            seen.add(v)
            merged.append(v)
    return merged

def _build_insights_overall_sections_meta() -> List[Dict[str, Any]]:
    """
    앱이 동적으로 Insights 탭을 렌더링할 수 있게
    섹션 정의(메타)만 내려준다.
    - 기존 수치 키(win_pct, goals_by_time_for 등)는 그대로 유지
    - 앱은 sections를 보고 어떤 섹션을 어떤 렌더러로 그릴지 결정
    """
    return [
        
{
    "id": "outcome_totals",
    "title": "FT Results",
    "renderer": "metrics_table",
    "metrics": [
        {"key": "win_pct", "label": "FT W", "format": "pct_hoa"},
        {"key": "draw_pct", "label": "FT D", "format": "pct_hoa"},
        {"key": "loss_pct", "label": "FT L", "format": "pct_hoa"},
        {"key": "clean_sheet_pct", "label": "FT Clean Sheet", "format": "pct_hoa"},
        {"key": "btts_pct", "label": "FT BTTS", "format": "pct_hoa"},
        {"key": "win_and_btts_pct", "label": "FT W & BTTS", "format": "pct_hoa"},

        {"key": "team_over05_pct", "label": "FT TG 0.5+", "format": "pct_hoa"},
        {"key": "team_over15_pct", "label": "FT TG 1.5+", "format": "pct_hoa"},
        {"key": "team_over25_pct", "label": "FT TG 2.5+", "format": "pct_hoa"},

        {"key": "over15_pct", "label": "FT Total 1.5+", "format": "pct_hoa"},
        {"key": "over25_pct", "label": "FT Total 2.5+", "format": "pct_hoa"},

        {"key": "win_and_over15_pct", "label": "FT W & Total 1.5+", "format": "pct_hoa"},
        {"key": "win_and_over25_pct", "label": "FT W & Total 2.5+", "format": "pct_hoa"},

        {"key": "first_goal_pct", "label": "FT First Goal", "format": "pct_hoa"},
        {"key": "first_concede_pct", "label": "FT First Concede", "format": "pct_hoa"},

        {"key": "corners_avg", "label": "FT Corners (AVG)", "format": "avg_hoa"},
        {"key": "yellow_avg", "label": "FT Yellow Card (AVG)", "format": "avg_hoa"},
        {"key": "red_avg", "label": "FT Red Card (AVG)", "format": "avg_hoa"},

        {"key": "pen_won_avg", "label": "FT Pen Won (AVG)", "format": "avg_hoa"},
        {"key": "pen_conv_pct", "label": "FT Pen Conv%", "format": "pct_hoa"},

        {"key": "opp_red_to_score_pct", "label": "FT Opp Red Card → Score", "format": "pct_hoa"},
        {"key": "own_red_to_concede_pct", "label": "FT Own Red Card → Concede", "format": "pct_hoa"},
    ]
},
        {
            "id": "goals_by_time",
            "title": "Goals by Time",
            "renderer": "goals_by_time",
            # 이 섹션은 배열 두 개를 사용
            "for_key": "goals_by_time_for",
            "against_key": "goals_by_time_against",
            # 버킷 정의(앱에서 라벨 만들 때 사용)
            "buckets": [
                {"from": 0, "to": 15},
                {"from": 16, "to": 30},
                {"from": 31, "to": 45},
                {"from": 46, "to": 60},
                {"from": 61, "to": 75},
                {"from": 76, "to": 90},
            ],
        },
    ]



# ─────────────────────────────────────
#  전체 insights 블록 생성
# ─────────────────────────────────────
def build_insights_overall_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if not header:
        return None

    meta = _get_meta_from_header(header)

    league_id = meta["league_id"]
    season_int = meta["season_int"]
    home_team_id = meta["home_team_id"]
    away_team_id = meta["away_team_id"]

    if None in (league_id, season_int, home_team_id, away_team_id):
        return None

    # 1) 선택된 last_n (라벨 → 숫자) 파싱
    last_n = _get_last_n_from_header(header)

    # 2) 헤더의 필터 블록 (라벨 그대로, comp / last_n 문자열 등)
    filters_block = _get_filters_from_header(header)
    comp_raw = filters_block.get("comp")

    # 3) Season YYYY 라벨이면 시즌을 바꾸고 last_n 은 0(전체 시즌)으로 사용
    season_for_calc = season_int
    last_n_for_calc = last_n

    raw_last_n_label = filters_block.get("last_n") or header.get("last_n")
    if isinstance(raw_last_n_label, str):
        s = raw_last_n_label.strip()
        lower = s.lower()
        if lower.startswith("season"):
            # 예: "Season 2024" → 2024
            digits = "".join(ch for ch in s if ch.isdigit())
            if digits:
                try:
                    season_override = int(digits)
                    season_for_calc = season_override
                    last_n_for_calc = 0  # 전체 시즌 모드
                except ValueError:
                    pass

    # ───────── 홈 / 어웨이 인사이트 계산 ─────────
    home_ins = _build_side_insights(
        league_id=league_id,
        season_int=season_for_calc,
        team_id=home_team_id,
        last_n=last_n_for_calc,
        comp_raw=comp_raw,
        header_filters=filters_block,
    )
    away_ins = _build_side_insights(
        league_id=league_id,
        season_int=season_for_calc,
        team_id=away_team_id,
        last_n=last_n_for_calc,
        comp_raw=comp_raw,
        header_filters=filters_block,
    )

    # ───────── UI에서 쓸 필터 옵션 리스트 구성 (동적 생성) ─────────
    # 1) 팀별 comp 옵션  → 시즌 기준은 season_for_calc 사용
    comp_opts_home = _build_comp_options_for_team(
        league_id=league_id,
        season_int=season_for_calc,
        team_id=home_team_id,
    )
    comp_opts_away = _build_comp_options_for_team(
        league_id=league_id,
        season_int=season_for_calc,
        team_id=away_team_id,
    )

    # 두 팀 합친(옛날과 동일한) 전체 리스트
    comp_options_union = _merge_options(comp_opts_home, comp_opts_away)
    if not comp_options_union:
        comp_options_union = ["All", "League"]

    # 팀별 리스트가 비어 있으면 최소 기본값은 보장
    if not comp_opts_home:
        comp_opts_home = ["All", "League"]
    if not comp_opts_away:
        comp_opts_away = ["All", "League"]

    # 현재 선택된 comp 라벨
    comp_label_raw = filters_block.get("comp") or "All"
    comp_label = str(comp_label_raw).strip() or "All"

    def _pick_selected(options: List[str]) -> str:
        if comp_label in options:
            return comp_label
        return options[0] if options else "All"

    comp_label_home = _pick_selected(comp_opts_home)
    comp_label_away = _pick_selected(comp_opts_away)

    # 2) last_n 옵션 (두 팀 시즌 정보를 기반으로)
    last_n_options = _build_last_n_options_for_match(
        home_team_id=home_team_id,
        away_team_id=away_team_id,
    )

    last_n_label_raw = filters_block.get("last_n") or "Last 10"
    last_n_label = str(last_n_label_raw).strip() or "Last 10"
    if last_n_label not in last_n_options:
        last_n_options.insert(0, last_n_label)

    filters_for_client: Dict[str, Any] = {
        # 예전과 동일한 전체 comp 옵션 (두 팀 합친 집합)
        "comp": {
            "options": comp_options_union,
            "selected": comp_label,
        },
        # 팀별 comp 옵션
        "comp_home": {
            "options": comp_opts_home,
            "selected": comp_label_home,
        },
        "comp_away": {
            "options": comp_opts_away,
            "selected": comp_label_away,
        },
        "last_n": {
            "options": last_n_options,
            "selected": last_n_label,
        },
    }

    return {
        "league_id": league_id,
        "season": season_for_calc,
        "last_n": last_n_for_calc,
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "filters": filters_for_client,

        # ✅ NEW: 동적 렌더링용 섹션 정의
        "sections": _build_insights_overall_sections_meta(),

        "home": home_ins,
        "away": away_ins,
    }


