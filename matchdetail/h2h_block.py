from typing import Any, Dict, Optional, List

from db import fetch_all


def _safe_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def build_h2h_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    맞대결(H2H) / 최근 경기 요약 블록.

    - scope: H2H_ONLY (두 팀 맞대결만)
    - sample: 기본 LAST10 (최근 10경기)
    - venue: ALL
    - competition: ALL
    """
    fixture_id = header.get("fixture_id")
    league_id = header.get("league_id")
    season = header.get("season")

    home = header.get("home") or {}
    away = header.get("away") or {}

    home_id = _safe_int(home.get("id"))
    away_id = _safe_int(away.get("id"))

    if not home_id or not away_id:
        return None

    # 최근 H2H 10경기 (두 팀 맞대결 + FT 경기만)
    sql = f"""
        SELECT
            m.fixture_id,
            m.date_utc,
            m.league_id,
            l.name AS league_name,
            m.season,
            m.home_id,
            m.away_id,
            th.name AS home_name,
            ta.name AS away_name,
            m.home_ft,
            m.away_ft
        FROM matches m
        LEFT JOIN leagues l ON l.id = m.league_id
        LEFT JOIN teams   th ON th.id = m.home_id
        LEFT JOIN teams   ta ON ta.id = m.away_id
        WHERE
            (
                (m.home_id = {home_id} AND m.away_id = {away_id})
                OR
                (m.home_id = {away_id} AND m.away_id = {home_id})
            )
            AND m.status = 'FT'          -- 🔥 여기 수정: status_group 이 아니라 status = 'FT'
        ORDER BY m.date_utc DESC
        LIMIT 10
    """

    rows_raw: List[Dict[str, Any]] = fetch_all(sql)

    if not rows_raw:
        return None

    # ─ 요약 통계 계산 ─
    sample_count = 0
    wins = draws = losses = 0
    total_goals = 0
    btts = o15 = o25 = o35 = 0
    clean_sheet_any = 0

    for r in rows_raw:
        hf = _safe_int(r.get("home_ft"))
        af = _safe_int(r.get("away_ft"))
        h_id = _safe_int(r.get("home_id"))
        a_id = _safe_int(r.get("away_id"))

        if hf is None or af is None:
            continue

        sample_count += 1
        total_goals += hf + af

        # header.home.id 기준 승무패
        if h_id == home_id:
            my_goals, opp_goals = hf, af
        elif a_id == home_id:
            my_goals, opp_goals = af, hf
        else:
            continue

        if my_goals > opp_goals:
            wins += 1
        elif my_goals == opp_goals:
            draws += 1
        else:
            losses += 1

        if hf > 0 and af > 0:
            btts += 1
        if hf + af > 1:
            o15 += 1
        if hf + af > 2:
            o25 += 1
        if hf + af > 3:
            o35 += 1
        if hf == 0 or af == 0:
            clean_sheet_any += 1

    if sample_count == 0:
        avg_goals = 0.0
        btts_rate = ou15_rate = ou25_rate = ou35_rate = clean_rate = 0
    else:
        avg_goals = round(total_goals / sample_count, 2)
        btts_rate = round(btts * 100 / sample_count)
        ou15_rate = round(o15 * 100 / sample_count)
        ou25_rate = round(o25 * 100 / sample_count)
        ou35_rate = round(o35 * 100 / sample_count)
        clean_rate = round(clean_sheet_any * 100 / sample_count)

    summary = {
        "sample_count": sample_count,
        "wins": wins,
        "draws": draws,
        "losses": losses,
        "avg_goals": avg_goals,
        "btts_rate": btts_rate,
        "ou15_rate": ou15_rate,
        "ou25_rate": ou25_rate,
        "ou35_rate": ou35_rate,
        "clean_sheet_rate": clean_rate,
    }

    # ─ 행 리스트 변환 ─
    rows: List[Dict[str, Any]] = []
    for r in rows_raw:
        hf = _safe_int(r.get("home_ft"))
        af = _safe_int(r.get("away_ft"))
        h_id = _safe_int(r.get("home_id"))

        rows.append(
            {
                "fixture_id": r.get("fixture_id"),
                "date_utc": str(r.get("date_utc")) if r.get("date_utc") is not None else None,
                "league_id": r.get("league_id"),
                "league_name": r.get("league_name"),
                "season": r.get("season"),
                "home_id": h_id,
                "away_id": _safe_int(r.get("away_id")),
                "home_name": r.get("home_name"),
                "away_name": r.get("away_name"),
                "home_ft": hf,
                "away_ft": af,
                "is_home_side": bool(h_id == home_id),
            }
        )

    return {
        "summary": summary,
        "rows": rows,
    }
