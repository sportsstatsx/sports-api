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
        # 호출 측에서 None 체크를 해야 하지만, 혹시 몰라 방어
        return datetime.now().date().isoformat()

    s = date_str.strip()

    # 이미 yyyy-mm-dd 형태면 그대로 사용
    if len(s) >= 10:
        only_date = s[:10]
        try:
            dt = datetime.fromisoformat(only_date)
            return dt.date().isoformat()
        except Exception:
            # fromisoformat 실패 시에도 그대로 잘라서 쓴다
            return only_date
    return s


# ─────────────────────────────────────
#  1) 홈 상단 리그 탭용
# ─────────────────────────────────────


def get_home_leagues(date_str: str) -> List[Dict[str, Any]]:
    """
    주어진 날짜(date_str)에 실제 경기가 편성된 리그 목록을 돌려준다.

    반환 예시:
    [
      {
        "league_id": 39,
        "league_name": "Premier League",
        "country": "England",
        "logo": "https://...",
        "season": 2025,
      },
      ...
    ]
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

    - date_str 에 가장 가까운 매치데이를 current_date 로 잡고
    - 그 주변 모든 매치데이를 items 리스트에 담아준다.

    반환 예시:
    {
      "current_date": "2025-11-15",
      "items": [
        {"date": "2025-11-10", "matches": 8},
        {"date": "2025-11-15", "matches": 6},
        {"date": "2025-11-20", "matches": 7},
        ...
      ]
    }
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
            # target 과의 차이가 더 작은 날짜를 current 로 선택
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
    # match_date 가 date 객체이든 문자열이든 str() 하면 YYYY-MM-DD 형태가 나옴
    return str(match_date)


def get_next_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    """지정 날짜 이후(포함) 첫 매치데이."""
    return _find_matchday(date_str, league_id, direction="next")


def get_prev_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    """지정 날짜 이전(포함) 마지막 매치데이."""
    return _find_matchday(date_str, league_id, direction="prev")


# ─────────────────────────────────────
#  4) 팀 시즌 스탯 (team_season_stats)
# ─────────────────────────────────────


def get_team_season_stats(team_id: int, league_id: int) -> Optional[Dict[str, Any]]:
    """
    team_season_stats 테이블에서
    (league_id, team_id) 에 해당하는 가장 최신 season 한 줄을 가져온다.

    value 컬럼은 API-Football /teams/statistics 의 JSON 이거나,
    그와 동등한 구조의 full_json 이라고 가정한다.

    여기서 일부 고급 지표(insights_overall.*)가 추가/보정된다.
    (예: shots_per_match, shots_on_target_pct, btts/오버/콤보 등)
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

    # value 컬럼(JSONB 혹은 TEXT)을 파이썬 dict 로 정규화
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

    # ─────────────────────────────────────────
    # Shots / Shots on Target 기반 고급 지표
    #   - 경기당 슈팅 수 (shots_per_match)
    #   - 슈팅 대비 유효슈팅 비율 (shots_on_target_pct)
    # ─────────────────────────────────────────
    shots = stats.get("shots") or {}
    if isinstance(shots, dict) and matches_total > 0:
        total_block = shots.get("total") or {}
        on_block = shots.get("on") or {}

        # API-Football 구조 상 total 키가 없을 수도 있으므로
        # home + away 합으로 보정
        st_home = total_block.get("home") or 0
        st_away = total_block.get("away") or 0
        st_total = total_block.get("total") or 0
        if not st_total:
            st_total = (st_home or 0) + (st_away or 0)

        so_home = on_block.get("home") or 0
        so_away = on_block.get("away") or 0
        so_total = on_block.get("total") or 0
        if not so_total:
            so_total = (so_home or 0) + (so_away or 0)

        def fmt_avg(n, m):
            v = safe_div(n, m)
            # 숫자로 내려주고, 클라이언트에서 포맷팅 (소수 자리수 등) 처리
            return round(v, 2) if v > 0 else 0.0

        def fmt_pct(n, d):
            v = safe_div(n, d)
            return int(round(v * 100)) if v > 0 else 0

        # 이미 값이 있다면 덮어쓰지 않고, 없으면 새로 채움
        insights.setdefault(
            "shots_per_match",
            {
                "total": fmt_avg(st_total, matches_total),
                "home": fmt_avg(st_home, matches_home or matches_total),
                "away": fmt_avg(st_away, matches_away or matches_total),
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

    # ─────────────────────────────────────────
    # Outcome & Totals / Result Combos용 고급 지표
    #  - BTTS, 팀 오버, 전체 오버, win+over2.5, lose+BTTS, 골득실 평균
    #   → matches 테이블의 실제 스코어 기반 계산
    # ─────────────────────────────────────────

    league_id_db = row["league_id"]
    season_db = row["season"]
    team_id_db = row["team_id"]

    def _pct(n: int, d: int) -> int:
        if not d or not n:
            return 0
        return int(round(100.0 * float(n) / float(d)))

    def _avg(n: float, d: int) -> float:
        if not d:
            return 0.0
        return float(n) / float(d)

    # matches 에서 이 팀의 경기 스코어를 모두 수집
    match_rows = fetch_all(
        """
        SELECT home_id, away_id, home_ft, away_ft
        FROM matches
        WHERE league_id = %s
          AND season    = %s
          AND status_group = 'FINISHED'
          AND (home_id = %s OR away_id = %s)
        """,
        (league_id_db, season_db, team_id_db, team_id_db),
    )

    total_matches = 0
    home_matches = 0
    away_matches = 0

    btts_total = btts_home = btts_away = 0
    team_o05_total = team_o05_home = team_o05_away = 0
    team_o15_total = team_o15_home = team_o15_away = 0
    o15_total = o15_home = o15_away = 0
    o25_total = o25_home = o25_away = 0
    win_o25_total = win_o25_home = win_o25_away = 0
    lose_btts_total = lose_btts_home = lose_btts_away = 0

    gd_sum_total = gd_sum_home = gd_sum_away = 0.0

    for r in match_rows:
        home_id = r["home_id"]
        away_id = r["away_id"]
        home_ft = r.get("home_ft")
        away_ft = r.get("away_ft")

        # 스코어가 비어 있으면 스킵
        if home_ft is None or away_ft is None:
            continue

        is_home = home_id == team_id_db
        if is_home:
            gf = home_ft
            ga = away_ft
        else:
            gf = away_ft
            ga = home_ft

        total_matches += 1
        if is_home:
            home_matches += 1
        else:
            away_matches += 1

        # BTTS
        if gf > 0 and ga > 0:
            btts_total += 1
            if is_home:
                btts_home += 1
            else:
                btts_away += 1

        # 팀 오버 0.5 / 1.5
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

        # 전체 득점
        total_goals = home_ft + away_ft

        if total_goals >= 2:  # over 1.5
            o15_total += 1
            if is_home:
                o15_home += 1
            else:
                o15_away += 1

        if total_goals >= 3:  # over 2.5
            o25_total += 1
            if is_home:
                o25_home += 1
            else:
                o25_away += 1

        # win + over2.5
        if gf > ga and total_goals >= 3:
            win_o25_total += 1
            if is_home:
                win_o25_home += 1
            else:
                win_o25_away += 1

        # lose + BTTS
        if gf < ga and gf > 0 and ga > 0:
            lose_btts_total += 1
            if is_home:
                lose_btts_home += 1
            else:
                lose_btts_away += 1

        # 골득실 합
        gd = float(gf - ga)
        gd_sum_total += gd
        if is_home:
            gd_sum_home += gd
        else:
            gd_sum_away += gd

    if total_matches > 0:
        insights.setdefault(
            "btts_pct",
            {
                "total": _pct(btts_total, total_matches),
                "home": _pct(btts_home, home_matches) if home_matches else 0,
                "away": _pct(btts_away, away_matches) if away_matches else 0,
            },
        )
        insights.setdefault(
            "team_over05_pct",
            {
                "total": _pct(team_o05_total, total_matches),
                "home": _pct(team_o05_home, home_matches) if home_matches else 0,
                "away": _pct(team_o05_away, away_matches) if away_matches else 0,
            },
        )
        insights.setdefault(
            "team_over15_pct",
            {
                "total": _pct(team_o15_total, total_matches),
                "home": _pct(team_o15_home, home_matches) if home_matches else 0,
                "away": _pct(team_o15_away, away_matches) if away_matches else 0,
            },
        )
        insights.setdefault(
            "over15_pct",
            {
                "total": _pct(o15_total, total_matches),
                "home": _pct(o15_home, home_matches) if home_matches else 0,
                "away": _pct(o15_away, away_matches) if away_matches else 0,
            },
        )
        insights.setdefault(
            "over25_pct",
            {
                "total": _pct(o25_total, total_matches),
                "home": _pct(o25_home, home_matches) if home_matches else 0,
                "away": _pct(o25_away, away_matches) if away_matches else 0,
            },
        )
        insights.setdefault(
            "win_and_over25_pct",
            {
                "total": _pct(win_o25_total, total_matches),
                "home": _pct(win_o25_home, home_matches) if home_matches else 0,
                "away": _pct(win_o25_away, away_matches) if away_matches else 0,
            },
        )
        insights.setdefault(
            "lose_and_btts_pct",
            {
                "total": _pct(lose_btts_total, total_matches),
                "home": _pct(lose_btts_home, home_matches) if home_matches else 0,
                "away": _pct(lose_btts_away, away_matches) if away_matches else 0,
            },
        )
        insights.setdefault(
            "goal_diff_avg",
            {
                "total": round(_avg(gd_sum_total, total_matches), 2),
                "home": round(_avg(gd_sum_home, home_matches), 2) if home_matches else 0.0,
                "away": round(_avg(gd_sum_away, away_matches), 2) if away_matches else 0.0,
            },
        )

    # ─────────────────────────────────────────
    # 최종 반환 – value 에 stats(dict)를 넣어서 반환
    # (DB에 다시 저장하진 않고, API 응답에서만 계산된 필드 사용)
    # ─────────────────────────────────────────
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
    """
    teams 테이블에서 단일 팀 정보 조회.
    """
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
