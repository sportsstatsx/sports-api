# hockey/services/hockey_insights_service.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one


FINISHED_STATUSES = ("FT", "AOT", "AP")


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _pct(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    # 앱에서 퍼센트로 그릴 가능성이 높아서 0~1 유지 (원하면 여기서 *100로 바꿔도 됨)
    return round(float(x), 4)


def _goal_rows_for_games(game_ids: List[int]) -> Dict[int, Dict[str, Dict[str, int]]]:
    """
    game_id -> period -> {home: goals, away: goals} 구조로 집계
    - period: P1, P2, P3, OT, SO (있을 수 있음)
    - goal 판정: type == 'goal' (소문자 비교)
    """
    if not game_ids:
        return {}

    placeholders = ", ".join(["%s"] * len(game_ids))

    # 홈/원정 팀을 game 테이블에서 가져와서 team_id를 비교해 home/away 득점으로 분류
    sql = f"""
        SELECT
            e.game_id,
            UPPER(TRIM(e.period)) AS period,
            e.team_id,
            LOWER(TRIM(e.type)) AS type_norm,
            g.home_team_id,
            g.away_team_id
        FROM hockey_game_events e
        JOIN hockey_games g ON g.id = e.game_id
        WHERE e.game_id IN ({placeholders})
          AND LOWER(TRIM(e.type)) = 'goal'
    """

    rows = hockey_fetch_all(sql, tuple(game_ids))

    out: Dict[int, Dict[str, Dict[str, int]]] = {}
    for r in rows:
        gid = _safe_int(r.get("game_id"))
        if gid is None:
            continue

        period = (r.get("period") or "").strip().upper()
        team_id = _safe_int(r.get("team_id"))
        home_id = _safe_int(r.get("home_team_id"))
        away_id = _safe_int(r.get("away_team_id"))

        if not period or team_id is None or home_id is None or away_id is None:
            continue

        is_home = team_id == home_id
        is_away = team_id == away_id
        if not (is_home or is_away):
            continue

        out.setdefault(gid, {}).setdefault(period, {"home": 0, "away": 0})
        if is_home:
            out[gid][period]["home"] += 1
        else:
            out[gid][period]["away"] += 1

    return out


def _first_goal_team_for_game(game_id: int) -> Optional[str]:
    """
    게임 전체 첫 골이 home/away 중 어디인지 반환.
    minute/order 기준으로 가장 빠른 goal 이벤트 1개를 가져온다.
    - minute이 NULL인 데이터도 있을 수 있어서 NULLS LAST 처리
    - 결과: 'home' | 'away' | None
    """
    sql = """
        SELECT
            e.team_id,
            g.home_team_id,
            g.away_team_id
        FROM hockey_game_events e
        JOIN hockey_games g ON g.id = e.game_id
        WHERE e.game_id = %s
          AND LOWER(TRIM(e.type)) = 'goal'
        ORDER BY
            e.period ASC,
            e.minute ASC NULLS LAST,
            e.event_order ASC
        LIMIT 1
    """
    r = hockey_fetch_one(sql, (game_id,))
    if not r:
        return None
    team_id = _safe_int(r.get("team_id"))
    home_id = _safe_int(r.get("home_team_id"))
    away_id = _safe_int(r.get("away_team_id"))
    if team_id is None or home_id is None or away_id is None:
        return None
    if team_id == home_id:
        return "home"
    if team_id == away_id:
        return "away"
    return None


def _rate(num: int, den: int) -> Optional[float]:
    if den <= 0:
        return None
    return num / den


def hockey_get_game_insights(game_id: int, sample_size: int = 200) -> Dict[str, Any]:
    """
    하키 인사이트(서버 계산)
    - 현재 game과 동일 league_id + season의 '종료경기'들 기반 경험적 확률 계산
    - 정규시간(1P~3P) 중심으로 계산
    - PowerPlay/Penalty는 이벤트 타입 매핑 확정 전까지 null 처리
    """

    # 1) 현재 게임 메타
    g = hockey_fetch_one(
        """
        SELECT
            id AS game_id,
            league_id,
            season,
            home_team_id,
            away_team_id,
            status,
            status_long
        FROM hockey_games
        WHERE id = %s
        LIMIT 1
        """,
        (game_id,),
    )
    if not g:
        raise ValueError("GAME_NOT_FOUND")

    league_id = _safe_int(g.get("league_id"))
    season = _safe_int(g.get("season"))
    home_id = _safe_int(g.get("home_team_id"))
    away_id = _safe_int(g.get("away_team_id"))
    if league_id is None or season is None:
        raise ValueError("BAD_GAME_DATA")

    # 2) 샘플 경기: 같은 league+season 종료경기(FT/AOT/AP) 최근 N개
    sample_games = hockey_fetch_all(
        """
        SELECT
            id AS game_id,
            home_team_id,
            away_team_id,
            status,
            score_json
        FROM hockey_games
        WHERE league_id = %s
          AND season = %s
          AND status = ANY(%s)
          AND id <> %s
        ORDER BY game_date DESC
        LIMIT %s
        """,
        (league_id, season, list(FINISHED_STATUSES), game_id, sample_size),
    )

    sample_ids = [_safe_int(r.get("game_id")) for r in sample_games]
    sample_ids = [x for x in sample_ids if x is not None]

    # 3) period goal 집계(샘플 전체)
    goals_map = _goal_rows_for_games(sample_ids)

    # 4) 카운팅 유틸: home/away "관점" 확률 산출
    # - 여기서 home/away는 '현재 경기의 home/away'가 아니라, "그 샘플 경기의 홈/원정" 기준이다.
    # - 하지만 인사이트 표는 보통 "현재 경기 홈팀/원정팀" 기준이 필요하니,
    #   팀 필터(현재 home_id/away_id)를 적용해서 각각의 팀 기준 확률을 만든다.
    #
    # => 방식:
    #   - 홈팀 확률: 샘플에서 (팀이 home로 나온 경기 + away로 나온 경기) 모두 포함해 '그 팀 기준'으로 집계
    #   - 원정팀도 동일

    def iter_team_samples(team_id: int) -> List[Tuple[int, str]]:
        # return: [(sample_game_id, 'home' or 'away' team_side_in_that_game)]
        out_pairs: List[Tuple[int, str]] = []
        for r in sample_games:
            gid = _safe_int(r.get("game_id"))
            if gid is None:
                continue
            h = _safe_int(r.get("home_team_id"))
            a = _safe_int(r.get("away_team_id"))
            if team_id == h:
                out_pairs.append((gid, "home"))
            elif team_id == a:
                out_pairs.append((gid, "away"))
        return out_pairs

    def reg_scores_for_game(gid: int) -> Tuple[int, int]:
        # 정규시간(P1~P3) 득점 합
        per = goals_map.get(gid, {})
        def getp(p: str) -> Dict[str, int]:
            return per.get(p, {"home": 0, "away": 0})
        h = getp("P1")["home"] + getp("P2")["home"] + getp("P3")["home"]
        a = getp("P1")["away"] + getp("P2")["away"] + getp("P3")["away"]
        return (h, a)

    def period_scores_for_game(gid: int, period: str) -> Tuple[int, int]:
        per = goals_map.get(gid, {})
        pa = per.get(period, {"home": 0, "away": 0})
        return (pa["home"], pa["away"])

    def team_reg_goals(gid: int, team_side: str) -> int:
        h, a = reg_scores_for_game(gid)
        return h if team_side == "home" else a

    def opp_reg_goals(gid: int, team_side: str) -> int:
        h, a = reg_scores_for_game(gid)
        return a if team_side == "home" else h

    def team_period_goals(gid: int, team_side: str, period: str) -> int:
        h, a = period_scores_for_game(gid, period)
        return h if team_side == "home" else a

    def opp_period_goals(gid: int, team_side: str, period: str) -> int:
        h, a = period_scores_for_game(gid, period)
        return a if team_side == "home" else h

    def team_reg_result(gid: int, team_side: str) -> str:
        tg = team_reg_goals(gid, team_side)
        og = opp_reg_goals(gid, team_side)
        if tg > og:
            return "W"
        if tg < og:
            return "L"
        return "D"

    def team_period_result(gid: int, team_side: str, period: str) -> str:
        tg = team_period_goals(gid, team_side, period)
        og = opp_period_goals(gid, team_side, period)
        if tg > og:
            return "W"
        if tg < og:
            return "L"
        return "D"

    def prob_team_metrics(team_id: int) -> Dict[str, Any]:
        pairs = iter_team_samples(team_id)
        den = len(pairs)

        # 기본: 정규시간 W/D/L
        w = d = l = 0

        # first goal
        fg = 0
        fg_den = 0

        # clean sheet (정규시간)
        cs = 0

        # team over thresholds (정규시간)
        t_over = {0.5: 0, 1.5: 0, 2.5: 0, 3.5: 0, 4.5: 0}

        # total goals over thresholds (정규시간)
        tot_over = {1.5: 0, 2.5: 0, 3.5: 0, 4.5: 0, 5.5: 0}

        # BTTS N+
        btts = {1: 0, 2: 0, 3: 0}

        # Win & Over / Win & BTTS
        win_over = {1.5: 0, 2.5: 0, 3.5: 0, 4.5: 0, 5.5: 0}
        win_btts = {1: 0, 2: 0, 3: 0}

        for gid, side in pairs:
            res = team_reg_result(gid, side)
            if res == "W":
                w += 1
            elif res == "D":
                d += 1
            else:
                l += 1

            # first goal team for that game (home/away)
            first = _first_goal_team_for_game(gid)
            if first is not None:
                fg_den += 1
                if first == side:
                    fg += 1

            tg = team_reg_goals(gid, side)
            og = opp_reg_goals(gid, side)
            total = tg + og

            # clean sheet: opponent reg goals == 0
            if og == 0:
                cs += 1

            for th in t_over:
                if tg > th:
                    t_over[th] += 1
            for th in tot_over:
                if total > th:
                    tot_over[th] += 1
            for n in btts:
                if tg >= n and og >= n:
                    btts[n] += 1

            if res == "W":
                for th in win_over:
                    if total > th:
                        win_over[th] += 1
                for n in win_btts:
                    if tg >= n and og >= n:
                        win_btts[n] += 1

        return {
            "sample": den,
            "win": _pct(_rate(w, den)),
            "draw": _pct(_rate(d, den)),
            "loss": _pct(_rate(l, den)),
            "first_goal": _pct(_rate(fg, fg_den)) if fg_den > 0 else None,
            "clean_sheet": _pct(_rate(cs, den)),
            "team_over": {str(k): _pct(_rate(v, den)) for k, v in t_over.items()},
            "total_over": {str(k): _pct(_rate(v, den)) for k, v in tot_over.items()},
            "btts": {str(k): _pct(_rate(v, den)) for k, v in btts.items()},
            "win_over": {str(k): _pct(_rate(v, den)) for k, v in win_over.items()},
            "win_btts": {str(k): _pct(_rate(v, den)) for k, v in win_btts.items()},
        }

    # 팀별(현재 경기 홈/원정팀) 메트릭
    home_metrics = prob_team_metrics(home_id) if home_id else {"sample": 0}
    away_metrics = prob_team_metrics(away_id) if away_id else {"sample": 0}

    # match-level(정규시간) 메트릭: league+season 샘플 전체에서 계산
    # - Total Goals Over / BTTS 등은 “경기 전체 확률”이므로 여기서 별도로 계산
    match_den = len(sample_ids)
    match_tot_over = {1.5: 0, 2.5: 0, 3.5: 0, 4.5: 0, 5.5: 0}
    match_btts = {1: 0, 2: 0, 3: 0}

    for gid in sample_ids:
        h, a = reg_scores_for_game(gid)
        total = h + a
        for th in match_tot_over:
            if total > th:
                match_tot_over[th] += 1
        for n in match_btts:
            if h >= n and a >= n:
                match_btts[n] += 1

    match_metrics = {
        "sample": match_den,
        "total_over": {str(k): _pct(_rate(v, match_den)) for k, v in match_tot_over.items()},
        "btts": {str(k): _pct(_rate(v, match_den)) for k, v in match_btts.items()},
    }

    # 5) 응답 섹션 구성 (앱이 그대로 그리기 쉬운 구조)
    def row_three(key: str, label: str, total: Optional[float], home: Optional[float], away: Optional[float]) -> Dict[str, Any]:
        return {
            "key": key,
            "label": label,
            "values": {"total": total, "home": home, "away": away},
        }

    sections: List[Dict[str, Any]] = []

    # Full Time (Regular Time)
    sections.append(
        {
            "id": "rt_full_time",
            "title": "Full Time (Regular Time)",
            "layout": "three_cols",
            "columns": ["Total", "Home", "Away"],
            "rows": [
                row_three("win", "Win", None, home_metrics.get("win"), away_metrics.get("win")),
                row_three("draw", "Draw", None, home_metrics.get("draw"), away_metrics.get("draw")),
                row_three("loss", "Loss", None, home_metrics.get("loss"), away_metrics.get("loss")),

                row_three("first_goal_scored", "First Goal Scored", None, home_metrics.get("first_goal"), away_metrics.get("first_goal")),

                # 아래 3개는 이벤트 타입 매핑 확정 전까지 null
                row_three("power_play_occurred", "Power Play Occurred", None, None, None),
                row_three("power_play_goal", "Power Play Goal", None, None, None),
                row_three("penalty_occurred", "Penalty Occurred", None, None, None),

                row_three("clean_sheet", "Clean Sheet", None, home_metrics.get("clean_sheet"), away_metrics.get("clean_sheet")),

                row_three("team_over_0_5", "Team Over 0.5 Goals", None, home_metrics.get("team_over", {}).get("0.5"), away_metrics.get("team_over", {}).get("0.5")),
                row_three("team_over_1_5", "Team Over 1.5 Goals", None, home_metrics.get("team_over", {}).get("1.5"), away_metrics.get("team_over", {}).get("1.5")),
                row_three("team_over_2_5", "Team Over 2.5 Goals", None, home_metrics.get("team_over", {}).get("2.5"), away_metrics.get("team_over", {}).get("2.5")),
                row_three("team_over_3_5", "Team Over 3.5 Goals", None, home_metrics.get("team_over", {}).get("3.5"), away_metrics.get("team_over", {}).get("3.5")),
                row_three("team_over_4_5", "Team Over 4.5 Goals", None, home_metrics.get("team_over", {}).get("4.5"), away_metrics.get("team_over", {}).get("4.5")),

                row_three("total_over_1_5", "Total Goals Over 1.5", match_metrics.get("total_over", {}).get("1.5"), None, None),
                row_three("total_over_2_5", "Total Goals Over 2.5", match_metrics.get("total_over", {}).get("2.5"), None, None),
                row_three("total_over_3_5", "Total Goals Over 3.5", match_metrics.get("total_over", {}).get("3.5"), None, None),
                row_three("total_over_4_5", "Total Goals Over 4.5", match_metrics.get("total_over", {}).get("4.5"), None, None),
                row_three("total_over_5_5", "Total Goals Over 5.5", match_metrics.get("total_over", {}).get("5.5"), None, None),

                row_three("btts_1", "Both Teams to Score 1+", match_metrics.get("btts", {}).get("1"), None, None),
                row_three("btts_2", "Both Teams to Score 2+", match_metrics.get("btts", {}).get("2"), None, None),
                row_three("btts_3", "Both Teams to Score 3+", match_metrics.get("btts", {}).get("3"), None, None),

                row_three("win_over_1_5", "Win & Over 1.5 Goals", None, home_metrics.get("win_over", {}).get("1.5"), away_metrics.get("win_over", {}).get("1.5")),
                row_three("win_over_2_5", "Win & Over 2.5 Goals", None, home_metrics.get("win_over", {}).get("2.5"), away_metrics.get("win_over", {}).get("2.5")),
                row_three("win_over_3_5", "Win & Over 3.5 Goals", None, home_metrics.get("win_over", {}).get("3.5"), away_metrics.get("win_over", {}).get("3.5")),
                row_three("win_over_4_5", "Win & Over 4.5 Goals", None, home_metrics.get("win_over", {}).get("4.5"), away_metrics.get("win_over", {}).get("4.5")),
                row_three("win_over_5_5", "Win & Over 5.5 Goals", None, home_metrics.get("win_over", {}).get("5.5"), away_metrics.get("win_over", {}).get("5.5")),

                row_three("win_btts_1", "Win & Both Teams to Score 1+", None, home_metrics.get("win_btts", {}).get("1"), away_metrics.get("win_btts", {}).get("1")),
                row_three("win_btts_2", "Win & Both Teams to Score 2+", None, home_metrics.get("win_btts", {}).get("2"), away_metrics.get("win_btts", {}).get("2")),
                row_three("win_btts_3", "Win & Both Teams to Score 3+", None, home_metrics.get("win_btts", {}).get("3"), away_metrics.get("win_btts", {}).get("3")),
            ],
        }
    )

    return {
        "ok": True,
        "game_id": game_id,
        "league_id": league_id,
        "season": season,
        "sections": sections,
        "meta": {
            "source": "db_empirical",
            "sample_size_league_season": match_den,
            "sample_size_home_team": home_metrics.get("sample"),
            "sample_size_away_team": away_metrics.get("sample"),
            "pending": [
                "power_play_occurred",
                "power_play_goal",
                "penalty_occurred",
                "period_transitions",
                "clutch_last_3_min",
                "start_score_impact",
                "goal_timing_distribution",
            ],
            "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        },
    }
