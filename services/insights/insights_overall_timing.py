# services/insights/insights_overall_timing.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all


def enrich_overall_timing(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    last_n: Optional[int] = None,
) -> None:
    """
    Timing 섹션을 채워 넣는 함수.

    - 실제 match_events 테이블(골 이벤트) 기반으로
      전/후반 득점/실점, 0–15분 / 80–90분 득점/실점 비율을 계산한다.
    - 로컬 DB 시절 InsightsOverallDao 에서 하던 계산을
      PostgreSQL + Python 코드로 옮긴 버전이다.

    last_n:
        > 0 이면 해당 시즌 내에서 최근 last_n 경기만 대상으로 Timing을 계산한다.
        None 또는 <= 0 이면 시즌 전체 경기(완료된 경기)를 사용한다.
    """

    if season_int is None:
        return

        # Competition / Last N에 따른 league_id 집합 결정
    filters = stats.get("insights_filters") if isinstance(stats, dict) else None
    target_ids = None
    if isinstance(filters, dict):
        target_ids = filters.get("target_league_ids_last_n")

    league_ids_for_query: List[int] = []
    if isinstance(target_ids, list):
        for v in target_ids:
            try:
                league_ids_for_query.append(int(v))
            except (TypeError, ValueError):
                continue

    # target이 비어있으면 현재 리그 한 개로 폴백
    if not league_ids_for_query:
        league_ids_for_query = [league_id]



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

    # 1) 이 팀이 뛴 완료된 경기 목록 (시즌 전체 or 최근 N경기)
    placeholders = ",".join(["%s"] * len(league_ids_for_query))

    base_sql = f"""
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            m.home_ft,
            m.away_ft,
            m.status_group
        FROM matches m
        WHERE m.league_id IN ({placeholders})
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND lower(m.status_group) IN ('finished','ft','fulltime')

        ORDER BY m.date_utc DESC
    """

    params: list[Any] = [*league_ids_for_query, season_int, team_id, team_id]
    if last_n and last_n > 0:
        # last_n 이 지정되면 시즌 내에서 최근 N경기만 사용
        base_sql += "\n        LIMIT %s"
        params.append(last_n)

    match_rows: List[Dict[str, Any]] = fetch_all(base_sql, tuple(params))

    if not match_rows:
        return

    fixture_ids = [mr["fixture_id"] for mr in match_rows]
    events_by_fixture: Dict[int, List[Dict[str, Any]]] = {}

    # 2) 해당 경기들의 골 이벤트 전부 가져오기
    if fixture_ids:
        placeholders = ",".join(["%s"] * len(fixture_ids))
        sql = f"""
            SELECT
                e.fixture_id,
                e.minute,
                e.team_id,
                e.detail
            FROM match_events e
            WHERE e.fixture_id IN ({placeholders})
              AND e.minute IS NOT NULL
              AND lower(e.type) IN ('goal','own goal','penalty','penalty goal')
            ORDER BY e.fixture_id, e.minute ASC
        """
        event_rows = fetch_all(sql, tuple(fixture_ids))
        for ev in event_rows:
            fid = ev["fixture_id"]
            events_by_fixture.setdefault(fid, []).append(ev)


    # 샘플 수 (Timing 계산에 사용된 경기 수)
    half_mt_tot = half_mt_home = half_mt_away = 0

    # Timing: 득점/실점 구간 플래그용 카운터
    score_1h_t = score_1h_h = score_1h_a = 0
    score_2h_t = score_2h_h = score_2h_a = 0
    concede_1h_t = concede_1h_h = concede_1h_a = 0
    concede_2h_t = concede_2h_h = concede_2h_a = 0

    score_015_t = score_015_h = score_015_a = 0
    concede_015_t = concede_015_h = concede_015_a = 0
    score_8090_t = score_8090_h = score_8090_a = 0
    concede_8090_t = concede_8090_h = concede_8090_a = 0

    # 3) 경기별로 이벤트를 훑으면서 플래그/카운터 계산
    for mr in match_rows:
        fid = mr["fixture_id"]
        home_id = mr["home_id"]
        away_id = mr["away_id"]
        home_ft = mr["home_ft"]
        away_ft = mr["away_ft"]

        # 스코어가 없는 경기는 제외
        if home_ft is None or away_ft is None:
            continue

        is_home = (team_id == home_id)

        evs = events_by_fixture.get(fid)
        if not evs:
            continue

        # 샘플 경기 수
        half_mt_tot += 1
        if is_home:
            half_mt_home += 1
        else:
            half_mt_away += 1

        scored_1h = conceded_1h = False
        scored_2h = conceded_2h = False
        scored_015 = conceded_015 = False
        scored_8090 = conceded_8090 = False

        for ev in evs:
            minute_raw = ev.get("minute")
            try:
                m_int = int(minute_raw)
            except (TypeError, ValueError):
                continue
            if m_int < 0:
                continue

            # ✅ 패널티 실축 / 세이브는 득점/실점 타이밍 계산에서 제외
            detail_str = (ev.get("detail") or "").lower()
            if ("pen" in detail_str or "penalty" in detail_str) and (
                "miss" in detail_str or "saved" in detail_str
            ):
                # 골이 아니므로 스킵
                continue

            is_for_goal = (ev["team_id"] == team_id)

            # 전/후반
            if m_int <= 45:
                if is_for_goal:
                    scored_1h = True
                else:
                    conceded_1h = True
            else:
                if is_for_goal:
                    scored_2h = True
                else:
                    conceded_2h = True

            # 0–15
            if m_int <= 15:
                if is_for_goal:
                    scored_015 = True
                else:
                    conceded_015 = True

            # 80+
            if m_int >= 80:
                if is_for_goal:
                    scored_8090 = True
                else:
                    conceded_8090 = True


        # ref 래핑 유틸
        def _inc(flag: bool, total_ref, home_ref, away_ref):
            if not flag:
                return
            if is_home:
                home_ref[0] += 1
            else:
                away_ref[0] += 1
            total_ref[0] += 1

        # 득점/실점 구간별 카운터 업데이트
        t_ref = [score_1h_t]
        h_ref = [score_1h_h]
        a_ref = [score_1h_a]
        _inc(scored_1h, t_ref, h_ref, a_ref)
        score_1h_t, score_1h_h, score_1h_a = t_ref[0], h_ref[0], a_ref[0]

        t_ref = [score_2h_t]
        h_ref = [score_2h_h]
        a_ref = [score_2h_a]
        _inc(scored_2h, t_ref, h_ref, a_ref)
        score_2h_t, score_2h_h, score_2h_a = t_ref[0], h_ref[0], a_ref[0]

        t_ref = [concede_1h_t]
        h_ref = [concede_1h_h]
        a_ref = [concede_1h_a]
        _inc(conceded_1h, t_ref, h_ref, a_ref)
        concede_1h_t, concede_1h_h, concede_1h_a = t_ref[0], h_ref[0], a_ref[0]

        t_ref = [concede_2h_t]
        h_ref = [concede_2h_h]
        a_ref = [concede_2h_a]
        _inc(conceded_2h, t_ref, h_ref, a_ref)
        concede_2h_t, concede_2h_h, concede_2h_a = t_ref[0], h_ref[0], a_ref[0]

        t_ref = [score_015_t]
        h_ref = [score_015_h]
        a_ref = [score_015_a]
        _inc(scored_015, t_ref, h_ref, a_ref)
        score_015_t, score_015_h, score_015_a = t_ref[0], h_ref[0], a_ref[0]

        t_ref = [concede_015_t]
        h_ref = [concede_015_h]
        a_ref = [concede_015_a]
        _inc(conceded_015, t_ref, h_ref, a_ref)
        concede_015_t, concede_015_h, concede_015_a = t_ref[0], h_ref[0], a_ref[0]

        t_ref = [score_8090_t]
        h_ref = [score_8090_h]
        a_ref = [score_8090_a]
        _inc(scored_8090, t_ref, h_ref, a_ref)
        score_8090_t, score_8090_h, score_8090_a = t_ref[0], h_ref[0], a_ref[0]

        t_ref = [concede_8090_t]
        h_ref = [concede_8090_h]
        a_ref = [concede_8090_a]
        _inc(conceded_8090, t_ref, h_ref, a_ref)
        concede_8090_t, concede_8090_h, concede_8090_a = t_ref[0], h_ref[0], a_ref[0]

    # 4) 퍼센트 계산해서 insights_overall 에 기록
    if half_mt_tot > 0:
        insights["score_1h_pct"] = {
            "total": fmt_pct(score_1h_t, half_mt_tot),
            "home": fmt_pct(score_1h_h, half_mt_home or half_mt_tot),
            "away": fmt_pct(score_1h_a, half_mt_away or half_mt_tot),
        }
        insights["score_2h_pct"] = {
            "total": fmt_pct(score_2h_t, half_mt_tot),
            "home": fmt_pct(score_2h_h, half_mt_home or half_mt_tot),
            "away": fmt_pct(score_2h_a, half_mt_away or half_mt_tot),
        }
        insights["concede_1h_pct"] = {
            "total": fmt_pct(concede_1h_t, half_mt_tot),
            "home": fmt_pct(concede_1h_h, half_mt_home or half_mt_tot),
            "away": fmt_pct(concede_1h_a, half_mt_away or half_mt_tot),
        }
        insights["concede_2h_pct"] = {
            "total": fmt_pct(concede_2h_t, half_mt_tot),
            "home": fmt_pct(concede_2h_h, half_mt_home or half_mt_tot),
            "away": fmt_pct(concede_2h_a, half_mt_away or half_mt_tot),
        }
        insights["score_0_15_pct"] = {
            "total": fmt_pct(score_015_t, half_mt_tot),
            "home": fmt_pct(score_015_h, half_mt_home or half_mt_tot),
            "away": fmt_pct(score_015_a, half_mt_away or half_mt_tot),
        }
        insights["concede_0_15_pct"] = {
            "total": fmt_pct(concede_015_t, half_mt_tot),
            "home": fmt_pct(concede_015_h, half_mt_home or half_mt_tot),
            "away": fmt_pct(concede_015_a, half_mt_away or half_mt_tot),
        }
        insights["score_80_90_pct"] = {
            "total": fmt_pct(score_8090_t, half_mt_tot),
            "home": fmt_pct(score_8090_h, half_mt_home or half_mt_tot),
            "away": fmt_pct(score_8090_a, half_mt_away or half_mt_tot),
        }
        insights["concede_80_90_pct"] = {
            "total": fmt_pct(concede_8090_t, half_mt_tot),
            "home": fmt_pct(concede_8090_h, half_mt_home or half_mt_tot),
            "away": fmt_pct(concede_8090_a, half_mt_away or half_mt_tot),
        }
