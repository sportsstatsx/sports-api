# services/insights/insights_overall_firstgoal_momentum.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all


def enrich_overall_firstgoal_momentum(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    last_n: Optional[int] = None,
) -> None:
    """
    First Goal / Momentum 섹션을 채워 넣는 함수.

    - 선제골/선실점 비율
    - 리드(선제골) 상황에서의 승/무/패 비율
    - 트레일링(상대 선제) 상황에서의 승/무/패 비율

    을 실제 match_events + 최종 스코어 기반으로 계산한다.
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
            m.status_group,
            m.date_utc
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

    # 2) 골 이벤트 로드
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


    # First goal 샘플 수/카운터
    fg_sample_t = fg_sample_h = fg_sample_a = 0
    fg_for_t = fg_for_h = fg_for_a = 0
    fg_against_t = fg_against_h = fg_against_a = 0

    # Momentum: 리드/트레일링 상황별 샘플/결과
    leading_sample_t = leading_sample_h = leading_sample_a = 0
    leading_win_t = leading_win_h = leading_win_a = 0
    leading_draw_t = leading_draw_h = leading_draw_a = 0
    leading_loss_t = leading_loss_h = leading_loss_a = 0

    trailing_sample_t = trailing_sample_h = trailing_sample_a = 0
    trailing_win_t = trailing_win_h = trailing_win_a = 0
    trailing_draw_t = trailing_draw_h = trailing_draw_a = 0
    trailing_loss_t = trailing_loss_h = trailing_loss_a = 0

    # 3) 경기별로 첫 골 주체와 최종 결과를 기반으로 카운터 계산
    for mr in match_rows:
        fid = mr["fixture_id"]
        home_id = mr["home_id"]
        away_id = mr["away_id"]
        home_ft = mr["home_ft"]
        away_ft = mr["away_ft"]

        if home_ft is None or away_ft is None:
            continue

        is_home = (team_id == home_id)
        gf = home_ft if is_home else away_ft
        ga = away_ft if is_home else home_ft

        evs = events_by_fixture.get(fid)
        if not evs:
            continue

        first_minute: Optional[int] = None
        first_for: Optional[bool] = None

        for ev in evs:
            minute_raw = ev.get("minute")
            try:
                m_int = int(minute_raw)
            except (TypeError, ValueError):
                continue
            if m_int < 0:
                continue

            # ✅ 패널티 실축 / 세이브는 첫 골에서 제외
            detail_str = (ev.get("detail") or "").lower()
            if ("pen" in detail_str or "penalty" in detail_str) and (
                "miss" in detail_str or "saved" in detail_str
            ):
                # 골이 아니므로 스킵
                continue

            is_for_goal = (ev["team_id"] == team_id)

            # 첫 골인지 판단 (가장 이른 분)
            if first_minute is None or m_int < first_minute:
                first_minute = m_int
                first_for = is_for_goal


        if first_minute is None or first_for is None:
            continue

        # 샘플 수
        fg_sample_t += 1
        if is_home:
            fg_sample_h += 1
        else:
            fg_sample_a += 1

        if first_for:
            # 선제골
            fg_for_t += 1
            if is_home:
                fg_for_h += 1
            else:
                fg_for_a += 1

            # 리드 상태에서의 결과
            leading_sample_t += 1
            if is_home:
                leading_sample_h += 1
            else:
                leading_sample_a += 1

            if gf > ga:
                leading_win_t += 1
                if is_home:
                    leading_win_h += 1
                else:
                    leading_win_a += 1
            elif gf == ga:
                leading_draw_t += 1
                if is_home:
                    leading_draw_h += 1
                else:
                    leading_draw_a += 1
            else:
                leading_loss_t += 1
                if is_home:
                    leading_loss_h += 1
                else:
                    leading_loss_a += 1

        else:
            # 상대 선제 (선실점)
            fg_against_t += 1
            if is_home:
                fg_against_h += 1
            else:
                fg_against_a += 1

            # 트레일링 상태에서의 결과
            trailing_sample_t += 1
            if is_home:
                trailing_sample_h += 1
            else:
                trailing_sample_a += 1

            if gf > ga:
                trailing_win_t += 1
                if is_home:
                    trailing_win_h += 1
                else:
                    trailing_win_a += 1
            elif gf == ga:
                trailing_draw_t += 1
                if is_home:
                    trailing_draw_h += 1
                else:
                    trailing_draw_a += 1
            else:
                trailing_loss_t += 1
                if is_home:
                    trailing_loss_h += 1
                else:
                    trailing_loss_a += 1

    # 4) 퍼센트 계산해서 insights_overall 에 기록
    if fg_sample_t > 0:
        insights["first_to_score_pct"] = {
            "total": fmt_pct(fg_for_t, fg_sample_t),
            "home": fmt_pct(fg_for_h, fg_sample_h or fg_sample_t),
            "away": fmt_pct(fg_for_a, fg_sample_a or fg_sample_t),
        }
        insights["first_conceded_pct"] = {
            "total": fmt_pct(fg_against_t, fg_sample_t),
            "home": fmt_pct(fg_against_h, fg_sample_h or fg_sample_t),
            "away": fmt_pct(fg_against_a, fg_sample_a or fg_sample_t),
        }

    if leading_sample_t > 0:
        insights["when_leading_win_pct"] = {
            "total": fmt_pct(leading_win_t, leading_sample_t),
            "home": fmt_pct(leading_win_h, leading_sample_h or leading_sample_t),
            "away": fmt_pct(leading_win_a, leading_sample_a or leading_sample_t),
        }
        insights["when_leading_draw_pct"] = {
            "total": fmt_pct(leading_draw_t, leading_sample_t),
            "home": fmt_pct(leading_draw_h, leading_sample_h or leading_sample_t),
            "away": fmt_pct(leading_draw_a, leading_sample_a or leading_sample_t),
        }
        insights["when_leading_loss_pct"] = {
            "total": fmt_pct(leading_loss_t, leading_sample_t),
            "home": fmt_pct(leading_loss_h, leading_sample_h or leading_sample_t),
            "away": fmt_pct(leading_loss_a, leading_sample_a or leading_sample_t),
        }

    if trailing_sample_t > 0:
        insights["when_trailing_win_pct"] = {
            "total": fmt_pct(trailing_win_t, trailing_sample_t),
            "home": fmt_pct(trailing_win_h, trailing_sample_h or trailing_sample_t),
            "away": fmt_pct(trailing_win_a, trailing_sample_a or trailing_sample_t),
        }
        insights["when_trailing_draw_pct"] = {
            "total": fmt_pct(trailing_draw_t, trailing_sample_t),
            "home": fmt_pct(trailing_draw_h, trailing_sample_h or trailing_sample_t),
            "away": fmt_pct(trailing_draw_a, trailing_sample_a or trailing_sample_t),
        }
        insights["when_trailing_loss_pct"] = {
            "total": fmt_pct(trailing_loss_t, trailing_sample_t),
            "home": fmt_pct(trailing_loss_h, trailing_sample_h or trailing_sample_t),
            "away": fmt_pct(trailing_loss_a, trailing_sample_a or trailing_sample_t),
        }

    # First Goal 관련 샘플 수
    insights["first_goal_sample"] = fg_sample_t
