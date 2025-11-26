# services/insights/insights_overall_shooting_efficiency.py
from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all
from .utils import fmt_pct, fmt_avg


def enrich_overall_shooting_efficiency(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
    matches_total_api: int = 0,
    last_n: Optional[int] = None,  # 🔹 Last N 필터용 (없으면 시즌 전체)
) -> None:
    """
    Insights Overall - Shooting & Efficiency 섹션.

    - shots_per_match : 경기당 슈팅 수 (total/home/away)
    - shots_on_target_pct : 유효슈팅 비율 (total/home/away)

    ⚠️ 로컬 InsightsOverallDao.kt 와 동일한 개념으로 맞춘다:
        - 항상 "우리 팀"의 슈팅만 합산
        - 분모는 실제 우리 팀이 뛴 경기 수(전체/홈/원정)를 사용
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



    # ─────────────────────────────────────────
    # 1) 경기별 우리 팀 슈팅 / 유효슈팅 집계
    #    - match_team_stats 에서 team_id = 우리 팀만 가져옴
    #    - finished / fulltime 경기만
    #    - last_n 이 있으면 "최근 N경기"만 사용
    # ─────────────────────────────────────────

    placeholders = ",".join(["%s"] * len(league_ids_for_query))

    base_sql = f"""
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id,
            m.date_utc,
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
         AND mts.team_id  = %s          -- ✅ 우리 팀만
        WHERE m.league_id IN ({placeholders})
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
        GROUP BY m.fixture_id, m.home_id, m.away_id, m.date_utc
        ORDER BY m.date_utc DESC
    """

    params = [team_id, *league_ids_for_query, season_int, team_id, team_id]

    # 🔹 last_n 이 지정된 경우 → 최근 N경기만 사용
    if last_n is not None and last_n > 0:
        base_sql += "\n        LIMIT %s"
        params.append(last_n)

    shot_rows = fetch_all(base_sql, tuple(params))

    if not shot_rows:
        return

    # ─────────────────────────────────────────
    # 2) 전체 / 홈 / 원정 경기 수 및 슈팅 합계
    # ─────────────────────────────────────────
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
            # 이론상 올 수 없지만 안전장치
            continue

        total_shots = r["total_shots"] or 0
        sog = r["shots_on_target"] or 0

        # ✅ 로컬 DAO는 "슈팅이 0인 경기"도 분모에 포함되므로
        #    여기서는 continue 하지 않고 그대로 경기 수에 포함시킨다.
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

    if total_matches == 0:
        return

    # ─────────────────────────────────────────
    # 3) 분모 설정
    #    - 로컬 DAO처럼 "실제 경기 수" 기준으로 계산
    #    - 나중에 comp / lastN 필터가 들어가면,
    #      여기 total_matches / home_matches / away_matches 가
    #      필터된 경기 수가 될 것.
    # ─────────────────────────────────────────
    eff_total = total_matches
    eff_home = home_matches or eff_total
    eff_away = away_matches or eff_total

    # stats["shots"] 블록: 서버/클라이언트에서 재사용 가능
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

    # ─────────────────────────────────────────
    # 4) 경기당 슈팅 수 (평균)  – Double 로 내려주고,
    #    클라이언트(InsightsOverallRepository)에서 포맷("0.0") 처리.
    # ─────────────────────────────────────────
    avg_total = fmt_avg(total_shots_total, eff_total)
    avg_home = fmt_avg(total_shots_home, eff_home)
    avg_away = fmt_avg(total_shots_away, eff_away)

    insights["shots_per_match"] = {
        "total": avg_total,
        "home": avg_home,
        "away": avg_away,
    }

    # ─────────────────────────────────────────
    # 5) 유효슈팅 비율 (%)
    # ─────────────────────────────────────────
    insights["shots_on_target_pct"] = {
        "total": fmt_pct(sog_total, total_shots_total),
        "home": fmt_pct(sog_home, total_shots_home),
        "away": fmt_pct(sog_away, total_shots_away),
    }
