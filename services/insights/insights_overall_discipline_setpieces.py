# services/insights/insights_overall_discipline_setpieces.py
from __future__ import annotations

from typing import Any, Dict, Optional

from db import fetch_all
from .utils import fmt_avg


def _pct_int(total: int, hit: int) -> int:
    """
    분모 total, 히트 hit  →  정수 퍼센트 (0~100)
    total == 0 이면 0으로.
    """
    if total <= 0:
        return 0
    return round(hit * 100.0 / total)


def enrich_overall_discipline_setpieces(
    stats: Dict[str, Any],
    insights: Dict[str, Any],
    *,
    league_id: int,
    season_int: Optional[int],
    team_id: int,
) -> None:
    """
    Discipline & Set Pieces 섹션 계산.

    - Corners / Yellow / Red per match
    - Opp Red → 우리가 득점했는지 비율
    - Own Red → 우리가 실점했는지 비율
    """

    if season_int is None:
        return

    # 1) 코너킥 / 카드 관련 기본 통계 (stats 의 원본 JSON 에서 계산)
    #    이미 SeasonStats 에서 누적값은 들어있고, 여기서는 경기당 평균만 계산.
    #    (insights_overall_outcome_totals 등과 동일한 방식으로 fmt_avg 사용)

    # ─────────────────────────────────────────
    # Corners per match
    # ─────────────────────────────────────────
    # stats["corners"] 구조:
    # {
    #   "total": {"home": x, "away": y},
    #   "average": {...}  // 기존 값 무시하고 다시 계산
    # }

    corners_root = stats.get("corners") or {}
    corners_total_obj = corners_root.get("total") or {}

    corners_total_home = corners_total_obj.get("home") or 0
    corners_total_away = corners_total_obj.get("away") or 0
    corners_total_all = (corners_total_home or 0) + (corners_total_away or 0)

    # 경기 수는 stats["fixtures"]["played"] 기준
    fixtures = stats.get("fixtures") or {}
    played = fixtures.get("played") or {}
    played_total = played.get("total") or 0
    played_home = played.get("home") or 0
    played_away = played.get("away") or 0

    corners_per_match_total = fmt_avg(corners_total_all, played_total)
    corners_per_match_home = fmt_avg(corners_total_home, played_home)
    corners_per_match_away = fmt_avg(corners_total_away, played_away)

    insights["corners_per_match"] = {
        "total": corners_per_match_total,
        "home": corners_per_match_home,
        "away": corners_per_match_away,
    }

    # ─────────────────────────────────────────
    # Yellow / Red 카드 per match
    # ─────────────────────────────────────────

    cards_root = stats.get("cards") or {}

    def _sum_card_totals(card_root: Optional[Dict[str, Any]]) -> int:
        if not card_root:
            return 0
        total = 0
        for _minute_key, obj in card_root.items():
            if not isinstance(obj, dict):
                continue
            total += int(obj.get("total") or 0)
        return total

    yellow_root = cards_root.get("yellow") or {}
    red_root = cards_root.get("red") or {}

    yellow_total = _sum_card_totals(yellow_root)
    red_total = _sum_card_totals(red_root)

    yellow_per_match = fmt_avg(yellow_total, played_total)
    red_per_match = fmt_avg(red_total, played_total)

    # 홈/원정 쪽은 stats 가 minute 기준 누적이라 팀별 분리가 안 되어,
    # 일단 total 기준으로만 두고 home/away 는 total 과 동일하게 둔다.
    insights["yellow_per_match"] = {
        "total": yellow_per_match,
        "home": yellow_per_match,
        "away": yellow_per_match,
    }
    insights["red_per_match"] = {
        "total": red_per_match,
        "home": red_per_match,
        "away": red_per_match,
    }

    # ─────────────────────────────────────────
    # 2) Opp Red / Own Red 계산
    #    (로컬 DAO 의 opp_red / own_red CTE 로직을 Python 으로 구현)
    # ─────────────────────────────────────────

    # 카드 이벤트 (레드 카드만 필터)
    card_rows = fetch_all(
        """
        SELECT
            e.fixture_id,
            e.minute,
            e.team_id,
            m.home_id,
            m.away_id
        FROM match_events e
        JOIN matches m ON m.fixture_id = e.fixture_id
        WHERE m.league_id = %s
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND (
                lower(m.status_group) IN ('finished','ft','fulltime')
             OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
          )
          AND lower(e.type) IN ('card','red card')
          AND (
                lower(e.detail) LIKE '%%red%%'
             OR lower(e.type) = 'red card'
          )
          AND e.minute IS NOT NULL
        """,
        (league_id, season_int, team_id, team_id),
    )

    # 골 이벤트
    goal_rows = fetch_all(
        """
        SELECT
            e.fixture_id,
            e.minute,
            e.team_id,
            m.home_id,
            m.away_id
        FROM match_events e
        JOIN matches m ON m.fixture_id = e.fixture_id
        WHERE m.league_id = %s
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
          AND e.minute IS NOT NULL
          AND lower(e.type) = 'goal'
        """,
        (league_id, season_int, team_id, team_id),
    )

    # 경기 venue (홈/원정) 맵
    fixture_venue: Dict[int, str] = {}
    match_rows = fetch_all(
        """
        SELECT
            m.fixture_id,
            m.home_id,
            m.away_id
        FROM matches m
        WHERE m.league_id = %s
          AND m.season    = %s
          AND (%s = m.home_id OR %s = m.away_id)
        """,
        (league_id, season_int, team_id, team_id),
    )
    for row in match_rows:
        fid = row["fixture_id"]
        home_id = row["home_id"]
        away_id = row["away_id"]
        if home_id == team_id:
            fixture_venue[fid] = "H"
        elif away_id == team_id:
            fixture_venue[fid] = "A"

    # 경기별 첫 Opp red / Own red 분(minute)
    opp_red_min: Dict[int, int] = {}
    own_red_min: Dict[int, int] = {}

    for row in card_rows:
        fid = row["fixture_id"]
        minute = row["minute"]
        card_team_id = row["team_id"]

        # 이 경기에서 우리 팀이 실제로 뛴 경우만 (안전 방어)
        if fid not in fixture_venue:
            continue

        if card_team_id == team_id:
            # Own red
            prev = own_red_min.get(fid)
            if prev is None or minute < prev:
                own_red_min[fid] = minute
        else:
            # Opp red
            prev = opp_red_min.get(fid)
            if prev is None or minute < prev:
                opp_red_min[fid] = minute

    # 골 이후 플래그
    opp_scored_after: Dict[int, bool] = {}
    own_conceded_after: Dict[int, bool] = {}

    for row in goal_rows:
        fid = row["fixture_id"]
        minute = row["minute"]
        scorer_id = row["team_id"]

        # 이 경기에서 우리 팀이 실제로 뛴 경우만
        if fid not in fixture_venue:
            continue

        # 상대 레드 이후 우리가 득점?
        if fid in opp_red_min and minute >= opp_red_min[fid] and scorer_id == team_id:
            opp_scored_after[fid] = True

        # 우리 레드 이후 우리가 실점?
        if fid in own_red_min and minute >= own_red_min[fid] and scorer_id != team_id:
            own_conceded_after[fid] = True

    # 샘플 수 및 히트 수 (T/H/A) 집계
    opp_sample_t = opp_sample_h = opp_sample_a = 0
    opp_scored_t = opp_scored_h = opp_scored_a = 0

    for fid, minute in opp_red_min.items():
        venue = fixture_venue.get(fid)
        if venue is None:
            continue

        opp_sample_t += 1
        if venue == "H":
            opp_sample_h += 1
        else:
            opp_sample_a += 1

        if opp_scored_after.get(fid):
            opp_scored_t += 1
            if venue == "H":
                opp_scored_h += 1
            else:
                opp_scored_a += 1

    own_sample_t = own_sample_h = own_sample_a = 0
    own_conceded_t = own_conceded_h = own_conceded_a = 0

    for fid, minute in own_red_min.items():
        venue = fixture_venue.get(fid)
        if venue is None:
            continue

        own_sample_t += 1
        if venue == "H":
            own_sample_h += 1
        else:
            own_sample_a += 1

        if own_conceded_after.get(fid):
            own_conceded_t += 1
            if venue == "H":
                own_conceded_h += 1
            else:
                own_conceded_a += 1

    # 퍼센트 계산 (정수)
    opp_pct_total = _pct_int(opp_sample_t, opp_scored_t)
    opp_pct_home = _pct_int(opp_sample_h, opp_scored_h)
    opp_pct_away = _pct_int(opp_sample_a, opp_scored_a)

    own_pct_total = _pct_int(own_sample_t, own_conceded_t)
    own_pct_home = _pct_int(own_sample_h, own_conceded_h)
    own_pct_away = _pct_int(own_sample_a, own_conceded_a)

    # JSON 기록
    # (샘플은 전체 기준 하나, 퍼센트는 T/H/A 3개)
    insights["opp_red_sample"] = opp_sample_t
    insights["opp_red_scored_pct"] = {
        "total": opp_pct_total,
        "home": opp_pct_home,
        "away": opp_pct_away,
    }

    insights["own_red_sample"] = own_sample_t
    insights["own_red_conceded_pct"] = {
        "total": own_pct_total,
        "home": own_pct_home,
        "away": own_pct_away,
    }
