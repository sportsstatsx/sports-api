from __future__ import annotations

import json
from datetime import datetime, date as date_cls
from typing import Any, Dict, List, Optional

from db import fetch_all

from .insights.insights_overall_shooting_efficiency import (
    enrich_overall_shooting_efficiency,
)
from .insights.insights_overall_outcome_totals import (
    enrich_overall_outcome_totals,
)
from .insights.insights_overall_goalsbytime import (
    enrich_overall_goals_by_time,
)
from .insights.insights_overall_timing import enrich_overall_timing
from .insights.insights_overall_firstgoal_momentum import (
    enrich_overall_firstgoal_momentum,
)
from .insights.insights_overall_discipline_setpieces import (
    enrich_overall_discipline_setpieces,
)
from .insights.utils import normalize_comp, parse_last_n


# ─────────────────────────────────────
#  공통: 날짜 파싱/정규화
# ─────────────────────────────────────


def _normalize_date(date_str: Optional[str]) -> str:
    """
    다양한 형태(YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS 등)의 문자열을
    안전하게 'YYYY-MM-DD' 로 정규화한다.
    """
    if not date_str:
        # 오늘 날짜
        return datetime.utcnow().date().isoformat()

    if isinstance(date_str, date_cls):
        return date_str.isoformat()

    try:
        dt = datetime.fromisoformat(str(date_str).replace("Z", "+00:00"))
        return dt.date().isoformat()
    except Exception:
        return datetime.utcnow().date().isoformat()


def _to_iso_or_str(val: Any) -> Optional[str]:
    """
    DB에서 가져온 date_utc가 datetime 일 수도, 문자열일 수도 있어서
    안전하게 문자열로 변환해주는 유틸.
    """
    if val is None:
        return None
    if isinstance(val, (datetime, date_cls)):
        return val.isoformat()
    # 이미 문자열이거나 다른 타입이면 str()로 통일
    return str(val)


# ─────────────────────────────────────
#  공통: Insights Overall 필터 메타
# ─────────────────────────────────────


def build_insights_filter_meta(
    comp_raw: Optional[str],
    last_n_raw: Optional[str],
) -> Dict[str, Any]:
    """
    클라이언트에서 넘어오는 competition / lastN 값을
    서버 내부 표준 형태로 정규화해서 메타데이터 딕셔너리로 돌려준다.

    현재 단계에서는:
      - 실제 계산에는 last_n (정수)만 쓰고,
      - comp 값은 응답 메타(insights_filters)로만 내려보낸다.
    """
    comp_norm = normalize_comp(comp_raw)
    last_n = parse_last_n(last_n_raw)

    return {
        "competition": comp_norm,
        "last_n": last_n,
    }


# ─────────────────────────────────────
#  1) 홈 화면: 리그 목록
# ─────────────────────────────────────


def get_home_leagues(
    date_str: Optional[str],
    league_ids: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """
    주어진 날짜(date_str)에 실제 경기가 편성된 리그 목록을 돌려준다.
    league_ids 가 주어지면 해당 리그들만 필터링.
    """
    norm_date = _normalize_date(date_str)

    params: List[Any] = [norm_date]
    where_clause = "m.date_utc::date = %s"

    if league_ids:
        placeholders = ", ".join(["%s"] * len(league_ids))
        where_clause += f" AND m.league_id IN ({placeholders})"
        params.extend(league_ids)

    rows = fetch_all(
        f"""
        SELECT
            m.league_id,
            l.name    AS league_name,
            l.country AS country,
            l.logo    AS league_logo,
            m.season
        FROM matches m
        JOIN leagues l
          ON m.league_id = l.id
        WHERE {where_clause}
        GROUP BY
            m.league_id,
            l.name,
            l.country,
            l.logo,
            m.season
        ORDER BY
            l.country,
            l.name
        """,
        tuple(params),
    )

    result: List[Dict[str, Any]] = []
    for r in rows:
        result.append(
            {
                "league_id": r["league_id"],
                "name": r["league_name"],
                "country": r["country"],
                "logo": r["league_logo"],
                "season": r["season"],
            }
        )
    return result


# ─────────────────────────────────────
#  2) 홈 화면: 특정 리그의 매치 디렉터리
# ─────────────────────────────────────


def get_home_league_directory(league_id: int, date_str: Optional[str]) -> Dict[str, Any]:
    """
    특정 리그의 주어진 날짜(date_str)에 대한 매치 디렉터리 정보.
    Postgres matches 스키마(라운드/short 컬럼 없음)에 맞춰서,
    각 경기의 홈/원정 레드카드 개수까지 함께 내려준다.
    """
    norm_date = _normalize_date(date_str)

    rows = fetch_all(
        """
        SELECT
            m.fixture_id,
            m.league_id,
            m.season,
            NULL::text AS round,
            m.date_utc,
            m.status AS status_short,
            m.status_group,
            m.home_id,
            th.name   AS home_name,
            th.logo   AS home_logo,
            m.away_id,
            ta.name   AS away_name,
            ta.logo   AS away_logo,
            m.home_ft,
            m.away_ft,
            (
                SELECT COUNT(*)
                FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id    = m.home_id
                  AND lower(e.type)   = 'card'
                  AND lower(e.detail) = 'red card'
            ) AS home_red_cards,
            (
                SELECT COUNT(*)
                FROM match_events e
                WHERE e.fixture_id = m.fixture_id
                  AND e.team_id    = m.away_id
                  AND lower(e.type)   = 'card'
                  AND lower(e.detail) = 'red card'
            ) AS away_red_cards
        FROM matches m
        JOIN teams th ON th.id = m.home_id
        JOIN teams ta ON ta.id = m.away_id
        WHERE m.league_id = %s
          AND m.date_utc::date = %s
        ORDER BY m.date_utc ASC, m.fixture_id ASC
        """,
        (league_id, norm_date),
    )

    fixtures: List[Dict[str, Any]] = []
    season: Optional[int] = None
    round_name: Optional[str] = None

    for r in rows:
        season = season or r["season"]
        round_name = round_name or r["round"]

        fixtures.append(
            {
                "fixture_id": r["fixture_id"],
                "league_id": r["league_id"],
                "season": r["season"],
                "round": r["round"],
                "date_utc": _to_iso_or_str(r["date_utc"]),
                "status_short": r["status_short"],
                "status_group": r["status_group"],
                "home": {
                    "id": r["home_id"],
                    "name": r["home_name"],
                    "logo": r["home_logo"],
                    "goals": r["home_ft"],
                    "red_cards": r["home_red_cards"],
                },
                "away": {
                    "id": r["away_id"],
                    "name": r["away_name"],
                    "logo": r["away_logo"],
                    "goals": r["away_ft"],
                    "red_cards": r["away_red_cards"],
                },
            }
        )

    return {
        "league_id": league_id,
        "date": norm_date,
        "season": season,
        "round": round_name,
        "fixtures": fixtures,
    }


# ─────────────────────────────────────
#  3) 다음/이전 매치데이
# ─────────────────────────────────────


def _find_matchday(date_str: str, league_id: Optional[int], direction: str) -> Optional[str]:
    """
    direction: 'next' or 'prev'
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
            COUNT(*)         AS matches
        FROM matches m
        WHERE {where_clause}
        GROUP BY match_date
        ORDER BY match_date ASC
        """,
        tuple(params),
    )

    target = datetime.fromisoformat(norm_date).date()
    nearest: Optional[date_cls] = None

    for r in rows:
        md: date_cls = r["match_date"]
        if direction == "next":
            if md > target and (nearest is None or md < nearest):
                nearest = md
        else:
            if md < target and (nearest is None or md > nearest):
                nearest = md

    if not nearest:
        return None
    return nearest.isoformat()


def get_next_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    return _find_matchday(date_str, league_id, direction="next")


def get_prev_matchday(date_str: str, league_id: Optional[int]) -> Optional[str]:
    return _find_matchday(date_str, league_id, direction="prev")


# ─────────────────────────────────────
#  4) 팀 시즌 스탯 + Insights Overall (시즌 전체 기준)
# ─────────────────────────────────────


def get_team_season_stats(
    team_id: int,
    league_id: int,
    season: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """
    team_season_stats 테이블에서 (league_id, team_id)에 해당하는
    한 시즌에 대한 한 줄을 가져오고,
    stats["value"] 안의 insights_overall 블록을
    섹션별 모듈(enrich_overall_*)을 통해 채워서 반환한다.

    season 이 None 이면 기존처럼 가장 최신 season 1개를 사용하고,
    season 이 지정되면 해당 season 만 사용한다.
    """
    # ─────────────────────────────────────
    # 1) team_season_stats 원본 row 조회
    # ─────────────────────────────────────
    where_clause = """
        WHERE league_id = %s
          AND team_id   = %s
    """
    params: list[Any] = [league_id, team_id]

    # season 이 지정되면 해당 시즌만 필터링
    if season is not None:
        where_clause += "\n          AND season   = %s"
        params.append(season)

    order_limit = ""
    if season is None:
        # season 이 지정되지 않은 경우에만 "가장 최신 시즌 1개" 규칙 적용
        order_limit = "\n        ORDER BY season DESC\n        LIMIT 1"

    rows = fetch_all(
        f"""
        SELECT
            league_id,
            season,
            team_id,
            name,
            value
        FROM team_season_stats
        {where_clause}
        {order_limit}
        """,
        tuple(params),
    )
    if not rows:
        return None

    row = rows[0]
    raw_value = row.get("value")

    # value(JSON) 파싱
    if isinstance(raw_value, str):
        try:
            stats: Dict[str, Any] = json.loads(raw_value)
        except Exception:
            stats = {}
    elif isinstance(raw_value, dict):
        stats = raw_value
    else:
        stats = {}

    if not isinstance(stats, dict):
        stats = {}

    # insights_overall 블록 보장
    insights = stats.get("insights_overall")
    if not isinstance(insights, dict):
        insights = {}
        stats["insights_overall"] = insights

    # ✅ 서버에서 다시 계산하는 지표인데,
    #    원래 JSON 안에서 null 로 들어온 값은 미리 지워준다.
    for k in [
        "win_pct",
        "btts_pct",
        "team_over05_pct",
        "team_over15_pct",
        "over15_pct",
        "over25_pct",
        "clean_sheet_pct",
        "no_goals_pct",
        "score_1h_pct",
        "score_2h_pct",
        "concede_1h_pct",
        "concede_2h_pct",
        "score_0_15_pct",
        "concede_0_15_pct",
        "score_80_90_pct",
        "concede_80_90_pct",
        "first_to_score_pct",
        "first_conceded_pct",
        "when_leading_win_pct",
        "when_leading_draw_pct",
        "when_leading_loss_pct",
        "when_trailing_win_pct",
        "when_trailing_draw_pct",
        "when_trailing_loss_pct",
        "shots_per_match",
        "shots_on_target_pct",
        "win_and_over25_pct",
        "lose_and_btts_pct",
        "goal_diff_avg",
        "corners_per_match",
        "yellow_per_match",
        "red_per_match",
        "opp_red_sample",
        "opp_red_scored_pct",
        "opp_red_goals_after_avg",
        "own_red_sample",
        "own_red_conceded_pct",
        "own_red_goals_after_avg",
        "goals_by_time_for",
        "goals_by_time_against",
    ]:
        if k in insights and insights[k] is None:
            del insights[k]

    # fixtures.played.total (API에서 온 경기수) 추출
    fixtures = stats.get("fixtures") or {}
    played = fixtures.get("played") or {}
    matches_total_api = played.get("total") or 0

    # 시즌 값
    season_val = row.get("season")
    try:
        season_int = int(season_val)
    except (TypeError, ValueError):
        season_int = None

    # season_int 가 있어야 나머지 enrich_* 계산 가능
    if season_int is not None:
        # Shooting & Efficiency
        try:
            enrich_overall_shooting_efficiency(
                stats,
                insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
                matches_total_api=matches_total_api,
            )
        except Exception:
            pass

        # Outcome & Totals + Result Combos & Draw
        try:
            enrich_overall_outcome_totals(
                stats,
                insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
            )
        except Exception:
            pass

        # Goals by Time (For / Against)
        try:
            enrich_overall_goals_by_time(
                stats,
                insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
            )
        except Exception:
            pass

        # Discipline & Set Pieces
        try:
            enrich_overall_discipline_setpieces(
                stats,
                insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
                matches_total_api=matches_total_api,
            )
        except Exception:
            pass

        # Timing
        try:
            enrich_overall_timing(
                stats,
                insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
            )
        except Exception:
            pass

        # First Goal & Momentum
        try:
            enrich_overall_firstgoal_momentum(
                stats,
                insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
            )
        except Exception:
            pass

    # 최종 결과 row 형태로 반환
    return {
        "league_id": row["league_id"],
        "season": row["season"],
        "team_id": row["team_id"],
        "name": row["name"],
        "value": stats,
    }


# ─────────────────────────────────────
#  4-1) 팀 인사이트 (필터 메타 + 필터 적용 Outcome)
# ─────────────────────────────────────


def get_team_insights_overall_with_filters(
    team_id: int,
    league_id: int,
    *,
    season: Optional[int] = None,
    comp: Optional[str] = None,
    last_n: Optional[str] = None,
) -> Optional[Dict[str, Any]]:
    """
    Insights Overall 탭에서 Season / Competition / Last N 필터를 적용하기 위한
    서비스 함수.

    현재 단계:
      1) get_team_season_stats() 를 호출해서
         (season 이 지정되면 해당 시즌, 아니면 최신 시즌) 기준으로
         시즌 전체 insights_overall 을 먼저 계산하고,
      2) 필터 메타(insights_filters)를 붙인 뒤,
      3) last_n > 0 인 경우에만 Outcome & Totals 섹션을
         해당 시즌의 '최근 N경기' 기준으로 다시 계산해서 덮어쓴다.
         (다른 섹션은 아직 시즌 전체 기준 그대로)
    """
    # 1) 필터 메타 정규화
    filters_meta = build_insights_filter_meta(comp, last_n)
    comp_norm = filters_meta.get("competition", "All")
    last_n_int = filters_meta.get("last_n", 0)

    # 2) 시즌 전체 기준 기본 데이터 로드
    base = get_team_season_stats(
        team_id=team_id,
        league_id=league_id,
        season=season,  # 🔹 시즌 필터 반영: 2025 / 2024 등
    )
    if base is None:
        return None

    value = base.get("value")
    if not isinstance(value, dict):
        value = {}
    insights = value.get("insights_overall")
    if not isinstance(insights, dict):
        insights = {}
        value["insights_overall"] = insights

    # 필터 메타를 value에 붙여준다.
    value["insights_filters"] = filters_meta
    base["value"] = value

    # 🔥 2-1) 기본 시즌 경기 수(fixtures.played.total)에서 샘플 수 베이스를 만든다.
    fixtures = value.get("fixtures") or {}
    played = fixtures.get("played") or {}
    matches_total_api = played.get("total") or 0
    try:
        matches_total_int = int(matches_total_api)
    except (TypeError, ValueError):
        matches_total_int = 0

    # 3) last_n > 0 이면 Outcome & Totals 만 최근 N경기 기준으로 다시 계산
    if last_n_int and last_n_int > 0:
        season_val = base.get("season")
        try:
            season_int = int(season_val)
        except (TypeError, ValueError):
            season_int = None

        if season_int is not None:
            try:
                enrich_overall_outcome_totals(
                    stats=value,
                    insights=insights,
                    league_id=league_id,
                    season_int=season_int,
                    team_id=team_id,
                    # 필터 샘플에서는 분모를 실제 매치 수로 쓰기 위해 0으로 넘긴다.
                    matches_total_api=0,
                    last_n=last_n_int,
                )
            except Exception:
                # 필터 계산에 실패해도 기본 시즌 전체 값은 이미 들어가 있으므로 응답은 유지
                pass

    # 🔥 3-1) Events / First Goal sample 수를 insights_overall 에 넣어준다.
    #        - last_n 이 없으면 시즌 전체 경기 수
    #        - last_n 이 있으면 min(last_n, 시즌 전체 경기 수)를 사용
    if last_n_int and last_n_int > 0:
        if matches_total_int > 0:
            events_sample = min(last_n_int, matches_total_int)
        else:
            # fixtures 정보가 없으면 일단 last_n 을 그대로 사용 (보수적 추정)
            events_sample = last_n_int
    else:
        events_sample = matches_total_int

    # first_goal_sample 은 현재는 별도의 분모를 쓰지 않고,
    # 일단 events_sample 과 동일하게 내려준다. (나중에 필요시 분리 가능)
    first_goal_sample = events_sample

    insights["events_sample"] = events_sample
    insights["first_goal_sample"] = first_goal_sample

    # (competition 필터(comp_norm)는 아직 계산에 직접 사용하지 않고,
    #  메타만 내려보내는 상태. 나중에 League/Cup/Europe/Continental 분기 로직을
    #  추가할 때 comp_norm도 같이 활용하게 된다.)
    return base


# ─────────────────────────────────────
#  X) 팀별 사용 가능한 시즌 목록
# ─────────────────────────────────────


def get_team_seasons(league_id: int, team_id: int) -> List[int]:
    """
    team_season_stats 테이블에서 해당 리그/팀의 시즌 목록만 뽑아서
    최신순으로 돌려준다. (예: [2025, 2024])
    """
    rows = fetch_all(
        """
        SELECT DISTINCT season
        FROM team_season_stats
        WHERE league_id = %s
          AND team_id   = %s
        ORDER BY season DESC
        """,
        (league_id, team_id),
    )
    seasons: List[int] = []
    for r in rows:
        try:
            seasons.append(int(r["season"]))
        except (TypeError, ValueError):
            continue
    return seasons


# ─────────────────────────────────────
#  5) 팀 기본 정보
# ─────────────────────────────────────


def get_team_info(team_id: int) -> Optional[Dict[str, Any]]:
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
