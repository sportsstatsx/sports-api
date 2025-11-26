from __future__ import annotations
from typing import Any, Dict, Optional, List

from db import fetch_all
from services.insights.insights_overall_outcome_totals import enrich_overall_outcome_totals
from services.insights.insights_overall_timing import enrich_overall_timing
from services.insights.insights_overall_firstgoal_momentum import enrich_overall_firstgoal_momentum
from services.insights.insights_overall_shooting_efficiency import enrich_overall_shooting_efficiency
from services.insights.insights_overall_discipline_setpieces import enrich_overall_discipline_setpieces
from services.insights.insights_overall_goalsbytime import enrich_overall_goals_by_time
from services.insights.insights_overall_resultscombos_draw import enrich_overall_resultscombos_draw
from services.insights.utils import parse_last_n, normalize_comp


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

    # last_n == 0 이면 시즌 전체 모드 → 각 섹션에서 기본 리그 한 개만 사용하도록 둔다.
    if not last_n or last_n <= 0:
        return filters

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

        # UEFA 계열 대회 (챔스/유로파/컨퍼런스 등)
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
        comp_lower = str(comp_std).strip().lower()

        # 완전 일치
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

    # 아래 모든 섹션은 동일한 stats["insights_filters"] 기준으로
    # league_ids_for_query + last_n 을 적용해서 같은 샘플을 사용한다.

    enrich_overall_outcome_totals(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=0,
        last_n=last_n,
    )

    enrich_overall_timing(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
    )

    enrich_overall_firstgoal_momentum(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        last_n=last_n,
    )

    enrich_overall_shooting_efficiency(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=0,
        last_n=last_n,
    )

    enrich_overall_discipline_setpieces(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=0,
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

    enrich_overall_resultscombos_draw(
        stats,
        insights,
        league_id=league_id,
        season_int=season_int,
        team_id=team_id,
        matches_total_api=0,
    )

    return insights


# ─────────────────────────────────────
#  필터 옵션용 헬퍼
# ─────────────────────────────────────
def _build_comp_options_for_team(
    *, league_id: int, season_int: int, team_id: int
) -> List[str]:
    """
    이 팀이 해당 시즌에 실제로 뛴 대회를 기준으로
    Competition 드롭다운 옵션을 만든다.
    (All / League / Cup / Europe (UEFA) / Continental + 개별 대회명)
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

    comp_options: List[str] = ["All", "League"]

    has_cup = False
    has_uefa = False
    has_acl = False
    league_names: List[str] = []

    for r in rows:
        name = (r.get("league_name") or "").strip()
        if not name:
            continue
        league_names.append(name)
        lower = name.lower()

        if (
            "cup" in lower
            or "copa" in lower
            or "컵" in lower
            or "taça" in lower
            or "杯" in lower
        ):
            has_cup = True

        if (
            "uefa" in lower
            or "champions league" in lower
            or "europa league" in lower
            or "conference league" in lower
        ):
            has_uefa = True

        if "afc" in lower or "acl" in lower or "afc champions league" in lower:
            has_acl = True

    if has_cup and "Cup" not in comp_options:
        comp_options.append("Cup")
    if has_uefa and "Europe (UEFA)" not in comp_options:
        comp_options.append("Europe (UEFA)")
    if has_acl and "Continental" not in comp_options:
        comp_options.append("Continental")

    # 개별 대회명 추가
    for name in league_names:
        if name not in comp_options:
            comp_options.append(name)

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

    # 선택된 last_n (라벨 → 숫자) 파싱
    last_n = _get_last_n_from_header(header)

    # 헤더의 필터 블록 (라벨 그대로, comp / last_n 문자열 등)
    filters_block = _get_filters_from_header(header)
    comp_raw = filters_block.get("comp")

    # 🔥 여기에서 comp + last_n 교집합 기준으로
    #    home / away 둘 다 같은 샘플을 쓰도록 이미 구현되어 있음
    home_ins = _build_side_insights(
        league_id=league_id,
        season_int=season_int,
        team_id=home_team_id,
        last_n=last_n,
        comp_raw=comp_raw,
        header_filters=filters_block,
    )
    away_ins = _build_side_insights(
        league_id=league_id,
        season_int=season_int,
        team_id=away_team_id,
        last_n=last_n,
        comp_raw=comp_raw,
        header_filters=filters_block,
    )

    # ───────── UI에서 쓸 필터 옵션 리스트 구성 (동적 생성) ─────────
    comp_opts_home = _build_comp_options_for_team(
        league_id=league_id,
        season_int=season_int,
        team_id=home_team_id,
    )
    comp_opts_away = _build_comp_options_for_team(
        league_id=league_id,
        season_int=season_int,
        team_id=away_team_id,
    )

    GROUP_LABELS = {"League", "Cup", "Europe (UEFA)", "Continental"}

    # 각 팀별 "실제 대회 이름"만 추출 (All / 그룹 라벨 제외)
    names_home = [
        opt for opt in comp_opts_home
        if opt not in GROUP_LABELS and opt != "All"
    ]
    names_away = [
        opt for opt in comp_opts_away
        if opt not in GROUP_LABELS and opt != "All"
    ]

    # 양 팀이 둘 다 뛴 대회(교집합)만 사용
    common_names = sorted(set(names_home) & set(names_away))

    # 혹시라도 교집합이 완전히 비면, 최소한 합집합이라도 보여주기 (안전장치)
    if not common_names:
        common_names = sorted(set(names_home) | set(names_away))

    # 최종 comp 옵션: All + 공통 대회들
    comp_options = ["All"] + common_names

    comp_label = (filters_block.get("comp") or "All").strip() or "All"

    # 이전에 League / Cup / Europe (UEFA) 같은 그룹이 선택돼 있었다면 All 로 폴백
    if comp_label in GROUP_LABELS:
        comp_label = "All"

    # comp_label 이 옵션 리스트에 없으면 All 다음에 추가
    if comp_label not in comp_options:
        if comp_label == "All":
            pass  # 이미 맨 앞
        else:
            comp_options.insert(1, comp_label)

    # last_n 옵션은 기존 로직 그대로
    last_n_options = _build_last_n_options_for_match(
        home_team_id=home_team_id,
        away_team_id=away_team_id,
    )
    last_n_label = (filters_block.get("last_n") or "Last 10").strip() or "Last 10"
    if last_n_label not in last_n_options:
        last_n_options.insert(0, last_n_label)

    filters_for_client: Dict[str, Any] = {
        "comp": {
            "options": comp_options,
            "selected": comp_label,
        },
        "last_n": {
            "options": last_n_options,
            "selected": last_n_label,
        },
    }

    return {
        "league_id": league_id,
        "season": season_int,
        "last_n": last_n,  # 숫자형 (실제 샘플 계산용)
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        # 🔥 여기부터는 앱 UI용 필터 메타
        "filters": filters_for_client,
        # 실제 섹션 데이터
        "home": home_ins,
        "away": away_ins,
    }
