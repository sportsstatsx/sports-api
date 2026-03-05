from __future__ import annotations

import json
from datetime import datetime, date as date_cls, time as time_cls
from typing import Any, Dict, List, Optional, Tuple

import pytz

from db import fetch_all

from matchdetail.insights_block import (
    enrich_overall_outcome_totals,
    enrich_overall_goals_by_time,
    parse_last_n,
    normalize_comp,
)


from .league_directory_service import build_league_directory



# ─────────────────────────────────────
#  공통: 날짜 파싱/정규화
# ─────────────────────────────────────


def _normalize_date(date_str: Optional[str]) -> str:
    """
    다양한 형태(YYYY-MM-DD, YYYY-MM-DDTHH:MM:SS 등)의 문자열을
    안전하게 'YYYY-MM-DD' 로 정규화한다.
    """
    if not date_str:
        # 오늘 날짜 (UTC 기준)
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


def _get_utc_range_for_local_date(
    date_str: Optional[str],
    timezone_str: str,
) -> Tuple[datetime, datetime]:
    """
    date_str(YYYY-MM-DD)을 timezone_str (예: 'Asia/Seoul') 기준 '하루'로 보고,
    그 하루가 커버하는 UTC 시작/끝 datetime 을 반환한다.

    - DB matches.date_utc 는 항상 UTC로 저장되어 있고,
    - 여기서 계산한 utc_start ~ utc_end 범위로 필터링하면
      "사용자 로컬 Today" 기준으로 경기를 가져올 수 있다.
    """
    try:
        tz = pytz.timezone(timezone_str)
    except Exception:
        tz = pytz.UTC

    if not date_str:
        local_now = datetime.now(tz)
        local_date = local_now.date()
    else:
        try:
            local_date = datetime.fromisoformat(str(date_str)).date()
        except Exception:
            local_date = datetime.now(tz).date()

    local_start = tz.localize(datetime.combine(local_date, time_cls(0, 0, 0)))
    local_end = tz.localize(datetime.combine(local_date, time_cls(23, 59, 59)))

    utc_start = local_start.astimezone(pytz.UTC)
    utc_end = local_end.astimezone(pytz.UTC)
    return utc_start, utc_end


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


def _resolve_default_team_season_for_league(
    *,
    team_id: int,
    league_id: int,
    min_finished: int = 5,
) -> Optional[int]:
    """
    A안(근본해결):
    - team_season_stats / matches 를 함께 보고,
      "해당 팀이 해당 리그에서 완료(FINISHED) 경기 수가 충분한 시즌" 중
      가장 최신 season을 기본 시즌으로 선택한다.
    - 시즌이 막 시작해서 완료 경기가 거의 없으면 자동으로 이전 시즌으로 폴백.
    """
    try:
        rows = fetch_all(
            """
            WITH seasons AS (
                SELECT DISTINCT season
                FROM team_season_stats
                WHERE league_id = %s
                  AND team_id   = %s
            ),
            finished AS (
                SELECT
                    m.season,
                    SUM(
                        CASE
                            WHEN COALESCE(m.status_group, '') = 'FINISHED'
                              OR COALESCE(m.status, '') IN ('FT', 'AET', 'PEN')
                              OR COALESCE(m.status_short, '') IN ('FT', 'AET', 'PEN')
                            THEN 1 ELSE 0
                        END
                    ) AS finished_cnt
                FROM matches m
                WHERE m.league_id = %s
                  AND (m.home_id = %s OR m.away_id = %s)
                GROUP BY m.season
            )
            SELECT
                s.season,
                COALESCE(f.finished_cnt, 0) AS finished_cnt
            FROM seasons s
            LEFT JOIN finished f
              ON f.season = s.season
            ORDER BY
                (COALESCE(f.finished_cnt, 0) >= %s) DESC,
                s.season DESC
            LIMIT 1
            """,
            (league_id, team_id, league_id, team_id, team_id, int(min_finished)),
        )
        if not rows:
            return None
        s = rows[0].get("season")
        return int(s) if s is not None else None
    except Exception:
        return None



def _get_team_competitions_for_season(
    team_id: int,
    base_league_id: int,
    season_int: int,
) -> Dict[str, Any]:
    """
    주어진 시즌에서 특정 팀이 실제로 뛴 대회(리그/국내컵/대륙컵)를
    DB 기준으로 분류해서 돌려준다.

    반환 형태 예시:

    {
        "base_league": {"league_id": 39, "name": "...", "country": "..."},
        "all_league_ids": [39, 45, 100, ...],
        "cup_league_ids": [100, 101, ...],
        "uefa_league_ids": [...],
        "acl_league_ids": [...],
        "other_continental_league_ids": [...],
        "competitions": [
            {"league_id": 39, "name": "...", "country": "...", "category": "league"},
            {"league_id": 100, "name": "...", "country": "...", "category": "cup"},
            {"league_id": 2, "name": "...", "country": "...", "category": "uefa"},
            ...
        ],
    }
    """

    # 1) 베이스 리그 메타 정보 (나라 비교용)
    base_rows = fetch_all(
        """
        SELECT id, name, country
        FROM leagues
        WHERE id = %s
        LIMIT 1
        """,
        (base_league_id,),
    )
    base_name: Optional[str] = None
    base_country: Optional[str] = None
    if base_rows:
        base_name = (base_rows[0].get("name") or "").strip() or None
        base_country = (base_rows[0].get("country") or "").strip() or None

    # 2) 이 팀이 해당 시즌에 실제로 뛴 모든 대회 목록
    rows = fetch_all(
        """
        SELECT DISTINCT
            m.league_id,
            l.name   AS league_name,
            l.country AS country
        FROM matches m
        JOIN leagues l
          ON m.league_id = l.id
        WHERE m.season = %s
          AND (m.home_id = %s OR m.away_id = %s)
        """,
        (season_int, team_id, team_id),
    )

    all_ids: set[int] = set()
    cup_ids: set[int] = set()
    uefa_ids: set[int] = set()
    acl_ids: set[int] = set()
    other_cont_ids: set[int] = set()
    competitions: List[Dict[str, Any]] = []

    for r in rows:
        lid = r["league_id"]
        name = (r.get("league_name") or "").strip()
        country = (r.get("country") or "").strip()

        all_ids.add(lid)

        if lid == base_league_id:
            # 현재 화면의 베이스 리그
            category = "league"
        else:
            lower_name = name.lower()
            lower_country = country.lower()

            # 같은 나라면 → 그 나라 컵대회로 본다.
            if base_country and country and country == base_country:
                category = "cup"
                cup_ids.add(lid)
            else:
                # 유럽 계열 대륙컵 (UCL/UEL/UECL/Conference/UEFA 등) → UEFA 그룹
                if (
                    "uefa" in lower_name
                    or "ucl" in lower_name
                    or "champions league" in lower_name
                    or "europa" in lower_name
                    or "conference" in lower_name
                    or "europe" in lower_country
                ):
                    category = "uefa"
                    uefa_ids.add(lid)
                # 아시아 계열 대륙컵 (AFC Champions League / ACL 등) → ACL 그룹
                elif (
                    "afc" in lower_name
                    or "asia" in lower_name
                    or "asian" in lower_name
                    or "acl" in lower_name
                    or "afc" in lower_country
                    or "asia" in lower_country
                ):
                    category = "acl"
                    acl_ids.add(lid)
                else:
                    # 그 외 대륙컵 (CONMEBOL/CONCACAF 등)
                    category = "other_continental"
                    other_cont_ids.add(lid)

        competitions.append(
            {
                "league_id": lid,
                "name": name or None,
                "country": country or None,
                "category": category,
            }
        )

    return {
        "base_league": {
            "league_id": base_league_id,
            "name": base_name,
            "country": base_country,
        },
        # 이 시즌에 이 팀이 실제로 뛴 모든 대회 ID
        "all_league_ids": sorted(all_ids),
        # 같은 나라의 컵대회들
        "cup_league_ids": sorted(cup_ids),
        # 유럽 계열 대륙컵 (UCL/UEL/UECL 등)
        "uefa_league_ids": sorted(uefa_ids),
        # 아시아 계열 대륙컵 (ACL 등)
        "acl_league_ids": sorted(acl_ids),
        # 그 외 대륙컵
        "other_continental_league_ids": sorted(other_cont_ids),
        # 디버깅/확인용 전체 목록
        "competitions": competitions,
    }

def _resolve_target_league_ids_for_last_n(
    base_league_id: int,
    comp_norm: str,
    comp_detail: Dict[str, Any],
) -> List[int]:
    """
    Competition 필터(comp_norm)와 competition_detail 메타 정보를 이용해서
    Last N 계산에 사용할 리그 ID 목록을 결정한다.

    - base_league_id: 현재 화면의 베이스 리그 (예: EPL, K League 1)
    - comp_norm: normalize_comp() 로 정규화된 competition 값
    - comp_detail: _get_team_competitions_for_season() 이 내려준 딕셔너리
    """
    # 안전장치: comp_detail 이 없으면 항상 베이스 리그만 사용
    if not isinstance(comp_detail, dict):
        return [base_league_id]

    comp_norm_str = (comp_norm or "All") if isinstance(comp_norm, str) else "All"
    comp_norm_lower = comp_norm_str.strip().lower()

    all_ids = comp_detail.get("all_league_ids") or []
    cup_ids = comp_detail.get("cup_league_ids") or []
    uefa_ids = comp_detail.get("uefa_league_ids") or []
    acl_ids = comp_detail.get("acl_league_ids") or []
    other_cont_ids = comp_detail.get("other_continental_league_ids") or []
    competitions = comp_detail.get("competitions") or []

    def _fallback(ids: List[int]) -> List[int]:
        ids_clean: List[int] = []
        for x in ids:
            try:
                ids_clean.append(int(x))
            except (TypeError, ValueError):
                continue
        if ids_clean:
            return ids_clean
        return [base_league_id]

    # 1) All → 이 시즌 이 팀이 출전한 모든 대회
    if comp_norm_lower in ("all", "", "전체"):
        return _fallback(all_ids)

    # 2) League → 항상 베이스 리그 한 개만
    if comp_norm_lower in ("league", "리그"):
        return [base_league_id]

    # 3) UEFA 대륙컵 그룹
    if comp_norm_lower in ("uefa", "europe (uefa)", "ucl", "champions league"):
        return _fallback(uefa_ids)

    # 4) ACL (아시아 대륙컵 그룹)
    if comp_norm_lower in ("acl", "asia (acl)", "afc champions league", "afc"):
        return _fallback(acl_ids)

    # 5) Domestic Cup 전체 (특정 이름 없이 "Cup" 만 들어온 경우)
    if comp_norm_lower in ("cup", "domestic cup", "국내컵"):
        return _fallback(cup_ids)

    # 6) 그 외에는 comp_norm 이 "FA Cup", "Emperor's Cup" 처럼
    #    특정 대회 이름과 같다고 보고, competitions 목록에서 매칭을 시도한다.
    for comp_row in competitions:
        name = (comp_row.get("name") or "").strip()
        category = comp_row.get("category")
        lid = comp_row.get("league_id")
        if not name or lid is None:
            continue
        if name.strip().lower() == comp_norm_lower:
            try:
                return [int(lid)]
            except (TypeError, ValueError):
                return [base_league_id]

    # 매칭되는 게 없으면 최종 fallback: 베이스 리그 한 개
    return [base_league_id]



# ─────────────────────────────────────
#  1) 홈 화면: 상단 리그 탭용 목록
# ─────────────────────────────────────


def get_home_leagues(
    date_str: Optional[str],
    timezone_str: str,
    league_ids: Optional[List[int]] = None,
) -> List[Dict[str, Any]]:
    """
    ✅ 서버 단일 기준(home_config.py)을 따르는 Today 리그 목록

    정책:
      - 오늘(사용자 로컬 date) 경기 있는 리그만
      - 반드시 SUPPORTED_LEAGUE_IDS 안에서만
      - (선택) league_ids 파라미터가 오면 그 subset만
      - 반환 정렬은 DB가 아니라 home_config의 "홈 매치리스트 섹션 순서" 기준
    """
    from services.home_config import SUPPORTED_LEAGUE_IDS, sort_leagues_for_home

    utc_start, utc_end = _get_utc_range_for_local_date(date_str, timezone_str)

    supported = set(int(x) for x in (SUPPORTED_LEAGUE_IDS or []))

    # 요청 league_ids가 오면: supported ∩ 요청값
    requested: Optional[set[int]] = None
    if league_ids:
        requested = set()
        for x in league_ids:
            try:
                requested.add(int(x))
            except (TypeError, ValueError):
                continue

    target_ids = supported if requested is None else (supported & requested)

    # 타겟이 비면 바로 빈 배열
    if not target_ids:
        return []

    params: List[Any] = [utc_start, utc_end]
    placeholders = ", ".join(["%s"] * len(target_ids))
    params.extend(sorted(target_ids))

    rows = fetch_all(
        f"""
        SELECT
            m.league_id,
            l.name    AS league_name,
            l.country AS country,
            l.logo    AS league_logo,
            MAX(m.season) AS season
        FROM matches m
        JOIN leagues l
          ON m.league_id = l.id
        WHERE m.date_utc::timestamptz BETWEEN %s AND %s
          AND m.league_id IN ({placeholders})
        GROUP BY
            m.league_id,
            l.name,
            l.country,
            l.logo
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
                "season": r.get("season"),
            }
        )

    # ✅ 정렬을 home_config 기준으로 고정
    try:
        return sort_leagues_for_home(result)
    except Exception:
        # 정렬 실패해도 최소한 name 기준 안정 정렬
        return sorted(result, key=lambda x: (str(x.get("country") or ""), str(x.get("name") or "")))



# ─────────────────────────────────────
#  2) 홈 화면: 리그 선택 바텀시트용 디렉터리
# ─────────────────────────────────────


def get_home_league_directory(
    date_str: Optional[str],
    timezone_str: str,
) -> Dict[str, Any]:
    """
    ✅ 리그 선택 바텀시트(스크린샷 구조 지원)

    반환(rows):
      {
        "today": [
          {"continent": "Europe", "count": 8, "items": [ ... ]},
          {"continent": "Asia", "count": 2, "items": [ ... ]},
          ...
        ],
        "no_games": [
          {"continent": "Europe", "count": 16, "items": [ ... ]},
          {"continent": "Asia", "count": 8, "items": [ ... ]},
          {"continent": "Americas", "count": 6, "items": [ ... ]},
          ...
        ]
      }

    - today: 오늘(사용자 로컬 date) 경기 있는 리그들
    - no_games: 오늘 경기 없는 리그들
    - 정렬/대륙/티어 규칙은 home_config.filter_order 기반(=build_league_directory_from_config 결과) 그대로 사용
    """
    from services.home_config import (
        SUPPORTED_LEAGUE_IDS,
        CONTINENT_ORDER,
        build_league_directory_from_config,
    )

    # 1) config 기준 "전체" 디렉터리 (대륙/정렬 규칙 반영된 섹션 리스트)
    #    full_sections: [{"section": "Europe", "items": [...]}, ...]
    full_sections = build_league_directory_from_config(
        date_str=date_str,
        timezone_str=timezone_str,
    )

    # ✅ countries(name->flag) 맵을 만들어서, config item에 country_flag를 주입
    try:
        crow = fetch_all("SELECT name, flag FROM countries", tuple())

        name_to_flag: Dict[str, str] = {}
        for r in (crow or []):
            n = ""
            f = ""

            # ✅ fetch_all 구현에 따라 dict 또는 tuple/list로 올 수 있어서 둘 다 처리
            if isinstance(r, dict):
                n = (r.get("name") or "").strip()
                f = (r.get("flag") or "").strip()
            elif isinstance(r, (list, tuple)) and len(r) >= 2:
                n = (str(r[0]) if r[0] is not None else "").strip()
                f = (str(r[1]) if r[1] is not None else "").strip()
            else:
                continue

            if n and f:
                name_to_flag[n.lower()] = f

        # ✅ full_sections에 주입
        for sec in (full_sections or []):
            if not isinstance(sec, dict):
                continue
            items = sec.get("items") or []
            if not isinstance(items, list):
                continue

            for it in items:
                if not isinstance(it, dict):
                    continue
                cname = (it.get("country") or "").strip().lower()
                flag = name_to_flag.get(cname)
                if flag:
                    it["country_flag"] = flag

    except Exception as e:
        # ✅ 조용히 삼키지 말고 서버 로그에 남겨서 다음에 바로 잡히게
        try:
            import logging
            logging.getLogger("home_service").exception("country_flag inject failed: %s", e)
        except Exception:
            pass

    # 2) 오늘(로컬 date) 기준 UTC 범위
    utc_start, utc_end = _get_utc_range_for_local_date(date_str, timezone_str)

    supported: List[int] = []
    for x in (SUPPORTED_LEAGUE_IDS or []):
        try:
            supported.append(int(x))
        except (TypeError, ValueError):
            continue

    if not supported:
        return {"today": [], "no_games": []}

    # 3) 오늘 경기 있는 league_id 집합 계산 (matches 기준)
    placeholders = ", ".join(["%s"] * len(supported))
    params: List[Any] = [utc_start, utc_end]
    params.extend(supported)

    rows = fetch_all(
        f"""
        SELECT DISTINCT m.league_id
        FROM matches m
        WHERE m.date_utc::timestamptz BETWEEN %s AND %s
          AND m.league_id IN ({placeholders})
        """,
        tuple(params),
    )

    today_ids: set[int] = set()
    for r in rows:
        try:
            today_ids.add(int(r.get("league_id")))
        except Exception:
            continue

    # 4) full_sections 를 today / no_games 로 분리 + count 부착
    today_out: List[Dict[str, Any]] = []
    nog_out: List[Dict[str, Any]] = []

    for sec in (full_sections or []):
        continent = (sec.get("section") or "").strip()
        items = sec.get("items") or []

        if not continent or not isinstance(items, list):
            continue

        today_items: List[Dict[str, Any]] = []
        nog_items: List[Dict[str, Any]] = []

        for it in items:
            lid = it.get("league_id")
            try:
                lid_int = int(lid)
            except Exception:
                continue

            if lid_int in today_ids:
                today_items.append(it)
            else:
                nog_items.append(it)

        if today_items:
            today_out.append({"continent": continent, "count": len(today_items), "items": today_items})
        if nog_items:
            nog_out.append({"continent": continent, "count": len(nog_items), "items": nog_items})

    # 5) 대륙 순서 고정 (Europe > Asia > Americas)
    order_idx: Dict[str, int] = {}
    for i, c in enumerate(CONTINENT_ORDER or []):
        order_idx[str(c)] = i

    today_out.sort(key=lambda x: order_idx.get(str(x.get("continent")), 999))
    nog_out.sort(key=lambda x: order_idx.get(str(x.get("continent")), 999))

    return {"today": today_out, "no_games": nog_out}





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

    # ✅ A안: season 미지정이면 '완료 경기 수가 충분한 시즌'을 기본으로 확정
    if season is None:
        resolved = _resolve_default_team_season_for_league(
            team_id=team_id,
            league_id=league_id,
            min_finished=5,
        )
        if resolved is not None:
            season = resolved


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

    동작 순서:
      1) get_team_season_stats() 를 호출해서
         (season 이 지정되면 해당 시즌, 아니면 최신 시즌) 기준으로
         시즌 전체 insights_overall 을 먼저 계산하고,
      2) 필터 메타(insights_filters)를 value 에 붙여준다.
         - competition / last_n
         - competition_detail (이 시즌에 이 팀이 출전한 리그/컵/대륙컵 목록)
         - target_league_ids_last_n (Last N 계산에 사용할 league_id 리스트)
      3) last_n > 0 인 경우에만 일부 섹션을
         해당 시즌의 '최근 N경기' 기준으로 다시 계산해서 덮어쓴다.
         - Outcome & Totals
         - Goals by Time

    """
    # 1) 필터 메타 정규화
    filters_meta = build_insights_filter_meta(comp, last_n)
    comp_norm = filters_meta.get("competition", "All")
    last_n_int = filters_meta.get("last_n", 0)

    # ✅ A안: Insights에서도 season 미지정이면 기본 시즌을 확정(팀/리그 기준)
    if season is None:
        resolved = _resolve_default_team_season_for_league(
            team_id=team_id,
            league_id=league_id,
            min_finished=5,
        )
        if resolved is not None:
            season = resolved


    # 2) 시즌 전체 기준 기본 데이터 로드
    base = get_team_season_stats(
        team_id=team_id,
        league_id=league_id,
        season=season,  # 🔹 시즌 필터 반영: 2025 / 2024 등
    )
    if base is None:
        return None

    # 2-1) 시즌 값 정규화 (competition_detail / Last N 계산에 모두 사용)
    season_val = base.get("season")
    try:
        season_int_meta = int(season_val) if season_val is not None else None
    except (TypeError, ValueError):
        season_int_meta = None

    # 2-2) 이 시즌에 팀이 실제로 뛴 대회(리그/국내컵/대륙컵) 메타 정보 계산
    comp_detail: Optional[Dict[str, Any]] = None
    if season_int_meta is not None:
        try:
            comp_detail = _get_team_competitions_for_season(
                team_id=team_id,
                base_league_id=league_id,
                season_int=season_int_meta,
            )
        except Exception:
            comp_detail = None

        if comp_detail is not None:
            filters_meta["competition_detail"] = comp_detail

    # 2-3) Last N 계산에 사용할 리그 ID 리스트를 competition 필터에 맞게 선택
    target_league_ids_last_n = _resolve_target_league_ids_for_last_n(
        base_league_id=league_id,
        comp_norm=str(comp_norm) if comp_norm is not None else "All",
        comp_detail=comp_detail or filters_meta.get("competition_detail") or {},
    )
    filters_meta["target_league_ids_last_n"] = target_league_ids_last_n

    # 2-4) value / insights 초기화 및 필터 메타 부착
    value = base.get("value")
    if not isinstance(value, dict):
        value = {}
    insights = value.get("insights_overall")
    if not isinstance(insights, dict):
        insights = {}
        value["insights_overall"] = insights

    value["insights_filters"] = filters_meta
    base["value"] = value

    # 🔥 2-5) 기본 시즌 경기 수(fixtures.played.total)에서 샘플 수 베이스를 만든다.
    fixtures = value.get("fixtures") or {}
    played = fixtures.get("played") or {}
    matches_total_api = played.get("total") or 0
    try:
        matches_total_int = int(matches_total_api)
    except (TypeError, ValueError):
        matches_total_int = 0

   
    if last_n_int and last_n_int > 0 and season_int_meta is not None:
        season_int = season_int_meta

        # ✅ Outcome & Totals (Last N)
        try:
            enrich_overall_outcome_totals(
                stats=value,
                insights=insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
                matches_total_api=0,
                last_n=last_n_int,
            )
        except Exception:
            pass

        # ✅ Goals by Time (Last N)
        try:
            enrich_overall_goals_by_time(
                stats=value,
                insights=insights,
                league_id=league_id,
                season_int=season_int,
                team_id=team_id,
                last_n=last_n_int,
            )
        except Exception:
            pass


    # 🔥 3-1) Events / First Goal sample 수를 insights_overall 에 넣어준다.
    #        - 섹션(enrich_overall_outcome_totals)에서 이미 기록해 둔 값이 있으면 우선 사용
    #        - 없으면 기존 시즌 전체 / lastN 기반 로직을 그대로 사용
    existing_events_sample = insights.get("events_sample")
    if isinstance(existing_events_sample, int) and existing_events_sample > 0:
        events_sample = existing_events_sample
    else:
        # 기존 로직 그대로 유지
        #   - last_n 이 없으면 시즌 전체 경기 수
        #   - last_n 이 있으면 min(last_n, 시즌 전체 경기 수)를 사용
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


    # (competition 필터(comp_norm)는 현재 단계에서는
    #  계산에 직접 사용되는 것은 target_league_ids_last_n 뿐이고,
    #  나머지는 메타(insights_filters)로만 내려보낸다.
    #  -> 각 섹션 모듈에서 stats["insights_filters"]["target_league_ids_last_n"]
    #     를 참고해서 league_id IN (...) 조건을 적용하게 된다.)
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
