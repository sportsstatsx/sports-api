# services/matchdetail/standings_block.py

from typing import Any, Dict, Optional, List
import re

from db import fetch_all


def build_standings_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Match Detail용 Standings 블록.

    - league_id / season / home.id / away.id 를 기반으로 standings 테이블 조회.
    - 팀당 중복 row(스플릿 라운드 등)는 played 가 가장 큰 row만 남긴다.
    - group_name 이 여러 개(컨퍼런스 등)면, 우선 home 팀이 속한 그룹
      (없으면 away 팀 그룹)의 테이블만 사용한다.

    + 순수 A방식 준비:
      - rows 를 기반으로 conferences / groups 컨텍스트도 같이 계산해서 내려준다.
      - 앱 쪽 StandingsContext와 대응되는 정보의 일부(conferences, groups)를
        context_options 키로 함께 반환한다.
    """

    league_id = header.get("league_id")
    season = header.get("season")

    league_name = None
    league_info = header.get("league") or {}
    if isinstance(league_info, dict):
        league_name = league_info.get("name")

    def _extract_team_id(side_key: str) -> Optional[int]:
        side = header.get(side_key) or {}
        if not isinstance(side, dict):
            return None
        tid = side.get("id")
        try:
            return int(tid) if tid is not None else None
        except (TypeError, ValueError):
            return None

    home_team_id = _extract_team_id("home")
    away_team_id = _extract_team_id("away")

    if not league_id or not season:
        return None

    try:
        rows: List[Dict[str, Any]] = fetch_all(
            """
            SELECT
                s.rank,
                s.team_id,
                t.name       AS team_name,
                t.logo       AS team_logo,
                s.played,
                s.win,
                s.draw,
                s.lose,
                s.goals_for,
                s.goals_against,
                s.goals_diff,
                s.points,
                s.description,
                s.group_name,
                s.form
            FROM standings AS s
            JOIN teams     AS t ON t.id = s.team_id
            WHERE s.league_id = %s
              AND s.season    = %s
            ORDER BY s.group_name, s.rank
            """,
            (league_id, season),
        )
    except Exception:
        return None

    # 🔥 변경점: standings 데이터가 없을 때도 "빈 블록 + 안내 문구"를 내려준다
    if not rows:
        return {
            "league": {
                "league_id": league_id,
                "season": season,
                "name": league_name,
            },
            "rows": [],
            "context_options": {"conferences": [], "groups": []},
            "message": "Standings are not available yet.",
        }

    def _coalesce_int(v: Any, default: int = 0) -> int:
        try:
            return int(v)
        except (TypeError, ValueError):
            return default

    # 1) 팀당 중복 row 정리 (played 가장 큰 row만)
    rows_by_team: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        tid = _coalesce_int(r.get("team_id"), 0)
        if tid == 0:
            continue
        prev = rows_by_team.get(tid)
        if prev is None:
            rows_by_team[tid] = r
        else:
            prev_played = _coalesce_int(prev.get("played"), 0)
            cur_played = _coalesce_int(r.get("played"), 0)
            if cur_played > prev_played:
                rows_by_team[tid] = r

    dedup_rows: List[Dict[str, Any]] = list(rows_by_team.values())

    # 2) group_name 이 여러 개면, 보통은 home/away 팀이 속한 group 하나만 사용.
    #    단, East/West 컨퍼런스 리그(MLS 등)는 ALL / East / West 탭이 필요하므로
    #    전체 컨퍼런스 데이터를 그대로 유지한다.
    group_names = {
        (r.get("group_name") or "").strip()
        for r in dedup_rows
        if r.get("group_name") is not None
    }

    def _is_east_west_split(names) -> bool:
        lower = {g.lower() for g in names if g}
        has_east = any("east" in g for g in lower)
        has_west = any("west" in g for g in lower)
        return has_east and has_west

    if len(group_names) > 1 and not _is_east_west_split(group_names):
        main_group = None

        # 먼저 home 팀이 속한 그룹
        if home_team_id is not None:
            for r in dedup_rows:
                if _coalesce_int(r.get("team_id"), 0) == _coalesce_int(home_team_id, 0):
                    main_group = (r.get("group_name") or "").strip()
                    break

        # 없으면 away 팀 기준
        if main_group is None and away_team_id is not None:
            for r in dedup_rows:
                if _coalesce_int(r.get("team_id"), 0) == _coalesce_int(away_team_id, 0):
                    main_group = (r.get("group_name") or "").strip()
                    break

        if main_group:
            dedup_rows = [
                r
                for r in dedup_rows
                if (r.get("group_name") or "").strip() == main_group
            ]

    # 3) position 기준 정렬 후 JSON 매핑
    dedup_rows.sort(key=lambda r: _coalesce_int(r.get("rank"), 0))

    table: List[Dict[str, Any]] = []
    for r in dedup_rows:
        team_id = _coalesce_int(r.get("team_id"), 0)
        table.append(
            {
                "position": _coalesce_int(r.get("rank"), 0),
                "team_id": team_id,
                "team_name": r.get("team_name") or "",
                "team_logo": r.get("team_logo"),
                "played": _coalesce_int(r.get("played"), 0),
                "win": _coalesce_int(r.get("win"), 0),
                "draw": _coalesce_int(r.get("draw"), 0),
                "loss": _coalesce_int(r.get("lose"), 0),
                "goals_for": _coalesce_int(r.get("goals_for"), 0),
                "goals_against": _coalesce_int(r.get("goals_against"), 0),
                "goal_diff": _coalesce_int(r.get("goals_diff"), 0),
                "points": _coalesce_int(r.get("points"), 0),
                "description": r.get("description"),
                "group_name": r.get("group_name"),
                "form": r.get("form"),
                "is_home": (home_team_id is not None and team_id == home_team_id),
                "is_away": (away_team_id is not None and team_id == away_team_id),
            }
        )

    # 4) 순수 A방식 준비: conferences / groups 컨텍스트 생성
    context_options = _build_context_options_from_rows(dedup_rows)

    return {
        "league": {
            "league_id": league_id,
            "season": season,
            "name": league_name,
        },
        "rows": table,
        # 🔥 앞으로 앱 StandingsContext로 넘겨줄 수 있는 컨텍스트 정보
        # (지금은 안 써도 되고, 나중에 점진적으로 마이그레이션 가능)
        "context_options": context_options,
    }



def _build_context_options_from_rows(
    rows: List[Dict[str, Any]]
) -> Dict[str, List[str]]:
    """
    StandingsDao.buildContext(...) 에서 하던 컨퍼런스/그룹 인식 로직을
    서버쪽으로 옮긴 버전 (순수 A방식 준비).

    - conferences: ["East", "West"] 등
    - groups: ["Group A", "Group B", "Championship Round", "Relegation Round"] 등
    """
    if not rows:
        return {"conferences": [], "groups": []}

    # group_name / description 수집
    group_raw: List[str] = []
    desc_raw: List[str] = []
    for r in rows:
        g = r.get("group_name")
        d = r.get("description")
        if isinstance(g, str):
            g = g.strip()
            if g:
                group_raw.append(re.sub(r"\s+", " ", g))
        if isinstance(d, str):
            desc_raw.append(d.lower())

    group_raw = list(dict.fromkeys(group_raw))  # distinct, 순서 유지

    # “챔피언십/강등 + round/rnd” 인지 체크
    rx_has_split_round = re.compile(
        r"(champ(ion)?ship\s+.*(round|rnd))|(releg(ation)?\s+.*(round|rnd))",
        re.IGNORECASE,
    )
    rx_group = re.compile(r"group\s*([A-Z])", re.IGNORECASE)

    # 1) description 기반 라운드 파생
    def derive_from_description() -> List[str]:
        if not desc_raw:
            return []
        has_champ_round = any(
            rx_has_split_round.search(d) and "champ" in d for d in desc_raw
        )
        has_releg_round = any(
            rx_has_split_round.search(d) and "releg" in d for d in desc_raw
        )
        out: List[str] = []
        if has_champ_round:
            out.append("Championship Round")
        if has_releg_round:
            out.append("Relegation Round")
        return out

    # 2) 그룹명에서 컨퍼런스/그룹 라벨 추출
    has_east = any("east" in g.lower() for g in group_raw)
    has_west = any("west" in g.lower() for g in group_raw)
    has_grp = any(rx_group.search(g) for g in group_raw)
    has_rnd = any(rx_has_split_round.search(g) for g in group_raw)

    conferences: List[str] = []
    if has_east:
        conferences.append("East")
    if has_west:
        conferences.append("West")

    groups: List[str] = []
    # East/West 제거한 나머지에서 Group/라운드 추출
    for g in group_raw:
        gl = g.lower()
        if "east" in gl or "west" in gl:
            continue
        m = rx_group.search(g)
        if m:
            groups.append(f"Group {m.group(1).upper()}")
        elif rx_has_split_round.search(g) and "champ" in gl:
            groups.append("Championship Round")
        elif rx_has_split_round.search(g) and "releg" in gl:
            groups.append("Relegation Round")

    # 유효한 정보가 하나도 없으면 description 기반으로 보완
    has_meaningful = has_east or has_west or has_grp or has_rnd or bool(groups)
    if not has_meaningful:
        groups = derive_from_description()

    # 중복 제거 (대소문자 무시)
    def _dedup_case_insensitive(items: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for x in items:
            key = x.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(x)
        return out

    conferences = _dedup_case_insensitive(conferences)
    groups = _dedup_case_insensitive(groups)

    return {
        "conferences": conferences,
        "groups": groups,
    }
