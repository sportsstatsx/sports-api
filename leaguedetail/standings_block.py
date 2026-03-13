# leaguedetail/standings_block.py
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple
import re

from db import fetch_all


def _coalesce_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _fetch_one(query: str, params: tuple) -> Optional[Dict[str, Any]]:
    """
    fetch_all 래핑해서 첫 번째 row만 돌려주는 헬퍼.
    """
    rows = fetch_all(query, params)
    return rows[0] if rows else None


def _resolve_season(league_id: int, season: Optional[int]) -> Optional[int]:
    """
    season 이 None 이면:
      1) standings 에서 해당 리그의 MAX(season)
      2) 없으면 fixtures 에서 MAX(season)
    순서대로 시도해서 하나라도 찾으면 그 값 리턴.
    """
    if season is not None:
        return season

    # 1) standings 기준
    row = _fetch_one(
        """
        SELECT MAX(season) AS season
        FROM standings
        WHERE league_id = %s
        """,
        (league_id,),
    )
    if row is not None:
        s = _coalesce_int(row.get("season"), 0)
        if s > 0:
            return s

    # 2) fixtures 기준
    row = _fetch_one(
        """
        SELECT MAX(season) AS season
        FROM fixtures
        WHERE league_id = %s
        """,
        (league_id,),
    )
    if row is not None:
        s = _coalesce_int(row.get("season"), 0)
        if s > 0:
            return s

    return None


def _detect_league_cup_like(league_id: int) -> bool:
    try:
        cols = fetch_all(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'leagues'
            """
        )
        colset = {str(r.get("column_name") or "") for r in cols if r.get("column_name")}
    except Exception:
        return False

    select_cols: List[str] = []
    for c in ("type", "cup", "is_cup"):
        if c in colset:
            select_cols.append(c)

    if not select_cols:
        return False

    row = _fetch_one(
        f"SELECT {', '.join(select_cols)} FROM leagues WHERE id = %s",
        (league_id,),
    )
    if row is None:
        return False

    v_type = row.get("type")
    if isinstance(v_type, str) and v_type.strip().lower() == "cup":
        return True

    for key in ("cup", "is_cup"):
        v = row.get(key)
        if v is True:
            return True
        if isinstance(v, (int, float)) and int(v) == 1:
            return True
        if isinstance(v, str) and v.strip().lower() in ("1", "true", "t", "yes", "y"):
            return True

    return False


def _effective_group_name(
    *,
    raw_group_name: Any,
    description: Any,
) -> Optional[str]:
    """
    rows의 group_name이 리그명/기본값으로만 채워지고,
    실제 구분(Championship/Relegation)이 description에만 있는 리그(Austria 등) 보정.

    - description에 championship/relegation 라운드가 있으면 group_name을 그 값으로 강제
    - 이미 group_name이 Group A/B, East/West, Championship Round 등 의미 있는 값이면 유지
    """
    g = raw_group_name.strip() if isinstance(raw_group_name, str) else ""
    d = description.strip().lower() if isinstance(description, str) else ""

    # group_name 자체가 의미있으면 그대로 둠
    gl = g.lower()
    if gl:
        if ("champ" in gl and "round" in gl) or ("releg" in gl and "round" in gl):
            return g
        if gl.startswith("group "):
            return g
        if "east" in gl or "west" in gl:
            return g

    # description 기반 split round 보정
    if "champ" in d and "round" in d:
        return "Championship Round"
    if "releg" in d and "round" in d:
        return "Relegation Round"

    # 그 외는 기존 group_name 유지(빈 값이면 None)
    return g if g else None




# ────────────────────────────────────────────────────────────
#  컨퍼런스 / 그룹 / 스플릿 정보 추출 (context_options)
# ────────────────────────────────────────────────────────────

def _build_context_options_from_rows(
    rows: List[Dict[str, Any]]
) -> Dict[str, List[str]]:
    """
    matchdetail과 동일한 컨퍼런스/그룹 인식 로직.

    - conferences: ["East", "West"] 등
    - groups: ["Group A", "Group B", "Championship Round", "Relegation Round"] 등
    """
    if not rows:
        return {"conferences": [], "groups": []}

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

    rx_has_split_round = re.compile(
        r"(champ(ion)?ship\s+.*(round|rnd))|(releg(ation)?\s+.*(round|rnd))",
        re.IGNORECASE,
    )
    rx_group = re.compile(r"group\s*([A-Z])", re.IGNORECASE)

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

    has_meaningful = has_east or has_west or has_grp or has_rnd or bool(groups)
    if not has_meaningful:
        groups = derive_from_description()

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

    return {"conferences": conferences, "groups": groups}



def build_standings_block(
    league_id: int,
    season: Optional[int],
) -> Dict[str, Any]:


    """
    League Detail 화면의 'Standings' 탭 데이터.

    ✅ 완전무결(절대 흔들리지 않게):
    1) standings 테이블에 (league_id, season) rows가 있으면 그걸 우선 사용
    2) standings가 비어 있어도,
       - 해당 시즌에 "완료된 경기"가 1개라도 있으면(matches 기준)
       - 즉시 standings를 계산해서 내려준다 (현재 시즌 Standings 보장)
    3) 완료된 경기 자체가 0이면 rows=[] (아직 시즌 시작 전)
    """

    if not league_id:
        return {
            "league_id": None,
            "season": None,
            "rows": [],
            "context_options": {"conferences": [], "groups": []},
        }

    season_resolved = _resolve_season(league_id, season)
    if season_resolved is None:
        print(f"[build_standings_block] WARN: no season found for league_id={league_id}")
        return {
            "league_id": league_id,
            "season": None,
            "rows": [],
            "context_options": {"conferences": [], "groups": []},
        }

    league_name: Optional[str] = None
    try:
        league_row = _fetch_one(
            """
            SELECT name
            FROM leagues
            WHERE id = %s
            """,
            (league_id,),
        )
        if league_row is not None:
            league_name = (league_row.get("name") or "").strip() or None
    except Exception as e:
        print(f"[build_standings_block] WARN: failed to load league name league_id={league_id}: {e}")

    is_cup_like = _detect_league_cup_like(league_id)

    
    def _cols_of(table_name: str) -> set[str]:
        try:
            cols = fetch_all(
                """
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND table_name = %s
                """,
                (table_name,),
            )
            return {str(r.get("column_name") or "") for r in cols if r.get("column_name")}
        except Exception:
            return set()

    def _pick_pair(cols: set[str], pairs: List[tuple[str, str]]) -> Optional[tuple[str, str]]:
        for a, b in pairs:
            if a in cols and b in cols:
                return (a, b)
        return None

    # 1) standings 테이블 우선
    try:
        rows_raw: List[Dict[str, Any]] = fetch_all(
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
            ORDER BY
                s.group_name NULLS FIRST,
                s.rank       NULLS LAST,
                t.name       ASC
            """,
            (league_id, season_resolved),
        )
    except Exception as e:
        print(f"[build_standings_block] ERROR standings query league_id={league_id}, season={season_resolved}: {e}")
        rows_raw = []

    # 2) standings가 비어 있고 cup 성격 리그면
    #    computed_from_matches fallback 을 막는다.
    if not rows_raw and is_cup_like:
        return {
            "league_id": league_id,
            "season": season_resolved,
            "league_name": league_name,
            "rows": [],
            "context_options": {"conferences": [], "groups": []},
            "mode": "TABLE",
            "bracket": None,
            "message": "Standings are not available for this competition stage.\nPlease check back later.",
        }

    # 3) standings가 비어 있으면 → matches에서 즉시 계산
    if not rows_raw:
        # 2-1) matches 컬럼 자동 탐지(환경/스키마 흔들림 방어)
        mcols = _cols_of("matches")

        team_pair = _pick_pair(
            mcols,
            [
                ("home_team_id", "away_team_id"),
                ("home_id", "away_id"),
            ],
        )
        goal_pair = _pick_pair(
            mcols,
            [
                ("home_goals", "away_goals"),
                ("home_ft", "away_ft"),
                ("goals_home", "goals_away"),
                ("home_score", "away_score"),
            ],
        )

        if not team_pair or not goal_pair:
            # matches로도 계산 불가 → 그냥 빈 값
            return {
                "league_id": league_id,
                "season": season_resolved,
                "league_name": league_name,
                "rows": [],
                "context_options": {"conferences": [], "groups": []},
            }

        ht, at = team_pair
        hg, ag = goal_pair

        # 2-2) 완료된 경기 수 확인 (0이면 시즌 시작 전)
        try:
            cnt_row = _fetch_one(
                f"""
                SELECT COUNT(*) AS cnt
                FROM matches
                WHERE league_id = %s
                  AND season = %s
                  AND (
                    lower(coalesce(status_group,'')) = 'finished'
                    OR coalesce(status,'') IN ('FT','AET','PEN')
                    OR coalesce(status_short,'') IN ('FT','AET','PEN')
                  )
                  AND {ht} IS NOT NULL AND {at} IS NOT NULL
                  AND {hg} IS NOT NULL AND {ag} IS NOT NULL
                """,
                (league_id, season_resolved),
            )
            finished_cnt = int((cnt_row or {}).get("cnt") or 0)
        except Exception:
            finished_cnt = 0

        if finished_cnt <= 0:
            return {
                "league_id": league_id,
                "season": season_resolved,
                "league_name": league_name,
                "rows": [],
                "context_options": {"conferences": [], "groups": []},
            }

        # 2-3) matches 기반 standings 계산 (기본 포인트/득실/다득점 정렬)
        try:
            rows_raw = fetch_all(
                f"""
                WITH finished AS (
                  SELECT
                    {ht} AS home_team_id,
                    {at} AS away_team_id,
                    {hg} AS home_goals,
                    {ag} AS away_goals
                  FROM matches
                  WHERE league_id = %s
                    AND season = %s
                    AND (
                      lower(coalesce(status_group,'')) = 'finished'
                      OR coalesce(status,'') IN ('FT','AET','PEN')
                      OR coalesce(status_short,'') IN ('FT','AET','PEN')
                    )
                    AND {ht} IS NOT NULL AND {at} IS NOT NULL
                    AND {hg} IS NOT NULL AND {ag} IS NOT NULL
                ),
                per_team AS (
                  SELECT
                    home_team_id AS team_id,
                    COUNT(*) AS played,
                    SUM(CASE WHEN home_goals > away_goals THEN 1 ELSE 0 END) AS win,
                    SUM(CASE WHEN home_goals = away_goals THEN 1 ELSE 0 END) AS draw,
                    SUM(CASE WHEN home_goals < away_goals THEN 1 ELSE 0 END) AS lose,
                    SUM(home_goals) AS goals_for,
                    SUM(away_goals) AS goals_against,
                    SUM(CASE WHEN home_goals > away_goals THEN 3 WHEN home_goals = away_goals THEN 1 ELSE 0 END) AS points
                  FROM finished
                  GROUP BY home_team_id

                  UNION ALL

                  SELECT
                    away_team_id AS team_id,
                    COUNT(*) AS played,
                    SUM(CASE WHEN away_goals > home_goals THEN 1 ELSE 0 END) AS win,
                    SUM(CASE WHEN away_goals = home_goals THEN 1 ELSE 0 END) AS draw,
                    SUM(CASE WHEN away_goals < home_goals THEN 1 ELSE 0 END) AS lose,
                    SUM(away_goals) AS goals_for,
                    SUM(home_goals) AS goals_against,
                    SUM(CASE WHEN away_goals > home_goals THEN 3 WHEN away_goals = home_goals THEN 1 ELSE 0 END) AS points
                  FROM finished
                  GROUP BY away_team_id
                ),
                agg AS (
                  SELECT
                    team_id,
                    SUM(played) AS played,
                    SUM(win) AS win,
                    SUM(draw) AS draw,
                    SUM(lose) AS lose,
                    SUM(goals_for) AS goals_for,
                    SUM(goals_against) AS goals_against,
                    (SUM(goals_for) - SUM(goals_against)) AS goals_diff,
                    SUM(points) AS points
                  FROM per_team
                  GROUP BY team_id
                ),
                ranked AS (
                  SELECT
                    ROW_NUMBER() OVER (
                      ORDER BY points DESC, goals_diff DESC, goals_for DESC, team_id ASC
                    ) AS rank,
                    *
                  FROM agg
                )
                SELECT
                  r.rank,
                  r.team_id,
                  COALESCE(t.name, '') AS team_name,
                  t.logo AS team_logo,
                  r.played,
                  r.win,
                  r.draw,
                  r.lose,
                  r.goals_for,
                  r.goals_against,
                  r.goals_diff,
                  r.points,
                  NULL::text AS description,
                  NULL::text AS group_name,
                  NULL::text AS form
                FROM ranked r
                LEFT JOIN teams t ON t.id = r.team_id
                ORDER BY r.rank ASC, team_name ASC
                """,
                (league_id, season_resolved),
            )
        except Exception as e:
            print(f"[build_standings_block] ERROR computed standings league_id={league_id}, season={season_resolved}: {e}")
            rows_raw = []

        if not rows_raw:
            return {
                "league_id": league_id,
                "season": season_resolved,
                "league_name": league_name,
                "rows": [],
                "context_options": {"conferences": [], "groups": []},
            }

    # ─────────────────────────────────────────────────────────────
    # 공통 후처리 (matchdetail과 동일):
    # - (team_id) 단독 디듀프 제거
    # - (team_id + group_name) 기준으로만 "진짜 중복" 정리
    # - 정렬: group_name -> rank -> team_name
    # - group_name은 _effective_group_name()으로 보정(오스트리아 split 등)
    # ─────────────────────────────────────────────────────────────
    def _norm_group(v: Any) -> str:
        if not isinstance(v, str):
            return ""
        return re.sub(r"\s+", " ", v).strip()

    def _better_row(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
        """
        같은 (team_id, group_name) 키에 대해 어떤 row를 남길지 결정.
        - played 큰 것 우선
        - points, goals_diff, goals_for 큰 것 우선
        - rank는 작은 것 우선 (있으면)
        """
        a_played = _coalesce_int(a.get("played"), 0)
        b_played = _coalesce_int(b.get("played"), 0)
        if b_played != a_played:
            return b if b_played > a_played else a

        a_pts = _coalesce_int(a.get("points"), 0)
        b_pts = _coalesce_int(b.get("points"), 0)
        if b_pts != a_pts:
            return b if b_pts > a_pts else a

        a_gd = _coalesce_int(a.get("goals_diff"), 0)
        b_gd = _coalesce_int(b.get("goals_diff"), 0)
        if b_gd != a_gd:
            return b if b_gd > a_gd else a

        a_gf = _coalesce_int(a.get("goals_for"), 0)
        b_gf = _coalesce_int(b.get("goals_for"), 0)
        if b_gf != a_gf:
            return b if b_gf > a_gf else a

        a_rank = _coalesce_int(a.get("rank"), 0) or 999999
        b_rank = _coalesce_int(b.get("rank"), 0) or 999999
        if b_rank != a_rank:
            return b if b_rank < a_rank else a

        return a

    rows_by_key: Dict[Tuple[int, str], Dict[str, Any]] = {}
    for r in rows_raw:
        tid = _coalesce_int(r.get("team_id"), 0)
        if tid == 0:
            continue
        gkey = _norm_group(
            _effective_group_name(
                raw_group_name=r.get("group_name"),
                description=r.get("description"),
            )
        )
        key = (tid, gkey)

        prev = rows_by_key.get(key)
        if prev is None:
            rows_by_key[key] = r
        else:
            rows_by_key[key] = _better_row(prev, r)

    dedup_rows: List[Dict[str, Any]] = list(rows_by_key.values())

    def _sort_key(r: Dict[str, Any]):
        eff_g = _effective_group_name(
            raw_group_name=r.get("group_name"),
            description=r.get("description"),
        )
        g = _norm_group(eff_g)
        rk = _coalesce_int(r.get("rank"), 0) or 999999
        tn = str(r.get("team_name") or "")
        return (g.lower(), rk, tn.lower())

    dedup_rows.sort(key=_sort_key)

    out_rows: List[Dict[str, Any]] = []
    for r in dedup_rows:
        out_rows.append(
            {
                "position": _coalesce_int(r.get("rank"), 0),
                "team_id": _coalesce_int(r.get("team_id"), 0),
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
                "group_name": _effective_group_name(
                    raw_group_name=r.get("group_name"),
                    description=r.get("description"),
                ),
                "form": r.get("form"),
            }
        )

    # context_options는 "보정된 group_name" 기준으로 만들어야
    # description에만 split 정보가 있는 리그(예: Austria)도 chips가 안정적이다.
    context_rows: List[Dict[str, Any]] = []
    for r in dedup_rows:
        rr = dict(r)
        rr["group_name"] = _effective_group_name(
            raw_group_name=r.get("group_name"),
            description=r.get("description"),
        )
        context_rows.append(rr)

    context_options = _build_context_options_from_rows(context_rows)


    out = {
        "league_id": league_id,
        "season": season_resolved,
        "league_name": league_name,
        "rows": out_rows,
        "context_options": context_options,
        "mode": "TABLE",
        "bracket": None,
    }

    return out



