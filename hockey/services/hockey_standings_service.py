# hockey/services/hockey_standings_service.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set

from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one


def _colset(table: str) -> Set[str]:
    rows = hockey_fetch_all(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema='public' AND table_name=%s
        """,
        (table,),
    )
    return {r["column_name"] for r in rows}


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _pick(row: Dict[str, Any], *names: str) -> Any:
    """row에서 가능한 컬럼명을 순서대로 찾아 반환"""
    for n in names:
        if n in row:
            return row.get(n)
    return None


def hockey_get_standings(league_id: int, season: int) -> Dict[str, Any]:
    """
    하키 스탠딩(정식)
    - hockey_standings + hockey_teams + hockey_leagues + hockey_countries
    - 스키마 컬럼명이 달라도(예: rank vs position) 깨지지 않도록 컬럼 감지 후 매핑
    - group(컨퍼런스/디비전/그룹)이 있으면 group별로 묶어서 반환
    """

    cols = _colset("hockey_standings")

    # league 메타
    league = hockey_fetch_one(
        """
        SELECT
            l.id,
            l.name,
            l.logo,
            c.name AS country
        FROM hockey_leagues l
        LEFT JOIN hockey_countries c ON c.id = l.country_id
        WHERE l.id = %s
        LIMIT 1
        """,
        (league_id,),
    )

    if not league:
        raise ValueError("LEAGUE_NOT_FOUND")

    # standings 테이블에서 최소로 필요한 조건 컬럼(league_id/season)은 있다고 가정
    # (없으면 스키마가 아예 다른 거라 그때는 \d hockey_standings 출력 필요)
    select_cols = ["s.*", "t.name AS team_name", "t.logo AS team_logo"]

    sql = f"""
        SELECT {", ".join(select_cols)}
        FROM hockey_standings s
        LEFT JOIN hockey_teams t ON t.id = s.team_id
        WHERE s.league_id = %s AND s.season = %s
    """

    rows = hockey_fetch_all(sql, (league_id, season))

    # group 컬럼 후보들 (있으면 이걸로 묶고, 없으면 Overall)
    group_key = None
    for candidate in ("group_name", "group", "conference", "division", "stage"):
        if candidate in cols:
            group_key = candidate
            break

    grouped: Dict[str, List[Dict[str, Any]]] = {}

    for r in rows:
        gname = "Overall"
        if group_key:
            v = r.get(group_key)
            if v:
                gname = str(v)

        rank = _safe_int(_pick(r, "rank", "position", "standing", "place"))
        played = _safe_int(_pick(r, "played", "games_played", "gp"))
        wins = _safe_int(_pick(r, "wins", "w"))
        losses = _safe_int(_pick(r, "losses", "l"))
        ot = _safe_int(_pick(r, "ot", "ot_losses", "otl"))
        points = _safe_int(_pick(r, "points", "pts"))
        gf = _safe_int(_pick(r, "goals_for", "gf", "for"))
        ga = _safe_int(_pick(r, "goals_against", "ga", "against"))

        # diff가 없으면 gf-ga로 계산(둘 다 있을 때만)
        diff = _safe_int(_pick(r, "diff", "goal_diff", "gd"))
        if diff is None and gf is not None and ga is not None:
            diff = gf - ga

        team_id = _safe_int(_pick(r, "team_id"))
        item = {
            "rank": rank,
            "team": {
                "id": team_id,
                "name": r.get("team_name"),
                "logo": r.get("team_logo"),
            },
            "stats": {
                "played": played,
                "wins": wins,
                "losses": losses,
                "ot": ot,
                "points": points,
                "gf": gf,
                "ga": ga,
                "diff": diff,
            },
        }

        # 원본을 보존하고 싶으면 raw_json이 있을 때만 meta에 넣어도 됨(지금은 불필요해서 제외)
        grouped.setdefault(gname, []).append(item)

    # 정렬 규칙(정식): rank 있으면 rank ASC, 없으면 points DESC -> wins DESC
    def sort_key(x: Dict[str, Any]):
        rnk = x.get("rank")
        pts = x.get("stats", {}).get("points")
        win = x.get("stats", {}).get("wins")
        # rank가 있으면 그걸 최우선
        if rnk is not None:
            return (0, rnk)
        # rank 없으면 points 큰 순(내림차순) -> wins 큰 순
        return (1, -(pts or 0), -(win or 0))

    groups_out = []
    for name, items in grouped.items():
        items_sorted = sorted(items, key=sort_key)
        groups_out.append({"name": name, "rows": items_sorted})

    # group 이름 정렬(Overall 먼저)
    groups_out.sort(key=lambda g: (0 if g["name"] == "Overall" else 1, g["name"]))

    return {
        "ok": True,
        "league": {
            "id": league["id"],
            "name": league["name"],
            "logo": league["logo"],
            "country": league.get("country"),
        },
        "season": season,
        "groups": groups_out,
        "meta": {
            "source": "db",
            "generated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z"),
        },
    }
