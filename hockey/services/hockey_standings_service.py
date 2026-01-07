# hockey/services/hockey_standings_service.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


# ─────────────────────────────────────────
# 정식 정렬 규칙(고정)
# 1) stage 우선순위: Regular -> Playoffs/Post -> Pre -> 기타(알파벳)
# 2) group 우선순위(동일 stage): Division(기본) -> Conference -> Overall -> 기타
#    (NHL 기본 정렬 포함: Atlantic/Metropolitan/Central/Pacific, Eastern/Western)
# ─────────────────────────────────────────
def _stage_rank(stage: str) -> tuple[int, str]:
    s0 = (stage or "").strip().lower()

    if "regular" in s0:
        return (1, s0)
    if "playoff" in s0 or "post" in s0 or "final" in s0:
        return (2, s0)
    if "pre" in s0 or "exhibition" in s0:
        return (3, s0)
    return (9, s0)


def _group_rank(group_name: str) -> tuple[int, str]:
    g0 = (group_name or "").strip().lower()

    # 기본: 디비전 먼저
    if "division" in g0:
        # NHL 디비전 정식 순서 고정
        order = {
            "atlantic division": 1,
            "metropolitan division": 2,
            "central division": 3,
            "pacific division": 4,
        }
        return (1, str(order.get(g0, 99)).zfill(2) + "_" + g0)

    if "conference" in g0:
        order = {
            "eastern conference": 1,
            "western conference": 2,
        }
        return (2, str(order.get(g0, 99)).zfill(2) + "_" + g0)

    # 전체/통합 느낌(리그마다 다르니 최소 규칙만)
    if g0 in ("overall", "all", "total") or "overall" in g0:
        return (3, g0)

    return (9, g0)


def hockey_get_standings(
    league_id: int,
    season: int,
    stage: Optional[str] = None,
    group_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    하키 스탠딩 (정식 고정: stage -> groups -> rows)
    - hockey_standings 정규화 컬럼을 신뢰 (raw_json 파싱 ❌)
    - 반환 구조: stages=[{stage, groups:[{group_name, rows:[...]}]}]
    - 필터 지원: stage, group_name
    """

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

    where = ["s.league_id = %s", "s.season = %s"]
    params: List[Any] = [league_id, season]

    if stage:
        where.append("s.stage = %s")
        params.append(stage)
    if group_name:
        where.append("s.group_name = %s")
        params.append(group_name)

    where_sql = " AND ".join(where)

    rows = hockey_fetch_all(
        f"""
        SELECT
            s.league_id,
            s.season,
            s.stage,
            s.group_name,
            s.team_id,
            s.position,
            s.games_played,
            s.win_total,
            s.win_ot_total,
            s.lose_total,
            s.lose_ot_total,
            s.goals_for,
            s.goals_against,
            s.points,
            s.form,
            s.description,
            t.name AS team_name,
            t.logo AS team_logo
        FROM hockey_standings s
        JOIN hockey_teams t ON t.id = s.team_id
        WHERE {where_sql}
        ORDER BY s.stage ASC, s.group_name ASC, s.position ASC
        """,
        tuple(params),
    )

    # stages_map[stage][group_name] = [rows...]
    stages_map: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

    for r in rows:
        st = (r.get("stage") or "Overall").strip()
        gn = (r.get("group_name") or "Overall").strip()

        gp = _safe_int(r.get("games_played"))
        w = _safe_int(r.get("win_total"))
        l = _safe_int(r.get("lose_total"))
        ot_w = _safe_int(r.get("win_ot_total"))
        ot_l = _safe_int(r.get("lose_ot_total"))
        gf = _safe_int(r.get("goals_for"))
        ga = _safe_int(r.get("goals_against"))
        pts = _safe_int(r.get("points"))

        diff = None
        if gf is not None and ga is not None:
            diff = gf - ga

        row_obj = {
            "rank": _safe_int(r.get("position")),
            "team": {
                "id": _safe_int(r.get("team_id")),
                "name": r.get("team_name"),
                "logo": r.get("team_logo"),
            },
            "stats": {
                "played": gp,
                "wins": w,
                "losses": l,
                "ot_wins": ot_w,
                "ot_losses": ot_l,
                "points": pts,
                "gf": gf,
                "ga": ga,
                "diff": diff,
                "form": r.get("form"),
                "description": r.get("description"),
            },
        }

        stages_map.setdefault(st, {}).setdefault(gn, []).append(row_obj)

    # 정렬 & 출력
    stages_out: List[Dict[str, Any]] = []

    for st, groups in stages_map.items():
        groups_out: List[Dict[str, Any]] = []
        for gn, items in groups.items():
            items_sorted = sorted(items, key=lambda x: (x["rank"] is None, x["rank"] or 10**9))
            groups_out.append(
                {
                    "group_name": gn,
                    "rows": items_sorted,
                }
            )

        # group 정식 정렬(기본=Division 우선)
        groups_out.sort(key=lambda g: _group_rank(g["group_name"]))

        stages_out.append(
            {
                "stage": st,
                "groups": groups_out,
            }
        )

    # stage 정식 정렬
    stages_out.sort(key=lambda s: _stage_rank(s["stage"]))

    return {
        "ok": True,
        "league": {
            "id": league["id"],
            "name": league["name"],
            "logo": league["logo"],
            "country": league.get("country"),
        },
        "season": season,
        "stages": stages_out,
        "meta": {
            "source": "db",
            "filters": {
                "stage": stage,
                "group_name": group_name,
            },
            "generated_at": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
        },
    }
