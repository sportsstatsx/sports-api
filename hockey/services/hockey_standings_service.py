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


def hockey_get_standings(
    league_id: int,
    season: int,
    stage: Optional[str] = None,
    group_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    하키 스탠딩 (정식 고정)
    - hockey_standings 정규화 컬럼을 신뢰 (raw_json 파싱 ❌)
    - 그룹 단위: (stage, group_name)로 분리해서 groups 배열로 반환
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

    # key: (stage, group_name)
    grouped: Dict[Tuple[str, str], List[Dict[str, Any]]] = {}

    for r in rows:
        st = r.get("stage") or "Overall"
        gn = r.get("group_name") or "Overall"
        key = (st, gn)

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

        grouped.setdefault(key, []).append(
            {
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
        )

    groups_out: List[Dict[str, Any]] = []
    for (st, gn), items in grouped.items():
        items_sorted = sorted(items, key=lambda x: (x["rank"] is None, x["rank"] or 10**9))
        groups_out.append(
            {
                "stage": st,
                "group_name": gn,
                "rows": items_sorted,
            }
        )
    # ─────────────────────────────────────────
    # 정식 정렬 규칙: stage 우선순위 고정
    #   1) Regular Season
    #   2) Playoffs/Postseason/Finals
    #   3) Pre-season/Exhibition
    #   4) 기타 stage는 맨 뒤(알파벳)
    # group_name은 동일 stage 내 알파벳 정렬(안정)
    # ─────────────────────────────────────────

    def _stage_rank(s: str) -> tuple[int, str]:
        s0 = (s or "").strip().lower()

        # 가장 중요한 정규시즌이 항상 위로
        if "regular" in s0:
            return (1, s0)

        # 플레이오프/포스트시즌
        if "playoff" in s0 or "post" in s0 or "final" in s0:
            return (2, s0)

        # 프리시즌/전시경기
        if "pre" in s0 or "exhibition" in s0:
            return (3, s0)

        # 그 외는 맨 뒤
        return (9, s0)

    groups_out.sort(key=lambda g: (_stage_rank(g["stage"])[0], _stage_rank(g["stage"])[1], g["group_name"]))


    return {
        "ok": True,
        "league": {
            "id": league["id"],
            "name": league["name"],
            "logo": league["logo"],
            "country": league.get("country"),
        },
        "season": season,
        # 요청 필터를 meta로 남겨두면 디버깅/운영에 좋음(포맷 고정)
        "groups": groups_out,
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
