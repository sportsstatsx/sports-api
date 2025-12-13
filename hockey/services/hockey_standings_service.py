# hockey/services/hockey_standings_service.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def hockey_get_standings(league_id: int, season: int) -> Dict[str, Any]:
    """
    하키 스탠딩 (정식 고정)
    - hockey_standings 스키마 기준(너가 올린 컬럼명 그대로 사용)
    - group: stage + group_name 단위로 묶어서 내려줌
    - rank는 position 사용
    - stats는 향후 변경 최소화: form/description도 고정 포함
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

    rows = hockey_fetch_all(
        """
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
        WHERE s.league_id = %s
          AND s.season = %s
        ORDER BY s.stage ASC, s.group_name ASC, s.position ASC
        """,
        (league_id, season),
    )

    grouped: Dict[str, List[Dict[str, Any]]] = {}

    for r in rows:
        stage = r.get("stage") or "Overall"
        group_name = r.get("group_name") or "Overall"
        group_key = f"{stage} / {group_name}"

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

        grouped.setdefault(group_key, []).append(
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
                    # 하키 특성상 OT를 “승/패” 둘 다 분리해서 고정
                    "ot_wins": ot_w,
                    "ot_losses": ot_l,
                    "points": pts,
                    "gf": gf,
                    "ga": ga,
                    "diff": diff,
                    # 향후 UI에서 바로 쓰게 정식 포함 (없으면 null)
                    "form": r.get("form"),
                    "description": r.get("description"),
                },
            }
        )

    # 그룹 출력(정렬)
    groups_out = []
    for key, items in grouped.items():
        # 이미 ORDER BY position이라 그대로여도 되지만, 방어적으로 rank ASC
        items_sorted = sorted(items, key=lambda x: (x["rank"] is None, x["rank"] or 10**9))
        groups_out.append({"name": key, "rows": items_sorted})

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
