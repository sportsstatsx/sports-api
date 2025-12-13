# hockey/services/hockey_standings_service.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one


def _norm_str(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


# ─────────────────────────────────────────
# (1) STAGE 정렬 규칙 (정식)
# ─────────────────────────────────────────
def _stage_sort_key(stage: str) -> Tuple[int, str]:
    """
    기본 원칙(정식):
      Regular Season 먼저
      Playoffs 다음
      Pre-season 마지막
      그 외는 뒤로
    """
    s = (stage or "").lower()
    if "regular" in s:
        pri = 0
    elif "play" in s:  # playoffs / play offs
        pri = 1
    elif "pre" in s:
        pri = 2
    else:
        pri = 50
    return (pri, stage or "")


# ─────────────────────────────────────────
# (2) GROUP 정렬 규칙 (정식)
# ─────────────────────────────────────────
def _group_sort_key(group_name: str) -> Tuple[int, int, str]:
    """
    기본 원칙(정식):
      - Division이 있으면 Division을 먼저(Atlantic, Metropolitan, Central, Pacific)
      - Conference는 그 다음(Eastern, Western)
      - 그 외는 마지막(이름 오름차순)
    """
    g = (group_name or "")
    gl = g.lower()

    # Division 우선
    if "division" in gl:
        div_order = {
            "atlantic": 0,
            "metropolitan": 1,
            "central": 2,
            "pacific": 3,
        }
        sub = 99
        for k, v in div_order.items():
            if k in gl:
                sub = v
                break
        return (0, sub, g)

    # Conference 다음
    if "conference" in gl:
        conf_order = {"eastern": 0, "western": 1}
        sub = 99
        for k, v in conf_order.items():
            if k in gl:
                sub = v
                break
        return (1, sub, g)

    # 기타
    return (2, 99, g)


def hockey_get_standings(
    league_id: int,
    season: int,
    stage: Optional[str] = None,
    group_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    하키 스탠딩 (정식)

    - 반환 구조(고정):
      {
        ok: true,
        league: {id,name,logo,country},
        season: 2025,
        stages: [
          {
            stage: "NHL - Regular Season",
            groups: [
              { group_name: "Atlantic Division", rows: [...] },
              ...
            ]
          },
          ...
        ],
        meta: {filters:{stage,group_name}, source:"db", generated_at:"..."}
      }

    - stage/group_name은 optional filter (없으면 전체)
    - 정렬은:
        (1) stage 정렬 규칙
        (2) stage 내부 group 정렬 규칙
        (3) rows는 position ASC (rank)
    """

    stage = _norm_str(stage)
    group_name = _norm_str(group_name)

    # -------------------------
    # 0) League 메타
    # -------------------------
    league_sql = """
        SELECT
            l.id,
            l.name,
            l.logo,
            c.name AS country
        FROM hockey_leagues l
        LEFT JOIN hockey_countries c ON c.id = l.country_id
        WHERE l.id = %s
        LIMIT 1
    """
    lrow = hockey_fetch_one(league_sql, (league_id,))
    if not lrow:
        raise ValueError("LEAGUE_NOT_FOUND")

    league_obj = {
        "id": lrow["id"],
        "name": lrow["name"],
        "logo": lrow.get("logo"),
        "country": lrow.get("country"),
    }

    # -------------------------
    # 1) Standings Rows (DB 정규화 컬럼 사용)
    # -------------------------
    params: List[Any] = [league_id, season]
    where = ["s.league_id = %s", "s.season = %s"]

    if stage:
        where.append("s.stage = %s")
        params.append(stage)

    if group_name:
        where.append("s.group_name = %s")
        params.append(group_name)

    where_sql = " AND ".join(where)

    sql = f"""
        SELECT
            s.league_id,
            s.season,
            s.stage,
            s.group_name,
            s.team_id,
            s.position,

            s.games_played,
            s.win_total,
            s.lose_total,
            s.win_ot_total,
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
    """

    rows = hockey_fetch_all(sql, tuple(params))

    # -------------------------
    # 2) stage → group → rows 로 묶기
    # -------------------------
    stages_map: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

    for r in rows:
        st = r["stage"]
        gn = r["group_name"]

        gf = _safe_int(r.get("goals_for"))
        ga = _safe_int(r.get("goals_against"))
        diff = (gf - ga) if (gf is not None and ga is not None) else None

        row_obj = {
            "rank": _safe_int(r.get("position")),
            "team": {
                "id": r.get("team_id"),
                "name": r.get("team_name"),
                "logo": r.get("team_logo"),
            },
            "stats": {
                "played": _safe_int(r.get("games_played")),
                "wins": _safe_int(r.get("win_total")),
                "losses": _safe_int(r.get("lose_total")),
                "ot_wins": _safe_int(r.get("win_ot_total")),
                "ot_losses": _safe_int(r.get("lose_ot_total")),
                "gf": gf,
                "ga": ga,
                "diff": diff,
                "points": _safe_int(r.get("points")),
                "form": r.get("form"),
                "description": r.get("description"),
            },
        }

        stages_map.setdefault(st, {}).setdefault(gn, []).append(row_obj)

    # -------------------------
    # 3) 정식 정렬 적용해서 리스트로 변환
    # -------------------------
    stage_names = sorted(stages_map.keys(), key=_stage_sort_key)

    stages_out: List[Dict[str, Any]] = []
    for st in stage_names:
        group_map = stages_map[st]
        group_names = sorted(group_map.keys(), key=_group_sort_key)

        groups_out: List[Dict[str, Any]] = []
        for gn in group_names:
            # rows는 이미 position ASC, 혹시 rank NULL 방어만(정식)
            rows_list = group_map[gn]
            rows_list.sort(key=lambda x: (x.get("rank") is None, x.get("rank") or 10**9))

            groups_out.append(
                {
                    "group_name": gn,
                    "rows": rows_list,
                }
            )

        stages_out.append(
            {
                "stage": st,
                "groups": groups_out,
            }
        )

    return {
        "ok": True,
        "league": league_obj,
        "season": season,
        "stages": stages_out,
        "meta": {
            "filters": {
                "stage": stage,
                "group_name": group_name,
            },
            "source": "db",
            "generated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        },
    }
