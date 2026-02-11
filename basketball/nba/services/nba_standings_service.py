# basketball/nba/services/nba_standings_service.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from basketball.nba.nba_db import nba_fetch_all


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _canon(s: Any) -> str:
    return str(s or "").strip().lower()


def _stage_rank(stage: str) -> int:
    s = _canon(stage)
    if s == "conference":
        return 0
    if s == "division":
        return 1
    return 9


def _conf_rank(name: str) -> int:
    n = _canon(name)
    # 보통 East -> West 순으로 보여주는게 자연스러움
    if n == "east":
        return 0
    if n == "west":
        return 1
    return 9


def _div_rank(name: str) -> int:
    n = _canon(name)
    # NBA 디비전 표준 순서(동부 3 + 서부 3)
    order = {
        "atlantic": 0,
        "central": 1,
        "southeast": 2,
        "northwest": 3,
        "pacific": 4,
        "southwest": 5,
    }
    return order.get(n, 99)


def _display_group(name: str) -> str:
    s = str(name or "").strip()
    if not s:
        return ""
    return s[:1].upper() + s[1:]


def nba_get_standings(
    league: str,
    season: int,
    stage: Optional[str] = None,
    group_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    NBA Standings

    입력:
      - league: text (기본 standard)
      - season: int (예: 2025)
      - stage/group_name: 옵션 필터 (conference/division + east/west/pacific...)

    출력(하키 스탠딩과 동일한 형태):
      {
        ok, available, league, season,
        stages: [
          { stage: "Conference", groups: [ {group_name:"East", rows:[...]} ... ] },
          { stage: "Division",   groups: [ ... ] }
        ],
        meta: { source, filters, generated_at }
      }
    """
    league = (league or "standard").strip()
    if not league:
        league = "standard"

    try:
        season = int(season)
    except Exception:
        raise ValueError("season must be int")

    stage_f = _canon(stage)
    group_f = _canon(group_name)

    # nba_standings 스키마 기반 정확 JOIN
    # - nba_standings: league(text), season(int), team_id, conference_name/rank, division_name/rank, win/loss/streak, raw_json
    # - nba_teams: id, name, nickname, code, city, logo, raw_json
    rows = nba_fetch_all(
        """
        SELECT
            s.league,
            s.season,
            s.team_id,
            s.conference_name,
            s.conference_rank,
            s.division_name,
            s.division_rank,
            s.win,
            s.loss,
            s.streak,
            s.raw_json,

            t.name      AS team_name,
            t.nickname  AS team_nickname,
            t.code      AS team_code,
            t.city      AS team_city,
            t.logo      AS team_logo
        FROM nba_standings s
        JOIN nba_teams t ON t.id = s.team_id
        WHERE s.league = %s
          AND s.season = %s
        """,
        (league, season),
    )

    if not rows:
        return {
            "ok": True,
            "available": False,
            "league": {"id": league, "name": ("NBA" if league == "standard" else league)},
            "season": season,
            "stages": [],
            "meta": {
                "source": "db",
                "filters": {"stage": stage, "group_name": group_name},
                "generated_at": datetime.now(timezone.utc)
                .replace(microsecond=0)
                .isoformat()
                .replace("+00:00", "Z"),
            },
        }

    stages_map: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

    for r in rows:
        conf = _canon(r.get("conference_name"))
        div = _canon(r.get("division_name"))

        # stats
        w = _safe_int(r.get("win")) or 0
        l = _safe_int(r.get("loss")) or 0
        gp = w + l

        raw = r.get("raw_json") if isinstance(r.get("raw_json"), dict) else {}

        win_obj = raw.get("win") if isinstance(raw, dict) else {}
        loss_obj = raw.get("loss") if isinstance(raw, dict) else {}
        conf_obj = raw.get("conference") if isinstance(raw, dict) else {}
        div_obj = raw.get("division") if isinstance(raw, dict) else {}

        win_pct = None
        try:
            # API-Sports는 percentage가 "0.615" 같은 문자열로 내려오는 케이스가 많음
            win_pct = win_obj.get("percentage")
        except Exception:
            win_pct = None

        games_behind = None
        try:
            games_behind = raw.get("gamesBehind")
        except Exception:
            games_behind = None

        last_ten = None
        try:
            # raw_json.win.lastTen / raw_json.loss.lastTen 존재
            lt_w = win_obj.get("lastTen")
            lt_l = loss_obj.get("lastTen")
            if lt_w is not None and lt_l is not None:
                last_ten = {"win": lt_w, "loss": lt_l}
        except Exception:
            last_ten = None

        home_away = {}
        try:
            home_away = {
                "home": {"win": win_obj.get("home"), "loss": loss_obj.get("home")},
                "away": {"win": win_obj.get("away"), "loss": loss_obj.get("away")},
            }
        except Exception:
            home_away = {}

        team_id = _safe_int(r.get("team_id"))
        row_obj = {
            # 하키와 맞춰서 rank 필드 유지
            "rank": _safe_int(r.get("conference_rank")) if conf else None,
            "team": {
                "id": team_id,
                "name": r.get("team_name"),
                "nickname": r.get("team_nickname"),
                "code": r.get("team_code"),
                "city": r.get("team_city"),
                "logo": r.get("team_logo"),
            },
            "stats": {
                "played": gp,
                "wins": w,
                "losses": l,
                "win_pct": win_pct,
                "streak": _safe_int(r.get("streak")),
                "games_behind": games_behind,
                "last_ten": last_ten,
                "home_away": home_away,
                # 참고용(원본에 있으면)
                "conference": {
                    "name": conf_obj.get("name") if isinstance(conf_obj, dict) else conf,
                    "rank": conf_obj.get("rank") if isinstance(conf_obj, dict) else r.get("conference_rank"),
                    "win": conf_obj.get("win") if isinstance(conf_obj, dict) else None,
                    "loss": conf_obj.get("loss") if isinstance(conf_obj, dict) else None,
                },
                "division": {
                    "name": div_obj.get("name") if isinstance(div_obj, dict) else div,
                    "rank": div_obj.get("rank") if isinstance(div_obj, dict) else r.get("division_rank"),
                    "win": div_obj.get("win") if isinstance(div_obj, dict) else None,
                    "loss": div_obj.get("loss") if isinstance(div_obj, dict) else None,
                },
            },
        }

        # stage = Conference
        st_conf = "Conference"
        gn_conf = _display_group(conf) if conf else ""
        if gn_conf:
            if (not stage_f or stage_f == "conference") and (not group_f or group_f == _canon(gn_conf)):
                stages_map.setdefault(st_conf, {}).setdefault(gn_conf, []).append(
                    dict(row_obj, rank=_safe_int(r.get("conference_rank")))
                )

        # stage = Division
        st_div = "Division"
        gn_div = _display_group(div) if div else ""
        if gn_div:
            if (not stage_f or stage_f == "division") and (not group_f or group_f == _canon(gn_div)):
                stages_map.setdefault(st_div, {}).setdefault(gn_div, []).append(
                    dict(row_obj, rank=_safe_int(r.get("division_rank")))
                )

    # 정렬 & 출력
    stages_out: List[Dict[str, Any]] = []

    for st, groups in stages_map.items():
        groups_out: List[Dict[str, Any]] = []
        for gn, items in groups.items():
            items_sorted = sorted(items, key=lambda x: (x["rank"] is None, x["rank"] or 10**9))
            groups_out.append({"group_name": gn, "rows": items_sorted})

        if _canon(st) == "conference":
            groups_out.sort(key=lambda g: _conf_rank(g["group_name"]))
        elif _canon(st) == "division":
            groups_out.sort(key=lambda g: _div_rank(g["group_name"]))
        else:
            groups_out.sort(key=lambda g: g["group_name"])

        stages_out.append({"stage": st, "groups": groups_out})

    stages_out.sort(key=lambda s: _stage_rank(s["stage"]))

    return {
        "ok": True,
        "available": True,
        "league": {"id": league, "name": ("NBA" if league == "standard" else league)},
        "season": season,
        "stages": stages_out,
        "meta": {
            "source": "db",
            "filters": {"stage": stage, "group_name": group_name},
            "generated_at": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
        },
    }
