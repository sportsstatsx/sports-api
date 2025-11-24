# services/matchdetail/lineups_block.py

from typing import Any, Dict, List, Optional
import json

from db import fetch_one, fetch_all  # 프로젝트 공통 DB 유틸 (다른 블록들과 동일 패턴 가정)


def _coerce_json(val: Any) -> Dict[str, Any]:
    """Postgres JSONB → dict 안전 변환."""
    if val is None:
        return {}
    if isinstance(val, (dict, list)):
        return val  # 이미 파싱되어 dict / list 인 경우
    try:
        return json.loads(val)
    except Exception:
        # 잘못된 JSON 이거나 단순 문자열이면 그냥 빈 dict
        return {}


def _load_lineup_for_team(fixture_id: int, team_id: int) -> Optional[Dict[str, Any]]:
    row = fetch_one(
        """
        SELECT data_json
          FROM match_lineups
         WHERE fixture_id = %s
           AND team_id = %s
        """,
        (fixture_id, team_id),
    )
    if not row:
        return None
    data = row.get("data_json") if isinstance(row, dict) else row[0]
    return _coerce_json(data)


def _load_events_for_team(fixture_id: int, team_id: int) -> List[Dict[str, Any]]:
    rows = fetch_all(
        """
        SELECT fixture_id, team_id, player_id, type, detail,
               minute, extra, assist_player_id, assist_name,
               player_in_id, player_in_name
          FROM match_events
         WHERE fixture_id = %s
           AND team_id = %s
      ORDER BY minute, extra, id
        """,
        (fixture_id, team_id),
    )
    # fetch_all 이 list[dict] 형태를 주도록 구현돼 있을 거라 가정
    return list(rows or [])


def _build_player_stats(events: List[Dict[str, Any]]) -> Dict[int, Dict[str, Any]]:
    """플레이어별 득점/카드/교체 통계를 집계한다."""
    stats: Dict[int, Dict[str, Any]] = {}

    def ensure(pid: Optional[int]) -> Optional[Dict[str, Any]]:
        if pid is None:
            return None
        if pid not in stats:
            stats[pid] = {
                "goals": 0,
                "own_goals": 0,
                "yellow_cards": 0,
                "red_cards": 0,
                "came_on": False,
                "came_on_minute": None,
                "came_on_extra": None,
                "subbed_off": False,
                "subbed_off_minute": None,
                "subbed_off_extra": None,
            }
        return stats[pid]

    for ev in events:
        ev_type = (ev.get("type") or "").lower()
        detail = (ev.get("detail") or "").lower()
        minute = ev.get("minute")
        extra = ev.get("extra")
        player_id = ev.get("player_id")

        # 골
        if ev_type == "goal":
            st = ensure(player_id)
            if not st:
                continue
            if "own" in detail:  # own goal
                st["own_goals"] += 1
            else:
                st["goals"] += 1

        # 카드
        elif ev_type == "card":
            st = ensure(player_id)
            if not st:
                continue
            if "yellow" in detail:
                st["yellow_cards"] += 1
            if "red" in detail:
                st["red_cards"] += 1

        # 교체 (type 이 'subst' 또는 'substitution' 형태일 수 있음)
        elif ev_type in {"subst", "substitution"}:
            # OUT: player_id
            if player_id is not None:
                st_out = ensure(player_id)
                if st_out:
                    st_out["subbed_off"] = True
                    st_out["subbed_off_minute"] = minute
                    st_out["subbed_off_extra"] = extra

            # IN: player_in_id
            in_id = ev.get("player_in_id")
            if in_id is not None:
                st_in = ensure(in_id)
                if st_in:
                    st_in["came_on"] = True
                    st_in["came_on_minute"] = minute
                    st_in["came_on_extra"] = extra

    return stats


def _build_side_payload(
    lineup_json: Optional[Dict[str, Any]],
    events: List[Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    if not lineup_json:
        return None

    team = lineup_json.get("team") or {}
    formation = lineup_json.get("formation")
    coach = lineup_json.get("coach") or {}
    start_xi = lineup_json.get("startXI") or []
    subs = lineup_json.get("substitutes") or []

    stats_by_player = _build_player_stats(events)

    def map_players(lst):
        out: List[Dict[str, Any]] = []
        for item in lst:
            p = (item or {}).get("player") or {}
            pid = p.get("id")
            s = stats_by_player.get(pid) or {}
            out.append(
                {
                    "player_id": pid,
                    "name": p.get("name"),
                    "number": p.get("number"),
                    "pos": p.get("pos"),
                    "grid": p.get("grid"),
                    # 집계값
                    "goals": s.get("goals", 0),
                    "own_goals": s.get("own_goals", 0),
                    "yellow_cards": s.get("yellow_cards", 0),
                    "red_cards": s.get("red_cards", 0),
                    "came_on": s.get("came_on", False),
                    "came_on_minute": s.get("came_on_minute"),
                    "came_on_extra": s.get("came_on_extra"),
                    "subbed_off": s.get("subbed_off", False),
                    "subbed_off_minute": s.get("subbed_off_minute"),
                    "subbed_off_extra": s.get("subbed_off_extra"),
                }
            )
        return out

    starters_payload = map_players(start_xi)
    bench_payload = map_players(subs)

    return {
        "team_id": team.get("id"),
        "team_name": team.get("name"),
        "team_logo": team.get("logo"),
        "formation": formation,
        "coach": {
            "id": coach.get("id"),
            "name": coach.get("name"),
        },
        "starters": starters_payload,
        "bench": bench_payload,
    }


def build_lineups_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Match Detail 번들용 라인업 블록.

    반환 형태 (match_detail_bundle.lineups):

    "lineups": {
      "home": {
        "team_id": 40,
        "team_name": "Liverpool",
        "team_logo": "...",
        "formation": "4-3-3",
        "coach": {"id": 2006, "name": "Arne Slot"},
        "starters": [ { ... player payload ... } ],
        "bench": [ { ... } ]
      },
      "away": { ... 동일 구조 ... }
    }
    """

    fixture_id = header.get("fixture_id")
    home = (header.get("home") or {})
    away = (header.get("away") or {})

    home_id = home.get("id")
    away_id = away.get("id")

    if not fixture_id or not home_id or not away_id:
        return None

    # DB에서 라인업 + 이벤트 로드
    home_lineup = _load_lineup_for_team(fixture_id, home_id)
    away_lineup = _load_lineup_for_team(fixture_id, away_id)

    # 라인업이 둘 다 없으면 None
    if not home_lineup and not away_lineup:
        return None

    home_events = _load_events_for_team(fixture_id, home_id)
    away_events = _load_events_for_team(fixture_id, away_id)

    home_payload = _build_side_payload(home_lineup, home_events) if home_lineup else None
    away_payload = _build_side_payload(away_lineup, away_events) if away_lineup else None

    return {
        "home": home_payload,
        "away": away_payload,
    }
