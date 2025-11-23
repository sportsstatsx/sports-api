# matchdetail/timeline_block.py

from typing import Any, Dict, List
from db import fetch_all


def _first_non_empty(row: Dict[str, Any], keys: List[str]) -> str | None:
    """
    row 안에서 여러 후보 컬럼 이름 중 먼저 나오는 non-empty 값을 반환.
    DB 스키마가 조금 달라도 안전하게 가져오려고 이렇게 처리.
    """
    for k in keys:
        if k in row:
            v = row.get(k)
            if v is not None and str(v).strip() != "":
                return str(v).strip()
    return None


def _map_period(minute: int) -> str:
    """
    분(minute) 기준으로 H1/H2/ET/PEN 구분.
    """
    if minute <= 45:
        return "H1"
    if minute <= 90:
        return "H2"
    if minute <= 120:
        return "ET"
    return "PEN"


def _build_minute_label(minute: int, time_extra: int | None, period: str) -> tuple[str, int | None]:
    """
    46분 -> 45’+1 형태 라벨 + extra 반환.
    """
    extra = time_extra
    if extra is None:
        if period == "H1":
            extra = max(0, minute - 45) or None
        elif period == "H2":
            extra = max(0, minute - 90) or None

    base_min = minute
    if period == "H1" and minute > 45:
        base_min = 45
    elif period == "H2" and minute > 90:
        base_min = 90

    if extra is not None and extra > 0:
        label = f"{base_min}\u2019+{extra}"
    else:
        label = f"{max(0, minute)}\u2019"

    return label, extra


def _map_type(type_raw: str | None, detail_raw: str | None) -> str:
    """
    DB type/detail 문자열을 UI에서 쓰기 좋은 canonical type 으로 매핑.
    - GOAL / PEN_GOAL / OWN_GOAL
    - YELLOW / RED
    - SUB
    - PEN_MISSED
    - VAR
    - OTHER
    """
    t = (type_raw or "").lower().strip()
    d = (detail_raw or "").lower().strip()

    if "goal" in t and "own" in d:
        return "OWN_GOAL"
    if "goal" in t and ("pen" in d or "penalty" in d):
        return "PEN_GOAL"
    if "goal" in t:
        return "GOAL"

    if "card" in t and "red" in d:
        return "RED"
    if "card" in t and "yellow" in d:
        return "YELLOW"

    if t.startswith("subst") or t.startswith("sub"):
        return "SUB"

    if ("pen" in t or "pen" in d) and ("miss" in d or "saved" in d):
        return "PEN_MISSED"

    if "var" in t or "var" in d:
        return "VAR"

    # detail 만 보고 보정
    if "own" in d and "goal" in d:
        return "OWN_GOAL"
    if "pen" in d and "goal" in d:
        return "PEN_GOAL"
    if "goal" in d:
        return "GOAL"
    if "red" in d:
        return "RED"
    if "yellow" in d:
        return "YELLOW"
    if "sub" in d:
        return "SUB"
    if "pen" in d and "miss" in d:
        return "PEN_MISSED"
    if "var" in d:
        return "VAR"

    return "OTHER"


def build_timeline_block(header: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    match_events 테이블을 기반으로, **앱에서 그대로 사용하는**
    타임라인 이벤트 리스트를 만든다.

    각 이벤트 구조 예시:

    {
      "id_stable": "12345-0",
      "minute": 33,
      "minute_label": "33’",
      "side": "home",
      "side_home": true,
      "type": "GOAL",
      "line1": "Salah (P)",
      "line2": "Assist Núñez",
      "snapshot_score": "1 - 0",
      "period": "H1",
      "minute_extra": null
    }
    """

    fixture_id = header["fixture_id"]
    home_id = header["home"]["id"]
    away_id = header["away"]["id"]

    rows = fetch_all(
        """
        SELECT *
        FROM match_events
        WHERE fixture_id = %s
        ORDER BY minute NULLS FIRST, id
        """,
        (fixture_id,),
    )

    events: List[Dict[str, Any]] = []

    home_score = 0
    away_score = 0

    for idx, r in enumerate(rows):
        minute = int(r.get("minute") or 0)
        detail = r.get("detail") or ""
        type_raw = r.get("type") or ""

        t_canon = _map_type(type_raw, detail)

        # 예전 앱에서 VAR은 안 보이게 했다고 했으니 여기서 제거
        if t_canon == "VAR":
            continue

        team_id = r.get("team_id")
        if team_id == home_id:
            side = "home"
        elif team_id == away_id:
            side = "away"
        else:
            side = "unknown"

        period = _map_period(minute)
        label, minute_extra = _build_minute_label(
            minute,
            r.get("time_extra") if isinstance(r.get("time_extra"), int) else None,
            period,
        )

        # 득점 이벤트면 스코어 누적
        snapshot_score: str | None = None
        if t_canon in ("GOAL", "PEN_GOAL", "OWN_GOAL"):
            if side == "home":
                if t_canon == "OWN_GOAL":
                    away_score += 1
                else:
                    home_score += 1
            elif side == "away":
                if t_canon == "OWN_GOAL":
                    home_score += 1
                else:
                    away_score += 1
            snapshot_score = f"{home_score} - {away_score}"

        # 이름들 (컬럼 이름 여러 형태 지원)
        player = _first_non_empty(
            r,
            ["player_name", "player", "scorer_name", "player1", "name"],
        )
        assist = _first_non_empty(
            r,
            ["assist_name", "assist", "assist1"],
        )
        player_in = _first_non_empty(
            r,
            ["player_in_name", "in_player_name", "sub_in_name"],
        )
        player_out = _first_non_empty(
            r,
            ["player_out_name", "out_player_name", "sub_out_name"],
        )

        # line1 / line2 구성
        if t_canon in ("GOAL", "PEN_GOAL", "OWN_GOAL"):
            base_name = player or "Unknown"
            suffix = ""
            if t_canon == "OWN_GOAL":
                suffix = " (OG)"
            elif t_canon == "PEN_GOAL":
                suffix = " (P)"

            line1 = f"{base_name}{suffix}"
            line2 = f"Assist {assist}" if assist else None

        elif t_canon in ("YELLOW", "RED"):
            name = player or "Unknown"
            line1 = name
            line2 = "Yellow card" if t_canon == "YELLOW" else "Red card"

        elif t_canon == "SUB":
            if player_in:
                line1 = f"In {player_in}"
            else:
                line1 = "Substitution"

            line2 = f"Out {player_out}" if player_out else None

        elif t_canon == "PEN_MISSED":
            name = player or "Unknown"
            line1 = name
            line2 = "Penalty missed"

        else:
            line1 = detail or type_raw or "Event"
            line2 = None

        events.append(
            {
                "id_stable": f"{fixture_id}-{idx}",
                "minute": minute,
                "minute_label": label,
                "side": side,
                "side_home": side == "home",
                "type": t_canon,
                "line1": line1,
                "line2": line2,
                "snapshot_score": snapshot_score,
                "period": period,
                "minute_extra": minute_extra,
            }
        )

    return events
