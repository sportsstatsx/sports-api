# matchdetail/timeline_block.py

from typing import Any, Dict, List
import json

from db import fetch_all


# ─────────────────────────────────────
#  내부 유틸: 이름 매핑
# ─────────────────────────────────────

def _build_player_name_map_from_stats(fixture_id: int) -> Dict[int, str]:
    """
    match_player_stats: (fixture_id, player_id, data_json)
    data_json 안의 player.name / name 을 읽어서 id -> name 맵 생성
    """
    rows = fetch_all(
        """
        SELECT player_id, data_json
        FROM match_player_stats
        WHERE fixture_id = %s
        """,
        (fixture_id,),
    )

    out: Dict[int, str] = {}
    for r in rows:
        pid = r.get("player_id")
        if pid is None:
            continue
        raw = r.get("data_json")
        if not raw:
            continue
        try:
            root = json.loads(raw)
        except Exception:
            continue

        name = None
        player = root.get("player")
        if isinstance(player, dict):
            name = player.get("name") or player.get("fullname")
        if not name:
            name = root.get("name")
        if isinstance(name, str) and name.strip():
            out.setdefault(int(pid), name.strip())

    return out


def _build_player_name_map_from_lineups(fixture_id: int) -> Dict[int, str]:
    """
    match_lineups: (fixture_id, data_json)
    data_json 안의 startXI / substitutes 배열에서 id + name 추출
    """
    rows = fetch_all(
        """
        SELECT data_json
        FROM match_lineups
        WHERE fixture_id = %s
        """,
        (fixture_id,),
    )

    out: Dict[int, str] = {}

    def absorb_from_array(arr: Any):
        if not isinstance(arr, list):
            return
        for item in arr:
            if not isinstance(item, dict):
                continue
            p = item.get("player") or item
            if not isinstance(p, dict):
                continue
            pid = p.get("id")
            name = p.get("name")
            if isinstance(pid, int) and isinstance(name, str) and name.strip():
                out.setdefault(int(pid), name.strip())

    for r in rows:
        raw = r.get("data_json")
        if not raw:
            continue
        try:
            root = json.loads(raw)
        except Exception:
            continue

        absorb_from_array(root.get("startXI"))
        absorb_from_array(root.get("startXi"))      # 대소문자 fallback
        absorb_from_array(root.get("substitutes"))
        absorb_from_array(root.get("subs"))         # 예비 키

    return out


def _build_player_name_map(fixture_id: int) -> Dict[int, str]:
    """
    stats + lineups 를 합쳐서 최종 player_id -> name 맵 생성
    """
    stats = _build_player_name_map_from_stats(fixture_id)
    lu = _build_player_name_map_from_lineups(fixture_id)
    for pid, name in lu.items():
        stats.setdefault(pid, name)
    return stats


def _normalize_name_light(s: str) -> str:
    return " ".join(s.lower().replace(".", " ").split())


# ─────────────────────────────────────
#  내부 유틸: 타입/기간/분 표시
# ─────────────────────────────────────

def _map_type(type_raw: str | None, detail_raw: str | None) -> str:
    """
    Kotlin TimelineType 과 거의 동일한 canonical type
    """
    t = (type_raw or "").lower().strip()
    d = (detail_raw or "").lower().strip()

    # ✅ 1) 패널티 실축을 최우선으로 판별
    if (("pen" in t or "pen" in d or "penalty" in d)
            and ("miss" in d or "saved" in d)):
        return "PEN_MISSED"

    # ✅ 2) 자책골 / PK 골 / 일반 골
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
    if ("pen" in d or "penalty" in d) and ("miss" in d or "saved" in d):
        return "PEN_MISSED"
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


def _map_period(minute: int) -> str:
    if minute <= 45:
        return "H1"
    if minute <= 90:
        return "H2"
    if minute <= 120:
        return "ET"
    return "PEN"


def _build_minute_label(minute: int, extra_raw: Any, period: str) -> tuple[str, int | None]:
    """
    Kotlin buildMinuteLabelAndExtra 와 동일한 규칙으로 minute_label / extra 계산
    """
    extra = None
    if isinstance(extra_raw, int):
        extra = extra_raw

    if extra is None:
        if period == "H1":
            extra = max(0, minute - 45) or None
        elif period == "H2":
            extra = max(0, minute - 90) or None

    base = minute
    if period == "H1" and minute > 45:
        base = 45
    elif period == "H2" and minute > 90:
        base = 90

    if extra is not None and extra > 0:
        label = f"{base}\u2019+{extra}"
    else:
        label = f"{max(0, minute)}\u2019"

    return label, extra


# ─────────────────────────────────────
#  메인: 타임라인 블록
# ─────────────────────────────────────

def build_timeline_block(header: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    match_events + (match_player_stats / match_lineups) 를 사용해
    앱에서 그대로 그릴 수 있는 타임라인 이벤트 배열을 만든다.
    """

    fixture_id = int(header["fixture_id"])
    home_id = header["home"]["id"]
    away_id = header["away"]["id"]

    # 이름 맵
    player_name_map = _build_player_name_map(fixture_id)

    def name_for(pid: Any | None) -> str | None:
        if pid is None:
            return None
        try:
            return player_name_map.get(int(pid))
        except Exception:
            return None

    def prefer_name(pid: Any | None, fallback: Any | None) -> str | None:
        n = name_for(pid)
        if n:
            return n
        if isinstance(fallback, str) and fallback.strip():
            return fallback.strip()
        return None

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

        # 예전 앱과 동일하게 VAR 이벤트는 숨김
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
            r.get("extra") if "extra" in r else r.get("time_extra"),
            period,
        )

        player_id = r.get("player_id")
        assist_id = r.get("assist_player_id")
        assist_name_raw = r.get("assist_name")
        in_id = r.get("player_in_id")
        in_name_raw = r.get("player_in_name")

        # 스코어 스냅샷 (득점 이벤트만)
        snapshot_score: str | None = None
        if t_canon in ("GOAL", "PEN_GOAL", "OWN_GOAL"):
            # API 기준: team_id = 득점 팀
            # → side 가 home/away 어느 쪽이든 그대로 +1 만 해주면 된다
            if side == "home":
                home_score += 1
            elif side == "away":
                away_score += 1

            snapshot_score = f"{home_score} - {away_score}"


        # line1 / line2
        line1: str
        line2: str | None = None

        if t_canon == "SUB":
            in_nm = prefer_name(in_id, in_name_raw)
            out_nm = name_for(player_id)
            line1 = f"In {in_nm}" if in_nm else "Substitution"
            line2 = f"Out {out_nm}" if out_nm else None

            # In / Out 이름이 같으면 line2 숨김
            if line2:
                a = _normalize_name_light(line1.replace("In", "", 1).strip())
                b = _normalize_name_light(line2.replace("Out", "", 1).strip())
                if a == b:
                    line2 = None

        elif t_canon in ("GOAL", "PEN_GOAL", "OWN_GOAL"):
            scorer = name_for(player_id)
            if t_canon == "OWN_GOAL":
                line1 = " ".join([x for x in [scorer, "(OG)"] if x])
            elif t_canon == "PEN_GOAL":
                line1 = " ".join([x for x in [scorer, "(P)"] if x])
            else:
                line1 = scorer or (detail or "Goal")

            assist_nm = prefer_name(assist_id, assist_name_raw)
            if assist_nm:
                line2 = f"Assist {assist_nm}"

        elif t_canon == "PEN_MISSED":
            who = name_for(player_id)
            line1 = " ".join([x for x in [who, "(P Missed)"] if x])

        elif t_canon == "YELLOW":
            who = name_for(player_id)
            line1 = who or "Card"

        elif t_canon == "RED":
            who = name_for(player_id)
            line1 = who or "Card"

        else:
            # CANCELLED_GOAL 등 기타
            who = name_for(player_id)
            if t_canon == "CANCELLED_GOAL":
                line1 = " ".join([x for x in [who, "Goal cancelled"] if x])
            else:
                line1 = who or (detail or "Event")

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

    # Kotlin 과 동일한 정렬 규칙
    order_map = {"H1": 0, "H2": 1, "ET": 2, "PEN": 3}

    events.sort(
        key=lambda e: (
            order_map.get(e["period"], 9),
            e["minute"],
            e.get("minute_extra") or 0,
            e["id_stable"],
        )
    )

    return events
