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

    # ✅ 0) VAR + "취소골" (DB 패턴 기반)
    # - type=Var + detail:
    #   * Goal cancelled
    #   * Goal Disallowed
    #   * Goal Disallowed - offside/handball/foul ...
    if (t == "var" or "var" in t or "var" in d) and ("goal" in d) and (
        "cancel" in d or "disallow" in d
    ):
        return "CANCELLED_GOAL"

    # ✅ 0-b) VAR + "Red card cancelled" 는 타임라인에 노출시키고 싶음
    # - 예: type=Var, detail="Red card cancelled"
    if (t == "var" or "var" in t or "var" in d) and ("red" in d) and ("cancel" in d):
        return "RED_CARD_CANCELLED"

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

    # ✅ VAR(취소골/레드취소 외 VAR)은 기존처럼 VAR로 유지 (아래에서 숨김 처리됨)
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
    if "var" in d:
        return "VAR"

    return "OTHER"





def _map_period(minute: int, has_et: bool) -> str:
    """
    - 리그(연장 없음): 90분 넘어도 전부 H2(후반 추가시간)
    - 컵/토너먼트(연장 있음): 91~104는 H2(후반 추가시간), 105~120은 ET, 이후는 PEN
    """
    if minute <= 45:
        return "H1"
    if minute <= 90:
        return "H2"

    if not has_et:
        return "H2"

    # has_et == True
    if minute < 105:
        return "H2"   # ✅ 90+X 구간
    if minute <= 120:
        return "ET"
    return "PEN"




def _build_minute_label(minute: int, extra_raw: Any, period: str) -> tuple[str, int | None]:
    """
    minute_label / extra 계산
    - 핵심: DB에 extra=0으로 저장된 케이스를 "없음(None)"으로 취급해서 추론 로직을 태운다.
    """
    extra: int | None = None
    if isinstance(extra_raw, int):
        extra = extra_raw

    # ✅ 0(또는 음수)은 "없음" 처리 -> 추론 로직으로 넘긴다
    if extra is not None and extra <= 0:
        extra = None

    # ✅ raw extra 방어 (비정상 값 제거)
    RAW_EXTRA_CAP = 30
    if extra is not None and extra > RAW_EXTRA_CAP:
        extra = None

    # ✅ extra가 없으면 H1/H2에서만 추론
    if extra is None:
        if period == "H1":
            inferred = max(0, minute - 45)
            extra = inferred if 0 < inferred <= 15 else None
        elif period == "H2":
            inferred = max(0, minute - 90)
            extra = inferred if 0 < inferred <= 20 else None

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

    status_u = str(header.get("status") or "").upper()
    elapsed = int(header.get("elapsed") or 0)
    has_et = (status_u in ("AET", "PEN", "ET")) or (elapsed >= 105)

    for idx, r in enumerate(rows):
        db_id = int(r.get("id") or 0)  # ✅ 정렬 tie-break용(숫자)
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

        period = _map_period(minute, has_et)
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

        elif t_canon == "RED_CARD_CANCELLED":
            who = name_for(player_id)
            line1 = " ".join([x for x in [who, "Red card cancelled"] if x]) if who else "Red card cancelled"

        else:
            who = name_for(player_id)
            if t_canon == "CANCELLED_GOAL":
                line1 = " ".join([x for x in [who, "Goal cancelled"] if x])
            else:
                line1 = who or (detail or "Event")

        # ✅ id_stable은 UI key용으로만 사용 (정렬에는 절대 사용하지 않음)
        #   db_id가 있으면 db_id 기반으로 만들어두면 디버깅도 쉬움
        id_stable = f"{fixture_id}-{db_id}" if db_id > 0 else f"{fixture_id}-{idx}"

        events.append(
            {
                "id_stable": id_stable,
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
                # ✅ 정렬/검증용 내부 값
                "_db_id": db_id,
                "_idx": idx,
            }
        )

    # Kotlin 과 동일한 정렬 규칙
    order_map = {"H1": 0, "H2": 1, "ET": 2, "PEN": 3}

    # ✅ 같은 시각(분/추가시간)에서 타입 우선순위 명시
    # - 카드 흐름 고정: YELLOW -> RED (그리고 취소는 맨 뒤)
    type_rank = {
        "PEN_MISSED": -1,
        "YELLOW": 10,
        "RED": 11,
        "RED_CARD_CANCELLED": 12,
    }

    def _sort_time_key(e: Dict[str, Any]) -> tuple[int, int]:
        m = int(e.get("minute") or 0)
        x = int(e.get("minute_extra") or 0)
        p = e.get("period")

        if p == "H1" and x > 0:
            return 45, x
        if p == "H2" and x > 0:
            return 90, x
        return m, x

    # ✅ 절대 문자열로 tie-break 하지 말 것
    # - 마지막은 DB id(정수) -> 없으면 idx(정수)
    events.sort(
        key=lambda e: (
            order_map.get(e.get("period"), 9),
            *_sort_time_key(e),
            type_rank.get(e.get("type"), 100),
            int(e.get("_db_id") or 0),
            int(e.get("_idx") or 0),
        )
    )

    # 내부 키 제거(클라에 보내지 않게)
    for e in events:
        e.pop("_db_id", None)
        e.pop("_idx", None)

    return events



