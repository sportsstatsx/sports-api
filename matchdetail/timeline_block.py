# matchdetail/timeline_block.py

from typing import Any, Dict, List, Optional
from db import fetch_all
import json
import unicodedata
import re


# ─────────────────────────────────────────────
#  Type / Period 매핑 (Kotlin TimelineRepository 그대로 포팅)
# ─────────────────────────────────────────────

def _map_type(type_raw: Optional[str], detail_raw: Optional[str]) -> str:
    """
    Kotlin:
      private fun mapType(typeRaw: String?, detailRaw: String?): TimelineType
    을 그대로 Python으로 옮긴 것.
    반환값은 TimelineType enum 이름 문자열 (e.g. "GOAL", "PEN_GOAL")
    """
    t = (type_raw or "").lower().strip()
    d = (detail_raw or "").lower().strip()

    # 1차 분기
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
    if "cancel" in d and "goal" in t:
        return "CANCELLED_GOAL"

    # 2차 디테일 기반
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


def _map_period_by_minute(minute: int) -> str:
    """
    Kotlin:
      private fun mapPeriodByMinute(minute: Int): Period
    """
    if minute <= 45:
        return "H1"
    if minute <= 90:
        return "H2"
    # ET 나중에 필요시 PEN 분리는 여기서 조정 가능
    return "ET"


def _build_minute_label_and_extra(minute: int, extra: Optional[int], period: str) -> (str, Optional[int]):
    """
    Kotlin:
      private fun buildMinuteLabelAndExtra(min: Int, extra: Int?, period: Period): Pair<String, Int?>
    과 동일한 동작.
    """
    ex: Optional[int] = extra if (extra is not None and extra > 0) else None
    if ex is None:
        if period == "H1":
            v = minute - 45
            ex = v if v > 0 else None
        elif period == "H2":
            v = minute - 90
            ex = v if v > 0 else None

    if period == "H1":
        base = min(minute, 45)
    elif period == "H2":
        base = min(minute, 90)
    else:
        base = minute

    # Kotlin: "${base}’+$ex" / "${max(0, min)}’"
    prime = "’"  # U+2019
    if ex is not None and ex > 0:
        label = f"{base}{prime}+{ex}"
    else:
        label = f"{max(0, minute)}{prime}"

    return label, ex


def _normalize_name_light(s: str) -> str:
    """
    Kotlin:
      private fun normalizeNameLight(s: String): String =
          Normalizer.normalize(...).replace(...)

    - 소문자
    - NFD 정규화 후 accent 제거
    - 마침표 제거
    - 공백 정리
    """
    s = unicodedata.normalize("NFD", s.lower().strip())
    s = "".join(ch for ch in s if unicodedata.category(ch) != "Mn")
    s = s.replace(".", "")
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ─────────────────────────────────────────────
#  Player name map (match_player_stats + match_lineups)
# ─────────────────────────────────────────────

def _build_player_name_map_from_stats(fixture_id: int) -> Dict[int, str]:
    """
    match_player_stats:
      - fixture_id
      - player_id
      - data_json (stats json)
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
        data_json = r.get("data_json")
        if not pid or not data_json:
            continue
        try:
            root = json.loads(data_json)
        except Exception:
            continue

        name = None
        if isinstance(root, dict):
            player_obj = root.get("player")
            if isinstance(player_obj, dict):
                nm = player_obj.get("name")
                if isinstance(nm, str) and nm.strip():
                    name = nm.strip()
            if not name:
                nm = root.get("name")
                if isinstance(nm, str) and nm.strip():
                    name = nm.strip()

        if name:
            out.setdefault(int(pid), name)
    return out


def _build_player_name_map_from_lineups(fixture_id: int) -> Dict[int, str]:
    """
    match_lineups:
      - fixture_id
      - team_id
      - data_json (lineups json)
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

    def absorb_from_array(arr):
        if not isinstance(arr, list):
            return
        for item in arr:
            if not isinstance(item, dict):
                continue
            p = item.get("player") or item
            pid = p.get("id")
            name = p.get("name")
            if isinstance(pid, int) and isinstance(name, str) and name.strip():
                out.setdefault(pid, name.strip())

    for r in rows:
        data_json = r.get("data_json")
        if not data_json:
            continue
        try:
            root = json.loads(data_json)
        except Exception:
            continue
        if not isinstance(root, dict):
            continue

        # API-Football typical keys: "startXI" / "substitutes", 케이스 변형 포함
        for key in ("startXI", "startXi", "substitutes", "subs"):
            arr = root.get(key)
            if isinstance(arr, list):
                absorb_from_array(arr)

    return out


def _build_player_name_map(fixture_id: int) -> Dict[int, str]:
    stats = _build_player_name_map_from_stats(fixture_id)
    lu = _build_player_name_map_from_lineups(fixture_id)
    for pid, name in lu.items():
        stats.setdefault(pid, name)
    return stats


# ─────────────────────────────────────────────
#  Main: build_timeline_block
# ─────────────────────────────────────────────

def build_timeline_block(header: Dict[str, Any]) -> List[Dict[str, Any]]:
    """
    match_events + stats + lineups 를 기반으로
    안드로이드 TimelineRepository.kt 와 동일한 의미의 타임라인 이벤트 리스트를 만든다. 

    반환값 예시:
    [
      {
        "id_stable": "...",
        "minute": 45,
        "minute_label": "45’+2",
        "side": "home",
        "side_home": True,
        "type": "GOAL",
        "line1": "Son Heung-Min (P)",
        "line2": "Assist James Maddison",
        "snapshot_score": null,
        "period": "H1",
        "minute_extra": 2
      },
      ...
    ]
    """

    fixture_id = header["fixture_id"]
    home_id = header["home"]["id"]
    away_id = header["away"]["id"]

    # 1) 선수 이름 맵 (stats + lineups)
    player_name_map = _build_player_name_map(fixture_id)

    # 2) 이벤트 질의: match_events (새 컬럼 포함)
    rows = fetch_all(
        """
        SELECT
            e.id AS rid,                -- 고유 ID
            e.minute,
            e.extra,
            e.team_id,
            e.player_id,
            e.type,
            e.detail,
            e.assist_player_id,
            e.assist_name,
            e.player_in_id,
            e.player_in_name
        FROM match_events AS e
        WHERE e.fixture_id = %s
        ORDER BY e.minute ASC, e.id ASC
        """,
        (fixture_id,),
    )

    def name_for(pid: Optional[int]) -> Optional[str]:
        if pid is None:
            return None
        return player_name_map.get(int(pid))

    def prefer_name(pid: Optional[int], fallback: Optional[str]) -> Optional[str]:
        return name_for(pid) or (fallback.strip() if isinstance(fallback, str) and fallback.strip() else None)

    out: List[Dict[str, Any]] = []

    for r in rows:
        rid = r.get("id") or r.get("rid") or 0
        minute = int(r.get("minute") or 0)
        extra = r.get("extra")
        extra = int(extra) if extra is not None else None
        team_id = r.get("team_id")
        player_id = r.get("player_id")
        type_raw = r.get("type")
        detail_raw = r.get("detail")
        assist_id = r.get("assist_player_id")
        assist_name = r.get("assist_name")
        in_id = r.get("player_in_id")
        in_name = r.get("player_in_name")

        type_code = _map_type(type_raw, detail_raw)
        is_home = bool(team_id == home_id) if team_id is not None else False
        side = "home" if is_home else ("away" if team_id == away_id else "unknown")

        period = _map_period_by_minute(minute)
        minute_label, minute_extra = _build_minute_label_and_extra(minute, extra, period)

        # Kotlin 동일 로직으로 line1/line2 구성
        line1: str
        line2: Optional[str] = None

        if type_code == "SUB":
            # 새 스키마: inId/inName + playerId(out)
            in_nm = prefer_name(in_id, in_name)
            out_nm = name_for(player_id)
            line1 = f"In {in_nm}" if in_nm else "Substitution"
            line2 = f"Out {out_nm}" if out_nm else None

            # In / Out 이름이 우연히 같은 경우 line2 숨김
            if line2:
                a = _normalize_name_light(line1.replace("In", "", 1).strip())
                b = _normalize_name_light(line2.replace("Out", "", 1).strip())
                if a.lower() == b.lower():
                    line2 = None

        elif type_code in ("GOAL", "PEN_GOAL", "OWN_GOAL"):
            scorer = name_for(player_id)
            if type_code == "OWN_GOAL":
                # "Scorer (OG)"
                parts = [p for p in [scorer, "(OG)"] if p]
                line1 = " ".join(parts) if parts else (detail_raw or "Goal")
            elif type_code == "PEN_GOAL":
                parts = [p for p in [scorer, "(P)"] if p]
                line1 = " ".join(parts) if parts else (detail_raw or "Goal")
            else:
                line1 = scorer or (detail_raw or "Goal")

            assist_nm = prefer_name(assist_id, assist_name)
            if assist_nm:
                line2 = f"Assist {assist_nm}"

        elif type_code == "PEN_MISSED":
            who = name_for(player_id)
            parts = [p for p in [who, "(P Missed)"] if p]
            line1 = " ".join(parts) if parts else (detail_raw or "Penalty Missed")

        elif type_code == "YELLOW":
            who = name_for(player_id)
            line1 = who or "Card"

        elif type_code == "RED":
            who = name_for(player_id)
            line1 = who or "Card"

        elif type_code == "CANCELLED_GOAL":
            who = name_for(player_id)
            parts = [p for p in [who, "Goal cancelled"] if p]
            line1 = " ".join(parts)

        else:
            # OTHER / VAR 등
            line1 = name_for(player_id) or (detail_raw or "Event")

        event = {
            "id_stable": f"{rid}-{minute}-{type_code}-{is_home}",
            "minute": minute,
            "minute_label": minute_label,
            "side": side,
            "side_home": is_home,
            "type": type_code,
            "line1": line1,
            "line2": line2,
            "snapshot_score": None,
            "period": period,
            "minute_extra": minute_extra,
        }
        out.append(event)

    # Kotlin: period.ordinal, minute, minuteExtra, idStable 정렬과 같게
    period_order = {"H1": 0, "H2": 1, "ET": 2, "PEN": 3}

    out.sort(
        key=lambda ev: (
            period_order.get(ev["period"], 9),
            ev["minute"],
            ev["minute_extra"] or 0,
            ev["id_stable"],
        )
    )

    return out
