# services/matchdetail/stats_block.py
from typing import Any, Dict, Optional, List, Tuple

from db import fetch_all


def _extract_ids_from_header(header: Dict[str, Any]) -> Tuple[Optional[int], Optional[int], Optional[int]]:
    """
    HeaderBlock(JSON) 구조를 기준으로 fixture_id / home_id / away_id 추출.
    """
    fixture_id: Optional[int] = None
    home_id: Optional[int] = None
    away_id: Optional[int] = None

    raw_fix = header.get("fixture_id")
    if raw_fix is not None:
        try:
            fixture_id = int(raw_fix)
        except (TypeError, ValueError):
            fixture_id = None

    home = header.get("home") or {}
    away = header.get("away") or {}

    if isinstance(home, dict):
        try:
            home_id = int(home.get("id")) if home.get("id") is not None else None
        except (TypeError, ValueError):
            home_id = None

    if isinstance(away, dict):
        try:
            away_id = int(away.get("id")) if away.get("id") is not None else None
        except (TypeError, ValueError):
            away_id = None

    return fixture_id, home_id, away_id


def _parse_numeric_value(raw: Any) -> Optional[float]:
    """
    "75%", "20", "1.77" 같은 문자열을 숫자로 파싱.
    실패하면 None.
    """
    if raw is None:
        return None
    if isinstance(raw, (int, float)):
        return float(raw)

    s = str(raw).strip()
    if not s:
        return None

    # 퍼센트 기호 제거
    if s.endswith("%"):
        s = s[:-1].strip()

    try:
        return float(s)
    except ValueError:
        return None


def _canonical_key(stat_name: str) -> Optional[str]:
    """
    API-Football style name → 내부 key 로 매핑.
    예시:
      "Total Shots"      → "shots_total"
      "Shots on Goal"    → "shots_on_goal"
      "Ball Possession"  → "possession_pct"
      "Passes %"         → "passes_pct"
      "Passes accurate"  → "passes_accurate"
      "Total passes"     → "passes_total"
      "Corner Kicks"     → "corners"
      "Yellow Cards"     → "yellow_cards"
      "Red Cards"        → "red_cards"
      "Fouls"            → "fouls"
    """
    n = stat_name.strip().lower()

    # 슈팅 관련
    if "total shots" in n:
        return "shots_total"
    if "shots on goal" in n:
        return "shots_on_goal"
    if "shots off goal" in n:
        return "shots_off_goal"
    if "shots insidebox" in n:
        return "shots_inside_box"
    if "shots outsidebox" in n:
        return "shots_outside_box"

    # 점유율
    if "ball possession" in n:
        return "possession_pct"

    # 패스
    if n.startswith("passes %") or "passes %" in n:
        return "passes_pct"
    if "passes accurate" in n:
        return "passes_accurate"
    if "total passes" in n:
        return "passes_total"

    # 세트피스 / 파울
    if "corner kicks" in n:
        return "corners"
    if n == "fouls":
        return "fouls"
    if "offsides" in n:
        return "offsides"

    # 카드
    if "yellow cards" in n:
        return "yellow_cards"
    if "red cards" in n:
        return "red_cards"

    # GK / xG 계열
    if "goalkeeper saves" in n:
        return "goalkeeper_saves"
    if "expected_goals" in n or "expected goals" in n:
        return "xg"
    if "goals_prevented" in n or "goals prevented" in n:
        return "goals_prevented"

    # 매핑 안 하는 값들은 None → extras 로만 보관
    return None


def _build_side_stats(rows: List[Dict[str, Any]], team_id: Optional[int]) -> Dict[str, Any]:
    """
    한 팀(team_id)에 해당하는 row 들을 모아서
    stats 딕셔너리로 정규화.

    rows 원소 예:
      { fixture_id, team_id, name, value }
    """
    side_rows: List[Dict[str, Any]] = []
    for r in rows:
        if team_id is None:
            continue
        if r.get("team_id") == team_id:
            side_rows.append(r)

    stats: Dict[str, Any] = {}
    extras: Dict[str, Any] = {}

    for r in side_rows:
        name = r.get("name")
        value_raw = r.get("value")

        if not name:
            continue

        key = _canonical_key(str(name))
        num_val = _parse_numeric_value(value_raw)

        if key:
            # 정규화된 key 에 숫자 값 저장
            stats[key] = num_val
        else:
            # 알 수 없는 stat 은 extras 로 보관 (디버깅용)
            extras[str(name)] = value_raw

    return {
        "team_id": team_id,
        "stats": stats,
        "extras": extras,
    }


def build_stats_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    match_team_stats 테이블을 기반으로 StatsBlock(team=...) 생성.

    ✅ 동기화 규칙(중요):
    - 기본은 DB(match_team_stats)
    - 하지만 matchdetail 번들에서 header가 'timeline 기반으로 이미 동기화'되어 내려온다면
      (home/away.ft/ht/red_cards 등),
      stats_block에서도 아래 항목은 header 값을 우선으로 덮어쓴다:

        - red_cards
        - (선택) yellow_cards  ※ header에 값이 있을 때만
        - goals(=ft), goals_ht(=ht)  ※ stats에 goals 계열 키가 있으면 같이 맞춤

    이걸 해야 admin에서 timeline 이벤트 삭제/추가 시
    stats / matchlist / scoreblock이 한 번에 일관되게 맞는다.
    """
    fixture_id, home_team_id, away_team_id = _extract_ids_from_header(header)

    if fixture_id is None:
        return None

    sql = """
        SELECT *
        FROM match_team_stats
        WHERE fixture_id = %s
    """
    rows: List[Dict[str, Any]] = fetch_all(sql, (fixture_id,))

    if not rows:
        return None

    home_block = _build_side_stats(rows, home_team_id) if home_team_id is not None else None
    away_block = _build_side_stats(rows, away_team_id) if away_team_id is not None else None

    # ─────────────────────────────────────────
    # ✅ header 기반 동기화(핵심)
    # - bundle_service에서 header를 최종 timeline 기준으로 맞춘 상태가 "정답"
    # - stats에서 이벤트성 지표는 header 값을 우선 반영
    # ─────────────────────────────────────────
    home_h = header.get("home") if isinstance(header.get("home"), dict) else {}
    away_h = header.get("away") if isinstance(header.get("away"), dict) else {}

    def _safe_int(v: Any) -> Optional[int]:
        if v is None:
            return None
        if isinstance(v, bool):
            return int(v)
        if isinstance(v, int):
            return v
        if isinstance(v, float):
            return int(v)
        s = str(v).strip()
        if not s:
            return None
        # "2" / "02" 같은 경우
        if s.isdigit():
            return int(s)
        return None

    # header에서 동기화 대상 값 추출
    home_red = _safe_int(home_h.get("red_cards"))
    away_red = _safe_int(away_h.get("red_cards"))

    home_yellow = _safe_int(home_h.get("yellow_cards"))
    away_yellow = _safe_int(away_h.get("yellow_cards"))

    home_ft = _safe_int(home_h.get("ft") if home_h.get("ft") is not None else home_h.get("score"))
    away_ft = _safe_int(away_h.get("ft") if away_h.get("ft") is not None else away_h.get("score"))

    home_ht = _safe_int(home_h.get("ht"))
    away_ht = _safe_int(away_h.get("ht"))

    def _apply_sync(side_block: Optional[Dict[str, Any]], *, red: Optional[int], yellow: Optional[int], ft: Optional[int], ht: Optional[int]) -> None:
        """
        side_block 구조:
          {
            "team_id": ...,
            "stats": {...},
            "extras": {...}
          }
        """
        if not isinstance(side_block, dict):
            return

        stats = side_block.get("stats")
        if not isinstance(stats, dict):
            return

        # ✅ Red Cards는 항상 동기화(값이 있으면)
        if red is not None:
            stats["red_cards"] = float(red)

        # ✅ Yellow Cards는 header에 값이 있을 때만 동기화
        if yellow is not None:
            stats["yellow_cards"] = float(yellow)

        # ✅ 득점 동기화:
        # - match_team_stats에 goals 계열을 저장해두는 환경이 있으면(혹은 extras로 들어온 경우)
        #   여기도 header 기준으로 맞춰준다.
        # - 내부 표준 키가 없으니, "goals" / "goals_ht" 후보 키들을 함께 세팅
        if ft is not None:
            stats.setdefault("goals", float(ft))
            # 일부 UI가 shots/possession 외에 goals 키를 본다면
            stats["goals"] = float(ft)

        if ht is not None:
            stats["goals_ht"] = float(ht)

    _apply_sync(home_block, red=home_red, yellow=home_yellow, ft=home_ft, ht=home_ht)
    _apply_sync(away_block, red=away_red, yellow=away_yellow, ft=away_ft, ht=away_ht)

    team_block: Dict[str, Any] = {
        "home_team_id": home_team_id,
        "away_team_id": away_team_id,
        "home": home_block,
        "away": away_block,
        "raw_rows": rows,
    }

    return {
        "team": team_block,
        "players": None,
    }

