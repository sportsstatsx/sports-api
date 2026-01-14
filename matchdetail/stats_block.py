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

    최종 구조:

      {
        "team": {
          "home_team_id": ...,
          "away_team_id": ...,
          "home": {
            "team_id": ...,
            "stats": { "shots_total": 20, "shots_on_goal": 4, ... },
            "extras": { ... name 원본 ... }
          },
          "away": { ... 동일 ... },
          "raw_rows": [ ... 원본 row ... ]
        },
        "players": null
      }

    Kotlin:
      data class StatsBlock(
          val team: Any? = null,
          val players: Any? = null
      )
    와 그대로 호환됨.
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
