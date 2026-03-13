# services/matchdetail/standings_block.py
from __future__ import annotations

from typing import Any, Dict, Optional, List, Tuple
import re
import datetime as dt


from db import fetch_all


def _coalesce_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _fetch_one(query: str, params: tuple) -> Optional[Dict[str, Any]]:
    rows = fetch_all(query, params)
    return rows[0] if rows else None

def _safe_int_or_none(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except (TypeError, ValueError):
        return None


def _safe_text_or_none(v: Any) -> Optional[str]:
    if v is None:
        return None
    s = str(v).strip()
    return s if s else None


def _parse_json_text(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return v
    if not isinstance(v, str):
        return {}
    try:
        import json
        parsed = json.loads(v)
        return parsed if isinstance(parsed, dict) else {}
    except Exception:
        return {}


def _read_fixture_score_from_raw(fixture_id: Optional[int]) -> Dict[str, Optional[int]]:
    """
    match_fixtures_raw.data_json 에서 score/fulltime/extratime/penalty를 읽는다.
    반환:
      {
        "ft_home": ...,
        "ft_away": ...,
        "et_home": ...,
        "et_away": ...,
        "pen_home": ...,
        "pen_away": ...,
      }
    """
    if fixture_id is None:
        return {
            "ft_home": None,
            "ft_away": None,
            "et_home": None,
            "et_away": None,
            "pen_home": None,
            "pen_away": None,
        }

    row = _fetch_one(
        """
        SELECT data_json
        FROM match_fixtures_raw
        WHERE fixture_id = %s
        LIMIT 1
        """,
        (fixture_id,),
    )
    if not row:
        return {
            "ft_home": None,
            "ft_away": None,
            "et_home": None,
            "et_away": None,
            "pen_home": None,
            "pen_away": None,
        }

    root = _parse_json_text(row.get("data_json"))
    score = root.get("score") if isinstance(root.get("score"), dict) else {}

    ft = score.get("fulltime") if isinstance(score.get("fulltime"), dict) else {}
    et = score.get("extratime") if isinstance(score.get("extratime"), dict) else {}
    pen = score.get("penalty") if isinstance(score.get("penalty"), dict) else {}

    return {
        "ft_home": _safe_int_or_none(ft.get("home")),
        "ft_away": _safe_int_or_none(ft.get("away")),
        "et_home": _safe_int_or_none(et.get("home")),
        "et_away": _safe_int_or_none(et.get("away")),
        "pen_home": _safe_int_or_none(pen.get("home")),
        "pen_away": _safe_int_or_none(pen.get("away")),
    }


def _resolve_bracket_display_scores(match_row: Dict[str, Any]) -> Dict[str, Optional[int]]:
    """
    브라켓 표시에 쓸 스코어를 결정한다.

    규칙:
    - FT 종료  -> FT 사용
    - AET 종료 -> FT + ET 사용
    - PEN 종료 -> FT + ET 사용, penalty는 별도 전달
    - raw 점수가 부족하면 matches.home_ft / away_ft fallback
    """
    fixture_id = _safe_int_or_none(match_row.get("fixture_id"))
    status = str(match_row.get("status") or "").strip().upper()
    status_group = str(match_row.get("status_group") or "").strip().upper()

    raw_score = _read_fixture_score_from_raw(fixture_id)

    match_home_ft = _safe_int_or_none(match_row.get("home_ft"))
    match_away_ft = _safe_int_or_none(match_row.get("away_ft"))

    ft_home = raw_score["ft_home"]
    ft_away = raw_score["ft_away"]
    et_home = raw_score["et_home"]
    et_away = raw_score["et_away"]

    disp_home: Optional[int] = None
    disp_away: Optional[int] = None
    score_source = "matches_ft"

    if status in {"AET", "PEN"}:
        if ft_home is not None and ft_away is not None:
            disp_home = ft_home + (et_home or 0)
            disp_away = ft_away + (et_away or 0)
            score_source = "raw_fulltime_plus_extratime"

    elif status_group == "FINISHED" or status in {"FT", "NS", "PST", "CANC", "ABD"}:
        if ft_home is not None and ft_away is not None:
            disp_home = ft_home
            disp_away = ft_away
            score_source = "raw_fulltime"

    if disp_home is None or disp_away is None:
        disp_home = match_home_ft
        disp_away = match_away_ft
        score_source = "matches_ft"

    return {
        "home": disp_home,
        "away": disp_away,
        "ft_home": ft_home,
        "ft_away": ft_away,
        "et_home": et_home,
        "et_away": et_away,
        "pen_home": raw_score["pen_home"],
        "pen_away": raw_score["pen_away"],
        "score_source": score_source,
        "status": status,
    }


def _resolve_season_from_fixture_id(fixture_id: Optional[int]) -> Optional[int]:
    if fixture_id is None:
        return None

    row = _fetch_one(
        """
        SELECT season
        FROM matches
        WHERE fixture_id = %s
        LIMIT 1
        """,
        (fixture_id,),
    )
    if row is not None:
        s = _coalesce_int(row.get("season"), 0)
        if s > 0:
            return s

    row = _fetch_one(
        """
        SELECT season
        FROM fixtures
        WHERE fixture_id = %s
        LIMIT 1
        """,
        (fixture_id,),
    )
    if row is not None:
        s = _coalesce_int(row.get("season"), 0)
        if s > 0:
            return s

    return None


def _resolve_season(
    league_id: int,
    season: Optional[int],
    fixture_id: Optional[int] = None,
) -> Optional[int]:
    """
    matchdetail header에서 season이 비어오는 경우 방어 우선순위:
      1) header season
      2) fixture_id 기준 matches.season
      3) fixture_id 기준 fixtures.season
      4) competition_season_meta MAX(season)
      5) standings MAX(season)
      6) matches MAX(season)
      7) fixtures MAX(season)
    """
    if season is not None:
        return season

    s = _resolve_season_from_fixture_id(fixture_id)
    if s is not None:
        return s

    row = _fetch_one(
        """
        SELECT MAX(season) AS season
        FROM competition_season_meta
        WHERE league_id = %s
        """,
        (league_id,),
    )
    if row is not None:
        s = _coalesce_int(row.get("season"), 0)
        if s > 0:
            return s

    row = _fetch_one(
        """
        SELECT MAX(season) AS season
        FROM standings
        WHERE league_id = %s
        """,
        (league_id,),
    )
    if row is not None:
        s = _coalesce_int(row.get("season"), 0)
        if s > 0:
            return s

    row = _fetch_one(
        """
        SELECT MAX(season) AS season
        FROM matches
        WHERE league_id = %s
        """,
        (league_id,),
    )
    if row is not None:
        s = _coalesce_int(row.get("season"), 0)
        if s > 0:
            return s

    row = _fetch_one(
        """
        SELECT MAX(season) AS season
        FROM fixtures
        WHERE league_id = %s
        """,
        (league_id,),
    )
    if row is not None:
        s = _coalesce_int(row.get("season"), 0)
        if s > 0:
            return s

    return None


def _cols_of(table_name: str) -> set[str]:
    try:
        cols = fetch_all(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = %s
            """,
            (table_name,),
        )
        return {str(r.get("column_name") or "") for r in cols if r.get("column_name")}
    except Exception:
        return set()


def _pick_pair(cols: set[str], pairs: List[Tuple[str, str]]) -> Optional[Tuple[str, str]]:
    for a, b in pairs:
        if a in cols and b in cols:
            return (a, b)
    return None

def _extract_fixture_id_from_header(header: Dict[str, Any]) -> Optional[int]:
    """
    matchdetail header에서 fixture_id 추출 (방어적으로 여러 형태 지원)
    - header['fixture_id']
    - header['fixture']['id'] or ['fixture_id']
    - header['match']['fixture_id'] or ['id']
    """
    candidates = []

    candidates.append(header.get("fixture_id"))

    fx = header.get("fixture")
    if isinstance(fx, dict):
        candidates.append(fx.get("fixture_id"))
        candidates.append(fx.get("id"))

    mt = header.get("match")
    if isinstance(mt, dict):
        candidates.append(mt.get("fixture_id"))
        candidates.append(mt.get("id"))

    for v in candidates:
        try:
            if v is None:
                continue
            return int(v)
        except (TypeError, ValueError):
            continue

    return None

def _parse_utc_dt(s: Any) -> Optional[dt.datetime]:
    """
    DB/헤더에서 오는 date_utc 문자열을 UTC datetime으로 파싱 (방어적)
    지원 예:
      - "2026-02-06 13:29:37"
      - "2026-02-06T13:29:37+00:00"
      - "2026-02-06 13:29:37+00:00"
    """
    if not isinstance(s, str):
        return None
    txt = s.strip()
    if not txt:
        return None

    # ISO offset 형태 우선
    try:
        # 공백 -> T 보정
        iso = txt.replace(" ", "T")
        d = dt.datetime.fromisoformat(iso)
        if d.tzinfo is None:
            d = d.replace(tzinfo=dt.timezone.utc)
        return d.astimezone(dt.timezone.utc)
    except Exception:
        pass

    # "yyyy-mm-dd HH:MM:SS" (초 19자리) fallback
    try:
        head = txt.replace("T", " ").split("+", 1)[0].strip()[:19]
        d = dt.datetime.strptime(head, "%Y-%m-%d %H:%M:%S")
        return d.replace(tzinfo=dt.timezone.utc)
    except Exception:
        return None


def _is_knockout_round_name(round_name: Any) -> bool:
    if not isinstance(round_name, str):
        return False

    rn = round_name.strip()
    if not rn:
        return False

    lo = rn.lower()

    if (
        "league stage" in lo
        or "regular season" in lo
        or lo.startswith("group ")
        or "group stage" in lo
        or lo.startswith("stage ")
        or re.match(r"^(apertura|clausura)\s*-\s*\d+$", lo)
    ):
        return False

    include_tokens = (
        "final",
        "semi",
        "quarter",
        "round of",
        "knockout",
        "playoff",
        "play-off",
        "play in",
        "play-in",
        "elimination",
        "preliminary",
        "qualifying",
        "qualifier",
        "reclasificacion",
        "reclasificación",
    )
    if any(t in lo for t in include_tokens):
        return True

    if re.search(r"(^|\s)(\d+)(st|nd|rd|th)\s+round(\s|$)", lo):
        return True
    if re.search(r"(^|\s)(1st|2nd|3rd|4th)\s+round(\s|$)", lo):
        return True

    return False



def _is_header_knockout_context(header: Dict[str, Any]) -> bool:
    league = header.get("league") or {}
    fixture = header.get("fixture") or {}
    match = header.get("match") or {}

    if isinstance(league, dict):
        league_type = league.get("type")
        if isinstance(league_type, str) and league_type.strip().lower() == "cup":
            return True

        league_cup = league.get("cup")
        if league_cup is True:
            return True
        if isinstance(league_cup, (int, float)) and int(league_cup) == 1:
            return True
        if isinstance(league_cup, str) and league_cup.strip().lower() in ("1", "true", "t", "yes", "y"):
            return True

    round_candidates: List[Any] = [
        header.get("league_round"),
    ]

    if isinstance(league, dict):
        round_candidates.append(league.get("round"))
    if isinstance(fixture, dict):
        round_candidates.append(fixture.get("league_round"))
        round_candidates.append(fixture.get("round"))
    if isinstance(match, dict):
        round_candidates.append(match.get("league_round"))
        round_candidates.append(match.get("round"))

    for rn in round_candidates:
        if _is_knockout_round_name(rn):
            return True

    return False

def _get_competition_meta(league_id: int, season: int) -> Optional[Dict[str, Any]]:
    return _fetch_one(
        """
        SELECT
            league_id,
            season,
            has_standings,
            groups_count,
            has_rounds,
            has_knockout_rounds,
            format_hint
        FROM competition_season_meta
        WHERE league_id = %s
          AND season = %s
        LIMIT 1
        """,
        (league_id, season),
    )


def _get_group_meta_names(league_id: int, season: int) -> List[str]:
    rows = fetch_all(
        """
        SELECT group_name
        FROM standings_group_meta
        WHERE league_id = %s
          AND season = %s
        ORDER BY group_order ASC, group_name ASC
        """,
        (league_id, season),
    )
    out: List[str] = []
    for r in rows or []:
        name = r.get("group_name")
        if isinstance(name, str):
            name = re.sub(r"\s+", " ", name).strip()
            if name:
                out.append(name)
    return out

def _competition_blocks_matches_fallback(comp_meta: Optional[Dict[str, Any]]) -> bool:
    """
    matches 기반 standings 계산을 막아야 하는 시즌/대회 포맷 판별.

    계산 fallback은 '순수 단일 리그 테이블(single_table_league)' 에서만 허용.
    아래는 모두 fallback 금지:
    - has_standings = 0
    - 컵/넉아웃 전용
    - league phase + knockout
    - multi group league
    - playoff 포함 리그
    """
    if comp_meta is None:
        return False

    has_standings_meta = _coalesce_int(comp_meta.get("has_standings"), 0)
    groups_count = _coalesce_int(comp_meta.get("groups_count"), 0)
    has_knockout_rounds = _coalesce_int(comp_meta.get("has_knockout_rounds"), 0)
    format_hint = str(comp_meta.get("format_hint") or "").strip().lower()

    if has_standings_meta == 0:
        return True

    if groups_count > 1:
        return True

    if has_knockout_rounds == 1 and format_hint != "single_table_league":
        return True

    blocked_hints = {
        "knockout_only",
        "league_phase_plus_knockout",
        "cup_with_standings",
        "cup_other",
        "multi_group_league",
        "multi_group_league_plus_playoff",
        "single_table_league_plus_playoff",
    }
    if format_hint in blocked_hints:
        return True

    return False


def _should_hide_standings_early_season(
    *,
    league_id: int,
    season: int,
    fixture_id: Optional[int],
    rows_raw: List[Dict[str, Any]],
    window_days: int = 14,
    played_threshold: int = 15,
) -> bool:
    """
    옵션 A:
    - 시즌 시작 초반(window_days 이내)인데
    - standings rows의 played가 비정상적으로 크면(played_threshold 이상)
    => API-Sports가 지난 시즌 최종 테이블을 current season으로 잘못 내려준 케이스로 보고 숨김.
    """
    if not rows_raw:
        return False

    # max played 체크
    max_played = 0
    for r in rows_raw:
        p = _coalesce_int(r.get("played"), 0)
        if p > max_played:
            max_played = p

    if max_played < played_threshold:
        return False

    # 시즌 시작일(우리 DB fixtures 기준)
    min_row = _fetch_one(
        """
        SELECT MIN(date_utc) AS min_date_utc
        FROM fixtures
        WHERE league_id = %s
          AND season = %s
        """,
        (league_id, season),
    )
    season_start_dt = _parse_utc_dt((min_row or {}).get("min_date_utc"))
    if season_start_dt is None:
        return False

    # 현재 경기(혹은 참조 시점) 날짜
    ref_dt: Optional[dt.datetime] = None
    if fixture_id is not None:
        fx_row = _fetch_one(
            """
            SELECT date_utc
            FROM fixtures
            WHERE fixture_id = %s
            LIMIT 1
            """,
            (fixture_id,),
        )
        ref_dt = _parse_utc_dt((fx_row or {}).get("date_utc"))

    if ref_dt is None:
        ref_dt = dt.datetime.now(dt.timezone.utc)

    # 시즌 시작 후 window_days 이내면 "초반"
    delta_days = (ref_dt - season_start_dt).total_seconds() / 86400.0
    if delta_days <= window_days:
        return True

    return False



def build_standings_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
Match Detail용 Standings 블록 (TABLE 전용)

✅ 규칙:
1) standings 테이블 우선
2) 비어있으면 matches로 계산
3) finished=0이면 rows=[] + message
4) mode="TABLE" 고정
"""

    league_id = header.get("league_id")
    season = header.get("season")

    league_name = None
    league_info = header.get("league") or {}
    if isinstance(league_info, dict):
        league_name = league_info.get("name")

    def _extract_team_id(side_key: str) -> Optional[int]:
        side = header.get(side_key) or {}
        if not isinstance(side, dict):
            return None
        tid = side.get("id")
        try:
            return int(tid) if tid is not None else None
        except (TypeError, ValueError):
            return None

    home_team_id = _extract_team_id("home")
    away_team_id = _extract_team_id("away")

    if not league_id:
        return None

    try:
        league_id_int = int(league_id)
    except (TypeError, ValueError):
        return None

    fixture_id = _extract_fixture_id_from_header(header)
    season_resolved = _resolve_season(
        league_id_int,
        season if isinstance(season, int) else None,
        fixture_id=fixture_id,
    )
    is_knockout_context = _is_header_knockout_context(header)

    # 시즌 자체를 못 찾으면: 빈 블록 + 안내
    if season_resolved is None:
        return {
            "league": {
                "league_id": league_id_int,
                "season": None,
                "name": league_name,
            },
            "mode": "TABLE",
            "rows": [],
            "bracket": None,
            "context_options": {"conferences": [], "groups": []},
            "message": "Standings are not available yet.\nPlease check back later.",
            "source": "standings_table",
        }



    # ─────────────────────────────────────────────────────────────
    # 1) standings 테이블 우선
    # ─────────────────────────────────────────────────────────────
    try:
        rows_raw: List[Dict[str, Any]] = fetch_all(
            """
            SELECT
                s.rank,
                s.team_id,
                t.name       AS team_name,
                t.logo       AS team_logo,
                s.played,
                s.win,
                s.draw,
                s.lose,
                s.goals_for,
                s.goals_against,
                s.goals_diff,
                s.points,
                s.description,
                s.group_name,
                s.form
            FROM standings AS s
            JOIN teams     AS t ON t.id = s.team_id
            WHERE s.league_id = %s
              AND s.season    = %s
            ORDER BY s.group_name NULLS FIRST, s.rank NULLS LAST, t.name ASC
            """,
            (league_id_int, season_resolved),
        )
    except Exception:
        rows_raw = []

    comp_meta = _get_competition_meta(league_id_int, season_resolved)
    source = "standings_table" if rows_raw else "computed_from_matches"

    # ─────────────────────────────────────────────────────────────
    # BRACKET 우선 시도 (컵 / 플레이오프 / 넉아웃 대회)
    # - 앱 MatchDetailJsonParser.parseBracketColumns() 구조와 100% 호환
    # - penalty / extra time 점수 반영
    # - single-leg / two-leg tie 모두 처리
    # ─────────────────────────────────────────────────────────────
    format_hint = str((comp_meta or {}).get("format_hint") or "").strip().lower()

    is_bracket_candidate = False

    if format_hint == "knockout_only":
        is_bracket_candidate = True

    elif format_hint in {
        "single_table_league_plus_playoff",
        "league_phase_plus_knockout",
        "multi_group_league_plus_playoff",
    }:
        if _is_header_knockout_context(header):
            is_bracket_candidate = True

    if is_bracket_candidate:
        try:
            knockout_rounds = fetch_all(
                """
                SELECT
                    round_name,
                    round_order,
                    round_kind,
                    is_knockout
                FROM competition_rounds_meta
                WHERE league_id = %s
                  AND season = %s
                  AND is_knockout = 1
                ORDER BY round_order ASC, round_name ASC
                """,
                (league_id_int, season_resolved),
            )

            if knockout_rounds:
                round_names = [
                    str(r.get("round_name")).strip()
                    for r in knockout_rounds
                    if r.get("round_name")
                ]

                matches = fetch_all(
                    """
                    SELECT
                        m.fixture_id,
                        m.league_round,
                        m.date_utc,
                        m.home_id,
                        m.away_id,
                        m.home_ft,
                        m.away_ft,
                        m.status,
                        m.status_group,
                        th.name AS home_name,
                        ta.name AS away_name,
                        th.logo AS home_logo,
                        ta.logo AS away_logo
                    FROM matches m
                    LEFT JOIN teams th ON th.id = m.home_id
                    LEFT JOIN teams ta ON ta.id = m.away_id
                    WHERE m.league_id = %s
                      AND m.season = %s
                      AND m.league_round = ANY(%s)
                    ORDER BY
                        m.league_round ASC,
                        m.date_utc ASC,
                        m.fixture_id ASC
                    """,
                    (league_id_int, season_resolved, round_names),
                )

                def _match_is_finished(m: Dict[str, Any]) -> bool:
                    sg = str(m.get("status_group") or "").strip().upper()
                    st = str(m.get("status") or "").strip().upper()
                    return sg == "FINISHED" or st in {"FT", "AET", "PEN"}

                def _unordered_pair_key(m: Dict[str, Any]) -> Tuple[int, int]:
                    h = _coalesce_int(m.get("home_id"), 0)
                    a = _coalesce_int(m.get("away_id"), 0)
                    return tuple(sorted((h, a)))

                def _leg_payload(m: Dict[str, Any], leg_index: Optional[int]) -> Dict[str, Any]:
                    resolved = _resolve_bracket_display_scores(m)
                    return {
                        "fixture_id": _safe_int_or_none(m.get("fixture_id")),
                        "leg_index": leg_index,
                        "date_utc": _safe_text_or_none(m.get("date_utc")),
                        "home_id": _coalesce_int(m.get("home_id"), 0),
                        "home_name": _safe_text_or_none(m.get("home_name")),
                        "home_logo": _safe_text_or_none(m.get("home_logo")),
                        "away_id": _coalesce_int(m.get("away_id"), 0),
                        "away_name": _safe_text_or_none(m.get("away_name")),
                        "away_logo": _safe_text_or_none(m.get("away_logo")),
                        "home_ft": resolved.get("home"),
                        "away_ft": resolved.get("away"),
                        "home_pen": resolved.get("pen_home"),
                        "away_pen": resolved.get("pen_away"),
                        "home_et": resolved.get("et_home"),
                        "away_et": resolved.get("et_away"),
                        "score_status": resolved.get("status"),
                    }

                def _agg_score_for_leg(leg: Dict[str, Any], original_match: Dict[str, Any]) -> Tuple[Optional[int], Optional[int]]:
                    """
                    aggregate 계산용 점수:
                    - AET면 extratime 우선
                    - FT면 fulltime 우선
                    - PEN이면 연장점수 있으면 extratime, 없으면 fulltime, 그것도 없으면 display score
                    """
                    resolved = _resolve_bracket_display_scores(original_match)
                    status = str(original_match.get("status") or "").strip().upper()

                    if status == "AET":
                        if resolved.get("et_home") is not None and resolved.get("et_away") is not None:
                            return resolved.get("et_home"), resolved.get("et_away")
                        if resolved.get("ft_home") is not None and resolved.get("ft_away") is not None:
                            return resolved.get("ft_home"), resolved.get("ft_away")

                    if status == "PEN":
                        if resolved.get("et_home") is not None and resolved.get("et_away") is not None:
                            return resolved.get("et_home"), resolved.get("et_away")
                        if resolved.get("ft_home") is not None and resolved.get("ft_away") is not None:
                            return resolved.get("ft_home"), resolved.get("ft_away")

                    if resolved.get("ft_home") is not None and resolved.get("ft_away") is not None:
                        return resolved.get("ft_home"), resolved.get("ft_away")

                    return leg.get("home_ft"), leg.get("away_ft")

                def _build_round_ties(round_label: str, round_matches: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
                    if not round_matches:
                        return []

                    grouped: Dict[Tuple[int, int], List[Dict[str, Any]]] = {}

                    for m in round_matches:
                        h = _coalesce_int(m.get("home_id"), 0)
                        a = _coalesce_int(m.get("away_id"), 0)
                        if h > 0 and a > 0:
                            grouped.setdefault(_unordered_pair_key(m), []).append(m)

                    ties: List[Dict[str, Any]] = []
                    used_fixture_ids: set[int] = set()
                    order_hint = 1

                    for _, pair_matches in grouped.items():
                        pair_matches = sorted(
                            pair_matches,
                            key=lambda x: (
                                str(x.get("date_utc") or ""),
                                _coalesce_int(x.get("fixture_id"), 0),
                            ),
                        )

                        # 왕복전 묶음: 동일 두 팀이 정확히 2경기일 때 우선 2leg tie 처리
                        if len(pair_matches) == 2:
                            m1 = pair_matches[0]
                            m2 = pair_matches[1]

                            used_fixture_ids.add(_coalesce_int(m1.get("fixture_id"), 0))
                            used_fixture_ids.add(_coalesce_int(m2.get("fixture_id"), 0))

                            leg1 = _leg_payload(m1, 1)
                            leg2 = _leg_payload(m2, 2)

                            home_anchor_id = leg1["home_id"]
                            away_anchor_id = leg1["away_id"]

                            agg_home: Optional[int] = None
                            agg_away: Optional[int] = None

                            if _match_is_finished(m1) and _match_is_finished(m2):
                                s1h, s1a = _agg_score_for_leg(leg1, m1)
                                s2h, s2a = _agg_score_for_leg(leg2, m2)

                                if None not in (s1h, s1a, s2h, s2a):
                                    h_sum = 0
                                    a_sum = 0

                                    # leg1
                                    if leg1["home_id"] == home_anchor_id:
                                        h_sum += int(s1h)
                                    elif leg1["home_id"] == away_anchor_id:
                                        a_sum += int(s1h)

                                    if leg1["away_id"] == home_anchor_id:
                                        h_sum += int(s1a)
                                    elif leg1["away_id"] == away_anchor_id:
                                        a_sum += int(s1a)

                                    # leg2
                                    if leg2["home_id"] == home_anchor_id:
                                        h_sum += int(s2h)
                                    elif leg2["home_id"] == away_anchor_id:
                                        a_sum += int(s2h)

                                    if leg2["away_id"] == home_anchor_id:
                                        h_sum += int(s2a)
                                    elif leg2["away_id"] == away_anchor_id:
                                        a_sum += int(s2a)

                                    agg_home = h_sum
                                    agg_away = a_sum

                            ties.append(
                                {
                                    "round_label": round_label,
                                    "order_hint": order_hint,
                                    "agg_home": agg_home,
                                    "agg_away": agg_away,
                                    "legs": [leg1, leg2],
                                }
                            )
                            order_hint += 1

                    # 나머지 단판 경기
                    remaining_matches = [
                        m for m in round_matches
                        if _coalesce_int(m.get("fixture_id"), 0) not in used_fixture_ids
                    ]

                    remaining_matches = sorted(
                        remaining_matches,
                        key=lambda x: (
                            str(x.get("date_utc") or ""),
                            _coalesce_int(x.get("fixture_id"), 0),
                        ),
                    )

                    for m in remaining_matches:
                        used_fixture_ids.add(_coalesce_int(m.get("fixture_id"), 0))
                        ties.append(
                            {
                                "round_label": round_label,
                                "order_hint": order_hint,
                                "agg_home": None,
                                "agg_away": None,
                                "legs": [_leg_payload(m, None)],
                            }
                        )
                        order_hint += 1

                    ties.sort(
                        key=lambda t: (
                            _coalesce_int(t.get("order_hint"), 999999),
                            str(
                                (
                                    (t.get("legs") or [{}])[0].get("date_utc")
                                    if (t.get("legs") or [])
                                    else ""
                                ) or ""
                            ),
                        )
                    )
                    return ties

                bracket_columns: List[Dict[str, Any]] = []

                for rnd in knockout_rounds:
                    round_label = _safe_text_or_none(rnd.get("round_name"))
                    if not round_label:
                        continue

                    round_matches = [
                        m for m in matches
                        if _safe_text_or_none(m.get("league_round")) == round_label
                    ]

                    ties = _build_round_ties(round_label, round_matches)
                    if not ties:
                        continue

                    bracket_columns.append(
                        {
                            "round_label": round_label,
                            "ties": ties,
                            "_round_order": rnd.get("round_order"),
                        }
                    )
                if bracket_columns:
                    # ✅ FA Cup 2025 한정 예외 보정:
                    # 1/128-finals 가 Round of 128 바로 이전 단계로 오도록만 위치 조정
                    if league_id_int == 45 and season_resolved == 2025:
                        idx_1128 = next(
                            (i for i, c in enumerate(bracket_columns)
                             if str(c.get("round_label") or "").strip() == "1/128-finals"),
                            None,
                        )
                        idx_r128 = next(
                            (i for i, c in enumerate(bracket_columns)
                             if str(c.get("round_label") or "").strip() == "Round of 128"),
                            None,
                        )

                        if idx_1128 is not None and idx_r128 is not None and idx_1128 > idx_r128:
                            col_1128 = bracket_columns.pop(idx_1128)
                            idx_r128 = next(
                                (i for i, c in enumerate(bracket_columns)
                                 if str(c.get("round_label") or "").strip() == "Round of 128"),
                                None,
                            )
                            if idx_r128 is not None:
                                bracket_columns.insert(idx_r128 + 1, col_1128)

                    for col in bracket_columns:
                        col.pop("_round_order", None)

                    return {
                        "league": {
                            "league_id": league_id_int,
                            "season": season_resolved,
                            "name": league_name,
                        },
                        "mode": "BRACKET",
                        "rows": [],
                        "bracket": bracket_columns,
                        "context_options": {"conferences": [], "groups": []},
                        "source": "computed_bracket",
                    }

        except Exception:
            pass

    # ✅ competition meta 기준으로 matches fallback 허용/차단을 먼저 결정
    if not rows_raw and _competition_blocks_matches_fallback(comp_meta):
        format_hint = str((comp_meta or {}).get("format_hint") or "").strip().lower()
        has_standings_meta = _coalesce_int((comp_meta or {}).get("has_standings"), 0)

        message = "Standings are not available for this competition."
        source_name = "competition_meta_no_standings"

        if format_hint in {
            "knockout_only",
            "league_phase_plus_knockout",
            "cup_with_standings",
            "cup_other",
        }:
            message = "Standings are not available for this competition stage.\nPlease check back later."
            source_name = "competition_meta_complex_format"
        elif format_hint in {
            "multi_group_league",
            "multi_group_league_plus_playoff",
            "single_table_league_plus_playoff",
        }:
            message = "Standings are not available yet.\nPlease check back later."
            source_name = "competition_meta_grouped_or_playoff_format"
        elif has_standings_meta == 0:
            message = "Standings are not available for this competition."
            source_name = "competition_meta_no_standings"

        return {
            "league": {
                "league_id": league_id_int,
                "season": season_resolved,
                "name": league_name,
            },
            "mode": "TABLE",
            "rows": [],
            "bracket": None,
            "context_options": {"conferences": [], "groups": []},
            "message": message,
            "source": source_name,
        }

    # ─────────────────────────────────────────────────────────────
    # ✅ 옵션 A: 시즌 초반 + played 비정상(지난 시즌 최종 테이블로 의심) => 스탠딩 숨김
    # ─────────────────────────────────────────────────────────────
    if rows_raw:
        if _should_hide_standings_early_season(
            league_id=league_id_int,
            season=season_resolved,
            fixture_id=fixture_id,
            rows_raw=rows_raw,
            window_days=14,
            played_threshold=15,
        ):
            return {
                "league": {
                    "league_id": league_id_int,
                    "season": season_resolved,
                    "name": league_name,
                },
                "mode": "TABLE",
                "rows": [],
                "bracket": None,
                "context_options": {"conferences": [], "groups": []},
                "message": "Standings are not available yet.\nPlease check back later.",
                "source": "standings_table",
            }


    # ─────────────────────────────────────────────────────────────
    # 2) standings가 비어 있고 현재 컨텍스트가 컵/넉아웃이면
    #    matches 기반 standings fallback 을 막는다.
    # ─────────────────────────────────────────────────────────────
    if not rows_raw and is_knockout_context:
        return {
            "league": {
                "league_id": league_id_int,
                "season": season_resolved,
                "name": league_name,
            },
            "mode": "TABLE",
            "rows": [],
            "bracket": None,
            "context_options": {"conferences": [], "groups": []},
            "message": "Standings are not available for this knockout round.\nPlease check back later.",
            "source": "knockout_no_standings",
        }

    # ─────────────────────────────────────────────────────────────
    # 3) standings 비어 있으면 → matches에서 계산 (리그/조별/리그페이즈만)
    # ─────────────────────────────────────────────────────────────
    if not rows_raw:
        mcols = _cols_of("matches")

        team_pair = _pick_pair(
            mcols,
            [
                ("home_team_id", "away_team_id"),
                ("home_id", "away_id"),
            ],
        )
        goal_pair = _pick_pair(
            mcols,
            [
                ("home_goals", "away_goals"),
                ("home_ft", "away_ft"),
                ("goals_home", "goals_away"),
                ("home_score", "away_score"),
            ],
        )

        if not team_pair or not goal_pair:
            return {
                "league": {
                    "league_id": league_id_int,
                    "season": season_resolved,
                    "name": league_name,
                },
                "mode": "TABLE",
                "rows": [],
                "bracket": None,
                "context_options": {"conferences": [], "groups": []},
                "message": "Standings are not available yet.\nPlease check back later.",
                "source": source,
            }

        ht, at = team_pair
        hg, ag = goal_pair

        try:
            cnt_row = _fetch_one(
                f"""
                SELECT COUNT(*) AS cnt
                FROM matches
                WHERE league_id = %s
                  AND season = %s
                  AND (
                    lower(coalesce(status_group,'')) = 'finished'
                    OR coalesce(status,'') IN ('FT','AET','PEN')
                    OR coalesce(status_short,'') IN ('FT','AET','PEN')
                  )
                  AND {ht} IS NOT NULL AND {at} IS NOT NULL
                  AND {hg} IS NOT NULL AND {ag} IS NOT NULL
                """,
                (league_id_int, season_resolved),
            )
            finished_cnt = int((cnt_row or {}).get("cnt") or 0)
        except Exception:
            finished_cnt = 0

        if finished_cnt <= 0:
            return {
                "league": {
                    "league_id": league_id_int,
                    "season": season_resolved,
                    "name": league_name,
                },
                "mode": "TABLE",
                "rows": [],
                "bracket": None,
                "context_options": {"conferences": [], "groups": []},
                "message": "Standings are not available yet.\nPlease check back later.",
                "source": source,
            }

        try:
            rows_raw = fetch_all(
                f"""
                WITH finished AS (
                  SELECT
                    {ht} AS home_team_id,
                    {at} AS away_team_id,
                    {hg} AS home_goals,
                    {ag} AS away_goals
                  FROM matches
                  WHERE league_id = %s
                    AND season = %s
                    AND (
                      lower(coalesce(status_group,'')) = 'finished'
                      OR coalesce(status,'') IN ('FT','AET','PEN')
                      OR coalesce(status_short,'') IN ('FT','AET','PEN')
                    )
                    AND {ht} IS NOT NULL AND {at} IS NOT NULL
                    AND {hg} IS NOT NULL AND {ag} IS NOT NULL
                ),
                per_team AS (
                  SELECT
                    home_team_id AS team_id,
                    COUNT(*) AS played,
                    SUM(CASE WHEN home_goals > away_goals THEN 1 ELSE 0 END) AS win,
                    SUM(CASE WHEN home_goals = away_goals THEN 1 ELSE 0 END) AS draw,
                    SUM(CASE WHEN home_goals < away_goals THEN 1 ELSE 0 END) AS lose,
                    SUM(home_goals) AS goals_for,
                    SUM(away_goals) AS goals_against,
                    SUM(CASE WHEN home_goals > away_goals THEN 3 WHEN home_goals = away_goals THEN 1 ELSE 0 END) AS points
                  FROM finished
                  GROUP BY home_team_id

                  UNION ALL

                  SELECT
                    away_team_id AS team_id,
                    COUNT(*) AS played,
                    SUM(CASE WHEN away_goals > home_goals THEN 1 ELSE 0 END) AS win,
                    SUM(CASE WHEN away_goals = home_goals THEN 1 ELSE 0 END) AS draw,
                    SUM(CASE WHEN away_goals < home_goals THEN 1 ELSE 0 END) AS lose,
                    SUM(away_goals) AS goals_for,
                    SUM(home_goals) AS goals_against,
                    SUM(CASE WHEN away_goals > home_goals THEN 3 WHEN away_goals = home_goals THEN 1 ELSE 0 END) AS points
                  FROM finished
                  GROUP BY away_team_id
                ),
                agg AS (
                  SELECT
                    team_id,
                    SUM(played) AS played,
                    SUM(win) AS win,
                    SUM(draw) AS draw,
                    SUM(lose) AS lose,
                    SUM(goals_for) AS goals_for,
                    SUM(goals_against) AS goals_against,
                    (SUM(goals_for) - SUM(goals_against)) AS goals_diff,
                    SUM(points) AS points
                  FROM per_team
                  GROUP BY team_id
                ),
                ranked AS (
                  SELECT
                    ROW_NUMBER() OVER (
                      ORDER BY points DESC, goals_diff DESC, goals_for DESC, team_id ASC
                    ) AS rank,
                    *
                  FROM agg
                )
                SELECT
                  r.rank,
                  r.team_id,
                  COALESCE(t.name, '') AS team_name,
                  t.logo AS team_logo,
                  r.played,
                  r.win,
                  r.draw,
                  r.lose,
                  r.goals_for,
                  r.goals_against,
                  r.goals_diff,
                  r.points,
                  NULL::text AS description,
                  NULL::text AS group_name,
                  NULL::text AS form
                FROM ranked r
                LEFT JOIN teams t ON t.id = r.team_id
                ORDER BY r.rank ASC, team_name ASC
                """,
                (league_id_int, season_resolved),
            )
        except Exception:
            rows_raw = []

        if not rows_raw:
            return {
                "league": {
                    "league_id": league_id_int,
                    "season": season_resolved,
                    "name": league_name,
                },
                "mode": "TABLE",
                "rows": [],
                "bracket": None,
                "context_options": {"conferences": [], "groups": []},
                "message": "Standings are not available yet.\nPlease check back later.",
                "source": source,
            }

    # ─────────────────────────────────────────────────────────────
    # 공통 후처리 (핵심 변경):
    # - (team_id) 단독 디듀프 제거
    # - (team_id + group_name) 기준으로만 "진짜 중복" 정리
    # - group 컷팅(main_group) 완전 제거 → 전체 rows 유지 + 앱에서 필터 선택
    # ─────────────────────────────────────────────────────────────
    def _norm_group(v: Any) -> str:
        if not isinstance(v, str):
            return ""
        return re.sub(r"\s+", " ", v).strip()

    def _better_row(a: Dict[str, Any], b: Dict[str, Any]) -> Dict[str, Any]:
        """
        같은 (team_id, group_name) 키에 대해 어떤 row를 남길지 결정.
        - played 큰 것 우선
        - points, goals_diff, goals_for 큰 것 우선
        - rank는 작은 것 우선 (있으면)
        """
        a_played = _coalesce_int(a.get("played"), 0)
        b_played = _coalesce_int(b.get("played"), 0)
        if b_played != a_played:
            return b if b_played > a_played else a

        a_pts = _coalesce_int(a.get("points"), 0)
        b_pts = _coalesce_int(b.get("points"), 0)
        if b_pts != a_pts:
            return b if b_pts > a_pts else a

        a_gd = _coalesce_int(a.get("goals_diff"), 0)
        b_gd = _coalesce_int(b.get("goals_diff"), 0)
        if b_gd != a_gd:
            return b if b_gd > a_gd else a

        a_gf = _coalesce_int(a.get("goals_for"), 0)
        b_gf = _coalesce_int(b.get("goals_for"), 0)
        if b_gf != a_gf:
            return b if b_gf > a_gf else a

        a_rank = _coalesce_int(a.get("rank"), 0) or 999999
        b_rank = _coalesce_int(b.get("rank"), 0) or 999999
        if b_rank != a_rank:
            return b if b_rank < a_rank else a

        return a

    rows_by_key: Dict[Tuple[int, str], Dict[str, Any]] = {}
    for r in rows_raw:
        tid = _coalesce_int(r.get("team_id"), 0)
        if tid == 0:
            continue
        gkey = _norm_group(r.get("group_name"))
        key = (tid, gkey)
        prev = rows_by_key.get(key)
        if prev is None:
            rows_by_key[key] = r
        else:
            rows_by_key[key] = _better_row(prev, r)

    dedup_rows: List[Dict[str, Any]] = list(rows_by_key.values())

    # 정렬: group_name -> rank -> team_name (그룹별로 붙어서 내려가게)
    def _sort_key(r: Dict[str, Any]):
        eff_g = _effective_group_name(
            raw_group_name=r.get("group_name"),
            description=r.get("description"),
        )
        g = _norm_group(eff_g)
        rk = _coalesce_int(r.get("rank"), 0) or 999999
        tn = str(r.get("team_name") or "")
        return (g.lower(), rk, tn.lower())


    dedup_rows.sort(key=_sort_key)

    table: List[Dict[str, Any]] = []
    for r in dedup_rows:
        team_id = _coalesce_int(r.get("team_id"), 0)
        table.append(
            {
                "position": _coalesce_int(r.get("rank"), 0),
                "team_id": team_id,
                "team_name": r.get("team_name") or "",
                "team_logo": r.get("team_logo"),
                "played": _coalesce_int(r.get("played"), 0),
                "win": _coalesce_int(r.get("win"), 0),
                "draw": _coalesce_int(r.get("draw"), 0),
                "loss": _coalesce_int(r.get("lose"), 0),
                "goals_for": _coalesce_int(r.get("goals_for"), 0),
                "goals_against": _coalesce_int(r.get("goals_against"), 0),
                "goal_diff": _coalesce_int(r.get("goals_diff"), 0),
                "points": _coalesce_int(r.get("points"), 0),
                "description": r.get("description"),
                "group_name": _effective_group_name(
                    raw_group_name=r.get("group_name"),
                    description=r.get("description"),
                ),
                "form": r.get("form"),

                "is_home": (home_team_id is not None and team_id == home_team_id),
                "is_away": (away_team_id is not None and team_id == away_team_id),
            }
        )

    context_options = _build_context_options_from_rows(dedup_rows)

    # ✅ 서버에서 저장한 standings_group_meta가 있으면 그 값을 우선 사용
    try:
        meta_group_names = _get_group_meta_names(league_id_int, season_resolved)
        if meta_group_names:
            conferences: List[str] = []
            groups: List[str] = []

            for name in meta_group_names:
                nl = name.lower()
                if "east" in nl:
                    conferences.append("East")
                    continue
                if "west" in nl:
                    conferences.append("West")
                    continue
                groups.append(name)

            def _dedup_keep_order(items: List[str]) -> List[str]:
                seen = set()
                out: List[str] = []
                for x in items:
                    k = x.lower()
                    if k in seen:
                        continue
                    seen.add(k)
                    out.append(x)
                return out

            context_options = {
                "conferences": _dedup_keep_order(conferences),
                "groups": _dedup_keep_order(groups),
            }
    except Exception:
        pass

    out: Dict[str, Any] = {
        "league": {
            "league_id": league_id_int,
            "season": season_resolved,
            "name": league_name,
        },
        "mode": "TABLE",
        "rows": table,
        "bracket": None,
        "context_options": context_options,
        "source": source,
    }


    if not table:
        out["message"] = "Standings are not available yet.\nPlease check back later."

    return out


def _effective_group_name(
    *,
    raw_group_name: Any,
    description: Any,
) -> Optional[str]:
    """
    rows의 group_name이 리그명/기본값으로만 채워지고,
    실제 구분(Championship/Relegation)이 description에만 있는 리그(Austria 등) 보정.

    - description에 championship/relegation 라운드가 있으면 group_name을 그 값으로 강제
    - 이미 group_name이 Group A/B, East/West, Championship Round 등 의미 있는 값이면 유지
    """
    g = raw_group_name.strip() if isinstance(raw_group_name, str) else ""
    d = description.strip().lower() if isinstance(description, str) else ""

    # group_name 자체가 의미있으면 그대로 둠
    gl = g.lower()
    if gl:
        if ("champ" in gl and "round" in gl) or ("releg" in gl and "round" in gl):
            return g
        if gl.startswith("group "):
            return g
        if "east" in gl or "west" in gl:
            return g

    # description 기반 split round 보정
    if "champ" in d and "round" in d:
        return "Championship Round"
    if "releg" in d and "round" in d:
        return "Relegation Round"

    # 그 외는 기존 group_name 유지(빈 값이면 None)
    return g if g else None



def _build_context_options_from_rows(
    rows: List[Dict[str, Any]]
) -> Dict[str, List[str]]:
    """
    StandingsDao.buildContext(...) 에서 하던 컨퍼런스/그룹 인식 로직을
    서버쪽으로 옮긴 버전 (순수 A방식 준비).

    - conferences: ["East", "West"] 등
    - groups: ["Group A", "Group B", "Championship Round", "Relegation Round"] 등
    """
    if not rows:
        return {"conferences": [], "groups": []}

    group_raw: List[str] = []
    desc_raw: List[str] = []
    for r in rows:
        g = r.get("group_name")
        d = r.get("description")
        if isinstance(g, str):
            g = g.strip()
            if g:
                group_raw.append(re.sub(r"\s+", " ", g))
        if isinstance(d, str):
            desc_raw.append(d.lower())

    group_raw = list(dict.fromkeys(group_raw))  # distinct, 순서 유지

    rx_has_split_round = re.compile(
        r"(champ(ion)?ship\s+.*(round|rnd))|(releg(ation)?\s+.*(round|rnd))",
        re.IGNORECASE,
    )
    rx_group = re.compile(r"group\s*([A-Z])", re.IGNORECASE)

    def derive_from_description() -> List[str]:
        if not desc_raw:
            return []
        has_champ_round = any(
            rx_has_split_round.search(d) and "champ" in d for d in desc_raw
        )
        has_releg_round = any(
            rx_has_split_round.search(d) and "releg" in d for d in desc_raw
        )
        out: List[str] = []
        if has_champ_round:
            out.append("Championship Round")
        if has_releg_round:
            out.append("Relegation Round")
        return out

    has_east = any("east" in g.lower() for g in group_raw)
    has_west = any("west" in g.lower() for g in group_raw)
    has_grp = any(rx_group.search(g) for g in group_raw)
    has_rnd = any(rx_has_split_round.search(g) for g in group_raw)

    conferences: List[str] = []
    if has_east:
        conferences.append("East")
    if has_west:
        conferences.append("West")

    groups: List[str] = []
    for g in group_raw:
        gl = g.lower()
        if "east" in gl or "west" in gl:
            continue
        m = rx_group.search(g)
        if m:
            groups.append(f"Group {m.group(1).upper()}")
        elif rx_has_split_round.search(g) and "champ" in gl:
            groups.append("Championship Round")
        elif rx_has_split_round.search(g) and "releg" in gl:
            groups.append("Relegation Round")

    has_meaningful = has_east or has_west or has_grp or has_rnd or bool(groups)
    if not has_meaningful:
        groups = derive_from_description()

    def _dedup_case_insensitive(items: List[str]) -> List[str]:
        seen = set()
        out: List[str] = []
        for x in items:
            key = x.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(x)
        return out

    conferences = _dedup_case_insensitive(conferences)
    groups = _dedup_case_insensitive(groups)

    return {"conferences": conferences, "groups": groups}
