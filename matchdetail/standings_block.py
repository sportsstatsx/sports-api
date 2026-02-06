# services/matchdetail/standings_block.py
from __future__ import annotations

from typing import Any, Dict, Optional, List, Tuple
import re

from db import fetch_all


def _coalesce_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except (TypeError, ValueError):
        return default


def _fetch_one(query: str, params: tuple) -> Optional[Dict[str, Any]]:
    rows = fetch_all(query, params)
    return rows[0] if rows else None


def _resolve_season(league_id: int, season: Optional[int]) -> Optional[int]:
    """
    matchdetail header에서 season이 비어오는 경우 방어:
      1) standings에서 MAX(season)
      2) 없으면 fixtures에서 MAX(season)
    """
    if season is not None:
        return season

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


def _is_knockout_round_for_bracket(league_id: int, round_name: Optional[str]) -> bool:
    """
    BRACKET 표시 대상 라운드 판정(규칙 기반).

    ✅ 우리가 합의한 정책:
    - "예선이라도 넉아웃이면 브라켓에 포함"
    - 단, '승점/스테이지/리그 예선'은 브라켓에서 제외
      (League Stage - n / Regular Season - n / Apertura - n / Clausura - n / Group A 등)
    """
    if not round_name or not isinstance(round_name, str):
        return False

    rn = round_name.strip()
    if not rn:
        return False

    lo = rn.lower()

    # 1) ✅ 승점/스테이지/리그 방식 예선 제외
    if (
        "league stage" in lo
        or "regular season" in lo
        or "apertura" in lo
        or "clausura" in lo
        or lo.startswith("group ")
        or "group stage" in lo
        or lo.startswith("stage ")
    ):
        return False

    # 2) ✅ 넉아웃 시사 키워드 포함이면 포함
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
    )
    if any(t in lo for t in include_tokens):
        return True

    # 3) ✅ 1st/2nd/3rd/4th Round 패턴 포함
    if re.search(r"(^|\s)(\d+)(st|nd|rd|th)\s+round(\s|$)", lo):
        return True
    if re.search(r"(^|\s)(1st|2nd|3rd|4th)\s+round(\s|$)", lo):
        return True

    return False


# ─────────────────────────────────────────
# Bracket round name normalization (ONE-SHOT)
# ─────────────────────────────────────────

def _norm_round_name(raw: Any) -> str:
    s = (raw or "").strip()
    if not s:
        return ""
    s = s.lower()
    s = s.replace("–", "-").replace("—", "-")
    s = re.sub(r"\s+", " ", s)
    s = s.replace("-", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


def _canonical_round_key(raw_round_name: Any) -> Optional[str]:
    """
    DB round_name(표기 제각각)을 canonical key로 정규화.
    - 흔한 변형(Last 16, 1/8 Finals, Round of Eight, Promotion/Relegation Playoffs 등)까지 흡수
    """
    s = _norm_round_name(raw_round_name)
    if not s:
        return None

    # ── Elimination (Final 포함) ───────────────────────────
    # "Elimination Final"이 먼저 final로 잡혀버리는 문제 방지
    if "elimination" in s:
        if re.search(r"(^|\s)elimination\s+final(s)?(\s|$)", s):
            return "elimination_final"
        return "elimination"

    # ── Semi / Quarter / Final ─────────────────────────────
    # ✅ Semi/Quarter를 Final보다 먼저 판정해야 "semi finals"가 final로 오염되지 않는다.
    if re.search(r"(^|\s)semi\s*finals?(\s|$)", s) or re.search(r"(^|\s)semifinals?(\s|$)", s):
        return "semi"

    if re.search(r"(^|\s)quarter\s*finals?(\s|$)", s) or re.search(r"(^|\s)quarterfinals?(\s|$)", s):
        return "quarter"

    # Final은 가장 마지막에 판정 (semi/quarter/elimination final과 충돌 방지)
    if re.search(r"(^|\s)grand\s+final(s)?(\s|$)", s) or re.search(r"(^|\s)finals?(\s|$)", s):
        return "final"



    # ── Last N / 1/8 Finals / 1/16 Finals / Round of words ──
    # Last 16 / Last16 / Last-16
    m = re.fullmatch(r"last (\d+)", s)
    if m:
        n = int(m.group(1))
        if n == 8:
            return "quarter"
        if n == 4:
            return "semi"
        if n == 2:
            return "final"
        return f"r{n}"

    # 1/8 finals, 1/16 finals, 1/32 finals ...
    m = re.fullmatch(r"1\s*/\s*(\d+)\s*finals?", s)
    if m:
        denom = int(m.group(1))
        n = denom * 2
        if n == 8:
            return "quarter"
        if n == 4:
            return "semi"
        if n == 2:
            return "final"
        return f"r{n}"

    # Round of N
    m = re.fullmatch(r"round of (\d+)", s)
    if m:
        n = int(m.group(1))
        if n == 8:
            return "quarter"
        if n == 4:
            return "semi"
        if n == 2:
            return "final"
        return f"r{n}"

    # Round of sixteen / thirty two / sixty four / one hundred twenty eight / two hundred fifty six
    word_map = {
        "sixteen": 16,
        "thirty two": 32,
        "thirtytwo": 32,
        "sixty four": 64,
        "sixtyfour": 64,
        "one hundred twenty eight": 128,
        "one hundred and twenty eight": 128,
        "one hundred twentyeight": 128,
        "two hundred fifty six": 256,
        "two hundred and fifty six": 256,
        "two hundred fiftysix": 256,
    }
    m = re.fullmatch(r"round of (.+)", s)
    if m:
        w = m.group(1).strip()
        n = word_map.get(w)
        if n:
            if n == 8:
                return "quarter"
            if n == 4:
                return "semi"
            if n == 2:
                return "final"
            return f"r{n}"

    # ── Play-in / Wildcard ─────────────────────────────────
    if "play in" in s or "playin" in s or "wild card" in s or "wildcard" in s:
        return "play_in"

    # ── Playoffs variants (promotion/relegation/championship) ──
    # promotion playoffs / relegation playoffs / championship playoffs 등
    if "play off" in s or "playoff" in s or "play offs" in s or "playoffs" in s:
        if "knockout round" in s:
            return "knockout_playoffs"
        return "playoffs"

    # ── Elimination ────────────────────────────────────────
    if "elimination" in s:
        if "final" in s:
            return "elimination_final"
        return "elimination"

    # ── Preliminary / Qualifying ───────────────────────────
    if "preliminary" in s:
        return "preliminary"
    if "qualifying" in s or "qualifier" in s:
        return "qualifying"

    # ── 1st/2nd/3rd/4th Round (숫자 기반) ───────────────────
    # 기존은 fullmatch라서 "Conference League Play-offs - 1st Round" 같은 케이스가 안 잡힘
    # → 포함 매칭으로 확장
    m = re.search(r"(^|\s)(\d+)(st|nd|rd|th)\s+round(\s|$)", s)
    if m:
        return f"round_{m.group(2)}"


    return None



def _canonical_round_label(canon: str) -> str:
    """
    canonical key -> UI label(표기 통일)
    """
    if canon == "final":
        return "Final"
    if canon == "semi":
        return "Semi-finals"
    if canon == "quarter":
        return "Quarter-finals"
    if canon.startswith("r") and canon[1:].isdigit():
        return f"Round of {canon[1:]}"
    if canon == "knockout_playoffs":
        return "Knockout Round Play-offs"
    if canon == "playoffs":
        return "Play-offs"
    if canon == "play_in":
        return "Play-In"
    if canon == "elimination_final":
        return "Elimination Final"
    if canon == "elimination":
        return "Elimination"
    if canon == "preliminary":
        return "Preliminary Round"
    if canon == "qualifying":
        return "Qualifying Round"

    # round_1/2/3/4... 는 1st/2nd/3rd/4th...로 표기
    if canon.startswith("round_"):
        n_str = canon.split("_", 1)[1]
        if n_str.isdigit():
            n = int(n_str)
            if n % 100 in (11, 12, 13):
                suf = "th"
            else:
                suf = {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")
            return f"{n}{suf} Round"

    return canon



# canonical round 정렬 우선순위(고정)
# - 더 큰 예선 라운드가 나와도 표시 가능하도록 r256 추가(있으면 쓰고, 없으면 그냥 무시됨)
_CANON_ORDER: List[str] = [
    "preliminary",
    "qualifying",
    "round_1",
    "round_2",
    "round_3",
    "round_4",
    "play_in",
    "elimination",
    "elimination_final",
    "playoffs",
    "knockout_playoffs",
    "r256",
    "r128",
    "r64",
    "r32",
    "r16",
    "quarter",
    "semi",
    "final",
]
_CANON_ORDER_INDEX = {k: i for i, k in enumerate(_CANON_ORDER)}




def _build_bracket_from_tournament_ties(
    league_id: int,
    season: int,
    *,
    start_round_name: Optional[str] = None,
    end_round_name: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    tournament_ties 기반 bracket 생성.

    - start_round_name: 이 라운드부터(포함) 보여줌
    - end_round_name: 이 라운드까지(포함) 보여줌

    ✅ 변경 포인트(원샷 안정화):
    - DB round_name 표기가 제각각이어도 canonical key로 정규화해서 정렬/표시
    - 매번 order 리스트에 문자열을 계속 추가하는 패치 반복을 제거
    - 기존 legs/팀매핑/knockout 필터 정책은 그대로 유지
    """

    # start/end도 canonical로 변환해서 범위 필터를 안정화
    start_canon = _canonical_round_key(start_round_name) if start_round_name else None
    end_canon = _canonical_round_key(end_round_name) if end_round_name else None

    start_idx = _CANON_ORDER_INDEX.get(start_canon) if start_canon else None
    end_idx = _CANON_ORDER_INDEX.get(end_canon) if end_canon else None

    # 해당 리그/시즌 ties 전부 가져오기(필터는 파이썬에서)
    ties_rows: List[Dict[str, Any]] = fetch_all(
        """
        SELECT
            round_name,
            tie_key,
            team_a_id,
            team_b_id,
            leg1_fixture_id,
            leg2_fixture_id,
            leg1_home_id,
            leg1_away_id,
            leg1_home_ft,
            leg1_away_ft,
            leg1_date_utc,
            leg2_home_id,
            leg2_away_id,
            leg2_home_ft,
            leg2_away_ft,
            leg2_date_utc,
            agg_a,
            agg_b,
            winner_team_id
        FROM tournament_ties
        WHERE league_id = %s
          AND season = %s
        """,
        (league_id, season),
    )

    # 브라켓에 등장하는 모든 팀 id를 한 번에 모아서 teams에서 이름/로고 매핑
    team_ids: set[int] = set()
    for tr in ties_rows:
        for k in (
            "team_a_id",
            "team_b_id",
            "leg1_home_id",
            "leg1_away_id",
            "leg2_home_id",
            "leg2_away_id",
            "winner_team_id",
        ):
            v = tr.get(k)
            try:
                if v is None:
                    continue
                iv = int(v)
                if iv > 0:
                    team_ids.add(iv)
            except (TypeError, ValueError):
                continue

    team_map: Dict[int, Dict[str, Any]] = {}
    if team_ids:
        team_rows = fetch_all(
            """
            SELECT id, name, logo
            FROM teams
            WHERE id = ANY(%s)
            """,
            (list(team_ids),),
        )
        for r in team_rows:
            try:
                tid = int(r.get("id") or 0)
            except (TypeError, ValueError):
                continue
            if tid > 0:
                team_map[tid] = {
                    "name": r.get("name"),
                    "logo": r.get("logo"),
                }

    def _team_name_logo(tid: Any) -> Tuple[Optional[str], Optional[str]]:
        try:
            tid_i = int(tid) if tid is not None else 0
        except (TypeError, ValueError):
            tid_i = 0
        if tid_i <= 0:
            return (None, None)
        info = team_map.get(tid_i) or {}
        name = info.get("name")
        logo = info.get("logo")
        return (
            name if isinstance(name, str) and name.strip() else None,
            logo if isinstance(logo, str) and logo.strip() else None,
        )

    # canonical round별로 모으기
    by_canon: Dict[str, List[Dict[str, Any]]] = {}
    for r in ties_rows:
        raw_rn = (r.get("round_name") or "").strip()

        # 기존 정책 유지: "넉아웃으로 판단되는 라운드만 브라켓 포함"
        if not _is_knockout_round_for_bracket(league_id, raw_rn):
            continue

        canon = _canonical_round_key(raw_rn)
        if not canon:
            # 기존과 동일 정책: 우리가 분류 못하는 round_name은 제외
            continue

        idx = _CANON_ORDER_INDEX.get(canon)
        if idx is None:
            continue

        # ✅ 범위 필터: start ~ end
        if start_idx is not None and idx < start_idx:
            continue
        if end_idx is not None and idx > end_idx:
            continue

        by_canon.setdefault(canon, []).append(r)

    # 정렬 및 출력 변환
    bracket: List[Dict[str, Any]] = []

    for canon in _CANON_ORDER:
        if canon not in by_canon:
            continue

        ties_sorted = sorted(by_canon[canon], key=lambda x: str(x.get("tie_key") or ""))

        ties_out: List[Dict[str, Any]] = []
        for i, tr in enumerate(ties_sorted, start=1):
            legs: List[Dict[str, Any]] = []

            # leg1
            if tr.get("leg1_fixture_id") is not None:
                h_id = _coalesce_int(tr.get("leg1_home_id"), 0) or None
                a_id = _coalesce_int(tr.get("leg1_away_id"), 0) or None

                h_name, h_logo = _team_name_logo(h_id)
                a_name, a_logo = _team_name_logo(a_id)

                legs.append(
                    {
                        "leg_index": 1,
                        "fixture_id": _coalesce_int(tr.get("leg1_fixture_id"), 0) or None,
                        "date_utc": tr.get("leg1_date_utc"),
                        "home_id": h_id,
                        "away_id": a_id,
                        "home_ft": tr.get("leg1_home_ft"),
                        "away_ft": tr.get("leg1_away_ft"),
                        "home_name": h_name,
                        "home_logo": h_logo,
                        "away_name": a_name,
                        "away_logo": a_logo,
                    }
                )

            # leg2
            if tr.get("leg2_fixture_id") is not None:
                h_id = _coalesce_int(tr.get("leg2_home_id"), 0) or None
                a_id = _coalesce_int(tr.get("leg2_away_id"), 0) or None

                h_name, h_logo = _team_name_logo(h_id)
                a_name, a_logo = _team_name_logo(a_id)

                legs.append(
                    {
                        "leg_index": 2,
                        "fixture_id": _coalesce_int(tr.get("leg2_fixture_id"), 0) or None,
                        "date_utc": tr.get("leg2_date_utc"),
                        "home_id": h_id,
                        "away_id": a_id,
                        "home_ft": tr.get("leg2_home_ft"),
                        "away_ft": tr.get("leg2_away_ft"),
                        "home_name": h_name,
                        "home_logo": h_logo,
                        "away_name": a_name,
                        "away_logo": a_logo,
                    }
                )

            a_id = tr.get("team_a_id")
            b_id = tr.get("team_b_id")
            a_name, a_logo = _team_name_logo(a_id)
            b_name, b_logo = _team_name_logo(b_id)

            ties_out.append(
                {
                    "tie_key": tr.get("tie_key"),
                    "order_hint": i,
                    "team_a_id": a_id,
                    "team_b_id": b_id,
                    "team_a_name": a_name,
                    "team_a_logo": a_logo,
                    "team_b_name": b_name,
                    "team_b_logo": b_logo,
                    "agg_a": tr.get("agg_a"),
                    "agg_b": tr.get("agg_b"),
                    "winner_team_id": tr.get("winner_team_id"),
                    "legs": legs,
                }
            )

        round_label = _canonical_round_label(canon)
        round_key = round_label.upper().replace(" ", "_").replace("-", "_")

        bracket.append(
            {
                "round_key": round_key,
                "round_label": round_label,
                "ties": ties_out,
            }
        )

    return bracket







def build_standings_block(header: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Match Detail용 Standings 블록 (TABLE + BRACKET 하이브리드)

    ✅ 규칙:
    - 넉아웃(브라켓) 대상 fixture면: mode="BRACKET" + bracket 채워서 반환 (rows는 [])
    - 그 외 TABLE:
      1) standings 테이블 우선
      2) 비어있으면 matches로 계산
      3) finished=0이면 rows=[] + message

    ✅ A 방식(서버가 필터용 context_options 제공) 강화:
    - MLS(East/West)는 기존처럼 "전체 rows 유지 + 필터" (컷팅 없음)
    - Split Round(Championship/Relegation)도 "전체 rows 유지 + 필터"
    - Group A/B(예: Argentina)도 "전체 rows 유지 + 필터"
    - 따라서 "홈/원정 팀이 속한 group만 남기는 컷팅"은 제거
    - 중복 제거는 team_id 단독이 아니라 (team_id + group_name) 기준으로만 안전하게 수행
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

    season_resolved = _resolve_season(league_id_int, season if isinstance(season, int) else None)

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
    # 0) 넉아웃(브라켓) 경기면: tournament_ties 기반 BRACKET 응답 우선
    # ─────────────────────────────────────────────────────────────
    fixture_id = _extract_fixture_id_from_header(header)
    league_round = header.get("league_round")
    league_round_str = league_round.strip() if isinstance(league_round, str) else None

    if fixture_id is not None and _is_knockout_round_for_bracket(league_id_int, league_round_str):
        tie_row = _fetch_one(
            """
            SELECT round_name
            FROM tournament_ties
            WHERE league_id = %s
              AND season = %s
              AND (%s = leg1_fixture_id OR %s = leg2_fixture_id)
            LIMIT 1
            """,
            (league_id_int, season_resolved, fixture_id, fixture_id),
        )

        tie_round_name = (tie_row or {}).get("round_name")
        tie_round_name = tie_round_name.strip() if isinstance(tie_round_name, str) else None

        current_round = (
            tie_round_name
            if _is_knockout_round_for_bracket(league_id_int, tie_round_name)
            else league_round_str
        )

        bracket = _build_bracket_from_tournament_ties(
            league_id_int,
            season_resolved,
            start_round_name=None,
            end_round_name=current_round,
        )

        if bracket:
            return {
                "league": {
                    "league_id": league_id_int,
                    "season": season_resolved,
                    "name": league_name,
                },
                "mode": "BRACKET",
                "rows": [],
                "bracket": bracket,
                "context_options": {"conferences": [], "groups": []},
                "message": None,
                "source": "tournament_ties",
            }
        # bracket이 비면 TABLE로 fallback

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

    source = "standings_table" if rows_raw else "computed_from_matches"

    # ─────────────────────────────────────────────────────────────
    # 2) standings 비어 있으면 → matches에서 계산 (기존 로직 유지)
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
