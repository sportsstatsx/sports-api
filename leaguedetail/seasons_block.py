from __future__ import annotations

from typing import Any, Dict, List, Optional

from db import fetch_all


def build_seasons_block(league_id: int) -> Dict[str, Any]:
    """
    League Detail 화면의 'Seasons' 탭 + 기본 시즌 선택에 사용할 시즌 목록.

    정책:
    - 기본은 standings(rank=1) 우승팀
    - 단, 플레이오프/브라켓 리그의 경우 tournament_ties의 "최종 라운드 winner"를 우선 우승팀으로 사용
      (round_name 표기 변형이 많으므로, canonical 정규화로 Final을 안정적으로 인식)
    - ✅ 최신 시즌(latest_season)은 "진행중인 경우에만" 우승팀 목록에서 제외
      (다음 시즌이 아직 시작 전이면 latest_season=직전 종료시즌이 될 수 있으므로, 종료시즌은 제외하면 안 됨)
    """
    import re  # ✅ 누락 방지: 이 함수 안에서 re 사용하므로 내부 import로 안전 고정

    seasons: List[int] = []
    season_champions: List[Dict[str, Any]] = []

    # ─────────────────────────────────────────────
    # Champion decision types (league_id based)
    # ─────────────────────────────────────────────
    # ① 정규시즌 1위 = 챔피언
    REGULAR_SEASON_CHAMPION = {
        189, 219, 145, 71, 72, 169, 346, 40, 39, 61, 62, 79, 78, 290, 135, 136,
        98, 99, 89, 88, 106, 107, 94, 95, 305, 307, 180, 293, 140, 141, 208,
        207, 204, 203,
    }

    # ② 플레이오프 우승 = 챔피언
    PLAYOFFS_CHAMPION = {188, 253}

    # ③ 정규시즌 챔프 + 플레이오프 챔프 (2개 존재)
    REGULAR_AND_PLAYOFFS = {218, 144, 345, 119, 292}

    # ④ 플레이오프/스플릿이 있어도 “챔피언은 정규시즌 1위”
    # (너가 준 분류 기준)
    PLAYOFFS_BUT_REGULAR_IS_CHAMPION = {179}

    # ⑤ 토너먼트 (정규시즌 개념 없음) = 최종 우승
    KNOCKOUT_TOURNAMENT = {17, 16, 2, 848, 3}

    def _champion_mode(lid: int) -> str:
        if lid in KNOCKOUT_TOURNAMENT:
            return "knockout"
        if lid in REGULAR_AND_PLAYOFFS:
            return "regular+playoffs"
        if lid in PLAYOFFS_CHAMPION:
            return "playoffs"
        if lid in PLAYOFFS_BUT_REGULAR_IS_CHAMPION:
            return "regular"
        if lid in REGULAR_SEASON_CHAMPION:
            return "regular"
        # 안전 폴백: 기본은 정규시즌(standings) 우선
        return "regular"


    # ─────────────────────────────────────────────
    # local helpers
    # ─────────────────────────────────────────────
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
        s = _norm_round_name(raw_round_name)
        if not s:
            return None

        # Final 계열
        if re.fullmatch(r"finals?", s) or re.fullmatch(r"grand final(s)?", s):
            return "final"

        # Semi / Quarter
        if re.fullmatch(r"semi finals?", s) or re.fullmatch(r"semifinals?", s):
            return "semi"
        if re.fullmatch(r"quarter finals?", s) or re.fullmatch(r"quarterfinals?", s):
            return "quarter"

        # Last N / Round of N / 1/8 Finals
        m = re.fullmatch(r"last (\d+)", s)
        if m:
            return f"r{int(m.group(1))}"

        m = re.fullmatch(r"round of (\d+)", s)
        if m:
            return f"r{int(m.group(1))}"

        m = re.fullmatch(r"1\s*/\s*(\d+)\s*finals?", s)
        if m:
            return f"r{int(m.group(1)) * 2}"

        # 1st/2nd/3rd/4th Round
        m = re.fullmatch(r"(\d+)(st|nd|rd|th) round", s)
        if m:
            return f"round_{m.group(1)}"

        # Play-in / Playoffs
        if "play in" in s or "playin" in s or "wild card" in s or "wildcard" in s:
            return "play_in"
        if "knockout round" in s and ("playoff" in s or "play off" in s):
            return "knockout_playoffs"
        if "playoff" in s or "play off" in s or "playoffs" in s or "play offs" in s:
            return "playoffs"

        # elimination / preliminary / qualifying
        if "elimination" in s:
            if "final" in s:
                return "elimination_final"
            return "elimination"
        if "preliminary" in s:
            return "preliminary"
        if "qualifying" in s or "qualifier" in s:
            return "qualifying"

        return None

    CANON_ORDER: List[str] = [
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
    CANON_ORDER_INDEX = {k: i for i, k in enumerate(CANON_ORDER)}

    def _is_season_in_progress(season_value: int) -> bool:
    """
    진행중 판정(강화판)

    1) matches 에서 미종료가 있으면 진행중(True)
    2) matches 에 미종료가 없어도, fixtures 에 미종료가 있으면 진행중(True)
       (⚠️ 챔스/유로파처럼 matches 에는 끝난 경기만 있고,
           fixtures 에만 예정경기가 들어있는 케이스 방어)
    3) 둘 다 없으면 진행중(False)
    """
    try:
        # 1) matches 기준
        rows = fetch_all(
            """
            SELECT COUNT(*) AS cnt
            FROM matches
            WHERE league_id = %s
              AND season = %s
              AND NOT (
                lower(coalesce(status_group,'')) = 'finished'
                OR coalesce(status,'') IN ('FT','AET','PEN')
                OR coalesce(status_short,'') IN ('FT','AET','PEN')
              )
            """,
            (league_id, season_value),
        )
        cnt = int((rows[0].get("cnt") if rows else 0) or 0)
        if cnt > 0:
            return True

        # 2) fixtures 기준(예정경기/진행중 경기 존재 여부)
        rows2 = fetch_all(
            """
            SELECT COUNT(*) AS cnt
            FROM fixtures
            WHERE league_id = %s
              AND season = %s
              AND NOT (
                lower(coalesce(status_group,'')) = 'finished'
                OR coalesce(status,'') IN ('FT','AET','PEN')
                OR coalesce(status_short,'') IN ('FT','AET','PEN')
              )
            """,
            (league_id, season_value),
        )
        cnt2 = int((rows2[0].get("cnt") if rows2 else 0) or 0)
        return cnt2 > 0

    except Exception as e:
        print(f"[build_seasons_block] _is_season_in_progress ERROR league_id={league_id} season={season_value}: {e}")
        return False


    # 1) 사용 가능한 시즌 목록 (matches 기준)
    try:
        rows = fetch_all(
            """
            SELECT DISTINCT season
            FROM matches
            WHERE league_id = %s
            ORDER BY season DESC
            """,
            (league_id,),
        )
        seasons = [int(r["season"]) for r in rows if r.get("season") is not None]
    except Exception as e:
        print(f"[build_seasons_block] ERROR league_id={league_id}: {e}")
        seasons = []

    # 2-A) tournament_ties 기반 우승팀 후보(있으면 우선)
    final_winner_map: Dict[int, Dict[str, Any]] = {}
    try:
        tie_rows = fetch_all(
            """
            SELECT
                tt.season,
                tt.round_name,
                tt.winner_team_id AS team_id,
                COALESCE(t.name, '') AS team_name,
                t.logo AS team_logo,
                COALESCE(tt.leg2_date_utc, tt.leg1_date_utc) AS tie_date_utc
            FROM tournament_ties AS tt
            LEFT JOIN teams AS t
              ON t.id = tt.winner_team_id
            WHERE tt.league_id = %s
              AND tt.winner_team_id IS NOT NULL
            ORDER BY
              tt.season DESC,
              COALESCE(tt.leg2_date_utc, tt.leg1_date_utc) DESC NULLS LAST
            """,
            (league_id,),
        )

        candidates_by_season: Dict[int, List[Dict[str, Any]]] = {}
        for r in tie_rows:
            sv = r.get("season")
            if sv is None:
                continue
            s_int = int(sv)

            canon = _canonical_round_key(r.get("round_name"))
            candidates_by_season.setdefault(s_int, []).append(
                {
                    "season": s_int,
                    "canon": canon,
                    "canon_idx": CANON_ORDER_INDEX.get(canon) if canon else None,
                    "team_id": r.get("team_id"),
                    "team_name": r.get("team_name") or "",
                    "team_logo": r.get("team_logo"),
                    "tie_date_utc": r.get("tie_date_utc"),
                }
            )

        for s_int, items in candidates_by_season.items():
            pick: Optional[Dict[str, Any]] = None

            finals = [x for x in items if x.get("canon") == "final"]
            if finals:
                finals.sort(
                    key=lambda x: (
                        x.get("tie_date_utc") is not None,
                        x.get("tie_date_utc"),
                    ),
                    reverse=True,
                )
                pick = finals[0]
            else:
                known = [x for x in items if isinstance(x.get("canon_idx"), int)]
                if known:
                    known.sort(
                        key=lambda x: (
                            x["canon_idx"],
                            x.get("tie_date_utc") is not None,
                            x.get("tie_date_utc"),
                        ),
                        reverse=True,
                    )
                    pick = known[0]
                else:
                    pick = items[0] if items else None

            if pick:
                final_winner_map[s_int] = {
                    "season": s_int,
                    "team_id": pick.get("team_id"),
                    "team_name": pick.get("team_name") or "",
                    "team_logo": pick.get("team_logo"),
                    "points": None,
                    "_source": "tournament_winner",
                }

    except Exception as e:
        print(f"[build_seasons_block] TOURNAMENT WINNER ERROR league_id={league_id}: {e}")
        final_winner_map = {}

    # 2-B) standings(rank=1) fallback
    # ✅ split 리그(Champ Rnd / Releg Rnd 등)에서 rank=1 후보가 여러 개 생기므로
    #    "Championship(Champ Rnd)"를 Regular Season Champion으로 우선 선택하고
    #    "Relegation(Releg Rnd)"는 뒤로 밀어버린다.
    standings_champ_map: Dict[int, Dict[str, Any]] = {}
    try:
        # 1) 가능한 경우 group_name/description까지 같이 받아서 우선순위 결정
        try:
            champ_rows = fetch_all(
                """
                SELECT
                    s.season,
                    s.team_id,
                    COALESCE(t.name, '') AS team_name,
                    t.logo AS team_logo,
                    s.points,
                    COALESCE(s.group_name, '') AS group_name,
                    COALESCE(s.description, '') AS description
                FROM standings AS s
                LEFT JOIN teams AS t
                  ON t.id = s.team_id
                WHERE s.league_id = %s
                  AND s.rank = 1
                ORDER BY s.season DESC;
                """,
                (league_id,),
            )

            def _stage_priority(g: str, d: str) -> int:
                x = f"{g} {d}".strip().lower()
                # ✅ Champ Rnd 우선
                if "champ" in x or "championship" in x:
                    return 1
                # ✅ Regular/Table/Overall 다음
                if "regular" in x or "table" in x or "overall" in x:
                    return 2
                # ✅ Releg Rnd는 최하위
                if "releg" in x:
                    return 99
                return 50

            # season 별로 rank=1 후보들 중 "가장 적절한" 1개만 pick
            candidates_by_season: Dict[int, List[Dict[str, Any]]] = {}
            for r in champ_rows:
                sv = r.get("season")
                if sv is None:
                    continue
                s_int = int(sv)
                candidates_by_season.setdefault(s_int, []).append(r)

            for s_int, items in candidates_by_season.items():
                # priority 낮을수록 우선, 동률이면 points 높은 쪽 우선
                best = sorted(
                    items,
                    key=lambda r: (
                        _stage_priority(r.get("group_name") or "", r.get("description") or ""),
                        -(int(r.get("points") or 0)),
                    )
                )[0]

                standings_champ_map[s_int] = {
                    "season": s_int,
                    "team_id": best.get("team_id"),
                    "team_name": best.get("team_name") or "",
                    "team_logo": best.get("team_logo"),
                    "points": best.get("points"),
                    "_source": "standings_rank1",
                }

        except Exception:
            # 2) group_name 컬럼이 없거나 실패하면 기존 방식으로 폴백
            champ_rows = fetch_all(
                """
                SELECT DISTINCT ON (s.season)
                    s.season,
                    s.team_id,
                    COALESCE(t.name, '') AS team_name,
                    t.logo AS team_logo,
                    s.points
                FROM standings AS s
                LEFT JOIN teams AS t
                  ON t.id = s.team_id
                WHERE s.league_id = %s
                  AND s.rank = 1
                ORDER BY s.season DESC, s.rank ASC;
                """,
                (league_id,),
            )

            for r in champ_rows:
                sv = r.get("season")
                if sv is None:
                    continue
                s_int = int(sv)
                standings_champ_map[s_int] = {
                    "season": s_int,
                    "team_id": r.get("team_id"),
                    "team_name": r.get("team_name") or "",
                    "team_logo": r.get("team_logo"),
                    "points": r.get("points"),
                    "_source": "standings_rank1",
                }

    except Exception as e:
        print(f"[build_seasons_block] CHAMPIONS ERROR league_id={league_id}: {e}")
        standings_champ_map = {}


    # 2-C) ✅ 리그 타입별 챔피언 결정
    mode = _champion_mode(league_id)

    merged_rows: List[Dict[str, Any]] = []

    if mode == "regular":
        # 정규시즌 1위만
        for s in sorted(standings_champ_map.keys(), reverse=True):
            v = standings_champ_map[s].copy()
            v["champion_type"] = "regular_season"
            v.pop("_source", None)
            merged_rows.append(v)

    elif mode == "playoffs":
        # 플레이오프 우승만
        for s in sorted(final_winner_map.keys(), reverse=True):
            v = final_winner_map[s].copy()
            v["champion_type"] = "playoffs"
            v.pop("_source", None)
            merged_rows.append(v)

    elif mode == "regular+playoffs":
        # 한 시즌에 2개: 정규(standings) + PO(tournament)
        all_seasons = sorted(
            set(list(standings_champ_map.keys()) + list(final_winner_map.keys())),
            reverse=True,
        )
        for s in all_seasons:
            if s in standings_champ_map:
                v = standings_champ_map[s].copy()
                v["champion_type"] = "regular_season"
                v.pop("_source", None)
                merged_rows.append(v)
            if s in final_winner_map:
                v = final_winner_map[s].copy()
                v["champion_type"] = "playoffs"
                v.pop("_source", None)
                merged_rows.append(v)

    elif mode == "knockout":
        # 토너먼트는 최종 우승만 (tournament winner)
        for s in sorted(final_winner_map.keys(), reverse=True):
            v = final_winner_map[s].copy()
            v["champion_type"] = "tournament"
            v.pop("_source", None)
            merged_rows.append(v)

    else:
        # 혹시 모를 폴백: 기존과 달리 "정규 우선, 없으면 PO"
        all_seasons = sorted(
            set(list(standings_champ_map.keys()) + list(final_winner_map.keys())),
            reverse=True,
        )
        for s in all_seasons:
            if s in standings_champ_map:
                v = standings_champ_map[s].copy()
                v["champion_type"] = "regular_season"
                v.pop("_source", None)
                merged_rows.append(v)
            elif s in final_winner_map:
                v = final_winner_map[s].copy()
                v["champion_type"] = "playoffs"
                v.pop("_source", None)
                merged_rows.append(v)

    # ---------------------------------------------------------
    # ✅ NEW POLICY:
    # - 최신 시즌도 포함해서 "박스(Season row)"는 항상 표시
    # - 단, 해당 시즌이 진행중이면(team/로고) 챔피언 정보는 비워서 내려준다
    #   (즉, 시즌 확정되면 그때부터 팀명이 표시됨)
    # ---------------------------------------------------------

    # merged_rows 는 "결과 후보" (standings/tournament_ties 기반) 목록
    # → 이를 season/champion_type 기준으로 빠르게 찾을 수 있게 맵으로 만든다.
    merged_by_key: Dict[tuple, Dict[str, Any]] = {}
    for r in merged_rows:
        try:
            s_int = int(r.get("season") or 0)
        except Exception:
            continue
        ctype = (r.get("champion_type") or "").strip()
        if s_int > 0 and ctype:
            merged_by_key[(s_int, ctype)] = r

    def _blank_row(season_value: int, champion_type: str) -> Dict[str, Any]:
        return {
            "season": int(season_value),
            "team_id": None,
            "team_name": "",
            "team_logo": None,
            "points": None,
            "champion_type": champion_type,
        }

    # 시즌 목록(seasons)은 matches 기준으로 이미 최신까지 포함됨
    # → 여기서 "항상 박스 row 생성"을 보장한다.
    season_champions_final: List[Dict[str, Any]] = []

    for s in seasons:
        try:
            s_int = int(s)
        except Exception:
            continue
        if s_int <= 0:
            continue

        in_progress = _is_season_in_progress(s_int)

        if mode == "regular":
            key = (s_int, "regular_season")
            if in_progress:
                season_champions_final.append(_blank_row(s_int, "regular_season"))
            else:
                season_champions_final.append(merged_by_key.get(key) or _blank_row(s_int, "regular_season"))

        elif mode == "playoffs":
            key = (s_int, "playoffs")
            if in_progress:
                season_champions_final.append(_blank_row(s_int, "playoffs"))
            else:
                season_champions_final.append(merged_by_key.get(key) or _blank_row(s_int, "playoffs"))

        elif mode == "knockout":
            key = (s_int, "tournament")
            if in_progress:
                season_champions_final.append(_blank_row(s_int, "tournament"))
            else:
                season_champions_final.append(merged_by_key.get(key) or _blank_row(s_int, "tournament"))

        elif mode == "regular+playoffs":
            # ✅ 한 시즌에 2개 박스(정규 + PO) 모두 항상 표시
            key_reg = (s_int, "regular_season")
            key_po  = (s_int, "playoffs")

            if in_progress:
                season_champions_final.append(_blank_row(s_int, "regular_season"))
                season_champions_final.append(_blank_row(s_int, "playoffs"))
            else:
                season_champions_final.append(merged_by_key.get(key_reg) or _blank_row(s_int, "regular_season"))
                season_champions_final.append(merged_by_key.get(key_po)  or _blank_row(s_int, "playoffs"))

        else:
            # 폴백: 정규 1개만
            key = (s_int, "regular_season")
            if in_progress:
                season_champions_final.append(_blank_row(s_int, "regular_season"))
            else:
                season_champions_final.append(merged_by_key.get(key) or _blank_row(s_int, "regular_season"))

    season_champions = season_champions_final

    return {
        "league_id": league_id,
        "seasons": seasons,
        "season_champions": season_champions,
    }







def resolve_season_for_league(league_id: int, season: Optional[int]) -> Optional[int]:
    """
    ✅ 완전무결(절대 흔들리지 않게) 시즌 결정 규칙

    1) season 파라미터가 들어온 경우:
       - (league_id, season) 이 DB에 존재하면 그대로 사용
       - 존재하지 않으면 "해당 리그의 최신 시즌(MAX(season))" 으로 강제 보정
         (예: 2027, 3000, 기타 어떤 값이 와도 절대 흔들리지 않게)

    2) season 파라미터가 없는 경우(None):
       - "해당 리그의 최신 시즌(MAX(season))" 을 기본으로 사용

    ⚠️ 이유:
    - finished_cnt 기반 폴백은 캘린더 시즌 리그(MLS/K리그/J리그/브라질/아르헨 등)에서
      시즌 시작 전/초기에 오히려 최신 시즌(일정/경기)을 못 보게 만드는 역효과가 큼.
    - “우승팀/통계는 이전 시즌을 보여주자” 같은 정책은
      기본 season 결정이 아니라 각 블록에서 별도 정책으로 처리하는 게 안전함.
    """

    def _safe_int(v: Any) -> int:
        try:
            return int(v)
        except Exception:
            return 0

    def _season_exists(s: int) -> bool:
        if s <= 0:
            return False
        try:
            hit = fetch_all(
                """
                SELECT 1
                FROM matches
                WHERE league_id = %s
                  AND season = %s
                LIMIT 1
                """,
                (league_id, s),
            )
            return bool(hit)
        except Exception:
            return False

    def _latest_season() -> Optional[int]:
        try:
            rows = fetch_all(
                """
                SELECT MAX(season) AS max_season
                FROM matches
                WHERE league_id = %s
                """,
                (league_id,),
            )
            if not rows:
                return None
            ms = _safe_int(rows[0].get("max_season"))
            return ms if ms > 0 else None
        except Exception as e:
            print(f"[resolve_season_for_league] latest season query ERROR league_id={league_id}: {e}")
            return None

    # 1) season 파라미터가 오면: 존재 검증 → 없으면 최신 시즌으로 보정
    if season is not None:
        s = _safe_int(season)

        # 비정상 값 방어 (예: 20262027 같은 값)
        if 0 < s <= 3000 and _season_exists(s):
            return s

        # ✅ 핵심: 존재하지 않으면 무조건 "최신 시즌"으로 고정 보정
        return _latest_season()

    # 2) season 파라미터가 없으면: 최신 시즌
    return _latest_season()



