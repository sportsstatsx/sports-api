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
        최신 시즌 제외 정책을 '진행중인 경우에만' 적용하기 위한 판정.
        - 해당 시즌에 미종료 경기가 1개라도 있으면 진행중(True)
        - 전부 Finished(FT/AET/PEN 포함)면 진행중(False)
        """
        try:
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
            return cnt > 0
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
    standings_champ_map: Dict[int, Dict[str, Any]] = {}
    try:
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

    # 2-C) 병합: tournament 우선
    merged: Dict[int, Dict[str, Any]] = {}
    for s, v in standings_champ_map.items():
        merged[s] = v
    for s, v in final_winner_map.items():
        merged[s] = v

    season_champions = [merged[s] for s in sorted(merged.keys(), reverse=True)]
    for c in season_champions:
        c.pop("_source", None)

    # 3) ✅ 최신 시즌 제외: "진행중일 때만" 제외
    try:
        latest_season = resolve_season_for_league(league_id, None)
    except Exception as e:
        latest_season = None
        print(f"[build_seasons_block] resolve_season_for_league ERROR league_id={league_id}: {e}")

    if latest_season is not None and len(season_champions) > 1:
        try:
            ls_int = int(latest_season)
        except Exception:
            ls_int = -1

        if ls_int > 0 and _is_season_in_progress(ls_int):
            season_champions = [
                c for c in season_champions
                if c.get("season") != latest_season
            ]

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



