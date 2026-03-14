from __future__ import annotations

import json

from typing import Any, Dict, List, Optional

from db import fetch_all


def build_seasons_block(league_id: int) -> Dict[str, Any]:
    """
    League Detail 화면의 'Seasons' 탭 + 기본 시즌 선택에 사용할 시즌 목록.

    정책:
    - 기본은 standings(rank=1) 우승팀
    - 토너먼트 타이 / 브라켓 기반 우승팀 결정은 더 이상 사용하지 않음
    - 컵/토너먼트 대회는 standings 우승 정보가 없을 수 있으므로 시즌 박스는 유지하되 챔피언 정보는 비워서 내려줌
    """

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
    # ✅ 실제로 "최종 챔피언"이 플레이오프/그랜드파이널 우승팀인 리그만 포함
    PLAYOFFS_CHAMPION = {
        188, 253,
        190, 254,  # A-League Women, NWSL Women
    }

    # ③ 정규시즌 챔프 + 플레이오프 챔프 (2개 존재)
    # ✅ 현재 지원 라이브리그 기준에서는 없음
    REGULAR_AND_PLAYOFFS = set()

    # ④ 플레이오프/스플릿이 있어도 “최종 챔피언은 standings 계열에서 결정”
    # - Scotland Premiership: split only
    # - Austria/Belgium/Czech/Denmark/K League 1: split + conference/europe/relegation playoff는 있어도
    #   그 playoff winner가 리그 챔피언은 아님
    PLAYOFFS_BUT_REGULAR_IS_CHAMPION = {
        179,
        218, 144, 345, 119, 292,
    }

    # ⑤ 토너먼트 (정규시즌 개념 없음) = 최종 우승
    KNOCKOUT_TOURNAMENT = {
        17, 16, 2, 848, 3,
        45,   # FA Cup
        143,  # Copa del Rey
        81,   # DFB Pokal
        137,  # Coppa Italia
        66,   # Coupe de France
    }

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


    def _is_season_in_progress(season_value: int) -> bool:
        """
        진행중 판정(강화판)

        1) matches 에서 미종료가 있으면 진행중(True)
        2) matches 에 미종료가 없어도, fixtures 에 미종료(예정/진행)가 있으면 진행중(True)
           (⚠️ 챔스/유로파처럼 matches 에는 끝난 경기만 있고,
               fixtures 에만 예정경기가 들어있는 케이스 방어)
        3) 둘 다 없으면 진행중(False)

        주의:
        - fixtures 테이블에 status_group 컬럼이 없을 수 있으니 fixtures에서는 status/status_short만 사용
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
                  )
                """,
                (league_id, season_value),
            )
            cnt = int((rows[0].get("cnt") if rows else 0) or 0)
            if cnt > 0:
                return True

            # 2) fixtures 기준 (status_group 사용 금지)
            rows2 = fetch_all(
                """
                SELECT COUNT(*) AS cnt
                FROM fixtures
                WHERE league_id = %s
                  AND season = %s
                  AND NOT (
                    coalesce(status,'') IN ('FT','AET','PEN')
                  )
                """,
                (league_id, season_value),
            )
            cnt2 = int((rows2[0].get("cnt") if rows2 else 0) or 0)
            return cnt2 > 0

        except Exception as e:
            print(
                f"[build_seasons_block] _is_season_in_progress ERROR "
                f"league_id={league_id} season={season_value}: {e}"
            )
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

    def _norm_text(v: Any) -> str:
        return str(v or "").strip().lower()

    def _candidate_bucket(group_name: str, description: str) -> str:
        g = str(group_name or "").strip().lower()
        d = str(description or "").strip().lower()
        x = f"{g} {d}".strip()

        if "releg" in x:
            return "relegation"

        # ✅ 상위 스플릿/챔피언십 단계
        # - 벨기에 Champ Rnd
        # - 오스트리아 Championship Round
        # - 덴마크 Championship Round
        # - 체코 Championship Group
        # 이건 "playoff champion"이 아니라
        # "최종 리그 테이블이 확정되는 상위 단계"로 별도 분리한다.
        if (
            "championship round" in g
            or "championship group" in g
            or "champ rnd" in g
            or "champ round" in g
            or "championship stage" in g
        ):
            return "championship_stage"

        if (
            "championship round" in d
            or "championship group" in d
            or "champ rnd" in d
            or "champ round" in d
            or "championship stage" in d
        ):
            return "championship_stage"

        # ✅ 진짜 playoff/group 이름이 명확한 경우만 playoff
        # (MLS, A-League 같은 "플레이오프 우승 = 챔피언" 리그용)
        if (
            "playoff" in g
            or "play-off" in g
            or "playoff" in d
            or "play-off" in d
        ):
            return "playoff"

        if "regular" in x or "table" in x or "overall" in x:
            return "regular"

        return "general"

    def _pick_best_candidate(
        items: List[Dict[str, Any]],
        preferred_buckets: List[str],
    ) -> Optional[Dict[str, Any]]:
        if not items:
            return None

        def _points_of(r: Dict[str, Any]) -> int:
            try:
                return int(r.get("points") or 0)
            except Exception:
                return 0

        enriched: List[Dict[str, Any]] = []
        for r in items:
            bucket = _candidate_bucket(
                str(r.get("group_name") or ""),
                str(r.get("description") or ""),
            )
            rr = dict(r)
            rr["_bucket"] = bucket
            enriched.append(rr)

        preferred = [r for r in enriched if r.get("_bucket") in preferred_buckets]
        if not preferred:
            preferred = [r for r in enriched if r.get("_bucket") != "relegation"]

        if not preferred:
            preferred = enriched

        bucket_order = {name: idx for idx, name in enumerate(preferred_buckets)}

        best = sorted(
            preferred,
            key=lambda r: (
                bucket_order.get(str(r.get("_bucket")), 999),
                -_points_of(r),
                str(r.get("team_name") or "").lower(),
            ),
        )[0]
        return best

    def _winner_team_id_from_fixture_raw(
        fixture_id: int,
        home_id: Any,
        away_id: Any,
        home_ft: Any,
        away_ft: Any,
    ) -> Optional[int]:
        try:
            rows = fetch_all(
                """
                SELECT data_json
                FROM match_fixtures_raw
                WHERE fixture_id = %s
                LIMIT 1
                """,
                (fixture_id,),
            )
            raw = rows[0].get("data_json") if rows else None

            if isinstance(raw, str) and raw.strip():
                root = json.loads(raw)

                teams = root.get("teams") if isinstance(root, dict) else {}
                if isinstance(teams, dict):
                    home = teams.get("home") if isinstance(teams.get("home"), dict) else {}
                    away = teams.get("away") if isinstance(teams.get("away"), dict) else {}

                    if home.get("winner") is True:
                        try:
                            return int(home_id)
                        except Exception:
                            return None

                    if away.get("winner") is True:
                        try:
                            return int(away_id)
                        except Exception:
                            return None
        except Exception:
            pass

        try:
            h = int(home_ft) if home_ft is not None else None
            a = int(away_ft) if away_ft is not None else None
        except Exception:
            h = None
            a = None

        if h is not None and a is not None:
            try:
                if h > a:
                    return int(home_id)
                if a > h:
                    return int(away_id)
            except Exception:
                return None

        return None

    def _round_priority(round_name: str) -> int:
        lo = _norm_text(round_name)

        if not lo:
            return 0

        if (
            ("final" == lo)
            or lo.endswith(" final")
            or lo.endswith(" finals")
            or "grand final" in lo
            or "playoff final" in lo
            or "play-off final" in lo
        ) and "semi" not in lo and "quarter" not in lo:
            return 1000

        if "semi" in lo:
            return 900

        if "quarter" in lo:
            return 800

        if "round of 16" in lo:
            return 700
        if "round of 32" in lo:
            return 650
        if "round of 64" in lo:
            return 600
        if "round of 128" in lo:
            return 550

        if "playoff" in lo or "play-off" in lo:
            return 500

        return 100

    def _build_final_winner_map() -> Dict[int, Dict[str, Any]]:
        out: Dict[int, Dict[str, Any]] = {}

        try:
            rows = fetch_all(
                """
                SELECT
                    m.season,
                    m.fixture_id,
                    COALESCE(m.league_round, '') AS league_round,
                    m.date_utc,
                    m.home_id,
                    m.away_id,
                    m.home_ft,
                    m.away_ft,
                    COALESCE(th.name, '') AS home_name,
                    th.logo AS home_logo,
                    COALESCE(ta.name, '') AS away_name,
                    ta.logo AS away_logo
                FROM matches AS m
                LEFT JOIN teams AS th
                  ON th.id = m.home_id
                LEFT JOIN teams AS ta
                  ON ta.id = m.away_id
                WHERE m.league_id = %s
                  AND COALESCE(m.league_round, '') <> ''
                  AND (
                    lower(coalesce(m.status_group,'')) = 'finished'
                    OR coalesce(m.status,'') IN ('FT','AET','PEN')
                  )
                ORDER BY
                    m.season DESC,
                    m.date_utc DESC NULLS LAST,
                    m.fixture_id DESC
                """,
                (league_id,),
            )
        except Exception as e:
            print(f"[build_seasons_block] FINAL WINNER QUERY ERROR league_id={league_id}: {e}")
            return out

        by_season: Dict[int, Dict[str, List[Dict[str, Any]]]] = {}
        for r in rows or []:
            sv = r.get("season")
            if sv is None:
                continue
            try:
                s_int = int(sv)
            except Exception:
                continue

            round_name = str(r.get("league_round") or "").strip()
            if not round_name:
                continue

            by_season.setdefault(s_int, {}).setdefault(round_name, []).append(r)

        for s_int, rounds_map in by_season.items():
            if not rounds_map:
                continue

            selected_round_name = sorted(
                rounds_map.keys(),
                key=lambda rn: (-_round_priority(rn), rn.lower()),
            )[0]

            selected_matches = rounds_map.get(selected_round_name) or []
            picked: Optional[Dict[str, Any]] = None

            for m in selected_matches:
                fixture_id = m.get("fixture_id")
                if fixture_id is None:
                    continue

                try:
                    fixture_id_int = int(fixture_id)
                except Exception:
                    continue

                winner_team_id = _winner_team_id_from_fixture_raw(
                    fixture_id=fixture_id_int,
                    home_id=m.get("home_id"),
                    away_id=m.get("away_id"),
                    home_ft=m.get("home_ft"),
                    away_ft=m.get("away_ft"),
                )

                if winner_team_id is None:
                    continue

                try:
                    home_id_int = int(m.get("home_id")) if m.get("home_id") is not None else None
                except Exception:
                    home_id_int = None

                try:
                    away_id_int = int(m.get("away_id")) if m.get("away_id") is not None else None
                except Exception:
                    away_id_int = None

                if winner_team_id == home_id_int:
                    picked = {
                        "season": s_int,
                        "team_id": winner_team_id,
                        "team_name": m.get("home_name") or "",
                        "team_logo": m.get("home_logo"),
                        "points": None,
                        "_source": "final_round_winner",
                    }
                    break

                if winner_team_id == away_id_int:
                    picked = {
                        "season": s_int,
                        "team_id": winner_team_id,
                        "team_name": m.get("away_name") or "",
                        "team_logo": m.get("away_logo"),
                        "points": None,
                        "_source": "final_round_winner",
                    }
                    break

            if picked:
                out[s_int] = picked

        return out

    final_winner_map: Dict[int, Dict[str, Any]] = _build_final_winner_map()

    # 2-B) standings(rank=1) 기반 후보 수집
    standings_regular_map: Dict[int, Dict[str, Any]] = {}
    standings_playoff_map: Dict[int, Dict[str, Any]] = {}

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

        candidates_by_season: Dict[int, List[Dict[str, Any]]] = {}
        for r in champ_rows:
            sv = r.get("season")
            if sv is None:
                continue
            s_int = int(sv)
            candidates_by_season.setdefault(s_int, []).append(r)

        for s_int, items in candidates_by_season.items():
            # ✅ 최종 챔피언용 standings 후보:
            # - split 리그에서는 Championship Round 1위를 우선
            # - 없으면 Regular / Overall / 일반 단일테이블 1위
            # - Relegation 은 제외
            regular_best = _pick_best_candidate(
                items,
                ["championship_stage", "regular", "general", "playoff"],
            )
            if regular_best:
                standings_regular_map[s_int] = {
                    "season": s_int,
                    "team_id": regular_best.get("team_id"),
                    "team_name": regular_best.get("team_name") or "",
                    "team_logo": regular_best.get("team_logo"),
                    "points": regular_best.get("points"),
                    "_source": "standings_rank1_regular",
                }

            playoff_best = _pick_best_candidate(
                [
                    r for r in items
                    if _candidate_bucket(
                        str(r.get("group_name") or ""),
                        str(r.get("description") or ""),
                    ) == "playoff"
                ],
                ["playoff"],
            )
            if playoff_best:
                standings_playoff_map[s_int] = {
                    "season": s_int,
                    "team_id": playoff_best.get("team_id"),
                    "team_name": playoff_best.get("team_name") or "",
                    "team_logo": playoff_best.get("team_logo"),
                    "points": playoff_best.get("points"),
                    "_source": "standings_rank1_playoff",
                }

    except Exception as e:
        print(f"[build_seasons_block] CHAMPIONS ERROR league_id={league_id}: {e}")
        standings_regular_map = {}
        standings_playoff_map = {}

    # 기존 변수명 호환용
    standings_champ_map = standings_regular_map

    # 2-C) ✅ 리그 타입별 챔피언 결정
    mode = _champion_mode(league_id)

    merged_rows: List[Dict[str, Any]] = []

    if mode == "regular":
        for s in sorted(standings_regular_map.keys(), reverse=True):
            v = standings_regular_map[s].copy()
            v["champion_type"] = "regular_season"
            v.pop("_source", None)
            merged_rows.append(v)

    elif mode == "playoffs":
        playoff_source_keys = sorted(
            set(list(final_winner_map.keys()) + list(standings_playoff_map.keys())),
            reverse=True,
        )
        for s in playoff_source_keys:
            picked = final_winner_map.get(s) or standings_playoff_map.get(s)
            if not picked:
                continue
            v = picked.copy()
            v["champion_type"] = "playoffs"
            v.pop("_source", None)
            merged_rows.append(v)

    elif mode == "regular+playoffs":
        all_seasons = sorted(
            set(
                list(standings_regular_map.keys())
                + list(standings_playoff_map.keys())
                + list(final_winner_map.keys())
            ),
            reverse=True,
        )
        for s in all_seasons:
            if s in standings_regular_map:
                v = standings_regular_map[s].copy()
                v["champion_type"] = "regular_season"
                v.pop("_source", None)
                merged_rows.append(v)

            picked_po = final_winner_map.get(s) or standings_playoff_map.get(s)
            if picked_po:
                v = picked_po.copy()
                v["champion_type"] = "playoffs"
                v.pop("_source", None)
                merged_rows.append(v)

    elif mode == "knockout":
        for s in sorted(final_winner_map.keys(), reverse=True):
            v = final_winner_map[s].copy()
            v["champion_type"] = "tournament"
            v.pop("_source", None)
            merged_rows.append(v)

    else:
        all_seasons = sorted(
            set(
                list(standings_regular_map.keys())
                + list(standings_playoff_map.keys())
                + list(final_winner_map.keys())
            ),
            reverse=True,
        )
        for s in all_seasons:
            if s in standings_regular_map:
                v = standings_regular_map[s].copy()
                v["champion_type"] = "regular_season"
                v.pop("_source", None)
                merged_rows.append(v)
            else:
                picked_po = final_winner_map.get(s) or standings_playoff_map.get(s)
                if picked_po:
                    v = picked_po.copy()
                    v["champion_type"] = "playoffs"
                    v.pop("_source", None)
                    merged_rows.append(v)

    # ---------------------------------------------------------
    # ✅ NEW POLICY:
    # - 최신 시즌도 포함해서 "박스(Season row)"는 항상 표시
    # - 단, 해당 시즌이 진행중이면(team/로고) 챔피언 정보는 비워서 내려준다
    #   (즉, 시즌 확정되면 그때부터 팀명이 표시됨)
    # ---------------------------------------------------------

    # merged_rows 는 "결과 후보" (standings 기반) 목록
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
            # ✅ 토너먼트/PO는 "남은 경기"로 판단 금지
            # ✅ 우승 확정 = merged 결과에 team_id가 존재하는가
            key = (s_int, "playoffs")
            picked = merged_by_key.get(key)
            winner_confirmed = bool(picked and picked.get("team_id"))
            if winner_confirmed:
                season_champions_final.append(picked)
            else:
                season_champions_final.append(_blank_row(s_int, "playoffs"))

        elif mode == "knockout":
            # ✅ 챔스/유로파 등: 우승 확정 = tournament winner(team_id) 존재
            key = (s_int, "tournament")
            picked = merged_by_key.get(key)
            winner_confirmed = bool(picked and picked.get("team_id"))
            if winner_confirmed:
                season_champions_final.append(picked)
            else:
                season_champions_final.append(_blank_row(s_int, "tournament"))

        elif mode == "regular+playoffs":
            key_reg = (s_int, "regular_season")
            key_po  = (s_int, "playoffs")

            # regular은 기존 정책 유지 (진행중이면 블랭크)
            if in_progress:
                season_champions_final.append(_blank_row(s_int, "regular_season"))
            else:
                season_champions_final.append(merged_by_key.get(key_reg) or _blank_row(s_int, "regular_season"))

            # ✅ playoffs는 "winner 확정 여부"로만 블랭크
            picked_po = merged_by_key.get(key_po)
            po_winner_confirmed = bool(picked_po and picked_po.get("team_id"))
            if po_winner_confirmed:
                season_champions_final.append(picked_po)
            else:
                season_champions_final.append(_blank_row(s_int, "playoffs"))

        else:
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



