# hockey/services/hockey_standings_service.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

from hockey.hockey_db import hockey_fetch_all, hockey_fetch_one
from hockey.regular_season_config import get_regular_season_start_utc


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


# ─────────────────────────────────────────
# 정식 정렬 규칙(고정)
# 1) stage 우선순위: Regular -> Playoffs/Post -> Pre -> 기타(알파벳)
# 2) group 우선순위(동일 stage): Division(기본) -> Conference -> Overall -> 기타
#    (NHL 기본 정렬 포함: Atlantic/Metropolitan/Central/Pacific, Eastern/Western)
# ─────────────────────────────────────────
def _stage_rank(stage: str) -> tuple[int, str]:
    s0 = (stage or "").strip().lower()

    if "regular" in s0:
        return (1, s0)
    if "playoff" in s0 or "post" in s0 or "final" in s0:
        return (2, s0)
    if "pre" in s0 or "exhibition" in s0:
        return (3, s0)
    return (9, s0)


def _group_rank(group_name: str) -> tuple[int, str]:
    g0 = (group_name or "").strip().lower()

    # 기본: 디비전 먼저
    if "division" in g0:
        # NHL 디비전 정식 순서 고정
        order = {
            "atlantic division": 1,
            "metropolitan division": 2,
            "central division": 3,
            "pacific division": 4,
        }
        return (1, str(order.get(g0, 99)).zfill(2) + "_" + g0)

    if "conference" in g0:
        order = {
            "eastern conference": 1,
            "western conference": 2,
        }
        return (2, str(order.get(g0, 99)).zfill(2) + "_" + g0)

    # 전체/통합 느낌(리그마다 다르니 최소 규칙만)
    if g0 in ("overall", "all", "total") or "overall" in g0:
        return (3, g0)

    return (9, g0)

def _is_finished_status(status: Any) -> bool:
    s = (status or "")
    s = str(s).strip().upper()
    return s in ("FT", "AOT", "AP", "AET", "PEN", "FIN", "END", "ENDED")


def _build_regular_stats_map_from_games(
    league_id: int,
    season: int,
    regular_start_utc: datetime,
) -> Dict[int, Dict[str, Optional[int]]]:
    """
    정규시즌 시작(UTC) 이후의 '종료 경기'만으로 팀별 누적 스탯을 재계산한다.

    ✅ 표기 정책(중요):
    - wins/losses는 "정규(레귤러 타임) 승/패" 로 반환 (OTW/OTL은 별도)
      => wins = total_wins - ot_wins
      => losses = total_losses - ot_losses
    - points는 NHL/AHL 기준으로:
      points = (total_wins * 2) + (ot_losses * 1)
      (여기서 total_wins = reg_wins + ot_wins)
    """
    games = hockey_fetch_all(
        """
        SELECT
            home_team_id,
            away_team_id,
            status,
            score_json
        FROM hockey_games
        WHERE league_id = %s
          AND season = %s
          AND game_date >= %s
        """,
        (league_id, season, regular_start_utc),
    )

    # team_id -> accumulator
    acc: Dict[int, Dict[str, int]] = {}

    def ensure(tid: int) -> Dict[str, int]:
        if tid not in acc:
            acc[tid] = {
                "played": 0,
                "total_wins": 0,     # ✅ 정규+OT 포함 승
                "total_losses": 0,   # ✅ 정규+OT 포함 패
                "ot_wins": 0,
                "ot_losses": 0,
                "gf": 0,
                "ga": 0,
            }
        return acc[tid]

    for g in games:
        status = g.get("status")
        if not _is_finished_status(status):
            continue

        sj = g.get("score_json") or {}
        try:
            hs = sj.get("home")
            as_ = sj.get("away")
            if hs is None or as_ is None:
                continue
            hs_i = int(hs)
            as_i = int(as_)
        except Exception:
            continue

        home_id = _safe_int(g.get("home_team_id"))
        away_id = _safe_int(g.get("away_team_id"))
        if home_id is None or away_id is None:
            continue

        st = str(status).strip().upper()
        is_ot_bucket = st in ("AOT", "AP", "PEN", "AET")  # ✅ OT/SO로 처리

        # HOME 누적
        h = ensure(home_id)
        h["played"] += 1
        h["gf"] += hs_i
        h["ga"] += as_i

        # AWAY 누적
        a = ensure(away_id)
        a["played"] += 1
        a["gf"] += as_i
        a["ga"] += hs_i

        if hs_i > as_i:
            h["total_wins"] += 1
            a["total_losses"] += 1
            if is_ot_bucket:
                h["ot_wins"] += 1
                a["ot_losses"] += 1
        elif hs_i < as_i:
            a["total_wins"] += 1
            h["total_losses"] += 1
            if is_ot_bucket:
                a["ot_wins"] += 1
                h["ot_losses"] += 1
        else:
            # 종료경기 동점은 거의 없지만 방어
            pass

    out: Dict[int, Dict[str, Optional[int]]] = {}
    for tid, a in acc.items():
        played = a["played"]
        total_wins = a["total_wins"]
        total_losses = a["total_losses"]
        ot_wins = a["ot_wins"]
        ot_losses = a["ot_losses"]

        # ✅ 표기용 정규 승/패
        reg_wins = total_wins - ot_wins
        reg_losses = total_losses - ot_losses

        # ✅ 포인트는 "전체 승" 기준
        points = (total_wins * 2) + (ot_losses * 1)

        out[tid] = {
            "played": played,
            "wins": reg_wins,          # ✅ W는 정규승
            "losses": reg_losses,      # ✅ L은 정규패
            "ot_wins": ot_wins,
            "ot_losses": ot_losses,
            "points": points,
            "gf": a["gf"],
            "ga": a["ga"],
        }

    return out




def hockey_get_standings(
    league_id: int,
    season: int,
    stage: Optional[str] = None,
    group_name: Optional[str] = None,
) -> Dict[str, Any]:
    """
    하키 스탠딩 (정식 고정: stage -> groups -> rows)
    - hockey_standings 정규화 컬럼을 신뢰 (raw_json 파싱 ❌)
    - 반환 구조: stages=[{stage, groups:[{group_name, rows:[...]}]}]
    - 필터 지원: stage, group_name

    ✅ 추가 정책(기능 추가, 기존 동작 유지):
    - 특정 리그/시즌(NHL/AHL 등)에서 프리시즌이 섞여 스탯이 깨지는 경우,
      '정규시즌 시작일' 이후의 hockey_games(종료 경기)로 팀 스탯을 재계산해서
      Regular Season stage에 한해 stats를 override 한다.
    """

    # league 메타
    league = hockey_fetch_one(
        """
        SELECT
            l.id,
            l.name,
            l.logo,
            c.name AS country
        FROM hockey_leagues l
        LEFT JOIN hockey_countries c ON c.id = l.country_id
        WHERE l.id = %s
        LIMIT 1
        """,
        (league_id,),
    )
    if not league:
        raise ValueError("LEAGUE_NOT_FOUND")

    # ─────────────────────────────────────────
    # ✅ Playoffs/Knockout → Bracket (DB ties 기반)
    # - stage/group 필터가 걸린 경우에는 기존 standings 동작 유지
    # - ties가 있으면 standings 대신 bracket 응답
    # ─────────────────────────────────────────
    if not stage and not group_name:
        tie_rows = hockey_fetch_all(
            """
            SELECT
                t.round,
                t.team1_id,
                t.team2_id,
                t.team1_wins,
                t.team2_wins,
                t.best_of,
                t.winner_team_id,
                t.first_game,
                t.last_game,
                a.name AS team1_name,
                a.logo AS team1_logo,
                b.name AS team2_name,
                b.logo AS team2_logo
            FROM hockey_tournament_ties t
            JOIN hockey_teams a ON a.id = t.team1_id
            JOIN hockey_teams b ON b.id = t.team2_id
            WHERE t.league_id = %s
              AND t.season = %s
            ORDER BY
              CASE t.round
                WHEN 'Quarter-finals' THEN 1
                WHEN 'Semi-finals' THEN 2
                WHEN 'Final' THEN 3
                WHEN '3rd place' THEN 4
                ELSE 99
              END,
              t.first_game NULLS LAST,
              t.team1_id,
              t.team2_id
            """,
            (league_id, season),
        )

        if tie_rows:
            rounds_map: Dict[str, List[Dict[str, Any]]] = {}
            for tr in tie_rows:
                rname = (tr.get("round") or "").strip() or "Unknown"
                rounds_map.setdefault(rname, []).append(
                    {
                        "round": rname,
                        "team1": {
                            "id": _safe_int(tr.get("team1_id")),
                            "name": tr.get("team1_name"),
                            "logo": tr.get("team1_logo"),
                        },
                        "team2": {
                            "id": _safe_int(tr.get("team2_id")),
                            "name": tr.get("team2_name"),
                            "logo": tr.get("team2_logo"),
                        },
                        "series": {
                            "team1_wins": _safe_int(tr.get("team1_wins")) or 0,
                            "team2_wins": _safe_int(tr.get("team2_wins")) or 0,
                            "best_of": _safe_int(tr.get("best_of")),
                            "winner_team_id": _safe_int(tr.get("winner_team_id")),
                            "first_game": (tr.get("first_game").isoformat().replace("+00:00", "Z") if tr.get("first_game") else None),
                            "last_game": (tr.get("last_game").isoformat().replace("+00:00", "Z") if tr.get("last_game") else None),
                        },
                    }
                )

            # round 순서 고정
            round_order = ["Quarter-finals", "Semi-finals", "Final", "3rd place"]
            rounds_out: List[Dict[str, Any]] = []

            for rn in round_order:
                if rn in rounds_map:
                    rounds_out.append({"name": rn, "ties": rounds_map[rn]})

            # 알 수 없는 라운드가 있으면 뒤에 붙임
            for rn in sorted([k for k in rounds_map.keys() if k not in set(round_order)]):
                rounds_out.append({"name": rn, "ties": rounds_map[rn]})

            return {
                "ok": True,
                "type": "bracket",
                "league": {
                    "id": league["id"],
                    "name": league["name"],
                    "logo": league["logo"],
                    "country": league.get("country"),
                },
                "season": season,
                "rounds": rounds_out,
                # ✅ 앱이 기존 stages를 강제 접근하는 경우를 대비해 빈 배열로 포함
                "stages": [],
                "meta": {
                    "source": "db_ties",
                    "filters": {"stage": None, "group_name": None},
                    "generated_at": datetime.now(timezone.utc)
                    .replace(microsecond=0)
                    .isoformat()
                    .replace("+00:00", "Z"),
                },
            }

    # ─────────────────────────────────────────
    # 기존 standings 로직 (그대로)
    # ─────────────────────────────────────────
    where = ["s.league_id = %s", "s.season = %s"]
    params: List[Any] = [league_id, season]

    if stage:
        where.append("s.stage = %s")
        params.append(stage)
    if group_name:
        where.append("s.group_name = %s")
        params.append(group_name)

    where_sql = " AND ".join(where)

    rows = hockey_fetch_all(
        f"""
        SELECT
            s.league_id,
            s.season,
            s.stage,
            s.group_name,
            s.team_id,
            s.position,
            s.games_played,
            s.win_total,
            s.win_ot_total,
            s.lose_total,
            s.lose_ot_total,
            s.goals_for,
            s.goals_against,
            s.points,
            s.form,
            s.description,
            t.name AS team_name,
            t.logo AS team_logo
        FROM hockey_standings s
        JOIN hockey_teams t ON t.id = s.team_id
        WHERE {where_sql}
        ORDER BY s.stage ASC, s.group_name ASC, s.position ASC
        """,
        tuple(params),
    )


    # ✅ 정규시즌 시작일이 등록된 리그/시즌이면, games 기반 재계산 맵 준비
    regular_start_utc = get_regular_season_start_utc(league_id, season)
    regular_stats_map: Dict[int, Dict[str, Optional[int]]] = {}
    if regular_start_utc is not None:
        regular_stats_map = _build_regular_stats_map_from_games(
            league_id=league_id,
            season=season,
            regular_start_utc=regular_start_utc,
        )

    # stages_map[stage][group_name] = [rows...]
    stages_map: Dict[str, Dict[str, List[Dict[str, Any]]]] = {}

    for r in rows:
        st = (r.get("stage") or "Overall").strip()
        gn = (r.get("group_name") or "Overall").strip()

        team_id = _safe_int(r.get("team_id"))

        # DB standings 값(기존)
        gp = _safe_int(r.get("games_played"))
        w = _safe_int(r.get("win_total"))
        l = _safe_int(r.get("lose_total"))
        ot_w = _safe_int(r.get("win_ot_total"))
        ot_l = _safe_int(r.get("lose_ot_total"))
        gf = _safe_int(r.get("goals_for"))
        ga = _safe_int(r.get("goals_against"))
        pts = _safe_int(r.get("points"))

        # ✅ Regular Season stage인 경우에만 override (기존 기능/정렬/필터는 유지)
        if regular_stats_map and team_id is not None:
            st_l = st.lower()
            is_regular_stage = ("regular" in st_l) or (st_l in ("overall",))
            if is_regular_stage:
                rs = regular_stats_map.get(team_id)
                if rs:
                    gp = rs.get("played")
                    w = rs.get("wins")
                    l = rs.get("losses")
                    ot_w = rs.get("ot_wins")
                    ot_l = rs.get("ot_losses")
                    gf = rs.get("gf")
                    ga = rs.get("ga")
                    pts = rs.get("points")

        diff = None
        if gf is not None and ga is not None:
            diff = gf - ga

        row_obj = {
            "rank": _safe_int(r.get("position")),
            "team": {
                "id": team_id,
                "name": r.get("team_name"),
                "logo": r.get("team_logo"),
            },
            "stats": {
                "played": gp,
                "wins": w,
                "losses": l,
                "ot_wins": ot_w,
                "ot_losses": ot_l,
                "points": pts,
                "gf": gf,
                "ga": ga,
                "diff": diff,
                "form": r.get("form"),
                "description": r.get("description"),
            },
        }

        stages_map.setdefault(st, {}).setdefault(gn, []).append(row_obj)

    # 정렬 & 출력
    stages_out: List[Dict[str, Any]] = []

    for st, groups in stages_map.items():
        groups_out: List[Dict[str, Any]] = []
        for gn, items in groups.items():
            items_sorted = sorted(items, key=lambda x: (x["rank"] is None, x["rank"] or 10**9))
            groups_out.append(
                {
                    "group_name": gn,
                    "rows": items_sorted,
                }
            )

        # group 정식 정렬(기본=Division 우선)
        groups_out.sort(key=lambda g: _group_rank(g["group_name"]))

        stages_out.append(
            {
                "stage": st,
                "groups": groups_out,
            }
        )

    # stage 정식 정렬
    stages_out.sort(key=lambda s: _stage_rank(s["stage"]))

    return {
        "ok": True,
        "league": {
            "id": league["id"],
            "name": league["name"],
            "logo": league["logo"],
            "country": league.get("country"),
        },
        "season": season,
        "stages": stages_out,
        "meta": {
            "source": "db",
            "filters": {
                "stage": stage,
                "group_name": group_name,
            },
            "generated_at": datetime.now(timezone.utc)
            .replace(microsecond=0)
            .isoformat()
            .replace("+00:00", "Z"),
        },
    }

