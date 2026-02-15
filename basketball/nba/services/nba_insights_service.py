# basketball/nba/services/nba_insights_service.py
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from math import ceil, floor
from typing import Any, Dict, List, Optional, Sequence, Tuple

from basketball.nba.nba_db import nba_fetch_all, nba_fetch_one


# ✅ 종료경기만 사용 (네 DB에서 status_short=3이 Finished였음)
FINISHED_STATUS_SHORT = 3


def _iso_utc_now() -> str:
    return (
        datetime.now(timezone.utc)
        .replace(microsecond=0)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _safe_int(v: Any) -> Optional[int]:
    try:
        if v is None:
            return None
        return int(v)
    except Exception:
        return None


def _safe_float(v: Any) -> Optional[float]:
    try:
        if v is None:
            return None
        return float(v)
    except Exception:
        return None


def _safe_div(n: int, d: int) -> Optional[float]:
    if d <= 0:
        return None
    return float(n) / float(d)


def _snap_half(x: float) -> float:
    # ✅ baseline은 항상 ".5" 고정: floor(avg) + 0.5
    # 예) 117.0 -> 117.5, 117.8 -> 117.5
    return float(floor(x)) + 0.5



def _ceil_line(line: float) -> int:
    # 점수는 정수 => score >= CEIL(line) 로 Over 판정
    return int(ceil(line))


def _triple(values_by_bucket: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "totals": values_by_bucket.get("totals"),
        "home": values_by_bucket.get("home"),
        "away": values_by_bucket.get("away"),
    }


def _build_section(
    title: str,
    rows: List[Dict[str, Any]],
    counts: Optional[Dict[str, int]] = None,
    subtitle: Optional[str] = None,
) -> Dict[str, Any]:
    out: Dict[str, Any] = {
        "title": title,
        "columns": ["Totals", "Home", "Away"],
        "rows": rows,
    }

    if subtitle is not None and str(subtitle).strip() != "":
        out["subtitle"] = str(subtitle)

    if counts is not None:
        t = int(counts.get("totals", 0))
        h = int(counts.get("home", 0))
        a = int(counts.get("away", 0))
        out["counts"] = {"totals": t, "total": t, "home": h, "away": a}
        out["count"] = {"total": t, "home": h, "away": a}

    return out


@dataclass
class _Bucket:
    games: List[int]
    home_flags: Dict[int, bool]  # game_id -> is_home_for_selected


def _load_available_seasons_for_league(league: str, limit: int = 6) -> List[int]:
    sql = """
        SELECT DISTINCT g.season
        FROM nba_games g
        WHERE
            g.league = %s
            AND g.season IS NOT NULL
        ORDER BY g.season DESC
        LIMIT %s
    """
    rows = nba_fetch_all(sql, (league, limit))
    out: List[int] = []
    for r in rows:
        y = _safe_int(r.get("season"))
        if y is not None:
            out.append(y)
    return out



def _load_recent_games(
    team_id: int,
    last_n: int,
    league: str,
    cutoff_utc: Optional[Any] = None,
    exclude_game_id: Optional[int] = None,
) -> _Bucket:
    """
    ✅ 현재 matchdetail 경기(exclude_game_id)는 항상 제외
    ✅ cutoff_utc(=현재 경기 시작시각) 이전(Finished) 경기만 대상으로 last_n 추출
    """
    sql = """
        SELECT
            g.id AS game_id,
            g.home_team_id,
            g.visitor_team_id
        FROM nba_games g
        WHERE
            g.league = %s
            AND g.status_short = %s
            AND (g.home_team_id = %s OR g.visitor_team_id = %s)
            AND (%s IS NULL OR g.date_start_utc < %s)
            AND (%s IS NULL OR g.id <> %s)
        ORDER BY g.date_start_utc DESC NULLS LAST, g.id DESC
        LIMIT %s
    """
    rows = nba_fetch_all(
        sql,
        (
            league,
            FINISHED_STATUS_SHORT,
            team_id,
            team_id,
            cutoff_utc,
            cutoff_utc,
            exclude_game_id,
            exclude_game_id,
            last_n,
        ),
    )

    games: List[int] = []
    home_flags: Dict[int, bool] = {}
    for r in rows:
        gid = _safe_int(r.get("game_id"))
        if gid is None:
            continue
        games.append(gid)
        home_flags[gid] = (_safe_int(r.get("home_team_id")) == team_id)

    return _Bucket(games=games, home_flags=home_flags)



def _load_games_for_season(
    team_id: int,
    league: str,
    season: int,
    cutoff_utc: Optional[Any] = None,
    exclude_game_id: Optional[int] = None,
) -> _Bucket:
    """
    ✅ 현재 matchdetail 경기(exclude_game_id)는 항상 제외
    ✅ cutoff_utc(=현재 경기 시작시각) 이전(Finished) 경기만 대상으로 시즌 표본 구성
    """
    sql = """
        SELECT
            g.id AS game_id,
            g.home_team_id,
            g.visitor_team_id
        FROM nba_games g
        WHERE
            g.league = %s
            AND g.status_short = %s
            AND g.season = %s
            AND (g.home_team_id = %s OR g.visitor_team_id = %s)
            AND (%s IS NULL OR g.date_start_utc < %s)
            AND (%s IS NULL OR g.id <> %s)
        ORDER BY g.date_start_utc DESC NULLS LAST, g.id DESC
        LIMIT 5000
    """
    rows = nba_fetch_all(
        sql,
        (
            league,
            FINISHED_STATUS_SHORT,
            season,
            team_id,
            team_id,
            cutoff_utc,
            cutoff_utc,
            exclude_game_id,
            exclude_game_id,
        ),
    )

    games: List[int] = []
    home_flags: Dict[int, bool] = {}
    for r in rows:
        gid = _safe_int(r.get("game_id"))
        if gid is None:
            continue
        games.append(gid)
        home_flags[gid] = (_safe_int(r.get("home_team_id")) == team_id)

    return _Bucket(games=games, home_flags=home_flags)



def _load_games_raw(game_ids: List[int]) -> Dict[int, Dict[str, Any]]:
    if not game_ids:
        return {}
    placeholders = ", ".join(["%s"] * len(game_ids))
    sql = f"""
        SELECT
            id,
            home_team_id,
            visitor_team_id,
            raw_json
        FROM nba_games
        WHERE id IN ({placeholders})
    """
    rows = nba_fetch_all(sql, tuple(game_ids))
    out: Dict[int, Dict[str, Any]] = {}
    for r in rows:
        gid = _safe_int(r.get("id"))
        if gid is None:
            continue
        out[gid] = r
    return out


def _extract_linescore_points(raw_json: Dict[str, Any], side: str) -> List[int]:
    """
    side: "home" or "visitors"
    nba raw_json 예시:
      raw_json["scores"]["home"]["linescore"] = ["25","30","..."]
    """
    scores = (raw_json or {}).get("scores") or {}
    block = scores.get(side) or {}
    ls = block.get("linescore") or []
    out: List[int] = []
    if isinstance(ls, list):
        for x in ls:
            v = _safe_int(x)
            if v is None:
                v = 0
            out.append(v)
    return out


def _segment_points(linescore: List[int], seg: str) -> int:
    # seg: "FT_REG", "Q1","Q2","Q3","Q4","OT_ALL"
    if seg == "FT_REG":
        return sum(linescore[:4])
    if seg == "Q1":
        return linescore[0] if len(linescore) >= 1 else 0
    if seg == "Q2":
        return linescore[1] if len(linescore) >= 2 else 0
    if seg == "Q3":
        return linescore[2] if len(linescore) >= 3 else 0
    if seg == "Q4":
        return linescore[3] if len(linescore) >= 4 else 0
    if seg == "OT_ALL":
        return sum(linescore[4:]) if len(linescore) > 4 else 0
    return 0


def _has_ot(linescore_home: List[int], linescore_away: List[int]) -> bool:
    # 라인스코어가 4개 초과면 OT 존재로 판정
    return (len(linescore_home) > 4) or (len(linescore_away) > 4)


def nba_get_game_insights(
    game_id: int,
    team_id: Optional[int] = None,
    last_n: int = 10,
    season: Optional[int] = None,
) -> Dict[str, Any]:
    # 0) game 존재 확인 + 기본 team_id 결정 + league/season 확보
    g = nba_fetch_one(
        """
        SELECT id, league, season, date_start_utc, home_team_id, visitor_team_id
        FROM nba_games
        WHERE id = %s
        LIMIT 1
        """,
        (game_id,),
    )

    if not g:
        raise ValueError("GAME_NOT_FOUND")

    league = (g.get("league") or "standard").strip() or "standard"
    default_team_id = _safe_int(g.get("home_team_id"))
    sel_team_id = _safe_int(team_id) or default_team_id
    cutoff_utc = g.get("date_start_utc")  # 현재 경기 시작시각(UTC)
    exclude_game_id = int(game_id)        # 현재 보고있는 경기는 항상 표본에서 제외

    if sel_team_id is None:
        return {"ok": True, "game_id": game_id, "sections": [], "meta": {"reason": "TEAM_ID_MISSING"}}

    if last_n < 1:
        last_n = 1
    if last_n > 50:
        last_n = 50

    available_seasons = _load_available_seasons_for_league(league, limit=6)

    if season is not None:
        mode = "season"
        bucket = _load_games_for_season(
            sel_team_id,
            league,
            season,
            cutoff_utc=cutoff_utc,
            exclude_game_id=exclude_game_id,
        )
    else:
        mode = "last_n"
        bucket = _load_recent_games(
            sel_team_id,
            last_n,
            league,
            cutoff_utc=cutoff_utc,
            exclude_game_id=exclude_game_id,
        )


    game_ids = bucket.games
    game_raw = _load_games_raw(game_ids)

    totals_ids = list(game_ids)
    home_ids = [gid for gid in game_ids if bucket.home_flags.get(gid) is True]
    away_ids = [gid for gid in game_ids if bucket.home_flags.get(gid) is False]

    def iter_bucket(name: str) -> List[int]:
        if name == "totals":
            return totals_ids
        if name == "home":
            return home_ids
        return away_ids

    def _points_for_game(gid: int, seg: str) -> Tuple[int, int]:
        """
        return (team_points, opp_points) for selected team in segment
        """
        row = game_raw.get(gid) or {}
        rj = row.get("raw_json") or {}

        h_id = _safe_int(row.get("home_team_id"))
        v_id = _safe_int(row.get("visitor_team_id"))

        ls_home = _extract_linescore_points(rj, "home")
        ls_vis = _extract_linescore_points(rj, "visitors")

        home_seg = _segment_points(ls_home, seg)
        vis_seg = _segment_points(ls_vis, seg)

        if sel_team_id == h_id:
            return home_seg, vis_seg
        if sel_team_id == v_id:
            return vis_seg, home_seg
        # 방어
        return home_seg, vis_seg

    def _is_ot_game(gid: int) -> bool:
        row = game_raw.get(gid) or {}
        rj = row.get("raw_json") or {}
        ls_home = _extract_linescore_points(rj, "home")
        ls_vis = _extract_linescore_points(rj, "visitors")
        return _has_ot(ls_home, ls_vis)

    def _result_label(tp: int, op: int) -> str:
        if tp > op:
            return "W"
        if tp < op:
            return "L"
        return "D"

    # ─────────────────────────────
    # 공통 통계 유틸 (bucket별)
    # ─────────────────────────────
def _avg(getter, denom_filter=None) -> Dict[str, Optional[float]]:
    out: Dict[str, Optional[float]] = {}
    for b in ("totals", "home", "away"):
        ids0 = iter_bucket(b)
        ids = [gid for gid in ids0 if (denom_filter(gid) if denom_filter else True)]
        n = len(ids)
        if n <= 0:
            out[b] = None
            continue
        s = 0
        for gid in ids:
            s += int(getter(gid))
        out[b] = float(s) / float(n)
    return out



    def _prob(pred, denom_filter=None) -> Dict[str, Optional[float]]:
        """
        pred(gid)->bool
        denom_filter(gid)->bool  (없으면 전체 bucket)
        """
        out: Dict[str, Optional[float]] = {}
        for b in ("totals", "home", "away"):
            ids0 = iter_bucket(b)
            ids = [gid for gid in ids0 if (denom_filter(gid) if denom_filter else True)]
            denom = len(ids)
            if denom <= 0:
                out[b] = None
                continue
            num = 0
            for gid in ids:
                if pred(gid):
                    num += 1
            out[b] = float(num) / float(denom)
        return out

    def _counts(denom_filter=None) -> Dict[str, int]:
        out: Dict[str, int] = {}
        for b in ("totals", "home", "away"):
            ids0 = iter_bucket(b)
            ids = [gid for gid in ids0 if (denom_filter(gid) if denom_filter else True)]
            out[b] = len(ids)
        return out

    # ─────────────────────────────
    # Baseline/Over 계산 (section별)
    # ─────────────────────────────
    def _baseline_and_over_probs(
        seg: str,
        lines: Sequence[float],
        use_total: bool,
        denom_filter=None,
    ) -> Tuple[Dict[str, Optional[float]], Dict[str, Dict[float, Optional[float]]]]:
        """
        return:
          baseline_by_bucket
          over_prob_by_bucket[line] = prob
        """
        # avg -> baseline(snap 0.5)
        def get_score(gid: int) -> int:
            tp, op = _points_for_game(gid, seg)
            return (tp + op) if use_total else tp

        avg_by = _avg(get_score, denom_filter=denom_filter)
        baseline_by: Dict[str, Optional[float]] = {}
        over_probs_by: Dict[str, Dict[float, Optional[float]]] = {}

        for b in ("totals", "home", "away"):
            ids0 = iter_bucket(b)
            ids = [gid for gid in ids0 if (denom_filter(gid) if denom_filter else True)]
            n = len(ids)
            if n <= 0:
                baseline_by[b] = None
                over_probs_by[b] = {float(x): None for x in lines}
                continue

            # baseline = snap(avg)
            avgv = avg_by.get(b)
            if avgv is None:
                baseline_by[b] = None
                over_probs_by[b] = {float(x): None for x in lines}
                continue

            base = _snap_half(float(avgv))
            baseline_by[b] = base

            # over probs for each line: score >= ceil(line)
            probs: Dict[float, Optional[float]] = {}
            for ln in lines:
                thr = _ceil_line(float(ln))
                num = 0
                for gid in ids:
                    sc = int(get_score(gid))
                    if sc >= thr:
                        num += 1
                probs[float(ln)] = _safe_div(num, n)
            over_probs_by[b] = probs

        return baseline_by, over_probs_by

    # ─────────────────────────────
    # 섹션 구성: 네가 준 스펙(FT/1Q~4Q/OT)
    # ─────────────────────────────
    def _section_spec(seg_title: str) -> Tuple[str, str, List[float]]:
        """
        return: (seg_key, prefix, lines_for_over)
        """
        if seg_title == "Full Time":
            return ("FT_REG", "FT", [-10, -5, 0, +5, +10])  # baseline +/- {10,5,0}
        if seg_title == "1 Quarter":
            return ("Q1", "1Q", [-2, -1, 0, +1, +2])
        if seg_title == "2 Quarter":
            return ("Q2", "2Q", [-2, -1, 0, +1, +2])
        if seg_title == "3 Quarter":
            return ("Q3", "3Q", [-2, -1, 0, +1, +2])
        if seg_title == "4 Quarter":
            return ("Q4", "4Q", [-2, -1, 0, +1, +2])
        # OT
        return ("OT_ALL", "OT", [-2, -1, 0, +1, +2])


    def _baseline_rule_text(seg_key: str) -> str:
        # FT
        if seg_key == "FT_REG":
            return (
                "기준점(baseline)=해당 섹션 Avg에서 정수 부분을 취한 뒤 +0.5 (floor(avg)+0.5). "
                "Over 라인: baseline-10, baseline-5, baseline, baseline+5, baseline+10. "
                "Over 판정은 점수가 정수이므로 score >= CEIL(line)로 계산하면 동일."
            )


        # Quarters
        if seg_key in ("Q1", "Q2", "Q3", "Q4"):
            return (
                "기준점(baseline)=해당 쿼터 Avg에서 정수 부분을 취한 뒤 +0.5 (floor(avg)+0.5). "
                "Over 라인: baseline-2, baseline-1, baseline, baseline+1, baseline+2. "
                "Over 판정은 점수가 정수이므로 score >= CEIL(line)로 계산하면 동일."
            )


        # OT
        return (
            "기준점(baseline)=OT Avg에서 정수 부분을 취한 뒤 +0.5 (floor(avg)+0.5). "
            "Over 라인: baseline-2, baseline-1, baseline, baseline+1, baseline+2. "
            "Over 판정은 점수가 정수이므로 score >= CEIL(line)로 계산하면 동일."
        )




    def _ot_sample_rule_text() -> str:
        return (
            "✅ 모수(분모)=OT(연장)까지 간 경기만 포함(정규시간 종료 시 동점으로 연장 발생한 경기). "
            "OT 없는 경기는 제외."
        )

    def _build_segment_section(seg_title: str) -> Dict[str, Any]:
        seg_key, prefix, line_offsets = _section_spec(seg_title)

        # OT는 "OT 간 경기만" 모수로 제한 (샘플룰)
        denom_filter = _is_ot_game if seg_key == "OT_ALL" else None
        cnt = _counts(denom_filter=denom_filter)

        # 평균(Team/Total)
        def team_score(gid: int) -> int:
            tp, _ = _points_for_game(gid, seg_key)
            return tp

        def total_score(gid: int) -> int:
            tp, op = _points_for_game(gid, seg_key)
            return tp + op

        team_avg = _avg(team_score, denom_filter=denom_filter)
        total_avg = _avg(total_score, denom_filter=denom_filter)

        # baseline = snap(avg) then build lines
        # lines are baseline + offset
        baseline_team_by, _ = _baseline_and_over_probs(seg_key, [], use_total=False, denom_filter=denom_filter)
        baseline_total_by, _ = _baseline_and_over_probs(seg_key, [], use_total=True, denom_filter=denom_filter)

        # 실제 over 라인별 확률 계산: bucket별 baseline이 달라서 bucket별로 라인도 달라짐
        # => "baseline±k" 텍스트는 label에 박고, 확률은 bucket baseline 기준으로 계산
        def _over_prob_bucket(use_total: bool, offset: float) -> Dict[str, Optional[float]]:
            """
            ✅ 확률 계산 기준점을 "Totals baseline" 하나로 통일
            - 라벨에 표시되는 기준점(=Totals baseline ± offset)과
              Totals/Home/Away 컬럼의 Over 확률 계산이 반드시 일치해야 함.
            """
            out: Dict[str, Optional[float]] = {}

            base_src = (baseline_total_by if use_total else baseline_team_by)
            base_t = base_src.get("totals")
            if base_t is None:
                # 기준점 자체가 없으면 전부 None
                return {"totals": None, "home": None, "away": None}

            line = float(base_t) + float(offset)
            thr = _ceil_line(line)

            for b in ("totals", "home", "away"):
                ids0 = iter_bucket(b)
                ids = [gid for gid in ids0 if (denom_filter(gid) if denom_filter else True)]
                n = len(ids)
                if n <= 0:
                    out[b] = None
                    continue

                num = 0
                for gid in ids:
                    tp, op = _points_for_game(gid, seg_key)
                    sc = (tp + op) if use_total else tp
                    if int(sc) >= thr:
                        num += 1
                out[b] = _safe_div(num, n)

            return out


        # W/D/L
        def _prob_res(res: str) -> Dict[str, Optional[float]]:
            return _prob(
                pred=lambda gid: (_result_label(*_points_for_game(gid, seg_key)) == res),
                denom_filter=denom_filter,
            )

        rows: List[Dict[str, Any]] = []

        def _fmt_line(x: float) -> str:
            # 23.0 -> "23", 23.5 -> "23.5"
            if abs(x - round(x)) < 1e-9:
                return str(int(round(x)))
            return f"{x:.1f}".rstrip("0").rstrip(".")

        def _row_bucket_only(label: str, bucket: str, v: Any) -> Dict[str, Any]:
            # bucket 하나만 값 넣고 나머지는 None -> 앱에서 "-"로 보임
            return {
                "label": label,
                "values": {
                    "totals": v if bucket == "totals" else None,
                    "home": v if bucket == "home" else None,
                    "away": v if bucket == "away" else None,
                },
            }

        # ✅ OT 샘플룰은 rows에 넣지 말고 subtitle에만 짧게(원하면 제거 가능)
        subtitle_extra = " · OT games only" if seg_key == "OT_ALL" else ""



        # W/D/L
        rows += [
            {"label": f"{prefix} W", "values": _triple(_prob_res("W"))},
            {"label": f"{prefix} D", "values": _triple(_prob_res("D"))},
            {"label": f"{prefix} L", "values": _triple(_prob_res("L"))},
        ]

        # Team Avg + Over lines (baseline offsets)
        # Team Avg (숫자는 셀에 표시)
        rows.append({"label": f"{prefix} Team Score Avg", "values": _triple(team_avg)})

        # ✅ Team Over lines: 라벨 숫자라인은 Totals baseline 기준(표 기준)으로 고정
        # ✅ 확률 값도 Totals baseline 기준으로 통일(아래 _over_prob_bucket 패치로 반영)
        for off in line_offsets:
            base_t = baseline_team_by.get("totals")
            if base_t is None:
                continue
            line_t = float(base_t) + float(off)

            label = f"{prefix} Team {_fmt_line(line_t)}+ Over"
            rows.append({
                "label": label,
                "values": _triple(_over_prob_bucket(use_total=False, offset=off))
            })




        # Total Avg + Over lines
        # Total Avg
        rows.append({"label": f"{prefix} Total Score Avg", "values": _triple(total_avg)})

        # ✅ Total Over lines: 라벨 숫자라인은 Totals baseline 기준(표 기준)으로 고정
        # ✅ 확률 값도 Totals baseline 기준으로 통일(아래 _over_prob_bucket 패치로 반영)
        for off in line_offsets:
            base_t = baseline_total_by.get("totals")
            if base_t is None:
                continue
            line_t = float(base_t) + float(off)

            label = f"{prefix} Total {_fmt_line(line_t)}+ Over"
            rows.append({
                "label": label,
                "values": _triple(_over_prob_bucket(use_total=True, offset=off))
            })




        # ✅ 섹션별 T/H/A 표시는 제거 (앱에서 Last N 필터 바로 아래 1회만 표시할 것)
        # ✅ 단, OT는 섹션 타이틀 오른쪽에 보여주고 싶으니 subtitle로만 남김
        subtitle = None
        if seg_key == "OT_ALL":
            subtitle = f"OT: T={cnt['totals']} / H={cnt['home']} / A={cnt['away']}"
        return _build_section(title=seg_title, rows=rows, counts=cnt, subtitle=subtitle)



    sections = [
        _build_segment_section("Full Time"),
        _build_segment_section("1 Quarter"),
        _build_segment_section("2 Quarter"),
        _build_segment_section("3 Quarter"),
        _build_segment_section("4 Quarter"),
        _build_segment_section("OT"),
    ]


    return {
        "ok": True,
        "game_id": game_id,
        "team_id": sel_team_id,
        "last_n": last_n,
        "sections": sections,
        "meta": {
            "source": "db",
            "league": league,
            "finished_status_short": FINISHED_STATUS_SHORT,
            "generated_at": _iso_utc_now(),
            "mode": mode,  # "last_n" or "season"
            "selected_season": season,
            "available_seasons": available_seasons,
            "sample_sizes": {
                "totals": len(totals_ids),
                "home": len(home_ids),
                "away": len(away_ids),
            },
                        "filter_options": {
                "last_n": [3, 5, 7, 10, 20],
                "seasons": available_seasons,
            },
            "echo": {
                "requested_last_n": last_n,
                "requested_season": season,
            },

            "sample_label": f"T={len(totals_ids)} / H={len(home_ids)} / A={len(away_ids)}",

        },
    }
