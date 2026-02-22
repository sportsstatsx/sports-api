# matchdetail/bundle_service.py

from typing import Any, Dict, Optional
import json
import time


from db import fetch_one

from .header_block import build_header_block
from .form_block import build_form_block
from .timeline_block import build_timeline_block
from .lineups_block import build_lineups_block
from .stats_block import build_stats_block
from .h2h_block import build_h2h_block
from .standings_block import build_standings_block
from .insights_block import build_insights_overall_block
from .ai_predictions_block import build_ai_predictions_block


def _deep_merge(base: Any, patch: Any) -> Any:
    """
    dict는 재귀 병합, list/primitive는 patch가 base를 대체.
    """
    if isinstance(base, dict) and isinstance(patch, dict):
        out = dict(base)
        for k, v in patch.items():
            if k in out:
                out[k] = _deep_merge(out[k], v)
            else:
                out[k] = v
        return out
    return patch


def _load_override_patch(fixture_id: int) -> Dict[str, Any]:
    row = fetch_one(
        "SELECT patch FROM match_overrides WHERE fixture_id = %s",
        (fixture_id,),
    )
    if not row:
        return {}

    p = row.get("patch")
    if p is None:
        return {}

    # jsonb가 dict로 올 수도/문자열로 올 수도 있으니 방어
    if isinstance(p, dict):
        return p
    if isinstance(p, str):
        try:
            v = json.loads(p)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}

    return {}


def _reconcile_header_aliases(header: Dict[str, Any]) -> None:
    """
    override 적용 후 동의어 키 동기화.
    - elapsed <-> minute
    - date_utc <-> kickoff_utc
    - home.ft <-> home.score
    - away.ft <-> away.score
    """
    if header.get("elapsed") is not None:
        header["minute"] = header.get("elapsed")
    elif header.get("minute") is not None:
        header["elapsed"] = header.get("minute")

    if header.get("date_utc") is not None:
        header["kickoff_utc"] = header.get("date_utc")
    elif header.get("kickoff_utc") is not None:
        header["date_utc"] = header.get("kickoff_utc")

    home = header.get("home")
    if isinstance(home, dict):
        if home.get("ft") is not None:
            home["score"] = home.get("ft")
        elif home.get("score") is not None:
            home["ft"] = home.get("score")

    away = header.get("away")
    if isinstance(away, dict):
        if away.get("ft") is not None:
            away["score"] = away.get("ft")
        elif away.get("score") is not None:
            away["ft"] = away.get("score")



def get_match_detail_bundle(
    fixture_id: int,
    league_id: int,
    season: int,
    *,
    comp: Optional[str] = None,
    last_n: Optional[str] = None,
    apply_override: bool = True,
    parts: Optional[list[str]] = None,  # ✅ 추가: 요청된 블럭만 생성
) -> Optional[Dict[str, Any]]:


    """
    매치디테일 번들의 진입점 (sync 버전).
    comp / last_n 필터를 라우터에서 받아 header.filters 에 반영한다.

    ✅ override 동작
    - match_overrides.patch.hidden=true 면 None 리턴 (디테일 숨김)
    - header 관련 키는 header에 먼저 merge (다른 블록 생성에 영향 주기 위해)
    - 그 외 키(예: timeline/insights_overall 같은 블록 override)는 bundle 완성 후 최종 merge
    """

    t0 = time.perf_counter()

    t_header0 = time.perf_counter()
    header = build_header_block(
        fixture_id=fixture_id,
        league_id=league_id,
        season=season,
    )
    dt_header = time.perf_counter() - t_header0


    # ✅ 근본 해결: season/league_id가 틀려도 fixture_id 기준으로 1회 자동 보정(fallback)
    # - 앱/클라이언트가 이전 시즌(예: 2025)을 보내도, DB에 실제 시즌(예: 2026)이 있으면 그걸로 재시도
    # - 이 로직은 "특정 리그"가 아니라 모든 리그/시즌 전환 케이스에 공통 적용됨
    if header is None:
        row = fetch_one(
            "SELECT league_id, season FROM matches WHERE fixture_id = %s",
            (fixture_id,),
        )
        if not row:
            row = fetch_one(
                "SELECT league_id, season FROM fixtures WHERE fixture_id = %s",
                (fixture_id,),
            )

        if row:
            real_league_id = row.get("league_id")
            real_season = row.get("season")

            if (
                isinstance(real_league_id, int)
                and isinstance(real_season, int)
                and (real_league_id != league_id or real_season != season)
            ):
                header = build_header_block(
                    fixture_id=fixture_id,
                    league_id=real_league_id,
                    season=real_season,
                )
                # 이후 로직에서도 header와 일치하도록 값 동기화
                league_id = real_league_id
                season = real_season

        if header is None:
            return None


    # filters 오버라이드(앱에서 내려준 comp/last_n 우선)
    header_filters = header.get("filters") or {}
    if comp is not None:
        header_filters["comp"] = comp
    if last_n is not None:
        header_filters["last_n"] = last_n
    header["filters"] = header_filters

    header_patch: Dict[str, Any] = {}
    bundle_patch: Dict[str, Any] = {}

    if apply_override:
        patch = _load_override_patch(fixture_id) or {}

        # hidden=true면 디테일 접근 차단
        if isinstance(patch, dict) and patch.get("hidden") is True:
            return None

        if isinstance(patch, dict) and patch:
            # patch가 {"header": {...}, "timeline": [...]} 구조면 header/bundle 분리
            if isinstance(patch.get("header"), dict):
                header_patch = patch.get("header") or {}
                bundle_patch = {k: v for k, v in patch.items() if k not in ("header", "hidden")}
            else:
                # legacy: header 필드와 bundle 필드가 섞여 있을 수 있어, header로 확실한 키만 분리
                header_keys = {
                    "fixture_id", "league_id", "season",
                    "date_utc", "kickoff_utc",
                    "status_group", "status", "elapsed", "minute", "status_long",
                    "league_round", "venue_name",
                    "league_name", "league_logo", "league_country",
                    "home", "away", "filters",
                }
                for k, v in patch.items():
                    if k in ("hidden",):
                        continue
                    if k in header_keys:
                        header_patch[k] = v
                    else:
                        bundle_patch[k] = v

            # header를 먼저 merge (이 값으로 form/timeline 등 블록 생성)
            if header_patch:
                header = _deep_merge(header, header_patch)
                _reconcile_header_aliases(header)

    # ✅ parts 없으면 기존(전체 생성) 유지 → 앱 패치 전에도 깨지지 않게 백워드 호환
    parts_set = None
    if parts:
        parts_set = {str(x).strip() for x in parts if str(x).strip()}

    def _need(name: str) -> bool:
        return (parts_set is None) or (name in parts_set)

    # 기본값
    form = timeline = lineups = stats = h2h = standings = insights_overall = ai_predictions = None
    dt_form = dt_timeline = dt_lineups = dt_stats = dt_h2h = dt_standings = dt_insights = dt_ai = 0.0

    # 블록 생성 (override 반영된 header 기반) + 타이밍
    if _need("form"):
        t_form0 = time.perf_counter()
        form = build_form_block(header)
        dt_form = time.perf_counter() - t_form0

    if _need("timeline"):
        t_timeline0 = time.perf_counter()
        timeline = build_timeline_block(header)
        dt_timeline = time.perf_counter() - t_timeline0

    if _need("lineups"):
        t_lineups0 = time.perf_counter()
        lineups = build_lineups_block(header)
        dt_lineups = time.perf_counter() - t_lineups0

    if _need("stats"):
        t_stats0 = time.perf_counter()
        stats = build_stats_block(header)
        dt_stats = time.perf_counter() - t_stats0

    if _need("h2h"):
        t_h2h0 = time.perf_counter()
        h2h = build_h2h_block(header)
        dt_h2h = time.perf_counter() - t_h2h0

    if _need("standings"):
        t_stand0 = time.perf_counter()
        standings = build_standings_block(header)
        dt_standings = time.perf_counter() - t_stand0

    # ✅ ai_predictions 의존성: ai를 원하면 insights_overall도 반드시 먼저 생성
    if _need("insights_overall") or _need("ai_predictions"):
        t_ins0 = time.perf_counter()
        insights_overall = build_insights_overall_block(header)
        dt_insights = time.perf_counter() - t_ins0

    if _need("ai_predictions"):
        t_ai0 = time.perf_counter()
        ai_predictions = build_ai_predictions_block(header, insights_overall)
        dt_ai = time.perf_counter() - t_ai0



    # ✅ total 먼저 확정
    dt_total = time.perf_counter() - t0

    # ✅ 응답/헤더/디버그에서 그대로 활용할 수 있도록 perf dict 구성
    perf = {
        "total": float(dt_total),
        "header": float(dt_header),
        "form": float(dt_form),
        "timeline": float(dt_timeline),
        "lineups": float(dt_lineups),
        "stats": float(dt_stats),
        "h2h": float(dt_h2h),
        "standings": float(dt_standings),
        "insights": float(dt_insights),
        "ai": float(dt_ai),
    }

    bundle = {
        "_perf": perf,  # ✅ 추가
        "header": header,
        "form": form,
        "timeline": timeline,
        "lineups": lineups,
        "stats": stats,
        "h2h": h2h,
        "standings": standings,
        "insights_overall": insights_overall,
        "ai_predictions": ai_predictions,
    }

    # ✅ bundle 자체 override 최종 반영 (timeline/events 같은 블록 수정이 여기서 실제로 먹음)
    if bundle_patch:
        bundle = _deep_merge(bundle, bundle_patch)

    # ✅ (선택) 로그는 유지. Render에서 안 보이더라도 perf는 응답에 포함됨.
    try:
        print(
            "[match_detail_bundle]"
            f" fixture_id={fixture_id} league_id={league_id} season={season}"
            f" total={dt_total:.3f}s"
            f" header={dt_header:.3f}s"
            f" form={dt_form:.3f}s"
            f" timeline={dt_timeline:.3f}s"
            f" lineups={dt_lineups:.3f}s"
            f" stats={dt_stats:.3f}s"
            f" h2h={dt_h2h:.3f}s"
            f" standings={dt_standings:.3f}s"
            f" insights={dt_insights:.3f}s"
            f" ai={dt_ai:.3f}s"
        )
    except Exception:
        pass

    return bundle




