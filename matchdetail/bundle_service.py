# matchdetail/bundle_service.py

from typing import Any, Dict, Optional
import json

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
) -> Optional[Dict[str, Any]]:

    """
    매치디테일 번들의 진입점 (sync 버전).
    comp / last_n 필터를 라우터에서 받아 header.filters 에 반영한다.

    ✅ 추가:
    - match_overrides.patch를 읽어서 header에 병합(디테일에서도 수정 반영)
    """

    # 1) header 블록 생성
    header = build_header_block(
        fixture_id=fixture_id,
        league_id=league_id,
        season=season,
    )
    if header is None:
        return None

    # 2) comp / last_n 필터 덮어쓰기 (앱 → 서버)
    header_filters = header.get("filters", {})

    if comp is not None:
        header_filters["comp"] = comp

    if last_n is not None:
        header_filters["last_n"] = last_n

    header["filters"] = header_filters  # 다시 덮어쓰기

    # 3) ✅ override 로드 (hidden 체크 포함)
    header_patch = {}
    bundle_patch = {}

    if apply_override:
        patch = _load_override_patch(fixture_id)

        # hidden=true면 디테일도 접근 불가 처리(리스트와 일관성)
        if isinstance(patch, dict) and patch.get("hidden") is True:
            return None

        if isinstance(patch, dict) and patch:
            # meta 키는 머지 대상에서 제외
            patch = {k: v for k, v in patch.items() if k != "hidden"}

            # (A) patch가 {"header": {...}} 구조면 header/bundle 분리
            if isinstance(patch.get("header"), dict):
                header_patch = patch.get("header") or {}
                bundle_patch = dict(patch)
                bundle_patch.pop("header", None)
            else:
                # (B) 하위호환: 예전엔 patch 전체를 header에 머지했음
                #     그런데 admin에서 timeline 같은 번들 키를 직접 넣기 시작했으니
                #     "번들 키가 하나라도 있으면" bundle_patch로 처리
                bundle_keys = {
                    "timeline",
                    "form",
                    "lineups",
                    "stats",
                    "h2h",
                    "standings",
                    "insights_overall",
                    "ai_predictions",
                    "header",
                }
                has_bundle_key = any((k in patch) for k in bundle_keys)

                if has_bundle_key:
                    bundle_patch = patch
                else:
                    header_patch = patch

            if isinstance(header_patch, dict) and header_patch:
                header = _deep_merge(header, header_patch)
                _reconcile_header_aliases(header)

    # 4) 나머지 블록 생성
    form = build_form_block(header)
    timeline = build_timeline_block(header)
    lineups = build_lineups_block(header)
    stats = build_stats_block(header)
    h2h = build_h2h_block(header)
    standings = build_standings_block(header)

    insights_overall = build_insights_overall_block(header)
    ai_predictions = build_ai_predictions_block(header, insights_overall)

    lineups = build_lineups_block(header)
    stats = build_stats_block(header)
    h2h = build_h2h_block(header)
    standings = build_standings_block(header)

    insights_overall = build_insights_overall_block(header)
    ai_predictions = build_ai_predictions_block(header, insights_overall)

    bundle = {
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

    # 5) ✅ 번들 완성 후, bundle_patch를 최종 머지 (timeline 삭제/수정 등이 여기서 반영됨)
    if apply_override and isinstance(bundle_patch, dict) and bundle_patch:
        # meta 키는 노출/머지에서 제외
        bundle_patch = {k: v for k, v in bundle_patch.items() if k != "hidden"}
        bundle = _deep_merge(bundle, bundle_patch)

        # header가 bundle_patch에서 바뀌었을 수도 있으니 동의어 재동기화
        if isinstance(bundle.get("header"), dict):
            _reconcile_header_aliases(bundle["header"])

    return bundle

