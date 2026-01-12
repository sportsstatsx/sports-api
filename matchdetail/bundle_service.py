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
    main.py 와 동일 컨셉:
    - dict는 재귀 병합
    - list/primitive는 patch가 base를 대체
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
    """
    match_overrides.patch(jsonb)를 읽어 dict로 반환.
    """
    row = fetch_one(
        "SELECT patch FROM match_overrides WHERE fixture_id = %s",
        (fixture_id,),
    )
    if not row:
        return {}

    p = row.get("patch")
    if p is None:
        return {}

    # psycopg가 jsonb를 dict로 주는 케이스/문자열로 주는 케이스 모두 방어
    if isinstance(p, dict):
        return p
    if isinstance(p, str):
        try:
            v = json.loads(p)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}

    return {}


def _reconcile_header_aliases(header: Dict[str, Any]) -> Dict[str, Any]:
    """
    override 적용 후 "동일 의미 키"를 동기화.
    - elapsed <-> minute
    - date_utc <-> kickoff_utc
    - home.ft <-> home.score
    - away.ft <-> away.score
    """
    # elapsed/minute
    if "elapsed" in header and header.get("elapsed") is not None:
        header["minute"] = header.get("elapsed")
    elif "minute" in header and header.get("minute") is not None:
        header["elapsed"] = header.get("minute")

    # date_utc/kickoff_utc
    if "date_utc" in header and header.get("date_utc") is not None:
        header["kickoff_utc"] = header.get("date_utc")
    elif "kickoff_utc" in header and header.get("kickoff_utc") is not None:
        header["date_utc"] = header.get("kickoff_utc")

    # home/away ft/score
    home = header.get("home")
    if isinstance(home, dict):
        if "ft" in home and home.get("ft") is not None:
            home["score"] = home.get("ft")
        elif "score" in home and home.get("score") is not None:
            home["ft"] = home.get("score")

    away = header.get("away")
    if isinstance(away, dict):
        if "ft" in away and away.get("ft") is not None:
            away["score"] = away.get("ft")
        elif "score" in away and away.get("score") is not None:
            away["ft"] = away.get("score")

    return header


def get_match_detail_bundle(
    fixture_id: int,
    league_id: int,
    season: int,
    *,
    comp: Optional[str] = None,
    last_n: Optional[str] = None,
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

    header["filters"] = header_filters

    # 3) ✅ override 적용 (header에 먼저)
    patch = _load_override_patch(fixture_id)

    # patch 형태가 "fixtures 스타일(venue_name/home.ft...)"인 경우를 우선 지원:
    # -> header 루트에 그대로 병합해도 의미있는 키들은 header_block에서 이미 제공하도록 맞춰둠.
    if patch:
        header = _deep_merge(header, patch)
        header = _reconcile_header_aliases(header)

    # 4) 나머지 블록
    form = build_form_block(header)
    timeline = build_timeline_block(header)
    lineups = build_lineups_block(header)
    stats = build_stats_block(header)
    h2h = build_h2h_block(header)
    standings = build_standings_block(header)

    insights_overall = build_insights_overall_block(header)
    ai_predictions = build_ai_predictions_block(header, insights_overall)

    return {
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
