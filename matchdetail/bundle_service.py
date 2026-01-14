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

    ✅ override 동작
    - match_overrides.patch.hidden=true 면 None 리턴 (디테일 숨김)
    - header 관련 키는 header에 먼저 merge (다른 블록 생성에 영향 주기 위해)
    - 그 외 키(예: timeline/insights_overall 같은 블록 override)는 bundle 완성 후 최종 merge

    ✅ 추가(중요):
    - override로 timeline을 수정/삭제한 경우,
      header(home/away)의 red_cards / ft / ht 를 최종 timeline 기준으로 재동기화한다.
      (스코어블럭/리스트/타임라인 불일치 방지)
    """

    def _is_red_event(e: Dict[str, Any]) -> bool:
        t = e.get("type")
        d = e.get("detail")
        if isinstance(t, str):
            tu = t.strip().upper()
            if tu in ("RED", "RED_CARD", "REDCARD"):
                return True
            if tu == "CARD" and isinstance(d, str) and "RED" in d.upper():
                return True
        if isinstance(d, str) and "RED" in d.upper():
            return True
        l1 = e.get("line1")
        if isinstance(l1, str) and "RED" in l1.upper():
            return True
        return False

    def _is_goal_event(e: Dict[str, Any]) -> bool:
        t = e.get("type")
        d = e.get("detail")
        if isinstance(t, str):
            tu = t.strip().upper()
            if tu in ("GOAL", "GOAL_NORMAL", "GOAL_PENALTY", "PENALTY_GOAL"):
                return True
        if isinstance(d, str) and "GOAL" in d.upper():
            return True
        l1 = e.get("line1")
        if isinstance(l1, str) and "GOAL" in l1.upper():
            return True
        return False

    def _get_minute(e: Dict[str, Any]) -> Optional[int]:
        for k in ("minute", "elapsed", "time", "min"):
            v = e.get(k)
            if isinstance(v, int):
                return v
            if isinstance(v, str):
                s = v.strip()
                if s.isdigit():
                    return int(s)
        v2 = e.get("minute")
        if isinstance(v2, str) and "+" in v2:
            base = v2.split("+", 1)[0].strip()
            if base.isdigit():
                return int(base)
        return None

    def _extract_timeline(bundle_obj: Dict[str, Any]) -> Optional[List[Any]]:
        tl = bundle_obj.get("timeline")
        if isinstance(tl, list):
            return tl
        if isinstance(tl, dict):
            ev = tl.get("events")
            if isinstance(ev, list):
                return ev
        return None

    def _sync_header_from_timeline(bundle_obj: Dict[str, Any]) -> None:
        if not isinstance(bundle_obj, dict):
            return

        tl = _extract_timeline(bundle_obj)
        if not isinstance(tl, list):
            return

        header_obj = bundle_obj.get("header")
        if not isinstance(header_obj, dict):
            return

        home = header_obj.get("home")
        away = header_obj.get("away")
        if not isinstance(home, dict) or not isinstance(away, dict):
            return

        home_id = home.get("id")
        away_id = away.get("id")

        home_rc = 0
        away_rc = 0
        home_ft = 0
        away_ft = 0
        home_ht = 0
        away_ht = 0

        for item in tl:
            if not isinstance(item, dict):
                continue

            # side 판별(최대한 많이 방어)
            resolved_side: Optional[str] = None

            side = item.get("side")
            if isinstance(side, str):
                s = side.strip().lower()
                if s in ("home", "away"):
                    resolved_side = s

            if resolved_side is None:
                side_home = item.get("side_home")
                if isinstance(side_home, bool):
                    resolved_side = "home" if side_home else "away"

            if resolved_side is None:
                team_id = item.get("team_id") or item.get("teamId")
                if team_id is not None:
                    if team_id == home_id:
                        resolved_side = "home"
                    elif team_id == away_id:
                        resolved_side = "away"

            # 레드카드
            if _is_red_event(item):
                if resolved_side == "home":
                    home_rc += 1
                elif resolved_side == "away":
                    away_rc += 1
                continue

            # 골(ft/ht)
            if _is_goal_event(item):
                if resolved_side == "home":
                    home_ft += 1
                elif resolved_side == "away":
                    away_ft += 1

                m = _get_minute(item)
                if m is not None and m <= 45:
                    if resolved_side == "home":
                        home_ht += 1
                    elif resolved_side == "away":
                        away_ht += 1
                continue

        # header에 반영
        home["red_cards"] = home_rc
        away["red_cards"] = away_rc

        home["ft"] = home_ft
        away["ft"] = away_ft
        home["ht"] = home_ht
        away["ht"] = away_ht

        # alias도 같이 맞춤(스코어블럭이 score를 볼 수도 있어서)
        if home.get("score") is not None or True:
            home["score"] = home_ft
        if away.get("score") is not None or True:
            away["score"] = away_ft

    header = build_header_block(
        fixture_id=fixture_id,
        league_id=league_id,
        season=season,
    )
    if header is None:
        return None

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

        if isinstance(patch, dict) and patch.get("hidden") is True:
            return None

        if isinstance(patch, dict) and patch:
            if isinstance(patch.get("header"), dict):
                header_patch = patch.get("header") or {}
                bundle_patch = {k: v for k, v in patch.items() if k not in ("header", "hidden")}
            else:
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

            if header_patch:
                header = _deep_merge(header, header_patch)
                _reconcile_header_aliases(header)

    form = build_form_block(header)
    timeline = build_timeline_block(header)
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

    if bundle_patch:
        bundle = _deep_merge(bundle, bundle_patch)

    # ✅ 최종 timeline 기준으로 header(red_cards/ft/ht/score) 재동기화
    _sync_header_from_timeline(bundle)

    return bundle




