# matchdetail/bundle_service.py

import json
from typing import Dict, Any, Optional, List, Tuple

from db import fetch_one

from matchdetail.header_block import build_header_block
from matchdetail.timeline_block import build_timeline_block
from matchdetail.stats_block import build_stats_block
from matchdetail.lineups_block import build_lineups_block


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
        "SELECT patch FROM match_overrides WHERE fixture_id=%s",
        (fixture_id,),
    )
    if not row:
        return {}

    patch = row.get("patch") or {}
    # JSON이 문자열로 들어오는 환경도 방어
    if isinstance(patch, str):
        try:
            patch = json.loads(patch)
        except Exception:
            return {}
    return patch if isinstance(patch, dict) else {}


def _recalc_cards_from_timeline(timeline: Any) -> Tuple[int, int, int, int]:
    """
    timeline 이벤트 리스트(서버 타임라인 블록 형식)에서
    RED/YELLOW 카드 수를 (home_red, away_red, home_yellow, away_yellow)로 재계산
    """
    if not isinstance(timeline, list):
        return 0, 0, 0, 0

    home_red = away_red = home_yellow = away_yellow = 0
    for e in timeline:
        if not isinstance(e, dict):
            continue
        t = str(e.get("type") or "").upper()
        side = str(e.get("side") or "").lower()

        if side not in ("home", "away"):
            continue

        if t == "RED":
            if side == "home":
                home_red += 1
            else:
                away_red += 1
        elif t == "YELLOW":
            if side == "home":
                home_yellow += 1
            else:
                away_yellow += 1

    return home_red, away_red, home_yellow, away_yellow


def _apply_timeline_card_counts_into_header(data: Dict[str, Any]) -> None:
    """
    최종 timeline 결과를 기준으로 header의 red/yellow 카운트를 덮어쓴다.
    (override로 타임라인에서 카드를 지웠을 때 스코어블럭도 같이 변해야 함)
    """
    header = data.get("header")
    if not isinstance(header, dict):
        return

    timeline = data.get("timeline")
    hr, ar, hy, ay = _recalc_cards_from_timeline(timeline)

    home = header.get("home")
    away = header.get("away")
    if isinstance(home, dict):
        home["red_cards"] = hr
        home["yellow_cards"] = hy
    if isinstance(away, dict):
        away["red_cards"] = ar
        away["yellow_cards"] = ay


def get_match_detail_bundle(
    fixture_id: int,
    league_id: int,
    season: int,
    *,
    apply_override: bool = True
) -> Optional[Dict[str, Any]]:

    header = build_header_block(fixture_id, league_id, season)
    if not header:
        return None

    # 원본 블록 생성
    data: Dict[str, Any] = {
        "header": header,
        "timeline": build_timeline_block(header),
        "stats": build_stats_block(header),
        "lineups": build_lineups_block(header),
    }

    # ✅ override 적용
    if apply_override:
        patch = _load_override_patch(fixture_id)
        if patch:
            # 1) header 패치: (a) patch["header"] 있으면 적용
            #              (b) 나머지 키 중 block키가 아닌 것들은 header로 간주하고 적용
            header_patch: Dict[str, Any] = {}
            if isinstance(patch.get("header"), dict):
                header_patch = _deep_merge(header_patch, patch["header"])

            for k, v in patch.items():
                if k in ("header", "timeline", "stats", "lineups"):
                    continue
                header_patch[k] = v

            if header_patch:
                data["header"] = _deep_merge(data["header"], header_patch)

            # 2) block 패치
            for k in ("timeline", "stats", "lineups"):
                if k in patch:
                    data[k] = _deep_merge(data.get(k), patch[k])

            # 3) ✅ 타임라인 최종 결과 기반으로 카드 카운트 재계산 → 스코어블럭까지 동기화
            _apply_timeline_card_counts_into_header(data)

            # 디버그/표시용
            if isinstance(data["header"], dict):
                data["header"]["_has_override"] = True

    return data
