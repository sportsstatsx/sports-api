# services/home_ui_config.py
from __future__ import annotations

import os
import json
from typing import Any, Dict, List, Optional

# ✅ 운영 편의:
# - 기본은 코드 내 DEFAULT_CONFIG
# - 필요하면 ENV(HOME_UI_CONFIG_JSON)로 덮어쓰기 가능
# - (나중에 admin에서 저장/수정까지 가고 싶으면 DB로 확장 가능)

DEFAULT_CONFIG: Dict[str, Any] = {
    "version": 1,
    "football": {
        # ✅ 지원 리그(없으면 전체 허용처럼 동작하도록 할 수도 있지만,
        # 운영 안정성 위해 "명시된 것만" 노출을 추천)
        "supported_league_ids": [],  # []면 "제한 없음"으로 처리(아래 로직에서)
        # ✅ 상단 탭(필터) 순서
        "league_order": [],          # 비어있으면 DB 정렬(기존) 유지
        # ✅ 매치리스트 섹션 순서 (앱에서 사용)
        "section_order": ["Favorites", "Live", "Top", "Other"],
        # ✅ league_id -> section_key (앱에서 그룹핑)
        "league_section_map": {},

        # (선택) 디렉터리 섹션 순서도 따로 두고 싶으면
        "directory_section_order": [],
    },
    "hockey": {
        "supported_league_ids": [],
        "league_order": [],
        "section_order": ["Favorites", "Live", "Top", "Other"],
        "league_section_map": {},
        "directory_section_order": [],
    },
}

_cached: Optional[Dict[str, Any]] = None


def _deep_merge(base: Any, patch: Any) -> Any:
    if isinstance(base, dict) and isinstance(patch, dict):
        out = dict(base)
        for k, v in patch.items():
            if k in out:
                out[k] = _deep_merge(out[k], v)
            else:
                out[k] = v
        return out
    return patch


def load_home_ui_config() -> Dict[str, Any]:
    global _cached
    if _cached is not None:
        return _cached

    cfg = dict(DEFAULT_CONFIG)

    raw = (os.getenv("HOME_UI_CONFIG_JSON", "") or "").strip()
    if raw:
        try:
            patch = json.loads(raw)
            if isinstance(patch, dict):
                cfg = _deep_merge(cfg, patch)
        except Exception:
            pass

    _cached = cfg
    return cfg


def get_sport_config(sport: str) -> Dict[str, Any]:
    cfg = load_home_ui_config()
    s = (sport or "").strip().lower()
    if s not in ("football", "hockey"):
        s = "football"
    sport_cfg = cfg.get(s)
    return sport_cfg if isinstance(sport_cfg, dict) else {}


def apply_supported_and_order(
    *,
    sport: str,
    rows: List[Dict[str, Any]],
    league_id_key: str = "league_id",
) -> List[Dict[str, Any]]:
    """
    rows: [{"league_id":..., ...}, ...]
    - supported_league_ids 있으면 필터
    - league_order 있으면 그 순서로 정렬(없는 애들은 뒤로)
    """
    scfg = get_sport_config(sport)
    supported = scfg.get("supported_league_ids")
    order = scfg.get("league_order")

    out = list(rows)

    # 1) allowlist
    if isinstance(supported, list) and len(supported) > 0:
        allow = set()
        for x in supported:
            try:
                allow.add(int(x))
            except Exception:
                continue
        out = [r for r in out if int(r.get(league_id_key) or 0) in allow]

    # 2) ordering
    if isinstance(order, list) and len(order) > 0:
        rank: Dict[int, int] = {}
        for i, x in enumerate(order):
            try:
                rank[int(x)] = i
            except Exception:
                continue

        def _k(r: Dict[str, Any]) -> tuple:
            lid = int(r.get(league_id_key) or 0)
            return (rank.get(lid, 10**9), lid)

        out.sort(key=_k)

    return out
