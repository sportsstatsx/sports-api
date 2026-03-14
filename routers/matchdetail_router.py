from flask import Blueprint, request, jsonify
from matchdetail.bundle_service import get_match_detail_bundle

matchdetail_bp = Blueprint("matchdetail", __name__)


@matchdetail_bp.route("/api/match_detail_bundle", methods=["GET"])
def match_detail_bundle():
    """
    ✅ 완전무결 매치디테일 번들:
    - fixture_id, league_id 는 필수
    - season 은 optional (오염돼도 무시하고 DB의 "그 경기 season"으로 강제 고정)
    """
    try:
        fixture_id = request.args.get("fixture_id", type=int)
        league_id = request.args.get("league_id", type=int)
        season = request.args.get("season", type=int)  # optional

        comp = request.args.get("comp")
        last_n = request.args.get("last_n")
        bracket_round = request.args.get("bracket_round")

        # ✅ optional: 부분 로딩용 parts
        # 예) parts=header,form  /  parts=timeline  /  parts=stats,h2h
        parts_raw = request.args.get("parts")
        parts = None
        if parts_raw:
            parts = [p.strip() for p in str(parts_raw).split(",") if p.strip()]


        ao_raw = request.args.get("apply_override")
        if ao_raw is None:
            apply_override = True
        else:
            v = str(ao_raw).strip().lower()
            apply_override = not (v in ("0", "false", "no", "off"))

        if fixture_id is None or league_id is None:
            return (
                jsonify(
                    {
                        "ok": False,
                        "error": "fixture_id, league_id are required",
                    }
                ),
                400,
            )

        # ✅ 1) 해당 fixture의 진짜 season을 DB에서 가져와서 강제 사용
        from db import fetch_one

        row = fetch_one(
            """
            SELECT season, league_id
            FROM matches
            WHERE fixture_id = %s
            LIMIT 1
            """,
            (fixture_id,),
        )

        if row and row.get("season") is not None:
            real_season = int(row.get("season"))
            # league_id가 불일치하면 DB league_id를 우선(데이터 정합성)
            real_league_id = row.get("league_id")
            if real_league_id is not None:
                league_id = int(real_league_id)
            season = real_season
        else:
            # ✅ 2) fixture가 matches에 아직 없다면(극초기) league_id 기준 보정
            from leaguedetail.seasons_block import resolve_season_for_league

            season = resolve_season_for_league(league_id=league_id, season=season)

        if season is None:
            return jsonify({"ok": False, "error": "season_not_resolvable"}), 400

        bundle = get_match_detail_bundle(
            fixture_id=fixture_id,
            league_id=league_id,
            season=season,
            comp=comp,
            last_n=last_n,
            bracket_round=bracket_round,
            apply_override=apply_override,
            parts=parts,
        )


        if not bundle:
            return jsonify({"ok": False, "error": "Match not found"}), 404

        resp = jsonify({"ok": True, "data": bundle})

        # ✅ bundle_service에서 만든 타이밍(있으면) 헤더로 노출
        # - prod에서는 나중에 제거하거나 ADMIN만 허용
        try:
            if isinstance(bundle, dict):
                perf = bundle.get("_perf")
                if isinstance(perf, dict):
                    # 문자열로 축약해서 헤더 1개에 담기
                    # 예: total=1.05;header=0.12;stats=0.40...
                    parts = []
                    for k in ("total","header","form","timeline","lineups","stats","h2h","standings","insights","ai"):
                        v = perf.get(k)
                        if isinstance(v, (int, float)):
                            parts.append(f"{k}={v:.3f}")
                    if parts:
                        resp.headers["X-MD-Perf"] = ";".join(parts)
        except Exception:
            pass

        return resp


    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500

