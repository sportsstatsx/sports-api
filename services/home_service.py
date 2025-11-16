def get_team_season_stats(team_id: int, league_id: int) -> Optional[Dict[str, Any]]:
    """
    team_season_stats 테이블에서 (league_id, team_id)에 해당하는
    가장 최신 season 한 줄을 가져오고, 거기에 insights_overall.* 지표를
    추가/보정해서 반환한다.

    ⚠️ 이번 버전에서 추가된 것:
      - match_events 를 이용해서 Timing / First Goal / Momentum 지표를
        실제 경기 이벤트 기반으로 계산해서 insights_overall 에 넣는다.
    """
    rows = fetch_all(
        """
        SELECT
            league_id,
            season,
            team_id,
            name,
            value
        FROM team_season_stats
        WHERE league_id = %s
          AND team_id   = %s
        ORDER BY season DESC
        LIMIT 1
        """,
        (league_id, team_id),
    )
    if not rows:
        return None

    row = rows[0]

    # value(JSON)를 파싱
    raw_value = row.get("value")
    if isinstance(raw_value, str):
        try:
            stats = json.loads(raw_value)
        except Exception:
            stats = {}
    elif isinstance(raw_value, dict):
        stats = raw_value
    else:
        stats = {}

    if not isinstance(stats, dict):
        stats = {}

    # insights_overall 보장
    insights = stats.get("insights_overall")
    if not isinstance(insights, dict):
        insights = {}
        stats["insights_overall"] = insights

    fixtures = stats.get("fixtures") or {}
    played = fixtures.get("played") or {}
    matches_total_api = played.get("total") or 0

    # 공통 유틸
    def safe_div(num, den) -> float:
        try:
            num_f = float(num)
        except (TypeError, ValueError):
            return 0.0
        try:
            den_f = float(den)
        except (TypeError, ValueError):
            return 0.0
        if den_f == 0:
            return 0.0
        return num_f / den_f

    def fmt_pct(n, d) -> int:
        v = safe_div(n, d)
        return int(round(v * 100)) if v > 0 else 0

    def fmt_avg(n, d) -> float:
        v = safe_div(n, d)
        return round(v, 2) if v > 0 else 0.0

    # 시즌
    season = row.get("season")
    try:
        season_int = int(season)
    except (TypeError, ValueError):
        season_int = None

    # ─────────────────────────────
    # Shooting & Efficiency (Shots)
    # ─────────────────────────────
    if season_int is not None:
        shot_rows = fetch_all(
            """
            SELECT
                m.fixture_id,
                m.home_id,
                m.away_id,
                SUM(
                    CASE
                        WHEN lower(mts.name) IN ('total shots','shots total','shots')
                             AND mts.value ~ '^[0-9]+$'
                        THEN mts.value::int
                        ELSE 0
                    END
                ) AS total_shots,
                SUM(
                    CASE
                        WHEN lower(mts.name) IN (
                            'shots on goal',
                            'shotsongoal',
                            'shots on target'
                        )
                        AND mts.value ~ '^[0-9]+$'
                        THEN mts.value::int
                        ELSE 0
                    END
                ) AS shots_on_goal
            FROM matches m
            LEFT JOIN match_team_stats mts
              ON mts.fixture_id = m.fixture_id
             AND mts.team_id   = %s
            WHERE m.league_id = %s
              AND m.season    = %s
              AND (%s = m.home_id OR %s = m.away_id)
              AND (
                    lower(m.status_group) IN ('finished','ft','fulltime')
                 OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
              )
            GROUP BY m.fixture_id, m.home_id, m.away_id
            """,
            (team_id, league_id, season_int, team_id, team_id),
        )

        if shot_rows:
            total_matches = 0
            home_matches = 0
            away_matches = 0

            total_shots_total = 0
            total_shots_home = 0
            total_shots_away = 0

            sog_total = 0
            sog_home = 0
            sog_away = 0

            for r2 in shot_rows:
                ts = r2["total_shots"] or 0
                sog = r2["shots_on_goal"] or 0

                is_home = (r2["home_id"] == team_id)
                is_away = (r2["away_id"] == team_id)
                if not (is_home or is_away):
                    continue

                total_matches += 1
                total_shots_total += ts
                sog_total += sog

                if is_home:
                    home_matches += 1
                    total_shots_home += ts
                    sog_home += sog
                else:
                    away_matches += 1
                    total_shots_away += ts
                    sog_away += sog

            # API 쪽 fixtures.played 값이 없으면 실제 경기 수 사용
            eff_total = matches_total_api or total_matches or 0
            eff_home = home_matches or 0
            eff_away = away_matches or 0

            # shots 블록도 같이 기록 (나중에 재사용 가능)
            stats["shots"] = {
                "total": {
                    "total": int(total_shots_total),
                    "home": int(total_shots_home),
                    "away": int(total_shots_away),
                },
                "on": {
                    "total": int(sog_total),
                    "home": int(sog_home),
                    "away": int(sog_away),
                },
            }

            avg_total = fmt_avg(total_shots_total, eff_total) if eff_total > 0 else 0.0
            avg_home = fmt_avg(total_shots_home, eff_home) if eff_home > 0 else 0.0
            avg_away = fmt_avg(total_shots_away, eff_away) if eff_away > 0 else 0.0

            insights["shots_per_match"] = {
                "total": avg_total,
                "home": avg_home,
                "away": avg_away,
            }
            insights["shots_on_target_pct"] = {
                "total": fmt_pct(sog_total, total_shots_total),
                "home": fmt_pct(sog_home, total_shots_home),
                "away": fmt_pct(sog_away, total_shots_away),
            }

    # ─────────────────────────────
    # Outcome & Totals / Result Combos
    # ─────────────────────────────
    match_rows: List[Dict[str, Any]] = []
    if season_int is not None:
        match_rows = fetch_all(
            """
            SELECT
                m.fixture_id,
                m.home_id,
                m.away_id,
                m.home_ft,
                m.away_ft,
                m.status_group
            FROM matches m
            WHERE m.league_id = %s
              AND m.season    = %s
              AND (%s = m.home_id OR %s = m.away_id)
              AND (
                    lower(m.status_group) IN ('finished','ft','fulltime')
                 OR (m.home_ft IS NOT NULL AND m.away_ft IS NOT NULL)
              )
            """,
            (league_id, season_int, team_id, team_id),
        )

    if match_rows:
        mt_tot = mh_tot = ma_tot = 0

        win_t = win_h = win_a = 0
        draw_t = draw_h = draw_a = 0
        lose_t = lose_h = lose_a = 0

        btts_t = btts_h = btts_a = 0
        team_o05_t = team_o05_h = team_o05_a = 0
        team_o15_t = team_o15_h = team_o15_a = 0
        o15_t = o15_h = o15_a = 0
        o25_t = o25_h = o25_a = 0
        win_o25_t = win_o25_h = win_o25_a = 0
        lose_btts_t = lose_btts_h = lose_btts_a = 0

        cs_t = cs_h = cs_a = 0
        ng_t = ng_h = ng_a = 0

        gf_sum_t = gf_sum_h = gf_sum_a = 0.0
        ga_sum_t = ga_sum_h = ga_sum_a = 0.0

        for mr in match_rows:
            home_id = mr["home_id"]
            away_id = mr["away_id"]
            home_ft = mr["home_ft"]
            away_ft = mr["away_ft"]

            if home_ft is None or away_ft is None:
                continue

            is_home = (team_id == home_id)
            gf = home_ft if is_home else away_ft
            ga = away_ft if is_home else home_ft

            if gf is None or ga is None:
                continue

            mt_tot += 1
            if is_home:
                mh_tot += 1
            else:
                ma_tot += 1

            if gf > ga:
                win_t += 1
                if is_home:
                    win_h += 1
                else:
                    win_a += 1
            elif gf == ga:
                draw_t += 1
                if is_home:
                    draw_h += 1
                else:
                    draw_a += 1
            else:
                lose_t += 1
                if is_home:
                    lose_h += 1
                else:
                    lose_a += 1

            gf_sum_t += gf
            ga_sum_t += ga
            if is_home:
                gf_sum_h += gf
                ga_sum_h += ga
            else:
                gf_sum_a += gf
                ga_sum_a += ga

            if gf > 0 and ga > 0:
                btts_t += 1
                if is_home:
                    btts_h += 1
                else:
                    btts_a += 1

            if gf >= 1:
                team_o05_t += 1
                if is_home:
                    team_o05_h += 1
                else:
                    team_o05_a += 1
            if gf >= 2:
                team_o15_t += 1
                if is_home:
                    team_o15_h += 1
                else:
                    team_o15_a += 1

            total_goals = gf + ga
            if total_goals >= 2:
                o15_t += 1
                if is_home:
                    o15_h += 1
                else:
                    o15_a += 1
            if total_goals >= 3:
                o25_t += 1
                if is_home:
                    o25_h += 1
                else:
                    o25_a += 1

            if gf > ga and total_goals >= 3:
                win_o25_t += 1
                if is_home:
                    win_o25_h += 1
                else:
                    win_o25_a += 1

            if gf < ga and gf > 0 and ga > 0:
                lose_btts_t += 1
                if is_home:
                    lose_btts_h += 1
                else:
                    lose_btts_a += 1

            if ga == 0:
                cs_t += 1
                if is_home:
                    cs_h += 1
                else:
                    cs_a += 1
            if gf == 0:
                ng_t += 1
                if is_home:
                    ng_h += 1
                else:
                    ng_a += 1

        if mt_tot > 0:
            insights.setdefault(
                "win_pct",
                {
                    "total": fmt_pct(win_t, mt_tot),
                    "home": fmt_pct(win_h, mh_tot or mt_tot),
                    "away": fmt_pct(win_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "btts_pct",
                {
                    "total": fmt_pct(btts_t, mt_tot),
                    "home": fmt_pct(btts_h, mh_tot or mt_tot),
                    "away": fmt_pct(btts_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "team_over05_pct",
                {
                    "total": fmt_pct(team_o05_t, mt_tot),
                    "home": fmt_pct(team_o05_h, mh_tot or mt_tot),
                    "away": fmt_pct(team_o05_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "team_over15_pct",
                {
                    "total": fmt_pct(team_o15_t, mt_tot),
                    "home": fmt_pct(team_o15_h, mh_tot or mt_tot),
                    "away": fmt_pct(team_o15_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "over15_pct",
                {
                    "total": fmt_pct(o15_t, mt_tot),
                    "home": fmt_pct(o15_h, mh_tot or mt_tot),
                    "away": fmt_pct(o15_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "over25_pct",
                {
                    "total": fmt_pct(o25_t, mt_tot),
                    "home": fmt_pct(o25_h, mh_tot or mt_tot),
                    "away": fmt_pct(o25_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "clean_sheet_pct",
                {
                    "total": fmt_pct(cs_t, mt_tot),
                    "home": fmt_pct(cs_h, mh_tot or mt_tot),
                    "away": fmt_pct(cs_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "no_goals_pct",
                {
                    "total": fmt_pct(ng_t, mt_tot),
                    "home": fmt_pct(ng_h, mh_tot or mt_tot),
                    "away": fmt_pct(ng_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "win_and_over25_pct",
                {
                    "total": fmt_pct(win_o25_t, mt_tot),
                    "home": fmt_pct(win_o25_h, mh_tot or mt_tot),
                    "away": fmt_pct(win_o25_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "lose_and_btts_pct",
                {
                    "total": fmt_pct(lose_btts_t, mt_tot),
                    "home": fmt_pct(lose_btts_h, mh_tot or mt_tot),
                    "away": fmt_pct(lose_btts_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "draw_pct",
                {
                    "total": fmt_pct(draw_t, mt_tot),
                    "home": fmt_pct(draw_h, mh_tot or mt_tot),
                    "away": fmt_pct(draw_a, ma_tot or mt_tot),
                },
            )
            insights.setdefault(
                "goal_diff_avg",
                {
                    "total": fmt_avg(gf_sum_t - ga_sum_t, mt_tot),
                    "home": fmt_avg(gf_sum_h - ga_sum_h, mh_tot or mt_tot),
                    "away": fmt_avg(gf_sum_a - ga_sum_a, ma_tot or mt_tot),
                },
            )

    # ─────────────────────────────
    # Timing / First Goal / Momentum
    #   - match_events 기반
    # ─────────────────────────────
    if match_rows:
        fixture_ids = [mr["fixture_id"] for mr in match_rows]
        events_by_fixture: Dict[int, List[Dict[str, Any]]] = {}

        if fixture_ids:
            placeholders = ",".join(["%s"] * len(fixture_ids))
            sql = f"""
                SELECT
                    e.fixture_id,
                    e.minute,
                    e.team_id
                FROM match_events e
                WHERE e.fixture_id IN ({placeholders})
                  AND e.minute IS NOT NULL
                  AND lower(e.type) IN ('goal','own goal','penalty','penalty goal')
                ORDER BY e.fixture_id, e.minute ASC
            """
            event_rows = fetch_all(sql, tuple(fixture_ids))
            for ev in event_rows:
                fid = ev["fixture_id"]
                events_by_fixture.setdefault(fid, []).append(ev)

        # 샘플 수
        half_mt_tot = half_mt_home = half_mt_away = 0

        # Timing: 득점/실점 구간 플래그
        score_1h_t = score_1h_h = score_1h_a = 0
        score_2h_t = score_2h_h = score_2h_a = 0
        concede_1h_t = concede_1h_h = concede_1h_a = 0
        concede_2h_t = concede_2h_h = concede_2h_a = 0

        score_015_t = score_015_h = score_015_a = 0
        concede_015_t = concede_015_h = concede_015_a = 0
        score_8090_t = score_8090_h = score_8090_a = 0
        concede_8090_t = concede_8090_h = concede_8090_a = 0

        # First goal 샘플
        fg_sample_t = fg_sample_h = fg_sample_a = 0
        fg_for_t = fg_for_h = fg_for_a = 0
        fg_against_t = fg_against_h = fg_against_a = 0

        # Momentum: 리드/열세일 때 결과
        leading_sample_t = leading_sample_h = leading_sample_a = 0
        leading_win_t = leading_win_h = leading_win_a = 0
        leading_draw_t = leading_draw_h = leading_draw_a = 0
        leading_loss_t = leading_loss_h = leading_loss_a = 0

        trailing_sample_t = trailing_sample_h = trailing_sample_a = 0
        trailing_win_t = trailing_win_h = trailing_win_a = 0
        trailing_draw_t = trailing_draw_h = trailing_draw_a = 0
        trailing_loss_t = trailing_loss_h = trailing_loss_a = 0

        for mr in match_rows:
            fid = mr["fixture_id"]
            home_id = mr["home_id"]
            away_id = mr["away_id"]
            home_ft = mr["home_ft"]
            away_ft = mr["away_ft"]

            if home_ft is None or away_ft is None:
                continue

            is_home = (team_id == home_id)
            gf = home_ft if is_home else away_ft
            ga = away_ft if is_home else home_ft

            evs = events_by_fixture.get(fid)
            if not evs:
                continue

            half_mt_tot += 1
            if is_home:
                half_mt_home += 1
            else:
                half_mt_away += 1

            scored_1h = conceded_1h = False
            scored_2h = conceded_2h = False
            scored_015 = conceded_015 = False
            scored_8090 = conceded_8090 = False

            first_minute: Optional[int] = None
            first_for: Optional[bool] = None

            for ev in evs:
                minute = ev["minute"]
                if minute is None:
                    continue
                try:
                    m_int = int(minute)
                except (TypeError, ValueError):
                    continue

                is_for_goal = (ev["team_id"] == team_id)

                # first goal
                if first_minute is None or m_int < first_minute:
                    first_minute = m_int
                    first_for = is_for_goal

                # 1H / 2H
                if m_int <= 45:
                    if is_for_goal:
                        scored_1h = True
                    else:
                        conceded_1h = True
                else:
                    if is_for_goal:
                        scored_2h = True
                    else:
                        conceded_2h = True

                # 0-15
                if m_int <= 15:
                    if is_for_goal:
                        scored_015 = True
                    else:
                        conceded_015 = True

                # 80+
                if m_int >= 80:
                    if is_for_goal:
                        scored_8090 = True
                    else:
                        conceded_8090 = True

            # Timing 카운트
            def _inc(flag: bool, total_ref, home_ref, away_ref):
                if not flag:
                    return
                if is_home:
                    home_ref[0] += 1
                else:
                    away_ref[0] += 1
                total_ref[0] += 1

            # Python에서 ref 처리를 위해 리스트 래핑
            t_ref = [score_1h_t]
            h_ref = [score_1h_h]
            a_ref = [score_1h_a]
            _inc(scored_1h, t_ref, h_ref, a_ref)
            score_1h_t, score_1h_h, score_1h_a = t_ref[0], h_ref[0], a_ref[0]

            t_ref = [score_2h_t]
            h_ref = [score_2h_h]
            a_ref = [score_2h_a]
            _inc(scored_2h, t_ref, h_ref, a_ref)
            score_2h_t, score_2h_h, score_2h_a = t_ref[0], h_ref[0], a_ref[0]

            t_ref = [concede_1h_t]
            h_ref = [concede_1h_h]
            a_ref = [concede_1h_a]
            _inc(conceded_1h, t_ref, h_ref, a_ref)
            concede_1h_t, concede_1h_h, concede_1h_a = t_ref[0], h_ref[0], a_ref[0]

            t_ref = [concede_2h_t]
            h_ref = [concede_2h_h]
            a_ref = [concede_2h_a]
            _inc(conceded_2h, t_ref, h_ref, a_ref)
            concede_2h_t, concede_2h_h, concede_2h_a = t_ref[0], h_ref[0], a_ref[0]

            t_ref = [score_015_t]
            h_ref = [score_015_h]
            a_ref = [score_015_a]
            _inc(scored_015, t_ref, h_ref, a_ref)
            score_015_t, score_015_h, score_015_a = t_ref[0], h_ref[0], a_ref[0]

            t_ref = [concede_015_t]
            h_ref = [concede_015_h]
            a_ref = [concede_015_a]
            _inc(conceded_015, t_ref, h_ref, a_ref)
            concede_015_t, concede_015_h, concede_015_a = t_ref[0], h_ref[0], a_ref[0]

            t_ref = [score_8090_t]
            h_ref = [score_8090_h]
            a_ref = [score_8090_a]
            _inc(scored_8090, t_ref, h_ref, a_ref)
            score_8090_t, score_8090_h, score_8090_a = t_ref[0], h_ref[0], a_ref[0]

            t_ref = [concede_8090_t]
            h_ref = [concede_8090_h]
            a_ref = [concede_8090_a]
            _inc(conceded_8090, t_ref, h_ref, a_ref)
            concede_8090_t, concede_8090_h, concede_8090_a = t_ref[0], h_ref[0], a_ref[0]

            # First goal & Momentum
            if first_minute is not None and first_for is not None:
                fg_sample_t += 1
                if is_home:
                    fg_sample_h += 1
                else:
                    fg_sample_a += 1

                if first_for:
                    fg_for_t += 1
                    if is_home:
                        fg_for_h += 1
                    else:
                        fg_for_a += 1

                    # 리딩 상태에서 결과
                    leading_sample_t += 1
                    if is_home:
                        leading_sample_h += 1
                    else:
                        leading_sample_a += 1

                    if gf > ga:
                        leading_win_t += 1
                        if is_home:
                            leading_win_h += 1
                        else:
                            leading_win_a += 1
                    elif gf == ga:
                        leading_draw_t += 1
                        if is_home:
                            leading_draw_h += 1
                        else:
                            leading_draw_a += 1
                    else:
                        leading_loss_t += 1
                        if is_home:
                            leading_loss_h += 1
                        else:
                            leading_loss_a += 1
                else:
                    fg_against_t += 1
                    if is_home:
                        fg_against_h += 1
                    else:
                        fg_against_a += 1

                    # 트레일링 상태에서 결과
                    trailing_sample_t += 1
                    if is_home:
                        trailing_sample_h += 1
                    else:
                        trailing_sample_a += 1

                    if gf > ga:
                        trailing_win_t += 1
                        if is_home:
                            trailing_win_h += 1
                        else:
                            trailing_win_a += 1
                    elif gf == ga:
                        trailing_draw_t += 1
                        if is_home:
                            trailing_draw_h += 1
                        else:
                            trailing_draw_a += 1
                    else:
                        trailing_loss_t += 1
                        if is_home:
                            trailing_loss_h += 1
                        else:
                            trailing_loss_a += 1

        # 퍼센트 저장
        if half_mt_tot > 0:
            insights["score_1h_pct"] = {
                "total": fmt_pct(score_1h_t, half_mt_tot),
                "home": fmt_pct(score_1h_h, half_mt_home or half_mt_tot),
                "away": fmt_pct(score_1h_a, half_mt_away or half_mt_tot),
            }
            insights["score_2h_pct"] = {
                "total": fmt_pct(score_2h_t, half_mt_tot),
                "home": fmt_pct(score_2h_h, half_mt_home or half_mt_tot),
                "away": fmt_pct(score_2h_a, half_mt_away or half_mt_tot),
            }
            insights["concede_1h_pct"] = {
                "total": fmt_pct(concede_1h_t, half_mt_tot),
                "home": fmt_pct(concede_1h_h, half_mt_home or half_mt_tot),
                "away": fmt_pct(concede_1h_a, half_mt_away or half_mt_tot),
            }
            insights["concede_2h_pct"] = {
                "total": fmt_pct(concede_2h_t, half_mt_tot),
                "home": fmt_pct(concede_2h_h, half_mt_home or half_mt_tot),
                "away": fmt_pct(concede_2h_a, half_mt_away or half_mt_tot),
            }
            insights["score_0_15_pct"] = {
                "total": fmt_pct(score_015_t, half_mt_tot),
                "home": fmt_pct(score_015_h, half_mt_home or half_mt_tot),
                "away": fmt_pct(score_015_a, half_mt_away or half_mt_tot),
            }
            insights["concede_0_15_pct"] = {
                "total": fmt_pct(concede_015_t, half_mt_tot),
                "home": fmt_pct(concede_015_h, half_mt_home or half_mt_tot),
                "away": fmt_pct(concede_015_a, half_mt_away or half_mt_tot),
            }
            insights["score_80_90_pct"] = {
                "total": fmt_pct(score_8090_t, half_mt_tot),
                "home": fmt_pct(score_8090_h, half_mt_home or half_mt_tot),
                "away": fmt_pct(score_8090_a, half_mt_away or half_mt_tot),
            }
            insights["concede_80_90_pct"] = {
                "total": fmt_pct(concede_8090_t, half_mt_tot),
                "home": fmt_pct(concede_8090_h, half_mt_home or half_mt_tot),
                "away": fmt_pct(concede_8090_a, half_mt_away or half_mt_tot),
            }

        if fg_sample_t > 0:
            insights["first_to_score_pct"] = {
                "total": fmt_pct(fg_for_t, fg_sample_t),
                "home": fmt_pct(fg_for_h, fg_sample_h or fg_sample_t),
                "away": fmt_pct(fg_for_a, fg_sample_a or fg_sample_t),
            }
            insights["first_conceded_pct"] = {
                "total": fmt_pct(fg_against_t, fg_sample_t),
                "home": fmt_pct(fg_against_h, fg_sample_h or fg_sample_t),
                "away": fmt_pct(fg_against_a, fg_sample_a or fg_sample_t),
            }

        if leading_sample_t > 0:
            insights["when_leading_win_pct"] = {
                "total": fmt_pct(leading_win_t, leading_sample_t),
                "home": fmt_pct(leading_win_h, leading_sample_h or leading_sample_t),
                "away": fmt_pct(leading_win_a, leading_sample_a or leading_sample_t),
            }
            insights["when_leading_draw_pct"] = {
                "total": fmt_pct(leading_draw_t, leading_sample_t),
                "home": fmt_pct(leading_draw_h, leading_sample_h or leading_sample_t),
                "away": fmt_pct(leading_draw_a, leading_sample_a or leading_sample_t),
            }
            insights["when_leading_loss_pct"] = {
                "total": fmt_pct(leading_loss_t, leading_sample_t),
                "home": fmt_pct(leading_loss_h, leading_sample_h or leading_sample_t),
                "away": fmt_pct(leading_loss_a, leading_sample_a or leading_sample_t),
            }

        if trailing_sample_t > 0:
            insights["when_trailing_win_pct"] = {
                "total": fmt_pct(trailing_win_t, trailing_sample_t),
                "home": fmt_pct(trailing_win_h, trailing_sample_h or trailing_sample_t),
                "away": fmt_pct(trailing_win_a, trailing_sample_a or trailing_sample_t),
            }
            insights["when_trailing_draw_pct"] = {
                "total": fmt_pct(trailing_draw_t, trailing_sample_t),
                "home": fmt_pct(trailing_draw_h, trailing_sample_h or trailing_sample_t),
                "away": fmt_pct(trailing_draw_a, trailing_sample_a or trailing_sample_t),
            }
            insights["when_trailing_loss_pct"] = {
                "total": fmt_pct(trailing_loss_t, trailing_sample_t),
                "home": fmt_pct(trailing_loss_h, trailing_sample_h or trailing_sample_t),
                "away": fmt_pct(trailing_loss_a, trailing_sample_a or trailing_sample_t),
            }

        # 샘플 수도 함께 저장
        insights["events_sample"] = half_mt_tot
        insights["first_goal_sample"] = fg_sample_t

    # 최종 반환
    return {
        "league_id": row["league_id"],
        "season": row["season"],
        "team_id": row["team_id"],
        "name": row.get("name"),
        "value": stats,
    }
