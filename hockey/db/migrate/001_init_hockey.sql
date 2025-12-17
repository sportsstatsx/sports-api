-- hockey/db/migrate/001_init_hockey.sql
-- SportsStatsX - Hockey DB Schema (API-Sports Hockey)
-- 핵심 목표:
-- 1) leagues(리그/컵) + seasons
-- 2) teams
-- 3) games(경기) + game_events(골/패널티 등 이벤트)
-- 4) standings(순위표)
-- 5) odds(현재 API에서 비어있어도 테이블은 준비)

BEGIN;

-- =========================================================
-- 0) 공통: updated_at 자동 갱신 트리거
-- =========================================================
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- =========================================================
-- 1) Country (API 응답에 country 객체가 항상 들어옴)
-- =========================================================
CREATE TABLE IF NOT EXISTS hockey_countries (
  id           INTEGER PRIMARY KEY,            -- API: country.id
  name         TEXT NOT NULL,                  -- API: country.name
  code         TEXT,                           -- API: country.code (nullable)
  flag         TEXT,                           -- API: country.flag (nullable)
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_hockey_countries_updated_at ON hockey_countries;
CREATE TRIGGER trg_hockey_countries_updated_at
BEFORE UPDATE ON hockey_countries
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_hockey_countries_code ON hockey_countries(code);

-- =========================================================
-- 2) League (leagues 응답)
-- =========================================================
CREATE TABLE IF NOT EXISTS hockey_leagues (
  id           INTEGER PRIMARY KEY,            -- API: league.id
  name         TEXT NOT NULL,                  -- API: league.name
  type         TEXT NOT NULL,                  -- API: league.type (League/Cup)
  logo         TEXT,                           -- API: league.logo
  country_id   INTEGER REFERENCES hockey_countries(id) ON DELETE SET NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_hockey_leagues_updated_at ON hockey_leagues;
CREATE TRIGGER trg_hockey_leagues_updated_at
BEFORE UPDATE ON hockey_leagues
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_hockey_leagues_country_id ON hockey_leagues(country_id);
CREATE INDEX IF NOT EXISTS idx_hockey_leagues_type ON hockey_leagues(type);

-- =========================================================
-- 3) League Seasons (leagues 응답의 seasons 배열)
--    예: { season: 2021, current: false, start, end }
-- =========================================================
CREATE TABLE IF NOT EXISTS hockey_league_seasons (
  league_id    INTEGER NOT NULL REFERENCES hockey_leagues(id) ON DELETE CASCADE,
  season       INTEGER NOT NULL,               -- API: seasons[].season
  current      BOOLEAN NOT NULL DEFAULT FALSE, -- API: seasons[].current
  start_date   DATE,                           -- API: seasons[].start
  end_date     DATE,                           -- API: seasons[].end
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (league_id, season)
);

DROP TRIGGER IF EXISTS trg_hockey_league_seasons_updated_at ON hockey_league_seasons;
CREATE TRIGGER trg_hockey_league_seasons_updated_at
BEFORE UPDATE ON hockey_league_seasons
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_hockey_league_seasons_current ON hockey_league_seasons(current);

-- =========================================================
-- 4) Team (standings/events에서 team 객체 제공)
-- =========================================================
CREATE TABLE IF NOT EXISTS hockey_teams (
  id           INTEGER PRIMARY KEY,            -- API: team.id
  name         TEXT NOT NULL,                  -- API: team.name
  logo         TEXT,                           -- API: team.logo
  country_id   INTEGER REFERENCES hockey_countries(id) ON DELETE SET NULL,
  created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_hockey_teams_updated_at ON hockey_teams;
CREATE TRIGGER trg_hockey_teams_updated_at
BEFORE UPDATE ON hockey_teams
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_hockey_teams_country_id ON hockey_teams(country_id);
CREATE INDEX IF NOT EXISTS idx_hockey_teams_name ON hockey_teams(name);

-- =========================================================
-- 5) Games (경기 마스터)
--    - API-Sports hockey의 fixture/game 상세를 수집할 때 저장
--    - 스코어/기간별 점수/상태 등은 JSONB로 유연하게(하키 리그마다 구조 다를 수 있음)
-- =========================================================
CREATE TABLE IF NOT EXISTS hockey_games (
  id              INTEGER PRIMARY KEY,         -- API: game.id (또는 fixture.id)
  league_id        INTEGER NOT NULL REFERENCES hockey_leagues(id) ON DELETE RESTRICT,
  season           INTEGER NOT NULL,
  -- stage/group 같은 구분이 standings에 존재하므로 게임에도 옵션으로 둠(리그별로 유용)
  stage            TEXT,                       -- 예: "OHL"
  group_name       TEXT,                       -- 예: "Western Conference" (리그 따라 없을 수 있음)

  -- 팀
  home_team_id     INTEGER REFERENCES hockey_teams(id) ON DELETE SET NULL,
  away_team_id     INTEGER REFERENCES hockey_teams(id) ON DELETE SET NULL,

  -- 일정/상태
  game_date        TIMESTAMPTZ,                -- 경기 시작 시간
  status           TEXT,                       -- 예: "NS", "LIVE", "FT" 등
  status_long      TEXT,                       -- 더 긴 설명(있으면)
  live_timer       TEXT,                       -- ✅ LIVE 진행 시간/타이머 (API-Sports: timer)
  timezone         TEXT,                       -- 저장해두면 앱 타임존 처리에 도움

  -- 스코어: period별 점수 등 변형이 많아서 JSONB 추천
  -- 예: { "total": {"home": 4,"away": 2}, "periods": {"P1":{"home":1,"away":0}, ... } }
  score_json       JSONB NOT NULL DEFAULT '{}'::jsonb,

  -- 원본 API payload 백업 (디버깅/추가필드 필요할 때 안전장치)
  raw_json         JSONB NOT NULL DEFAULT '{}'::jsonb,

  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_hockey_games_updated_at ON hockey_games;
CREATE TRIGGER trg_hockey_games_updated_at
BEFORE UPDATE ON hockey_games
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_hockey_games_league_season ON hockey_games(league_id, season);
CREATE INDEX IF NOT EXISTS idx_hockey_games_date ON hockey_games(game_date);
CREATE INDEX IF NOT EXISTS idx_hockey_games_home ON hockey_games(home_team_id);
CREATE INDEX IF NOT EXISTS idx_hockey_games_away ON hockey_games(away_team_id);
CREATE INDEX IF NOT EXISTS idx_hockey_games_status ON hockey_games(status);

-- =========================================================
-- 6) Game Events (네가 준 games?game=398684 응답 형태 그대로 대응)
--    예:
--    { period:"P1", minute:"03", team:{...}, players:["..."], assists:["..."], comment:"Power-play", type:"goal" }
-- =========================================================
CREATE TABLE IF NOT EXISTS hockey_game_events (
  id              BIGSERIAL PRIMARY KEY,
  game_id         INTEGER NOT NULL REFERENCES hockey_games(id) ON DELETE CASCADE,

  -- 이벤트 시점
  period          TEXT NOT NULL,               -- API: period (예: P1/P2/P3/OT)
  minute          SMALLINT,                    -- API: minute 문자열이지만 숫자화 (예: "03" -> 3)

  -- 이벤트 주체 팀
  team_id         INTEGER REFERENCES hockey_teams(id) ON DELETE SET NULL,

  -- 내용
  type            TEXT NOT NULL,               -- API: type (goal 등)
  comment         TEXT,                        -- API: comment ("Power-play" 등)

  -- 사람 이름 목록 (API에 string 배열로 옴)
  players         TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],   -- API: players[]
  assists         TEXT[] NOT NULL DEFAULT ARRAY[]::TEXT[],   -- API: assists[]

  -- 중복방지/정렬용(같은 분/같은 이벤트가 여러개 가능)
  -- 수집 로직에서 period+minute+team_id+type+players[0] 등을 조합해 넣어도 되고
  -- 그냥 API 수신 순서대로 1..n 넣어도 됨.
  event_order     INTEGER NOT NULL DEFAULT 0,

  raw_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_hockey_game_events_updated_at ON hockey_game_events;
CREATE TRIGGER trg_hockey_game_events_updated_at
BEFORE UPDATE ON hockey_game_events
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_hockey_game_events_game ON hockey_game_events(game_id);
CREATE INDEX IF NOT EXISTS idx_hockey_game_events_team ON hockey_game_events(team_id);
CREATE INDEX IF NOT EXISTS idx_hockey_game_events_time ON hockey_game_events(game_id, period, minute, event_order);
CREATE INDEX IF NOT EXISTS idx_hockey_game_events_type ON hockey_game_events(type);

-- (선택) 중복 삽입 방지용 유니크 키 (수집 방식 확정되면 유지/조정)
-- minute가 NULL일 수 있어 완벽하진 않지만 goal 같은 건 대부분 minute 존재
CREATE UNIQUE INDEX IF NOT EXISTS uq_hockey_game_events_dedupe
ON hockey_game_events (game_id, period, minute, team_id, type, event_order);

-- =========================================================
-- 7) Standings (네가 준 standings 응답 형태 그대로 대응)
--    - response가 [[ {...}, {...} ]] 처럼 2중 배열로 옴 -> 내부 배열을 "rows"로 저장하면 됨
--    예:
--    position, stage, group.name, team, league(season 포함), games(win/lose/OT), goals, points, form, description
-- =========================================================
CREATE TABLE IF NOT EXISTS hockey_standings (
  league_id        INTEGER NOT NULL REFERENCES hockey_leagues(id) ON DELETE CASCADE,
  season           INTEGER NOT NULL,

  stage            TEXT,                       -- API: stage (예: "OHL")
  group_name       TEXT,                       -- API: group.name (예: "Western Conference")

  team_id          INTEGER NOT NULL REFERENCES hockey_teams(id) ON DELETE CASCADE,
  position         INTEGER NOT NULL,           -- API: position

  games_played     INTEGER,
  win_total        INTEGER,
  win_pct          NUMERIC(7,3),               -- API: "0.618"
  win_ot_total     INTEGER,                    -- API: win_overtime.total
  win_ot_pct       NUMERIC(7,3),

  lose_total       INTEGER,
  lose_pct         NUMERIC(7,3),
  lose_ot_total    INTEGER,                    -- API: lose_overtime.total
  lose_ot_pct      NUMERIC(7,3),

  goals_for        INTEGER,
  goals_against    INTEGER,
  points           INTEGER,

  form             TEXT,                       -- API: form ("WWWWW" / "WWWLWO" 등)
  description      TEXT,                       -- API: description

  raw_json         JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  PRIMARY KEY (league_id, season, stage, group_name, team_id)
);

DROP TRIGGER IF EXISTS trg_hockey_standings_updated_at ON hockey_standings;
CREATE TRIGGER trg_hockey_standings_updated_at
BEFORE UPDATE ON hockey_standings
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_hockey_standings_lookup
ON hockey_standings(league_id, season, stage, group_name, position);

CREATE INDEX IF NOT EXISTS idx_hockey_standings_team
ON hockey_standings(team_id);

-- =========================================================
-- 8) Odds (네가 준 odds 예시는 results:0 이었지만 테이블은 준비)
--    - API 구조가 리그/북메이커/마켓/밸류로 갈 수 있어서 raw_json 중심으로 유연하게
-- =========================================================
CREATE TABLE IF NOT EXISTS hockey_odds (
  id              BIGSERIAL PRIMARY KEY,
  game_id         INTEGER NOT NULL REFERENCES hockey_games(id) ON DELETE CASCADE,

  -- API가 "id" 파라미터로 조회되는 값이 게임ID일 수도/오즈ID일 수도 있어서 둘 다 저장 가능
  api_odds_id     TEXT,                        -- 요청 파라미터 id 그대로 저장해도 됨

  provider        TEXT,                        -- 북메이커/프로바이더 (있으면)
  market          TEXT,                        -- 예: "1X2", "Moneyline", "Totals" 등
  selection       TEXT,                        -- 예: "Home", "Away", "Over", "Under"
  odd_value       NUMERIC(12,4),               -- 숫자로 파싱 가능하면 저장

  raw_json        JSONB NOT NULL DEFAULT '{}'::jsonb,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

DROP TRIGGER IF EXISTS trg_hockey_odds_updated_at ON hockey_odds;
CREATE TRIGGER trg_hockey_odds_updated_at
BEFORE UPDATE ON hockey_odds
FOR EACH ROW
EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_hockey_odds_game ON hockey_odds(game_id);
CREATE INDEX IF NOT EXISTS idx_hockey_odds_api_odds_id ON hockey_odds(api_odds_id);

-- =========================================================
-- 9) (선택) 스키마 버전 테이블 (수동 migrate 추적용)
--    - 축구쪽에 이미 비슷한 방식이 있다면 맞춰서 이름 바꿔도 됨
-- =========================================================
CREATE TABLE IF NOT EXISTS hockey_schema_migrations (
  version       TEXT PRIMARY KEY,  -- 예: "001_init_hockey"
  applied_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

INSERT INTO hockey_schema_migrations(version)
VALUES ('001_init_hockey')
ON CONFLICT (version) DO NOTHING;

COMMIT;
