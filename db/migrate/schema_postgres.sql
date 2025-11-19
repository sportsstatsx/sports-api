-- schema_postgres.sql
-- PostgreSQL schema equivalent of your current SQLite football.db

BEGIN;

-- Drop tables if they already exist (for re-runs)
DROP TABLE IF EXISTS predictions CASCADE;
DROP TABLE IF EXISTS team_season_stats CASCADE;
DROP TABLE IF EXISTS transfers CASCADE;
DROP TABLE IF EXISTS injuries CASCADE;
DROP TABLE IF EXISTS toplists CASCADE;
DROP TABLE IF EXISTS rounds CASCADE;
DROP TABLE IF EXISTS venues CASCADE;
DROP TABLE IF EXISTS coaches CASCADE;
DROP TABLE IF EXISTS squads CASCADE;
DROP TABLE IF EXISTS players CASCADE;
DROP TABLE IF EXISTS odds_history CASCADE;
DROP TABLE IF EXISTS odds CASCADE;
DROP TABLE IF EXISTS match_events_raw CASCADE;
DROP TABLE IF EXISTS match_player_stats CASCADE;
DROP TABLE IF EXISTS match_lineups CASCADE;
DROP TABLE IF EXISTS match_team_stats CASCADE;
DROP TABLE IF EXISTS match_events CASCADE;
DROP TABLE IF EXISTS standings CASCADE;
DROP TABLE IF EXISTS fixtures CASCADE;
DROP TABLE IF EXISTS matches CASCADE;
DROP TABLE IF EXISTS teams CASCADE;
DROP TABLE IF EXISTS leagues CASCADE;

-- 1) Base lookup tables
CREATE TABLE leagues (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    country     TEXT,
    logo        TEXT
);

CREATE TABLE teams (
    id          INTEGER PRIMARY KEY,
    name        TEXT NOT NULL,
    country     TEXT,
    logo        TEXT
);

-- 2) Matches / fixtures
CREATE TABLE matches (
    fixture_id      INTEGER PRIMARY KEY,
    league_id       INTEGER NOT NULL,
    season          INTEGER NOT NULL,
    date_utc        TIMESTAMPTZ NOT NULL,
    status          TEXT NOT NULL,
    status_group    TEXT NOT NULL,
    elapsed         INTEGER,
    home_id         INTEGER NOT NULL,
    away_id         INTEGER NOT NULL,
    home_ft         INTEGER,
    away_ft         INTEGER
);

CREATE INDEX idx_matches_league_season_date
    ON matches(league_id, season, date_utc);

CREATE TABLE fixtures (
    fixture_id      INTEGER PRIMARY KEY,
    league_id       INTEGER,
    season          INTEGER,
    date_utc        TIMESTAMPTZ,
    status          TEXT,
    status_group    TEXT
);

-- 3) Standings
CREATE TABLE standings (
    league_id       INTEGER NOT NULL,
    season          INTEGER NOT NULL,
    group_name      TEXT NOT NULL DEFAULT 'Overall',
    rank            INTEGER NOT NULL,
    team_id         INTEGER NOT NULL,
    points          INTEGER,
    goals_diff      INTEGER,
    played          INTEGER,
    win             INTEGER,
    draw            INTEGER,
    lose            INTEGER,
    goals_for       INTEGER,
    goals_against   INTEGER,
    form            TEXT,
    updated_utc     TIMESTAMPTZ,
    description     TEXT,
    PRIMARY KEY (league_id, season, group_name, team_id)
);

-- 4) Match events
CREATE TABLE match_events (
    id                  BIGSERIAL PRIMARY KEY,
    fixture_id          INTEGER NOT NULL,
    team_id             INTEGER,
    player_id           INTEGER,
    type                TEXT NOT NULL,
    detail              TEXT,
    minute              INTEGER NOT NULL,
    extra               INTEGER DEFAULT 0,
    assist_player_id    INTEGER,
    assist_name         TEXT,
    player_in_id        INTEGER,
    player_in_name      TEXT
);

CREATE INDEX idx_events_fix_min
    ON match_events(fixture_id, minute, extra);

CREATE INDEX idx_events_fix_type
    ON match_events(fixture_id, type);

CREATE INDEX idx_events_fix_team
    ON match_events(fixture_id, team_id);

-- 5) Match team stats
CREATE TABLE match_team_stats (
    fixture_id      INTEGER NOT NULL,
    team_id         INTEGER NOT NULL,
    name            TEXT NOT NULL,
    value           TEXT,
    PRIMARY KEY (fixture_id, team_id, name)
);

-- 6) Lineups
CREATE TABLE match_lineups (
    fixture_id      INTEGER NOT NULL,
    team_id         INTEGER NOT NULL,
    data_json       JSONB NOT NULL,
    updated_utc     TIMESTAMPTZ,
    PRIMARY KEY (fixture_id, team_id)
);

-- 7) Player stats
CREATE TABLE match_player_stats (
    fixture_id      INTEGER NOT NULL,
    player_id       INTEGER NOT NULL,
    data_json       JSONB NOT NULL,
    PRIMARY KEY (fixture_id, player_id)
);

-- 8) Raw events blob
CREATE TABLE match_events_raw (
    fixture_id      INTEGER PRIMARY KEY,
    data_json       JSONB NOT NULL
);

-- 9) Odds
CREATE TABLE odds (
    fixture_id      INTEGER NOT NULL,
    bookmaker       TEXT NOT NULL,
    market          TEXT NOT NULL,
    selection       TEXT NOT NULL,
    odd             TEXT,
    updated_at      TIMESTAMPTZ,
    data_json       JSONB NOT NULL DEFAULT '{}'::jsonb,
    PRIMARY KEY (fixture_id, bookmaker, market, selection)
);

CREATE UNIQUE INDEX ux_odds_fixture_book_market_sel
    ON odds(fixture_id, bookmaker, market, selection);

CREATE INDEX idx_odds_fixture
    ON odds(fixture_id);

CREATE INDEX idx_odds_book_market
    ON odds(bookmaker, market);

-- 10) Odds history
CREATE TABLE odds_history (
    id              BIGSERIAL PRIMARY KEY,
    fixture_id      INTEGER NOT NULL,
    bookmaker       TEXT NOT NULL,
    market          TEXT NOT NULL,
    selection       TEXT NOT NULL,
    odd             TEXT,
    updated_at      TIMESTAMPTZ,
    data_json       JSONB NOT NULL DEFAULT '{}'::jsonb,
    inserted_at     TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_odds_hist_fixture
    ON odds_history(fixture_id);

CREATE INDEX idx_odds_hist_key
    ON odds_history(bookmaker, market, selection);

-- 11) Players / squads / coaches / venues
CREATE TABLE players (
    player_id   INTEGER,
    team_id     INTEGER,
    season      INTEGER,
    data_json   JSONB NOT NULL,
    PRIMARY KEY (player_id, team_id, season)
);

CREATE TABLE squads (
    team_id     INTEGER,
    season      INTEGER,
    data_json   JSONB NOT NULL,
    PRIMARY KEY (team_id, season)
);

CREATE TABLE coaches (
    coach_id    INTEGER,
    team_id     INTEGER,
    season      INTEGER,
    data_json   JSONB NOT NULL,
    PRIMARY KEY (coach_id, team_id, season)
);

CREATE TABLE venues (
    venue_id    INTEGER PRIMARY KEY,
    data_json   JSONB NOT NULL
);

-- 12) Rounds / toplists / injuries / transfers / team_season_stats
CREATE TABLE rounds (
    league_id   INTEGER,
    season      INTEGER,
    round       TEXT,
    PRIMARY KEY (league_id, season, round)
);

CREATE TABLE toplists (
    league_id   INTEGER,
    season      INTEGER,
    kind        TEXT,
    rank        INTEGER,
    data_json   JSONB NOT NULL,
    PRIMARY KEY (league_id, season, kind, rank)
);

CREATE TABLE injuries (
    player_id   INTEGER,
    team_id     INTEGER,
    season      INTEGER,
    data_json   JSONB NOT NULL,
    PRIMARY KEY (player_id, team_id, season)
);

CREATE TABLE transfers (
    player_id   INTEGER,
    season      INTEGER,
    data_json   JSONB NOT NULL,
    PRIMARY KEY (player_id, season)
);

CREATE TABLE team_season_stats (
    league_id   INTEGER,
    season      INTEGER,
    team_id     INTEGER,
    name        TEXT NOT NULL,
    value       TEXT,
    PRIMARY KEY (league_id, season, team_id, name)
);

-- 13) Predictions
CREATE TABLE predictions (
    fixture_id  INTEGER PRIMARY KEY,
    data_json   JSONB NOT NULL
);

COMMIT;

