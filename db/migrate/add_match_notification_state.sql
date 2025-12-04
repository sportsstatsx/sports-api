-- db/migrate/add_match_notification_state.sql

CREATE TABLE IF NOT EXISTS match_notification_state (
    match_id        INTEGER PRIMARY KEY,
    last_status     TEXT NOT NULL,
    last_home_goals INTEGER NOT NULL DEFAULT 0,
    last_away_goals INTEGER NOT NULL DEFAULT 0,
    last_home_red   INTEGER NOT NULL DEFAULT 0,
    last_away_red   INTEGER NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_match_notif_state_updated
    ON match_notification_state(updated_at);
