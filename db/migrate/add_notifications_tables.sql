-- add_notifications_tables.sql

-- 1) 디바이스 정보
CREATE TABLE IF NOT EXISTS user_devices (
    id                      BIGSERIAL PRIMARY KEY,
    device_id               TEXT NOT NULL UNIQUE,      -- 앱에서 생성한 고유 ID (UUID 등)
    fcm_token               TEXT NOT NULL,             -- FCM registration token
    platform                TEXT NOT NULL,             -- 'android', 'ios' 등
    app_version             TEXT,                      -- 예: '1.6.0'
    timezone                TEXT,                      -- 예: 'Asia/Seoul'
    language                TEXT,                      -- 예: 'ko'
    notifications_enabled   BOOLEAN NOT NULL DEFAULT TRUE,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_user_devices_device_id
    ON user_devices(device_id);

-- 2) 즐겨찾기 경기별 알림 설정
CREATE TABLE IF NOT EXISTS match_notification_subscriptions (
    id                      BIGSERIAL PRIMARY KEY,
    device_id               TEXT NOT NULL,
    match_id                INTEGER NOT NULL,          -- fixtures.fixture_id 참조용

    notify_kickoff          BOOLEAN NOT NULL DEFAULT TRUE,
    notify_score            BOOLEAN NOT NULL DEFAULT TRUE,
    notify_redcard          BOOLEAN NOT NULL DEFAULT TRUE,
    notify_ft               BOOLEAN NOT NULL DEFAULT TRUE,

    created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT fk_match_notif_device
        FOREIGN KEY (device_id)
        REFERENCES user_devices(device_id)
        ON DELETE CASCADE
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_match_notif_device_match
    ON match_notification_subscriptions(device_id, match_id);

CREATE INDEX IF NOT EXISTS idx_match_notif_match
    ON match_notification_subscriptions(match_id);
