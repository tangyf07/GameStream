-- DWD: cleaned player events (deduped, typed, partitioned by dt)
-- Prod: Doris UNIQUE KEY(event_id) 或 Iceberg upsert；见 doris_ads.sql / iceberg_notes.md
-- Flink: flink/sql/01_ods_clean.sql (upsert-kafka PRIMARY KEY event_id)

CREATE SCHEMA IF NOT EXISTS dwd;

CREATE TABLE IF NOT EXISTS dwd.player_events_clean (
    event_id      VARCHAR,
    event_type    VARCHAR,
    event_time    TIMESTAMP,
    dt            DATE,
    player_id     BIGINT,
    role_id       BIGINT,
    server_id     INTEGER,
    session_id    VARCHAR,
    payload       JSON,
    dungeon_id    INTEGER,
    amount_fen    BIGINT,
    online_sec    INTEGER
);

-- Doris: UNIQUE KEY(event_id) + PARTITION BY dt + HASH(player_id) BUCKETS 16
-- Iceberg: PARTITION BY dt, bucket(player_id, 16); MERGE INTO on event_id
