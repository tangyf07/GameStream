-- ODS DDL — DuckDB-compatible; Doris notes in comments
-- Layer: raw landing of player behavior events

CREATE SCHEMA IF NOT EXISTS ods;

-- DuckDB
CREATE TABLE IF NOT EXISTS ods.player_events (
    event_id      VARCHAR,
    event_type    VARCHAR,
    event_time    TIMESTAMP,
    player_id     BIGINT,
    role_id       BIGINT,
    server_id     INTEGER,
    session_id    VARCHAR,
    payload       JSON,
    dungeon_id    INTEGER,
    amount_fen    BIGINT,
    online_sec    INTEGER
);

-- Doris (production reference):
-- CREATE TABLE ods.player_events (
--   event_id VARCHAR(64), event_type VARCHAR(32), event_time DATETIME,
--   player_id BIGINT, role_id BIGINT, server_id INT, session_id VARCHAR(64),
--   payload JSON, dungeon_id INT, amount_fen BIGINT, online_sec INT
-- ) DUPLICATE KEY(event_id, event_time)
-- PARTITION BY RANGE(event_time) ()
-- DISTRIBUTED BY HASH(player_id) BUCKETS 16
-- PROPERTIES ("replication_num"="3");
