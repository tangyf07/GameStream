-- GameStream G3 Doris sink for watermark / tumble demo (single-BE)
-- replication_num=1 REQUIRED; NOT G4–G6.

CREATE DATABASE IF NOT EXISTS ads;

USE ads;

CREATE TABLE IF NOT EXISTS g3_window_demo (
    window_start DATETIME     NOT NULL COMMENT 'TUMBLE start (event time)',
    window_end   DATETIME     NOT NULL COMMENT 'TUMBLE end (event time)',
    event_cnt    BIGINT       NULL DEFAULT "0" COMMENT 'deduped event count in window',
    player_cnt   BIGINT       NULL DEFAULT "0" COMMENT 'distinct player_id in window',
    metric_id    VARCHAR(64)  NULL DEFAULT "g3_window_demo",
    update_time  DATETIME     NULL DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(window_start, window_end)
DISTRIBUTED BY HASH(window_start) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
