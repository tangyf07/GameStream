-- GameStream G4 Doris sink for recharge idempotency demo (single-BE)
-- replication_num=1 REQUIRED; NOT G5–G6.
-- Consistency: UNIQUE KEY absorbs at-least-once JDBC/load retries — NOT EO-2PC.

CREATE DATABASE IF NOT EXISTS ads;

USE ads;

CREATE TABLE IF NOT EXISTS g4_recharge_di (
    metric_id     VARCHAR(64)   NOT NULL COMMENT 'fixed bucket key g4_recharge',
    recharge_cnt  BIGINT        NULL DEFAULT "0" COMMENT 'deduped recharge event count',
    amount_sum    DECIMAL(18,2) NULL DEFAULT "0" COMMENT 'deduped amount sum',
    player_cnt    BIGINT        NULL DEFAULT "0" COMMENT 'distinct paying players',
    update_time   DATETIME      NULL DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(metric_id)
DISTRIBUTED BY HASH(metric_id) BUCKETS 1
PROPERTIES (
    "replication_num" = "1"
);
