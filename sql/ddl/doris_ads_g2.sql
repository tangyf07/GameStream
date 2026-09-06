-- GameStream G2 Doris ADS (single-BE demo)
-- replication_num=1 REQUIRED for one BE; no dynamic_partition for simpler bring-up.
-- Aligns with config/metrics.yaml metric_id: ads_dau_di / ads_pay_rate_di

CREATE DATABASE IF NOT EXISTS ads;

USE ads;

CREATE TABLE IF NOT EXISTS ads_dau_di (
    dt          DATE         NOT NULL COMMENT '业务日',
    server_id   INT          NOT NULL COMMENT '游戏服',
    dau         BIGINT       REPLACE NULL_DEFAULT 0 COMMENT '当日去重玩家数',
    metric_id   VARCHAR(64)  REPLACE DEFAULT 'ads_dau_di',
    update_time DATETIME     REPLACE DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(dt, server_id)
DISTRIBUTED BY HASH(server_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);

CREATE TABLE IF NOT EXISTS ads_pay_rate_di (
    dt          DATE         NOT NULL,
    server_id   INT          NOT NULL,
    dau         BIGINT       REPLACE NULL_DEFAULT 0,
    pay_users   BIGINT       REPLACE NULL_DEFAULT 0,
    pay_rate    DOUBLE       REPLACE COMMENT 'pay_users / dau',
    metric_id   VARCHAR(64)  REPLACE DEFAULT 'ads_pay_rate_di',
    update_time DATETIME     REPLACE DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(dt, server_id)
DISTRIBUTED BY HASH(server_id) BUCKETS 4
PROPERTIES (
    "replication_num" = "1"
);
