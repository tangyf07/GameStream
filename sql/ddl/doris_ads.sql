-- Apache Doris ADS 表设计（游戏行为指标）
-- 与 config/metrics.yaml / sql/metrics/*.sql / DuckDB ads.* 口径一致
-- 模型：UNIQUE KEY 覆盖写，便于 Flink upsert-kafka / Routine Load 幂等刷新

-- =============================================================================
-- ads_dau_di — 日活
-- =============================================================================
CREATE TABLE IF NOT EXISTS ads.ads_dau_di (
    dt          DATE         NOT NULL COMMENT '业务日',
    server_id   INT          NOT NULL COMMENT '游戏服',
    dau         BIGINT       REPLACE NULL_DEFAULT 0 COMMENT '当日去重玩家数',
    metric_id   VARCHAR(64)  REPLACE DEFAULT 'ads_dau_di',
    update_time DATETIME     REPLACE DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(dt, server_id)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(server_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);

-- =============================================================================
-- ads_retention_nd — N 日留存（cohort）
-- =============================================================================
CREATE TABLE IF NOT EXISTS ads.ads_retention_nd (
    cohort_dt       DATE         NOT NULL COMMENT '新增日',
    server_id       INT          NOT NULL,
    n_days          INT          NOT NULL COMMENT '1/3/7',
    cohort_size     BIGINT       REPLACE NULL_DEFAULT 0,
    retained_cnt    BIGINT       REPLACE NULL_DEFAULT 0,
    retention_rate  DOUBLE       REPLACE,
    metric_id       VARCHAR(64)  REPLACE DEFAULT 'ads_retention_nd',
    update_time     DATETIME     REPLACE DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(cohort_dt, server_id, n_days)
PARTITION BY RANGE(cohort_dt) ()
DISTRIBUTED BY HASH(server_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-60",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);

-- =============================================================================
-- ads_online_duration_di — 日在线时长
-- =============================================================================
CREATE TABLE IF NOT EXISTS ads.ads_online_duration_di (
    dt                DATE         NOT NULL,
    server_id         INT          NOT NULL,
    total_online_sec  BIGINT       REPLACE NULL_DEFAULT 0,
    players           BIGINT       REPLACE NULL_DEFAULT 0,
    avg_online_sec    DOUBLE       REPLACE,
    metric_id         VARCHAR(64)  REPLACE DEFAULT 'ads_online_duration_di',
    update_time       DATETIME     REPLACE DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(dt, server_id)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(server_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);

-- =============================================================================
-- ads_pay_rate_di — 付费率
-- =============================================================================
CREATE TABLE IF NOT EXISTS ads.ads_pay_rate_di (
    dt          DATE         NOT NULL,
    server_id   INT          NOT NULL,
    dau         BIGINT       REPLACE NULL_DEFAULT 0,
    pay_users   BIGINT       REPLACE NULL_DEFAULT 0,
    pay_rate    DOUBLE       REPLACE COMMENT 'pay_users / dau',
    metric_id   VARCHAR(64)  REPLACE DEFAULT 'ads_pay_rate_di',
    update_time DATETIME     REPLACE DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(dt, server_id)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(server_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);

-- =============================================================================
-- ads_arpu_di — ARPU（元）
-- =============================================================================
CREATE TABLE IF NOT EXISTS ads.ads_arpu_di (
    dt           DATE         NOT NULL,
    server_id    INT          NOT NULL,
    dau          BIGINT       REPLACE NULL_DEFAULT 0,
    revenue_cny  DOUBLE       REPLACE NULL_DEFAULT 0 COMMENT 'SUM(amount_fen)/100',
    arpu_cny     DOUBLE       REPLACE COMMENT 'revenue_cny / dau',
    metric_id    VARCHAR(64)  REPLACE DEFAULT 'ads_arpu_di',
    update_time  DATETIME     REPLACE DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(dt, server_id)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(server_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);

-- =============================================================================
-- ads_dungeon_clear_rate_di — 副本通关率
-- =============================================================================
CREATE TABLE IF NOT EXISTS ads.ads_dungeon_clear_rate_di (
    dt          DATE         NOT NULL,
    server_id   INT          NOT NULL,
    dungeon_id  INT          NOT NULL,
    enter_cnt   BIGINT       REPLACE NULL_DEFAULT 0,
    clear_cnt   BIGINT       REPLACE NULL_DEFAULT 0,
    clear_rate  DOUBLE       REPLACE COMMENT 'clear_cnt / enter_cnt',
    metric_id   VARCHAR(64)  REPLACE DEFAULT 'ads_dungeon_clear_rate_di',
    update_time DATETIME     REPLACE DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(dt, server_id, dungeon_id)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(dungeon_id) BUCKETS 16
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "16"
);

-- =============================================================================
-- ads_churn_di — 流失风险
-- =============================================================================
CREATE TABLE IF NOT EXISTS ads.ads_churn_di (
    dt                DATE         NOT NULL,
    server_id         INT          NOT NULL,
    active_7d_users   BIGINT       REPLACE NULL_DEFAULT 0,
    churn_risk_users  BIGINT       REPLACE NULL_DEFAULT 0 COMMENT '7日活跃且近3日沉默',
    churn_risk_rate   DOUBLE       REPLACE,
    metric_id         VARCHAR(64)  REPLACE DEFAULT 'ads_churn_di',
    update_time       DATETIME     REPLACE DEFAULT CURRENT_TIMESTAMP
)
UNIQUE KEY(dt, server_id)
PARTITION BY RANGE(dt) ()
DISTRIBUTED BY HASH(server_id) BUCKETS 8
PROPERTIES (
    "replication_num" = "1",
    "dynamic_partition.enable" = "true",
    "dynamic_partition.time_unit" = "DAY",
    "dynamic_partition.start" = "-30",
    "dynamic_partition.end" = "3",
    "dynamic_partition.prefix" = "p",
    "dynamic_partition.buckets" = "8"
);

-- 设计要点（面试）：
-- 1) UNIQUE KEY = 指标自然主键，Flink upsert / Spark overwrite 分区可幂等。
-- 2) HASH(server_id) 或 HASH(dungeon_id)：查询常按服/副本过滤；桶数按服数量与 QPS 调。
-- 3) 动态分区保留 30~60 天热数据；冷数据可沉到 Iceberg/对象存储（见 iceberg_notes.md）。
-- 4) metric_id 冗余便于 DataPilot 统一扫表 / Union 多指标视图。
