-- =============================================================================
-- Flink SQL: 近实时 ADS（DAU / 付费率 / ARPU / 副本通关率）
-- 面试要点：
--   * 从 DWS upsert 流再聚合；PRIMARY KEY 覆盖写 → 看板可订正
--   * Retention / Churn：跨天窗口，放 Spark + sql/metrics（本文件不算）
--   * metric_id 与 config/metrics.yaml、Doris ads 表一致，供 DataPilot 引用
-- 本地对照：local_runner.step_ads
-- =============================================================================

CREATE TABLE dws_player_rt_src (
    window_start      TIMESTAMP(3),
    window_end        TIMESTAMP(3),
    dt                DATE,
    server_id         INT,
    player_id         BIGINT,
    event_cnt         BIGINT,
    login_cnt         BIGINT,
    enter_dungeon_cnt BIGINT,
    clear_dungeon_cnt BIGINT,
    recharge_cnt      BIGINT,
    recharge_fen      BIGINT,
    online_sec_sum    BIGINT,
    PRIMARY KEY (dt, server_id, player_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gamestream.dws.player_behavior_di',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- --- DAU -------------------------------------------------------------------
CREATE TABLE ads_dau_di_rt (
    dt         DATE,
    server_id  INT,
    dau        BIGINT,
    metric_id  STRING,
    PRIMARY KEY (dt, server_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gamestream.ads.metrics_stream',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO ads_dau_di_rt
SELECT
    dt,
    server_id,
    COUNT(DISTINCT player_id) AS dau,
    'ads_dau_di' AS metric_id
FROM dws_player_rt_src
GROUP BY dt, server_id;

-- --- 付费率（与 sql/metrics/ads_pay_rate_di.sql 同口径）----------------------
CREATE TABLE ads_pay_rate_di_rt (
    dt         DATE,
    server_id  INT,
    dau        BIGINT,
    pay_users  BIGINT,
    pay_rate   DOUBLE,
    metric_id  STRING,
    PRIMARY KEY (dt, server_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gamestream.ads.pay_rate_di',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO ads_pay_rate_di_rt
SELECT
    dt,
    server_id,
    COUNT(DISTINCT player_id) AS dau,
    COUNT(DISTINCT CASE WHEN recharge_cnt > 0 THEN player_id END) AS pay_users,
    CAST(COUNT(DISTINCT CASE WHEN recharge_cnt > 0 THEN player_id END) AS DOUBLE)
        / NULLIF(COUNT(DISTINCT player_id), 0) AS pay_rate,
    'ads_pay_rate_di' AS metric_id
FROM dws_player_rt_src
GROUP BY dt, server_id;

-- --- ARPU（元）--------------------------------------------------------------
CREATE TABLE ads_arpu_di_rt (
    dt           DATE,
    server_id    INT,
    dau          BIGINT,
    revenue_cny  DOUBLE,
    arpu_cny     DOUBLE,
    metric_id    STRING,
    PRIMARY KEY (dt, server_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gamestream.ads.arpu_di',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO ads_arpu_di_rt
SELECT
    dt,
    server_id,
    COUNT(DISTINCT player_id) AS dau,
    SUM(recharge_fen) / 100.0 AS revenue_cny,
    (SUM(recharge_fen) / 100.0) / NULLIF(COUNT(DISTINCT player_id), 0) AS arpu_cny,
    'ads_arpu_di' AS metric_id
FROM dws_player_rt_src
GROUP BY dt, server_id;

-- --- 副本通关率：需 dungeon 粒度；从 DWD 另开源（避免玩家日表丢 dungeon_id）-
CREATE TABLE dwd_events_for_dungeon (
    event_id      STRING,
    event_type    STRING,
    event_time    TIMESTAMP(3),
    dt            DATE,
    player_id     BIGINT,
    server_id     INT,
    dungeon_id    INT,
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'gamestream.dwd.player_events_clean',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'gamestream-ads-dungeon-rt',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

CREATE TABLE ads_dungeon_clear_rate_di_rt (
    dt          DATE,
    server_id   INT,
    dungeon_id  INT,
    enter_cnt   BIGINT,
    clear_cnt   BIGINT,
    clear_rate  DOUBLE,
    metric_id   STRING,
    PRIMARY KEY (dt, server_id, dungeon_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gamestream.ads.dungeon_clear_rate_di',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO ads_dungeon_clear_rate_di_rt
SELECT
    CAST(window_start AS DATE) AS dt,
    server_id,
    dungeon_id,
    SUM(CASE WHEN event_type = 'enter_dungeon' THEN 1 ELSE 0 END) AS enter_cnt,
    SUM(CASE WHEN event_type = 'clear_dungeon' THEN 1 ELSE 0 END) AS clear_cnt,
    CAST(SUM(CASE WHEN event_type = 'clear_dungeon' THEN 1 ELSE 0 END) AS DOUBLE)
        / NULLIF(SUM(CASE WHEN event_type = 'enter_dungeon' THEN 1 ELSE 0 END), 0) AS clear_rate,
    'ads_dungeon_clear_rate_di' AS metric_id
FROM TABLE(
    TUMBLE(TABLE dwd_events_for_dungeon, DESCRIPTOR(event_time), INTERVAL '1' DAY)
)
WHERE dungeon_id IS NOT NULL
GROUP BY window_start, window_end, server_id, dungeon_id;

-- 在线时长近实时：对 online_sec_sum 再聚合（logout 已在 DWS 累加）
CREATE TABLE ads_online_duration_di_rt (
    dt                DATE,
    server_id         INT,
    total_online_sec  BIGINT,
    players           BIGINT,
    avg_online_sec    DOUBLE,
    metric_id         STRING,
    PRIMARY KEY (dt, server_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gamestream.ads.online_duration_di',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

INSERT INTO ads_online_duration_di_rt
SELECT
    dt,
    server_id,
    SUM(online_sec_sum) AS total_online_sec,
    COUNT(DISTINCT player_id) AS players,
    CAST(SUM(online_sec_sum) AS DOUBLE) / NULLIF(COUNT(DISTINCT player_id), 0) AS avg_online_sec,
    'ads_online_duration_di' AS metric_id
FROM dws_player_rt_src
GROUP BY dt, server_id;

-- Retention / Churn：见 sql/metrics/ads_retention_nd.sql、ads_churn_di.sql + Spark batch
