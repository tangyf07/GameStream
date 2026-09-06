-- =============================================================================
-- Flink SQL: DWD → DWS 日窗玩家行为（实时）
-- 面试要点：
--   * TUMBLE 1 DAY on event_time：按业务日关窗，非处理时间
--   * GROUP BY server_id, player_id → keyed state（每 key 一份累加器）
--   * Watermark 10s：窗口触发略晚于 ODS，给清洗链路留缓冲
--   * 热点玩家倾斜：见 docs/flink-kafka-deep-dive.md §3（盐拆 key / 两阶段）
--   * 留存/流失不在此算：跨天 lookback → Spark / DuckDB batch
--   * 大 DAU：RocksDB + 增量 checkpoint；改并行度走 savepoint（conf + deep-dive §6–§7）
-- 本地对照：local_runner.step_dws → dws.player_behavior_di
-- Job stub：flink/jobs/dws_ads_submit.py
-- =============================================================================

CREATE TABLE dwd_events_src (
    event_id      STRING,
    event_type    STRING,
    event_time    TIMESTAMP(3),
    dt            DATE,
    player_id     BIGINT,
    role_id       BIGINT,
    server_id     INT,
    session_id    STRING,
    dungeon_id    INT,
    amount_fen    BIGINT,
    online_sec    INT,
    WATERMARK FOR event_time AS event_time - INTERVAL '10' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'gamestream.dwd.player_events_clean',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'gamestream-dws-rt',
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

CREATE TABLE dws_player_behavior_rt (
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

-- 日滚动：关窗后 upsert 覆盖当日该玩家行；迟到数据策略见 deep-dive §4
INSERT INTO dws_player_behavior_rt
SELECT
    window_start,
    window_end,
    CAST(window_start AS DATE) AS dt,
    server_id,
    player_id,
    COUNT(*) AS event_cnt,
    SUM(CASE WHEN event_type = 'login' THEN 1 ELSE 0 END) AS login_cnt,
    SUM(CASE WHEN event_type = 'enter_dungeon' THEN 1 ELSE 0 END) AS enter_dungeon_cnt,
    SUM(CASE WHEN event_type = 'clear_dungeon' THEN 1 ELSE 0 END) AS clear_dungeon_cnt,
    SUM(CASE WHEN event_type = 'recharge' THEN 1 ELSE 0 END) AS recharge_cnt,
    SUM(CASE WHEN event_type = 'recharge' THEN COALESCE(amount_fen, 0) ELSE 0 END) AS recharge_fen,
    SUM(CASE WHEN event_type = 'logout' THEN COALESCE(online_sec, 0) ELSE 0 END) AS online_sec_sum
FROM TABLE(
    TUMBLE(TABLE dwd_events_src, DESCRIPTOR(event_time), INTERVAL '1' DAY)
)
GROUP BY window_start, window_end, server_id, player_id;
