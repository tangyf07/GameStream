-- =============================================================================
-- Flink SQL: Kafka ODS → DWD clean (production-shaped)
-- 面试要点：
--   * event_time WATERMARK：bounded out-of-orderness 5s，抗弱网乱序
--   * group.id = gamestream-ods-clean：消费组；真正一致性靠 Checkpoint 存 offset
--   * upsert-kafka + PRIMARY KEY(event_id)：at-least-once 重放下幂等去重
--   * 过滤 11 类事件，与 simulator/schemas/events.json、local_runner DWD 对齐
-- 本地对照：pipeline/local_runner.py :: step_ods_dwd
-- 深挖文档：docs/flink-kafka-deep-dive.md
-- =============================================================================

CREATE TABLE kafka_ods_player_events (
    event_id      STRING,
    event_type    STRING,
    event_time    TIMESTAMP(3),
    player_id     BIGINT,
    role_id       BIGINT,
    server_id     INT,
    session_id    STRING,
    -- payload 只展开指标相关字段；完整 JSON 可另存 RAW topic / Iceberg
    payload       ROW<
        dungeon_id INT,
        amount_fen BIGINT,
        online_sec INT,
        client_version STRING,
        device_os STRING,
        channel STRING
    >,
    -- 乱序容忍 5s；游戏客户端时钟漂移大时可调大或加 idleness
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'gamestream.ods.player_events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'gamestream-ods-clean',
    -- 生产常与 checkpoint 配合用 group-offsets / timestamp；demo 用 latest
    'scan.startup.mode' = 'latest-offset',
    'format' = 'json'
);

CREATE TABLE dwd_player_events_clean (
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
    -- NOT ENFORCED：Kafka 不强制约束；语义靠 upsert 覆盖
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gamestream.dwd.player_events_clean',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

-- 清洗：非空主键、合法类型枚举；重复 event_id 由 upsert 覆盖（等价 ROW_NUMBER=1）
INSERT INTO dwd_player_events_clean
SELECT
    event_id,
    event_type,
    event_time,
    CAST(event_time AS DATE) AS dt,
    player_id,
    role_id,
    server_id,
    session_id,
    payload.dungeon_id,
    payload.amount_fen,
    payload.online_sec
FROM kafka_ods_player_events
WHERE event_id IS NOT NULL
  AND player_id IS NOT NULL
  AND player_id > 0
  AND event_type IN (
      'login','create_role','enter_dungeon','clear_dungeon','death',
      'equip','enhance','recharge','gacha','friend','logout'
  );
