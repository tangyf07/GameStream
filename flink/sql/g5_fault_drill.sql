-- =============================================================================
-- GameStream G5 ONLY: TaskManager kill / failover drill
-- Reuses G4 checkpoint + Rank(event_id) idempotency pattern.
-- NOT G6 skew/Lag/P95, NOT G7 polish-only.
--
-- Teachable chain (interview):
--   Checkpoint → barrier → operator state → Kafka offset → sink
--   → restart-strategy → docker kill TM → JM failover → replay from CP
--   → Rank dedup / upsert idempotency → aggregates stable (no double money)
--
-- Sink honesty:
--   Kafka upsert sink = at-least-once + primary-key idempotent upsert
--   Doris UNIQUE KEY = absorbs ALS retries — NOT end-to-end EO-2PC
-- =============================================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'pipeline.name' = 'g5-fault-drill';

-- Checkpoint (compose FLINK_PROPERTIES + SET reinforce)
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause-between-checkpoints' = '5s';
SET 'execution.checkpointing.timeout' = '2min';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'execution.checkpointing.num-retained' = '3';
SET 'state.backend.type' = 'hashmap';
SET 'state.checkpoints.dir' = 'file:///checkpoints';

-- G5: allow job to recover after TaskManager loss (JM stays up)
SET 'restart-strategy.type' = 'fixed-delay';
SET 'restart-strategy.fixed-delay.attempts' = '10';
SET 'restart-strategy.fixed-delay.delay' = '5s';

CREATE TABLE kafka_g5_recharge (
    event_id      STRING,
    event_type    STRING,
    event_time    TIMESTAMP(3),
    player_id     BIGINT,
    server_id     INT,
    amount        DECIMAL(18, 2),
    tag           STRING,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'gamestream.g5.recharge',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'gamestream-g5-fault-v1',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.timestamp-format.standard' = 'SQL'
);

-- Rank dedup (changelog). Unbounded GROUP BY supports retract — unlike TUMBLE.
CREATE VIEW v_g5_recharge_dedup AS
SELECT event_id, event_type, event_time, player_id, server_id, amount, tag
FROM (
    SELECT
        e.*,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time ASC) AS rn
    FROM kafka_g5_recharge e
    WHERE event_id IS NOT NULL
      AND player_id IS NOT NULL
      AND amount IS NOT NULL
      AND event_type = 'recharge'
) t
WHERE rn = 1;

CREATE TABLE kafka_g5_recharge_dedup (
    event_id      STRING,
    event_type    STRING,
    event_time    TIMESTAMP(3),
    player_id     BIGINT,
    server_id     INT,
    amount        DECIMAL(18, 2),
    tag           STRING,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gamestream.g5.recharge_dedup',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE kafka_g5_recharge_agg (
    metric_id      STRING,
    recharge_cnt   BIGINT,
    amount_sum     DECIMAL(18, 2),
    player_cnt     BIGINT,
    PRIMARY KEY (metric_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gamestream.g5.recharge_agg',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

BEGIN STATEMENT SET;

INSERT INTO kafka_g5_recharge_dedup
SELECT event_id, event_type, event_time, player_id, server_id, amount, tag
FROM v_g5_recharge_dedup;

INSERT INTO kafka_g5_recharge_agg
SELECT
    CAST('g5_recharge' AS STRING) AS metric_id,
    CAST(COUNT(*) AS BIGINT) AS recharge_cnt,
    CAST(SUM(amount) AS DECIMAL(18, 2)) AS amount_sum,
    CAST(COUNT(DISTINCT player_id) AS BIGINT) AS player_cnt
FROM v_g5_recharge_dedup;

END;
