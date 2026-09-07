-- =============================================================================
-- GameStream G4 ONLY: Checkpoint + recharge event_id idempotency
-- NOT G5 kill-TM, NOT G6 skew, NOT invented Lag/P95.
--
-- Teachable chain:
--   checkpoint interval → barrier alignment → operator state snapshot
--   → Kafka source offsets in CP → cancel/retain → restore
--   → replay / re-produce dups → Rank dedup absorbs → aggregates stable
--
-- Sink honesty:
--   Kafka upsert sink = at-least-once + primary-key idempotent upsert
--   Doris JDBC (optional load) = at-least-once + UNIQUE KEY dedup
--   NOT end-to-end exactly-once 2PC (no Kafka transactional sink here)
-- =============================================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'pipeline.name' = 'g4-checkpoint-idempotency';

-- Checkpoint (also set in compose FLINK_PROPERTIES; SET reinforces for sql-client)
SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause-between-checkpoints' = '5s';
SET 'execution.checkpointing.timeout' = '2min';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'execution.checkpointing.num-retained' = '3';
SET 'state.backend.type' = 'hashmap';
SET 'state.checkpoints.dir' = 'file:///checkpoints';

CREATE TABLE kafka_g4_recharge (
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
    'topic' = 'gamestream.g4.recharge',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'gamestream-g4-cp-v1',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.timestamp-format.standard' = 'SQL'
);

-- Rank dedup (changelog). Unbounded GROUP BY supports retract — unlike TUMBLE.
CREATE VIEW v_g4_recharge_dedup AS
SELECT event_id, event_type, event_time, player_id, server_id, amount, tag
FROM (
    SELECT
        e.*,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time ASC) AS rn
    FROM kafka_g4_recharge e
    WHERE event_id IS NOT NULL
      AND player_id IS NOT NULL
      AND amount IS NOT NULL
      AND event_type = 'recharge'
) t
WHERE rn = 1;

-- Per-event audit (upsert by event_id) — proves first-wins dedup
CREATE TABLE kafka_g4_recharge_dedup (
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
    'topic' = 'gamestream.g4.recharge_dedup',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.json.timestamp-format.standard' = 'SQL'
);

-- Global recharge totals (upsert by metric_id)
CREATE TABLE kafka_g4_recharge_agg (
    metric_id      STRING,
    recharge_cnt   BIGINT,
    amount_sum     DECIMAL(18, 2),
    player_cnt     BIGINT,
    PRIMARY KEY (metric_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gamestream.g4.recharge_agg',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json'
);

BEGIN STATEMENT SET;

INSERT INTO kafka_g4_recharge_dedup
SELECT event_id, event_type, event_time, player_id, server_id, amount, tag
FROM v_g4_recharge_dedup;

INSERT INTO kafka_g4_recharge_agg
SELECT
    CAST('g4_recharge' AS STRING) AS metric_id,
    CAST(COUNT(*) AS BIGINT) AS recharge_cnt,
    CAST(SUM(amount) AS DECIMAL(18, 2)) AS amount_sum,
    CAST(COUNT(DISTINCT player_id) AS BIGINT) AS player_cnt
FROM v_g4_recharge_dedup;

END;
