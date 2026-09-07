-- =============================================================================
-- GameStream G6 ONLY: measured bench (Kafka→Flink→Kafka)
-- NOT G7 polish/docs sprawl. Prefer Kafka sink (Doris optional / fragile on 7.6Gi).
-- =============================================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'pipeline.name' = 'g6-bench';

SET 'execution.checkpointing.interval' = '10s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
SET 'execution.checkpointing.min-pause-between-checkpoints' = '5s';
SET 'execution.checkpointing.timeout' = '2min';
SET 'execution.checkpointing.externalized-checkpoint-retention' = 'RETAIN_ON_CANCELLATION';
SET 'execution.checkpointing.num-retained' = '3';
SET 'state.backend.type' = 'hashmap';
SET 'state.checkpoints.dir' = 'file:///checkpoints';

-- Input: produce_ts = wallclock when message was published (ISO/SQL string → TIMESTAMP)
CREATE TABLE kafka_g6_in (
    event_id      STRING,
    event_type    STRING,
    event_time    TIMESTAMP(3),
    player_id     BIGINT,
    server_id     INT,
    produce_ts    TIMESTAMP(3),
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'gamestream.g6.events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'gamestream-g6-bench-v1',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.timestamp-format.standard' = 'SQL'
);

-- Output: sink_ts = Flink CURRENT_TIMESTAMP at sink projection (processing wallclock)
CREATE TABLE kafka_g6_out (
    event_id      STRING,
    event_type    STRING,
    event_time    TIMESTAMP(3),
    player_id     BIGINT,
    server_id     INT,
    produce_ts    TIMESTAMP(3),
    sink_ts       TIMESTAMP(3)
) WITH (
    'connector' = 'kafka',
    'topic' = 'gamestream.g6.out',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'SQL',
    'sink.partitioner' = 'fixed'
);

INSERT INTO kafka_g6_out
SELECT
    event_id,
    event_type,
    event_time,
    player_id,
    server_id,
    produce_ts,
    CAST(CURRENT_TIMESTAMP AS TIMESTAMP(3)) AS sink_ts
FROM kafka_g6_in
WHERE event_id IS NOT NULL
  AND player_id IS NOT NULL
  AND produce_ts IS NOT NULL;
