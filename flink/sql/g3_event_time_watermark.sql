-- =============================================================================
-- GameStream G3 ONLY: Event Time + Watermark + Dedup + TUMBLE (streaming)
-- NOT G4 / G5 / G6.
--
-- Interview points:
--   1. Event time from payload `event_time` (NOT processing time)
--   2. WATERMARK FOR event_time AS event_time - INTERVAL 'N' SECOND (default N=5)
--   3. Late = drop (allowedLateness=0). Side-output needs DataStream API — N/A in pure SQL.
--   4. Dedup: ROW_NUMBER → upsert-kafka audit; window uses COUNT(DISTINCT event_id)
--      (Rank→Window rejected: window needs append-only; Rank is changelog).
--   5. idle-timeout so watermark can advance when source idles after produce.
--
-- RAM note (~7.6Gi): ONE window aggregate → Kafka only. E2E loads Doris from
-- Kafka results (avoids dual window state + JDBC during the streaming job).
-- =============================================================================

SET 'execution.runtime-mode' = 'streaming';
SET 'parallelism.default' = '1';
SET 'sql-client.execution.result-mode' = 'TABLEAU';
SET 'pipeline.name' = 'g3-event-time-watermark';
SET 'table.exec.source.idle-timeout' = '5 s';

CREATE TABLE kafka_g3_events (
    event_id      STRING,
    event_type    STRING,
    event_time    TIMESTAMP(3),
    player_id     BIGINT,
    server_id     INT,
    tag           STRING,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'gamestream.g3.events',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'gamestream-g3-wm-v1',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.ignore-parse-errors' = 'true',
    'json.timestamp-format.standard' = 'SQL'
);

CREATE VIEW v_g3_dedup AS
SELECT event_id, event_type, event_time, player_id, server_id, tag
FROM (
    SELECT
        e.*,
        ROW_NUMBER() OVER (PARTITION BY event_id ORDER BY event_time ASC) AS rn
    FROM kafka_g3_events e
    WHERE event_id IS NOT NULL
      AND player_id IS NOT NULL
      AND event_time IS NOT NULL
) t
WHERE rn = 1;

CREATE TABLE kafka_g3_dedup_audit (
    event_id      STRING,
    event_type    STRING,
    event_time    TIMESTAMP(3),
    player_id     BIGINT,
    server_id     INT,
    tag           STRING,
    PRIMARY KEY (event_id) NOT ENFORCED
) WITH (
    'connector' = 'upsert-kafka',
    'topic' = 'gamestream.g3.dedup_audit',
    'properties.bootstrap.servers' = 'kafka:9092',
    'key.format' = 'json',
    'value.format' = 'json',
    'value.json.timestamp-format.standard' = 'SQL'
);

CREATE TABLE kafka_g3_window_results (
    window_start  TIMESTAMP(3),
    window_end    TIMESTAMP(3),
    event_cnt     BIGINT,
    player_cnt    BIGINT,
    metric_id     STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'gamestream.g3.window_results',
    'properties.bootstrap.servers' = 'kafka:9092',
    'format' = 'json',
    'json.timestamp-format.standard' = 'SQL',
    'sink.delivery-guarantee' = 'at-least-once'
);

BEGIN STATEMENT SET;

INSERT INTO kafka_g3_dedup_audit
SELECT event_id, event_type, event_time, player_id, server_id, tag
FROM v_g3_dedup;

INSERT INTO kafka_g3_window_results
SELECT
    TUMBLE_START(event_time, INTERVAL '1' MINUTE) AS window_start,
    TUMBLE_END(event_time, INTERVAL '1' MINUTE) AS window_end,
    CAST(COUNT(DISTINCT event_id) AS BIGINT) AS event_cnt,
    CAST(COUNT(DISTINCT player_id) AS BIGINT) AS player_cnt,
    CAST('g3_window_demo' AS STRING) AS metric_id
FROM kafka_g3_events
WHERE event_id IS NOT NULL
  AND player_id IS NOT NULL
  AND event_time IS NOT NULL
GROUP BY TUMBLE(event_time, INTERVAL '1' MINUTE);

END;
